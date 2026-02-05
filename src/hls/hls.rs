//! HLS (HTTP Live Streaming) ダウンローダー
//!
//! M3U8 プレイリストのダウンロードと解析、MPEG-TS セグメントのダウンロード、
//! AES-128 暗号化の処理、および FFmpeg へのデータストリーミングを担当

use crate::httpx;
use aes::Aes128;
use cbc::Decryptor;
use cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};
use kdam::{BarExt, tqdm};
use m3u8_rs;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinError;

/// HLS ダウンローダー
///
/// HTTP クライアントを使用して HLS ストリームをダウンロードし、以下をサポート：
/// - Master Playlist と Media Playlist の解析
/// - AES-128 暗号化セグメントの復号化
/// - FFmpeg へのストリーミング転送によるパッケージング
pub struct HLSDownloader {
    /// HTTP クライアント
    hc: Arc<httpx::HttpXClient>,
}

/// HLS 下载器错误类型
#[derive(Error, Debug)]
pub enum Error {
    #[error("http error")]
    Http(#[from] reqwest::Error),
    #[error("httpx error")]
    HttpXError(#[from] httpx::DownloadError),
    #[error("retry error")]
    RetryError(#[from] backoff::Error<reqwest::Error>),
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("decrypted error")]
    DecryptedError,
    #[error("send error")]
    SendError(#[from] tokio_mpsc::error::SendError<Vec<u8>>),
    #[error("join error")]
    JoinError(#[from] JoinError),
}

impl HLSDownloader {
    /// 新しい HLS ダウンローダーを作成
    pub fn new(hc: Arc<httpx::HttpXClient>) -> Self {
        Self { hc }
    }

    /// FFmpeg プロセスを起動
    ///
    /// FFmpeg は標準入力から MPEG-TS ストリームを読み取り、output_args に基づいてパッケージングする
    ///
    /// # 引数
    /// - `output`: 出力ファイルパス
    /// - `output_args`: FFmpeg の追加パラメータ（メタデータ、添付ファイルなど）
    fn spawn_ffmpeg<I, S, P>(output: P, output_args: Option<I>) -> tokio::process::Child
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut cmd = Command::new("ffmpeg");
        // 基本パラメータ：バナーを非表示、エラーログレベル、パイプから MPEG-TS を読み取り
        cmd.args([
            "-hide_banner", // バージョン情報を非表示
            "-loglevel",
            "error",    // エラーのみ表示
            "-nostats", // 統計情報を表示しない
            "-y",       // 出力ファイルを上書き
            "-f",
            "mpegts", // 入力形式は MPEG-TS
            "-i",
            "pipe:0", // 標準入力から読み取り
        ]);

        // 追加の出力パラメータを追加（メタデータ、添付ファイルなど）
        if let Some(args) = output_args {
            cmd.args(args);
        }

        cmd.arg(output.as_ref());

        // 標準入力をパイプに設定し、標準出力とエラー出力をリダイレクト
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ffmpeg failed")
    }

    /// IV (Initialization Vector) 文字列を解析
    ///
    /// IV は16進数文字列の可能性があり、形式は "0x..." または純粋な16進数
    ///
    /// # 引数
    /// - `hex_iv`: 16進数 IV 文字列
    ///
    /// # 戻り値
    /// 16バイトの IV 配列
    fn parse_iv(hex_iv: &str) -> [u8; 16] {
        // "0x" プレフィックスを削除（存在する場合）
        let hex_iv = hex_iv.trim_start_matches("0x");
        let bytes = hex::decode(hex_iv).unwrap();

        let mut iv = [0u8; 16];
        iv.copy_from_slice(&bytes);
        iv
    }

    /// AES-128-CBC を使用して HLS TS セグメントを復号化
    ///
    /// # 引数
    /// - `encrypted`: 暗号化されたデータ
    /// - `key`: 16バイトの AES キー
    /// - `iv`: 16バイトの初期化ベクトル
    ///
    /// # 戻り値
    /// 復号化されたデータ
    fn decrypt_hls_ts(encrypted: &[u8], key: &[u8; 16], iv: &[u8; 16]) -> Result<Vec<u8>, Error> {
        // AES-128-CBC 復号化器を作成
        let cipher = Decryptor::<Aes128>::new(key.into(), iv.into());

        let mut buffer = encrypted.to_vec();

        // PKCS7 パディングを使用して復号化
        let decrypted = cipher
            .decrypt_padded_mut::<Pkcs7>(&mut buffer)
            .map_err(|_| Error::DecryptedError)?;

        Ok(decrypted.to_vec())
    }

    /// 単一の HLS セグメントをダウンロードして処理
    ///
    /// フロー：
    /// 1. セグメントを一時ファイルにダウンロード
    /// 2. セグメントが暗号化されている場合、復号化
    /// 3. 復号化されたデータを channel に送信し、FFmpeg が読み取れるようにする
    ///
    /// # 引数
    /// - `frag`: M3U8 セグメント情報
    /// - `tx`: 復号化されたデータを送信するための channel 送信側
    /// - `key`: AES キー（セグメントが暗号化されている場合）
    /// - `iv`: 初期化ベクトル（セグメントが暗号化されている場合）
    /// - `fragment_pb`: セグメントダウンロードのプログレスバー（オプション）
    async fn parse_segment(
        &mut self,
        frag: &m3u8_rs::MediaSegment,
        tx: &tokio_mpsc::Sender<Vec<u8>>,
        key: Option<&[u8; 16]>,
        iv: Option<&[u8; 16]>,
        fragment_pb: Option<&mut kdam::Bar>,
    ) -> Result<(), Error> {
        // セグメントを保存する一時ファイルを作成
        let tmp_file = tempfile::NamedTempFile::new()?;
        let tmp_path = tmp_file.path();

        // セグメントをダウンロード（ライフタイムの問題のため、プログレスコールバックは使用しない）
        // プログレスバーはダウンロード完了後に更新
        let progress_cb: Option<Box<dyn Fn(usize, u64, u64) + Send + Sync>> = None;

        self.hc
            .download_with_retry(
                &frag.uri.as_str(),
                tmp_path,
                true,
                None,
                progress_cb.as_deref(), // レジューム機能をサポート
            )
            .await?;

        // セグメントプログレスバーを更新（ダウンロード完了後）
        if let Some(pb) = fragment_pb {
            let file_size = tokio::fs::metadata(tmp_path).await?.len();
            if pb.total == 0 && file_size > 0 {
                pb.total = file_size as usize;
            }
            pb.update(file_size as usize).expect("TODO: panic message");
            pb.refresh().expect("TODO: panic message");
        }

        // ダウンロードしたファイルを読み取り
        let mut file = OpenOptions::new().read(true).open(tmp_path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        // セグメントが暗号化されている場合、復号化
        let decrypted = if let (Some(key), Some(iv)) = (key, iv) {
            Self::decrypt_hls_ts(&buf, key, iv)?
        } else {
            buf
        };

        // 復号化されたデータを channel に送信し、FFmpeg が読み取れるようにする
        tx.send(decrypted).await?;
        Ok(())
    }

    /// Media Playlist を解析してダウンロード
    ///
    /// 処理フロー：
    /// 1. 暗号化キーを取得（プレイリストが暗号化されている場合）
    /// 2. FFmpeg プロセスを起動
    /// 3. 復号化されたデータを FFmpeg の標準入力に書き込む非同期タスクを作成
    /// 4. すべてのセグメントを走査し、ダウンロード、復号化して FFmpeg に送信
    /// 5. FFmpeg のパッケージング完了を待機
    ///
    /// # 引数
    /// - `pl`: Media Playlist オブジェクト
    /// - `output`: 出力ファイルパス
    /// - `ffmpeg_args`: FFmpeg の追加パラメータ
    async fn parse_media_playlist(
        &mut self,
        pl: m3u8_rs::MediaPlaylist,
        output: &Path,
        ffmpeg_args: Option<&[&str]>,
    ) -> Result<(), Error> {
        // ===== ステップ 1: 暗号化キーを取得 =====
        // HLS 標準では、キーは通常最初のセグメントの EXT-X-KEY タグで定義される
        let mut key: Option<[u8; 16]> = None;
        let mut iv: Option<[u8; 16]> = None;

        if let Some(first_seg) = pl.segments.first() {
            if let Some(ref key_item) = first_seg.key {
                if let m3u8_rs::KeyMethod::AES128 = key_item.method {
                    if let Some(ref uri) = key_item.uri {
                        // キーサーバーからキーをダウンロード
                        let key_resp = self.hc.get_with_retry(uri, None).await?;
                        let key_bytes = key_resp.bytes().await?;
                        if key_bytes.len() < 16 {
                            return Err(Error::DecryptedError);
                        }
                        let key_arr: [u8; 16] = key_bytes[..16].try_into().unwrap();
                        key = Some(key_arr);

                        // IV を解析（提供されている場合）
                        if let Some(ref iv_str) = key_item.iv {
                            iv = Some(Self::parse_iv(iv_str));
                        } else {
                            // IV が提供されていない場合、すべてゼロの IV を使用
                            iv = Some([0u8; 16]);
                        }
                    }
                }
            }
        }

        // ===== ステップ 2: ダウンロードタスクと FFmpeg の間でデータを渡すための channel を作成 =====
        let (tx, mut rx) = tokio_mpsc::channel::<Vec<u8>>(100);

        // ===== ステップ 3: FFmpeg プロセスを起動 =====
        let mut ffmpeg: tokio::process::Child = Self::spawn_ffmpeg(output, ffmpeg_args);
        let stdin = ffmpeg.stdin.take().unwrap();

        // ===== ステップ 4: 復号化されたデータを FFmpeg の標準入力に書き込む非同期タスクを作成 =====
        let writer = tokio::spawn(async move {
            let mut stdin = stdin;
            loop {
                match rx.recv().await {
                    Some(ts) => {
                        // セグメントデータを FFmpeg の標準入力に書き込み
                        if stdin.write_all(&ts).await.is_err() {
                            break;
                        }
                        // 各セグメントを書き込むたびに flush し、データが確実に送信されるようにする
                        if stdin.flush().await.is_err() {
                            break;
                        }
                    }
                    None => {
                        // channel が閉じられ、これ以上のデータがない
                        break;
                    }
                }
            }
            // 閉じる前に再度 flush して shutdown し、すべてのデータが書き込まれるようにする
            let _ = stdin.flush().await;
            let _ = stdin.shutdown().await;
        });

        // ===== ステップ 5: プログレスバーを作成 =====
        // 総合プログレスバー：すべてのセグメントのダウンロード進捗を表示
        let mut total_pb = tqdm!(
            total = pl.segments.len(),
            desc = "Total",
            ncols = 80,
            position = 0,
            leave = true
        );

        // ===== ステップ 6: すべてのセグメントを走査し、ダウンロード、復号化して送信 =====
        for (idx, f) in pl.segments.iter().enumerate() {
            // セグメントプログレスバーを作成：現在のセグメントのダウンロード進捗を表示
            let mut fragment_pb = tqdm!(
                desc = format!("Fragment: {}", idx + 1),
                unit = "B",
                unit_scale = true,
                ncols = 80,
                position = 1,
                leave = false
            );

            // 現在のセグメントに独自のキーがあるか確認
            // セグメントに独自のキーがある場合、セグメントのキーを使用；それ以外はプレイリストレベルのキーを使用
            let seg_key = if let Some(ref k) = f.key {
                if let Some(ref uri) = k.uri {
                    if let m3u8_rs::KeyMethod::AES128 = k.method {
                        // セグメント固有のキーをダウンロード
                        let key_resp = self.hc.get_with_retry(uri, None).await?;
                        let key_bytes = key_resp.bytes().await?;
                        if key_bytes.len() >= 16 {
                            let key_arr: [u8; 16] = key_bytes[..16].try_into().unwrap();
                            let seg_iv = if let Some(ref iv_str) = k.iv {
                                Self::parse_iv(iv_str)
                            } else {
                                [0u8; 16]
                            };
                            (Some(key_arr), Some(seg_iv))
                        } else {
                            // キー長が不足している場合、プレイリストレベルのキーを使用
                            (key, iv)
                        }
                    } else {
                        (key, iv)
                    }
                } else {
                    (key, iv)
                }
            } else {
                // セグメントにキーがない場合、プレイリストレベルのキーを使用
                (key, iv)
            };

            // セグメントをダウンロード、復号化して送信
            self.parse_segment(
                f,
                &tx,
                seg_key.0.as_ref(),
                seg_key.1.as_ref(),
                Some(&mut fragment_pb),
            )
            .await?;
            fragment_pb.refresh().expect("TODO: panic message");
            total_pb.update(1).expect("TODO: panic message");
        }
        total_pb.refresh().expect("TODO: panic message");

        // ===== ステップ 7: channel を閉じ、すべてのデータの書き込み完了を待機 =====
        drop(tx); // 送信側を閉じ、writer タスクにこれ以上のデータがないことを通知
        writer.await?;

        // ===== ステップ 8: FFmpeg のパッケージング完了を待機 =====
        let status = ffmpeg.wait().await?;
        if !status.success() {
            return Err(Error::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("ffmpeg exited with status: {}", status),
            )));
        }

        Ok(())
    }

    /// プレイリストをダウンロード（Master Playlist と Media Playlist をサポート）
    ///
    /// URL が Master Playlist を指している場合、最高ビットレートのバリアントを選択して再帰的にダウンロード
    /// URL が Media Playlist を指している場合、すべてのセグメントを直接ダウンロード
    ///
    /// # 引数
    /// - `url`: M3U8 プレイリスト URL
    /// - `output`: 出力ファイルパス
    /// - `ffmpeg_args`: FFmpeg の追加パラメータ
    async fn download_playlist(
        &mut self,
        mut url: String,
        output: &Path,
        ffmpeg_args: Option<&[&str]>,
    ) -> Result<(), Error> {
        loop {
            // プレイリストをダウンロードして解析
            let resp = self.hc.get_with_retry(&url, None).await?;
            let payload = resp.bytes().await?;
            let (_, pl) = m3u8_rs::parse_playlist(&payload).unwrap();

            match pl {
                // Media Playlist: すべてのセグメントを直接処理
                m3u8_rs::Playlist::MediaPlaylist(pl) => {
                    return self.parse_media_playlist(pl, output, ffmpeg_args).await;
                }
                // Master Playlist: 最高ビットレートのバリアントを選択して続行
                m3u8_rs::Playlist::MasterPlaylist(mp) => {
                    // 帯域幅が最も高いバリアントを選択（通常は最高画質を意味する）
                    let target_pl = mp
                        .variants
                        .iter()
                        .max_by(|&a, &b| a.bandwidth.cmp(&b.bandwidth))
                        .unwrap();

                    // 選択したプレイリスト情報を記録
                    if let Some(res) = target_pl.resolution {
                        log::info!(
                            "choose playlist: bandwidth: {} resolution: {}",
                            target_pl.bandwidth,
                            res
                        );
                    } else {
                        log::info!("choose playlist: bandwidth: {}", target_pl.bandwidth);
                    }

                    // URL を選択した Media Playlist URL に更新し、ループを継続
                    url = target_pl.uri.as_str().to_string();
                }
            }
        }
    }

    /// HLS ストリームをダウンロード
    ///
    /// これは外部に提供されるパブリックインターフェースで、内部で Master Playlist と Media Playlist を処理する
    ///
    /// # 引数
    /// - `url`: M3U8 プレイリスト URL
    /// - `output`: 出力ファイルパス
    /// - `ffmpeg_args`: FFmpeg の追加パラメータ（メタデータ、添付ファイルなど）
    pub async fn download<P: AsRef<Path>>(
        &mut self,
        url: &str,
        output: P,
        ffmpeg_args: Option<&[&str]>,
    ) -> Result<(), Error> {
        self.download_playlist(url.to_string(), output.as_ref(), ffmpeg_args)
            .await
    }
}
