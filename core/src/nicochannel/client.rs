//! NicoChannel API クライアント
//!
//! nicochannel.jp API との相互作用を担当し、以下を含む：
//! - チャンネル情報の取得
//! - 動画リストの取得（一括 `video_pages` または遅延ストリーム `video_iter`）
//! - 動画詳細情報と HLS ストリーム URL の取得
//! - 動画のダウンロードとメタデータ・サムネイルの埋め込み

use crate::hls::{HLSDownloader, HlsProgress};
use crate::httpx::HttpXClient;
use crate::utils::FileNameUtils;
use async_stream::stream;
use futures_util::Stream;
use lazy_static::lazy_static;
use log;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

/// NicoChannel API ベース URL
const PREFIX: &str = "https://api.nicochannel.jp";

lazy_static! {
    static ref METADATA_KEY: HashSet<&'static str> = {
        HashSet::from([
            "content_code",      // コンテンツコード
            "display_date",      // 表示日付
            "description",       // 説明
            "live_finished_at",  // 配信終了時刻
            "released_at",       // 公開時刻
            "closed_at",         // 閉鎖時刻
            "title",             // タイトル
        ])
    };
}

lazy_static! {
    pub static ref DEFAULT_HEADERS: reqwest::header::HeaderMap = {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Origin", "https://nicochannel.jp".parse().unwrap());
        headers.insert("Referer", "https://nicochannel.jp/".parse().unwrap());
        headers.insert("Fc_site_id", "1".parse().unwrap());
        headers.insert("Fc_use_device", "null".parse().unwrap());
        headers
    };
}

/// NicoChannel クライアントエラータイプ
#[derive(Error, Debug)]
pub enum NicoChannelError {
    /// ファイルが既に存在
    #[error("file exist")]
    NCDFileExist,

    /// 指定ページに動画が存在しない（ページネーション終端）
    #[error("video pages empty")]
    NCDEmptyPages,

    #[error("httpx error")]
    HttpX(#[from] crate::httpx::Error),

    #[error("http error")]
    Http(#[from] reqwest::Error),
}

/// NicoChannel API クライアント
///
/// nicochannel.jp API とのすべての相互作用をカプセル化し、以下を含む：
/// - HTTP リクエスト（リトライ機構付き）
/// - 動画情報キャッシュ
/// - HLS ストリームダウンロード
pub struct NicoChannelClient<'hc> {
    /// HTTP クライアント
    hc: &'hc HttpXClient,
    /// HLS ダウンローダー
    hls_downloader: HLSDownloader<'hc>,
    /// 動画情報キャッシュ（重複リクエストを回避）
    cache: HashMap<String, Arc<serde_json::Value>>,
    /// 現在のチャンネル ID（fc_site_id ヘッダーの設定に使用）
    channel_id: Option<i64>,
}
impl<'hc> NicoChannelClient<'hc> {
    /// 新しい NicoChannel クライアントを作成
    pub fn new(hc: &'hc HttpXClient) -> Self {
        Self {
            hc,
            hls_downloader: HLSDownloader::new(hc),
            cache: HashMap::new(),
            channel_id: None,
        }
    }

    /// 現在のチャンネルの fc_site_id を取得（API リクエストのヘッダーに使用）
    fn fc_site_id(&self) -> Option<String> {
        self.channel_id.map(|id| id.to_string())
    }

    /// チャンネル名に基づいてチャンネル ID を読み込む
    ///
    /// # 引数
    /// - `channel_name`: チャンネル名（URL 内のチャンネル識別子）
    ///
    /// # 戻り値
    /// チャンネル ID、同時にクライアントの `channel_id` フィールドを更新
    pub async fn load_channel_id<S: AsRef<str>>(
        &mut self,
        channel_name: S,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let channel_name = channel_name.as_ref().to_owned();
        let url = format!("{}/fc/content_providers/channel_domain", PREFIX);
        let resp = self
            .hc
            .get_with_retry(
                &url,
                Some(&move |builder| {
                    builder.query(&[(
                        "current_site_domain",
                        format!("https://nicochannel.jp/{}", channel_name),
                    )])
                }),
            )
            .await?
            .error_for_status()?;
        let body: serde_json::Value = resp.json().await?;
        let channel_id = body["data"]["content_providers"]["id"]
            .as_i64()
            .ok_or_else(|| {
                format!(
                    "Failed to get channel_id from response: {}",
                    serde_json::to_string_pretty(&body).unwrap_or_default()
                )
            })
            .map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                    as Box<dyn std::error::Error>
            })?;

        self.channel_id = Some(channel_id);
        Ok(channel_id)
    }

    #[allow(dead_code)]
    async fn page_base_info(
        &self,
        channel_id: i64,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let url = format!("{}/fc/fanclub_sites/{}/page_base_info", PREFIX, channel_id);
        let resp = self
            .hc
            .get_with_retry(&url, None)
            .await?
            .error_for_status()?;
        let body: serde_json::Value = resp.json().await?;
        Ok(body["data"]["fanclub_site"].clone())
    }

    /// チャンネル動画の一ページ分を取得（内部用）
    ///
    /// `video_pages` および `video_iter` から利用される。
    ///
    /// # 引数
    /// - `channel_id`: チャンネル ID
    /// - `page`: ページ番号（1 始まり）
    /// - `page_size`: 1 ページあたりの件数
    /// - `sort`: ソート指定（例: `-display_date`）
    ///
    /// # 戻り値
    /// 成功時は当該ページの動画 JSON リスト。空ページの場合は `NCDEmptyPages`。
    pub(crate) async fn video_page(
        &self,
        channel_id: i64,
        page: u32,
        page_size: u32,
        sort: &str,
    ) -> Result<Vec<serde_json::Value>, NicoChannelError> {
        let url = format!("{}/fc/fanclub_sites/{}/video_pages", PREFIX, channel_id);
        let fc_site_id = self.fc_site_id().unwrap_or("1".to_string());
        let sort = sort.to_string();
        let resp = self
            .hc
            .get_with_retry(
                &url,
                Some(&move |b| {
                    b.query(&[
                        ("page", page.to_string().as_str()),
                        ("per_page", page_size.to_string().as_str()), // 1ページあたり24個の動画
                        ("sort", sort.as_str()),                      // 表示日付の降順でソート
                    ])
                    .header("Fc_site_id", fc_site_id.as_str())
                }),
            )
            .await?
            .error_for_status()?;
        let data: serde_json::Value = resp.json().await?;
        match data["data"]["video_pages"]["list"].as_array() {
            Some(list) if !list.is_empty() => Ok(list.to_vec()),
            _ => Err(NicoChannelError::NCDEmptyPages),
        }
    }

    /// チャンネル内の全動画を一括取得（ページネーション自動）
    ///
    /// 全ページを順に取得して単一の `Vec` にまとめる。メモリに載る規模でよい場合に使用。
    ///
    /// # 引数
    /// - `channel_id`: チャンネル ID
    ///
    /// # 戻り値
    /// 全動画の JSON リスト（表示日付降順）。空でない限り `NCDEmptyPages` で終端を検知して打ち切り。
    pub async fn video_pages(
        &self,
        channel_id: i64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        let mut page = 1;
        let mut videos: Vec<serde_json::Value> = Vec::new();

        // すべてのページをループで取得
        loop {
            let page_resp = self.video_page(channel_id, page, 24, "-display_date").await;
            match page_resp {
                Ok(list) => {
                    videos.extend(list);
                    page += 1;
                }
                Err(e) => match e {
                    NicoChannelError::NCDEmptyPages => break,
                    _ => return Err(Box::new(e)),
                },
            }
        }
        Ok(videos)
    }

    /// チャンネル動画の遅延ストリームを返す（async-stream）
    ///
    /// ページ単位で API を呼び出し、動画を一つずつ `yield` する。大量動画でもメモリを抑えられる。
    /// ソートは表示日付降順（`-display_date`）。
    ///
    /// # 引数
    /// - `channel_id`: チャンネル ID
    /// - `page_size`: 1 ページあたりの件数
    ///
    /// # 戻り値
    /// `Stream<Item = Result<serde_json::Value, NicoChannelError>>`。ページ取得エラー時はそのエラーを yield して終了。
    pub fn video_iter(
        self: Arc<Self>,
        channel_id: i64,
        page_size: u32,
    ) -> impl Stream<Item = Result<serde_json::Value, NicoChannelError>> + Send + use<'hc> {
        let client = self;
        stream! {
            let mut page = 1u32;
            loop {
                match client
                    .video_page(channel_id, page, page_size, "-display_date")
                    .await
                {
                    Ok(list) => {
                        if list.is_empty() {
                            break;
                        }
                        for v in list {
                            yield Ok(v);
                        }
                        page += 1;
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        }
    }

    /// `Arc<Mutex<NicoChannelClient>>` 用の遅延ストリーム（main 等で `&mut` とストリームを両立させる場合に使用）
    ///
    /// 戻り値の挙動は [`Self::video_iter`] と同じ。
    pub fn video_iter_mutex(
        client: Arc<Mutex<NicoChannelClient<'_>>>,
        channel_id: i64,
        page_size: u32,
    ) -> impl Stream<Item = Result<serde_json::Value, NicoChannelError>> + Send + use<'_> {
        stream! {
            let mut page = 1u32;
            loop {
                let list = client
                    .lock()
                    .await
                    .video_page(channel_id, page, page_size, "-display_date")
                    .await;
                match list {
                    Ok(videos) => {
                        if videos.is_empty() {
                            break;
                        }
                        for v in videos {
                            yield Ok(v);
                        }
                        page += 1;
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        }
    }

    /// 出力ファイル名を生成
    ///
    /// 形式：`{タイトル} - {コンテンツコード}`
    /// タイトルは不正な文字がクリーンアップされ、220バイトに切り詰められる
    ///
    /// # 引数
    /// - `title`: 動画タイトル
    /// - `content_code`: コンテンツコード
    pub fn output_filename(title: &str, content_code: &str) -> String {
        let title = FileNameUtils::truncate_str_by_byte_len(
            &FileNameUtils::replace_illegal_filename_characters(title),
            220,
        );
        format!("{} - {}", title, content_code)
    }

    /// 動画の詳細情報を取得（キャッシュ付き）
    ///
    /// # 引数
    /// - `video_id`: 動画 ID
    ///
    /// # 戻り値
    /// 動画情報の JSON データ（キャッシュ済みの場合は直接返却）
    pub async fn video_info(
        &mut self,
        video_id: &str,
    ) -> Result<Arc<serde_json::Value>, Box<dyn std::error::Error>> {
        if let Some(cache) = self.cache.get(video_id) {
            return Ok(cache.clone());
        }
        let url = format!("{}/fc/video_pages/{}", PREFIX, video_id);
        let fc_site_id = self.fc_site_id().unwrap_or("1".to_string());
        let resp = self
            .hc
            .get_with_retry(
                &url,
                Some(&move |b| b.header("Fc_site_id", fc_site_id.as_str())),
            )
            .await?
            .error_for_status()?;
        let body: serde_json::Value = resp.json().await?;
        let info = Arc::new(body["data"]["video_page"].clone());
        self.cache.insert(video_id.to_string(), info.clone());
        Ok(info)
    }

    /// 動画の HLS ストリーム URL を取得
    ///
    /// まず session_id を取得し、その後 authenticated_url 内のプレースホルダーを置換する必要がある
    ///
    /// # 引数
    /// - `video_id`: 動画 ID
    ///
    /// # 戻り値
    /// 完全な HLS M3U8 URL
    pub async fn get_video_hls_url(
        &mut self,
        video_id: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // 動画情報を取得（authenticated_url テンプレートを含む）
        let info = self.video_info(video_id).await?;

        // session_id を取得
        let url = format!("{}/fc/video_pages/{}/session_ids", PREFIX, video_id);
        let fc_site_id = self.fc_site_id().unwrap_or("1".to_string());
        let resp = self
            .hc
            .post_with_retry(
                &url,
                Some(&move |b| {
                    b.json(&serde_json::json!({}))
                        .header("Fc_site_id", fc_site_id.as_str())
                }),
            )
            .await?
            .error_for_status()?;
        let body: serde_json::Value = resp.json().await?;
        let session_id = body["data"]["session_id"]
            .as_str()
            .ok_or("Failed to get session_id from response")?;

        // URL テンプレート内の session_id プレースホルダーを置換
        let auth_url = info["video_stream"]["authenticated_url"]
            .as_str()
            .ok_or("Failed to get authenticated_url from video info")?;
        Ok(auth_url.replace("{session_id}", session_id))
    }

    /// 動画をダウンロード
    ///
    /// 完全なフロー：
    /// 1. 動画情報と HLS URL を取得
    /// 2. サムネイルをダウンロード
    /// 3. FFmpeg パラメータを準備（メタデータ、添付ファイルなど）
    /// 4. HLS ダウンローダーを使用して動画ストリームをダウンロード
    /// 5. ファイルのタイムスタンプを設定
    ///
    /// # 引数
    /// - `video_id`: 動画 ID
    /// - `download_dir`: ダウンロードディレクトリ
    ///
    /// # エラー
    /// 出力ファイルが既に存在する場合、`NicoChannelError::NCDFileExist` を返す
    pub async fn download_video(
        &mut self,
        video_id: &str,
        download_dir: impl AsRef<OsStr>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.download_video_with_progress(video_id, download_dir, None)
            .await
    }

    /// 動画をダウンロード（進捗コールバック付き）
    pub async fn download_video_with_progress(
        &mut self,
        video_id: &str,
        download_dir: impl AsRef<OsStr>,
        hls_progress: Option<&dyn HlsProgress>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // ===== ステップ 1: 動画情報と HLS URL を取得 =====
        let video_info = self.video_info(video_id).await?;
        let hls_url = self.get_video_hls_url(video_id).await?;
        let download_dir = PathBuf::from(download_dir.as_ref());

        // 動画情報を抽出
        let title = video_info["title"]
            .as_str()
            .ok_or("Failed to get title from video info")?;
        let content_code = video_info["content_code"]
            .as_str()
            .ok_or("Failed to get content_code from video info")?;

        // 出力ファイル名を生成
        let output_file = download_dir.join(format!(
            "{}.mkv",
            Self::output_filename(title, content_code)
        ));

        // ファイルが既に存在するか確認
        if output_file.exists() {
            return Err(Box::new(NicoChannelError::NCDFileExist));
        }

        // ===== ステップ 2: サムネイルをダウンロード =====
        // 動画にサムネイル URL がない場合、チャンネルのデフォルトサムネイルを使用
        let thumbnail_url = video_info["thumbnail_url"].as_str();
        let thumbnail_url = match thumbnail_url {
            Some(thumbnail_url) => thumbnail_url.to_string(),
            None => {
                let fanclub_site_id = video_info["fanclub_site"]["id"]
                    .as_i64()
                    .ok_or("Failed to get fanclub_site id from video info")?;
                format!(
                    "https://cdn.nicochannel.jp/public_html/site_design/fanclub_sites/{}/thumbnail_image_path",
                    fanclub_site_id
                )
            }
        };

        // サムネイルを一時ファイルにダウンロード
        let thumbnail_temp = tempfile::NamedTempFile::new()?;
        let thumbnail_path = thumbnail_temp.path();
        self.hc
            .download_with_retry(&thumbnail_url, thumbnail_path, true, None, None)
            .await?;

        // ===== ステップ 3: FFmpeg パラメータを準備 =====
        let thumbnail_path_str = thumbnail_path
            .to_str()
            .ok_or("Failed to convert thumbnail path to string")?;

        // 基本パラメータ：サムネイルを添付、ストリームコピーを使用（再エンコードしない）
        let mut ffmpeg_args: Vec<String> = vec![
            "-attach".to_string(), // 添付ファイルを追加（サムネイル）
            thumbnail_path_str.to_string(),
            "-c".to_string(),   // エンコーダー
            "copy".to_string(), // ストリームコピー（再エンコードしない）
        ];

        // 動画メタデータを追加
        let video_obj = video_info
            .as_object()
            .ok_or("Failed to get video info as object")?;
        for (k, v) in video_obj.iter() {
            if METADATA_KEY.contains(k.as_str()) {
                ffmpeg_args.push("-metadata:g".to_string()); // グローバルメタデータ
                let value = match v {
                    serde_json::Value::String(s) => s.clone(),
                    _ => v.to_string(),
                };
                ffmpeg_args.push(format!("{}={}", k, value));
            }
        }

        // サムネイル添付ファイルのメタデータを設定
        ffmpeg_args.push("-metadata:s:t:0".to_string()); // 最初の添付ファイルのメタデータ
        ffmpeg_args.push("filename=cover.jpg".to_string());
        ffmpeg_args.push("-metadata:s:t:0".to_string());
        ffmpeg_args.push("mimetype=image/jpeg".to_string());
        ffmpeg_args.push("-f".to_string()); // 出力形式
        ffmpeg_args.push("matroska".to_string()); // Matroska (MKV)

        let ffmpeg_args_str: Vec<&str> = ffmpeg_args.iter().map(|s| s.as_str()).collect();

        log::info!("Downloading video: {} to {}", title, output_file.display());

        self.hls_downloader
            .download_with_progress(&hls_url, &output_file, Some(&ffmpeg_args_str), hls_progress)
            .await?;

        // ファイルのタイムスタンプを設定
        if let Some(display_date) = video_info["display_date"].as_str() {
            if let Ok(tm) = chrono::NaiveDateTime::parse_from_str(display_date, "%Y-%m-%d %H:%M:%S")
            {
                let timestamp = tm.and_utc().timestamp();
                let _ = filetime::set_file_times(
                    &output_file,
                    filetime::FileTime::from_unix_time(timestamp, 0),
                    filetime::FileTime::from_unix_time(timestamp, 0),
                );
            }
        }

        Ok(())
    }
}
