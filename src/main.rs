//! NicoChannel 動画ダウンロードツール
//!
//! nicochannel.jp から単一の動画またはチャンネル全体のすべての動画をダウンロードすることをサポート
//! HLS (HTTP Live Streaming) プロトコルを使用して動画ストリームをダウンロード

use clap::Parser;
use log::{error, info};
use ncd::nicochannel::client::{NicoChannelClient, NicoChannelError};
use regex::Regex;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use tokio::signal;

/// コマンドライン引数の定義
#[derive(Parser, Debug)]
#[command(name = "ncd")]
#[command(about = "Download videos from nicochannel.jp", long_about = None)]
struct Args {
    /// 出力ディレクトリ
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// インクリメンタルモード：既存のファイルに遭遇した場合、ダウンロードを停止
    #[arg(short, long)]
    incremental: bool,

    /// 永続化データベースではなくメモリデータベースを使用（未実装）
    #[arg(long)]
    no_persistence_db: bool,

    /// ダウンロードする URL のリスト
    urls: Vec<String>,
}

lazy_static::lazy_static! {
    // グローバル状態：ダウンロード中のファイルを追跡
    static ref DOWNLOADING_FILES: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(Vec::new()));
}

async fn download(
    nc: &mut NicoChannelClient,
    args: &Args,
    vid: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // ===== 単一の動画をダウンロード =====
    // 出力ファイルパスを取得して登録
    let output_file = {
        let video_info = nc.video_info(vid).await?;
        let title = video_info["title"]
            .as_str()
            .ok_or("Failed to get title from video info")?;
        let content_code = video_info["content_code"]
            .as_str()
            .ok_or("Failed to get content_code from video info")?;
        args.output_dir.join(format!(
            "{}.mkv",
            NicoChannelClient::output_filename(title, content_code)
        ))
    };

    // ファイルパスを登録
    {
        let mut files = DOWNLOADING_FILES.lock()?;
        files.push(output_file.clone());
    }

    // ダウンロード実行
    let result = nc.download_video(vid, &args.output_dir).await;

    // ファイルパスを登録から削除
    {
        let mut files = DOWNLOADING_FILES.lock()?;
        files.retain(|f| f != &output_file);
    }

    result
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ログシステムを初期化
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // シグナルハンドラーを設定：Ctrl+C (SIGINT) と SIGTERM を監視
    let downloading_files = Arc::clone(&DOWNLOADING_FILES);
    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }

        // シグナルを受信したら、ダウンロード中のファイルを削除
        let files = downloading_files.lock().unwrap();
        for file in files.iter() {
            if file.exists() {
                let _ = std::fs::remove_file(file).inspect_err(|e| {
                    error!("failed to remove file: {:?}", e);
                });
            }
        }
        std::process::exit(130); // SIGINT の標準的な終了コード
    });

    // ffmpeg バイナリが存在するか確認
    let ffmpeg_check = if cfg!(target_os = "windows") {
        Command::new("where").arg("ffmpeg").output()
    } else {
        Command::new("which").arg("ffmpeg").output()
    };

    match ffmpeg_check {
        Ok(output) if output.status.success() => {
            // ffmpeg が見つかった
        }
        _ => {
            eprintln!("Error: ffmpeg binary not found.");
            eprintln!("Please install ffmpeg and ensure it is in your PATH.");
            eprintln!("Visit https://ffmpeg.org/download.html for installation instructions.");
            std::process::exit(1);
        }
    }

    let args = Args::parse();

    // 少なくとも1つの URL が提供されているか確認
    if args.urls.is_empty() {
        eprintln!("Error: at least one URL is required");
        std::process::exit(1);
    }

    // 出力ディレクトリを作成
    std::fs::create_dir_all(&args.output_dir)?;

    // URL を解析する正規表現
    // 形式に一致: https://nicochannel.jp/{channel_name}/video/{video_id}
    // または: https://nicochannel.jp/{channel_name}
    let site_regex = Regex::new(
        r"https?://nicochannel.jp/(?P<channel_name>[^/]*)/?(?:video/(?P<video_id>\w*))?",
    )?;

    // 各 URL を処理
    for url in &args.urls {
        // URL からチャンネル名と動画 ID を抽出
        let caps = site_regex.captures(&url).ok_or("Invalid URL format")?;
        let channel_name = caps
            .name("channel_name")
            .ok_or("Failed to extract channel name from URL")?
            .as_str();
        let video_id = caps.name("video_id").map(|m| m.as_str());

        // クライアントを作成し、チャンネル ID を読み込む
        let mut client = NicoChannelClient::new();
        let channel_id = client.load_channel_id(channel_name).await?;

        if let Some(vid) = video_id {
            match download(&mut client, &args, vid).await {
                Ok(_) => info!("Successfully downloaded video {}", vid),
                Err(e) => {
                    if let Some(_) = e.downcast_ref::<NicoChannelError>() {
                        if args.incremental {
                            // ファイルが既に存在する場合の処理
                            info!("File exists, stopping (incremental mode)");
                            return Ok(());
                        }
                    } else {
                        error!("Error downloading video {}: {}", vid, e);
                    }
                }
            }
        } else {
            // ===== チャンネルのすべての動画をダウンロード =====
            let videos = client.video_pages(channel_id).await?;

            for video in videos {
                // 動画タイプのみを処理（video_media_type.id == 1）
                if video["video_media_type"]["id"].as_i64() != Some(1) {
                    continue;
                }

                // 動画の権限を確認
                // delivery_target_id: 1=会員限定, 3=有料限定
                // 限定動画で無料期間がない場合はスキップ
                let delivery_target_id = video["video_delivery_target"]["id"].as_i64();
                if delivery_target_id == Some(1) || delivery_target_id == Some(3) {
                    if video["video_free_periods"].is_null() {
                        continue;
                    }
                }

                // 動画情報を抽出
                let content_code = video["content_code"]
                    .as_str()
                    .ok_or("Failed to get content_code from video")?;

                match download(&mut client, &args, content_code).await {
                    Ok(_) => info!("Successfully downloaded video {}", content_code),
                    Err(e) => {
                        // ファイルが既に存在する場合の処理
                        if let Some(_) = e.downcast_ref::<NicoChannelError>() {
                            if args.incremental {
                                info!("File exists, stopping (incremental mode)");
                                return Ok(());
                            }
                        } else {
                            error!("Error downloading video {}: {}", content_code, e);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
