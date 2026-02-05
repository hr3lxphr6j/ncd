//! HTTP クライアントのラッパー
//!
//! リトライ機構付きの HTTP リクエストとファイルダウンロード機能を提供
//! nicochannel.jp API に必要なデフォルトヘッダーを自動追加

use backoff::ExponentialBackoff;
use backoff::future::retry;
use futures_util::TryStreamExt;
use std::path::Path;
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// ユーザーエージェント文字列（ブラウザを模倣）
static UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36";

/// ダウンロードエラータイプ
#[derive(Error, Debug)]
pub enum DownloadError {
    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("http error")]
    Http(#[from] reqwest::Error),
}

/// HTTP クライアント
///
/// reqwest::Client をカプセル化し、以下を提供：
/// - 自動リトライ機構（指数バックオフ）
/// - デフォルトヘッダー（Origin, Referer, fc_site_id など）
/// - ファイルダウンロード（レジューム機能をサポート）
pub struct HttpXClient {
    inner: reqwest::Client,
}

/// ダウンロード進捗コールバック関数の型
///
/// 引数：`(chunk_size, downloaded_size, total_size)`
pub type ProgressCallback = dyn Fn(usize, u64, u64) + Send + Sync;

pub type ReqBuilderCallback =
    dyn Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder + Send + Sync;

impl HttpXClient {
    /// 新しい HTTP クライアントを作成
    pub fn new(headers: Option<reqwest::header::HeaderMap>) -> Result<HttpXClient, reqwest::Error> {
        let mut builder = reqwest::Client::builder();
        builder = builder.user_agent(UA);
        if let Some(h) = headers {
            builder = builder.default_headers(h);
        }
        let client = builder.build()?;
        Ok(HttpXClient { inner: client })
    }

    /// nicochannel.jp API に必要なデフォルトヘッダーを追加
    ///
    /// # 引数
    /// - `builder`: リクエストビルダー
    /// - `fc_site_id`: チャンネルサイト ID（提供されている場合は使用、それ以外はデフォルト値 "1" を使用）
    ///
    /// # 戻り値
    /// ヘッダーが追加されたリクエストビルダー
    // fn add_default_headers(
    //     &self,
    //     builder: reqwest::RequestBuilder,
    //     fc_site_id: Option<&str>,
    // ) -> reqwest::RequestBuilder {
    //     let mut builder = builder
    //         .header("Origin", "https://nicochannel.jp") // リクエストの送信元
    //         .header("Referer", "https://nicochannel.jp/") // 参照ページ
    //         .header("fc_use_device", "null"); // デバイスタイプ
    //
    //     // サイト ID を設定（API 認証に使用）
    //     if let Some(site_id) = fc_site_id {
    //         builder = builder.header("fc_site_id", site_id);
    //     } else {
    //         builder = builder.header("fc_site_id", "1"); // デフォルト値
    //     }
    //
    //     builder
    // }

    pub fn inner(&self) -> &reqwest::Client {
        &self.inner
    }

    pub async fn get_with_retry(
        &self,
        url: &str,
        builder_fn: Option<&ReqBuilderCallback>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        retry(ExponentialBackoff::default(), {
            move || {
                let mut builder = self.inner.get(url);
                if let Some(f) = builder_fn {
                    builder = f(builder)
                }
                async move { Ok(builder.send().await?) }
            }
        })
        .await
    }

    pub async fn post_with_retry(
        &self,
        url: &str,
        builder_fn: Option<&ReqBuilderCallback>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        retry(ExponentialBackoff::default(), {
            move || {
                let mut builder = self.inner.post(url);
                if let Some(f) = builder_fn {
                    builder = f(builder)
                }
                async move { Ok(builder.send().await?) }
            }
        })
        .await
    }

    pub async fn download<P: AsRef<Path>>(
        &self,
        url: &str,
        output: P,
        resume: bool,
        builder_fn: Option<&ReqBuilderCallback>,
        progress: Option<&ProgressCallback>,
    ) -> Result<(), DownloadError> {
        let output = output.as_ref();

        // ===== 1. レジュームかどうかを判断 =====
        let mut downloaded_size: u64 = 0;
        let mut mode_append = false;

        if resume && output.exists() {
            downloaded_size = tokio::fs::metadata(output).await?.len();
            if downloaded_size > 0 {
                mode_append = true;
            }
        }

        // ===== 2. HTTP リクエストを構築 =====
        let mut builder = self.inner.get(url);
        if let Some(f) = builder_fn {
            builder = f(builder);
        }
        if resume && downloaded_size > 0 {
            builder = builder.header(
                reqwest::header::RANGE,
                format!("bytes={}-", downloaded_size),
            );
        }
        let resp = builder.send().await?;
        resp.error_for_status_ref()?;

        // ===== 3. サーバーが Range をサポートしていない場合の処理 =====
        let status = resp.status();
        if resume && downloaded_size > 0 && status != reqwest::StatusCode::PARTIAL_CONTENT {
            // サーバーが 206 をサポートしていない場合、全量ダウンロードにフォールバック
            downloaded_size = 0;
            mode_append = false;
        }

        // ===== 4. total_size を計算 =====
        let content_length = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let total_size = downloaded_size + content_length;

        // ===== 5. ファイルを開く =====
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(mode_append)
            .truncate(!mode_append)
            .open(output)
            .await?;

        // ===== 6. ストリーミング書き込み =====
        let mut stream = resp.bytes_stream();

        while let Some(chunk) = stream.try_next().await? {
            file.write_all(&chunk).await?;
            downloaded_size += chunk.len() as u64;

            if let Some(cb) = progress {
                cb(chunk.len(), downloaded_size, total_size);
            }
        }

        Ok(())
    }

    pub async fn download_with_retry<P: AsRef<Path>>(
        &self,
        url: &str,
        output: P,
        resume: bool,
        builder_fn: Option<&ReqBuilderCallback>,
        progress: Option<&ProgressCallback>,
    ) -> Result<(), DownloadError> {
        let output = output.as_ref();
        retry(ExponentialBackoff::default(), || async {
            self.download(url, output, resume, builder_fn, progress)
                .await?;
            Ok(())
        })
        .await
    }
}

impl From<reqwest::Client> for HttpXClient {
    fn from(client: reqwest::Client) -> Self {
        HttpXClient { inner: client }
    }
}
