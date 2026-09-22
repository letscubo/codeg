//! fork(letscubo)专属: 把一个本地文件传到 MyClaw 的对象存储, 返回公开链接。
//!
//! ## 为什么在 codeg 里
//!
//! 交付产物的上传此前走容器里的 `myclaw deliver` shell 命令, 凭证是
//! `~/.myclaw/agent.env` 里一把**永不过期**的 secret —— agent 能直接读到。
//! 这里改成 codeg 自己来做: 认证复用 codeg **已有的出站 webhook**
//! (`?vmId=<id>&s=<secret>`, 存在 codeg 自己的库里), 容器不必再为上传保存任何凭证,
//! 模型也只看见一个工具, 碰不到机制。
//!
//! ## 两跳
//!
//! ```text
//!   ① POST <平台>/api/codeg/upload-url?vmId&s   { filename, mimeType, size }
//!        → { uploadUrl(预签名), publicUrl, maxBytes }
//!   ② PUT  <uploadUrl>  <文件字节>              → 返回 publicUrl
//! ```
//!
//! 文件直传对象存储, 不经平台中转 —— 200MB 走平台的函数层不现实。
//!
//! 上限与重试按平台侧约定: 200MB, 每次 PUT 超时 60s, 最多 3 次。会话归属不在这里,
//! 平台按 vmId 决定文件落在谁的命名空间下, 调用方给不了、也不需要给。

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::chat_channel::webhook::{redact_url, WebhookConfig};
use crate::db::AppDatabase;

/// 出站 webhook 的路径 —— 一切端点从它派生(与 `myclaw_route` 同源)。
const EVENTS_PATH: &str = "/api/codeg/events";
/// 换预签名地址的路径。平台侧: `web/src/app/api/codeg/upload-url/route.ts`。
const UPLOAD_URL_PATH: &str = "/api/codeg/upload-url";

/// 与平台侧 `CODEG_UPLOAD_MAX_BYTES` 对齐; 本地先挡一次, 省掉一次必然被拒的往返。
pub const MAX_UPLOAD_BYTES: u64 = 200 * 1024 * 1024;
/// 单次 PUT 的超时与重试次数。
const PUT_TIMEOUT_SECS: u64 = 60;
const PUT_ATTEMPTS: u32 = 3;
/// 换地址是一次小 JSON 往返, 不需要 60s。
const SIGN_TIMEOUT_SECS: u64 = 20;

#[derive(Debug, Deserialize)]
struct SignedTarget {
    #[serde(rename = "uploadUrl")]
    upload_url: String,
    #[serde(rename = "publicUrl")]
    public_url: String,
}

/// 平台的 apiOk 外壳: `{ code, data, msg }`。
#[derive(Debug, Deserialize)]
struct ApiEnvelope {
    code: i64,
    data: Option<SignedTarget>,
    msg: Option<String>,
}

/// 从配置好的出站 webhook 派生换票端点。
///
/// `None` = 这台实例没有指向平台的 webhook, 即它不是平台托管的, 上传无从谈起。
fn upload_endpoint(hooks: &[WebhookConfig]) -> Option<String> {
    hooks
        .iter()
        .filter(|w| w.enabled)
        .find(|w| w.url.contains(EVENTS_PATH))
        .map(|w| w.url.replacen(EVENTS_PATH, UPLOAD_URL_PATH, 1))
}

/// 描述一次 reqwest 失败, **不带 URL**。
///
/// 端点的 query 里有 webhook secret, 而预签名地址本身就是一张短期凭证 ——
/// `Display` 会把 URL 原样带进日志, 那是要被 SSH 排障和诊断包捞走的文件。
fn redacted_err(e: reqwest::Error) -> String {
    e.without_url().to_string()
}

/// 猜一个 content type。猜不出用 octet-stream —— 类型不限, 不该因为认不出后缀就拒。
fn guess_mime(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("pptx") => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        Some("docx") => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("webp") => "image/webp",
        Some("mp4") => "video/mp4",
        Some("zip") => "application/zip",
        Some("json") => "application/json",
        Some("csv") => "text/csv",
        Some("md") => "text/markdown",
        Some("txt" | "log") => "text/plain",
        Some("html" | "htm") => "text/html",
        _ => "application/octet-stream",
    }
}

/// 文件名 —— 平台只拿它取扩展名, 路径部分不会被使用, 但也没必要送过去。
fn file_name_of(path: &Path) -> String {
    path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string()
}

/// 传一个文件, 成功返回公开链接。
///
/// `path` 由调用方(MCP 工具)给出。这里只要求它存在且是普通文件 —— 会话目录的限制
/// 平台侧与本模块都**不做**(2026-09-20 用户定)。
pub async fn upload_file(db: &AppDatabase, path: &str) -> Result<String, String> {
    let path_buf = PathBuf::from(path);
    let meta = tokio::fs::metadata(&path_buf)
        .await
        .map_err(|e| format!("cannot read {}: {e}", path_buf.display()))?;
    if !meta.is_file() {
        return Err(format!("not a file: {}", path_buf.display()));
    }
    let size = meta.len();
    if size == 0 {
        return Err(format!("file is empty: {}", path_buf.display()));
    }
    if size > MAX_UPLOAD_BYTES {
        return Err(format!(
            "file is {size} bytes; the limit is {MAX_UPLOAD_BYTES} bytes (200MB)"
        ));
    }

    let hooks = crate::commands::chat_channel::get_chat_event_webhooks_core(db)
        .await
        .map_err(|_| "could not read this instance's platform webhook config".to_string())?;
    let endpoint = upload_endpoint(&hooks).ok_or_else(|| {
        "this instance is not platform-managed (no webhook configured)".to_string()
    })?;

    let mime = guess_mime(&path_buf);
    let target = sign(&endpoint, &file_name_of(&path_buf), mime, size).await?;
    let body = tokio::fs::read(&path_buf)
        .await
        .map_err(|e| format!("cannot read {}: {e}", path_buf.display()))?;
    put(&target.upload_url, mime, body).await?;
    Ok(target.public_url)
}

/// ① 换预签名地址。4xx 是判决(密钥不对 / 超限), 重试没有意义, 直接失败。
async fn sign(
    endpoint: &str,
    filename: &str,
    mime: &str,
    size: u64,
) -> Result<SignedTarget, String> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(SIGN_TIMEOUT_SECS))
        .build()
        .map_err(redacted_err)?;

    let res = client
        .post(endpoint)
        .json(&serde_json::json!({
            "filename": filename,
            "mimeType": mime,
            "size": size,
        }))
        .send()
        .await
        .map_err(redacted_err)?;

    let status = res.status();
    let text = res.text().await.map_err(redacted_err)?;
    if !status.is_success() {
        tracing::error!(
            "[MyclawUpload] platform refused to sign at {}: {status}",
            redact_url(endpoint)
        );
        return Err(format!(
            "the platform refused the upload (HTTP {status}): {}",
            text.chars().take(200).collect::<String>()
        ));
    }
    let env: ApiEnvelope = serde_json::from_str(&text)
        .map_err(|e| format!("could not parse the platform's response: {e}"))?;
    if env.code != 0 {
        return Err(env
            .msg
            .unwrap_or_else(|| "the platform rejected the upload".into()));
    }
    env.data
        .ok_or_else(|| "the platform returned no upload target".to_string())
}

/// ② 直传。超时 / 5xx 重试, 4xx(预签名过期、长度不符)直接失败。
async fn put(upload_url: &str, mime: &str, body: Vec<u8>) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(PUT_TIMEOUT_SECS))
        .build()
        .map_err(redacted_err)?;

    let mut last = String::new();
    for attempt in 1..=PUT_ATTEMPTS {
        let res = client
            .put(upload_url)
            .header(reqwest::header::CONTENT_TYPE, mime)
            .body(body.clone())
            .send()
            .await;
        match res {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) if r.status().is_client_error() => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                return Err(format!(
                    "object storage refused the upload (HTTP {status}): {}",
                    text.chars().take(200).collect::<String>()
                ));
            }
            Ok(r) => last = format!("HTTP {}", r.status()),
            Err(e) => last = redacted_err(e),
        }
        if attempt < PUT_ATTEMPTS {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    Err(format!(
        "upload failed after {PUT_ATTEMPTS} attempts: {last}"
    ))
}

/// 让 delegation listener 通过 trait 调到本模块 —— listener 不直接依赖 commands。
#[async_trait::async_trait]
pub trait ArtifactUploadAccess: Send + Sync {
    /// 传一个本地文件, 成功回公开链接, 失败回一句给模型看的原因。
    async fn upload(&self, path: &str) -> Result<String, String>;
}

/// 生产实现: 认证与端点都从 codeg 自己的 webhook 配置派生。
pub struct DbArtifactUpload {
    db: std::sync::Arc<AppDatabase>,
}

impl DbArtifactUpload {
    pub fn new(db: std::sync::Arc<AppDatabase>) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ArtifactUploadAccess for DbArtifactUpload {
    async fn upload(&self, path: &str) -> Result<String, String> {
        upload_file(&self.db, path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hook(url: &str, enabled: bool) -> WebhookConfig {
        WebhookConfig {
            url: url.to_string(),
            enabled,
        }
    }

    #[test]
    fn endpoint_swaps_the_path_and_keeps_the_query() {
        let hooks = vec![hook("https://myclaw.ai/api/codeg/events?vmId=a&s=b", true)];
        assert_eq!(
            upload_endpoint(&hooks).as_deref(),
            Some("https://myclaw.ai/api/codeg/upload-url?vmId=a&s=b")
        );
    }

    #[test]
    fn disabled_or_missing_webhook_yields_no_endpoint() {
        assert_eq!(
            upload_endpoint(&[hook("https://myclaw.ai/api/codeg/events?vmId=a&s=b", false)]),
            None
        );
        assert_eq!(
            upload_endpoint(&[hook("https://example.com/other", true)]),
            None
        );
    }

    #[test]
    fn mime_is_guessed_from_the_extension_and_falls_back() {
        assert_eq!(
            guess_mime(Path::new("/tmp/deck.PPTX")),
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        );
        assert_eq!(guess_mime(Path::new("/tmp/a.png")), "image/png");
        assert_eq!(
            guess_mime(Path::new("/tmp/weird.xyz")),
            "application/octet-stream"
        );
        assert_eq!(
            guess_mime(Path::new("/tmp/noext")),
            "application/octet-stream"
        );
    }

    #[test]
    fn file_name_drops_the_directories() {
        assert_eq!(file_name_of(Path::new("/a/b/deck.pptx")), "deck.pptx");
        assert_eq!(file_name_of(Path::new("deck.pptx")), "deck.pptx");
    }
}
