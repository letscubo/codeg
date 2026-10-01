//! fork(letscubo)专属: 伴生的「平台工具」—— 把 `tools/list`、`tools/call` 转给 MyClaw 平台。
//!
//! ## 做什么
//!
//! 模型在对话里查询 / 修改**本实例**的智能体、技能、MCP、应用、定时任务、runtime。工具的
//! 名字、参数、实现全在平台(`web/src/lib/codeg/platform-tools/`),codeg 只负责:
//!
//! 1. 从本机 webhook 配置派生平台地址与密钥(与 `myclaw_upload` 同源);
//! 2. 带签名打 `GET /api/codeg/tools`(清单,带缓存)与 `POST /api/codeg/tools/call`;
//! 3. 把调用方线索(会话 / agent)一并带上,平台据此定「自己」是谁。
//!
//! 以后加工具只发平台,不用发 codeg。
//!
//! ## 签名
//!
//! 不再把密钥放在查询串里(`?vmId&s=` 会被反代日志原样记下)。用同一把密钥做 HMAC:
//!
//! ```text
//!   X-Codeg-Vm:  <vmId>
//!   X-Codeg-Ts:  <unix 秒>
//!   X-Codeg-Sig: base64url( HMAC-SHA256(secret, "<ts>\n<METHOD>\n<path>\n<sha256hex(body)>") )
//! ```
//!
//! 平台侧 `signed-request.ts` 必须逐字一致;两边测试钉着同一组向量。
//!
//! ## 信任边界
//!
//! 平台只信「是哪台实例」。会话 / agent 线索只用于默认目标与记账 —— 容器里 codeg 与
//! agent 同属一个系统用户,密钥对它们都可读。高风险操作由平台建待确认记录,只有用户
//! 在网页上以自己的登录态批准才会执行。

use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::chat_channel::webhook::WebhookConfig;
use crate::db::AppDatabase;

/// 出站 webhook 的路径 —— 平台地址与凭证都从它派生(与 myclaw_upload 同源)。
const EVENTS_PATH: &str = "/api/codeg/events";
pub const CATALOG_PATH: &str = "/api/codeg/tools";
pub const CALL_PATH: &str = "/api/codeg/tools/call";

/// 清单缓存:成功 5 分钟,失败 30 秒(不是平台托管 / 平台暂时不可达时别每次都打)。
const CATALOG_TTL_OK: Duration = Duration::from_secs(300);
const CATALOG_TTL_ERR: Duration = Duration::from_secs(30);
const CATALOG_TIMEOUT: Duration = Duration::from_secs(8);
/// 调用超时:平台端 maxDuration 60s,再留余量。
const CALL_TIMEOUT: Duration = Duration::from_secs(70);

/// 从 webhook 配置派生的平台端点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformEndpoint {
    /// `scheme://host[:port]`
    pub base: String,
    pub vm_id: String,
    pub secret: String,
}

/// `None` = 这台实例没有指向平台的 webhook,即不是平台托管的。
pub fn endpoint_from_hooks(hooks: &[WebhookConfig]) -> Option<PlatformEndpoint> {
    let hook = hooks
        .iter()
        .filter(|w| w.enabled)
        .find(|w| w.url.contains(EVENTS_PATH))?;
    let url = reqwest::Url::parse(&hook.url).ok()?;
    let mut vm_id = None;
    let mut secret = None;
    for (k, v) in url.query_pairs() {
        match k.as_ref() {
            "vmId" => vm_id = Some(v.into_owned()),
            "s" => secret = Some(v.into_owned()),
            _ => {}
        }
    }
    let (vm_id, secret) = (
        vm_id.filter(|s| !s.is_empty())?,
        secret.filter(|s| !s.is_empty())?,
    );
    let mut base = format!("{}://{}", url.scheme(), url.host_str()?);
    if let Some(port) = url.port() {
        base.push_str(&format!(":{port}"));
    }
    Some(PlatformEndpoint {
        base,
        vm_id,
        secret,
    })
}

fn sha256_hex(body: &str) -> String {
    Sha256::digest(body.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// 被签的规范串。平台 `codegSignaturePayload` 必须逐字一致。
pub fn signature_payload(ts: u64, method: &str, path: &str, body: &str) -> String {
    format!(
        "{ts}\n{}\n{path}\n{}",
        method.to_ascii_uppercase(),
        sha256_hex(body)
    )
}

pub fn sign(secret: &str, ts: u64, method: &str, path: &str, body: &str) -> String {
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(signature_payload(ts, method, path, body).as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 平台的 apiOk 外壳: `{ code, data, msg }`。
#[derive(Debug, Deserialize)]
struct ApiEnvelope {
    code: i64,
    data: Option<Value>,
    msg: Option<String>,
}

/// 描述一次 reqwest 失败,**不带 URL**(与 myclaw_upload 同一条:别把地址带进日志)。
fn redacted_err(e: reqwest::Error) -> String {
    e.without_url().to_string()
}

async fn signed_request(
    ep: &PlatformEndpoint,
    method: reqwest::Method,
    path: &str,
    body: Option<String>,
    timeout: Duration,
) -> Result<Value, String> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(timeout)
        .build()
        .map_err(redacted_err)?;
    let body_str = body.unwrap_or_default();
    let ts = now_secs();
    let sig = sign(&ep.secret, ts, method.as_str(), path, &body_str);
    let mut req = client
        .request(method.clone(), format!("{}{path}", ep.base))
        .header("x-codeg-vm", &ep.vm_id)
        .header("x-codeg-ts", ts.to_string())
        .header("x-codeg-sig", sig);
    if method != reqwest::Method::GET {
        req = req
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body_str);
    }
    let res = req.send().await.map_err(redacted_err)?;
    let status = res.status();
    let text = res.text().await.map_err(redacted_err)?;
    let env: ApiEnvelope = serde_json::from_str(&text).map_err(|_| {
        format!(
            "the platform returned HTTP {status}: {}",
            text.chars().take(200).collect::<String>()
        )
    })?;
    if env.code != 0 {
        return Err(env
            .msg
            .unwrap_or_else(|| format!("the platform refused the request (HTTP {status})")));
    }
    Ok(env.data.unwrap_or(Value::Null))
}

async fn endpoint(db: &AppDatabase) -> Result<PlatformEndpoint, String> {
    let hooks = crate::commands::chat_channel::get_chat_event_webhooks_core(db)
        .await
        .map_err(|_| "could not read this instance's platform webhook config".to_string())?;
    endpoint_from_hooks(&hooks)
        .ok_or_else(|| "this instance is not platform-managed (no webhook configured)".to_string())
}

/// 缓存的是清单接口的整个 data(`{ tools, instructions }`):工具与总览同一次请求、同一个 TTL。
type CatalogCache = Mutex<Option<(Instant, Result<Value, String>)>>;

fn catalog_cache() -> &'static CatalogCache {
    static CACHE: OnceLock<CatalogCache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// 清单接口的 data。有缓存;失败也缓存一小会儿。
async fn catalog_data(db: &AppDatabase) -> Result<Value, String> {
    let mut guard = catalog_cache().lock().await;
    if let Some((at, cached)) = guard.as_ref() {
        let ttl = if cached.is_ok() {
            CATALOG_TTL_OK
        } else {
            CATALOG_TTL_ERR
        };
        if at.elapsed() < ttl {
            return cached.clone();
        }
    }
    let fetched = async {
        let ep = endpoint(db).await?;
        signed_request(
            &ep,
            reqwest::Method::GET,
            CATALOG_PATH,
            None,
            CATALOG_TIMEOUT,
        )
        .await
    }
    .await;
    if let Err(e) = &fetched {
        tracing::warn!("[MyclawPlatform] tool catalog unavailable: {e}");
    }
    *guard = Some((Instant::now(), fetched.clone()));
    fetched
}

/// 平台工具清单(MCP Tool 形状的数组)。
pub async fn catalog(db: &AppDatabase) -> Result<Vec<Value>, String> {
    Ok(tools_of(&catalog_data(db).await?))
}

/// 平台工具总览(一段给模型看的文字,拼进伴生的 MCP `instructions`)。平台没给就是 None。
pub async fn overview(db: &AppDatabase) -> Result<Option<String>, String> {
    Ok(overview_of(&catalog_data(db).await?))
}

fn tools_of(data: &Value) -> Vec<Value> {
    data.get("tools")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default()
}

fn overview_of(data: &Value) -> Option<String> {
    data.get("instructions")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// 调一个平台工具。成功回 MCP `CallToolResult`(平台已按 MCP 形状组好,原样转给模型)。
pub async fn call(
    db: &AppDatabase,
    name: &str,
    arguments: Value,
    identity: Value,
) -> Result<Value, String> {
    let ep = endpoint(db).await?;
    let body = serde_json::json!({ "name": name, "arguments": arguments, "identity": identity });
    let data = signed_request(
        &ep,
        reqwest::Method::POST,
        CALL_PATH,
        Some(body.to_string()),
        CALL_TIMEOUT,
    )
    .await?;
    data.get("result")
        .cloned()
        .filter(|r| r.get("content").is_some())
        .ok_or_else(|| "the platform returned no tool result".to_string())
}

/// 失败时给模型的结果:MCP 约定用 isError 结果,不是协议错误。
pub fn error_result(message: &str) -> Value {
    serde_json::json!({
        "content": [{ "type": "text", "text": message }],
        "isError": true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_data_splits_into_tools_and_overview() {
        let data = serde_json::json!({ "tools": [{ "name": "agents" }], "instructions": "  - agents: …  " });
        assert_eq!(tools_of(&data).len(), 1);
        assert_eq!(overview_of(&data).as_deref(), Some("- agents: …"));
        // 老平台没有 instructions / 空串 → None,伴生照旧只给自己的说明
        assert_eq!(overview_of(&serde_json::json!({ "tools": [] })), None);
        assert_eq!(
            overview_of(&serde_json::json!({ "instructions": "  " })),
            None
        );
        assert!(tools_of(&serde_json::json!({})).is_empty());
    }

    /// 与平台 `platform-tools.behavior.test.ts` 同一组向量 —— 两边任何一边改了拼法都会先红。
    #[test]
    fn signature_vector_matches_the_platform() {
        let body = r#"{"name":"list_agents","arguments":{}}"#;
        assert_eq!(
            signature_payload(1790000000, "post", CALL_PATH, body),
            "1790000000\nPOST\n/api/codeg/tools/call\n9d95a6417a2a43bc34ca527c36d494cf7c037069ae26961d3353ee538ccf98ba"
        );
        assert_eq!(
            sign("test-webhook-secret", 1790000000, "POST", CALL_PATH, body),
            "4VAEyLqioWN0vN4fKefVNEKcV2lupsDCqOssoH5ebnI"
        );
        assert_eq!(
            sign("test-webhook-secret", 1790000000, "GET", CATALOG_PATH, ""),
            "YtOx5Ry7-R9n1HW6OYxk56K6sYem7zNKpLdBrnIU3Zg"
        );
    }

    fn hook(url: &str, enabled: bool) -> WebhookConfig {
        WebhookConfig {
            url: url.to_string(),
            enabled,
        }
    }

    #[test]
    fn endpoint_is_derived_from_the_events_webhook() {
        let ep = endpoint_from_hooks(&[
            hook("https://example.com/other", true),
            hook(
                "https://myclaw.ai/api/codeg/events?vmId=abc-123&s=sekret",
                true,
            ),
        ])
        .unwrap();
        assert_eq!(
            ep,
            PlatformEndpoint {
                base: "https://myclaw.ai".into(),
                vm_id: "abc-123".into(),
                secret: "sekret".into(),
            }
        );
        let local = endpoint_from_hooks(&[hook(
            "http://localhost:3000/api/codeg/events?vmId=v&s=x",
            true,
        )])
        .unwrap();
        assert_eq!(local.base, "http://localhost:3000");
    }

    #[test]
    fn no_endpoint_without_an_enabled_platform_webhook_or_credentials() {
        assert!(endpoint_from_hooks(&[]).is_none());
        assert!(endpoint_from_hooks(&[hook(
            "https://myclaw.ai/api/codeg/events?vmId=v&s=x",
            false
        )])
        .is_none());
        assert!(
            endpoint_from_hooks(&[hook("https://myclaw.ai/api/codeg/events?vmId=v", true)])
                .is_none()
        );
        assert!(
            endpoint_from_hooks(&[hook("https://myclaw.ai/api/codeg/events?s=x", true)]).is_none()
        );
    }

    #[test]
    fn error_result_is_an_mcp_is_error_result() {
        let r = error_result("nope");
        assert_eq!(r["isError"], true);
        assert_eq!(r["content"][0]["text"], "nope");
    }
}
