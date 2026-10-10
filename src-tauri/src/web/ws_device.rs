//! MyClaw fork ext (letscubo) —— 「我的电脑」:让 agent 用用户自己电脑上的能力(原型)。
//!
//! 用户在自己电脑上跑 MyClaw 连接器,连接器**主动外连**到这里的 `/ws/device`(不开入站
//! 端口,公司内网 / 家里路由器后面都能用)。agent 那边看到的是一个普通的 HTTP MCP 工具
//! (`/myclaw/device-mcp/<agent>`):每个调用原样转给连接器,连接器在本机执行(只在用户授权
//! 的文件夹里,每条命令在本机弹窗确认),再把结果原样交回来。
//!
//! ```text
//! agent ──HTTP MCP(agent 派生凭证)──> codeg ──WS /ws/device──> 用户电脑上的连接器
//!                                     <── {t:"rpc_result"} ────  (本机执行 + 弹窗确认)
//! ```
//!
//! 这里**只转发,不理解工具**:工具清单与实现都在连接器里。以后换成操作软件界面的驱动,
//! 这条管道不用改。
//!
//! 凭证三把,互不相通(都由主 token 经 HMAC 派生,确定性、不落库):
//!   · 平台取条目 / 取设备凭证:主 token(`require_token` 组)
//!   · agent 调工具:`cdmcp_` + HMAC(主 token, `codeg-device-mcp/v1:<agent>`),只认这个 agent 的这个端点
//!   · 连接器连 `/ws/device`:`cdvc_` + HMAC(主 token, `codeg-device/v1:<设备名>`),只能连设备端点,
//!     **主 token 绝不下发到用户电脑**
//!
//! 原型限制:每个实例一台设备(名字 `default`);电脑不在线时工具清单用最近一次连接器报的,
//! 调用一律如实回「未连接」。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, Path, Query};
use axum::http::header::HeaderName;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use tokio::sync::{mpsc, oneshot};

use super::handlers::myclaw::download::ServerToken;

/// 连接器选的子协议(凭证走 `codeg-token.<base64url>`,与 /ws/events 同一写法)
pub const DEVICE_PROTOCOL: &str = "myclaw-device";
/// 原型:每个实例一台设备
pub const DEFAULT_DEVICE: &str = "default";
const DEVICE_TOKEN_PREFIX: &str = "cdvc_";
const DEVICE_TOKEN_DOMAIN: &str = "codeg-device/v1:";
const AGENT_TOKEN_PREFIX: &str = "cdmcp_";
const AGENT_TOKEN_DOMAIN: &str = "codeg-device-mcp/v1:";
/// 一次调用最多等多久 —— 用户要在电脑上看弹窗、点允许,给足时间
const CALL_TIMEOUT: Duration = Duration::from_secs(300);
/// 设备没连时,判「离线」前先等它(重)连上多久。
///
/// 覆盖「实例被闲忙逻辑暂停 → 一醒过来 codeg 重启、连接器还在重连」那几秒:此时 agent 会话
/// 刚起、立刻 tools/list,设备却还没连回来。等一下再判离线,连接器重连(封顶 5s)后这次调用
/// 就能拿到真工具,而不是让整个会话以为「没有这个工具」。连接器根本没在跑时,顶多多等这点时间。
const RECONNECT_GRACE: Duration = Duration::from_secs(8);
const KEEPALIVE: Duration = Duration::from_secs(25);
const PROTOCOL_VERSION: &str = "2024-11-05";
const OFFLINE_MESSAGE: &str = "The user's computer is not connected right now. Ask the user to start the MyClaw connector on their computer, then try again. Do not retry in a loop.";

struct Device {
    conn: u64,
    tx: mpsc::UnboundedSender<Message>,
}

static DEVICE: LazyLock<Mutex<Option<Device>>> = LazyLock::new(|| Mutex::new(None));
/// 等连接器回包的调用:codeg 自己的调用号 → 结果送回去的口子
static PENDING: LazyLock<Mutex<HashMap<u64, oneshot::Sender<Value>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
/// 连接器最近一次报的工具清单(`tools/list` 的 result)—— 离线时也照它答,agent 不至于看不到工具
static TOOLS: LazyLock<Mutex<Option<Value>>> = LazyLock::new(|| Mutex::new(None));
static NEXT: AtomicU64 = AtomicU64::new(1);

fn mac(secret: &str, domain: &str, subject: &str) -> Option<Hmac<Sha256>> {
    let mut m = <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).ok()?;
    m.update(domain.as_bytes());
    m.update(subject.as_bytes());
    Some(m)
}

fn derive(secret: &str, prefix: &str, domain: &str, subject: &str) -> Option<String> {
    if secret.is_empty() {
        return None;
    }
    let m = mac(secret, domain, subject)?;
    Some(format!("{prefix}{}", URL_SAFE_NO_PAD.encode(m.finalize().into_bytes())))
}

/// 恒定时间核对派生凭证。主 token 为空一律拒(与 auth.rs 同一条 fail-closed)。
fn verify(secret: &str, prefix: &str, domain: &str, subject: &str, presented: &str) -> bool {
    if secret.is_empty() {
        return false;
    }
    let Some(sig) = presented.strip_prefix(prefix) else {
        return false;
    };
    let (Ok(sig), Some(m)) = (URL_SAFE_NO_PAD.decode(sig), mac(secret, domain, subject)) else {
        return false;
    };
    m.verify_slice(&sig).is_ok()
}

pub(crate) fn device_token(secret: &str, device: &str) -> Option<String> {
    derive(secret, DEVICE_TOKEN_PREFIX, DEVICE_TOKEN_DOMAIN, device)
}

pub(crate) fn agent_device_token(secret: &str, agent: &str) -> Option<String> {
    derive(secret, AGENT_TOKEN_PREFIX, AGENT_TOKEN_DOMAIN, agent)
}

/// agent id 拼进 URL 和条目名:只收平台真会传的那种(小写字母数字和 `-`/`_`)
pub(crate) fn valid_agent(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

fn codeg_port() -> u16 {
    std::env::var("CODEG_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3080)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

// ---- 平台调(require_token 组) ----

/// 配对用:签出连接器的设备凭证。
pub async fn device_token_handler(Extension(ServerToken(secret)): Extension<ServerToken>) -> Response {
    match device_token(&secret, DEFAULT_DEVICE) {
        Some(token) => Json(json!({ "device": DEFAULT_DEVICE, "token": token, "online": DEVICE.lock().map(|d| d.is_some()).unwrap_or(false) })).into_response(),
        None => (StatusCode::INTERNAL_SERVER_ERROR, "server token unavailable").into_response(),
    }
}

/// 投影用:这个 agent 的「我的电脑」工具条目(名字、地址、凭证都由这里定,平台原样搬)。
pub async fn device_entry(
    Path(agent): Path<String>,
    Extension(ServerToken(secret)): Extension<ServerToken>,
) -> Response {
    if !valid_agent(&agent) {
        return (StatusCode::BAD_REQUEST, "invalid agent segment").into_response();
    }
    let Some(token) = agent_device_token(&secret, &agent) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "server token unavailable").into_response();
    };
    Json(json!({
        "name": "my-computer",
        "transport": "streamable-http",
        // 同容器内直连,不出网
        "url": format!("http://127.0.0.1:{}/api/myclaw/device-mcp/{agent}", codeg_port()),
        "headers": { "Authorization": format!("Bearer {token}") },
    }))
    .into_response()
}

// ---- agent 调(公共组,自己认凭证) ----

#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    #[serde(default)]
    id: Option<Value>,
    method: String,
}

/// 电脑不在线时由这里直接答:握手照常、工具清单用最近一次的、调用如实说「未连接」。
pub(crate) fn offline_reply(id: Value, method: &str, cached_tools: Option<Value>) -> Value {
    let result = match method {
        "initialize" => json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "my-computer", "version": "offline" },
            "instructions": OFFLINE_MESSAGE,
        }),
        "tools/list" => cached_tools.unwrap_or_else(|| json!({ "tools": [] })),
        "tools/call" => json!({
            "content": [{ "type": "text", "text": OFFLINE_MESSAGE }],
            "isError": true,
        }),
        "ping" => json!({}),
        other => {
            return json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("method not found: {other}") } })
        }
    };
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// MCP streamable-http 响应:带上 `Mcp-Session-Id` 头。
///
/// 真实的 MCP 客户端(Claude Agent SDK / openclaw)在 `initialize` 后要拿到一个会话 id 才认为
/// 连接建立好了;**拿不到就一直等到超时**(实测 claude_code 报 `CONNECT_TIMEOUT: dialing …`,
/// 而宽松的 curl 不要求它、所以一直"能用"假象)。我们这端是无状态转发,不校验回传的 session,
/// 给一个按 agent 固定的值即可(与 /api/apps/gateway 的做法一致:initialize 必带 Mcp-Session-Id)。
fn reply(agent: &str, body: Value) -> Response {
    let mut resp = Json(body).into_response();
    if let Ok(value) = format!("mycomp-{agent}").parse() {
        resp.headers_mut()
            .insert(HeaderName::from_static("mcp-session-id"), value);
    }
    resp
}

/// 取设备的发送口;没连就最多等 `grace`(每 250ms 看一次)等它(重)连上。
/// 返回 None = 等满了还没连上。std Mutex 只在每次查看时短暂持有,不跨 await。
async fn wait_for_device(grace: Duration) -> Option<mpsc::UnboundedSender<Message>> {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        if let Some(tx) = DEVICE.lock().ok().and_then(|d| d.as_ref().map(|d| d.tx.clone())) {
            return Some(tx);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn device_mcp(
    Path(agent): Path<String>,
    Extension(ServerToken(secret)): Extension<ServerToken>,
    headers: HeaderMap,
    Json(raw): Json<Value>,
) -> Response {
    if !valid_agent(&agent) {
        return (StatusCode::BAD_REQUEST, "invalid agent segment").into_response();
    }
    let ok = bearer(&headers)
        .is_some_and(|t| verify(&secret, AGENT_TOKEN_PREFIX, AGENT_TOKEN_DOMAIN, &agent, t));
    if !ok {
        return (StatusCode::UNAUTHORIZED, "Invalid or missing token").into_response();
    }
    let Ok(req) = serde_json::from_value::<RpcRequest>(raw.clone()) else {
        return (StatusCode::BAD_REQUEST, "not a JSON-RPC request").into_response();
    };
    // 通知不需要响应体
    let Some(id) = req.id.clone() else {
        return StatusCode::ACCEPTED.into_response();
    };

    // 握手(initialize / ping)由本端直接答,**不转发给本机 cua-driver**:
    //   · 必带 Mcp-Session-Id(见 reply),否则 Claude Agent SDK 握手超时;
    //   · 结果形状可控(protocolVersion / serverInfo 齐全),不依赖 cua-driver 的 initialize 格式;
    //   · 本机在不在线都能握手成功 —— 具体工具能不能用,交给 tools/list / tools/call 如实体现。
    match req.method.as_str() {
        "initialize" => {
            let proto = raw
                .get("params")
                .and_then(|p| p.get("protocolVersion"))
                .and_then(Value::as_str)
                .unwrap_or(PROTOCOL_VERSION);
            return reply(
                &agent,
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": proto,
                        "capabilities": { "tools": {} },
                        "serverInfo": { "name": "my-computer", "version": env!("CARGO_PKG_VERSION") },
                    }
                }),
            );
        }
        "ping" => return reply(&agent, json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
        _ => {}
    }

    // 到这里的都是真要本机的调用(tools/list、tools/call …;握手已在上面本地答掉)。
    // 设备没连时先等它(重)连上一会儿再判离线,平掉「实例刚唤醒、连接器还在重连」的竞态。
    let tx = wait_for_device(RECONNECT_GRACE).await;
    let Some(tx) = tx else {
        let cached = TOOLS.lock().ok().and_then(|t| t.clone());
        return reply(&agent, offline_reply(id, &req.method, cached));
    };

    let call = NEXT.fetch_add(1, Ordering::Relaxed);
    let (done_tx, done_rx) = oneshot::channel();
    if let Ok(mut p) = PENDING.lock() {
        p.insert(call, done_tx);
    }
    let frame = json!({ "t": "rpc", "id": call, "agent": agent, "req": raw });
    if tx.send(Message::Text(frame.to_string().into())).is_err() {
        if let Ok(mut p) = PENDING.lock() {
            p.remove(&call);
        }
        let cached = TOOLS.lock().ok().and_then(|t| t.clone());
        return reply(&agent, offline_reply(id, &req.method, cached));
    }
    match tokio::time::timeout(CALL_TIMEOUT, done_rx).await {
        Ok(Ok(res)) => reply(&agent, res),
        _ => {
            if let Ok(mut p) = PENDING.lock() {
                p.remove(&call);
            }
            reply(
                &agent,
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32000, "message": "The user's computer did not answer in time (it may be waiting for the user to approve, or it disconnected)." },
                }),
            )
        }
    }
}

// ---- 连接器连(公共组,自己认凭证) ----

#[derive(Deserialize)]
pub struct DeviceQuery {
    #[serde(default)]
    name: Option<String>,
}

pub async fn ws_device_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<DeviceQuery>,
    Extension(ServerToken(secret)): Extension<ServerToken>,
    headers: HeaderMap,
) -> Response {
    let device = q.name.unwrap_or_else(|| DEFAULT_DEVICE.to_string());
    if device != DEFAULT_DEVICE {
        return (StatusCode::BAD_REQUEST, "unknown device").into_response();
    }
    let presented = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .and_then(super::auth::token_from_ws_protocols)
        .or_else(|| bearer(&headers).map(str::to_string));
    let ok = presented.is_some_and(|t| {
        verify(&secret, DEVICE_TOKEN_PREFIX, DEVICE_TOKEN_DOMAIN, &device, &t)
    });
    if !ok {
        return (StatusCode::UNAUTHORIZED, "Invalid or missing device token").into_response();
    }
    ws.protocols([DEVICE_PROTOCOL]).on_upgrade(run_device).into_response()
}

async fn run_device(socket: WebSocket) {
    let conn = NEXT.fetch_add(1, Ordering::Relaxed);
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    // 新连接顶掉旧的(同一台电脑重连):旧的发送口被丢掉,它的写任务随之结束
    if let Ok(mut d) = DEVICE.lock() {
        *d = Some(Device { conn, tx: tx.clone() });
    }
    let writer = tokio::spawn(async move {
        let mut keepalive = tokio::time::interval(KEEPALIVE);
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(m) => if sink.send(m).await.is_err() { break },
                    None => break,
                },
                _ = keepalive.tick() => {
                    if sink.send(Message::Ping(Vec::new().into())).await.is_err() { break }
                }
            }
        }
        let _ = sink.close().await;
    });

    while let Some(Ok(msg)) = stream.next().await {
        let Message::Text(text) = msg else {
            if matches!(msg, Message::Close(_)) {
                break;
            }
            continue;
        };
        let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
        match v.get("t").and_then(Value::as_str) {
            Some("hello") => {
                if let Some(tools) = v.get("tools").filter(|t| t.is_array()) {
                    if let Ok(mut t) = TOOLS.lock() {
                        *t = Some(json!({ "tools": tools }));
                    }
                }
            }
            Some("rpc_result") => {
                let (Some(id), Some(res)) = (v.get("id").and_then(Value::as_u64), v.get("res")) else {
                    continue;
                };
                let waiter = PENDING.lock().ok().and_then(|mut p| p.remove(&id));
                if let Some(w) = waiter {
                    let _ = w.send(res.clone());
                }
            }
            _ => {}
        }
    }

    // 只清自己:已经被新连接顶掉的话不动新的
    if let Ok(mut d) = DEVICE.lock() {
        if d.as_ref().is_some_and(|d| d.conn == conn) {
            *d = None;
        }
    }
    drop(tx);
    writer.abort();
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "master-token";

    #[test]
    fn credentials_are_scoped_and_never_the_master() {
        let dev = device_token(SECRET, "default").unwrap();
        let agent = agent_device_token(SECRET, "va-abc").unwrap();
        assert!(dev.starts_with("cdvc_") && agent.starts_with("cdmcp_"));
        assert!(!dev.contains(SECRET) && !agent.contains(SECRET));
        assert!(verify(SECRET, DEVICE_TOKEN_PREFIX, DEVICE_TOKEN_DOMAIN, "default", &dev));
        assert!(verify(SECRET, AGENT_TOKEN_PREFIX, AGENT_TOKEN_DOMAIN, "va-abc", &agent));
        // 换 agent、换用途、换主 token 都不认
        assert!(!verify(SECRET, AGENT_TOKEN_PREFIX, AGENT_TOKEN_DOMAIN, "va-other", &agent));
        assert!(!verify(SECRET, DEVICE_TOKEN_PREFIX, DEVICE_TOKEN_DOMAIN, "default", &agent));
        assert!(!verify("other", DEVICE_TOKEN_PREFIX, DEVICE_TOKEN_DOMAIN, "default", &dev));
        assert!(!verify("", DEVICE_TOKEN_PREFIX, DEVICE_TOKEN_DOMAIN, "default", &dev));
        assert!(!verify(SECRET, DEVICE_TOKEN_PREFIX, DEVICE_TOKEN_DOMAIN, "default", SECRET));
        assert!(device_token("", "default").is_none());
    }

    #[test]
    fn reply_carries_mcp_session_id() {
        // 真实 MCP 客户端(Claude Agent SDK)要在 initialize 响应里拿到 Mcp-Session-Id 才认为连上了。
        let resp = reply("va-abc", json!({ "ok": true }));
        assert_eq!(
            resp.headers().get("mcp-session-id").unwrap().to_str().unwrap(),
            "mycomp-va-abc"
        );
    }

    #[test]
    fn offline_answers_handshake_and_says_not_connected() {
        let init = offline_reply(json!(1), "initialize", None);
        assert_eq!(init["result"]["serverInfo"]["name"], "my-computer");
        let tools = offline_reply(json!(2), "tools/list", Some(json!({"tools": [{"name": "run_command"}]})));
        assert_eq!(tools["result"]["tools"][0]["name"], "run_command", "离线也给最近一次的清单");
        let call = offline_reply(json!(3), "tools/call", None);
        assert_eq!(call["result"]["isError"], true);
        assert!(call["result"]["content"][0]["text"].as_str().unwrap().contains("not connected"));
        let bad = offline_reply(json!(4), "nope", None);
        assert_eq!(bad["error"]["code"], -32601);
    }

    #[test]
    fn agent_segments_cannot_escape_the_url() {
        assert!(valid_agent("va-1a2b-3c"));
        assert!(valid_agent("5917b7d5-0b0f-45ba-9f51-a3cc353c3945"));
        assert!(!valid_agent("../x"));
        assert!(!valid_agent("A"));
        assert!(!valid_agent(""));
    }
}
