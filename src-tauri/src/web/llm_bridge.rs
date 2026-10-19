//! MyClaw fork ext (letscubo) —— 让 claude_code 跑非 Claude 模型:Anthropic Messages ⇄ OpenAI Chat 协议转接。
//!
//! claude_code(Claude Code CLI)只会说 Anthropic Messages(`POST /v1/messages`),平台网关的
//! `/v1/messages` 只收 Claude 模型。于是 claude_code 的 `ANTHROPIC_BASE_URL` 指到这里:
//!
//! ```text
//! claude ──Anthropic Messages──> 127.0.0.1:3081/v1/messages(本模块)
//!            · Claude 模型:原样透传网关 /v1/messages(提示缓存、tool search 等 Anthropic 专有能力照旧)
//!            · 其余模型:请求转成 OpenAI Chat 发网关 /v1/chat/completions,响应(含流式)转回 Anthropic
//! ```
//!
//! 转换本身不自己写,用 NVIDIA Switchyard 的 `switchyard-translation`(纯 Rust、无 HTTP/SDK 依赖、
//! ~1 万行测试)。2026-10-19 在 C9 实测:真 Claude Code CLI 经它跑 kimi-k3 / deepseek 多步工具任务、
//! 会话中途在 Claude 与非 Claude 之间来回切换都正常。这里只做三件事:路由、转发、补网关用量字段。
//!
//! **只监听 127.0.0.1**(默认 3081,`CODEG_LLM_BRIDGE_PORT` 可改,0 = 不开)。不挂在 3080 主服务上:
//! 3080 经 `cg-*.moltly.ai` 对公网开放,而网关的平台凭证靠「来源 IP 白名单」挡外部调用 ——
//! 这里若对外可达,就成了绕过白名单的转发口。凭证本身不在这里存:claude 带来的 `Authorization`
//! 原样转给网关,由网关认。

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use switchyard_translation::{
    StreamTranslationState, TranslationEngine, TranslationPolicy, WireFormat,
};

pub const DEFAULT_PORT: u16 = 3081;
pub const DEFAULT_UPSTREAM: &str = "https://api.myclaw.ai/v1";
const ENV_PORT: &str = "CODEG_LLM_BRIDGE_PORT";
const ENV_UPSTREAM: &str = "CODEG_LLM_UPSTREAM";
/// claude 一次请求体的上限(长会话 + 图片),超了直接拒,别把内存吃光
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct Bridge {
    upstream: String,
    client: reqwest::Client,
}

/// codeg-server 启动时调用:在 127.0.0.1 上起转接服务。失败只记日志,不影响 codeg 本身。
pub fn spawn() {
    let port = match std::env::var(ENV_PORT) {
        Ok(v) => match v.trim().parse::<u16>() {
            Ok(0) => {
                tracing::info!("[llm-bridge] {ENV_PORT}=0,不开转接");
                return;
            }
            Ok(p) => p,
            Err(_) => {
                tracing::warn!("[llm-bridge] {ENV_PORT}={v} 不是端口号,用默认 {DEFAULT_PORT}");
                DEFAULT_PORT
            }
        },
        Err(_) => DEFAULT_PORT,
    };
    let upstream = std::env::var(ENV_UPSTREAM)
        .ok()
        .map(|u| u.trim().trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| DEFAULT_UPSTREAM.to_string());
    tokio::spawn(async move {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("[llm-bridge] 绑定 {addr} 失败,不开转接:{e}");
                return;
            }
        };
        tracing::info!("[llm-bridge] listening on {addr} → {upstream}");
        if let Err(e) = axum::serve(listener, router(upstream)).await {
            tracing::warn!("[llm-bridge] 退出:{e}");
        }
    });
}

fn router(upstream: String) -> Router {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_default();
    Router::new()
        .fallback(any(handle))
        .with_state(Bridge { upstream, client })
}

/// 与平台 `isAnthropicModel` 同一判据:名字以 claude 开头的走 Anthropic 原生。
pub(crate) fn is_claude_model(model: &str) -> bool {
    model.trim().to_lowercase().starts_with("claude")
}

/// 网关的 OpenAI 用量是非标的:命中数单独放 `cached_prompt_tokens`,而 `prompt_tokens` 含不含命中
/// **随上游而变**(2026-10-19 C9 实测):
///   · deepseek:不含 —— 259 + 75 + 15360 = total_tokens 15694;
///   · kimi:含 —— 14253 + 133 = total_tokens 14386,命中 256 已在 14253 里。
/// 标准 OpenAI 是 prompt_tokens 含命中、命中放 `prompt_tokens_details.cached_tokens`,转换库按标准读。
/// 不改的话 deepseek 转出的 `input_tokens` 只剩几百、缓存的上万 token 丢掉 → Claude Code 严重低估上下文,
/// 自动压缩不触发;一律相加的话 kimi 又会重复计算。所以用 total_tokens 判断属于哪种;没有 total 时,
/// 命中比 prompt 还大只能是「不含」,否则按「含」算(宁可少算也不重复算)。
/// 已经是标准形状(带 prompt_tokens_details.cached_tokens)的不动。
pub(crate) fn normalize_gateway_usage(value: &mut Value) {
    let Some(usage) = value.get_mut("usage").and_then(Value::as_object_mut) else {
        return;
    };
    let Some(cached) = usage.get("cached_prompt_tokens").and_then(Value::as_u64) else {
        return;
    };
    let has_standard = usage
        .get("prompt_tokens_details")
        .and_then(|d| d.get("cached_tokens"))
        .is_some();
    if has_standard || cached == 0 {
        return;
    }
    let prompt = usage.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0);
    let completion = usage.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0);
    let excludes_cached = match usage.get("total_tokens").and_then(Value::as_u64) {
        Some(total) => total == prompt + completion + cached,
        None => cached > prompt,
    };
    if excludes_cached {
        usage.insert("prompt_tokens".into(), json!(prompt + cached));
    }
    let details = usage
        .entry("prompt_tokens_details")
        .or_insert_with(|| json!({}));
    if let Some(d) = details.as_object_mut() {
        d.insert("cached_tokens".into(), json!(cached));
    }
}

fn anthropic_error(status: StatusCode, message: impl Into<String>) -> Response {
    let kind = match status.as_u16() {
        400 | 422 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        529 => "overloaded_error",
        _ => "api_error",
    };
    let body = json!({"type": "error", "error": {"type": kind, "message": message.into()}});
    (status, [("content-type", "application/json")], body.to_string()).into_response()
}

/// claude 带来的凭证:`Authorization: Bearer …`(ANTHROPIC_AUTH_TOKEN)或 `x-api-key`(ANTHROPIC_API_KEY)。
fn bearer_of(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        return Some(v.to_string());
    }
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|k| format!("Bearer {k}"))
}

async fn handle(State(bridge): State<Bridge>, uri: Uri, headers: HeaderMap, body: Body) -> Response {
    // 只接 Messages 本身;Claude Code 启动时会探 /api/hello 之类,回 404 它就不再问(实测无影响)
    if uri.path() != "/v1/messages" {
        return anthropic_error(StatusCode::NOT_FOUND, format!("{} is not served by the codeg LLM bridge", uri.path()));
    }
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return anthropic_error(StatusCode::PAYLOAD_TOO_LARGE, "request body too large"),
    };
    let Some(auth) = bearer_of(&headers) else {
        return anthropic_error(StatusCode::UNAUTHORIZED, "missing credentials");
    };
    let request: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return anthropic_error(StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")),
    };
    let model = request.get("model").and_then(Value::as_str).unwrap_or("").to_string();
    if is_claude_model(&model) {
        return passthrough(&bridge, &headers, &auth, bytes).await;
    }
    translate(&bridge, &auth, &model, &request).await
}

/// Claude 模型:请求体一个字节不改发给网关 /v1/messages,响应(含流)原样回。
async fn passthrough(bridge: &Bridge, headers: &HeaderMap, auth: &str, bytes: Bytes) -> Response {
    let mut req = bridge
        .client
        .post(format!("{}/messages", bridge.upstream))
        .header("authorization", auth)
        .header("content-type", "application/json");
    for name in ["anthropic-version", "anthropic-beta"] {
        if let Some(v) = headers.get(name) {
            req = req.header(name, v.as_bytes());
        }
    }
    match req.body(bytes).send().await {
        Ok(up) => relay_raw(up),
        Err(e) => anthropic_error(StatusCode::BAD_GATEWAY, format!("upstream unreachable: {e}")),
    }
}

fn relay_raw(up: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(up.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    for name in ["content-type", "request-id", "x-request-id"] {
        if let Some(v) = up.headers().get(name) {
            builder = builder.header(name, v.as_bytes());
        }
    }
    builder
        .body(Body::from_stream(up.bytes_stream()))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

/// 非 Claude 模型:Anthropic → OpenAI Chat → 网关 → 转回 Anthropic。
async fn translate(bridge: &Bridge, auth: &str, model: &str, request: &Value) -> Response {
    let engine = TranslationEngine::default();
    let policy = TranslationPolicy::default();
    let stream = request.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let mut chat = match engine.translate_request(
        WireFormat::AnthropicMessages,
        WireFormat::OpenAiChat,
        request,
        &policy,
    ) {
        Ok(out) => {
            if !out.diagnostics.is_empty() {
                tracing::debug!("[llm-bridge] {model} 请求转换诊断 {} 条", out.diagnostics.len());
            }
            out.body
        }
        Err(e) => return anthropic_error(StatusCode::BAD_REQUEST, format!("cannot translate request: {e}")),
    };
    if stream {
        // 要用量(上下文统计、自动压缩都靠它);网关流式默认不一定带
        chat["stream_options"] = json!({"include_usage": true});
    }
    let up = match bridge
        .client
        .post(format!("{}/chat/completions", bridge.upstream))
        .header("authorization", auth)
        .json(&chat)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return anthropic_error(StatusCode::BAD_GATEWAY, format!("upstream unreachable: {e}")),
    };
    let status = up.status();
    if !status.is_success() {
        let text = up.text().await.unwrap_or_default();
        let message = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|v| {
                v.pointer("/error/message")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or(text);
        tracing::info!("[llm-bridge] {model} upstream {status}");
        let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        return anthropic_error(status, message);
    }
    if stream {
        return stream_back(up, engine);
    }
    let mut body: Value = match up.json().await {
        Ok(v) => v,
        Err(e) => return anthropic_error(StatusCode::BAD_GATEWAY, format!("invalid upstream response: {e}")),
    };
    normalize_gateway_usage(&mut body);
    match engine.translate_response(WireFormat::OpenAiChat, WireFormat::AnthropicMessages, &body, &policy) {
        Ok(out) => (StatusCode::OK, [("content-type", "application/json")], out.body.to_string()).into_response(),
        Err(e) => anthropic_error(StatusCode::BAD_GATEWAY, format!("cannot translate response: {e}")),
    }
}

/// 把网关的 OpenAI SSE 逐行解析、逐事件转成 Anthropic SSE(`event: <type>\ndata: <json>\n\n`)。
fn stream_back(up: reqwest::Response, engine: TranslationEngine) -> Response {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    tokio::spawn(async move {
        let mut state = StreamTranslationState::new(WireFormat::OpenAiChat, WireFormat::AnthropicMessages);
        let mut upstream = up.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        'read: while let Some(chunk) = upstream.next().await {
            let Ok(chunk) = chunk else { break };
            buf.extend_from_slice(&chunk);
            while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=pos).collect();
                let Some(events) = translate_sse_line(&engine, &mut state, &line) else {
                    continue;
                };
                if !events.is_empty() && tx.send(Ok(encode_events(events))).await.is_err() {
                    break 'read; // claude 断开了
                }
            }
        }
        if let Ok(events) = engine.finish_stream(&mut state, WireFormat::AnthropicMessages) {
            if !events.is_empty() {
                let _ = tx.send(Ok(encode_events(events))).await;
            }
        }
    });
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(rx))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

/// 一行 OpenAI SSE → 若干 Anthropic 事件。非 `data:` 行、`[DONE]`、解析不了的行 → None(忽略)。
pub(crate) fn translate_sse_line(
    engine: &TranslationEngine,
    state: &mut StreamTranslationState,
    line: &[u8],
) -> Option<Vec<Value>> {
    let line = std::str::from_utf8(line).ok()?.trim_end_matches(['\r', '\n']);
    let data = line.strip_prefix("data:")?.trim();
    if data == "[DONE]" {
        return None;
    }
    let mut event: Value = serde_json::from_str(data).ok()?;
    normalize_gateway_usage(&mut event);
    match engine.translate_event(state, WireFormat::OpenAiChat, WireFormat::AnthropicMessages, &event) {
        Ok(events) => Some(events),
        Err(e) => {
            tracing::warn!("[llm-bridge] 流事件转换失败:{e}");
            None
        }
    }
}

fn encode_events(events: Vec<Value>) -> Bytes {
    let mut out = String::new();
    for event in events {
        let kind = event.get("type").and_then(Value::as_str).unwrap_or("message");
        out.push_str(&format!("event: {kind}\ndata: {event}\n\n"));
    }
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use tower::ServiceExt;

    #[test]
    fn claude_models_are_recognized_like_the_platform() {
        assert!(is_claude_model("claude-opus-5"));
        assert!(is_claude_model(" Claude-Sonnet-5"));
        assert!(!is_claude_model("kimi-k3"));
        assert!(!is_claude_model("deepseek-v4.1-flash"));
        assert!(!is_claude_model(""));
    }

    #[test]
    fn gateway_usage_becomes_standard_openai_usage() {
        // deepseek 原样(实测):prompt_tokens 不含命中 → 加回去
        let mut v = json!({"usage": {"prompt_tokens": 259, "completion_tokens": 75, "total_tokens": 15694, "cached_prompt_tokens": 15360}});
        normalize_gateway_usage(&mut v);
        assert_eq!(v["usage"]["prompt_tokens"], 15619);
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 15360);

        // kimi 原样(实测):prompt_tokens 已含命中 → 不能再加,只补标准字段
        let mut k = json!({"usage": {"prompt_tokens": 14253, "completion_tokens": 133, "total_tokens": 14386, "cached_prompt_tokens": 256}});
        normalize_gateway_usage(&mut k);
        assert_eq!(k["usage"]["prompt_tokens"], 14253, "kimi 不能重复计算命中");
        assert_eq!(k["usage"]["prompt_tokens_details"]["cached_tokens"], 256);

        // 没有 total:命中比 prompt 大只能是「不含」;否则按「含」(宁可少算)
        let mut a = json!({"usage": {"prompt_tokens": 300, "cached_prompt_tokens": 9000}});
        normalize_gateway_usage(&mut a);
        assert_eq!(a["usage"]["prompt_tokens"], 9300);
        let mut b = json!({"usage": {"prompt_tokens": 9000, "cached_prompt_tokens": 300}});
        normalize_gateway_usage(&mut b);
        assert_eq!(b["usage"]["prompt_tokens"], 9000);

        // 已是标准形状 / 没命中 / 没 usage:不动
        let std_usage = json!({"usage": {"prompt_tokens": 100, "prompt_tokens_details": {"cached_tokens": 80}, "cached_prompt_tokens": 80}});
        let mut same = std_usage.clone();
        normalize_gateway_usage(&mut same);
        assert_eq!(same, std_usage);
        let mut zero = json!({"usage": {"prompt_tokens": 10, "cached_prompt_tokens": 0}});
        normalize_gateway_usage(&mut zero);
        assert_eq!(zero["usage"]["prompt_tokens"], 10);
        let mut none = json!({"choices": []});
        normalize_gateway_usage(&mut none);
        assert_eq!(none, json!({"choices": []}));
    }

    #[test]
    fn stream_lines_translate_tool_calls_and_cache_usage() {
        let engine = TranslationEngine::default();
        let mut state = StreamTranslationState::new(WireFormat::OpenAiChat, WireFormat::AnthropicMessages);
        let lines = [
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{"role":"assistant","content":"Checking."}}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]}}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[],"usage":{"prompt_tokens":259,"completion_tokens":75,"total_tokens":15694,"cached_prompt_tokens":15360}}"#,
            "data: [DONE]",
            ": keep-alive",
        ];
        let mut events = Vec::new();
        for line in lines {
            if let Some(evs) = translate_sse_line(&engine, &mut state, line.as_bytes()) {
                events.extend(evs);
            }
        }
        events.extend(engine.finish_stream(&mut state, WireFormat::AnthropicMessages).unwrap());
        let types: Vec<&str> = events.iter().filter_map(|e| e["type"].as_str()).collect();
        assert_eq!(types.first(), Some(&"message_start"));
        assert_eq!(types.last(), Some(&"message_stop"));
        let tool_start = events
            .iter()
            .find(|e| e["type"] == "content_block_start" && e["content_block"]["type"] == "tool_use")
            .expect("tool_use block");
        assert_eq!(tool_start["content_block"]["name"], "get_weather");
        let args: String = events
            .iter()
            .filter(|e| e["delta"]["type"] == "input_json_delta")
            .filter_map(|e| e["delta"]["partial_json"].as_str())
            .collect();
        assert_eq!(serde_json::from_str::<Value>(&args).unwrap(), json!({"city": "Paris"}));
        let delta = events.iter().rev().find(|e| e["type"] == "message_delta").expect("message_delta");
        assert_eq!(delta["delta"]["stop_reason"], "tool_use");
        assert_eq!(delta["usage"]["input_tokens"], 259);
        assert_eq!(delta["usage"]["cache_read_input_tokens"], 15360, "网关的缓存命中不能丢");
    }

    /// 起一个假网关:记下收到的路径和 Authorization,按路径回 Anthropic / OpenAI 形状。
    async fn mock_gateway() -> (String, std::sync::Arc<std::sync::Mutex<Vec<(String, String, Value)>>>) {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        let app = Router::new()
            .route(
                "/v1/messages",
                post({
                    let log = log.clone();
                    move |h: HeaderMap, body: Bytes| async move {
                        let auth = h.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                        log.lock().unwrap().push(("/v1/messages".into(), auth, serde_json::from_slice(&body).unwrap_or(Value::Null)));
                        ([("content-type", "application/json")], r#"{"type":"message","id":"msg_up","content":[{"type":"text","text":"from claude"}]}"#)
                    }
                }),
            )
            .route(
                "/v1/chat/completions",
                post({
                    let log = log.clone();
                    move |h: HeaderMap, body: Bytes| async move {
                        let auth = h.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                        let req: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                        log.lock().unwrap().push(("/v1/chat/completions".into(), auth, req.clone()));
                        if req["model"] == "broken-model" {
                            return (StatusCode::BAD_REQUEST, [("content-type", "application/json")], r#"{"error":{"message":"model not available"}}"#.to_string());
                        }
                        let body = json!({"id":"c1","object":"chat.completion","model":req["model"],
                            "choices":[{"index":0,"message":{"role":"assistant","content":"hi there"},"finish_reason":"stop"}],
                            "usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":1013,"cached_prompt_tokens":1000}});
                        (StatusCode::OK, [("content-type", "application/json")], body.to_string())
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/v1"), seen)
    }

    fn messages_request(model: &str) -> axum::http::Request<Body> {
        let body = json!({"model": model, "max_tokens": 64,
            "system": [{"type": "text", "text": "be brief", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]});
        axum::http::Request::post("/v1/messages")
            .header("authorization", "Bearer tok-123")
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn json_of(resp: Response) -> (StatusCode, Value) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn claude_models_pass_through_untouched() {
        let (upstream, seen) = mock_gateway().await;
        let (status, body) = json_of(router(upstream).oneshot(messages_request("claude-opus-5")).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["content"][0]["text"], "from claude");
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, "/v1/messages");
        assert_eq!(seen[0].1, "Bearer tok-123", "凭证原样转给网关");
        assert_eq!(seen[0].2["system"][0]["cache_control"]["type"], "ephemeral", "透传不动请求体");
    }

    #[tokio::test]
    async fn other_models_are_translated_both_ways() {
        let (upstream, seen) = mock_gateway().await;
        let (status, body) = json_of(router(upstream).oneshot(messages_request("kimi-k3")).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["type"], "message");
        assert_eq!(body["content"][0]["text"], "hi there");
        assert_eq!(body["stop_reason"], "end_turn");
        assert_eq!(body["usage"]["input_tokens"], 10);
        assert_eq!(body["usage"]["cache_read_input_tokens"], 1000);
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, "/v1/chat/completions");
        assert_eq!(seen[0].1, "Bearer tok-123");
        assert_eq!(seen[0].2["model"], "kimi-k3");
        assert_eq!(seen[0].2["messages"][0]["role"], "system", "Anthropic system 转成 OpenAI system 消息");
    }

    #[tokio::test]
    async fn upstream_errors_come_back_in_anthropic_shape() {
        let (upstream, _) = mock_gateway().await;
        let (status, body) = json_of(router(upstream).oneshot(messages_request("broken-model")).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["type"], "error");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["message"], "model not available");
    }

    #[tokio::test]
    async fn other_paths_and_missing_credentials_are_refused() {
        let (upstream, seen) = mock_gateway().await;
        let app = router(upstream);
        let hello = axum::http::Request::get("/api/hello").body(Body::empty()).unwrap();
        assert_eq!(app.clone().oneshot(hello).await.unwrap().status(), StatusCode::NOT_FOUND);
        let mut req = messages_request("kimi-k3");
        req.headers_mut().remove("authorization");
        assert_eq!(app.oneshot(req).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert!(seen.lock().unwrap().is_empty(), "没凭证不该打到网关");
    }
}
