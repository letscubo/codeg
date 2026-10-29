//! MyClaw fork ext (letscubo) —— 让 claude_code / codex 跑任意模型的协议转接。
//!
//! 两个 CLI 各只会说一种协议,平台网关上对应的入口又只收自家模型:
//!
//! - claude_code 只说 Anthropic Messages,网关 `/v1/messages` 只收 Claude 模型;
//! - codex 只说 OpenAI Responses(codex 0.95.0 起去掉了 `wire_api = "chat"`),网关 `/v1/responses`
//!   只收 GPT 模型。
//!
//! 于是两者的模型地址都指到这里,按模型分流:
//!
//! ```text
//! claude ──Anthropic Messages──> 127.0.0.1:3080/api/myclaw/llm/v1/messages
//! codex  ──OpenAI Responses───> 127.0.0.1:3080/api/myclaw/llm/v1/responses(本模块,挂在 codeg 主服务上)
//!            · 自家模型(Claude / GPT):原样透传网关同名入口(提示缓存、加密推理等专有能力照旧)
//!            · 其余模型:请求转成 OpenAI Chat 发网关 /v1/chat/completions,响应(含流式)转回客户端的协议
//! ```
//!
//! 转换本身不自己写,用 NVIDIA Switchyard 的 `switchyard-translation`(纯 Rust、无 HTTP/SDK 依赖、
//! ~1 万行测试,带 codex 专用的自定义工具 / namespace / `additional_tools` 处理)。C9 实测:
//!
//! - 2026-10-19 真 Claude Code CLI 跑 kimi-k3 / deepseek 多步工具任务、会话中途在 Claude 与非 Claude 间来回切;
//! - 2026-10-21 真 codex 0.155.0 跑 kimi-k3 / deepseek(shell + apply_patch)、同一会话
//!   kimi → gpt → deepseek → kimi 来回切。
//!
//! 都正常。这里只做三件事:路由、转发、补网关用量字段。非 GPT 模型下 codex 的托管工具(`web_search`)
//! 会被转换库丢掉 —— Chat 协议没有对应物,搜索靠平台工具。
//!
//! ## 只放行本机
//! 3080 经 `cg-*.moltly.ai` 对公网开放,而网关的平台凭证(`<user_id>.<vm_id>`)靠「来源 IP 白名单」
//! 挡外部调用 —— 这里若对外可达,就成了绕过白名单的转发口。所以 handler 自己认两条,都满足才放行:
//!
//! - 对端地址是回环。主服务以 `into_make_service_with_connect_info` 起(见 codeg_server.rs);
//!   OVH 实例的公网请求由宿主机 nginx 转进来,对端是 docker 网关 172.17.0.1(2026-10-19 C9 实测)。
//! - 不带任何转发头(X-Forwarded-For / X-Real-IP / Forwarded / CF-Connecting-IP)。宿主机 nginx、
//!   集群 ingress、sidecar 这类反向代理都会加,容器里的 CLI 不会发;万一某种部署里代理把公网
//!   请求以 127.0.0.1 送进来,这一条照样挡住。
//!
//! 拿不到对端地址(没接 connect info 的启动方式,如桌面版)一律拒。凭证本身不在这里存:CLI 带来的
//! `Authorization` 原样转给网关,由网关认。

use std::net::SocketAddr;
use std::sync::LazyLock;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Request};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use switchyard_translation::{
    StreamTranslationState, TranslationEngine, TranslationPolicy, WireFormat,
};

pub const DEFAULT_UPSTREAM: &str = "https://api.myclaw.ai/v1";
const ENV_UPSTREAM: &str = "CODEG_LLM_UPSTREAM";
/// 一次请求体的上限(长会话 + 图片),超了直接拒,别把内存吃光
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
/// 反向代理会加、容器里的 CLI 不会发的头 —— 带了就说明请求是从外面转进来的
const FORWARDED_HEADERS: [&str; 4] = ["x-forwarded-for", "x-real-ip", "forwarded", "cf-connecting-ip"];

/// 转接的客户端。决定:说哪种协议、哪类模型原样透传、透传时带上哪些请求头、报错长什么样。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Client {
    /// claude_code:Anthropic Messages,Claude 模型透传网关 `/v1/messages`
    Claude,
    /// codex:OpenAI Responses,GPT 模型透传网关 `/v1/responses`
    Codex,
}

impl Client {
    fn wire(self) -> WireFormat {
        match self {
            Client::Claude => WireFormat::AnthropicMessages,
            Client::Codex => WireFormat::OpenAiResponses,
        }
    }

    /// 网关上与客户端同协议的入口(相对 upstream)
    fn native_path(self) -> &'static str {
        match self {
            Client::Claude => "messages",
            Client::Codex => "responses",
        }
    }

    /// 网关这个入口收的模型 —— 是就原样透传,不是就转换
    fn is_native_model(self, model: &str) -> bool {
        match self {
            Client::Claude => is_claude_model(model),
            Client::Codex => is_gpt_model(model),
        }
    }

    /// 透传时原样带上的请求头(凭证与 content-type 另加)
    fn forwarded_request_headers(self) -> &'static [&'static str] {
        match self {
            Client::Claude => &["anthropic-version", "anthropic-beta"],
            Client::Codex => &["openai-beta", "session_id", "conversation_id"],
        }
    }

    fn error(self, status: StatusCode, message: impl Into<String>) -> Response {
        match self {
            Client::Claude => anthropic_error(status, message),
            Client::Codex => openai_error(status, message),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Bridge {
    upstream: String,
    client: reqwest::Client,
}

impl Bridge {
    pub(crate) fn new(upstream: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self { upstream: upstream.into().trim().trim_end_matches('/').to_string(), client }
    }

    fn from_env() -> Self {
        let upstream = std::env::var(ENV_UPSTREAM)
            .ok()
            .filter(|u| !u.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_UPSTREAM.to_string());
        Self::new(upstream)
    }
}

static BRIDGE: LazyLock<Bridge> = LazyLock::new(Bridge::from_env);

/// `POST /api/myclaw/llm/v1/messages`(公共组,handler 自己只放行本机)。claude_code 用。
pub async fn messages(req: Request) -> Response {
    serve(&BRIDGE, Client::Claude, req).await
}

/// `POST /api/myclaw/llm/v1/responses`(公共组,handler 自己只放行本机)。codex 用。
pub async fn responses(req: Request) -> Response {
    serve(&BRIDGE, Client::Codex, req).await
}

/// `GET /api/myclaw/llm/health` —— 平台在 open 路径上据此判断「这台 codeg 有没有转接」,
/// 有才把 CLI 的模型地址指过来。同样只放行本机。
pub async fn health(req: Request) -> Response {
    if let Some(denied) = guard_local(&req, Client::Claude) {
        return denied;
    }
    (StatusCode::OK, [("content-type", "application/json")], r#"{"ok":true}"#).into_response()
}

/// 只放行容器里的本机调用(见文件头「只放行本机」)。放行返回 None,拒绝返回要回给对方的响应。
pub(crate) fn guard_local(req: &Request, client: Client) -> Option<Response> {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip().to_canonical());
    let local = peer.is_some_and(|ip| ip.is_loopback());
    let forwarded = FORWARDED_HEADERS.iter().any(|h| req.headers().contains_key(*h));
    if local && !forwarded {
        return None;
    }
    tracing::warn!("[llm-bridge] 拒绝非本机调用 peer={peer:?} forwarded={forwarded}");
    Some(client.error(StatusCode::FORBIDDEN, "the codeg LLM bridge only accepts calls from inside the instance"))
}

/// 与平台 `isAnthropicModel` 同一判据:名字以 claude 开头的走 Anthropic 原生。
pub(crate) fn is_claude_model(model: &str) -> bool {
    model.trim().to_lowercase().starts_with("claude")
}

/// 与平台 `isGptModel` 同一判据:名字以 gpt 开头的走网关 `/v1/responses` 原生。
pub(crate) fn is_gpt_model(model: &str) -> bool {
    model.trim().to_lowercase().starts_with("gpt")
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

/// 把紧跟在工具结果后面的纯文字 user 消息并进前一条 tool 消息,返回并了几条。
///
/// Claude Code 会在工具结果那条 user 消息里再追加提示(`<total_tokens>…</total_tokens>` 之类的
/// system-reminder)。转换库把同一条消息里 tool_result 之外的内容另起一条 `role: "user"`,放在
/// tool 消息之后 —— 模型看到的最后一条成了「用户刚说了话」,于是当成新一轮,从用户原话重新答。
///
/// 2026-10-28 A3(claude_code + DeepSeek V4.1 Flash):Linear 未连接时一轮里把 app_status 调了 15 次,
/// 每段思考都从「The user says 再试一次」开头;网关回「别再调、回复用户」也不管用。
///
/// 按结构判断、不认提示的写法:Anthropic 要求角色交替,所以 Chat 里紧跟 tool 消息的 user 消息
/// 只可能出自同一条带工具结果的消息。带图片等非文字内容的不动(tool 消息只收文字)。
pub(crate) fn fold_text_after_tool_results(chat: &mut Value) -> usize {
    let Some(messages) = chat.get_mut("messages").and_then(Value::as_array_mut) else {
        return 0;
    };
    let mut folded = 0;
    let mut i = 1;
    while i < messages.len() {
        let after_tool = messages[i - 1].get("role").and_then(Value::as_str) == Some("tool");
        let text = (messages[i].get("role").and_then(Value::as_str) == Some("user"))
            .then(|| plain_text_of(&messages[i]["content"]))
            .flatten();
        match (after_tool, text) {
            (true, Some(text)) => {
                let tool = &mut messages[i - 1];
                let joined = match tool.get("content").and_then(Value::as_str) {
                    Some(prev) if !prev.is_empty() => format!("{prev}\n\n{text}"),
                    _ => text,
                };
                tool["content"] = Value::String(joined);
                messages.remove(i);
                folded += 1;
            }
            _ => i += 1,
        }
    }
    folded
}

/// 历史里 assistant 的推理改用 `reasoning_content` 字段发给网关,返回改了几条。
///
/// 转换库把 Anthropic thinking 块编成 `reasoning`,网关只认 `reasoning_content`(它回给我们的推理
/// 也在这个字段),`reasoning` 被静默丢掉。DeepSeek / Kimi 这类思考模型在工具调用之间要靠回传的
/// 推理接上思路;丢了就每一步都从用户原话重新想。
///
/// 2026-10-28 A3 实测(网关 `/v1/chat/completions`,DeepSeek V4.1 Flash):把一句暗号放进上一步的
/// `reasoning`,模型答不出;放进 `reasoning_content` 能原样答出。同一天 A3-CC 一轮里把 app_status
/// 连调 15 次,每段思考都以「The user says 再试一次」开头,就是这个原因。
pub(crate) fn send_reasoning_as_reasoning_content(chat: &mut Value) -> usize {
    let Some(messages) = chat.get_mut("messages").and_then(Value::as_array_mut) else {
        return 0;
    };
    let mut moved = 0;
    for message in messages.iter_mut() {
        let Some(m) = message.as_object_mut() else { continue };
        if m.contains_key("reasoning_content") {
            continue;
        }
        if let Some(reasoning) = m.remove("reasoning") {
            m.insert("reasoning_content".into(), reasoning);
            moved += 1;
        }
    }
    moved
}

/// 只有文字时拼成一段;带任何非文字片段返回 None。
fn plain_text_of(content: &Value) -> Option<String> {
    match content {
        Value::String(s) => Some(s.clone()),
        Value::Array(parts) => parts
            .iter()
            .map(|p| match p.get("type").and_then(Value::as_str) {
                Some("text") => p.get("text").and_then(Value::as_str).map(str::to_string),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(|texts| texts.join("\n\n")),
        _ => None,
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

/// OpenAI 的错误形状 —— codex 从 `error.message` 取文案展示给用户。
fn openai_error(status: StatusCode, message: impl Into<String>) -> Response {
    let kind = match status.as_u16() {
        400 | 413 | 422 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        429 => "rate_limit_error",
        _ => "server_error",
    };
    let body = json!({"error": {"message": message.into(), "type": kind, "param": null, "code": null}});
    (status, [("content-type", "application/json")], body.to_string()).into_response()
}

/// CLI 带来的凭证:`Authorization: Bearer …`,或 claude 用 ANTHROPIC_API_KEY 时的 `x-api-key`。
fn bearer_of(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        return Some(v.to_string());
    }
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|k| format!("Bearer {k}"))
}

pub(crate) async fn serve(bridge: &Bridge, client: Client, req: Request) -> Response {
    if let Some(denied) = guard_local(&req, client) {
        return denied;
    }
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return client.error(StatusCode::PAYLOAD_TOO_LARGE, "request body too large"),
    };
    let Some(auth) = bearer_of(&headers) else {
        return client.error(StatusCode::UNAUTHORIZED, "missing credentials");
    };
    let request: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => return client.error(StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")),
    };
    let model = request.get("model").and_then(Value::as_str).unwrap_or("").to_string();
    if client.is_native_model(&model) {
        return passthrough(bridge, client, &headers, &auth, bytes).await;
    }
    translate(bridge, client, &auth, &model, &request).await
}

/// 自家模型:请求体一个字节不改发给网关同协议入口,响应(含流)原样回。
async fn passthrough(
    bridge: &Bridge,
    client: Client,
    headers: &HeaderMap,
    auth: &str,
    bytes: Bytes,
) -> Response {
    let mut req = bridge
        .client
        .post(format!("{}/{}", bridge.upstream, client.native_path()))
        .header("authorization", auth)
        .header("content-type", "application/json");
    for name in client.forwarded_request_headers() {
        if let Some(v) = headers.get(*name) {
            req = req.header(*name, v.as_bytes());
        }
    }
    match req.body(bytes).send().await {
        Ok(up) => relay_raw(up),
        Err(e) => client.error(StatusCode::BAD_GATEWAY, format!("upstream unreachable: {e}")),
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

/// 其余模型:客户端协议 → OpenAI Chat → 网关 → 转回客户端协议。
async fn translate(bridge: &Bridge, client: Client, auth: &str, model: &str, request: &Value) -> Response {
    let engine = TranslationEngine::default();
    let policy = TranslationPolicy::default();
    let stream = request.get("stream").and_then(Value::as_bool).unwrap_or(false);
    // 分两步(解码成中间形态 → 编成 Chat),留住中间形态的 extensions:codex 工具的 namespace 映射
    // 记在那里,回程靠它把展平的工具名拆回去(见 ToolNames)。
    let decoded = match engine.decode_request(client.wire(), request, &policy) {
        Ok(out) => out,
        Err(e) => return client.error(StatusCode::BAD_REQUEST, format!("cannot translate request: {e}")),
    };
    let extensions = decoded.request.extensions.clone();
    let mut chat = match engine.encode_request(WireFormat::OpenAiChat, &decoded.request, &policy) {
        Ok(out) => {
            let count = decoded.diagnostics.len() + out.diagnostics.len();
            if count > 0 {
                tracing::debug!("[llm-bridge] {model} 请求转换诊断 {count} 条");
            }
            out.body
        }
        Err(e) => return client.error(StatusCode::BAD_REQUEST, format!("cannot translate request: {e}")),
    };
    let folded = fold_text_after_tool_results(&mut chat);
    let reasoning = send_reasoning_as_reasoning_content(&mut chat);
    if folded + reasoning > 0 {
        tracing::debug!("[llm-bridge] {model} 工具结果后的文字并进 tool 消息 {folded} 段,推理回传 {reasoning} 条");
    }
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
        Err(e) => return client.error(StatusCode::BAD_GATEWAY, format!("upstream unreachable: {e}")),
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
        return client.error(status, message);
    }
    if stream {
        return stream_back(up, engine, client, ToolNames::from_request(&extensions));
    }
    let mut body: Value = match up.json().await {
        Ok(v) => v,
        Err(e) => return client.error(StatusCode::BAD_GATEWAY, format!("invalid upstream response: {e}")),
    };
    normalize_gateway_usage(&mut body);
    let encoded = engine
        .decode_response(WireFormat::OpenAiChat, &body, &policy)
        .and_then(|out| engine.encode_response_with_extensions(client.wire(), &out.response, &extensions, &policy));
    match encoded {
        Ok(mut out) => {
            ToolNames::from_request(&extensions).restore(&mut out.body);
            (StatusCode::OK, [("content-type", "application/json")], out.body.to_string()).into_response()
        }
        Err(e) => client.error(StatusCode::BAD_GATEWAY, format!("cannot translate response: {e}")),
    }
}

/// codex 工具的 namespace 还原(流式回程用)。
///
/// codex 把 MCP 服务器(`mcp__myclaw__`)、`multi_agent_v1` 这类工具编成 `namespace` 组,按「工具名 +
/// namespace」分派:回来的调用必须是 `{"name": "scheduled_tasks", "namespace": "mcp__myclaw__"}`。
/// 转成 Chat 时转换库把组展平成 `<namespace>__<工具>` 的普通函数,并把映射记在请求中间形态的
/// extensions 里;逐事件的 `translate_event` 不会拆回去,库里做这件事的函数又不公开 —— 于是在这里
/// 照库的规则补:精确名优先,其次去掉 `mcp__` 前缀的写法、只有一个组认领的裸工具名。
///
/// 另加一种库没有的写法:codex 的 MCP 组名自带结尾 `__`(`mcp__myclaw__`),展平后模型看到的是
/// `mcp__myclaw____scheduled_tasks`(四个下划线),而模型几乎总是回 codex 自己显示用的
/// `mcp__myclaw__scheduled_tasks`(codex `join_tool_name`:组名去掉结尾 `_`,工具名去掉开头 `_`,
/// 中间一个 `__`;codeg 给模型的说明里也是这么写的)。所以非流式回程也走这里,不只靠库。
///
/// 不补的后果(2026-10-28 A3 实测):codex 选「均衡」(deepseek)等非 GPT 模型时,模型回
/// `mcp__myclaw__scheduled_tasks`,codex 认不出,每个平台工具都回 `unsupported call`。
#[derive(Default)]
pub(crate) struct ToolNames(std::collections::HashMap<String, (String, String)>);

/// 转换库存映射用的 extensions 键(`switchyard-translation` 0.3 `codex_namespaces::TOOL_NAMESPACES_KEY`,
/// 该模块不公开)。库改名时 `codex_tool_namespaces_survive_the_stream` 会挂。
const CODEX_TOOL_NAMESPACES_KEY: &str = "switchyard_codex_tool_namespaces";

impl ToolNames {
    pub(crate) fn from_request(extensions: &switchyard_translation::ProviderExtensions) -> Self {
        let Some(map) = extensions.fields.get(CODEX_TOOL_NAMESPACES_KEY).and_then(Value::as_object) else {
            return Self::default();
        };
        let split: Vec<(&str, String, String)> = map
            .iter()
            .filter_map(|(qualified, v)| {
                let namespace = v.as_str().or_else(|| v.get("namespace").and_then(Value::as_str))?;
                let tool = qualified.strip_prefix(namespace)?.strip_prefix("__")?;
                Some((qualified.as_str(), tool.to_string(), namespace.to_string()))
            })
            .collect();
        let mut claims: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for (_, tool, _) in &split {
            *claims.entry(tool.as_str()).or_default() += 1;
        }
        let mut origins = std::collections::HashMap::new();
        for (qualified, tool, namespace) in &split {
            let origin = (tool.clone(), namespace.clone());
            let displayed = format!("{}__{}", namespace.trim_end_matches('_'), tool.trim_start_matches('_'));
            if !map.contains_key(&displayed) {
                origins.entry(displayed).or_insert_with(|| origin.clone());
            }
            if let Some(stripped) = namespace.strip_prefix("mcp__") {
                let spelling = format!("{stripped}__{tool}");
                if !map.contains_key(&spelling) {
                    origins.entry(spelling).or_insert_with(|| origin.clone());
                }
            }
            if claims.get(tool.as_str()) == Some(&1) && !map.contains_key(tool) {
                origins.entry(tool.clone()).or_insert_with(|| origin.clone());
            }
            origins.insert(qualified.to_string(), origin);
        }
        Self(origins)
    }

    /// 走遍整个事件:`function_call` 项(`output_item.added` / `.done`、`response.completed` 里的
    /// output)和 `response.function_call_arguments.done` 的顶层 name。已带 namespace 的不动。
    pub(crate) fn restore(&self, value: &mut Value) {
        if self.0.is_empty() {
            return;
        }
        match value {
            Value::Array(items) => items.iter_mut().for_each(|v| self.restore(v)),
            Value::Object(object) => {
                let is_call = matches!(
                    object.get("type").and_then(Value::as_str),
                    Some("function_call" | "response.function_call_arguments.done")
                );
                if is_call {
                    if let Some((tool, namespace)) = object.get("name").and_then(Value::as_str).and_then(|n| self.0.get(n)) {
                        let (tool, namespace) = (tool.clone(), namespace.clone());
                        object.insert("name".into(), Value::String(tool));
                        object.entry("namespace").or_insert(Value::String(namespace));
                    }
                }
                object.values_mut().for_each(|v| self.restore(v));
            }
            _ => {}
        }
    }
}

/// 把网关的 OpenAI SSE 逐行解析、逐事件转成客户端协议的 SSE(`event: <type>\ndata: <json>\n\n`,
/// Anthropic 与 Responses 都是这个形状)。
fn stream_back(up: reqwest::Response, engine: TranslationEngine, client: Client, names: ToolNames) -> Response {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    tokio::spawn(async move {
        let mut state = StreamTranslationState::new(WireFormat::OpenAiChat, client.wire());
        let mut indexes = ToolCallIndexes::default();
        let mut upstream = up.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        'read: while let Some(chunk) = upstream.next().await {
            let Ok(chunk) = chunk else { break };
            buf.extend_from_slice(&chunk);
            while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=pos).collect();
                let Some(mut events) = translate_sse_line(&engine, &mut state, &mut indexes, client.wire(), &line) else {
                    continue;
                };
                events.iter_mut().for_each(|e| names.restore(e));
                if !events.is_empty() && tx.send(Ok(encode_events(events))).await.is_err() {
                    break 'read; // CLI 断开了
                }
            }
        }
        if let Ok(mut events) = engine.finish_stream(&mut state, client.wire()) {
            events.iter_mut().for_each(|e| names.restore(e));
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

/// 流式工具调用补 `index`。
///
/// OpenAI 流式格式里每个工具调用片段都带 `index`,转换库(switchyard-translation `stream.rs`)按它把片段
/// 拼回一个调用,缺了就当 0。网关给 Gemini 系模型(gemini-3.7-flash / gemini-3.1-pro,2026-10-20 C9
/// 直连网关实测)的流**不带 `index`**,一条消息里的两个工具调用各自整块发来、只带 `id`:
/// 两个都被当成 0 号,名字和参数接在一起 —— `browser_screenshot` + `pdf_guide` 变成
/// `browser_screenshotpdf_guide`,参数变成 `{..}{..}`,模型拿到「工具不存在」反复重试。
/// kimi / deepseek 的流带 `index`,原样不动。
///
/// 规则:带 `index` 的照用并记下;缺的按 `id` 认 —— 见过的 `id` 用它原来的号,新 `id` 给下一个号;
/// 既没 `index` 也没 `id` 的是上一个调用的续片,沿用上一个号。
#[derive(Default)]
pub(crate) struct ToolCallIndexes {
    by_id: std::collections::HashMap<String, u64>,
    next: u64,
    last: Option<u64>,
}

impl ToolCallIndexes {
    fn fill(&mut self, event: &mut Value) {
        let Some(choices) = event.get_mut("choices").and_then(Value::as_array_mut) else {
            return;
        };
        for choice in choices {
            let Some(calls) = choice
                .get_mut("delta")
                .and_then(|d| d.get_mut("tool_calls"))
                .and_then(Value::as_array_mut)
            else {
                continue;
            };
            for call in calls.iter_mut().filter_map(Value::as_object_mut) {
                let id = call.get("id").and_then(Value::as_str).map(str::to_owned);
                let index = match call.get("index").and_then(Value::as_u64) {
                    Some(index) => index,
                    None => match &id {
                        Some(id) => match self.by_id.get(id) {
                            Some(index) => *index,
                            None => self.next,
                        },
                        None => self.last.unwrap_or(0),
                    },
                };
                call.insert("index".into(), json!(index));
                if let Some(id) = id {
                    self.by_id.insert(id, index);
                }
                self.next = self.next.max(index + 1);
                self.last = Some(index);
            }
        }
    }
}

/// 一行 OpenAI SSE → 若干客户端协议(`target`)的事件。非 `data:` 行、`[DONE]`、解析不了的行 → None(忽略)。
pub(crate) fn translate_sse_line(
    engine: &TranslationEngine,
    state: &mut StreamTranslationState,
    indexes: &mut ToolCallIndexes,
    target: WireFormat,
    line: &[u8],
) -> Option<Vec<Value>> {
    let line = std::str::from_utf8(line).ok()?.trim_end_matches(['\r', '\n']);
    let data = line.strip_prefix("data:")?.trim();
    if data == "[DONE]" {
        return None;
    }
    let mut event: Value = serde_json::from_str(data).ok()?;
    normalize_gateway_usage(&mut event);
    indexes.fill(&mut event);
    match engine.translate_event(state, WireFormat::OpenAiChat, target, &event) {
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
    use axum::routing::{any, post};
    use axum::Router;
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
        let mut indexes = ToolCallIndexes::default();
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
            if let Some(evs) = translate_sse_line(&engine, &mut state, &mut indexes, WireFormat::AnthropicMessages, line.as_bytes()) {
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

    /// 网关给 Gemini 的流(2026-10-20 C9 直连网关抓的原样形状):两个工具调用各自整块、只带 `id`、没有 `index`。
    /// 不补 `index` 时转换库把它们都当 0 号,名字拼成 `browser_screenshotpdf_guide`。
    #[test]
    fn gemini_parallel_tool_calls_without_index_stay_separate() {
        let engine = TranslationEngine::default();
        let mut state = StreamTranslationState::new(WireFormat::OpenAiChat, WireFormat::AnthropicMessages);
        let mut indexes = ToolCallIndexes::default();
        let lines = [
            r#"data: {"id":"c2","model":"gemini-3.7-flash","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"id":"call_138361","type":"function","function":{"name":"browser_screenshot","arguments":"{\"x\":\"1\"}"}}]}}]}"#,
            r#"data: {"id":"c2","model":"gemini-3.7-flash","choices":[{"index":0,"delta":{"tool_calls":[{"id":"call_138364","type":"function","function":{"name":"pdf_guide","arguments":"{\"y\":\"2\"}"}}]}}]}"#,
            r#"data: {"id":"c2","model":"gemini-3.7-flash","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
            "data: [DONE]",
        ];
        let mut events = Vec::new();
        for line in lines {
            if let Some(evs) = translate_sse_line(&engine, &mut state, &mut indexes, WireFormat::AnthropicMessages, line.as_bytes()) {
                events.extend(evs);
            }
        }
        events.extend(engine.finish_stream(&mut state, WireFormat::AnthropicMessages).unwrap());
        // 每个 tool_use 块:名字 + 它自己那段参数
        let mut calls: Vec<(String, String)> = Vec::new();
        let mut by_block: std::collections::HashMap<u64, usize> = Default::default();
        for e in &events {
            if e["type"] == "content_block_start" && e["content_block"]["type"] == "tool_use" {
                by_block.insert(e["index"].as_u64().unwrap(), calls.len());
                calls.push((e["content_block"]["name"].as_str().unwrap().to_string(), String::new()));
            }
            if e["delta"]["type"] == "input_json_delta" {
                let slot = by_block[&e["index"].as_u64().unwrap()];
                calls[slot].1.push_str(e["delta"]["partial_json"].as_str().unwrap());
            }
        }
        let names: Vec<&str> = calls.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["browser_screenshot", "pdf_guide"]);
        assert_eq!(serde_json::from_str::<Value>(&calls[0].1).unwrap(), json!({"x": "1"}));
        assert_eq!(serde_json::from_str::<Value>(&calls[1].1).unwrap(), json!({"y": "2"}));
    }

    #[test]
    fn tool_call_indexes_keep_given_ones_and_fill_by_id() {
        let mut ix = ToolCallIndexes::default();
        let mut ev = |calls: Value| {
            let mut e = json!({"choices": [{"index": 0, "delta": {"tool_calls": calls}}]});
            ix.fill(&mut e);
            e["choices"][0]["delta"]["tool_calls"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c["index"].as_u64().unwrap())
                .collect::<Vec<_>>()
        };
        // 带 index 的(kimi / deepseek 形状)原样
        assert_eq!(ev(json!([{"index": 0, "id": "a", "function": {"name": "f"}}])), [0]);
        assert_eq!(ev(json!([{"index": 0, "function": {"arguments": "{}"}}])), [0]);
        // 缺 index:新 id 拿下一个号,同一行里两个也分开
        assert_eq!(ev(json!([{"id": "b"}, {"id": "c"}])), [1, 2]);
        // 见过的 id 用原来的号;既没 index 也没 id 的续片跟上一个
        assert_eq!(ev(json!([{"id": "b", "function": {"arguments": "x"}}])), [1]);
        assert_eq!(ev(json!([{"function": {"arguments": "y"}}])), [1]);
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
                "/v1/responses",
                post({
                    let log = log.clone();
                    move |h: HeaderMap, body: Bytes| async move {
                        let auth = h.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                        let session = h.get("session_id").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                        log.lock().unwrap().push((format!("/v1/responses session={session}"), auth, serde_json::from_slice(&body).unwrap_or(Value::Null)));
                        ([("content-type", "application/json")], r#"{"object":"response","id":"resp_up","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"from gpt"}]}]}"#)
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
                        if req["model"] == "tool-model" {
                            // 模型回 codex 显示用的写法,不是请求里展平的四下划线名
                            let body = json!({"id":"c1","object":"chat.completion","model":"tool-model",
                                "choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,
                                    "tool_calls":[{"id":"call_9","type":"function",
                                        "function":{"name":"mcp__myclaw__scheduled_tasks","arguments":"{\"action\":\"list\"}"}}]}}]});
                            return (StatusCode::OK, [("content-type", "application/json")], body.to_string());
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

    /// 跟主服务里一样的挂法:handler 拿整个 Request,对端地址来自 ConnectInfo 扩展。
    fn router(upstream: String) -> Router {
        let bridge = Bridge::new(upstream);
        Router::new()
            .route("/v1/messages", any({
                let bridge = bridge.clone();
                move |req: Request| {
                    let bridge = bridge.clone();
                    async move { serve(&bridge, Client::Claude, req).await }
                }
            }))
            .route("/v1/responses", any(move |req: Request| {
                let bridge = bridge.clone();
                async move { serve(&bridge, Client::Codex, req).await }
            }))
            .route("/health", any(health))
    }

    fn from_peer(mut req: axum::http::Request<Body>, ip: [u8; 4]) -> axum::http::Request<Body> {
        req.extensions_mut().insert(ConnectInfo(SocketAddr::from((ip, 45678))));
        req
    }

    /// 容器里的 claude 发来的请求(对端 127.0.0.1、不带转发头)
    fn messages_request(model: &str) -> axum::http::Request<Body> {
        let body = json!({"model": model, "max_tokens": 64,
            "system": [{"type": "text", "text": "be brief", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]});
        let req = axum::http::Request::post("/v1/messages")
            .header("authorization", "Bearer tok-123")
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        from_peer(req, [127, 0, 0, 1])
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
    async fn reminders_after_tool_results_do_not_become_a_user_turn() {
        let (upstream, seen) = mock_gateway().await;
        // Claude Code 的形状:工具结果那条 user 消息里还追加了提示
        let body = json!({"model": "deepseek-v4.1-flash", "max_tokens": 64, "messages": [
            {"role": "user", "content": "再试一次"},
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "check the status first", "signature": ""},
                {"type": "tool_use", "id": "call_1", "name": "app_status", "input": {}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call_1", "is_error": true, "content": "{\"code\":\"APP_NOT_CONNECTED\"}"},
                {"type": "text", "text": "<system-reminder>\n<total_tokens>14999786 tokens left</total_tokens>\n</system-reminder>"}
            ]},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "call_2", "name": "Read", "input": {"file_path": "a.png"}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call_2", "content": "ok"},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "iVBORw0KGgo="}}
            ]}
        ]});
        let req = axum::http::Request::post("/v1/messages")
            .header("authorization", "Bearer tok-123")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let (status, _) = json_of(router(upstream).oneshot(from_peer(req, [127, 0, 0, 1])).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let seen = seen.lock().unwrap();
        let roles: Vec<&str> = seen[0].2["messages"].as_array().unwrap().iter()
            .map(|m| m["role"].as_str().unwrap_or("")).collect();
        assert_eq!(roles, ["user", "assistant", "tool", "assistant", "tool", "user"],
            "提示并进 tool 消息;图片 tool 消息装不下,照旧单独一条");
        let tool = seen[0].2["messages"][2]["content"].as_str().unwrap();
        assert!(tool.starts_with("{\"code\":\"APP_NOT_CONNECTED\"}\n\n<system-reminder>"), "{tool}");
        assert!(tool.contains("<total_tokens>"));
        let thought = &seen[0].2["messages"][1];
        assert_eq!(thought["reasoning_content"], "check the status first", "推理用网关认的字段回传");
        assert!(thought.get("reasoning").is_none());
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
    async fn only_calls_from_inside_the_instance_get_through() {
        let (upstream, seen) = mock_gateway().await;
        let app = router(upstream);

        // 公网经宿主机 nginx 转进来:对端是 docker 网关(C9 实测 172.17.0.1)
        let mut remote = messages_request("kimi-k3");
        remote.extensions_mut().insert(ConnectInfo(SocketAddr::from(([172, 17, 0, 1], 40856))));
        let resp = app.clone().oneshot(remote).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(json_of(resp).await.1["error"]["type"], "permission_error");

        // 对端是回环、但带了反向代理加的头 —— 代理把公网请求以 127.0.0.1 送进来的情形
        for header in FORWARDED_HEADERS {
            let mut proxied = messages_request("kimi-k3");
            proxied.headers_mut().insert(header, "203.0.113.9".parse().unwrap());
            assert_eq!(app.clone().oneshot(proxied).await.unwrap().status(), StatusCode::FORBIDDEN, "{header}");
        }

        // 拿不到对端地址(没接 connect info 的启动方式)一律拒
        let mut unknown = messages_request("kimi-k3");
        unknown.extensions_mut().remove::<ConnectInfo<SocketAddr>>();
        assert_eq!(app.clone().oneshot(unknown).await.unwrap().status(), StatusCode::FORBIDDEN);

        // 本机但没凭证:401
        let mut no_auth = messages_request("kimi-k3");
        no_auth.headers_mut().remove("authorization");
        assert_eq!(app.clone().oneshot(no_auth).await.unwrap().status(), StatusCode::UNAUTHORIZED);

        assert!(seen.lock().unwrap().is_empty(), "以上都不该打到网关");
    }

    #[tokio::test]
    async fn health_answers_only_inside_the_instance() {
        let (upstream, _) = mock_gateway().await;
        let app = router(upstream);
        let get = || axum::http::Request::get("/health").body(Body::empty()).unwrap();
        let (status, body) = json_of(app.clone().oneshot(from_peer(get(), [127, 0, 0, 1])).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ok"], true);
        assert_eq!(app.clone().oneshot(from_peer(get(), [172, 17, 0, 1])).await.unwrap().status(), StatusCode::FORBIDDEN);
        let mut proxied = from_peer(get(), [127, 0, 0, 1]);
        proxied.headers_mut().insert("x-forwarded-for", "203.0.113.9".parse().unwrap());
        assert_eq!(app.oneshot(proxied).await.unwrap().status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn ipv4_mapped_loopback_counts_as_local() {
        // 双栈监听时 127.0.0.1 会以 ::ffff:127.0.0.1 出现
        let mut req = axum::http::Request::get("/health").body(Body::empty()).unwrap();
        req.extensions_mut().insert(ConnectInfo(SocketAddr::new("::ffff:127.0.0.1".parse().unwrap(), 1)));
        assert!(guard_local(&req, Client::Claude).is_none());
        let mut v6 = axum::http::Request::get("/health").body(Body::empty()).unwrap();
        v6.extensions_mut().insert(ConnectInfo(SocketAddr::new("::1".parse().unwrap(), 1)));
        assert!(guard_local(&v6, Client::Codex).is_none());
    }

    #[test]
    fn gpt_models_are_recognized_like_the_platform() {
        assert!(is_gpt_model("gpt-5.6-terra"));
        assert!(is_gpt_model(" GPT-6-sol"));
        assert!(!is_gpt_model("kimi-k3"));
        assert!(!is_gpt_model("claude-opus-5"));
        assert!(!is_gpt_model(""));
    }

    /// 容器里的 codex 发来的请求(codex 0.155.0 实测形状的缩减版:instructions + input 项 +
    /// 函数 / namespace / 托管工具,store=false 并要加密推理)
    fn responses_request(model: &str, stream: bool) -> axum::http::Request<Body> {
        let body = json!({"model": model, "instructions": "be brief", "stream": stream, "store": false,
            "include": ["reasoning.encrypted_content"], "reasoning": {"summary": "auto"},
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}],
            "tools": [
                {"type": "function", "name": "exec_command", "description": "run a command",
                    "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}}},
                {"type": "namespace", "name": "multi_agent_v1", "description": "sub-agents",
                    "tools": [{"type": "function", "name": "spawn_agent", "description": "spawn",
                        "parameters": {"type": "object", "properties": {}}}]},
                {"type": "namespace", "name": "mcp__myclaw__", "description": "MyClaw companion",
                    "tools": [{"type": "function", "name": "scheduled_tasks", "description": "tasks",
                        "parameters": {"type": "object", "properties": {"action": {"type": "string"}}}}]},
                {"type": "web_search", "external_web_access": true}
            ],
            "tool_choice": "auto", "parallel_tool_calls": true});
        let req = axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer tok-123")
            .header("session_id", "sess-1")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        from_peer(req, [127, 0, 0, 1])
    }

    #[tokio::test]
    async fn codex_gpt_models_pass_through_untouched() {
        let (upstream, seen) = mock_gateway().await;
        let (status, body) = json_of(router(upstream).oneshot(responses_request("gpt-5.6-terra", false)).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["output"][0]["content"][0]["text"], "from gpt");
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, "/v1/responses session=sess-1", "GPT 走网关 /v1/responses,会话头一起带上");
        assert_eq!(seen[0].1, "Bearer tok-123");
        assert_eq!(seen[0].2["tools"][3]["type"], "web_search", "透传不动请求体");
    }

    #[tokio::test]
    async fn codex_other_models_are_translated_both_ways() {
        let (upstream, seen) = mock_gateway().await;
        let (status, body) = json_of(router(upstream).oneshot(responses_request("kimi-k3", false)).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["object"], "response");
        let text = body["output"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "message")
            .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
            .filter_map(|part| part["text"].as_str().map(str::to_string))
            .collect::<String>();
        assert_eq!(text, "hi there");
        assert_eq!(body["usage"]["input_tokens"], 1010, "网关不含命中的 prompt_tokens 要加回命中");
        assert_eq!(body["usage"]["input_tokens_details"]["cached_tokens"], 1000);

        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, "/v1/chat/completions");
        assert_eq!(seen[0].1, "Bearer tok-123");
        let chat = &seen[0].2;
        assert_eq!(chat["model"], "kimi-k3");
        assert_eq!(chat["messages"][0]["role"], "system", "instructions 转成 system 消息");
        let names: Vec<&str> = chat["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["function"]["name"].as_str())
            .collect();
        assert!(names.contains(&"exec_command"), "{names:?}");
        assert!(names.contains(&"multi_agent_v1__spawn_agent"), "namespace 工具展平成普通函数:{names:?}");
        assert!(names.contains(&"mcp__myclaw____scheduled_tasks"), "MCP 组名自带结尾 __:{names:?}");
        assert_eq!(names.len(), 3, "托管的 web_search 在 Chat 里没有对应物,丢掉:{names:?}");
    }

    #[tokio::test]
    async fn codex_tool_calls_get_their_namespace_back() {
        let (upstream, _seen) = mock_gateway().await;
        let (status, body) = json_of(router(upstream).oneshot(responses_request("tool-model", false)).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let call = body["output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "function_call")
            .expect("function_call item");
        assert_eq!(call["name"], "scheduled_tasks", "{call}");
        assert_eq!(call["namespace"], "mcp__myclaw__", "{call}");
    }

    #[test]
    fn codex_tool_namespaces_survive_the_stream() {
        let engine = TranslationEngine::default();
        let policy = TranslationPolicy::default();
        let request = json!({"model": "kimi-k3", "stream": true,
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
            "tools": [
                {"type": "namespace", "name": "multi_agent_v1", "tools": [{"type": "function", "name": "spawn_agent", "parameters": {"type": "object"}}]},
                {"type": "namespace", "name": "mcp__myclaw__", "tools": [{"type": "function", "name": "scheduled_tasks", "parameters": {"type": "object"}}]},
                {"type": "function", "name": "exec_command", "parameters": {"type": "object"}}
            ]});
        let decoded = engine.decode_request(WireFormat::OpenAiResponses, &request, &policy).unwrap();
        assert!(
            decoded.request.extensions.fields.contains_key(CODEX_TOOL_NAMESPACES_KEY),
            "转换库换了存映射的键,ToolNames 拿不到映射"
        );
        let names = ToolNames::from_request(&decoded.request.extensions);

        let mut state = StreamTranslationState::new(WireFormat::OpenAiChat, WireFormat::OpenAiResponses);
        let mut indexes = ToolCallIndexes::default();
        let call = |i: u64, id: &str, name: &str| {
            format!(r#"data: {{"id":"c1","model":"kimi-k3","choices":[{{"index":0,"delta":{{"tool_calls":[{{"index":{i},"id":"{id}","type":"function","function":{{"name":"{name}","arguments":"{{}}"}}}}]}}}}]}}"#)
        };
        let lines = [
            call(0, "call_a", "mcp__myclaw__scheduled_tasks"), // codex 显示用写法
            call(1, "call_b", "multi_agent_v1__spawn_agent"),  // 精确的展平名
            call(2, "call_c", "exec_command"),                 // 不在组里的,不动
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#.to_string(),
            "data: [DONE]".to_string(),
        ];
        let mut events = Vec::new();
        for line in &lines {
            if let Some(evs) = translate_sse_line(&engine, &mut state, &mut indexes, WireFormat::OpenAiResponses, line.as_bytes()) {
                events.extend(evs);
            }
        }
        events.extend(engine.finish_stream(&mut state, WireFormat::OpenAiResponses).unwrap());
        events.iter_mut().for_each(|e| names.restore(e));

        let done = |id: &str| {
            events
                .iter()
                .find(|e| e["type"] == "response.output_item.done" && e["item"]["call_id"] == id)
                .map(|e| e["item"].clone())
                .unwrap_or_else(|| panic!("no done item for {id}"))
        };
        assert_eq!((done("call_a")["name"].clone(), done("call_a")["namespace"].clone()), (json!("scheduled_tasks"), json!("mcp__myclaw__")));
        assert_eq!((done("call_b")["name"].clone(), done("call_b")["namespace"].clone()), (json!("spawn_agent"), json!("multi_agent_v1")));
        assert_eq!(done("call_c")["name"], "exec_command");
        assert!(done("call_c").get("namespace").is_none());
        // 收尾的 response.completed 里的 output 也要还原
        let completed = events.iter().find(|e| e["type"] == "response.completed").unwrap();
        let out_a = completed["response"]["output"].as_array().unwrap().iter().find(|i| i["call_id"] == "call_a").unwrap();
        assert_eq!(out_a["namespace"], "mcp__myclaw__");
    }

    #[test]
    fn ambiguous_bare_tool_names_are_left_alone() {
        let mut ext = switchyard_translation::ProviderExtensions::default();
        ext.fields.insert(
            CODEX_TOOL_NAMESPACES_KEY.into(),
            json!({"mcp__a____search": "mcp__a__", "mcp__b____search": "mcp__b__"}),
        );
        let names = ToolNames::from_request(&ext);
        let mut bare = json!({"type": "function_call", "name": "search"});
        names.restore(&mut bare);
        assert!(bare.get("namespace").is_none(), "两个组都有 search,猜不出是哪个");
        let mut shown = json!({"type": "function_call", "name": "mcp__b__search"});
        names.restore(&mut shown);
        assert_eq!(shown, json!({"type": "function_call", "name": "search", "namespace": "mcp__b__"}));
    }

    #[test]
    fn chat_stream_becomes_responses_events() {
        let engine = TranslationEngine::default();
        let mut state = StreamTranslationState::new(WireFormat::OpenAiChat, WireFormat::OpenAiResponses);
        let mut indexes = ToolCallIndexes::default();
        let lines = [
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{"role":"assistant","content":"Checking."}}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"exec_command","arguments":"{\"cmd\":"}}]}}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"ls\"}"}}]}}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
            r#"data: {"id":"c1","model":"kimi-k3","choices":[],"usage":{"prompt_tokens":259,"completion_tokens":75,"total_tokens":15694,"cached_prompt_tokens":15360}}"#,
            "data: [DONE]",
        ];
        let mut events = Vec::new();
        for line in lines {
            if let Some(evs) = translate_sse_line(&engine, &mut state, &mut indexes, WireFormat::OpenAiResponses, line.as_bytes()) {
                events.extend(evs);
            }
        }
        events.extend(engine.finish_stream(&mut state, WireFormat::OpenAiResponses).unwrap());
        let types: Vec<&str> = events.iter().filter_map(|e| e["type"].as_str()).collect();
        assert_eq!(types.first(), Some(&"response.created"), "{types:?}");
        assert_eq!(types.last(), Some(&"response.completed"), "{types:?}");
        let call = events
            .iter()
            .find(|e| e["type"] == "response.output_item.done" && e["item"]["type"] == "function_call")
            .expect("function_call item");
        assert_eq!(call["item"]["name"], "exec_command");
        assert_eq!(call["item"]["call_id"], "call_1");
        assert_eq!(serde_json::from_str::<Value>(call["item"]["arguments"].as_str().unwrap()).unwrap(), json!({"cmd": "ls"}));
        let done = events.last().unwrap();
        assert_eq!(done["response"]["usage"]["input_tokens"], 15619);
        assert_eq!(done["response"]["usage"]["input_tokens_details"]["cached_tokens"], 15360, "网关的缓存命中不能丢");
        // Responses 的 SSE 也是 `event: <type>` + `data:` 形状
        let wire = String::from_utf8(encode_events(vec![done.clone()]).to_vec()).unwrap();
        assert!(wire.starts_with("event: response.completed\ndata: {"), "{wire}");
    }

    #[tokio::test]
    async fn codex_errors_come_back_in_openai_shape() {
        let (upstream, seen) = mock_gateway().await;
        let app = router(upstream);
        let (status, body) = json_of(app.clone().oneshot(responses_request("broken-model", false)).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["message"], "model not available");
        assert!(body.get("type").is_none(), "不是 Anthropic 的错误形状");

        // 公网转进来的同样拒,错误也是 OpenAI 形状
        let mut remote = responses_request("kimi-k3", false);
        remote.extensions_mut().insert(ConnectInfo(SocketAddr::from(([172, 17, 0, 1], 40856))));
        let (status, body) = json_of(app.clone().oneshot(remote).await.unwrap()).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"]["type"], "permission_error");
        let mut proxied = responses_request("gpt-5.6-terra", false);
        proxied.headers_mut().insert("x-forwarded-for", "203.0.113.9".parse().unwrap());
        assert_eq!(app.oneshot(proxied).await.unwrap().status(), StatusCode::FORBIDDEN);
        assert_eq!(seen.lock().unwrap().len(), 1, "只有第一个请求打到了网关");
    }
}
