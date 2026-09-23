//! fork(letscubo)专属: 伴生工具的 HTTP(MCP streamable-http)端点。
//!
//! ## 为什么要它
//!
//! 伴生(`codeg-mcp`)对 CLI 类 agent 走 `--mcp-config`,对多数 ACP agent 走
//! `session/new.mcpServers`。两条路都到不了 openclaw 与 pi:
//!
//!   · **openclaw** —— 带条目的 `session/new` 直接失败。2026-09-23 在实例上对
//!     `openclaw acp` 实测(2026.9.4):空 `mcpServers` 建得起来,带一条 stdio 条目回
//!     `-32603 "ACP bridge mode does not support per-session MCP servers. Configure
//!     MCP on the OpenClaw gateway or agent instead."` 所以注册表里它
//!     `supports_mcp: false`,整条 MCP wire(含伴生)对它关闭。
//!   · **pi** —— pi-acp 收下 `mcpServers` 却不转发给内层 `pi --mode rpc`,pi 本体又无
//!     原生 MCP。注入只会让 delegation / feedback 被标成"可用"而实际调不到,所以
//!     `agent_delivers_wire_mcp` 专门把它排除。
//!
//! 但两者都有自己的 MCP 配置文件(openclaw 的 `mcp.servers`、pi overlay 的
//! `mcp.json`),平台已经在往里写应用条目。这个端点就是给那份配置用的:一条指向
//! 本机 codeg 的 streamable-http 条目,凭证是 codeg 自己的 token。openclaw 的报错
//! 里那句 "Configure MCP on the gateway or agent instead" 说的正是这条路。
//!
//! ## 与 stdio 那条的关系
//!
//! 工具定义(`TOOL_SCHEMA_JSON`)、结果渲染(`render_upload_result`)、实现
//! (`commands::myclaw_upload`)三者共用,这里只换传输。任何一处文案改动两条链路
//! 同时生效 —— 不要在本文件里另写一份。
//!
//! ## 身份放在 URL 里
//!
//! URL 末段是调用方 agent(openclaw 名册 id `va-<vmAgentId>`)。openclaw 的
//! `mcp.servers` 是**实例级**的,而且条目没有 agent 维度 —— 2026-09-23 取
//! `openclaw config schema` 核对,一条 server 的字段只有 `enabled/command/args/env/
//! cwd/url/transport/headers/connectionTimeoutMs/requestTimeoutMs/
//! supportsParallelToolCalls/auth/oauth/sslVerify/clientCert/clientKey/toolFilter`。
//! `headers` 又是条目上的静态值,MCP 协议本身也不传"谁在调" —— 所以**共享一条条目
//! 时,调用方是谁这件事根本没被发出来**。平台因此按 agent 各写一条,身份进 URL。
//!
//! 代价是工具前缀随 agent 变(openclaw 的工具名是 `<条目名>__<工具名>`)。可以接受:
//! openclaw 侧的模型本来就是先 `tool_search` 再调用,不依赖写死的名字。
//!
//! ## v1 只开 uploads
//!
//! `upload_file` 是实例级的(listener 的 `process_upload` 只用 `path`),不需要解析到
//! 某条连接。会话级工具(delegate / ask / feedback / taskboard)全都要
//! `parent_connection_id`,得先把 `va-<id>` → 活跃连接的映射做出来;在那之前它们既
//! 不出现在 `tools/list`,被按名硬调也一律回 "unknown tool"(与 stdio 同一句,不泄露
//! "功能存在但关着")。
//!
//! ## 无状态
//!
//! 不开 SSE、不发 `Mcp-Session-Id`:每个 POST 独立返回一条 JSON-RPC 响应,通知
//! (没有 `id`)只回 202。依据是平台 apps 网关(Notion 等条目)就是这么实现的,
//! openclaw 已经在用。

use std::sync::Arc;

use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::acp::delegation::companion::{
    render_upload_result, CompanionFeatures, COMPANION_SERVER_NAME, TOOL_SCHEMA_JSON,
};
use crate::app_state::AppState;
use crate::commands::myclaw_upload::ArtifactUploadAccess;

/// 这条传输暴露的工具组 —— 见模块头「v1 只开 uploads」。
const HTTP_FEATURES: &str = "uploads";

/// 与 companion 自报的一致(companion.rs 的 `initialize`)。两处必须同版本,否则同一个
/// 伴生在两条传输上自称不同协议版本。
const PROTOCOL_VERSION: &str = "2024-11-05";

/// codeg 监听端口的来源,与 `bin/codeg_server.rs` 同一个变量、同一个缺省。
fn codeg_port() -> u16 {
    std::env::var("CODEG_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3080)
}

/// URL 末段与条目名都由它拼,所以先挡一道:只收平台真会传的那种 id
/// (`va-<uuid>`),不收路径分隔符、空白与奇怪字符。与平台侧 `vm_agent_mcp` 的
/// server 名规则(`^[a-z0-9][a-z0-9_-]{0,63}$`)同源,只是更短 —— 拼完 `myclaw-`
/// 前缀后仍要落在 openclaw / claude 的 server 名长度内。
fn valid_agent_segment(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 48
        && s.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    /// 通知没有 id —— MCP 的 `notifications/*` 走这条,不需要响应体。
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

fn envelope_ok(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn envelope_err(id: Value, code: i32, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() },
    })
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "serverInfo": { "name": COMPANION_SERVER_NAME, "version": env!("CARGO_PKG_VERSION") },
        "capabilities": { "tools": {} },
    })
}

/// 嵌入的 schema 是**全部**伴生工具的数组;按本传输开启的组过滤,关掉的组一律不
/// 露面。与 companion 的 `tools/list` 同一份数据、同一个判据(`allows_tool`)。
fn tools_list_result(features: CompanionFeatures) -> Result<Value, String> {
    let all: Value =
        serde_json::from_str(TOOL_SCHEMA_JSON).map_err(|e| format!("embedded schema invalid: {e}"))?;
    let tools = match all.as_array() {
        Some(arr) => Value::Array(
            arr.iter()
                .filter(|t| {
                    t.get("name")
                        .and_then(|v| v.as_str())
                        .map(|n| features.allows_tool(n))
                        .unwrap_or(false)
                })
                .cloned()
                .collect(),
        ),
        None => all,
    };
    Ok(json!({ "tools": tools }))
}

/// 一次 `tools/call`。`uploads` 是注入进来的,测试据此不必起真实 DB。
async fn call_tool(
    uploads: &dyn ArtifactUploadAccess,
    agent: &str,
    id: Value,
    features: CompanionFeatures,
    params: Option<Value>,
) -> Value {
    let params = params.unwrap_or(Value::Null);
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    // 未开启与不存在给同一句 —— 与 companion.rs 的拒绝形状逐字一致。
    if !features.allows_tool(&name) {
        return envelope_err(id, -32602, format!("unknown tool: {name}"));
    }
    let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
    match name.as_str() {
        "upload_file" => {
            let path = arguments
                .get("path")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let Some(path) = path else {
                return envelope_err(
                    id,
                    -32602,
                    "upload_file requires a non-empty `path` string (the file to upload)",
                );
            };
            let outcome = match uploads.upload(path).await {
                Ok(url) => json!({ "ok": true, "url": url }),
                Err(e) => {
                    // 失败原因原样回给模型(渲染层负责),这里只留一行可排查的痕迹。
                    // 路径可能含用户内容,不记;agent 段已被 `valid_agent_segment` 挡过。
                    tracing::warn!("[MyclawMcp] upload failed for {agent}: {e}");
                    json!({ "ok": false, "error": e })
                }
            };
            // 成功只回链接一行、失败回 isError + 原因 —— 渲染与 stdio 共用同一函数。
            envelope_ok(id, render_upload_result(&outcome))
        }
        other => envelope_err(id, -32602, format!("unknown tool: {other}")),
    }
}

/// 借用 `AppState.db` 的上传实现。`DbArtifactUpload` 要 `Arc<AppDatabase>`,而 state
/// 里是按值持有的,借一层比把库塞进 Arc 便宜,也不改 AppState 的形状。
struct StateUpload<'a>(&'a crate::db::AppDatabase);

#[async_trait::async_trait]
impl ArtifactUploadAccess for StateUpload<'_> {
    async fn upload(&self, path: &str) -> Result<String, String> {
        crate::commands::myclaw_upload::upload_file(self.0, path).await
    }
}

/// `POST /api/myclaw/mcp/{agent}` —— MCP over streamable-http。
pub async fn rpc(
    Path(agent): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<RpcRequest>,
) -> Response {
    if !valid_agent_segment(&agent) {
        return (StatusCode::BAD_REQUEST, "invalid agent segment").into_response();
    }
    // 通知(没有 id)不产生响应体 —— MCP 客户端只看状态码。
    let Some(id) = req.id else {
        return StatusCode::ACCEPTED.into_response();
    };
    let features = CompanionFeatures::parse(Some(HTTP_FEATURES));
    let body = match req.method.as_str() {
        "initialize" => envelope_ok(id, initialize_result()),
        "tools/list" => match tools_list_result(features) {
            Ok(result) => envelope_ok(id, result),
            Err(e) => envelope_err(id, -32603, e),
        },
        "tools/call" => {
            call_tool(&StateUpload(&state.db), &agent, id, features, req.params).await
        }
        other => envelope_err(id, -32601, format!("method not found: {other}")),
    };
    Json(body).into_response()
}

/// 一条 agent 的 MCP 条目,平台照抄进 runtime 自己的配置文件。
///
/// 「伴生跟随 codeg」的落点:名字、传输、URL、开了哪些工具全由这里说了算,平台只
/// 搬运。以后加工具 / 改名 / 换端口,平台一行都不用动。
///
/// `Authorization` 原样回显调用方刚用过的那把凭证 —— 它就是 codeg 的 token,平台本来
/// 就拿着(不然调不到这个接口)。这样 codeg 不必为了拼条目去读 WebServerState。
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpEntry {
    /// 条目名 = 模型看到的工具前缀。openclaw 是 `<name>__<tool>`,claude 是
    /// `mcp__<name>__<tool>`。按 agent 唯一 —— openclaw 的 servers 是实例级共享的。
    pub name: String,
    pub transport: &'static str,
    pub url: String,
    pub headers: std::collections::BTreeMap<String, String>,
    /// 这条传输开启的工具组与工具名,给平台做投影自检用(不影响 openclaw 读配置)。
    pub features: Vec<String>,
    pub tools: Vec<String>,
}

/// `GET /api/myclaw/mcp-entry/{agent}`
pub async fn entry(Path(agent): Path<String>, headers: HeaderMap) -> Response {
    if !valid_agent_segment(&agent) {
        return (StatusCode::BAD_REQUEST, "invalid agent segment").into_response();
    }
    let features = CompanionFeatures::parse(Some(HTTP_FEATURES));
    let tools = match tools_list_result(features) {
        Ok(v) => v["tools"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };
    let mut hdrs = std::collections::BTreeMap::new();
    if let Some(auth) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        hdrs.insert("Authorization".to_string(), auth.to_string());
    }
    Json(McpEntry {
        name: format!("{COMPANION_SERVER_NAME}-{agent}"),
        transport: "streamable-http",
        // 127.0.0.1:同容器内的 openclaw / pi 直连,不出网、不经公网路由。
        url: format!("http://127.0.0.1:{}/api/myclaw/mcp/{agent}", codeg_port()),
        headers: hdrs,
        features: HTTP_FEATURES.split(',').map(str::to_string).collect(),
        tools,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubUpload(Result<String, String>);

    #[async_trait::async_trait]
    impl ArtifactUploadAccess for StubUpload {
        async fn upload(&self, _path: &str) -> Result<String, String> {
            self.0.clone()
        }
    }

    fn features() -> CompanionFeatures {
        CompanionFeatures::parse(Some(HTTP_FEATURES))
    }

    fn tool_names(result: &Value) -> Vec<String> {
        result["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }

    /// v1 的契约:这条传输只端上 `upload_file`。会话级工具要 parent_connection_id,
    /// 在映射做出来之前露面就是骗人(pi 那个教训)。
    #[test]
    fn tools_list_exposes_only_upload_file() {
        let names = tool_names(&tools_list_result(features()).unwrap());
        assert_eq!(names, vec!["upload_file".to_string()]);
    }

    #[tokio::test]
    async fn session_scoped_tools_are_rejected_as_unknown() {
        for name in [
            "delegate_to_agent",
            "ask_user_question",
            "check_user_feedback",
            "create_work_task",
        ] {
            let out = call_tool(
                &StubUpload(Ok("https://example/x".into())),
                "va-abc",
                json!(1),
                features(),
                Some(json!({ "name": name, "arguments": {} })),
            )
            .await;
            assert_eq!(out["error"]["code"], -32602, "{name} must be refused");
            assert_eq!(out["error"]["message"], format!("unknown tool: {name}"));
        }
    }

    #[tokio::test]
    async fn upload_success_renders_the_url_only() {
        let out = call_tool(
            &StubUpload(Ok("https://bucket/x.pptx".into())),
            "va-abc",
            json!(7),
            features(),
            Some(json!({ "name": "upload_file", "arguments": { "path": "./x.pptx" } })),
        )
        .await;
        assert_eq!(out["id"], 7);
        assert_eq!(out["result"]["content"][0]["text"], "https://bucket/x.pptx");
        assert_eq!(out["result"]["isError"], false);
    }

    #[tokio::test]
    async fn upload_failure_is_an_is_error_result_not_a_protocol_error() {
        let out = call_tool(
            &StubUpload(Err("the platform refused the upload (HTTP 413)".into())),
            "va-abc",
            json!(8),
            features(),
            Some(json!({ "name": "upload_file", "arguments": { "path": "./x.pptx" } })),
        )
        .await;
        assert!(out.get("error").is_none(), "must not be a JSON-RPC error");
        assert_eq!(out["result"]["isError"], true);
        assert_eq!(
            out["result"]["content"][0]["text"],
            "the platform refused the upload (HTTP 413)"
        );
    }

    #[tokio::test]
    async fn blank_path_is_rejected_before_any_upload() {
        for args in [json!({}), json!({ "path": "   " }), json!({ "path": 5 })] {
            let out = call_tool(
                &StubUpload(Err("must not be called".into())),
                "va-abc",
                json!(1),
                features(),
                Some(json!({ "name": "upload_file", "arguments": args })),
            )
            .await;
            assert_eq!(out["error"]["code"], -32602);
        }
    }

    #[test]
    fn initialize_matches_the_stdio_companion() {
        let r = initialize_result();
        assert_eq!(r["protocolVersion"], "2024-11-05");
        assert_eq!(r["serverInfo"]["name"], COMPANION_SERVER_NAME);
    }

    /// 末段会同时进 URL 和配置文件里的条目名 —— 两处都不能被污染。
    #[test]
    fn agent_segment_is_validated() {
        assert!(valid_agent_segment("va-c4a9f7cd-c3b1-4ee4-baac-24ae8c8c465f"));
        assert!(valid_agent_segment("main"));
        for bad in [
            "",
            "-leading",
            "../etc",
            "va/../x",
            "VA-UPPER",
            "has space",
            "x?y=1",
            &"a".repeat(49),
        ] {
            assert!(!valid_agent_segment(bad), "{bad:?} must be refused");
        }
    }
}
