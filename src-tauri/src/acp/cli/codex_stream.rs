//! fork(letscubo)专属: map `codex app-server` notifications to codeg
//! `AcpEvent`s. One mapper per turn; pure so it can be tested against recorded
//! output (shapes checked against @openai/codex 0.155.0, `generate-ts`).
//!
//! A turn streams, in order: `turn/started` → per item `item/started` … deltas
//! (`item/agentMessage/delta`, `item/reasoning/*Delta`) … `item/completed` →
//! `thread/tokenUsage/updated` → `turn/completed{turn.status}`. A non-retried
//! failure also sends `error{willRetry:false}` just before `turn/completed`.
//!
//! Items are keyed by `item.id`; tool items (`commandExecution`, `fileChange`,
//! `mcpToolCall`, `webSearch`, `dynamicToolCall`, `collabAgentToolCall`,
//! `imageGeneration`) become a tool card opened on `item/started` and settled
//! on `item/completed`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use super::stream_json::{
    CliTurnError, LineOutcome, TurnFinish, STOP_CANCELLED, STOP_END_TURN, STOP_UNKNOWN,
};
use super::tool_info::ToolInfo;
use crate::acp::types::AcpEvent;

pub struct CodexStreamMapper {
    cwd: Option<PathBuf>,
    model: Option<String>,
    /// The turn this mapper follows; notifications for other turns (a resumed
    /// thread replays none, but be strict) are ignored once it is known.
    turn_id: Option<String>,
    /// Kind of the last block the mapper emitted, to separate two agent
    /// messages that arrive back to back.
    last_block: Block,
    /// Items that already streamed deltas (their `item/completed` carries the
    /// same text again and must not be re-emitted).
    streamed: HashSet<String>,
    /// Tool items whose card was opened.
    open_tools: HashSet<String>,
    /// `(used, size)` from the latest `thread/tokenUsage/updated`.
    usage: Option<(u64, u64)>,
    /// A non-retried `error` notification, for a `turn/completed` without one.
    last_error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Block {
    Text,
    Other,
}

impl CodexStreamMapper {
    pub fn new(cwd: Option<PathBuf>, model: Option<String>) -> Self {
        Self {
            cwd,
            model,
            turn_id: None,
            last_block: Block::Other,
            streamed: HashSet::new(),
            open_tools: HashSet::new(),
            usage: None,
            last_error: None,
        }
    }

    pub fn set_turn_id(&mut self, turn_id: &str) {
        self.turn_id = Some(turn_id.to_string());
    }

    /// Map one server notification (`{method, params}`).
    pub fn map_notification(&mut self, msg: &Value) -> LineOutcome {
        let method = msg["method"].as_str().unwrap_or("");
        let params = &msg["params"];
        if let (Some(ours), Some(theirs)) = (self.turn_id.as_deref(), params["turnId"].as_str()) {
            if ours != theirs {
                return LineOutcome::Events(Vec::new());
            }
        }
        let events = match method {
            "item/agentMessage/delta" => self.on_text_delta(params),
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                self.on_reasoning_delta(params)
            }
            "item/started" => self.on_item_started(&params["item"]),
            "item/completed" => self.on_item_completed(&params["item"]),
            "thread/tokenUsage/updated" => {
                self.on_usage(&params["tokenUsage"]);
                Vec::new()
            }
            "error" => {
                if params["willRetry"].as_bool() != Some(true) {
                    self.last_error = error_message(&params["error"]);
                }
                Vec::new()
            }
            "turn/completed" => return self.on_turn_completed(&params["turn"]),
            _ => Vec::new(),
        };
        LineOutcome::Events(events)
    }

    fn on_text_delta(&mut self, p: &Value) -> Vec<AcpEvent> {
        let Some(delta) = p["delta"].as_str().filter(|d| !d.is_empty()) else {
            return Vec::new();
        };
        if let Some(id) = p["itemId"].as_str() {
            self.streamed.insert(id.to_string());
        }
        self.text(delta.to_string())
    }

    fn on_reasoning_delta(&mut self, p: &Value) -> Vec<AcpEvent> {
        let Some(delta) = p["delta"].as_str().filter(|d| !d.is_empty()) else {
            return Vec::new();
        };
        if let Some(id) = p["itemId"].as_str() {
            self.streamed.insert(id.to_string());
        }
        self.last_block = Block::Other;
        vec![AcpEvent::Thinking {
            text: delta.to_string(),
            parent_tool_use_id: None,
        }]
    }

    fn on_item_started(&mut self, item: &Value) -> Vec<AcpEvent> {
        let (Some(id), Some(kind)) = (item["id"].as_str(), item["type"].as_str()) else {
            return Vec::new();
        };
        // Two agent messages back to back (no tool between them) would run
        // together as one paragraph.
        if kind == "agentMessage" && self.last_block == Block::Text {
            self.last_block = Block::Other;
            return vec![AcpEvent::ContentDelta {
                text: "\n\n".to_string(),
                parent_tool_use_id: None,
            }];
        }
        if !is_tool_item(kind) {
            return Vec::new();
        }
        self.open_tools.insert(id.to_string());
        self.last_block = Block::Other;
        let info = codex_tool_info(item, self.cwd.as_deref());
        vec![AcpEvent::ToolCall {
            tool_name: codex_tool_name(item),
            description: None,
            tool_call_id: id.to_string(),
            title: info.title,
            kind: info.kind.to_string(),
            status: "in_progress".to_string(),
            content: None,
            raw_input: Some(tool_input(item).to_string()),
            raw_output: None,
            locations: info.locations,
            meta: None,
            images: None,
        }]
    }

    fn on_item_completed(&mut self, item: &Value) -> Vec<AcpEvent> {
        let (Some(id), Some(kind)) = (item["id"].as_str(), item["type"].as_str()) else {
            return Vec::new();
        };
        match kind {
            // Normally already streamed; a build or route that sends no deltas
            // still shows the whole message.
            "agentMessage" if !self.streamed.contains(id) => item["text"]
                .as_str()
                .filter(|t| !t.is_empty())
                .map(|t| self.text(t.to_string()))
                .unwrap_or_default(),
            "reasoning" if !self.streamed.contains(id) => {
                let text = joined(&item["summary"]).or_else(|| joined(&item["content"]));
                match text {
                    Some(text) => {
                        self.last_block = Block::Other;
                        vec![AcpEvent::Thinking {
                            text,
                            parent_tool_use_id: None,
                        }]
                    }
                    None => Vec::new(),
                }
            }
            k if is_tool_item(k) => {
                let mut events = Vec::new();
                // A tool item that only ever completes (no `item/started` seen)
                // still gets its card.
                if !self.open_tools.contains(id) {
                    events.extend(self.on_item_started(item));
                }
                self.open_tools.remove(id);
                self.last_block = Block::Other;
                events.push(AcpEvent::ToolCallUpdate {
                    tool_name: codex_tool_name(item),
                    description: None,
                    tool_call_id: id.to_string(),
                    title: None,
                    status: Some(
                        if tool_failed(item) {
                            "failed"
                        } else {
                            "completed"
                        }
                        .to_string(),
                    ),
                    content: None,
                    raw_input: None,
                    // Same convention as the claude / dsh mappers: serialized JSON.
                    raw_output: Some(tool_output(item).to_string()),
                    raw_output_append: None,
                    locations: None,
                    meta: None,
                    images: None,
                });
                events
            }
            _ => Vec::new(),
        }
    }

    fn on_usage(&mut self, usage: &Value) {
        let last = &usage["last"];
        let (Some(input), Some(output)) =
            (last["inputTokens"].as_u64(), last["outputTokens"].as_u64())
        else {
            return;
        };
        let size = usage["modelContextWindow"]
            .as_u64()
            .filter(|s| *s > 0)
            .or_else(|| {
                crate::parsers::infer_context_window_max_tokens(self.model.as_deref())
                    .filter(|s| *s > 0)
            });
        if let Some(size) = size {
            self.usage = Some((input + output, size));
        }
    }

    fn on_turn_completed(&mut self, turn: &Value) -> LineOutcome {
        let mut events: Vec<AcpEvent> = self
            .open_tools
            .drain()
            .map(|id| AcpEvent::ToolCallUpdate {
                // 轮次收尾把没关的调用标失败,工具名首帧已带
                tool_name: None,
                description: None,
                tool_call_id: id,
                title: None,
                status: Some("failed".to_string()),
                content: None,
                raw_input: None,
                raw_output: None,
                raw_output_append: None,
                locations: None,
                meta: None,
                images: None,
            })
            .collect();
        if let Some((used, size)) = self.usage {
            events.push(AcpEvent::UsageUpdate { used, size });
        }
        let finish = match turn["status"].as_str() {
            Some("completed") => TurnFinish {
                stop_reason: STOP_END_TURN.to_string(),
                error: None,
            },
            Some("interrupted") => TurnFinish {
                stop_reason: STOP_CANCELLED.to_string(),
                error: None,
            },
            other => {
                let message = error_message(&turn["error"])
                    .or_else(|| self.last_error.take())
                    .unwrap_or_else(|| {
                        format!("Codex turn ended with {}", other.unwrap_or("no status"))
                    });
                let auth = is_auth_error(&message);
                TurnFinish {
                    stop_reason: if auth { "auth_required" } else { STOP_UNKNOWN }.to_string(),
                    error: Some(CliTurnError {
                        code: "cli_api_error",
                        message,
                        details: None,
                    }),
                }
            }
        };
        LineOutcome::Finished { events, finish }
    }

    fn text(&mut self, text: String) -> Vec<AcpEvent> {
        self.last_block = Block::Text;
        vec![AcpEvent::ContentDelta {
            text,
            parent_tool_use_id: None,
        }]
    }
}

fn is_tool_item(kind: &str) -> bool {
    matches!(
        kind,
        "commandExecution"
            | "fileChange"
            | "mcpToolCall"
            | "webSearch"
            | "dynamicToolCall"
            | "collabAgentToolCall"
            | "imageGeneration"
    )
}

fn tool_failed(item: &Value) -> bool {
    let status = item["status"].as_str().unwrap_or("");
    matches!(status, "failed" | "declined")
        || !item["error"].is_null()
        || item["success"].as_bool() == Some(false)
        || (item["type"] == "commandExecution" && item["exitCode"].as_i64().is_some_and(|c| c != 0))
}

/// What the card shows as the call's input.
fn tool_input(item: &Value) -> Value {
    match item["type"].as_str().unwrap_or("") {
        "commandExecution" => json!({ "command": display_command(item), "cwd": item["cwd"] }),
        "fileChange" => json!({
            "changes": item["changes"].as_array().map(|changes| changes
                .iter()
                .map(|c| json!({ "path": c["path"], "kind": c["kind"]["type"] }))
                .collect::<Vec<_>>()).unwrap_or_default()
        }),
        "mcpToolCall" | "dynamicToolCall" => item["arguments"].clone(),
        "webSearch" => json!({ "query": item["query"] }),
        _ => json!({}),
    }
}

/// What the card shows as the call's result.
fn tool_output(item: &Value) -> Value {
    match item["type"].as_str().unwrap_or("") {
        "commandExecution" => {
            Value::String(item["aggregatedOutput"].as_str().unwrap_or("").to_string())
        }
        "fileChange" => Value::String(
            item["changes"]
                .as_array()
                .map(|changes| {
                    changes
                        .iter()
                        .filter_map(|c| {
                            let path = c["path"].as_str()?;
                            let diff = c["diff"].as_str().unwrap_or("");
                            Some(format!("{path}\n{diff}"))
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default(),
        ),
        "mcpToolCall" => {
            if let Some(message) = error_message(&item["error"]) {
                return Value::String(message);
            }
            let content = &item["result"]["content"];
            match content.as_array() {
                Some(blocks) => {
                    let text: Vec<&str> =
                        blocks.iter().filter_map(|b| b["text"].as_str()).collect();
                    if text.is_empty() {
                        content.clone()
                    } else {
                        Value::String(text.join("\n"))
                    }
                }
                None => item["result"].clone(),
            }
        }
        "dynamicToolCall" => item["contentItems"].clone(),
        _ => Value::Null,
    }
}

/// Title / kind / locations for a Codex tool item.
/// codex 的 item type → 规范工具名。界面按工具类型分形态要用它,而 `title` 早被换成人话了。
/// 命名对齐 claude_code 的内置工具(Bash/Edit/WebSearch),MCP 调用拼成 `mcp__<server>__<tool>`。
pub fn codex_tool_name(item: &Value) -> Option<String> {
    let s = |key: &str| item[key].as_str().filter(|v| !v.is_empty());
    Some(match item["type"].as_str().unwrap_or("") {
        "commandExecution" => "Bash".to_string(),
        "fileChange" => "Edit".to_string(),
        "webSearch" => "WebSearch".to_string(),
        "imageGeneration" => "ImageGeneration".to_string(),
        "mcpToolCall" => format!(
            "mcp__{}__{}",
            s("server").unwrap_or("mcp"),
            s("tool").unwrap_or("tool")
        ),
        "dynamicToolCall" | "collabAgentToolCall" => s("tool")?.to_string(),
        "" => return None,
        other => other.to_string(),
    })
}

pub fn codex_tool_info(item: &Value, cwd: Option<&Path>) -> ToolInfo {
    let s = |key: &str| item[key].as_str().filter(|v| !v.is_empty());
    match item["type"].as_str().unwrap_or("") {
        "commandExecution" => info(&display_command(item), "execute", None),
        "fileChange" => {
            let paths: Vec<&str> = item["changes"]
                .as_array()
                .map(|c| c.iter().filter_map(|c| c["path"].as_str()).collect())
                .unwrap_or_default();
            let title = match paths.as_slice() {
                [one] => format!("Edit {}", display_path(one, cwd)),
                [] => "Edit files".to_string(),
                many => format!("Edit {} files", many.len()),
            };
            let locations = (!paths.is_empty())
                .then(|| Value::Array(paths.iter().map(|p| json!({ "path": p })).collect()));
            info(&title, "edit", locations)
        }
        "mcpToolCall" => info(
            &format!(
                "{}: {}",
                s("server").unwrap_or("mcp"),
                s("tool").unwrap_or("tool")
            ),
            "other",
            None,
        ),
        "webSearch" => info(s("query").unwrap_or("Web search"), "fetch", None),
        "dynamicToolCall" => info(s("tool").unwrap_or("Tool"), "other", None),
        "collabAgentToolCall" => info(s("tool").unwrap_or("Task"), "think", None),
        "imageGeneration" => info("Generate image", "other", None),
        other => info(other, "other", None),
    }
}

/// The command the model asked for, without the `bash -lc '…'` wrapper codex
/// adds (`commandActions[].command` carries the bare one).
fn display_command(item: &Value) -> String {
    let actions: Vec<&str> = item["commandActions"]
        .as_array()
        .map(|a| a.iter().filter_map(|a| a["command"].as_str()).collect())
        .unwrap_or_default();
    if !actions.is_empty() {
        return actions.join(" && ");
    }
    let command = item["command"].as_str().unwrap_or("Terminal");
    unwrap_shell(command).unwrap_or(command).to_string()
}

fn unwrap_shell(command: &str) -> Option<&str> {
    let rest = command
        .strip_prefix("/bin/bash -lc ")
        .or_else(|| command.strip_prefix("bash -lc "))?;
    rest.strip_prefix('\'')?.strip_suffix('\'')
}

fn display_path(path: &str, cwd: Option<&Path>) -> String {
    match cwd.and_then(|cwd| Path::new(path).strip_prefix(cwd).ok()) {
        Some(rel) if !rel.as_os_str().is_empty() => rel.display().to_string(),
        _ => path.to_string(),
    }
}

fn info(title: &str, kind: &'static str, locations: Option<Value>) -> ToolInfo {
    ToolInfo {
        title: title.to_string(),
        kind,
        locations,
    }
}

fn joined(parts: &Value) -> Option<String> {
    let text: Vec<&str> = parts
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .filter(|s| !s.is_empty())
        .collect();
    (!text.is_empty()).then(|| text.join("\n\n"))
}

/// `TurnError` / `McpToolCallError` → a readable message. An upstream API error
/// arrives as a JSON string (`{"error":{"message":…}}`); unwrap it.
fn error_message(error: &Value) -> Option<String> {
    let raw = error["message"].as_str().filter(|m| !m.is_empty())?;
    let inner = serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|v| v["error"]["message"].as_str().map(str::to_string));
    Some(inner.unwrap_or_else(|| raw.to_string()))
}

fn is_auth_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("401")
        || lower.contains("403")
        || lower.contains("unauthorized")
        || lower.contains("invalid api key")
        || lower.contains("invalid_api_key")
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../../../resources/test-fixtures/codex/turn.jsonl");

    fn mapper() -> CodexStreamMapper {
        CodexStreamMapper::new(
            Some(PathBuf::from("/tmp/codex-probe/work")),
            Some("gpt-5.4-mini".into()),
        )
    }

    /// Replay the recorded turn: every server notification through the mapper.
    fn replay() -> (Vec<AcpEvent>, TurnFinish) {
        let mut m = mapper();
        let mut events = Vec::new();
        for line in FIXTURE.lines() {
            let Some(json) = line.strip_prefix("<< ") else {
                continue;
            };
            let msg: Value = serde_json::from_str(json).unwrap();
            if msg.get("method").is_none() || msg.get("id").is_some() {
                continue;
            }
            match m.map_notification(&msg) {
                LineOutcome::Events(e) => events.extend(e),
                LineOutcome::Finished { events: e, finish } => {
                    events.extend(e);
                    return (events, finish);
                }
                LineOutcome::Unparsable => panic!("unparsable"),
            }
        }
        panic!("no turn/completed in fixture");
    }

    #[test]
    fn recorded_turn_streams_text_tools_usage_and_ends_the_turn() {
        let (events, finish) = replay();
        assert_eq!(finish.stop_reason, STOP_END_TURN);
        assert!(finish.error.is_none());

        let deltas = events
            .iter()
            .filter(|e| matches!(e, AcpEvent::ContentDelta { .. }))
            .count();
        assert!(
            deltas > 10,
            "text should arrive as many small deltas, got {deltas}"
        );

        let calls: Vec<(&str, &str)> = events
            .iter()
            .filter_map(|e| match e {
                AcpEvent::ToolCall { title, kind, .. } => Some((title.as_str(), kind.as_str())),
                _ => None,
            })
            .collect();
        assert_eq!(
            calls,
            vec![("ls -1 /etc", "execute"), ("Edit note.txt", "edit")]
        );

        let updates: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                AcpEvent::ToolCallUpdate { status, .. } => status.as_deref(),
                _ => None,
            })
            .collect();
        assert_eq!(updates, vec!["completed", "completed"]);

        assert!(events
            .iter()
            .any(|e| matches!(e, AcpEvent::UsageUpdate { size, .. } if *size > 0)));
    }

    #[test]
    fn command_card_shows_the_bare_command_and_its_output() {
        let (events, _) = replay();
        let output = events
            .iter()
            .find_map(|e| match e {
                AcpEvent::ToolCallUpdate { raw_output, .. } => raw_output.clone(),
                _ => None,
            })
            .unwrap();
        assert!(output.contains("adduser.conf"));
    }

    #[test]
    fn failed_and_interrupted_turns_map_to_error_and_cancelled() {
        let mut m = mapper();
        m.map_notification(&json!({"method":"error","params":{"error":{"message":"{\"error\":{\"message\":\"The encrypted content could not be verified.\"}}"},"willRetry":false}}));
        let LineOutcome::Finished { finish, .. } = m.map_notification(&json!({
            "method":"turn/completed","params":{"turn":{"id":"t","status":"failed","error":null}}
        })) else {
            panic!()
        };
        assert_eq!(finish.stop_reason, STOP_UNKNOWN);
        assert_eq!(
            finish.error.unwrap().message,
            "The encrypted content could not be verified."
        );

        let mut m = mapper();
        let LineOutcome::Finished { finish, .. } = m.map_notification(&json!({
            "method":"turn/completed","params":{"turn":{"id":"t","status":"interrupted","error":null}}
        })) else { panic!() };
        assert_eq!(finish.stop_reason, STOP_CANCELLED);
        assert!(finish.error.is_none());
    }

    #[test]
    fn auth_failures_ask_for_auth() {
        let mut m = mapper();
        let LineOutcome::Finished { finish, .. } = m.map_notification(&json!({
            "method":"turn/completed","params":{"turn":{"status":"failed","error":{"message":"unexpected status 401 Unauthorized"}}}
        })) else { panic!() };
        assert_eq!(finish.stop_reason, "auth_required");
    }

    #[test]
    fn notifications_for_another_turn_are_ignored() {
        let mut m = mapper();
        m.set_turn_id("mine");
        let LineOutcome::Events(e) = m.map_notification(&json!({
            "method":"item/agentMessage/delta","params":{"turnId":"other","itemId":"x","delta":"hi"}
        })) else {
            panic!()
        };
        assert!(e.is_empty());
    }

    #[test]
    fn message_without_deltas_is_emitted_on_completion_once() {
        let mut m = mapper();
        let item = json!({"type":"agentMessage","id":"m1","text":"whole"});
        let LineOutcome::Events(e) =
            m.map_notification(&json!({"method":"item/completed","params":{"item":item}}))
        else {
            panic!()
        };
        assert!(matches!(&e[..], [AcpEvent::ContentDelta { text, .. }] if text == "whole"));

        let mut m = mapper();
        m.map_notification(
            &json!({"method":"item/agentMessage/delta","params":{"itemId":"m1","delta":"who"}}),
        );
        let LineOutcome::Events(e) =
            m.map_notification(&json!({"method":"item/completed","params":{"item":item}}))
        else {
            panic!()
        };
        assert!(e.is_empty());
    }

    #[test]
    fn mcp_tool_call_title_and_failure() {
        let item = json!({"type":"mcpToolCall","id":"c1","server":"app-notion","tool":"notion-fetch",
            "status":"failed","arguments":{"id":"x"},"result":null,"error":{"message":"boom"}});
        assert_eq!(
            codex_tool_info(&item, None).title,
            "app-notion: notion-fetch"
        );
        assert!(tool_failed(&item));
        assert_eq!(tool_output(&item), Value::String("boom".into()));
    }
}
