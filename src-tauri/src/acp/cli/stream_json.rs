//! fork(letscubo)专属: map `claude -p --output-format stream-json` lines to
//! codeg `AcpEvent`s. One mapper per turn; pure so it can be tested against
//! recorded output (shapes checked against claude 2.1.257).
//!
//! With `--include-partial-messages` the CLI streams every text/thinking block
//! as `stream_event` deltas and then repeats the whole block in an `assistant`
//! frame. Deltas are forwarded; the repeat is dropped unless no delta was seen
//! for that block. `tool_use` blocks are announced from `content_block_start`
//! (input still empty) and completed from the `assistant` frame.
//!
//! Background work (`Agent` / `Bash` with `run_in_background`): `-p` does not
//! exit when the model ends its turn while such a task runs. It waits, feeds the
//! task's `<task-notification>` back as a queued user message, lets the model
//! run a follow-up turn, and prints one `result` per turn (verified against
//! claude 2.1.276). The mapper tracks the live background set from the
//! `system` `task_*` / `background_tasks_changed` frames so the driver can tell
//! "this `result` ends the codeg turn" from "more turns are coming".

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use serde_json::{Map, Value};

use super::tool_info::tool_info;
use crate::acp::types::AcpEvent;

pub const STOP_END_TURN: &str = "end_turn";
pub const STOP_CANCELLED: &str = "cancelled";
pub const STOP_UNKNOWN: &str = "unknown";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliTurnError {
    pub code: &'static str,
    pub message: String,
    pub details: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnFinish {
    pub stop_reason: String,
    pub error: Option<CliTurnError>,
}

#[derive(Debug)]
pub enum LineOutcome {
    Events(Vec<AcpEvent>),
    /// The `result` line: the turn is over.
    Finished {
        events: Vec<AcpEvent>,
        finish: TurnFinish,
    },
    Unparsable,
}

pub struct StreamMapper {
    session_id: Option<String>,
    cwd: Option<PathBuf>,
    announced_tools: HashSet<String>,
    /// Per `parent_tool_use_id`: whether the open text/thinking block has
    /// already been streamed as deltas.
    streamed_block: HashMap<Option<String>, bool>,
    /// Background task ids still running (`background_tasks_changed` is the
    /// authority; `task_started` / `task_notification` keep it current when a
    /// build does not send that frame).
    background: HashSet<String>,
    /// Backgrounded tasks a SUB-AGENT started (`owned_by_subagent`). claude
    /// keeps waiting for them — the sub-agent resumes when they finish — but
    /// `background_tasks_changed` lists only the main thread's own tasks, so
    /// they are tracked apart and cleared only by their own settle frames
    /// (A3, 2026-09-18: a sub-agent's `sleep 25` outlived the list going empty).
    sub_background: HashSet<String>,
    /// `tool_use_id`s of backgrounded tool calls: their immediate tool_result
    /// only says "launched", so the card stays in progress until the task's
    /// `task_notification` settles it.
    background_tools: HashSet<String>,
}

impl StreamMapper {
    pub fn new(session_id: Option<String>, cwd: Option<PathBuf>) -> Self {
        Self {
            session_id,
            cwd,
            announced_tools: HashSet::new(),
            streamed_block: HashMap::new(),
            background: HashSet::new(),
            sub_background: HashSet::new(),
            background_tools: HashSet::new(),
        }
    }

    /// Whether a backgrounded task is still running — i.e. a `result` seen now
    /// is not the last one this process will print.
    pub fn background_pending(&self) -> bool {
        !self.background.is_empty() || !self.sub_background.is_empty()
    }

    pub fn map_line(&mut self, line: &str) -> LineOutcome {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return LineOutcome::Unparsable;
        };
        let events = match value.get("type").and_then(Value::as_str) {
            Some("system") => self.on_system(&value),
            Some("stream_event") => self.on_stream_event(&value),
            Some("assistant") => self.on_assistant(&value),
            Some("user") => self.on_user(&value),
            Some("result") => {
                return LineOutcome::Finished {
                    events: usage_from_result(&value)
                        .map(|(used, size)| AcpEvent::UsageUpdate { used, size })
                        .into_iter()
                        .collect(),
                    finish: finish_from_result(&value),
                }
            }
            // compact_boundary, api_retry, hook_*, task_*, rate limits, …
            _ => Vec::new(),
        };
        LineOutcome::Events(events)
    }

    fn on_system(&mut self, v: &Value) -> Vec<AcpEvent> {
        match v["subtype"].as_str() {
            Some("init") => {}
            Some("background_tasks_changed") => {
                if let Some(tasks) = v["tasks"].as_array() {
                    self.background = tasks
                        .iter()
                        .filter_map(|t| t["task_id"].as_str().map(str::to_string))
                        .collect();
                }
                return Vec::new();
            }
            Some("task_started") => {
                if v["is_backgrounded"].as_bool() == Some(true) {
                    let by_subagent = v["owned_by_subagent"].as_bool() == Some(true);
                    if let Some(id) = v["task_id"].as_str() {
                        if by_subagent {
                            self.sub_background.insert(id.to_string());
                        } else {
                            self.background.insert(id.to_string());
                        }
                    }
                    // Only the main thread's own calls own a card here; a
                    // sub-agent's backgrounded Bash is its own business.
                    if !by_subagent {
                        if let Some(tool) = v["tool_use_id"].as_str() {
                            self.background_tools.insert(tool.to_string());
                        }
                    }
                }
                return Vec::new();
            }
            Some("task_updated") => {
                let status = v["patch"]["status"].as_str().unwrap_or("");
                if matches!(
                    status,
                    "completed" | "failed" | "killed" | "stopped" | "cancelled"
                ) {
                    if let Some(id) = v["task_id"].as_str() {
                        self.background.remove(id);
                        self.sub_background.remove(id);
                    }
                }
                return Vec::new();
            }
            Some("task_notification") => {
                if let Some(id) = v["task_id"].as_str() {
                    self.background.remove(id);
                    self.sub_background.remove(id);
                }
                return self.settle_background_tool(v);
            }
            _ => return Vec::new(),
        }
        match v["session_id"].as_str() {
            Some(sid) if self.session_id.as_deref() != Some(sid) => {
                self.session_id = Some(sid.to_string());
                vec![AcpEvent::SessionStarted {
                    session_id: sid.to_string(),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn on_stream_event(&mut self, v: &Value) -> Vec<AcpEvent> {
        let parent = parent_of(v);
        let event = &v["event"];
        match event["type"].as_str() {
            Some("content_block_start") => {
                let block = &event["content_block"];
                match block["type"].as_str() {
                    Some("tool_use") => {
                        let Some(id) = block["id"].as_str() else {
                            return Vec::new();
                        };
                        if !self.announced_tools.insert(id.to_string()) {
                            return Vec::new();
                        }
                        let name = block["name"].as_str().unwrap_or("");
                        let info = tool_info(name, &Value::Object(Map::new()), self.cwd.as_deref());
                        vec![AcpEvent::ToolCall {
                            description: None,
                            tool_call_id: id.to_string(),
                            title: info.title,
                            kind: info.kind.to_string(),
                            status: "pending".to_string(),
                            content: None,
                            raw_input: None,
                            raw_output: None,
                            locations: info.locations,
                            meta: None,
                            images: None,
                        }]
                    }
                    Some("text" | "thinking") => {
                        self.streamed_block.insert(parent, false);
                        Vec::new()
                    }
                    _ => Vec::new(),
                }
            }
            Some("content_block_delta") => {
                let delta = &event["delta"];
                let (text, thinking) = match delta["type"].as_str() {
                    Some("text_delta") => (delta["text"].as_str(), false),
                    Some("thinking_delta") => (delta["thinking"].as_str(), true),
                    _ => return Vec::new(),
                };
                let Some(text) = text.filter(|t| !t.is_empty()) else {
                    return Vec::new();
                };
                self.streamed_block.insert(parent.clone(), true);
                vec![text_event(text, thinking, parent)]
            }
            _ => Vec::new(),
        }
    }

    fn on_assistant(&mut self, v: &Value) -> Vec<AcpEvent> {
        let parent = parent_of(v);
        let Some(blocks) = v["message"]["content"].as_array() else {
            return Vec::new();
        };
        let mut events = Vec::new();
        for block in blocks {
            match block["type"].as_str() {
                Some(kind @ ("text" | "thinking")) => {
                    let streamed = self.streamed_block.remove(&parent).unwrap_or(false);
                    let field = if kind == "text" { "text" } else { "thinking" };
                    if let Some(text) = block[field].as_str().filter(|t| !t.is_empty()) {
                        if !streamed {
                            events.push(text_event(text, kind == "thinking", parent.clone()));
                        }
                    }
                }
                Some("tool_use") => {
                    let Some(id) = block["id"].as_str() else {
                        continue;
                    };
                    let name = block["name"].as_str().unwrap_or("");
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    let info = tool_info(name, &input, self.cwd.as_deref());
                    let raw_input = Some(input.to_string());
                    if self.announced_tools.insert(id.to_string()) {
                        events.push(AcpEvent::ToolCall {
                            description: None,
                            tool_call_id: id.to_string(),
                            title: info.title,
                            kind: info.kind.to_string(),
                            status: "in_progress".to_string(),
                            content: None,
                            raw_input,
                            raw_output: None,
                            locations: info.locations,
                            meta: None,
                            images: None,
                        });
                    } else {
                        events.push(AcpEvent::ToolCallUpdate {
                            description: None,
                            tool_call_id: id.to_string(),
                            title: Some(info.title),
                            status: Some("in_progress".to_string()),
                            content: None,
                            raw_input,
                            raw_output: None,
                            raw_output_append: None,
                            locations: info.locations,
                            meta: None,
                            images: None,
                        });
                    }
                }
                _ => {}
            }
        }
        events
    }
}

impl StreamMapper {
    fn on_user(&self, v: &Value) -> Vec<AcpEvent> {
        let Some(blocks) = v["message"]["content"].as_array() else {
            return Vec::new();
        };
        blocks
            .iter()
            .filter(|b| b["type"] == "tool_result")
            .filter_map(|b| {
                let id = b["tool_use_id"].as_str()?;
                let failed = b["is_error"].as_bool().unwrap_or(false);
                // A backgrounded call's result only acknowledges the launch.
                let status = if failed {
                    "failed"
                } else if self.background_tools.contains(id) {
                    "in_progress"
                } else {
                    "completed"
                };
                Some(tool_result_update(id, status, &b["content"]))
            })
            .collect()
    }

    /// `task_notification` for a backgrounded main-thread call: settle its card
    /// with the task's outcome and summary.
    fn settle_background_tool(&mut self, v: &Value) -> Vec<AcpEvent> {
        let Some(tool) = v["tool_use_id"].as_str() else {
            return Vec::new();
        };
        if !self.background_tools.remove(tool) {
            return Vec::new();
        }
        let status = match v["status"].as_str() {
            Some("completed") => "completed",
            _ => "failed",
        };
        let summary = v["summary"].as_str().unwrap_or_default();
        vec![tool_result_update(
            tool,
            status,
            &Value::String(summary.to_string()),
        )]
    }
}

fn tool_result_update(id: &str, status: &str, content: &Value) -> AcpEvent {
    AcpEvent::ToolCallUpdate {
        description: None,
        tool_call_id: id.to_string(),
        title: None,
        status: Some(status.to_string()),
        content: None,
        raw_input: None,
        // `raw_output` carries serialized JSON; encode the text as a JSON
        // string so it always renders as text.
        raw_output: Some(Value::String(tool_result_text(content)).to_string()),
        raw_output_append: None,
        locations: None,
        meta: None,
        images: None,
    }
}

fn text_event(text: &str, thinking: bool, parent_tool_use_id: Option<String>) -> AcpEvent {
    if thinking {
        AcpEvent::Thinking {
            text: text.to_string(),
            parent_tool_use_id,
        }
    } else {
        AcpEvent::ContentDelta {
            text: text.to_string(),
            parent_tool_use_id,
        }
    }
}

fn parent_of(v: &Value) -> Option<String> {
    v["parent_tool_use_id"].as_str().map(str::to_string)
}

fn tool_result_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|p| p["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// `(used, size)` for `usage_update`: the last API call's context footprint
/// against the model's context window (`modelUsage.*.contextWindow`).
fn usage_from_result(v: &Value) -> Option<(u64, u64)> {
    let size = v["modelUsage"]
        .as_object()?
        .values()
        .filter_map(|m| m["contextWindow"].as_u64())
        .max()
        .filter(|size| *size > 0)?;
    let usage = &v["usage"];
    let last = usage["iterations"]
        .as_array()
        .and_then(|a| a.last())
        .unwrap_or(usage);
    let used = [
        "input_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "output_tokens",
    ]
    .iter()
    .filter_map(|k| last[*k].as_u64())
    .sum();
    Some((used, size))
}

/// Stop reasons use the strings `acp::lifecycle` maps to conversation status.
pub fn finish_from_result(v: &Value) -> TurnFinish {
    let subtype = v["subtype"].as_str().unwrap_or("");
    let is_error = v["is_error"].as_bool().unwrap_or(false);
    match subtype {
        "success" if !is_error => TurnFinish {
            stop_reason: match v["stop_reason"].as_str() {
                Some("refusal") => "refusal",
                Some("max_tokens") => "max_tokens",
                _ => STOP_END_TURN,
            }
            .to_string(),
            error: None,
        },
        "success" => {
            let status = v["api_error_status"].as_u64();
            TurnFinish {
                stop_reason: if matches!(status, Some(401 | 403)) {
                    "auth_required"
                } else {
                    STOP_UNKNOWN
                }
                .to_string(),
                error: Some(CliTurnError {
                    code: "cli_api_error",
                    message: v["result"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .unwrap_or("Claude Code reported an API error")
                        .to_string(),
                    details: status.map(|s| format!("api_error_status={s}")),
                }),
            }
        }
        "error_max_turns" => TurnFinish {
            stop_reason: "max_turn_requests".to_string(),
            error: None,
        },
        _ => {
            let errors: Vec<&str> = v["errors"]
                .as_array()
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let code = if errors.iter().any(|e| e.contains("No conversation found")) {
                "cli_resume_failed"
            } else {
                "cli_execution_error"
            };
            TurnFinish {
                stop_reason: STOP_UNKNOWN.to_string(),
                error: Some(CliTurnError {
                    code,
                    message: if errors.is_empty() {
                        format!("Claude Code turn failed ({subtype})")
                    } else {
                        errors.join("; ")
                    },
                    details: Some(format!("subtype={subtype}")),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SID: &str = "7a170f72-bff6-475a-bd3c-a3ead269f0b5";

    fn events(mapper: &mut StreamMapper, lines: &[Value]) -> Vec<Value> {
        let mut out = Vec::new();
        for line in lines {
            match mapper.map_line(&line.to_string()) {
                LineOutcome::Events(evs) | LineOutcome::Finished { events: evs, .. } => {
                    out.extend(evs.iter().map(|e| serde_json::to_value(e).unwrap()))
                }
                LineOutcome::Unparsable => panic!("unparsable fixture line"),
            }
        }
        out
    }

    fn types(evs: &[Value]) -> Vec<&str> {
        evs.iter().map(|e| e["type"].as_str().unwrap()).collect()
    }

    #[test]
    fn plain_text_turn_streams_deltas_once() {
        let mut m = StreamMapper::new(Some(SID.into()), None);
        let evs = events(
            &mut m,
            &[
                json!({"type":"system","subtype":"init","session_id":SID,"model":"claude-opus-5"}),
                json!({"type":"system","subtype":"status"}),
                json!({"type":"stream_event","event":{"type":"message_start","message":{"id":"msg_1"}},"parent_tool_use_id":null}),
                json!({"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"parent_tool_use_id":null}),
                json!({"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"pong"}},"parent_tool_use_id":null}),
                json!({"type":"assistant","message":{"id":"msg_1","content":[{"type":"text","text":"pong"}]},"parent_tool_use_id":null}),
                json!({"type":"stream_event","event":{"type":"content_block_stop","index":0},"parent_tool_use_id":null}),
            ],
        );
        assert_eq!(types(&evs), vec!["content_delta"]);
        assert_eq!(evs[0]["text"], "pong");
    }

    #[test]
    fn assistant_text_without_deltas_is_emitted() {
        let mut m = StreamMapper::new(Some(SID.into()), None);
        let evs = events(
            &mut m,
            &[
                json!({"type":"assistant","message":{"id":"msg_system","content":[{"type":"text","text":"Please restart"}]},"parent_tool_use_id":null}),
            ],
        );
        assert_eq!(types(&evs), vec!["content_delta"]);
    }

    #[test]
    fn a_new_session_id_in_init_is_announced() {
        let mut m = StreamMapper::new(Some("old".into()), None);
        let evs = events(
            &mut m,
            &[json!({"type":"system","subtype":"init","session_id":SID})],
        );
        assert_eq!(
            evs,
            vec![json!({"type":"session_started","session_id":SID})]
        );
    }

    #[test]
    fn tool_use_is_announced_completed_then_resolved() {
        let mut m = StreamMapper::new(Some(SID.into()), Some(PathBuf::from("/w")));
        let id = "toolu_015jqmuYEMbzTfZHit6metvt";
        let evs = events(
            &mut m,
            &[
                json!({"type":"stream_event","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":id,"name":"Bash","input":{}}},"parent_tool_use_id":null}),
                json!({"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"command\""}},"parent_tool_use_id":null}),
                json!({"type":"assistant","message":{"id":"msg_2","content":[{"type":"tool_use","id":id,"name":"Bash","input":{"command":"ls /etc | head -3"}}]},"parent_tool_use_id":null}),
                json!({"type":"user","message":{"role":"user","content":[{"tool_use_id":id,"type":"tool_result","content":"adduser.conf\napt","is_error":false}]},"parent_tool_use_id":null,"tool_use_result":{"stdout":"adduser.conf\napt"}}),
            ],
        );
        assert_eq!(
            types(&evs),
            vec!["tool_call", "tool_call_update", "tool_call_update"]
        );
        assert_eq!(evs[0]["status"], "pending");
        assert_eq!(evs[0]["kind"], "execute");
        assert_eq!(evs[1]["title"], "ls /etc | head -3");
        assert_eq!(
            evs[1]["raw_input"],
            json!({"command":"ls /etc | head -3"}).to_string()
        );
        assert_eq!(evs[2]["status"], "completed");
        assert_eq!(evs[2]["raw_output"], "\"adduser.conf\\napt\"");
    }

    #[test]
    fn failed_tool_result_is_marked_failed() {
        let evs = StreamMapper::new(None, None).on_user(
            &json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":"Exit code 137","is_error":true}]}}),
        );
        assert_eq!(serde_json::to_value(&evs[0]).unwrap()["status"], "failed");
    }

    #[test]
    fn success_result_reports_usage_and_end_turn() {
        let mut m = StreamMapper::new(Some(SID.into()), None);
        let line = json!({"type":"result","subtype":"success","is_error":false,"stop_reason":"end_turn","result":"pong",
            "usage":{"input_tokens":2004,"cache_creation_input_tokens":10850,"cache_read_input_tokens":43816,"output_tokens":25,
                "iterations":[{"input_tokens":2004,"output_tokens":25,"cache_read_input_tokens":43816,"cache_creation_input_tokens":10850}]},
            "modelUsage":{"claude-opus-5[1m]":{"contextWindow":1000000}}});
        match m.map_line(&line.to_string()) {
            LineOutcome::Finished { events, finish } => {
                assert_eq!(
                    serde_json::to_value(&events[0]).unwrap(),
                    json!({"type":"usage_update","used":56695,"size":1000000})
                );
                assert_eq!(
                    finish,
                    TurnFinish {
                        stop_reason: "end_turn".into(),
                        error: None
                    }
                );
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[test]
    fn missing_resume_session_maps_to_cli_resume_failed() {
        let finish = finish_from_result(
            &json!({"type":"result","subtype":"error_during_execution","is_error":true,
            "errors":["No conversation found with session ID: 03c1c0df-b77b-42d7-88c4-cd34cb1b83f6"]}),
        );
        assert_eq!(finish.stop_reason, "unknown");
        assert_eq!(finish.error.unwrap().code, "cli_resume_failed");
    }

    #[test]
    fn api_errors_and_turn_limits_map_to_lifecycle_reasons() {
        let auth = finish_from_result(
            &json!({"subtype":"success","is_error":true,"api_error_status":401,"result":"Invalid key"}),
        );
        assert_eq!(auth.stop_reason, "auth_required");
        assert_eq!(auth.error.unwrap().code, "cli_api_error");
        let turns = finish_from_result(&json!({"subtype":"error_max_turns","is_error":true}));
        assert_eq!(
            turns,
            TurnFinish {
                stop_reason: "max_turn_requests".into(),
                error: None
            }
        );
    }

    /// Shapes captured from claude 2.1.276 `-p` on A3 (2026-09-18).
    #[test]
    fn a_backgrounded_agent_stays_in_progress_until_its_notification() {
        let mut m = StreamMapper::new(Some(SID.to_string()), None);
        let tool = "toolu_bg";
        let evs = events(
            &mut m,
            &[
                json!({"type":"assistant","message":{"id":"m1","content":[{"type":"tool_use","id":tool,"name":"Agent","input":{"description":"sleep","prompt":"sleep 25","run_in_background":true}}]},"parent_tool_use_id":null}),
                json!({"type":"system","subtype":"background_tasks_changed","tasks":[{"task_id":"a01","task_type":"local_agent","description":"sleep"}]}),
                json!({"type":"system","subtype":"task_started","task_id":"a01","tool_use_id":tool,"is_backgrounded":true,"task_type":"local_agent"}),
                json!({"type":"user","message":{"role":"user","content":[{"tool_use_id":tool,"type":"tool_result","content":[{"type":"text","text":"Async agent launched successfully."}]}]},"parent_tool_use_id":null}),
                // A sub-agent's own foreground Bash is not the main thread's background work.
                json!({"type":"system","subtype":"task_started","task_id":"b01","owned_by_subagent":true,"tool_use_id":"toolu_sub","is_backgrounded":false,"task_type":"local_bash"}),
            ],
        );
        assert_eq!(
            evs.last().unwrap()["status"],
            "in_progress",
            "launch ack is not completion"
        );
        assert!(m.background_pending());

        let evs = events(
            &mut m,
            &[
                json!({"type":"system","subtype":"background_tasks_changed","tasks":[]}),
                json!({"type":"system","subtype":"task_updated","task_id":"a01","patch":{"status":"completed"}}),
                json!({"type":"system","subtype":"task_notification","task_id":"a01","tool_use_id":tool,"status":"completed","summary":"SUBAGENT_DONE"}),
            ],
        );
        assert!(!m.background_pending());
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0]["tool_call_id"], tool);
        assert_eq!(evs[0]["status"], "completed");
        assert!(evs[0]["raw_output"]
            .as_str()
            .unwrap()
            .contains("SUBAGENT_DONE"));
    }

    #[test]
    fn a_sub_agents_background_task_survives_the_main_list_going_empty() {
        let mut m = StreamMapper::new(Some(SID.to_string()), None);
        events(
            &mut m,
            &[
                json!({"type":"system","subtype":"task_started","task_id":"b1","owned_by_subagent":true,"tool_use_id":"toolu_sub","is_backgrounded":true,"task_type":"local_bash"}),
                json!({"type":"system","subtype":"background_tasks_changed","tasks":[]}),
            ],
        );
        assert!(m.background_pending(), "the sub-agent still waits on it");
        let evs = events(
            &mut m,
            &[
                json!({"type":"system","subtype":"task_notification","task_id":"b1","tool_use_id":"toolu_sub","status":"completed","summary":"BG_OK"}),
            ],
        );
        assert!(!m.background_pending());
        assert!(
            evs.is_empty(),
            "a sub-agent's task owns no main-thread card"
        );
    }

    #[test]
    fn a_failed_background_task_settles_its_card_as_failed() {
        let mut m = StreamMapper::new(Some(SID.to_string()), None);
        events(
            &mut m,
            &[
                json!({"type":"system","subtype":"task_started","task_id":"t","tool_use_id":"toolu_x","is_backgrounded":true}),
            ],
        );
        assert!(m.background_pending());
        let evs = events(
            &mut m,
            &[
                json!({"type":"system","subtype":"task_notification","task_id":"t","tool_use_id":"toolu_x","status":"failed","summary":"boom"}),
            ],
        );
        assert!(!m.background_pending());
        assert_eq!(evs[0]["status"], "failed");
    }

    #[test]
    fn garbage_is_unparsable() {
        let mut m = StreamMapper::new(None, None);
        assert!(matches!(m.map_line("not json"), LineOutcome::Unparsable));
    }
}
