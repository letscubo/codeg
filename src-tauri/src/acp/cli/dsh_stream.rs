//! fork(letscubo)专属: map `dsh --profile headless --json` lines to codeg
//! `AcpEvent`s. One mapper per turn; pure so it can be tested against recorded
//! output (shapes checked against @deepseek-ai/dsh 0.1.6-alpha.2).
//!
//! The headless runner prints one JSON object per line:
//! `session` → `status{turn_start,step_start}` → `thinking`/`text` (whole
//! committed blocks, not deltas) → `tool_call`/`tool_result` →
//! `status{step_end,usage}` → `status{turn_end,reason}` → `final{text}`.
//! A runner-level failure prints `error{message}` and no `final`.

use std::path::PathBuf;

use serde_json::Value;

use super::dsh_tool_info::dsh_tool_info;
use super::stream_json::{
    CliTurnError, LineOutcome, TurnFinish, STOP_CANCELLED, STOP_END_TURN, STOP_UNKNOWN,
};
use crate::acp::types::AcpEvent;

/// `turn_end.reason.error.code` values that mean the credential was refused.
const AUTH_CODES: &[&str] = &[
    "unauthorized",
    "forbidden",
    "401",
    "403",
    "invalid_api_key",
    "missing_credential",
];

pub struct DshStreamMapper {
    session_id: Option<String>,
    cwd: Option<PathBuf>,
    model: Option<String>,
    saw_text: bool,
    /// `(inputTokens, outputTokens)` of the most recent `step_end`.
    last_step_usage: Option<(u64, u64)>,
    /// Set by `turn_end`; consumed by `final` (or by the driver on EOF).
    pending_finish: Option<TurnFinish>,
}

impl DshStreamMapper {
    pub fn new(session_id: Option<String>, cwd: Option<PathBuf>, model: Option<String>) -> Self {
        Self {
            session_id,
            cwd,
            model,
            saw_text: false,
            last_step_usage: None,
            pending_finish: None,
        }
    }

    /// The `turn_end` seen so far, for a stream that lost its `final` line.
    pub fn take_pending_finish(&mut self) -> Option<TurnFinish> {
        self.pending_finish.take()
    }

    pub fn map_line(&mut self, line: &str) -> LineOutcome {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return LineOutcome::Unparsable;
        };
        let events = match value.get("type").and_then(Value::as_str) {
            Some("session") => self.on_session(&value),
            Some("status") => {
                self.on_status(&value);
                Vec::new()
            }
            Some("thinking") => text_of(&value)
                .map(|text| AcpEvent::Thinking {
                    text,
                    parent_tool_use_id: None,
                })
                .into_iter()
                .collect(),
            Some("text") => match text_of(&value) {
                Some(text) => {
                    self.saw_text = true;
                    vec![AcpEvent::ContentDelta {
                        text,
                        parent_tool_use_id: None,
                    }]
                }
                None => Vec::new(),
            },
            Some("tool_call") => self.on_tool_call(&value),
            Some("tool_result") => on_tool_result(&value),
            Some("final") => return self.on_final(&value),
            Some("error") => return on_runner_error(&value),
            _ => Vec::new(),
        };
        LineOutcome::Events(events)
    }

    fn on_session(&mut self, v: &Value) -> Vec<AcpEvent> {
        match v["sessionId"].as_str() {
            Some(sid) if !sid.is_empty() && self.session_id.as_deref() != Some(sid) => {
                self.session_id = Some(sid.to_string());
                vec![AcpEvent::SessionStarted {
                    session_id: sid.to_string(),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn on_status(&mut self, v: &Value) {
        match v["phase"].as_str() {
            Some("step_end") => {
                let usage = &v["usage"];
                if let (Some(input), Some(output)) = (
                    usage["inputTokens"].as_u64(),
                    usage["outputTokens"].as_u64(),
                ) {
                    self.last_step_usage = Some((input, output));
                }
            }
            Some("turn_end") => {
                self.pending_finish = Some(finish_from_reason(&v["reason"]));
            }
            _ => {}
        }
    }

    fn on_tool_call(&self, v: &Value) -> Vec<AcpEvent> {
        let Some(id) = v["callId"].as_str().filter(|s| !s.is_empty()) else {
            return Vec::new();
        };
        let name = v["tool"].as_str().unwrap_or("");
        let input = v
            .get("input")
            .cloned()
            .unwrap_or(Value::Object(Default::default()));
        let info = dsh_tool_info(name, &input, self.cwd.as_deref());
        vec![AcpEvent::ToolCall {
            tool_call_id: id.to_string(),
            title: info.title,
            kind: info.kind.to_string(),
            status: "in_progress".to_string(),
            content: None,
            raw_input: Some(input.to_string()),
            raw_output: None,
            locations: info.locations,
            meta: None,
            images: None,
        }]
    }

    fn on_final(&mut self, v: &Value) -> LineOutcome {
        let mut events = Vec::new();
        if !self.saw_text {
            if let Some(text) = text_of(v) {
                events.push(AcpEvent::ContentDelta {
                    text,
                    parent_tool_use_id: None,
                });
            }
        }
        if let Some((used, size)) = self.usage_update() {
            events.push(AcpEvent::UsageUpdate { used, size });
        }
        let finish = self.pending_finish.take().unwrap_or(TurnFinish {
            stop_reason: STOP_END_TURN.to_string(),
            error: None,
        });
        LineOutcome::Finished { events, finish }
    }

    /// `(used, size)`: the last step's request footprint against the model's
    /// context window. The last step is the one whose context the next turn
    /// inherits — summing steps would re-count the cached prefix.
    fn usage_update(&self) -> Option<(u64, u64)> {
        let (input, output) = self.last_step_usage?;
        let size = crate::parsers::infer_context_window_max_tokens(self.model.as_deref())
            .filter(|s| *s > 0)?;
        Some((input + output, size))
    }
}

fn text_of(v: &Value) -> Option<String> {
    v["text"]
        .as_str()
        .filter(|t| !t.is_empty())
        .map(str::to_string)
}

fn on_tool_result(v: &Value) -> Vec<AcpEvent> {
    let Some(id) = v["callId"].as_str().filter(|s| !s.is_empty()) else {
        return Vec::new();
    };
    let failed = v["status"].as_str() == Some("error");
    let result = v["result"].as_str().unwrap_or("").to_string();
    vec![AcpEvent::ToolCallUpdate {
        tool_call_id: id.to_string(),
        title: None,
        status: Some(if failed { "failed" } else { "completed" }.to_string()),
        content: None,
        raw_input: None,
        // Same convention as the claude mapper: `raw_output` carries serialized
        // JSON, so encode the text as a JSON string and it always renders as text.
        raw_output: Some(Value::String(result).to_string()),
        raw_output_append: None,
        locations: None,
        meta: None,
        images: None,
    }]
}

/// Stop reasons use the strings `acp::lifecycle` maps to conversation status.
fn finish_from_reason(reason: &Value) -> TurnFinish {
    match reason["kind"].as_str() {
        Some("completed") | None => TurnFinish {
            stop_reason: STOP_END_TURN.to_string(),
            error: None,
        },
        Some("aborted") => TurnFinish {
            stop_reason: STOP_CANCELLED.to_string(),
            error: None,
        },
        Some(kind) => {
            let error = &reason["error"];
            let code = error["code"].as_str().unwrap_or("").to_string();
            let message = error["message"]
                .as_str()
                .filter(|m| !m.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("DeepSeek Harness turn ended with {kind}"));
            let auth = AUTH_CODES.iter().any(|c| {
                code.eq_ignore_ascii_case(c) || message.contains(" 401") || message.contains(" 403")
            });
            TurnFinish {
                stop_reason: if auth { "auth_required" } else { STOP_UNKNOWN }.to_string(),
                error: Some(CliTurnError {
                    code: "cli_api_error",
                    message,
                    details: (!code.is_empty()).then(|| format!("code={code}")),
                }),
            }
        }
    }
}

/// A runner-level `error` line: the run never reached a turn (usage error,
/// unknown `--session-id`, profile failed to load).
fn on_runner_error(v: &Value) -> LineOutcome {
    let message = v["message"]
        .as_str()
        .filter(|m| !m.is_empty())
        .unwrap_or("DeepSeek Harness reported an error")
        .to_string();
    let lower = message.to_ascii_lowercase();
    let code = if lower.contains("session")
        && (lower.contains("unknown") || lower.contains("not found") || lower.contains("no stored"))
    {
        "cli_resume_failed"
    } else {
        "cli_execution_error"
    };
    LineOutcome::Finished {
        events: Vec::new(),
        finish: TurnFinish {
            stop_reason: STOP_UNKNOWN.to_string(),
            error: Some(CliTurnError {
                code,
                message,
                details: None,
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mapper() -> DshStreamMapper {
        DshStreamMapper::new(None, Some(PathBuf::from("/ws")), Some("kimi-k3".into()))
    }

    fn events(outcome: LineOutcome) -> Vec<AcpEvent> {
        match outcome {
            LineOutcome::Events(e) => e,
            other => panic!("expected Events, got {other:?}"),
        }
    }

    #[test]
    fn session_line_announces_the_id_once() {
        let mut m = mapper();
        let line = json!({"type":"session","sessionId":"session-abc","cwd":"/ws"}).to_string();
        let first = events(m.map_line(&line));
        assert!(
            matches!(&first[0], AcpEvent::SessionStarted { session_id } if session_id == "session-abc")
        );
        assert!(events(m.map_line(&line)).is_empty());
    }

    #[test]
    fn text_and_thinking_become_whole_block_events() {
        let mut m = mapper();
        let t = events(m.map_line(&json!({"type":"thinking","text":"hmm"}).to_string()));
        assert!(matches!(&t[0], AcpEvent::Thinking { text, .. } if text == "hmm"));
        let c = events(m.map_line(&json!({"type":"text","text":"hi"}).to_string()));
        assert!(matches!(&c[0], AcpEvent::ContentDelta { text, .. } if text == "hi"));
    }

    #[test]
    fn tool_call_and_result_map_to_call_and_update() {
        let mut m = mapper();
        let call = events(m.map_line(
            &json!({"type":"tool_call","callId":"c1","tool":"mcp__app-notion__notion-fetch","input":{"id":"self"}}).to_string(),
        ));
        match &call[0] {
            AcpEvent::ToolCall {
                tool_call_id,
                status,
                raw_input,
                kind,
                ..
            } => {
                assert_eq!(tool_call_id, "c1");
                assert_eq!(status, "in_progress");
                assert_eq!(raw_input.as_deref(), Some(r#"{"id":"self"}"#));
                assert_eq!(kind, "other");
            }
            other => panic!("{other:?}"),
        }
        let result = events(
            m.map_line(
                &json!({"type":"tool_result","callId":"c1","status":"error","result":"boom"})
                    .to_string(),
            ),
        );
        match &result[0] {
            AcpEvent::ToolCallUpdate {
                tool_call_id,
                status,
                raw_output,
                ..
            } => {
                assert_eq!(tool_call_id, "c1");
                assert_eq!(status.as_deref(), Some("failed"));
                assert_eq!(raw_output.as_deref(), Some("\"boom\""));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn final_after_completed_turn_end_finishes_with_last_step_usage() {
        let mut m = mapper();
        m.map_line(&json!({"type":"status","phase":"step_end","turn":1,"step":1,"usage":{"inputTokens":8000,"outputTokens":100,"totalTokens":8100}}).to_string());
        m.map_line(&json!({"type":"status","phase":"step_end","turn":1,"step":2,"usage":{"inputTokens":9000,"outputTokens":50,"totalTokens":9050}}).to_string());
        m.map_line(&json!({"type":"text","text":"answer"}).to_string());
        m.map_line(
            &json!({"type":"status","phase":"turn_end","turn":1,"reason":{"kind":"completed"}})
                .to_string(),
        );
        match m.map_line(&json!({"type":"final","text":"answer"}).to_string()) {
            LineOutcome::Finished { events, finish } => {
                assert_eq!(finish.stop_reason, "end_turn");
                assert!(finish.error.is_none());
                assert_eq!(events.len(), 1, "no duplicate text, one usage_update");
                assert!(
                    matches!(events[0], AcpEvent::UsageUpdate { used: 9050, size } if size > 0)
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn final_without_a_text_block_supplies_the_text() {
        let mut m = mapper();
        match m.map_line(&json!({"type":"final","text":"only here"}).to_string()) {
            LineOutcome::Finished { events, finish } => {
                assert!(
                    matches!(&events[0], AcpEvent::ContentDelta { text, .. } if text == "only here")
                );
                assert_eq!(finish.stop_reason, "end_turn");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn turn_end_error_is_an_api_error_and_auth_codes_map_to_auth_required() {
        let mut m = mapper();
        m.map_line(&json!({"type":"status","phase":"turn_end","turn":1,"reason":{"kind":"error","error":{"message":"no API key","code":"MISSING_CREDENTIAL"}}}).to_string());
        match m.map_line(&json!({"type":"final","text":""}).to_string()) {
            LineOutcome::Finished { finish, .. } => {
                assert_eq!(finish.stop_reason, "auth_required");
                let err = finish.error.unwrap();
                assert_eq!(err.code, "cli_api_error");
                assert_eq!(err.message, "no API key");
            }
            other => panic!("{other:?}"),
        }
        let mut m = mapper();
        m.map_line(&json!({"type":"status","phase":"turn_end","turn":1,"reason":{"kind":"error","error":{"message":"rate limited","code":"RATE_LIMIT"}}}).to_string());
        match m.map_line(&json!({"type":"final","text":""}).to_string()) {
            LineOutcome::Finished { finish, .. } => assert_eq!(finish.stop_reason, "unknown"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn aborted_turn_end_is_cancelled() {
        let mut m = mapper();
        m.map_line(
            &json!({"type":"status","phase":"turn_end","turn":1,"reason":{"kind":"aborted"}})
                .to_string(),
        );
        assert_eq!(m.take_pending_finish().unwrap().stop_reason, "cancelled");
    }

    #[test]
    fn runner_error_finishes_immediately_and_classifies_resume_failures() {
        let mut m = mapper();
        match m.map_line(
            &json!({"type":"error","message":"unknown session id session-x: no stored Session"})
                .to_string(),
        ) {
            LineOutcome::Finished { finish, .. } => {
                assert_eq!(finish.error.unwrap().code, "cli_resume_failed")
            }
            other => panic!("{other:?}"),
        }
        match m.map_line(
            &json!({"type":"error","message":"error: unknown option '--bogus'"}).to_string(),
        ) {
            LineOutcome::Finished { finish, .. } => {
                assert_eq!(finish.error.unwrap().code, "cli_execution_error")
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn garbage_is_unparsable_and_unknown_types_are_ignored() {
        let mut m = mapper();
        assert!(matches!(m.map_line("not json"), LineOutcome::Unparsable));
        assert!(events(
            m.map_line(&json!({"type":"status","phase":"turn_start","turn":1}).to_string())
        )
        .is_empty());
        assert!(events(m.map_line(&json!({"type":"something_new"}).to_string())).is_empty());
    }
}
