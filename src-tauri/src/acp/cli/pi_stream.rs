//! fork(letscubo)专属: map `pi --mode rpc` records to codeg `AcpEvent`s.
//! One mapper per turn; pure so it can be tested against recorded output
//! (shapes checked against @earendil-works/pi-coding-agent 0.84.2 and 1.1.0).
//!
//! Pi speaks JSONL on stdout with three record families: command `response`s,
//! session events, and extension UI requests. A prompt produces:
//! `agent_start` → `turn_start` → `message_start` → `message_update`
//! (`text_start`/`text_delta`/`text_end`) → `message_end` → `turn_end` →
//! `agent_end` → `agent_settled`.
//!
//! ## Why `agent_settled` is the only terminator
//!
//! `agent_end` ends one low-level agent run, not the turn: auto-retry, context
//! compaction and steering all continue afterwards and emit another
//! `agent_start`. Pi's own RPC docs say to wait for `agent_settled` when the
//! client needs to know Pi will not continue on its own. Finishing on
//! `agent_end` would emit several `TurnComplete`s for one turn, and the driver
//! contract is exactly one.
//!
//! ## Why retry notices are not content
//!
//! A failing provider call produces `auto_retry_start` with the attempt number,
//! the backoff and the error text, and a terminal `auto_retry_end` carrying
//! `finalError`. The third-party ACP shim we used before (`pi-acp`) rendered
//! these as assistant prose — "Retrying (attempt 1/3, waiting 2s)..." — and then
//! settled the turn as a normal `end_turn`. The failure reached the user as a
//! reply that looked like the model talking, and the real error was lost.
//! Here the notices never become content: they are kept as the turn's failure
//! so `TurnFinish.error` carries the provider's own words.

use std::path::PathBuf;

use serde_json::Value;

// pi 的工具名与参数键和 dsh 逐字相同(`bash`+`command`、`read`/`write`/`edit`+`path`),
// 两边都是 pi-ai 那套命名,所以标题/类别/定位直接复用,不另起一份会漂移的副本。
use super::dsh_tool_info::dsh_tool_info;
use super::stream_json::{
    tool_result_images, CliTurnError, LineOutcome, TurnFinish, STOP_CANCELLED, STOP_END_TURN,
    STOP_UNKNOWN,
};
use crate::acp::types::AcpEvent;

/// One failed provider call, as pi reports it.
#[derive(Clone, Debug, Default)]
struct PiFailure {
    message: String,
    /// `attempt/maxAttempts` of the last retry pi scheduled, when it retried.
    attempts: Option<(u64, u64)>,
}

pub(super) struct PiStreamMapper {
    session_id: Option<String>,
    cwd: Option<PathBuf>,
    /// The most recent failure seen on a message / turn / agent record, or from
    /// `auto_retry_*`. Cleared as soon as a run produces real content, so a
    /// turn that recovered after a retry does not settle as an error.
    failure: Option<PiFailure>,
    /// `agent_end.willRetry` of the last run; `false` means pi gave up.
    gave_up: bool,
    /// Deltas already forwarded for the current content index, so the repeated
    /// whole block in `text_end` is dropped (same rule as the claude mapper).
    streamed: bool,
    pending_finish: Option<TurnFinish>,
}

impl PiStreamMapper {
    pub(super) fn new(session_id: Option<String>, cwd: Option<PathBuf>) -> Self {
        Self {
            session_id,
            cwd,
            failure: None,
            gave_up: false,
            streamed: false,
            pending_finish: None,
        }
    }

    pub(super) fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    pub(super) fn map_line(&mut self, line: &str) -> LineOutcome {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return LineOutcome::Unparsable;
        };
        let Some(kind) = v["type"].as_str() else {
            return LineOutcome::Unparsable;
        };
        match kind {
            "session" => {
                // Pi announces the session it created / resumed. Announce a new
                // one so the driver can bind it and resume it on the next turn
                // (`--session-id`); a repeat of the one we already know is not
                // re-announced, or every turn would look like a new session.
                match v["id"].as_str() {
                    Some(id) if self.session_id.as_deref() != Some(id) => {
                        self.session_id = Some(id.to_string());
                        LineOutcome::Events(vec![AcpEvent::SessionStarted {
                            session_id: id.to_string(),
                        }])
                    }
                    _ => LineOutcome::Events(Vec::new()),
                }
            }
            "message_update" => LineOutcome::Events(self.on_message_update(&v)),
            "tool_execution_start" => LineOutcome::Events(self.on_tool_start(&v)),
            "tool_execution_update" => LineOutcome::Events(on_tool_update(&v)),
            "tool_execution_end" => LineOutcome::Events(on_tool_end(&v)),
            "message_end" | "turn_end" => {
                self.note_failure(&v["message"]);
                LineOutcome::Events(Vec::new())
            }
            "agent_end" => {
                // `willRetry` is the only field that says whether pi is done
                // trying. It flips to false on the last attempt.
                self.gave_up = !v["willRetry"].as_bool().unwrap_or(false);
                if let Some(last) = v["messages"].as_array().and_then(|m| m.last()) {
                    self.note_failure(last);
                }
                LineOutcome::Events(Vec::new())
            }
            "auto_retry_start" => {
                // Deliberately emits no content — see the module docs.
                let attempts = match (v["attempt"].as_u64(), v["maxAttempts"].as_u64()) {
                    (Some(a), Some(m)) => Some((a, m)),
                    _ => None,
                };
                if let Some(message) = text_field(&v["errorMessage"]) {
                    self.failure = Some(PiFailure { message, attempts });
                } else if let Some(f) = self.failure.as_mut() {
                    f.attempts = attempts;
                }
                LineOutcome::Events(Vec::new())
            }
            "auto_retry_end" => {
                if v["success"].as_bool().unwrap_or(false) {
                    // It recovered; the turn is a normal one again.
                    self.failure = None;
                } else if let Some(message) = text_field(&v["finalError"]) {
                    let attempts = self
                        .failure
                        .as_ref()
                        .and_then(|f| f.attempts)
                        .or_else(|| v["attempt"].as_u64().map(|a| (a, a)));
                    self.failure = Some(PiFailure { message, attempts });
                }
                LineOutcome::Events(Vec::new())
            }
            "agent_settled" => {
                let aborted = v["aborted"].as_bool().unwrap_or(false);
                let finish = self
                    .pending_finish
                    .take()
                    .unwrap_or_else(|| self.finish(aborted));
                LineOutcome::Finished {
                    events: Vec::new(),
                    finish,
                }
            }
            // Everything else (agent_start, turn_start, message_start,
            // entry_appended, status chatter, extension UI) carries nothing the
            // transcript needs. Unknown records are ignored rather than guessed:
            // pi adds records between minor versions (`entry_appended` and
            // `agent_settled.aborted` both arrived in 1.x).
            _ => LineOutcome::Events(Vec::new()),
        }
    }

    /// Assistant text. Pi streams `text_delta`s and then repeats the whole block
    /// in `text_end`; forward the deltas and drop the repeat, unless the block
    /// never streamed (short replies arrive whole).
    fn on_message_update(&mut self, v: &Value) -> Vec<AcpEvent> {
        let ev = &v["assistantMessageEvent"];
        let thinking = ev["type"].as_str().is_some_and(|t| t.starts_with("thinking"));
        match ev["type"].as_str() {
            Some("text_start") | Some("thinking_start") => {
                self.streamed = false;
                Vec::new()
            }
            Some("text_delta") | Some("thinking_delta") => match text_field(&ev["delta"]) {
                Some(text) => {
                    self.streamed = true;
                    self.failure = None; // real output: this run is not a failure
                    vec![content_event(text, thinking)]
                }
                None => Vec::new(),
            },
            Some("text_end") | Some("thinking_end") => {
                if self.streamed {
                    return Vec::new();
                }
                match text_field(&ev["content"]) {
                    Some(text) => {
                        self.failure = None;
                        vec![content_event(text, thinking)]
                    }
                    None => Vec::new(),
                }
            }
            _ => Vec::new(),
        }
    }

    /// `tool_execution_start`: `{toolCallId, toolName, args}`. Pi announces the
    /// call with its arguments already complete, so unlike the claude mapper
    /// there is no "announced with empty input, filled in later" step.
    fn on_tool_start(&mut self, v: &Value) -> Vec<AcpEvent> {
        let Some(id) = v["toolCallId"].as_str() else {
            return Vec::new();
        };
        let name = v["toolName"].as_str().unwrap_or("tool");
        let args = v["args"].clone();
        let info = dsh_tool_info(name, &args, self.cwd.as_deref());
        // A tool call is real output: a turn that ran tools is not a failed one
        // just because an earlier attempt errored.
        self.failure = None;
        vec![AcpEvent::ToolCall {
            tool_call_id: id.to_string(),
            title: info.title,
            kind: info.kind.to_string(),
            status: "in_progress".to_string(),
            tool_name: Some(name.to_string()),
            description: None,
            content: None,
            raw_input: Some(args.to_string()),
            raw_output: None,
            locations: info.locations,
            meta: None,
            images: None,
        }]
    }

    /// Record a failure carried on a pi message (`stopReason: "error"` plus
    /// `errorMessage`). Any other stop reason clears it: the run produced a
    /// normal answer.
    fn note_failure(&mut self, message: &Value) {
        match message["stopReason"].as_str() {
            Some("error") => {
                if let Some(text) = text_field(&message["errorMessage"]) {
                    let attempts = self.failure.as_ref().and_then(|f| f.attempts);
                    self.failure = Some(PiFailure {
                        message: text,
                        attempts,
                    });
                }
            }
            Some(_) => self.failure = None,
            None => {}
        }
    }

    fn finish(&self, aborted: bool) -> TurnFinish {
        if aborted {
            return TurnFinish {
                stop_reason: STOP_CANCELLED.to_string(),
                error: None,
            };
        }
        // A failure only settles the turn when pi stopped retrying. While
        // `willRetry` is true another run is coming and may still succeed.
        match self.failure.as_ref().filter(|_| self.gave_up) {
            Some(failure) => TurnFinish {
                stop_reason: STOP_UNKNOWN.to_string(),
                error: Some(CliTurnError {
                    code: "cli_api_error",
                    message: failure.message.clone(),
                    details: failure
                        .attempts
                        .map(|(a, m)| format!("provider call failed after {a}/{m} attempts")),
                }),
            },
            None => TurnFinish {
                stop_reason: STOP_END_TURN.to_string(),
                error: None,
            },
        }
    }
}

/// `tool_execution_update`: streaming output while a tool runs. The recorded
/// `bash` sample carried `partialResult: {"content": []}` — nothing to show —
/// so an update with no text produces no event rather than an empty card.
fn on_tool_update(v: &Value) -> Vec<AcpEvent> {
    let Some(id) = v["toolCallId"].as_str() else {
        return Vec::new();
    };
    let Some(text) = result_text(&v["partialResult"]["content"]) else {
        return Vec::new();
    };
    vec![tool_update(id, "in_progress", Some(text), None)]
}

/// `tool_execution_end`: `{toolCallId, toolName, result:{content,structuredContent},
/// isError, durationMs}`. `isError` is the only failure signal — a non-zero
/// `exit_code` inside `structuredContent` does not set it (pi reports a command
/// that ran and failed as a successful tool call, same as every other agent).
fn on_tool_end(v: &Value) -> Vec<AcpEvent> {
    let Some(id) = v["toolCallId"].as_str() else {
        return Vec::new();
    };
    let failed = v["isError"].as_bool().unwrap_or(false);
    let status = if failed { "failed" } else { "completed" };
    let text = result_text(&v["result"]["content"]);
    // Same convention as the claude / dsh mappers: `raw_output` carries the
    // serialized string, not the raw JSON.
    let raw = text.clone().map(|t| Value::String(t).to_string());
    let mut event = tool_update(id, status, None, raw);
    // 截图等图片:只取文字的话,纯图片的结果(浏览器截图)整条变空 —— agent 看得到图,卡片和历史里
    // 却是 null(2026-10-13 C10 实测,直连 pi 之后才出现;走 ACP 桥时图是带着的)。
    // 与 claude 线路同一个解析(MCP 原样的 {type:image,data,mimeType});落盘由 pi_driver 的 emit_recorded 做。
    if let AcpEvent::ToolCallUpdate { images, .. } = &mut event {
        *images = Some(tool_result_images(&v["result"]["content"])).filter(|imgs| !imgs.is_empty());
    }
    vec![event]
}

fn tool_update(
    id: &str,
    status: &str,
    content: Option<String>,
    raw_output: Option<String>,
) -> AcpEvent {
    AcpEvent::ToolCallUpdate {
        tool_call_id: id.to_string(),
        title: None,
        tool_name: None,
        description: None,
        status: Some(status.to_string()),
        content,
        raw_input: None,
        raw_output,
        raw_output_append: None,
        locations: None,
        meta: None,
        images: None,
    }
}

/// Join the `text` blocks of a pi tool result's `content` array.
fn result_text(content: &Value) -> Option<String> {
    let parts: Vec<&str> = content
        .as_array()?
        .iter()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect();
    (!parts.is_empty()).then(|| parts.join(""))
}

fn content_event(text: String, thinking: bool) -> AcpEvent {
    if thinking {
        AcpEvent::Thinking {
            text,
            parent_tool_use_id: None,
        }
    } else {
        AcpEvent::ContentDelta {
            text,
            parent_tool_use_id: None,
        }
    }
}

/// A non-empty string field, or `None`.
fn text_field(v: &Value) -> Option<String> {
    v.as_str().filter(|s| !s.is_empty()).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mapper() -> PiStreamMapper {
        PiStreamMapper::new(None, None)
    }

    fn feed(m: &mut PiStreamMapper, v: Value) -> LineOutcome {
        m.map_line(&v.to_string())
    }

    fn texts(outcome: &LineOutcome) -> Vec<String> {
        let events = match outcome {
            LineOutcome::Events(e) => e,
            LineOutcome::Finished { events, .. } => events,
            LineOutcome::Unparsable => return Vec::new(),
        };
        events
            .iter()
            .filter_map(|e| match e {
                AcpEvent::ContentDelta { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    /// Recorded from pi 1.1.0 `--mode rpc` against an unreachable provider:
    /// four failed calls with 2s/4s/8s backoff, then `agent_settled`.
    fn failing_turn() -> Vec<Value> {
        let errored = json!({
            "role": "assistant", "content": [], "provider": "myclaw",
            "model": "GLM-5.3-FlashX", "stopReason": "error",
            "errorMessage": "503: {\"code\":\"PROVIDER_ERROR\",\"message\":\"The requested model is temporarily unavailable.\"}"
        });
        let mut lines = vec![json!({"type":"agent_start"}), json!({"type":"turn_start"})];
        for attempt in 1..=3u64 {
            lines.push(json!({"type":"message_end","message":errored}));
            lines.push(json!({"type":"turn_end","message":errored,"toolResults":[]}));
            lines.push(json!({"type":"agent_end","messages":[errored],"willRetry":true}));
            lines.push(json!({
                "type":"auto_retry_start","attempt":attempt,"maxAttempts":3,
                "delayMs": 2000 * 2u64.pow(attempt as u32 - 1),
                "errorMessage":"503: {\"code\":\"PROVIDER_ERROR\",\"message\":\"The requested model is temporarily unavailable.\"}"
            }));
            lines.push(json!({"type":"entry_appended"}));
            lines.push(json!({"type":"agent_start"}));
            lines.push(json!({"type":"turn_start"}));
        }
        lines.push(json!({"type":"agent_end","messages":[errored],"willRetry":false}));
        lines.push(json!({
            "type":"auto_retry_end","success":false,"attempt":3,
            "finalError":"503: {\"code\":\"PROVIDER_ERROR\",\"message\":\"The requested model is temporarily unavailable.\"}"
        }));
        lines.push(json!({"type":"agent_settled","aborted":false}));
        lines
    }

    #[test]
    fn a_failed_turn_carries_the_providers_own_words() {
        let mut m = mapper();
        let mut finished = Vec::new();
        for line in failing_turn() {
            if let LineOutcome::Finished { events, finish } = feed(&mut m, line) {
                assert!(events.is_empty());
                finished.push(finish);
            }
        }
        assert_eq!(finished.len(), 1, "exactly one TurnComplete per turn");
        let finish = &finished[0];
        assert_eq!(finish.stop_reason, STOP_UNKNOWN);
        let error = finish.error.as_ref().expect("a failed turn reports an error");
        assert_eq!(error.code, "cli_api_error");
        assert!(
            error.message.contains("PROVIDER_ERROR"),
            "the gateway's own text survives: {}",
            error.message
        );
        assert_eq!(
            error.details.as_deref(),
            Some("provider call failed after 3/3 attempts")
        );
    }

    #[test]
    fn retry_notices_never_become_assistant_text() {
        // The whole point: pi-acp turned these into "Retrying (attempt 1/3...)"
        // prose and the failure reached the user as a reply.
        let mut m = mapper();
        for line in failing_turn() {
            assert!(
                texts(&feed(&mut m, line)).is_empty(),
                "no retry notice may reach the transcript"
            );
        }
    }

    #[test]
    fn agent_end_is_not_the_turn_boundary() {
        // Three runs end before the turn does; finishing on `agent_end` would
        // emit three TurnCompletes and wedge the connection.
        let mut m = mapper();
        let mut early = 0;
        for line in failing_turn() {
            let is_settled = line["type"] == "agent_settled";
            if matches!(feed(&mut m, line), LineOutcome::Finished { .. }) && !is_settled {
                early += 1;
            }
        }
        assert_eq!(early, 0);
    }

    #[test]
    fn deltas_stream_once_and_the_repeat_is_dropped() {
        let mut m = mapper();
        feed(&mut m, json!({"type":"agent_start"}));
        let ev = |t: &str, k: &str, v: &str| {
            json!({"type":"message_update","assistantMessageEvent":{"type":t,"contentIndex":0,k:v}})
        };
        feed(&mut m, ev("text_start", "contentIndex", ""));
        assert_eq!(texts(&feed(&mut m, ev("text_delta", "delta", "Hi!"))), ["Hi!"]);
        assert_eq!(
            texts(&feed(&mut m, ev("text_delta", "delta", " there"))),
            [" there"]
        );
        assert!(
            texts(&feed(&mut m, ev("text_end", "content", "Hi! there"))).is_empty(),
            "the whole-block repeat is dropped"
        );
    }

    #[test]
    fn a_short_reply_that_never_streamed_is_still_emitted() {
        let mut m = mapper();
        let whole = json!({"type":"message_update","assistantMessageEvent":{"type":"text_end","contentIndex":0,"content":"Hi!"}});
        assert_eq!(texts(&feed(&mut m, whole)), ["Hi!"]);
    }

    #[test]
    fn a_turn_that_recovered_after_a_retry_is_not_an_error() {
        let mut m = mapper();
        feed(
            &mut m,
            json!({"type":"auto_retry_start","attempt":1,"maxAttempts":3,"delayMs":2000,"errorMessage":"Connection error."}),
        );
        feed(
            &mut m,
            json!({"type":"auto_retry_end","success":true,"attempt":1}),
        );
        feed(&mut m, json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"ok"}}));
        feed(
            &mut m,
            json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"stop"}],"willRetry":false}),
        );
        match feed(&mut m, json!({"type":"agent_settled","aborted":false})) {
            LineOutcome::Finished { finish, .. } => {
                assert_eq!(finish.stop_reason, STOP_END_TURN);
                assert!(finish.error.is_none());
            }
            other => panic!("expected a finish, got {other:?}"),
        }
    }

    #[test]
    fn an_aborted_run_settles_as_cancelled() {
        let mut m = mapper();
        feed(
            &mut m,
            json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"error","errorMessage":"Connection error."}],"willRetry":false}),
        );
        match feed(&mut m, json!({"type":"agent_settled","aborted":true})) {
            LineOutcome::Finished { finish, .. } => {
                assert_eq!(finish.stop_reason, STOP_CANCELLED);
                assert!(finish.error.is_none(), "a cancel is not a failure");
            }
            other => panic!("expected a finish, got {other:?}"),
        }
    }

    #[test]
    fn a_failure_while_pi_still_retries_does_not_settle_the_turn() {
        // `willRetry: true` on the last run means another attempt is coming;
        // settling as an error there would report a failure pi recovered from.
        let mut m = mapper();
        feed(
            &mut m,
            json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"error","errorMessage":"Connection error."}],"willRetry":true}),
        );
        match feed(&mut m, json!({"type":"agent_settled"})) {
            LineOutcome::Finished { finish, .. } => assert_eq!(finish.stop_reason, STOP_END_TURN),
            other => panic!("expected a finish, got {other:?}"),
        }
    }

    #[test]
    fn the_session_id_is_picked_up_and_announced_once() {
        let mut m = mapper();
        let line = json!({"type":"session","version":3,"id":"01a1271a-81fb-7fbd-9bb4-5299598a4dbc","cwd":"/tmp"});
        match feed(&mut m, line.clone()) {
            LineOutcome::Events(e) => match &e[..] {
                [AcpEvent::SessionStarted { session_id }] => {
                    assert_eq!(session_id, "01a1271a-81fb-7fbd-9bb4-5299598a4dbc")
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
        assert_eq!(m.session_id(), Some("01a1271a-81fb-7fbd-9bb4-5299598a4dbc"));
        // 续接的轮次会再报一次同一个 id —— 不能每轮都当成新会话
        assert!(matches!(feed(&mut m, line), LineOutcome::Events(e) if e.is_empty()));
    }

    #[test]
    fn unknown_records_are_ignored_and_garbage_is_unparsable() {
        let mut m = mapper();
        // `entry_appended` only exists from pi 1.x; older and newer records
        // must not break the mapper.
        assert!(matches!(
            feed(&mut m, json!({"type":"entry_appended"})),
            LineOutcome::Events(e) if e.is_empty()
        ));
        assert!(matches!(
            feed(&mut m, json!({"type":"something_new_in_pi_2"})),
            LineOutcome::Events(e) if e.is_empty()
        ));
        assert!(matches!(m.map_line("not json"), LineOutcome::Unparsable));
        assert!(matches!(m.map_line("{\"no\":\"type\"}"), LineOutcome::Unparsable));
    }

    // ---- 工具调用:报文逐字取自 pi 1.1.0 实跑(bash + read)----

    fn bash_start() -> Value {
        json!({"type":"tool_execution_start","toolCallId":"toolu_bdrk_017nBpKWcazZUtp3Cno8Zyfe",
               "toolName":"bash","args":{"command":"echo hi > /tmp/pi-tool-sample.txt"}})
    }

    #[test]
    fn a_bash_call_becomes_a_tool_card() {
        let mut m = mapper();
        let events = match feed(&mut m, bash_start()) {
            LineOutcome::Events(e) => e,
            other => panic!("expected events, got {other:?}"),
        };
        match &events[..] {
            [AcpEvent::ToolCall {
                tool_call_id,
                tool_name,
                kind,
                status,
                raw_input,
                title,
                ..
            }] => {
                assert_eq!(tool_call_id, "toolu_bdrk_017nBpKWcazZUtp3Cno8Zyfe");
                assert_eq!(tool_name.as_deref(), Some("bash"));
                assert_eq!(kind, "execute");
                assert_eq!(status, "in_progress");
                assert_eq!(title, "echo hi > /tmp/pi-tool-sample.txt");
                assert!(raw_input.as_deref().unwrap().contains("echo hi"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_tool_result_settles_the_card() {
        let mut m = mapper();
        feed(&mut m, bash_start());
        let end = json!({"type":"tool_execution_end","toolCallId":"toolu_bdrk_017nBpKWcazZUtp3Cno8Zyfe",
            "toolName":"bash",
            "result":{"content":[{"type":"text","text":"(no output)"}],
                      "structuredContent":{"output":"","truncated":false,"exit_code":0,"wall_time_seconds":0}},
            "isError":false,"durationMs":6});
        match feed(&mut m, end) {
            LineOutcome::Events(e) => match &e[..] {
                [AcpEvent::ToolCallUpdate {
                    tool_call_id,
                    status,
                    raw_output,
                    ..
                }] => {
                    assert_eq!(tool_call_id, "toolu_bdrk_017nBpKWcazZUtp3Cno8Zyfe");
                    assert_eq!(status.as_deref(), Some("completed"));
                    assert_eq!(raw_output.as_deref(), Some("\"(no output)\""));
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_read_result_carries_the_file_text() {
        let mut m = mapper();
        let end = json!({"type":"tool_execution_end","toolCallId":"toolu_bdrk_018MnJYSMXoWvtPFkUFvgCcM",
            "toolName":"read","result":{"content":[{"type":"text","text":"hi\n"}],"structuredContent":"hi\n"},
            "isError":false,"durationMs":2});
        match feed(&mut m, end) {
            LineOutcome::Events(e) => match &e[..] {
                [AcpEvent::ToolCallUpdate { raw_output, .. }] => {
                    assert_eq!(raw_output.as_deref(), Some("\"hi\\n\""));
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_screenshot_result_carries_its_image() {
        // 浏览器截图:结果只有一张图、没有文字。以前只取文字,整条结果是空的。
        let mut m = mapper();
        let end = json!({"type":"tool_execution_end","toolCallId":"c9","toolName":"mcp__app_myclaw_browser",
            "result":{"content":[{"type":"image","data":"iVBORw0KGgo=","mimeType":"image/png"}]},
            "isError":false,"durationMs":900});
        match feed(&mut m, end) {
            LineOutcome::Events(e) => match &e[..] {
                [AcpEvent::ToolCallUpdate { images: Some(images), raw_output, status, .. }] => {
                    assert_eq!(status.as_deref(), Some("completed"));
                    assert_eq!(raw_output, &None);
                    assert_eq!(images.len(), 1);
                    assert_eq!(images[0].data, "iVBORw0KGgo=");
                    assert_eq!(images[0].mime_type, "image/png");
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_text_only_result_has_no_images() {
        let mut m = mapper();
        let end = json!({"type":"tool_execution_end","toolCallId":"c1","toolName":"read",
            "result":{"content":[{"type":"text","text":"hi"}]},"isError":false,"durationMs":1});
        match feed(&mut m, end) {
            LineOutcome::Events(e) => match &e[..] {
                [AcpEvent::ToolCallUpdate { images, .. }] => assert!(images.is_none()),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_failed_tool_is_marked_failed() {
        let mut m = mapper();
        let end = json!({"type":"tool_execution_end","toolCallId":"c1","toolName":"bash",
            "result":{"content":[{"type":"text","text":"boom"}]},"isError":true,"durationMs":1});
        match feed(&mut m, end) {
            LineOutcome::Events(e) => match &e[..] {
                [AcpEvent::ToolCallUpdate { status, .. }] => {
                    assert_eq!(status.as_deref(), Some("failed"))
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_empty_partial_result_makes_no_event() {
        // 实测的 bash 更新就是空的 —— 空更新会在界面上留一张没内容的卡。
        let mut m = mapper();
        let upd = json!({"type":"tool_execution_update","toolCallId":"c1","toolName":"bash",
            "args":{"command":"x"},"partialResult":{"content":[]}});
        assert!(matches!(feed(&mut m, upd), LineOutcome::Events(e) if e.is_empty()));
    }

    #[test]
    fn a_turn_that_ran_tools_is_not_a_failed_turn() {
        // 先失败一次、重试、这一轮跑了工具 —— 结算时不该还挂着上一次的错。
        let mut m = mapper();
        feed(&mut m, json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"error","errorMessage":"Connection error."}],"willRetry":true}));
        feed(&mut m, json!({"type":"auto_retry_start","attempt":1,"maxAttempts":3,"delayMs":2000,"errorMessage":"Connection error."}));
        feed(&mut m, bash_start());
        feed(&mut m, json!({"type":"agent_end","messages":[{"role":"assistant","stopReason":"stop"}],"willRetry":false}));
        match feed(&mut m, json!({"type":"agent_settled","aborted":false})) {
            LineOutcome::Finished { finish, .. } => {
                assert_eq!(finish.stop_reason, STOP_END_TURN);
                assert!(finish.error.is_none());
            }
            other => panic!("{other:?}"),
        }
    }
}
