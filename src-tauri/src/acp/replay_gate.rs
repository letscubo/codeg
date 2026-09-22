//! Separate a `session/load` / `session/resume` history replay from live output,
//! using the RPC response itself as the boundary.
//!
//! ## The problem
//!
//! ACP requires an agent that restores a session to stream the prior
//! conversation as `session/update` notifications **before** it answers the
//! restoring request (hermes, pi, openclaw and claude-agent-acp all do this;
//! hermes does it for `session/resume` too, not only `session/load`).
//!
//! Those notifications arrive before codeg has attached the session, so sacp's
//! default client handler parks them (`retry: true`) and hands them to the
//! session's handler the moment `attach_session` registers it. Whatever is
//! parked therefore lands in the live update stream, and the next prompt's
//! conversation loop reads it as that turn's output: the transcript records the
//! history a second time and a channel relay (Telegram) re-sends old replies.
//!
//! The previous defence guessed where the replay ended — drain until 100ms pass
//! with nothing new — and only on `session/load`; the resume path assumed no
//! replay at all. Measured 2026-09-22 on a hermes 0.21.2 instance: an idle-swept
//! connection was re-created, `session/resume` replayed five old exchanges, and
//! all of them were pushed to the user's Telegram chat as the new answer.
//!
//! ## The fix: the response is the boundary
//!
//! sacp's incoming actor dispatches every message from the agent in wire order
//! through one loop, and a dynamic handler is offered responses as well as
//! notifications. So [`ReplayGate`] is registered **before** the restoring
//! request is sent and:
//!
//! - until then, captures this session's notifications — by the spec, and by
//!   wire order, those are the replay;
//! - closes the capture the instant it sees the response to the restoring
//!   method, passing the response on untouched so the caller still receives it.
//!
//! When the caller's request future resolves, the actor has already dispatched
//! every captured notification, so [`ReplayGuard::finish`] returns the complete
//! replay with no waiting and no timing guess. Anything the agent sends after
//! its response is live and flows to the session as usual.
//!
//! Two things always pass through, even before the response:
//!
//! - requests — a replay is notifications only, and swallowing a request would
//!   leave the agent blocked on a reply that never comes;
//! - **state** updates (`current_mode_update`, `config_option_update`,
//!   `available_commands_update`, `usage_update`, `session_info_update`). They
//!   describe the session as it is now, not its history — openclaw's resume
//!   sends its mode/config snapshot this way before answering, and dropping it
//!   would leave the UI without them. Everything else on `session/update`
//!   (message/thought chunks, tool calls, plans, and any kind codeg does not
//!   know) is treated as history, as are extension notifications for the
//!   session — matching what the old drain discarded.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use sacp::{Agent, ConnectionTo, Dispatch, HandleDispatchFrom, Handled};

/// What the gate does with one incoming message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateAction {
    /// A replayed notification for this session: keep it out of the live stream.
    Capture,
    /// The response to the restoring request: close the gate, let it through.
    CloseAndPass,
    /// Anything else (other sessions, requests, the gate already closed).
    Pass,
}

/// `session/update` kinds that carry current session state rather than history.
const STATE_UPDATE_KINDS: &[&str] = &[
    "current_mode_update",
    "config_option_update",
    "available_commands_update",
    "usage_update",
    "session_info_update",
];

/// Pure decision, split out so the rules are testable without a connection.
///
/// `update_kind` is `params.update.sessionUpdate` of a `session/update`
/// notification (`None` for anything else).
pub(crate) fn decide(
    closed: bool,
    is_notification: bool,
    is_response: bool,
    method: &str,
    restoring_method: &str,
    for_this_session: bool,
    update_kind: Option<&str>,
) -> GateAction {
    if closed {
        return GateAction::Pass;
    }
    if is_response && method == restoring_method {
        return GateAction::CloseAndPass;
    }
    if !is_notification || !for_this_session {
        return GateAction::Pass;
    }
    if method == "session/update" && update_kind.is_some_and(|k| STATE_UPDATE_KINDS.contains(&k)) {
        return GateAction::Pass;
    }
    GateAction::Capture
}

fn session_id_of(message: &Dispatch) -> Option<&str> {
    message
        .message()
        .and_then(|m| m.params().get("sessionId"))
        .and_then(|v| v.as_str())
}

fn update_kind_of(message: &Dispatch) -> Option<&str> {
    message
        .message()
        .and_then(|m| m.params().get("update"))
        .and_then(|u| u.get("sessionUpdate"))
        .and_then(|v| v.as_str())
}

struct ReplayGate {
    session_id: String,
    restoring_method: &'static str,
    closed: Arc<AtomicBool>,
    captured: Arc<Mutex<Vec<Dispatch>>>,
}

impl HandleDispatchFrom<Agent> for ReplayGate {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _cx: ConnectionTo<Agent>,
    ) -> Result<Handled<Dispatch>, sacp::Error> {
        let action = decide(
            self.closed.load(Ordering::Acquire),
            matches!(message, Dispatch::Notification(_)),
            matches!(message, Dispatch::Response(..)),
            message.method(),
            self.restoring_method,
            session_id_of(&message) == Some(self.session_id.as_str()),
            update_kind_of(&message),
        );
        match action {
            GateAction::Capture => {
                if let Ok(mut captured) = self.captured.lock() {
                    captured.push(message);
                }
                Ok(Handled::Yes)
            }
            GateAction::CloseAndPass => {
                self.closed.store(true, Ordering::Release);
                Ok(Handled::No {
                    message,
                    retry: false,
                })
            }
            GateAction::Pass => Ok(Handled::No {
                message,
                retry: false,
            }),
        }
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        format!(
            "ReplayGate({} until {} response)",
            self.session_id, self.restoring_method
        )
    }
}

/// Armed gate for one restoring request. Dropping it removes the gate.
pub(crate) struct ReplayGuard {
    closed: Arc<AtomicBool>,
    captured: Arc<Mutex<Vec<Dispatch>>>,
    /// sacp's registration guard (removes the gate on drop). Held type-erased:
    /// the type is not re-exported by sacp, and only its `Drop` matters here.
    _registration: Box<dyn Send>,
}

impl ReplayGuard {
    /// Register the gate. Call BEFORE sending `restoring_method` for `session_id`.
    ///
    /// `None` only if the connection's actor is already gone, in which case the
    /// request that follows fails anyway.
    pub(crate) fn arm(
        cx: &ConnectionTo<Agent>,
        session_id: &str,
        restoring_method: &'static str,
    ) -> Option<Self> {
        let closed = Arc::new(AtomicBool::new(false));
        let captured = Arc::new(Mutex::new(Vec::new()));
        let registration = cx
            .add_dynamic_handler(ReplayGate {
                session_id: session_id.to_string(),
                restoring_method,
                closed: Arc::clone(&closed),
                captured: Arc::clone(&captured),
            })
            .map_err(|e| {
                tracing::warn!("[ACP] could not arm replay gate for {session_id}: {e}");
            })
            .ok()?;
        Some(Self {
            closed,
            captured,
            _registration: Box::new(registration),
        })
    }

    /// Close the gate (if the response did not already) and return the replay,
    /// in wire order. Call after the restoring request has resolved.
    pub(crate) fn finish(self) -> Vec<Dispatch> {
        self.closed.store(true, Ordering::Release);
        self.captured
            .lock()
            .map(|mut c| std::mem::take(&mut *c))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESUME: &str = "session/resume";

    #[test]
    fn captures_this_sessions_notifications_while_open() {
        assert_eq!(
            decide(false, true, false, "session/update", RESUME, true, Some("agent_message_chunk")),
            GateAction::Capture
        );
    }

    #[test]
    fn the_restoring_response_closes_the_gate_and_passes() {
        assert_eq!(
            decide(false, false, true, RESUME, RESUME, false, None),
            GateAction::CloseAndPass
        );
    }

    #[test]
    fn a_different_response_does_not_close_the_gate() {
        // e.g. the answer to an unrelated request that happens to land mid-replay
        assert_eq!(
            decide(false, false, true, "session/set_mode", RESUME, false, None),
            GateAction::Pass
        );
    }

    #[test]
    fn nothing_is_captured_after_the_gate_closes() {
        // live output after the response must reach the session
        assert_eq!(
            decide(true, true, false, "session/update", RESUME, true, Some("agent_message_chunk")),
            GateAction::Pass
        );
    }

    #[test]
    fn other_sessions_pass_through() {
        assert_eq!(
            decide(false, true, false, "session/update", RESUME, false, Some("agent_message_chunk")),
            GateAction::Pass
        );
    }

    #[test]
    fn requests_are_never_captured() {
        // swallowing a request would leave the agent waiting forever
        assert_eq!(
            decide(false, false, false, "session/request_permission", RESUME, true, None),
            GateAction::Pass
        );
    }

    #[test]
    fn state_updates_pass_before_the_response() {
        // openclaw's resume snapshot (mode / config) must reach the UI
        for kind in STATE_UPDATE_KINDS {
            assert_eq!(
                decide(false, true, false, "session/update", RESUME, true, Some(kind)),
                GateAction::Pass,
                "{kind}"
            );
        }
    }

    #[test]
    fn history_kinds_and_unknown_kinds_are_captured() {
        for kind in [
            "user_message_chunk",
            "agent_message_chunk",
            "agent_thought_chunk",
            "tool_call",
            "tool_call_update",
            "plan",
            "some_future_kind",
        ] {
            assert_eq!(
                decide(false, true, false, "session/update", RESUME, true, Some(kind)),
                GateAction::Capture,
                "{kind}"
            );
        }
    }

    #[test]
    fn extension_notifications_for_the_session_are_captured() {
        // the old drain discarded these too (a past compaction alert is not news)
        assert_eq!(
            decide(false, true, false, "_grok/alert", RESUME, true, None),
            GateAction::Capture
        );
    }

    /// End to end over sacp's in-memory transport: a fake agent answers
    /// `session/resume` the way hermes does — history first, then the response,
    /// then live output. The gate must hold exactly the history; the state
    /// update and the live notification must reach the attached session.
    #[tokio::test]
    async fn gate_splits_replay_from_live_output_at_the_response() {
        use futures::{SinkExt as _, StreamExt as _};
        use sacp::schema::NewSessionResponse;
        use sacp::{Client, SessionMessage, UntypedMessage};

        let (client_end, mut agent_end) = sacp::Channel::duplex();

        let update = |kind: &str, text: &str| {
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": { "sessionId": "s1", "update": {
                    "sessionUpdate": kind,
                    "content": { "type": "text", "text": text }
                }}
            })
        };
        let mode = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": { "sessionId": "s1", "update": {
                "sessionUpdate": "current_mode_update", "currentModeId": "default"
            }}
        });

        // Fake agent: wait for the resume request, replay, respond, go live.
        let agent = tokio::spawn(async move {
            let req = agent_end.rx.next().await.unwrap().unwrap();
            let id = serde_json::to_value(&req).unwrap()["id"].clone();
            let send = |v: serde_json::Value| Ok(serde_json::from_value(v).unwrap());
            let mut tx = agent_end.tx.clone();
            tx.send(send(update("user_message_chunk", "old question"))).await.unwrap();
            tx.send(send(mode.clone())).await.unwrap();
            tx.send(send(update("agent_message_chunk", "old answer"))).await.unwrap();
            tx.send(send(serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": {} })))
                .await
                .unwrap();
            tx.send(send(update("agent_message_chunk", "live answer"))).await.unwrap();
            // keep the transport open until the client is done
            let _ = agent_end.rx.next().await;
        });

        Client
            .builder()
            .connect_with(client_end, async move |cx| -> Result<(), sacp::Error> {
                let gate = ReplayGuard::arm(&cx, "s1", "session/resume").expect("armed");
                let req = UntypedMessage::new(
                    "session/resume",
                    serde_json::json!({ "sessionId": "s1", "cwd": "/" }),
                )?;
                cx.send_request_to(sacp::Agent, req).block_task().await?;

                let replayed = gate.finish();
                let kinds: Vec<String> = replayed.iter().map(|d| update_kind_of(d).unwrap().to_string()).collect();
                assert_eq!(kinds, ["user_message_chunk", "agent_message_chunk"], "replay held exactly");

                let mut session = cx.attach_session(
                    NewSessionResponse::new(sacp::schema::SessionId::new("s1")),
                    Default::default(),
                )?;
                let mut seen = Vec::new();
                for _ in 0..2 {
                    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), session.read_update())
                        .await
                        .expect("update arrives")?;
                    if let SessionMessage::SessionMessage(d) = msg {
                        seen.push(update_kind_of(&d).unwrap().to_string());
                    }
                }
                assert_eq!(seen, ["current_mode_update", "agent_message_chunk"], "state + live only");
                Ok(())
            })
            .await
            .expect("client run");
        agent.abort();
    }
}
