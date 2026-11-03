//! The bundled runner's private, version-gated same-native-turn protocol.
//! Inbox insertion/claim is not delivery. Only its native user/message receipt
//! is accepted; EOF, lost output and mismatched turns remain ambiguous.
use std::collections::HashMap;

use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::acp::connection::SteerOutcome;
use crate::acp::error::AcpError;
use crate::acp::types::PromptInputBlock;

pub(super) struct Pending {
    pub blocks: Vec<PromptInputBlock>,
    pub reply: oneshot::Sender<Result<SteerOutcome, AcpError>>,
    turn: u64,
}

pub(super) enum Control {
    State(Option<u64>),
    Receipt(Pending, Result<SteerOutcome, AcpError>),
    Ignore,
}

#[derive(Default)]
pub(super) struct NativeBridge {
    pub turn: Option<u64>,
    pending: HashMap<String, Pending>,
}

impl NativeBridge {
    pub fn request(
        &mut self,
        session_id: &str,
        blocks: Vec<PromptInputBlock>,
        reply: oneshot::Sender<Result<SteerOutcome, AcpError>>,
    ) -> Option<String> {
        let text: Option<Vec<&str>> = blocks
            .iter()
            .map(|block| match block {
                PromptInputBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        let text = text.map(|texts| texts.join("\n"));
        let Some((turn, text)) = self.turn.zip(text).filter(|(_, text)| {
            !text.trim().is_empty() && text.chars().count() <= 16000 && self.pending.len() < 1024
        }) else {
            let _ = reply.send(Ok(SteerOutcome::PromptRequired));
            return None;
        };
        let id = uuid::Uuid::new_v4().to_string();
        let line = json!({"type":"codeg_native_steer","protocol":1,"sessionId":session_id,
            "id":id,"turn":turn,"text":text});
        self.pending.insert(
            id,
            Pending {
                blocks,
                reply,
                turn,
            },
        );
        Some(format!("{line}\n"))
    }

    pub fn control(&mut self, line: &str, session_id: Option<&str>) -> Option<Control> {
        let value: Value = serde_json::from_str(line).ok()?;
        let kind = value.get("type").and_then(Value::as_str)?;
        if kind != "codeg_native_state" && kind != "codeg_native_result" {
            return None;
        }
        let valid = value.get("protocol").and_then(Value::as_u64) == Some(1)
            && value.get("version").and_then(Value::as_str) == Some("0.1.6-alpha.2")
            && session_id.is_some()
            && value.get("sessionId").and_then(Value::as_str) == session_id;
        if kind == "codeg_native_state" {
            let turn = value
                .get("turn")
                .and_then(Value::as_u64)
                .filter(|turn| *turn <= 9_007_199_254_740_991);
            self.turn = turn
                .filter(|_| valid && value.get("supported").and_then(Value::as_bool) == Some(true));
            return Some(Control::State(self.turn));
        }
        if !valid {
            return Some(Control::Ignore);
        }
        let Some(pending) = value
            .get("id")
            .and_then(Value::as_str)
            .and_then(|id| self.pending.remove(id))
        else {
            return Some(Control::Ignore);
        };
        let outcome = if value.get("turn").and_then(Value::as_u64) != Some(pending.turn) {
            Err(AcpError::Protocol(
                "dsh steering receipt belongs to another native turn".into(),
            ))
        } else {
            match value.get("status").and_then(Value::as_str) {
                Some("injected") => Ok(SteerOutcome::Injected),
                Some("not_injected") => Ok(SteerOutcome::PromptRequired),
                _ => Err(AcpError::Protocol(
                    "dsh steering consumption is unconfirmed".into(),
                )),
            }
        };
        Some(Control::Receipt(pending, outcome))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(turn: u64) -> String {
        json!({"type":"codeg_native_state","protocol":1,"version":"0.1.6-alpha.2",
            "sessionId":"session-exact","turn":turn,"supported":true})
        .to_string()
    }

    #[test]
    fn myclaw_prompt_dsh_native_steering_versions_and_session_are_exact() {
        let mut bridge = NativeBridge::default();
        assert!(matches!(
            bridge.control(&state(7), Some("session-exact")),
            Some(Control::State(Some(7)))
        ));
        assert!(matches!(
            bridge.control(&state(7), Some("other")),
            Some(Control::State(None))
        ));
        let unknown = state(7).replace("0.1.6-alpha.2", "0.1.7");
        assert!(matches!(
            bridge.control(&unknown, Some("session-exact")),
            Some(Control::State(None))
        ));
        assert_eq!(bridge.turn, None);
    }

    #[test]
    fn myclaw_prompt_dsh_native_steering_receipt_keeps_exact_native_turn_and_blocks() {
        let mut bridge = NativeBridge::default();
        bridge.control(&state(7), Some("session-exact"));
        let (tx, _rx) = oneshot::channel();
        let blocks = vec![PromptInputBlock::Text { text: "B".into() }];
        let request: Value =
            serde_json::from_str(&bridge.request("session-exact", blocks, tx).unwrap()).unwrap();
        bridge.control(&state(8), Some("session-exact"));
        let receipt = json!({"type":"codeg_native_result","protocol":1,"version":"0.1.6-alpha.2",
            "sessionId":"session-exact","id":request["id"],"turn":7,"status":"injected"});
        let Some(Control::Receipt(pending, Ok(SteerOutcome::Injected))) =
            bridge.control(&receipt.to_string(), Some("session-exact"))
        else {
            panic!("missing exact receipt")
        };
        assert!(matches!(&pending.blocks[0], PromptInputBlock::Text { text } if text == "B"));
        assert!(matches!(
            bridge.control(&receipt.to_string(), Some("session-exact")),
            Some(Control::Ignore)
        ));
    }

    #[tokio::test]
    async fn myclaw_prompt_dsh_native_steering_missing_receipt_is_unknown_not_fallback() {
        let mut bridge = NativeBridge::default();
        bridge.control(&state(1), Some("session-exact"));
        let (tx, rx) = oneshot::channel();
        bridge.request(
            "session-exact",
            vec![PromptInputBlock::Text { text: "B".into() }],
            tx,
        );
        drop(bridge);
        assert!(
            rx.await.is_err(),
            "oneshot loss must stay ambiguous, never PromptRequired"
        );
    }

    #[test]
    fn myclaw_prompt_dsh_native_steering_wrong_turn_receipt_is_unconfirmed() {
        let mut bridge = NativeBridge::default();
        bridge.control(&state(1), Some("session-exact"));
        let (tx, _rx) = oneshot::channel();
        let request: Value = serde_json::from_str(
            &bridge
                .request(
                    "session-exact",
                    vec![PromptInputBlock::Text { text: "B".into() }],
                    tx,
                )
                .unwrap(),
        )
        .unwrap();
        let receipt = json!({"type":"codeg_native_result","protocol":1,"version":"0.1.6-alpha.2",
            "sessionId":"session-exact","id":request["id"],"turn":2,"status":"injected"});
        assert!(matches!(
            bridge.control(&receipt.to_string(), Some("session-exact")),
            Some(Control::Receipt(_, Err(AcpError::Protocol(_))))
        ));
    }
}
