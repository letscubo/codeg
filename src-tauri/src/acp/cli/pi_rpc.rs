//! Pi 1.3.0 JSONL RPC steering receipts. A `queued` ACK is custody, not
//! consumption. Only a matching user message in this running subprocess proves
//! injection; after settlement `clear_queue` can prove the opposite.
//! Official contract: earendil-works/pi v1.3.0, docs/rpc-commands.md and
//! src/core/agent-session.ts (_queueUserInput / _handleAgentEvent).

use std::collections::BTreeMap;

use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::acp::connection::SteerOutcome;
use crate::acp::error::AcpError;
use crate::acp::types::PromptInputBlock;

type Reply = oneshot::Sender<Result<SteerOutcome, AcpError>>;

pub(super) fn user_message_text(value: &Value) -> Option<String> {
    if value["type"] != "message_start" || value["message"]["role"] != "user" {
        return None;
    }
    if let Some(text) = value["message"]["content"].as_str() {
        return Some(text.to_string());
    }
    value["message"]["content"].as_array().map(|blocks| {
        blocks
            .iter()
            .filter(|block| block["type"] == "text")
            .filter_map(|block| block["text"].as_str())
            .collect::<String>()
    })
}

struct Receipt {
    text: String,
    blocks: Vec<PromptInputBlock>,
    reply: Reply,
    acknowledged: bool,
    consumed: bool,
}

#[derive(Default)]
pub(super) struct PiSteeringReceipts {
    next_id: u64,
    pending: BTreeMap<String, Receipt>,
    clearing: bool,
}

impl PiSteeringReceipts {
    pub(super) fn enqueue(&mut self, blocks: Vec<PromptInputBlock>, reply: Reply) -> Option<Value> {
        let mut texts = Vec::new();
        for block in &blocks {
            match block {
                PromptInputBlock::Text { text } => texts.push(text.as_str()),
                _ => {
                    let _ = reply.send(Err(AcpError::NoActiveTurn));
                    return None;
                }
            }
        }
        let text = texts.join("\n");
        if text.trim().is_empty() {
            let _ = reply.send(Err(AcpError::NoActiveTurn));
            return None;
        }
        self.next_id += 1;
        let id = format!("codeg-steer-{:020}", self.next_id);
        let command = json!({"id": id, "type": "steer", "message": text});
        self.pending.insert(
            id,
            Receipt {
                text,
                blocks,
                reply,
                acknowledged: false,
                consumed: false,
            },
        );
        Some(command)
    }

    /// Returns prompts whose consumption was observed, for the existing CLI
    /// transcript recorder. Initial prompt events must be excluded by caller.
    pub(super) fn observe(&mut self, value: &Value) -> Vec<Vec<PromptInputBlock>> {
        let mut recorded = Vec::new();
        if let Some(text) = user_message_text(value) {
            if let Some(receipt) = self
                .pending
                .values_mut()
                .find(|receipt| !receipt.consumed && receipt.text == text)
            {
                receipt.consumed = true;
                recorded.push(receipt.blocks.clone());
            }
        }
        if value["type"] == "response" {
            if let Some(id) = value["id"].as_str() {
                if let Some(receipt) = self.pending.get_mut(id) {
                    if value["success"] == false {
                        if let Some(receipt) = self.pending.remove(id) {
                            let outcome = if receipt.consumed {
                                Ok(SteerOutcome::Injected)
                            } else {
                                Ok(SteerOutcome::PromptRequired)
                            };
                            let _ = receipt.reply.send(outcome);
                        }
                    } else if value["success"] == true {
                        receipt.acknowledged = true;
                    }
                }
            }
        }
        let consumed: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, receipt)| receipt.consumed && receipt.acknowledged)
            .map(|(id, _)| id.clone())
            .collect();
        for id in consumed {
            if let Some(receipt) = self.pending.remove(&id) {
                let _ = receipt.reply.send(Ok(SteerOutcome::Injected));
            }
        }
        recorded
    }

    /// Wait for in-flight handlers to acknowledge before clearing. Pi handles
    /// RPC commands asynchronously; clearing before an ACK could miss a late
    /// handler enqueueing its steer into an already idle runtime.
    pub(super) fn clear_after_settle(&mut self) -> Option<Value> {
        if self.clearing
            || self.pending.is_empty()
            || self.pending.values().any(|receipt| !receipt.acknowledged)
        {
            return None;
        }
        self.clearing = true;
        Some(json!({"id":"codeg-clear-steering", "type":"clear_queue"}))
    }

    pub(super) fn observe_clear(&mut self, value: &Value) -> bool {
        if value["type"] != "response" || value["id"] != "codeg-clear-steering" {
            return false;
        }
        let mut remaining = value["data"]["steering"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for (_, receipt) in std::mem::take(&mut self.pending) {
            let index = remaining
                .iter()
                .position(|text| text.as_str() == Some(&receipt.text));
            let outcome = if receipt.consumed {
                Ok(SteerOutcome::Injected)
            } else if value["success"] == true && index.is_some() {
                remaining.remove(index.unwrap());
                Ok(SteerOutcome::PromptRequired)
            } else {
                Err(AcpError::Protocol(
                    "pi_steering_delivery_unconfirmed".into(),
                ))
            };
            let _ = receipt.reply.send(outcome);
        }
        true
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn unconfirmed(&mut self) {
        for (_, receipt) in std::mem::take(&mut self.pending) {
            let _ = receipt.reply.send(Err(AcpError::Protocol(
                "pi_steering_delivery_unconfirmed".into(),
            )));
        }
    }
}

impl Drop for PiSteeringReceipts {
    fn drop(&mut self) {
        self.unconfirmed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(
        receipts: &mut PiSteeringReceipts,
        text: &str,
    ) -> (Value, oneshot::Receiver<Result<SteerOutcome, AcpError>>) {
        let (reply, receiver) = oneshot::channel();
        let command = receipts
            .enqueue(vec![PromptInputBlock::Text { text: text.into() }], reply)
            .unwrap();
        (command, receiver)
    }

    #[test]
    fn myclaw_prompt_pi_rpc_ack_is_not_injection_and_unicode_is_framed() {
        let mut receipts = PiSteeringReceipts::default();
        let (command, mut receiver) = add(&mut receipts, "日本語\nالعربية\u{2028}new");
        assert_eq!(command["type"], "steer");
        assert!(!command.to_string().contains('\n'));
        receipts.observe(&json!({"type":"response","id":command["id"],"success":true,"data":{"disposition":"queued"}}));
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let recorded = receipts.observe(&json!({"type":"message_start","message":{"role":"user","content":[{"type":"text","text":command["message"]}]}}));
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            receiver.try_recv().unwrap().unwrap(),
            SteerOutcome::Injected
        );
    }

    #[test]
    fn myclaw_prompt_pi_rpc_settle_race_recovers_only_proven_queue_items() {
        let mut receipts = PiSteeringReceipts::default();
        let (command, mut receiver) = add(&mut receipts, "next direction");
        assert!(
            receipts.clear_after_settle().is_none(),
            "late handler must acknowledge first"
        );
        receipts.observe(&json!({"type":"response","id":command["id"],"success":true}));
        assert_eq!(
            receipts.clear_after_settle().unwrap()["type"],
            "clear_queue"
        );
        assert!(receipts.observe_clear(&json!({"type":"response","id":"codeg-clear-steering","success":true,"data":{"steering":["next direction"]}})));
        assert_eq!(
            receiver.try_recv().unwrap().unwrap(),
            SteerOutcome::PromptRequired
        );
    }

    #[test]
    fn myclaw_prompt_pi_rpc_unknown_or_transformed_delivery_never_requeues() {
        let mut receipts = PiSteeringReceipts::default();
        let (command, mut receiver) = add(&mut receipts, "direction");
        receipts.observe(&json!({"type":"response","id":command["id"],"success":true,"data":{"disposition":"handled"}}));
        receipts.clear_after_settle();
        receipts.observe_clear(&json!({"type":"response","id":"codeg-clear-steering","success":true,"data":{"steering":["transformed direction"]}}));
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Err(AcpError::Protocol(_))
        ));
    }

    #[test]
    fn myclaw_prompt_pi_rpc_duplicate_messages_have_distinct_receipts() {
        let mut receipts = PiSteeringReceipts::default();
        let (a, mut ar) = add(&mut receipts, "same");
        let (b, mut br) = add(&mut receipts, "same");
        assert_ne!(a["id"], b["id"]);
        receipts.observe(&json!({"type":"response","id":a["id"],"success":true}));
        receipts.observe(&json!({"type":"response","id":b["id"],"success":true}));
        receipts
            .observe(&json!({"type":"message_start","message":{"role":"user","content":"same"}}));
        assert_eq!(ar.try_recv().unwrap().unwrap(), SteerOutcome::Injected);
        assert!(br.try_recv().is_err());
        receipts.unconfirmed();
        assert!(matches!(br.try_recv().unwrap(), Err(AcpError::Protocol(_))));
    }
}
