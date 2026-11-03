//! Native-only, turn-bound steering. Submissions survive HTTP cancellation and
//! are never retried after an ambiguous delivery. The process epoch prevents a
//! restart from forgetting an accepted id and executing it a second time.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::connection::{ConnectionCommand, SteerOutcome};
use super::error::AcpError;
use super::manager::ConnectionManager;
use super::session_state::SessionState;
use super::types::{AcpEvent, PromptInputBlock};
use crate::web::event_bridge::emit_with_state_gated;

const MAX_SUBMISSIONS: usize = 8192;
const MAX_STEER_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSteeringState {
    pub server_epoch: String,
    pub conversation_id: Option<i32>,
    pub agent_type: Option<String>,
    pub native_supported: bool,
    pub active_turn_id: Option<String>,
    pub attachments_supported: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeSteerInput {
    pub connection_id: String,
    pub expected_conversation_id: i32,
    pub expected_agent_type: String,
    pub server_epoch: String,
    pub expected_turn_id: String,
    pub submission_id: String,
    pub text: String,
    #[serde(default)]
    pub blocks: Option<Vec<PromptInputBlock>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeSteerLookup {
    pub connection_id: String,
    pub expected_conversation_id: i32,
    pub expected_agent_type: String,
    pub server_epoch: String,
    pub submission_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeSteerStatus {
    Pending,
    Injected,
    NotInjected,
    Unconfirmed,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSteerResult {
    pub submission_id: String,
    pub state: NativeSteerStatus,
    pub reason: Option<String>,
}

struct Submission {
    fingerprint: Vec<u8>,
    connection_id: String,
    conversation_id: i32,
    agent_type: String,
    result: NativeSteerResult,
}

pub(crate) struct NativeSteeringLedger {
    epoch: String,
    submissions: Mutex<HashMap<String, Submission>>,
}

impl Default for NativeSteeringLedger {
    fn default() -> Self {
        Self {
            epoch: uuid::Uuid::new_v4().to_string(),
            submissions: Mutex::new(HashMap::new()),
        }
    }
}

fn native_supported(state: &SessionState) -> bool {
    // Legacy ACP feedback readiness does not prove the v1 receipt/history
    // contract. Each tested driver opts in explicitly while it is running.
    state.native_steer_v1_available
}

pub fn turn_id(state: &SessionState) -> String {
    if let Some(id) = &state.native_steer_v1_turn_id {
        return id.clone();
    }
    format!(
        "{}:{}:{}",
        state.connection_id,
        state.conversation_id.unwrap_or_default(),
        state.turns_completed
    )
}

pub fn matches_turn(state: &SessionState, expected: &str) -> bool {
    state.turn_in_flight && turn_id(state) == expected
}

fn result(id: &str, state: NativeSteerStatus, reason: &str) -> NativeSteerResult {
    NativeSteerResult {
        submission_id: id.to_string(),
        state,
        reason: (!reason.is_empty()).then(|| reason.to_string()),
    }
}

impl ConnectionManager {
    pub async fn native_steering_state(&self, connection_id: &str) -> NativeSteeringState {
        let state = self.get_state(connection_id).await;
        let guard = match state.as_ref() {
            Some(state) => Some(state.read().await),
            None => None,
        };
        NativeSteeringState {
            server_epoch: self.native_steering.epoch.clone(),
            conversation_id: guard.as_ref().and_then(|s| s.conversation_id),
            agent_type: guard.as_ref().map(|s| s.agent_type.as_wire().into_owned()),
            native_supported: guard.as_ref().is_some_and(|s| native_supported(s)),
            active_turn_id: guard
                .as_ref()
                .filter(|s| s.turn_in_flight)
                .map(|s| turn_id(s)),
            // The first contract deliberately accepts text only. Uploads stay
            // queued intact instead of losing their image/file content.
            attachments_supported: false,
        }
    }

    pub async fn native_steer_result(&self, input: NativeSteerLookup) -> NativeSteerResult {
        if input.server_epoch != self.native_steering.epoch {
            return result(
                &input.submission_id,
                NativeSteerStatus::Unconfirmed,
                "server_restarted",
            );
        }
        self.native_steering
            .submissions
            .lock()
            .await
            .get(&input.submission_id)
            .filter(|s| {
                s.connection_id == input.connection_id
                    && s.conversation_id == input.expected_conversation_id
                    && s.agent_type == input.expected_agent_type
            })
            .map(|s| s.result.clone())
            .unwrap_or_else(|| {
                result(
                    &input.submission_id,
                    NativeSteerStatus::Unconfirmed,
                    "submission_not_found",
                )
            })
    }

    pub async fn native_steer(&self, input: NativeSteerInput) -> Result<NativeSteerResult, String> {
        if input.server_epoch != self.native_steering.epoch {
            return Ok(result(
                &input.submission_id,
                NativeSteerStatus::Unconfirmed,
                "server_restarted",
            ));
        }
        if uuid::Uuid::parse_str(&input.submission_id).is_err() || input.expected_turn_id.is_empty()
        {
            return Err("invalid steering submission or turn id".into());
        }
        let blocks = input.blocks.clone().unwrap_or_else(|| {
            vec![PromptInputBlock::Text {
                text: input.text.clone(),
            }]
        });
        if input.text.trim().is_empty() || input.text.len() > MAX_STEER_BYTES || blocks.len() > 16 {
            return Err("invalid steering text".into());
        }
        if blocks
            .iter()
            .all(|b| matches!(b, PromptInputBlock::Text { .. }))
        {
            let delivered_text = blocks
                .iter()
                .filter_map(|b| match b {
                    PromptInputBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            if delivered_text != input.text {
                return Err("steering text and blocks differ".into());
            }
        }
        let payload = serde_json::to_vec(&(
            &input.connection_id,
            &input.expected_conversation_id,
            &input.expected_agent_type,
            &input.expected_turn_id,
            &input.text,
            &blocks,
        ))
        .map_err(|_| "invalid steering input")?;
        if payload.len() > MAX_STEER_BYTES {
            return Err("steering input too large".into());
        }
        let fingerprint = Sha256::digest(payload).to_vec();
        let key = input.submission_id.clone();
        let pending = result(&input.submission_id, NativeSteerStatus::Pending, "");
        {
            let mut ledger = self.native_steering.submissions.lock().await;
            if let Some(existing) = ledger.get(&key) {
                if existing.fingerprint != fingerprint {
                    return Err("submission_id_conflict".into());
                }
                return Ok(existing.result.clone());
            }
            // Never evict an accepted id: forgetting it could duplicate a steer.
            if ledger.len() >= MAX_SUBMISSIONS {
                return Err("steering ledger capacity reached".into());
            }
            ledger.insert(
                key.clone(),
                Submission {
                    fingerprint,
                    connection_id: input.connection_id.clone(),
                    conversation_id: input.expected_conversation_id,
                    agent_type: input.expected_agent_type.clone(),
                    result: pending.clone(),
                },
            );
        }
        let manager = self.clone_ref();
        // The ledger reservation and spawn have no intervening await. Once
        // admitted, a disconnected HTTP caller cannot abandon the dispatch.
        tokio::spawn(async move {
            let outcome = manager.dispatch_native_steer(&input, blocks).await;
            if let Some(entry) = manager
                .native_steering
                .submissions
                .lock()
                .await
                .get_mut(&key)
            {
                entry.result = outcome;
            }
        });
        Ok(pending)
    }

    async fn dispatch_native_steer(
        &self,
        input: &NativeSteerInput,
        blocks: Vec<PromptInputBlock>,
    ) -> NativeSteerResult {
        let rejected =
            |reason| result(&input.submission_id, NativeSteerStatus::NotInjected, reason);
        if blocks
            .iter()
            .any(|b| !matches!(b, PromptInputBlock::Text { .. }))
        {
            return rejected("attachments_not_supported");
        }
        let connection = {
            let connections = self.connections.lock().await;
            connections
                .get(&input.connection_id)
                .map(|c| (c.state.clone(), c.cmd_tx.clone(), c.emitter.clone()))
        };
        let Some((state, commands, emitter)) = connection else {
            return rejected("connection_not_found");
        };
        {
            let s = state.read().await;
            if s.conversation_id != Some(input.expected_conversation_id)
                || s.agent_type.as_wire() != input.expected_agent_type
            {
                return rejected("connection_scope_mismatch");
            }
            if !native_supported(&s) {
                return rejected("native_steering_unsupported");
            }
            if !matches_turn(&s, &input.expected_turn_id) {
                return rejected("turn_changed");
            }
        }
        let (reply, receiver) = tokio::sync::oneshot::channel();
        if commands
            .send(ConnectionCommand::NativeSteer {
                submission_id: input.submission_id.clone(),
                expected_turn_id: input.expected_turn_id.clone(),
                blocks,
                reply,
            })
            .await
            .is_err()
        {
            return rejected("connection_closed_before_dispatch");
        }
        match receiver.await {
            Ok(Ok(SteerOutcome::Injected)) => {
                emit_with_state_gated(
                    &state,
                    &emitter,
                    AcpEvent::FeedbackSubmitted {
                        item: super::feedback::FeedbackItem::new_delivered(
                            input.submission_id.clone(),
                            input.text.clone(),
                            chrono::Utc::now(),
                            None,
                        ),
                    },
                    |s| matches_turn(s, &input.expected_turn_id),
                )
                .await;
                result(&input.submission_id, NativeSteerStatus::Injected, "")
            }
            Ok(Ok(SteerOutcome::PromptRequired)) | Ok(Err(AcpError::NoActiveTurn)) => {
                rejected("turn_changed")
            }
            Ok(Ok(SteerOutcome::StartedNewTurn)) => {
                let mut state = state.write().await;
                state.native_steer_v1_available = false;
                result(
                    &input.submission_id,
                    NativeSteerStatus::Unconfirmed,
                    "native_contract_violated",
                )
            }
            // Unknown failures MUST NOT fall back to a second send.
            _ => result(
                &input.submission_id,
                NativeSteerStatus::Unconfirmed,
                "delivery_unconfirmed",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::agent::AgentType;
    use crate::web::event_bridge::EventEmitter;

    async fn fixture() -> (
        ConnectionManager,
        tokio::sync::mpsc::Receiver<ConnectionCommand>,
        NativeSteerInput,
    ) {
        let manager = ConnectionManager::new();
        let rx = manager
            .insert_test_connection_live("c", AgentType::Codex, None, EventEmitter::Noop)
            .await;
        let state = manager.get_state("c").await.unwrap();
        {
            let mut s = state.write().await;
            s.conversation_id = Some(42);
            s.turn_in_flight = true;
            s.native_steer_v1_available = true;
        }
        let status = manager.native_steering_state("c").await;
        let input = NativeSteerInput {
            connection_id: "c".into(),
            expected_conversation_id: 42,
            expected_agent_type: AgentType::Codex.as_wire().into_owned(),
            server_epoch: status.server_epoch,
            expected_turn_id: status.active_turn_id.unwrap(),
            submission_id: uuid::Uuid::new_v4().to_string(),
            text: "Focus on the failing test".into(),
            blocks: None,
        };
        (manager, rx, input)
    }
    fn lookup(input: &NativeSteerInput) -> NativeSteerLookup {
        NativeSteerLookup {
            connection_id: input.connection_id.clone(),
            expected_conversation_id: input.expected_conversation_id,
            expected_agent_type: input.expected_agent_type.clone(),
            server_epoch: input.server_epoch.clone(),
            submission_id: input.submission_id.clone(),
        }
    }
    async fn settled(manager: &ConnectionManager, input: &NativeSteerInput) -> NativeSteerResult {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let result = manager.native_steer_result(lookup(input)).await;
                if result.state != NativeSteerStatus::Pending {
                    return result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn myclaw_prompt_native_steer_deduplicates_and_survives_caller_cancellation() {
        let (manager, mut rx, input) = fixture().await;
        assert_eq!(
            manager.native_steer(input.clone()).await.unwrap().state,
            NativeSteerStatus::Pending
        );
        assert_eq!(
            manager.native_steer(input.clone()).await.unwrap().state,
            NativeSteerStatus::Pending
        );
        match rx.recv().await.unwrap() {
            ConnectionCommand::NativeSteer {
                submission_id,
                expected_turn_id,
                reply,
                ..
            } => {
                assert_eq!(expected_turn_id, input.expected_turn_id);
                assert_eq!(submission_id, input.submission_id);
                assert!(rx.try_recv().is_err());
                reply.send(Ok(SteerOutcome::Injected)).unwrap();
            }
            _ => panic!("only NativeSteer may be sent"),
        }
        assert_eq!(
            settled(&manager, &input).await.state,
            NativeSteerStatus::Injected
        );
        assert_eq!(
            manager.native_steer(input.clone()).await.unwrap().state,
            NativeSteerStatus::Injected
        );
        assert!(rx.try_recv().is_err());
        let state = manager.get_state("c").await.unwrap();
        assert!(
            state.read().await.turn_in_flight,
            "steering cannot finish the parent turn"
        );
        let mut rebound = input.clone();
        rebound.connection_id = "another-connection".into();
        assert!(manager
            .native_steer(rebound)
            .await
            .unwrap_err()
            .contains("conflict"));
        let mut changed = input;
        changed.text = "different payload".into();
        assert!(manager
            .native_steer(changed)
            .await
            .unwrap_err()
            .contains("conflict"));
    }

    #[tokio::test]
    async fn myclaw_prompt_native_steer_rejects_scope_and_changed_turn_before_dispatch() {
        let (manager, mut rx, mut input) = fixture().await;
        input.expected_conversation_id = 99;
        manager.native_steer(input.clone()).await.unwrap();
        assert_eq!(
            settled(&manager, &input).await.state,
            NativeSteerStatus::NotInjected
        );
        assert!(rx.try_recv().is_err());
        input.submission_id = uuid::Uuid::new_v4().to_string();
        input.expected_conversation_id = 42;
        manager
            .get_state("c")
            .await
            .unwrap()
            .write()
            .await
            .turns_completed += 1;
        manager.native_steer(input.clone()).await.unwrap();
        assert_eq!(
            settled(&manager, &input).await.state,
            NativeSteerStatus::NotInjected
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn myclaw_prompt_native_steer_unknown_delivery_never_becomes_retryable() {
        let (manager, mut rx, input) = fixture().await;
        manager.native_steer(input.clone()).await.unwrap();
        drop(rx.recv().await.unwrap());
        assert_eq!(
            settled(&manager, &input).await.state,
            NativeSteerStatus::Unconfirmed
        );
        assert_eq!(
            manager.native_steer(input.clone()).await.unwrap().state,
            NativeSteerStatus::Unconfirmed
        );
        let mut missing = lookup(&input);
        missing.submission_id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            manager.native_steer_result(missing).await.state,
            NativeSteerStatus::Unconfirmed
        );
        let mut restarted = input;
        restarted.server_epoch = "previous-process".into();
        assert_eq!(
            manager.native_steer(restarted).await.unwrap().state,
            NativeSteerStatus::Unconfirmed
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn myclaw_prompt_native_steer_never_uses_pull_or_drops_attachments() {
        let (manager, mut rx, mut input) = fixture().await;
        input.blocks = Some(vec![PromptInputBlock::Text {
            text: "a different direction".into(),
        }]);
        assert!(manager
            .native_steer(input.clone())
            .await
            .unwrap_err()
            .contains("differ"));
        assert!(rx.try_recv().is_err());
        input.blocks = None;
        {
            let state = manager.get_state("c").await.unwrap();
            let mut s = state.write().await;
            s.native_steer_v1_available = false;
            s.feedback_tool_available = true;
            s.native_steering_available = true;
        }
        manager.native_steer(input.clone()).await.unwrap();
        assert_eq!(
            settled(&manager, &input).await.reason.as_deref(),
            Some("native_steering_unsupported")
        );
        input.submission_id = uuid::Uuid::new_v4().to_string();
        input.blocks = Some(vec![PromptInputBlock::Image {
            data: "image".into(),
            mime_type: "image/png".into(),
            uri: None,
        }]);
        manager.native_steer(input.clone()).await.unwrap();
        assert_eq!(
            settled(&manager, &input).await.reason.as_deref(),
            Some("attachments_not_supported")
        );
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn myclaw_prompt_native_steer_each_runtime_requires_driver_readiness() {
        for agent in [
            AgentType::ClaudeCode,
            AgentType::OpenClaw,
            AgentType::Hermes,
            AgentType::DeepSeek,
            AgentType::Codex,
            AgentType::Pi,
        ] {
            let manager = ConnectionManager::new();
            let mut commands = manager
                .insert_test_connection_live("unready", agent, None, EventEmitter::Noop)
                .await;
            let state = manager.get_state("unready").await.unwrap();
            {
                let mut s = state.write().await;
                s.conversation_id = Some(42);
                s.turn_in_flight = true;
                // A legacy native/pull capability cannot prove that the
                // running driver implements the new consumption contract.
                s.native_steering_available = true;
                s.feedback_tool_available = true;
            }
            let capability = manager.native_steering_state("unready").await;
            assert!(!capability.native_supported, "{agent:?}");
            let input = NativeSteerInput {
                connection_id: "unready".into(),
                expected_conversation_id: 42,
                expected_agent_type: agent.as_wire().into_owned(),
                server_epoch: capability.server_epoch,
                expected_turn_id: capability.active_turn_id.unwrap(),
                submission_id: uuid::Uuid::new_v4().to_string(),
                text: "Keep this queued until the runtime is ready".into(),
                blocks: None,
            };
            manager.native_steer(input.clone()).await.unwrap();
            let receipt = settled(&manager, &input).await;
            assert_eq!(receipt.state, NativeSteerStatus::NotInjected, "{agent:?}");
            assert_eq!(
                receipt.reason.as_deref(),
                Some("native_steering_unsupported")
            );
            assert!(commands.try_recv().is_err(), "{agent:?}");
            let state = state.read().await;
            assert!(state.turn_in_flight, "{agent:?}");
            assert!(state.feedback.is_empty(), "{agent:?}");
        }
    }

    #[tokio::test]
    async fn myclaw_prompt_native_steer_cannot_cross_native_turns_within_one_host_turn() {
        let (manager, mut commands, mut input) = fixture().await;
        let state = manager.get_state("c").await.unwrap();
        state.write().await.native_steer_v1_turn_id = Some("c:42:0:native:first".into());
        input.expected_turn_id = manager
            .native_steering_state("c")
            .await
            .active_turn_id
            .unwrap();
        assert_eq!(input.expected_turn_id, "c:42:0:native:first");
        {
            let mut s = state.write().await;
            s.native_steer_v1_turn_id = Some("c:42:0:native:second".into());
            assert_eq!(s.turns_completed, 0);
            assert!(s.turn_in_flight);
        }
        manager.native_steer(input.clone()).await.unwrap();
        assert_eq!(
            settled(&manager, &input).await.reason.as_deref(),
            Some("turn_changed")
        );
        assert!(commands.try_recv().is_err());
        state.write().await.native_steer_v1_turn_id = None;
        assert_eq!(
            manager
                .native_steering_state("c")
                .await
                .active_turn_id
                .as_deref(),
            Some("c:42:0")
        );
        {
            let mut s = state.write().await;
            s.native_steer_v1_turn_id = Some("c:42:0:native:second".into());
            s.apply_event(&AcpEvent::TurnComplete {
                session_id: "native-session".into(),
                stop_reason: "end_turn".into(),
                agent_type: "codex".into(),
                duration_ms: None,
            });
            assert!(s.native_steer_v1_turn_id.is_none());
        }
        let capability = manager.native_steering_state("c").await;
        assert!(!capability.native_supported);
        assert!(capability.active_turn_id.is_none());
    }
}
