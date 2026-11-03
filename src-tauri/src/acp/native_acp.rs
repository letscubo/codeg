//! Codeg-owned ACP native steering. Capability, active host/native turn and
//! consumption receipts are separate proofs; legacy feedback never opts in.
use sacp::{Agent, ConnectionTo, UntypedMessage};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use super::connection::{map_prompt_blocks, SteerOutcome};
use super::error::AcpError;
use super::session_state::SessionState;
use super::types::PromptInputBlock;
use crate::models::agent::AgentType;

pub const CAPABILITY: &str = "ai.myclaw/nativeSteering";
pub const PROMPT_META: &str = "ai.myclaw/nativeTurn";
pub const STATE_METHOD: &str = "_ai.myclaw/nativeSteeringState";
pub const STEER_METHOD: &str = "_ai.myclaw/steer";

#[derive(Debug, Clone)]
pub struct NativeAcpTurn {
    pub session_id: String,
    pub host_turn_id: String,
    pub native_turn_id: String,
}

pub fn advertised(agent: AgentType, meta: Option<&Map<String, Value>>) -> bool {
    if !matches!(agent, AgentType::OpenClaw | AgentType::Hermes) {
        return false;
    }
    let Some(capability) = meta.and_then(|meta| meta.get(CAPABILITY)) else {
        return false;
    };
    capability["version"] == 1
        && capability["activeOnly"] == true
        && capability["receipts"] == "consumed"
}

/// Called once for an admitted ordinary prompt, before it reaches the peer.
/// The host id remains stable even after readiness supplies a native token.
pub fn begin_prompt(state: &mut SessionState) -> Option<Value> {
    if !state.native_steer_acp_v1 || !state.turn_in_flight {
        return None;
    }
    let host = format!(
        "{}:{}:{}",
        state.connection_id,
        state.conversation_id.unwrap_or_default(),
        state.turns_completed
    );
    state.native_steer_acp_host = Some(host.clone());
    state.native_steer_acp_turn = None;
    state.native_steer_v1_available = false;
    state.native_steer_v1_turn_id = None;
    Some(json!({"version":1,"hostTurnId":host}))
}

fn identifier(value: &Value) -> Option<&str> {
    value
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 2048)
}

pub fn apply_state(state: &mut SessionState, value: &Value) {
    if !state.native_steer_acp_v1 || !state.turn_in_flight || value["version"] != 1 {
        return;
    }
    let (Some(session), Some(host), Some(native)) = (
        identifier(&value["sessionId"]),
        identifier(&value["hostTurnId"]),
        identifier(&value["nativeTurnId"]),
    ) else {
        return;
    };
    if state.external_id.as_deref() != Some(session)
        || state.native_steer_acp_host.as_deref() != Some(host)
    {
        return;
    }
    if value["ready"] == false {
        // A delayed retirement of native A cannot switch off a newer native B.
        if state
            .native_steer_acp_turn
            .as_ref()
            .is_some_and(|turn| turn.native_turn_id == native)
        {
            state.native_steer_v1_available = false;
            state.native_steer_v1_turn_id = None;
            state.native_steer_acp_turn = None;
        }
    } else if value["ready"] == true {
        let identity =
            serde_json::to_vec(&(&state.connection_id, state.conversation_id, host, native))
                .expect("string turn identity is serializable");
        let token = format!("acp:{:x}", Sha256::digest(identity));
        state.native_steer_acp_turn = Some(NativeAcpTurn {
            session_id: session.into(),
            host_turn_id: host.into(),
            native_turn_id: native.into(),
        });
        state.native_steer_v1_turn_id = Some(token);
        state.native_steer_v1_available = true;
    }
}

pub fn build_request(
    state: &SessionState,
    session_id: &str,
    expected_turn_id: &str,
    submission_id: &str,
    blocks: &[PromptInputBlock],
) -> Result<Value, AcpError> {
    let Some(turn) = &state.native_steer_acp_turn else {
        return Err(AcpError::NoActiveTurn);
    };
    if !state.native_steer_acp_v1
        || !state.native_steer_v1_available
        || !super::steering::matches_turn(state, expected_turn_id)
        || turn.session_id != session_id
        || state.native_steer_acp_host.as_deref() != Some(turn.host_turn_id.as_str())
    {
        return Err(AcpError::NoActiveTurn);
    }
    Ok(json!({
        "version":1,"sessionId":session_id,"hostTurnId":turn.host_turn_id,
        "expectedNativeTurnId":turn.native_turn_id,"submissionId":submission_id,
        "prompt":map_prompt_blocks(blocks.to_vec()),
    }))
}

pub fn parse_result(value: &Value, request: &Value) -> Result<SteerOutcome, AcpError> {
    if value["version"] != 1
        || value["submissionId"] != request["submissionId"]
        || value["hostTurnId"] != request["hostTurnId"]
        || value["nativeTurnId"] != request["expectedNativeTurnId"]
    {
        return Err(AcpError::protocol("native steer receipt identity mismatch"));
    }
    match value["outcome"].as_str() {
        Some("injected") => Ok(SteerOutcome::Injected),
        Some("not_injected") => Ok(SteerOutcome::PromptRequired),
        _ => Err(AcpError::protocol("native steer consumption unconfirmed")),
    }
}

pub async fn send(cx: ConnectionTo<Agent>, request: Value) -> Result<SteerOutcome, AcpError> {
    let message = UntypedMessage::new(STEER_METHOD, request.clone())
        .map_err(|error| AcpError::protocol(format!("invalid native steer request: {error}")))?;
    let result = cx
        .send_request_to(Agent, message)
        .block_task()
        .await
        .map_err(|error| {
            AcpError::protocol(format!("native steer receipt unavailable: {error}"))
        })?;
    parse_result(&result, &request)
}

/// Keep late B receipts from being appended after a successor C's prompt.
/// The reactor still handles cancellation while A runs. Only A's terminal
/// path briefly drains receipts before writing its TurnEnd/releasing admission.
pub struct ReceiptBatch(Arc<ReceiptState>);
struct ReceiptState {
    open: AtomicBool,
    pending: AtomicUsize,
    changed: tokio::sync::Notify,
    writer: tokio::sync::Mutex<()>,
}
pub struct ReceiptPermit(Arc<ReceiptState>);

impl Default for ReceiptBatch {
    fn default() -> Self {
        Self(Arc::new(ReceiptState {
            open: AtomicBool::new(true),
            pending: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
            writer: tokio::sync::Mutex::new(()),
        }))
    }
}
impl ReceiptBatch {
    pub fn start(&self) -> ReceiptPermit {
        self.0.pending.fetch_add(1, Ordering::AcqRel);
        ReceiptPermit(self.0.clone())
    }
    pub async fn finish(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let notified = self.0.changed.notified();
            if self.0.pending.load(Ordering::Acquire) == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }
        self.0.open.store(false, Ordering::Release);
        // Any writer that already admitted B completes before A's TurnEnd.
        let _writer = self.0.writer.lock().await;
    }
}
impl Drop for ReceiptBatch {
    fn drop(&mut self) {
        self.0.open.store(false, Ordering::Release);
    }
}
impl ReceiptPermit {
    pub async fn record(
        &self,
        agent: AgentType,
        session: &str,
        blocks: &[PromptInputBlock],
    ) -> bool {
        let _writer = self.0.writer.lock().await;
        if !self.0.open.load(Ordering::Acquire) {
            return false;
        }
        super::connection::record_prompt(agent, session, &map_prompt_blocks(blocks.to_vec())).await;
        true
    }
}
impl Drop for ReceiptPermit {
    fn drop(&mut self) {
        self.0.pending.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}

#[cfg(test)]
#[path = "native_acp_tests.rs"]
mod tests;
#[cfg(test)]
#[path = "native_acp_transport_tests.rs"]
mod transport_tests;
