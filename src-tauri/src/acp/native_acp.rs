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
    pub initial_backend: bool,
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

pub fn preparation_wait_advertised(agent: AgentType, meta: Option<&Map<String, Value>>) -> bool {
    agent == AgentType::OpenClaw
        && advertised(agent, meta)
        && meta
            .and_then(|meta| meta.get(CAPABILITY))
            .is_some_and(|cap| cap["preparationWait"] == 1)
}

pub fn token_for_turn(state: &SessionState, turn: &NativeAcpTurn) -> String {
    let identity = serde_json::to_vec(&(
        &state.connection_id,
        state.conversation_id,
        &turn.host_turn_id,
        &turn.native_turn_id,
    ))
    .expect("string turn identity is serializable");
    format!("acp:{:x}", Sha256::digest(identity))
}

/// Called at admission and again before the ordinary prompt reaches the peer.
/// Reusing this host must not reset a first-backend proof or native readiness.
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
    if state.native_steer_acp_host.as_deref() != Some(host.as_str()) {
        state.native_steer_acp_host = Some(host.clone());
        state.native_steer_acp_turn = None;
        state.native_steer_acp_first_turn = None;
        state.native_steer_v1_available = false;
        state.native_steer_v1_turn_id = None;
    }
    Some(json!({"version":1,"hostTurnId":host}))
}

/// Stop intent is not a synthetic terminal event. It fences preparation and
/// queued steering while the original ACP prompt drains its real terminal.
pub fn cancel_prompt(state: &mut SessionState) {
    if state.agent_type == AgentType::OpenClaw && state.turn_in_flight {
        state.native_steer_acp_cancelled_turn = Some(state.turns_completed);
    }
}

pub fn cancel_requested(state: &SessionState) -> bool {
    state.agent_type == AgentType::OpenClaw
        && state.native_steer_acp_cancelled_turn == Some(state.turns_completed)
}

fn identifier(value: &Value) -> Option<&str> {
    value
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 2048)
}

/// A peer's stderr is untrusted. Only this bounded, version-owned diagnostic
/// vocabulary may be promoted to INFO; the original line is never returned.
pub(super) fn parse_owned_diagnostic(line: &str) -> Option<Value> {
    if line.len() > 2048 {
        return None;
    }
    let raw = line.strip_prefix("[codeg-openclaw-native] ")?;
    let value: Value = serde_json::from_str(raw).ok()?;
    let fields = value.as_object()?;
    let phase = fields.get("phase")?.as_str()?;
    let (allowed, required): (&[&str], &[&str]) = match phase {
        "boot" => (&["phase", "event", "reason"], &["event"]),
        "initialize" => (
            &[
                "phase",
                "result",
                "elapsedMs",
                "reason",
                "code",
                "detailCode",
            ],
            &["result", "elapsedMs"],
        ),
        "prompt" => (
            &["phase", "supported", "validHostMeta", "tracking"],
            &["supported", "tracking"],
        ),
        "readiness" => (
            &[
                "phase",
                "turn",
                "ready",
                "reason",
                "elapsedMs",
                "code",
                "detailCode",
                "sessionKeyChanged",
                "activeRunMatches",
                "controllerMatches",
            ],
            &["turn", "ready", "reason", "elapsedMs"],
        ),
        "connection" => (
            &["phase", "event", "activeTurns"],
            &["event", "activeTurns"],
        ),
        _ => return None,
    };
    if fields.keys().any(|key| !allowed.contains(&key.as_str()))
        || required.iter().any(|key| !fields.contains_key(*key))
    {
        return None;
    }
    let mut safe = Map::new();
    for (key, value) in fields {
        let valid = match key.as_str() {
            "phase" => true, // The phase was matched to a static schema above.
            "supported" | "validHostMeta" | "tracking" | "ready" | "sessionKeyChanged"
            | "activeRunMatches" | "controllerMatches" => value.is_boolean(),
            "elapsedMs" => value.as_u64().is_some_and(|v| v <= 86_400_000),
            "turn" => value.as_u64().is_some_and(|v| v <= 1_000_000_000),
            "activeTurns" => value.as_u64().is_some_and(|v| v <= 10_000),
            "result" => matches!(
                value.as_str(),
                Some("ready" | "timeout" | "unavailable" | "unsupported_contract" | "failed")
            ),
            "code" => matches!(
                value.as_str(),
                Some("FORBIDDEN" | "UNAVAILABLE" | "INVALID_REQUEST" | "CLIENT_TIMEOUT")
            ),
            "detailCode" => matches!(
                value.as_str(),
                Some("MISSING_SCOPE" | "AUTHENTICATED_PROFILE_UNAVAILABLE")
            ),
            "event" if phase == "boot" => matches!(
                value.as_str(),
                Some("registered" | "skipped" | "patch_applied" | "guard_rejected")
            ),
            "event" => matches!(value.as_str(), Some("gateway_disconnect" | "shutdown")),
            "reason" if phase == "boot" => matches!(
                value.as_str(),
                Some("not_acp" | "node_unsupported" | "version_guard" | "bundle_guard")
            ),
            "reason" if phase == "initialize" => matches!(
                value.as_str(),
                Some(
                    "unsupported_runtime"
                        | "unsupported_bundle"
                        | "unsupported_exports"
                        | "load_failed"
                )
            ),
            "reason" => matches!(
                value.as_str(),
                Some(
                    "ready"
                        | "inactive_or_unowned"
                        | "backend_missing"
                        | "backend_unsupported"
                        | "backend_not_available"
                        | "authority_missing"
                        | "unsupported_contract"
                        | "rpc_timeout"
                        | "rpc_unavailable"
                        | "rpc_failed"
                )
            ),
            _ => false,
        };
        if !valid {
            return None;
        }
        safe.insert(key.clone(), value.clone());
    }
    Some(Value::Object(safe))
}

fn log_state_gate(state: &SessionState, reason: &'static str) {
    tracing::info!(
        connection_id = %state.connection_id,
        conversation_id = ?state.conversation_id,
        reason,
        advertised = state.native_steer_acp_v1,
        in_flight = state.turn_in_flight,
        ready = state.native_steer_v1_available,
        "[ACP][native_steer_v1] state notification"
    );
}

pub fn apply_state(state: &mut SessionState, value: &Value) {
    if !state.native_steer_acp_v1 {
        log_state_gate(state, "protocol_unadvertised");
        return;
    }
    if !state.turn_in_flight {
        log_state_gate(state, "not_in_flight");
        return;
    }
    if value["version"] != 1 {
        log_state_gate(state, "version_invalid");
        return;
    }
    let (Some(session), Some(host), Some(native)) = (
        identifier(&value["sessionId"]),
        identifier(&value["hostTurnId"]),
        identifier(&value["nativeTurnId"]),
    ) else {
        log_state_gate(state, "identifier_invalid");
        return;
    };
    if state.external_id.as_deref() != Some(session) {
        log_state_gate(state, "session_mismatch");
        return;
    }
    if state.native_steer_acp_host.as_deref() != Some(host) {
        log_state_gate(state, "host_mismatch");
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
            log_state_gate(state, "retired");
        } else {
            log_state_gate(state, "stale_retirement");
        }
    } else if value["ready"] == true {
        let turn = NativeAcpTurn {
            session_id: session.into(),
            host_turn_id: host.into(),
            native_turn_id: native.into(),
            initial_backend: state.native_steer_acp_preparation_wait
                && value["initialBackend"] == true,
        };
        let token = token_for_turn(state, &turn);
        if state.agent_type == AgentType::OpenClaw {
            state
                .native_steer_acp_first_turn
                .get_or_insert_with(|| turn.clone());
        }
        state.native_steer_acp_turn = Some(turn);
        state.native_steer_v1_turn_id = Some(token);
        state.native_steer_v1_available = true;
        log_state_gate(state, "ready_accepted");
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
        || cancel_requested(state)
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
