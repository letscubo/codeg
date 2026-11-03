//! Admission snapshots and bounded preparation waits. This module never sends
//! an ordinary prompt, starts an agent, or constructs application/database state.
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{timeout_at, Instant};

use super::{matches_turn, native_supported, NativeSteerInput};
use crate::acp::connection::{ConnectionCommand, SteerOutcome};
use crate::acp::error::AcpError;
use crate::acp::manager::ConnectionManager;
use crate::acp::native_acp;
use crate::acp::session_state::SessionState;
use crate::acp::types::PromptInputBlock;
use crate::models::agent::AgentType;
use crate::web::event_bridge::EventEmitter;

const CHECK_INTERVAL: Duration = Duration::from_millis(100);

enum Target {
    Exact(String),
    FirstNative {
        host: String,
        session: String,
        generation: u64,
        deadline: Instant,
    },
}

pub(super) struct Admission {
    state: Arc<RwLock<SessionState>>,
    commands: mpsc::Sender<ConnectionCommand>,
    emitter: EventEmitter,
    target: Target,
}

pub(super) struct Dispatched {
    pub state: Arc<RwLock<SessionState>>,
    pub emitter: EventEmitter,
    pub expected_turn_id: String,
    pub receiver: oneshot::Receiver<Result<SteerOutcome, AcpError>>,
}

fn scope_matches(state: &SessionState, input: &NativeSteerInput) -> bool {
    state.connection_id == input.connection_id
        && state.conversation_id == Some(input.expected_conversation_id)
        && state.agent_type.as_wire() == input.expected_agent_type
}

pub(super) async fn capture(
    manager: &ConnectionManager,
    input: &NativeSteerInput,
    budget: Duration,
) -> Result<Admission, &'static str> {
    // Match the existing map -> state lock order. The snapshot and a concurrent
    // first-ready transition cannot observe different halves of a native turn.
    let connections = manager.connections.lock().await;
    let connection = connections
        .get(&input.connection_id)
        .ok_or("connection_not_found")?;
    let state = connection.state.read().await;
    if !scope_matches(&state, input) {
        return Err("connection_scope_mismatch");
    }
    if state.deliberate_teardown || native_acp::cancel_requested(&state) {
        return Err("turn_changed");
    }
    let preparation = state.agent_type == AgentType::OpenClaw
        && state.native_steer_acp_v1
        && state.native_steer_acp_preparation_wait
        && state.turn_in_flight
        && state.native_steer_acp_host.as_deref() == Some(input.expected_turn_id.as_str());
    let target = if preparation {
        Target::FirstNative {
            host: input.expected_turn_id.clone(),
            session: state
                .external_id
                .clone()
                .filter(|id| !id.is_empty())
                .ok_or("turn_changed")?,
            generation: state.turns_completed,
            deadline: Instant::now() + budget,
        }
    } else {
        if !native_supported(&state) {
            return Err("native_steering_unsupported");
        }
        if !matches_turn(&state, &input.expected_turn_id) {
            return Err("turn_changed");
        }
        Target::Exact(input.expected_turn_id.clone())
    };
    let admission = Admission {
        state: Arc::clone(&connection.state),
        commands: connection.cmd_tx.clone(),
        emitter: connection.emitter.clone(),
        target,
    };
    // Also reject an already-retired or unproven first identity at admission.
    admission.resolve(&state, input)?;
    Ok(admission)
}

impl Admission {
    fn deadline(&self) -> Option<Instant> {
        match self.target {
            Target::FirstNative { deadline, .. } => Some(deadline),
            Target::Exact(_) => None,
        }
    }

    pub(super) fn waits_for_preparation(&self) -> bool {
        matches!(self.target, Target::FirstNative { .. })
    }

    fn resolve(
        &self,
        state: &SessionState,
        input: &NativeSteerInput,
    ) -> Result<Option<String>, &'static str> {
        if !scope_matches(state, input) {
            return Err("connection_scope_mismatch");
        }
        if state.deliberate_teardown || native_acp::cancel_requested(state) {
            return Err("turn_changed");
        }
        match &self.target {
            Target::Exact(expected) => {
                if !native_supported(state) || !matches_turn(state, expected) {
                    return Err("turn_changed");
                }
                Ok(Some(expected.clone()))
            }
            Target::FirstNative {
                host,
                session,
                generation,
                deadline,
            } => {
                if Instant::now() >= *deadline {
                    return Err("readiness_timeout");
                }
                if !state.turn_in_flight
                    || state.turns_completed != *generation
                    || state.native_steer_acp_host.as_deref() != Some(host)
                    || state.external_id.as_deref() != Some(session)
                    || !state.native_steer_acp_v1
                    || !state.native_steer_acp_preparation_wait
                {
                    return Err("turn_changed");
                }
                let Some(first) = &state.native_steer_acp_first_turn else {
                    return Ok(None);
                };
                let token = native_acp::token_for_turn(state, first);
                // initial_backend is supplied by the negotiated Gateway's
                // synchronous lifecycle observer, never inferred from polling.
                if !first.initial_backend
                    || first.host_turn_id != *host
                    || first.session_id != *session
                    || !native_supported(state)
                    || !matches_turn(state, &token)
                    || state.native_steer_acp_turn.as_ref().is_none_or(|now| {
                        now.native_turn_id != first.native_turn_id
                            || now.host_turn_id != *host
                            || now.session_id != *session
                    })
                {
                    return Err("turn_changed");
                }
                Ok(Some(token))
            }
        }
    }

    async fn current(
        &self,
        manager: &ConnectionManager,
        input: &NativeSteerInput,
    ) -> Result<Option<String>, &'static str> {
        let connections = manager.connections.lock().await;
        let connection = connections
            .get(&input.connection_id)
            .ok_or("connection_not_found")?;
        if !Arc::ptr_eq(&connection.state, &self.state)
            || !connection.cmd_tx.same_channel(&self.commands)
        {
            return Err("connection_scope_mismatch");
        }
        self.resolve(&*self.state.read().await, input)
    }

    async fn bounded<T>(
        &self,
        work: impl std::future::Future<Output = T>,
    ) -> Result<T, &'static str> {
        if let Some(deadline) = self.deadline() {
            timeout_at(deadline, work)
                .await
                .map_err(|_| "readiness_timeout")
        } else {
            Ok(work.await)
        }
    }

    pub(super) async fn dispatch(
        self,
        manager: &ConnectionManager,
        input: &NativeSteerInput,
        blocks: Vec<PromptInputBlock>,
    ) -> Result<Dispatched, &'static str> {
        loop {
            if self.bounded(self.current(manager, input)).await??.is_some() {
                break;
            }
            self.bounded(tokio::time::sleep(CHECK_INTERVAL)).await?;
        }
        // Capacity is reserved before either mutex is held. Cancellation and
        // the complete 120s budget still apply while a command channel is full.
        let permit = self
            .bounded(self.commands.reserve())
            .await?
            .map_err(|_| "connection_closed_before_dispatch")?;
        let connections = self.bounded(manager.connections.lock()).await?;
        let connection = connections
            .get(&input.connection_id)
            .ok_or("connection_not_found")?;
        if !Arc::ptr_eq(&connection.state, &self.state)
            || !connection.cmd_tx.same_channel(&self.commands)
        {
            return Err("connection_scope_mismatch");
        }
        let state = self.bounded(self.state.read()).await?;
        let expected_turn_id = self.resolve(&state, input)?.ok_or("turn_changed")?;
        let (reply, receiver) = oneshot::channel();
        // No await between the final host/native/cancel/map check and enqueue.
        // The driver checks this exact token again before touching its peer.
        permit.send(ConnectionCommand::NativeSteer {
            submission_id: input.submission_id.clone(),
            expected_turn_id: expected_turn_id.clone(),
            blocks,
            reply,
        });
        drop(state);
        drop(connections);
        Ok(Dispatched {
            state: self.state,
            emitter: self.emitter,
            expected_turn_id,
            receiver,
        })
    }
}
