//! No DB/AppState/runtime/SDK: real manager admission, private ACP state
//! notifications and in-memory command channels with synthetic consumption.
use super::*;
use crate::acp::native_acp;
use crate::models::agent::AgentType;
use crate::web::event_bridge::EventEmitter;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

struct Fixture {
    manager: ConnectionManager,
    commands: mpsc::Receiver<ConnectionCommand>,
    state: Arc<RwLock<SessionState>>,
    input: NativeSteerInput,
}

impl Fixture {
    async fn new() -> Self {
        let manager = ConnectionManager::new();
        let commands = manager
            .insert_test_connection_live("prepare", AgentType::OpenClaw, None, EventEmitter::Noop)
            .await;
        let state = manager.get_state("prepare").await.unwrap();
        {
            let mut state = state.write().await;
            state.conversation_id = Some(209);
            state.external_id = Some("acp-A".into());
            state.turn_in_flight = true;
            state.native_steer_acp_v1 = true;
            state.native_steer_acp_preparation_wait = true;
            native_acp::begin_prompt(&mut state).unwrap();
        }
        let status = manager.native_steering_state("prepare").await;
        assert!(!status.native_supported);
        let input = NativeSteerInput {
            connection_id: "prepare".into(),
            expected_conversation_id: 209,
            expected_agent_type: "open_claw".into(),
            server_epoch: status.server_epoch,
            expected_turn_id: status.active_turn_id.unwrap(),
            submission_id: uuid::Uuid::new_v4().to_string(),
            text: "B should steer only A".into(),
            blocks: None,
        };
        Self {
            manager,
            commands,
            state,
            input,
        }
    }

    fn notification(&self, native: &str, ready: bool, initial: bool) -> serde_json::Value {
        json!({"version":1,"sessionId":"acp-A","hostTurnId":self.input.expected_turn_id,
            "nativeTurnId":native,"ready":ready,"initialBackend":initial})
    }

    async fn ready(&self) {
        native_acp::apply_state(
            &mut *self.state.write().await,
            &self.notification("native-1", true, true),
        );
    }

    async fn submit(&self) {
        assert_eq!(
            self.manager
                .native_steer(self.input.clone())
                .await
                .unwrap()
                .state,
            NativeSteerStatus::Pending
        );
    }

    async fn receipt(&self) -> NativeSteerResult {
        self.manager
            .native_steer_result(NativeSteerLookup {
                connection_id: self.input.connection_id.clone(),
                expected_conversation_id: 209,
                expected_agent_type: self.input.expected_agent_type.clone(),
                server_epoch: self.input.server_epoch.clone(),
                submission_id: self.input.submission_id.clone(),
            })
            .await
    }

    async fn settled(&self) -> NativeSteerResult {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let result = self.receipt().await;
                if result.state != NativeSteerStatus::Pending {
                    return result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    async fn fill_channel(&self) {
        let commands = self
            .manager
            .connections
            .lock()
            .await
            .get("prepare")
            .unwrap()
            .cmd_tx
            .clone();
        for _ in 0..4 {
            commands
                .try_send(ConnectionCommand::SetMode {
                    mode_id: "fake-capacity".into(),
                })
                .unwrap();
        }
    }

    async fn consume(&mut self, outcome: Result<SteerOutcome, AcpError>) -> String {
        let command = tokio::time::timeout(Duration::from_secs(1), self.commands.recv())
            .await
            .unwrap()
            .unwrap();
        match command {
            ConnectionCommand::NativeSteer {
                submission_id,
                expected_turn_id,
                blocks,
                reply,
            } => {
                assert_eq!(submission_id, self.input.submission_id);
                assert_eq!(blocks.len(), 1);
                assert!(
                    matches!(&blocks[0], PromptInputBlock::Text { text } if text == &self.input.text)
                );
                reply.send(outcome).unwrap();
                expected_turn_id
            }
            _ => panic!("preparation admission may only dispatch NativeSteer"),
        }
    }
}

fn finish(state: &mut SessionState) {
    state.apply_event(&AcpEvent::TurnComplete {
        session_id: "acp-A".into(),
        stop_reason: "end_turn".into(),
        agent_type: "open_claw".into(),
        duration_ms: None,
    });
}

#[tokio::test(start_paused = true)]
async fn myclaw_prompt_openclaw_preparation_waits_then_injects_once_and_keeps_its_receipt() {
    let mut h = Fixture::new().await;
    h.submit().await;
    assert_eq!(
        h.manager.native_steer(h.input.clone()).await.unwrap().state,
        NativeSteerStatus::Pending
    );
    tokio::time::advance(Duration::from_secs(107)).await;
    assert!(h.commands.try_recv().is_err());
    assert_eq!(h.receipt().await.state, NativeSteerStatus::Pending);
    h.ready().await;
    let token = h.consume(Ok(SteerOutcome::Injected)).await;
    assert!(token.starts_with("acp:"));
    assert_ne!(token, h.input.expected_turn_id);
    assert_eq!(h.settled().await.state, NativeSteerStatus::Injected);
    assert!(h.state.read().await.turn_in_flight);
    finish(&mut *h.state.write().await);
    assert_eq!(
        h.manager.native_steer(h.input.clone()).await.unwrap().state,
        NativeSteerStatus::Injected,
        "dedupe must precede today's mutable admission state"
    );
    assert!(h.commands.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_cached_host_atomically_binds_an_already_ready_first_native(
) {
    let mut h = Fixture::new().await;
    h.ready().await;
    h.submit().await;
    let token = h.consume(Ok(SteerOutcome::Injected)).await;
    assert_eq!(
        h.state.read().await.native_steer_v1_turn_id.as_deref(),
        Some(token.as_str())
    );
    assert_eq!(h.settled().await.state, NativeSteerStatus::Injected);
    assert!(h.commands.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_cannot_follow_first_native_retirement_or_missed_first_backend(
) {
    for missed in [false, true] {
        let mut h = Fixture::new().await;
        h.submit().await;
        {
            let mut state = h.state.write().await;
            if !missed {
                native_acp::apply_state(&mut state, &h.notification("native-1", true, true));
                native_acp::apply_state(&mut state, &h.notification("native-1", false, true));
            }
            native_acp::apply_state(&mut state, &h.notification("native-2", true, false));
        }
        assert_eq!(h.settled().await.state, NativeSteerStatus::NotInjected);
        assert!(h.commands.try_recv().is_err());
        assert!(h.state.read().await.turn_in_flight);
        // A separate explicit admission against native-2 still uses the normal
        // exact-token contract; only the old preparation waiter is fenced.
        h.input.submission_id = uuid::Uuid::new_v4().to_string();
        h.input.expected_turn_id = turn_id(&*h.state.read().await);
        h.submit().await;
        h.consume(Ok(SteerOutcome::Injected)).await;
        assert_eq!(h.settled().await.state, NativeSteerStatus::Injected);
    }
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_rechecks_first_native_after_channel_backpressure() {
    let mut h = Fixture::new().await;
    h.fill_channel().await;
    h.ready().await;
    h.submit().await;
    tokio::task::yield_now().await;
    {
        let mut state = h.state.write().await;
        native_acp::apply_state(&mut state, &h.notification("native-1", false, true));
        native_acp::apply_state(&mut state, &h.notification("native-2", true, false));
    }
    for _ in 0..4 {
        assert!(matches!(
            h.commands.recv().await,
            Some(ConnectionCommand::SetMode { .. })
        ));
    }
    assert_eq!(h.settled().await.state, NativeSteerStatus::NotInjected);
    assert!(h.commands.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_rejects_completed_host_and_successor_on_the_same_connection(
) {
    let mut h = Fixture::new().await;
    h.submit().await;
    {
        let mut state = h.state.write().await;
        finish(&mut state);
        state.turn_in_flight = true;
        native_acp::begin_prompt(&mut state).unwrap();
        let value = json!({"version":1,"sessionId":"acp-A","hostTurnId":state.native_steer_acp_host,
            "nativeTurnId":"successor","ready":true,"initialBackend":true});
        native_acp::apply_state(&mut state, &value);
    }
    assert_eq!(h.settled().await.state, NativeSteerStatus::NotInjected);
    assert!(h.commands.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_rejects_replaced_connection_arc_even_with_identical_ids(
) {
    let mut h = Fixture::new().await;
    h.submit().await;
    let mut replacement = h
        .manager
        .insert_test_connection_live("prepare", AgentType::OpenClaw, None, EventEmitter::Noop)
        .await;
    let state = h.manager.get_state("prepare").await.unwrap();
    assert!(!Arc::ptr_eq(&state, &h.state));
    {
        let mut state = state.write().await;
        state.conversation_id = Some(209);
        state.external_id = Some("acp-A".into());
        state.turn_in_flight = true;
        state.native_steer_acp_v1 = true;
        state.native_steer_acp_preparation_wait = true;
        native_acp::begin_prompt(&mut state).unwrap();
        native_acp::apply_state(&mut state, &h.notification("native-1", true, true));
    }
    h.ready().await;
    assert_eq!(
        h.settled().await.reason.as_deref(),
        Some("connection_scope_mismatch")
    );
    assert!(h.commands.try_recv().is_err());
    assert!(replacement.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_rechecks_conversation_agent_and_acp_session_scope() {
    for scope in ["conversation", "agent", "acp-session"] {
        let mut h = Fixture::new().await;
        h.submit().await;
        {
            let mut state = h.state.write().await;
            match scope {
                "conversation" => state.conversation_id = Some(210),
                "agent" => state.agent_type = AgentType::Hermes,
                _ => state.external_id = Some("acp-other".into()),
            }
        }
        assert_eq!(
            h.settled().await.state,
            NativeSteerStatus::NotInjected,
            "{scope}"
        );
        assert!(h.commands.try_recv().is_err());
    }
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_cancel_admission_fences_late_ready_before_terminal() {
    let mut h = Fixture::new().await;
    h.fill_channel().await;
    h.submit().await;
    let manager = h.manager.clone_ref();
    let cancellation = tokio::spawn(async move {
        manager.enqueue_cancel("prepare").await.unwrap();
    });
    while !native_acp::cancel_requested(&*h.state.read().await) {
        tokio::task::yield_now().await;
    }
    // The ordinary Prompt may still be queued when Cancel is admitted. Its
    // begin_prompt must not clear a same-generation cancellation.
    native_acp::begin_prompt(&mut *h.state.write().await).unwrap();
    h.ready().await;
    assert!(
        h.state.read().await.turn_in_flight,
        "Stop must not fake A's terminal event"
    );
    assert_eq!(h.settled().await.reason.as_deref(), Some("turn_changed"));
    for _ in 0..4 {
        assert!(matches!(
            h.commands.recv().await,
            Some(ConnectionCommand::SetMode { .. })
        ));
    }
    cancellation.await.unwrap();
    assert!(matches!(
        h.commands.recv().await,
        Some(ConnectionCommand::Cancel)
    ));
    assert!(h.commands.try_recv().is_err());
    let state = h.state.read().await;
    assert!(native_acp::build_request(&state, "acp-A", &turn_id(&state), "id", &[]).is_err());
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_idle_cancel_does_not_poison_the_next_prompt() {
    let mut h = Fixture::new().await;
    h.state.write().await.turn_in_flight = false;
    h.manager.enqueue_cancel("prepare").await.unwrap();
    assert!(matches!(
        h.commands.recv().await,
        Some(ConnectionCommand::Cancel)
    ));
    {
        let mut state = h.state.write().await;
        state.turn_in_flight = true;
        native_acp::begin_prompt(&mut state).unwrap();
        assert!(!native_acp::cancel_requested(&state));
    }
    h.submit().await;
    h.ready().await;
    h.consume(Ok(SteerOutcome::Injected)).await;
    assert_eq!(h.settled().await.state, NativeSteerStatus::Injected);
}

#[tokio::test(start_paused = true)]
async fn myclaw_prompt_openclaw_preparation_budget_covers_wait_and_command_capacity() {
    for ready in [false, true] {
        let mut h = Fixture::new().await;
        if ready {
            h.fill_channel().await;
            h.ready().await;
        }
        h.submit().await;
        tokio::time::advance(Duration::from_secs(119)).await;
        assert_eq!(h.receipt().await.state, NativeSteerStatus::Pending);
        tokio::time::advance(Duration::from_secs(1)).await;
        let result = h.settled().await;
        assert_eq!(result.state, NativeSteerStatus::NotInjected);
        assert_eq!(result.reason.as_deref(), Some("readiness_timeout"));
        if ready {
            for _ in 0..4 {
                assert!(matches!(
                    h.commands.recv().await,
                    Some(ConnectionCommand::SetMode { .. })
                ));
            }
        }
        assert!(h.commands.try_recv().is_err());
    }
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_dispatched_unknown_is_never_retried_after_finish() {
    let mut h = Fixture::new().await;
    h.submit().await;
    h.ready().await;
    let command = tokio::time::timeout(Duration::from_secs(1), h.commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(command, ConnectionCommand::NativeSteer { .. }));
    drop(command);
    assert_eq!(h.settled().await.state, NativeSteerStatus::Unconfirmed);
    finish(&mut *h.state.write().await);
    assert_eq!(
        h.manager.native_steer(h.input.clone()).await.unwrap().state,
        NativeSteerStatus::Unconfirmed
    );
    let mut restarted = h.input.clone();
    restarted.server_epoch = "old-process".into();
    assert_eq!(
        h.manager
            .native_steer(restarted)
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("server_restarted")
    );
    assert!(h.commands.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_openclaw_preparation_requires_its_new_contract_and_leaves_other_agents_unchanged(
) {
    for agent in [AgentType::OpenClaw, AgentType::Hermes, AgentType::Codex] {
        let mut h = Fixture::new().await;
        {
            let mut state = h.state.write().await;
            state.agent_type = agent;
            state.native_steer_acp_preparation_wait = agent != AgentType::OpenClaw;
        }
        h.input.expected_agent_type = agent.as_wire().into_owned();
        h.submit().await;
        assert_eq!(
            h.settled().await.reason.as_deref(),
            Some("native_steering_unsupported")
        );
        assert!(h.commands.try_recv().is_err());
    }
    let meta = json!({native_acp::CAPABILITY:{"version":1,"activeOnly":true,"receipts":"consumed","preparationWait":1}});
    assert!(native_acp::preparation_wait_advertised(
        AgentType::OpenClaw,
        meta.as_object()
    ));
    assert!(!native_acp::preparation_wait_advertised(
        AgentType::Hermes,
        meta.as_object()
    ));
}
