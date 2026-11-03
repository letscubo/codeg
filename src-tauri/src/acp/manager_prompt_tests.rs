//! Reviewed database-free tests: SessionState, Noop emitter and in-memory channels only.
use super::*;

async fn fixture() -> (
    ConnectionManager,
    tokio::sync::mpsc::Receiver<ConnectionCommand>,
) {
    let manager = ConnectionManager::new();
    let rx = manager
        .insert_test_connection_live("c", AgentType::ClaudeCode, None, EventEmitter::Noop)
        .await;
    (manager, rx)
}
fn hello() -> Vec<PromptInputBlock> {
    vec![PromptInputBlock::Text {
        text: "Hello".into(),
    }]
}
fn text_of(blocks: &[PromptInputBlock]) -> String {
    blocks
        .iter()
        .filter_map(|b| match b {
            PromptInputBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
async fn take(
    rx: &mut tokio::sync::mpsc::Receiver<ConnectionCommand>,
) -> (
    Vec<PromptInputBlock>,
    Option<(String, Vec<crate::acp::UserMessageBlock>)>,
) {
    match rx.recv().await.unwrap() {
        ConnectionCommand::Prompt {
            blocks,
            user_message,
            ..
        } => (blocks, user_message),
        _ => panic!("unexpected command"),
    }
}
async fn idle(manager: &ConnectionManager) {
    manager
        .get_state("c")
        .await
        .unwrap()
        .write()
        .await
        .turn_in_flight = false;
}

#[tokio::test]
async fn myclaw_prompt_native_steering_client_message_proof_tracks_admitted_turn() {
    let manager = ConnectionManager::new();
    let mut rx = manager
        .insert_test_connection_live("c", AgentType::OpenClaw, None, EventEmitter::Noop)
        .await;
    let state = manager.get_state("c").await.unwrap();
    {
        let mut state = state.write().await;
        state.external_id = Some("test-session".into());
        state.conversation_id = Some(42);
        state.native_steer_acp_v1 = true;
        state.native_steer_acp_preparation_wait = true;
    }
    let first = uuid::Uuid::new_v4().to_string();
    let second = uuid::Uuid::new_v4().to_string();
    let echo = |id: &str| {
        Some((
            id.to_string(),
            crate::acp::user_blocks_from_prompt(&hello()),
        ))
    };
    assert!(manager
        .native_steering_state("c")
        .await
        .client_message_id
        .is_none());
    manager
        .send_prompt_inner_raw("c", hello(), echo(&first), None)
        .await
        .unwrap();
    let admitted = manager.native_steering_state("c").await;
    assert!(!admitted.native_supported);
    assert_eq!(admitted.client_message_id.as_deref(), Some(first.as_str()));
    assert_eq!(
        serde_json::to_value(&admitted).unwrap()["clientMessageId"],
        first
    );
    assert_eq!(
        state.read().await.native_steer_acp_host,
        admitted.active_turn_id
    );
    // B is accepted before the connection has dequeued A or called begin_prompt.
    let submission = uuid::Uuid::new_v4().to_string();
    assert_eq!(
        manager
            .native_steer(crate::acp::steering::NativeSteerInput {
                connection_id: "c".into(),
                expected_conversation_id: 42,
                expected_agent_type: "open_claw".into(),
                server_epoch: admitted.server_epoch.clone(),
                expected_turn_id: admitted.active_turn_id.clone().unwrap(),
                submission_id: submission.clone(),
                text: "B belongs only to A".into(),
                blocks: None,
            })
            .await
            .unwrap()
            .state,
        crate::acp::steering::NativeSteerStatus::Pending
    );
    tokio::task::yield_now().await;
    assert_eq!(
        manager
            .native_steer_result(crate::acp::steering::NativeSteerLookup {
                connection_id: "c".into(),
                expected_conversation_id: 42,
                expected_agent_type: "open_claw".into(),
                server_epoch: admitted.server_epoch,
                submission_id: submission,
            })
            .await
            .state,
        crate::acp::steering::NativeSteerStatus::Pending
    );
    assert_eq!(take(&mut rx).await.1.unwrap().0, first);
    assert!(matches!(
        manager
            .send_prompt_inner_raw("c", hello(), echo(&second), None)
            .await,
        Err(AcpError::TurnInProgress)
    ));
    assert!(rx.try_recv().is_err());
    assert_eq!(
        manager
            .native_steering_state("c")
            .await
            .client_message_id
            .as_deref(),
        Some(first.as_str())
    );
    state.write().await.apply_event(&AcpEvent::TurnComplete {
        session_id: "test-session".into(),
        stop_reason: "end_turn".into(),
        agent_type: "open_claw".into(),
        duration_ms: None,
    });
    assert!(state.read().await.native_steer_client_message_id.is_none());
    assert!(manager
        .native_steering_state("c")
        .await
        .client_message_id
        .is_none());
    manager
        .send_prompt_inner_raw("c", hello(), echo(&second), None)
        .await
        .unwrap();
    assert_eq!(take(&mut rx).await.1.unwrap().0, second);
    assert_eq!(
        manager
            .native_steering_state("c")
            .await
            .client_message_id
            .as_deref(),
        Some(second.as_str())
    );
    // Even stale internal data must never become an idle proof.
    idle(&manager).await;
    assert_eq!(
        state.read().await.native_steer_client_message_id.as_deref(),
        Some(second.as_str())
    );
    let inactive = manager.native_steering_state("c").await;
    assert!(inactive.client_message_id.is_none());
    assert!(serde_json::to_value(inactive)
        .unwrap()
        .get("clientMessageId")
        .is_none());
}

#[tokio::test]
async fn myclaw_prompt_native_steering_client_message_proof_clears_without_uuid() {
    let (manager, mut rx) = fixture().await;
    let state = manager.get_state("c").await.unwrap();
    let previous = uuid::Uuid::new_v4().to_string();
    for id in [
        None,
        Some("user-c-1"),
        Some("optimistic-d93b83f8-12d5-41c3-a659-83cafc721627"),
        Some(""),
    ] {
        {
            let mut state = state.write().await;
            state.turn_in_flight = false;
            state.native_steer_client_message_id = Some(previous.clone());
        }
        let echo = id.map(|id| {
            (
                id.to_string(),
                crate::acp::user_blocks_from_prompt(&hello()),
            )
        });
        manager
            .send_prompt_inner_raw("c", hello(), echo, None)
            .await
            .unwrap();
        take(&mut rx).await;
        assert!(state.read().await.native_steer_client_message_id.is_none());
        assert!(manager
            .native_steering_state("c")
            .await
            .client_message_id
            .is_none());
    }
    idle(&manager).await;
    state.write().await.native_steer_client_message_id = Some(previous.clone());
    let rejected = uuid::Uuid::new_v4().to_string();
    assert!(manager
        .send_prompt_inner_raw("c", vec![], Some((rejected.clone(), vec![])), None)
        .await
        .is_err());
    assert_eq!(
        state.read().await.native_steer_client_message_id.as_deref(),
        Some(previous.as_str())
    );
    drop(rx);
    assert!(matches!(
        manager
            .send_prompt_inner_raw("c", hello(), Some((rejected, vec![])), None)
            .await,
        Err(AcpError::ProcessExited)
    ));
    assert_eq!(
        state.read().await.native_steer_client_message_id.as_deref(),
        Some(previous.as_str())
    );
    assert!(manager
        .native_steering_state("c")
        .await
        .client_message_id
        .is_none());
}

#[tokio::test]
async fn myclaw_prompt_inline_context_preserves_user_echo_and_attachments() {
    let (manager, mut rx) = fixture().await;
    let mut original = hello();
    original.push(PromptInputBlock::Image {
        data: "test-image".into(),
        mime_type: "image/png".into(),
        uri: None,
    });
    let echo = Some((
        "message-1".into(),
        crate::acp::user_blocks_from_prompt(&original),
    ));
    manager
        .send_prompt_inner(
            "c",
            original.clone(),
            echo.clone(),
            Some("FIRST\nNEXT"),
            None,
        )
        .await
        .unwrap();
    let (blocks, user_message) = take(&mut rx).await;
    assert_eq!(user_message, echo);
    assert_eq!(&blocks[1..], original.as_slice());
    assert!(text_of(&blocks[..1]).contains("FIRST\nNEXT"));
    assert_eq!(
        text_of(&blocks).matches("<myclaw_internal_prompt ").count(),
        1
    );
}

#[tokio::test]
async fn myclaw_prompt_absent_null_empty_and_whitespace_do_not_inject() {
    let (manager, mut rx) = fixture().await;
    for value in [None, Some(""), Some(" \n\t")] {
        manager
            .send_prompt_inner("c", hello(), None, value, None)
            .await
            .unwrap();
        assert_eq!(take(&mut rx).await.0, hello());
        idle(&manager).await;
    }
    // Legacy/internal callers remain valid and do not request a platform snapshot.
    manager.send_prompt("c", hello()).await.unwrap();
    assert_eq!(take(&mut rx).await.0, hello());
}

#[tokio::test]
async fn myclaw_prompt_each_request_uses_only_its_own_context() {
    let (manager, mut rx) = fixture().await;
    for value in [Some("FIRST_ONLY"), None, Some("SECOND_ONLY"), Some("")] {
        manager
            .clone_ref()
            .send_prompt_inner("c", hello(), None, value, None)
            .await
            .unwrap();
        let (blocks, _) = take(&mut rx).await;
        match value.filter(|s| !s.is_empty()) {
            Some(text) => {
                assert_eq!(blocks.len(), 2);
                assert!(text_of(&blocks[..1]).contains(text));
            }
            None => assert_eq!(blocks, hello()),
        }
        idle(&manager).await;
    }
}

#[tokio::test]
async fn myclaw_prompt_context_does_not_cross_connections_or_depend_on_history() {
    let (manager, mut rx) = fixture().await;
    let mut other = manager
        .insert_test_connection_live("other", AgentType::ClaudeCode, None, EventEmitter::Noop)
        .await;
    {
        let state = manager.get_state("c").await.unwrap();
        let mut s = state.write().await;
        s.external_id = Some("existing-native-id".into());
        s.conversation_id = Some(42);
    }
    manager
        .send_prompt_inner("c", hello(), None, Some("THIS_TURN_ONLY"), None)
        .await
        .unwrap();
    assert!(text_of(&take(&mut rx).await.0).contains("THIS_TURN_ONLY"));
    manager.send_prompt("other", hello()).await.unwrap();
    assert_eq!(take(&mut other).await.0, hello());
    idle(&manager).await;
    manager
        .get_state("c")
        .await
        .unwrap()
        .write()
        .await
        .conversation_id = Some(43);
    manager.send_prompt("c", hello()).await.unwrap();
    assert_eq!(take(&mut rx).await.0, hello());
}

#[tokio::test]
async fn myclaw_prompt_invalid_context_never_enqueues_or_wedges_gate() {
    let (manager, mut rx) = fixture().await;
    let oversized = "private".repeat(10_000);
    assert!(manager
        .send_prompt_inner("c", hello(), None, Some(&oversized), None)
        .await
        .is_err());
    assert!(rx.try_recv().is_err());
    assert!(
        !manager
            .get_state("c")
            .await
            .unwrap()
            .read()
            .await
            .turn_in_flight
    );
    manager
        .send_prompt_inner("c", hello(), None, Some("VALID"), None)
        .await
        .unwrap();
    assert!(text_of(&take(&mut rx).await.0).contains("VALID"));
}

#[tokio::test]
async fn myclaw_prompt_busy_and_empty_are_still_rejected() {
    let (manager, mut rx) = fixture().await;
    assert!(manager
        .send_prompt_inner("c", vec![], None, Some("RULES"), None)
        .await
        .is_err());
    assert!(rx.try_recv().is_err());
    manager
        .get_state("c")
        .await
        .unwrap()
        .write()
        .await
        .turn_in_flight = true;
    assert!(matches!(
        manager
            .send_prompt_inner("c", hello(), None, Some("RULES"), None)
            .await,
        Err(AcpError::TurnInProgress)
    ));
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_closed_channel_and_cancelled_reserve_do_not_retain_context() {
    let (manager, rx) = fixture().await;
    drop(rx);
    assert!(matches!(
        manager
            .send_prompt_inner("c", hello(), None, Some("PRIVATE"), None)
            .await,
        Err(AcpError::ProcessExited)
    ));
    assert!(
        !manager
            .get_state("c")
            .await
            .unwrap()
            .read()
            .await
            .turn_in_flight
    );
    let (manager, mut rx) = fixture().await;
    manager
        .get_state("c")
        .await
        .unwrap()
        .write()
        .await
        .cli_model = Some("claude-opus-5".into());
    let tx = manager
        .connections
        .lock()
        .await
        .get("c")
        .unwrap()
        .cmd_tx
        .clone();
    for _ in 0..4 {
        tx.send(ConnectionCommand::Prompt {
            blocks: hello(),
            user_message: None,
            cli_options: None,
        })
        .await
        .unwrap();
    }
    assert!(tokio::time::timeout(
        Duration::from_millis(20),
        manager.send_prompt_inner(
            "c",
            hello(),
            None,
            Some("CANCELLED"),
            Some(crate::acp::cli::effort::CliTurnOptions {
                model: Some("claude-sonnet-5".into()),
                provider: None,
                reasoning_effort: Some(Some("max".into())),
            }),
        )
    )
    .await
    .is_err());
    assert!(
        !manager
            .get_state("c")
            .await
            .unwrap()
            .read()
            .await
            .turn_in_flight
    );
    assert_eq!(
        manager
            .get_state("c")
            .await
            .unwrap()
            .read()
            .await
            .cli_model
            .as_deref(),
        Some("claude-opus-5")
    );
    for _ in 0..4 {
        take(&mut rx).await;
    }
    manager.send_prompt("c", hello()).await.unwrap();
    let ConnectionCommand::Prompt {
        blocks,
        cli_options,
        ..
    } = rx.recv().await.unwrap()
    else {
        panic!("expected legacy prompt after cancelled reserve");
    };
    assert_eq!(blocks, hello());
    assert!(cli_options.is_none());
}

#[tokio::test]
async fn myclaw_prompt_context_and_effort_stay_with_the_accepted_turn() {
    use crate::acp::cli::effort::CliTurnOptions;
    let (manager, mut rx) = fixture().await;
    let original = hello();
    let echo = Some((
        "optimistic-first".into(),
        crate::acp::user_blocks_from_prompt(&original),
    ));
    let options = |provider: &str, model: &str, level: &str| {
        Some(CliTurnOptions {
            model: Some(model.into()),
            provider: Some(provider.into()),
            reasoning_effort: Some(Some(level.into())),
        })
    };
    manager
        .send_prompt_inner(
            "c",
            original.clone(),
            echo.clone(),
            Some("ACCEPTED_ONLY"),
            options("myclaw-anthropic", "claude-opus-5", "max"),
        )
        .await
        .unwrap();
    assert!(matches!(
        manager
            .send_prompt_inner(
                "c",
                hello(),
                None,
                Some("REJECTED_ONLY"),
                options("other-provider", "claude-sonnet-5", "low"),
            )
            .await,
        Err(AcpError::TurnInProgress)
    ));
    let ConnectionCommand::Prompt {
        blocks,
        user_message,
        cli_options,
    } = rx.recv().await.unwrap()
    else {
        panic!("expected prompt");
    };
    assert_eq!(user_message, echo);
    assert_eq!(&blocks[1..], original.as_slice());
    assert!(text_of(&blocks[..1]).contains("ACCEPTED_ONLY"));
    assert!(!text_of(&blocks).contains("REJECTED_ONLY"));
    let accepted = cli_options.unwrap();
    assert_eq!(accepted.provider.as_deref(), Some("myclaw-anthropic"));
    assert_eq!(accepted.model.as_deref(), Some("claude-opus-5"));
    assert_eq!(accepted.reasoning_effort, Some(Some("max".into())));
    assert_eq!(
        manager
            .get_state("c")
            .await
            .unwrap()
            .read()
            .await
            .cli_model
            .as_deref(),
        Some("claude-opus-5")
    );
    assert!(rx.try_recv().is_err());
    idle(&manager).await;
    // Legacy callers add neither option, even after a rejected mixed request.
    manager.send_prompt("c", hello()).await.unwrap();
    let ConnectionCommand::Prompt {
        blocks,
        user_message,
        cli_options,
    } = rx.recv().await.unwrap()
    else {
        panic!("expected legacy prompt");
    };
    assert_eq!(blocks, hello());
    assert!(user_message.is_none());
    assert!(cli_options.is_none());
    idle(&manager).await;
    manager
        .send_prompt_inner(
            "c",
            hello(),
            None,
            Some("NEXT_ONLY"),
            options("myclaw", "claude-opus-5", "low"),
        )
        .await
        .unwrap();
    let ConnectionCommand::Prompt {
        blocks,
        cli_options,
        ..
    } = rx.recv().await.unwrap()
    else {
        panic!("expected next prompt");
    };
    assert!(text_of(&blocks).contains("NEXT_ONLY"));
    assert!(!text_of(&blocks).contains("ACCEPTED_ONLY"));
    assert!(!text_of(&blocks).contains("REJECTED_ONLY"));
    assert_eq!(
        cli_options.unwrap().reasoning_effort,
        Some(Some("low".into()))
    );
    idle(&manager).await;
    let mut clear_blocks = hello();
    clear_blocks.push(PromptInputBlock::Image {
        data: "clear-image".into(),
        mime_type: "image/png".into(),
        uri: None,
    });
    let clear_echo = Some((
        "optimistic-clear".into(),
        crate::acp::user_blocks_from_prompt(&clear_blocks),
    ));
    manager
        .send_prompt_inner(
            "c",
            clear_blocks.clone(),
            clear_echo.clone(),
            Some("CLEAR_ONLY"),
            Some(CliTurnOptions {
                model: Some("claude-opus-5".into()),
                provider: None,
                reasoning_effort: Some(None),
            }),
        )
        .await
        .unwrap();
    let ConnectionCommand::Prompt {
        blocks,
        user_message,
        cli_options,
    } = rx.recv().await.unwrap()
    else {
        panic!("expected clear prompt");
    };
    assert_eq!(user_message, clear_echo);
    assert_eq!(&blocks[1..], clear_blocks.as_slice());
    assert!(text_of(&blocks[..1]).contains("CLEAR_ONLY"));
    assert!(!text_of(&blocks).contains("NEXT_ONLY"));
    assert_eq!(cli_options.unwrap().reasoning_effort, Some(None));
}

#[tokio::test]
async fn myclaw_prompt_accepted_provider_switch_is_used_by_the_next_fast_turn() {
    use crate::acp::cli::effort::{CliEffortContext, CliTurnOptions};
    let (manager, mut rx) = fixture().await;
    let state = manager.get_state("c").await.unwrap();
    state.write().await.cli_effort_context = Some(CliEffortContext {
        agent_type: AgentType::Pi,
        provider: Some("route-a".into()),
        executable: "/not-run".into(),
        working_dir: "/not-read".into(),
        runtime_env: Default::default(),
    });
    manager
        .send_prompt_inner(
            "c",
            hello(),
            None,
            None,
            Some(CliTurnOptions {
                model: Some("model-b".into()),
                provider: Some("route-b".into()),
                reasoning_effort: Some(Some("high".into())),
            }),
        )
        .await
        .unwrap();
    assert!(matches!(
        manager
            .send_prompt_inner(
                "c",
                hello(),
                None,
                None,
                Some(CliTurnOptions {
                    model: Some("model-c".into()),
                    provider: Some("route-c".into()),
                    reasoning_effort: Some(Some("max".into())),
                })
            )
            .await,
        Err(AcpError::TurnInProgress)
    ));
    assert!(matches!(
        manager
            .send_prompt_inner(
                "c",
                hello(),
                None,
                None,
                Some(CliTurnOptions {
                    model: Some("legacy-model".into()),
                    provider: Some("legacy-route".into()),
                    reasoning_effort: None,
                })
            )
            .await,
        Err(AcpError::TurnInProgress)
    ));
    let ConnectionCommand::Prompt { cli_options, .. } = rx.recv().await.unwrap() else {
        panic!("expected prompt")
    };
    assert_eq!(cli_options.unwrap().provider.as_deref(), Some("route-b"));
    idle(&manager).await;
    let saved = state.read().await;
    // The HTTP fast path has connectionId + effort, with no new launch/provider.
    let options = saved.cli_effort_context.as_ref().unwrap().turn_options(
        saved.cli_model.clone(),
        None,
        Some(Some("low".into())),
    );
    drop(saved);
    assert_eq!(options.model.as_deref(), Some("model-b"));
    assert_eq!(options.provider.as_deref(), Some("route-b"));
    manager
        .send_prompt_inner("c", hello(), None, None, Some(options))
        .await
        .unwrap();
    let ConnectionCommand::Prompt { cli_options, .. } = rx.recv().await.unwrap() else {
        panic!("expected fast prompt")
    };
    let options = cli_options.unwrap();
    assert_eq!(options.provider.as_deref(), Some("route-b"));
    assert_eq!(options.reasoning_effort, Some(Some("low".into())));
    let saved = state.read().await;
    let (model, provider) =
        crate::acp::cli::effort::model_route_for_turn(None, &saved, Some("route-a"));
    assert_eq!(model.as_deref(), Some("model-b"));
    assert_eq!(provider.as_deref(), Some("route-b"));
}
