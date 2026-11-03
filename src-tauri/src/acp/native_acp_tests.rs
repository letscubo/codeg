use super::*;
use crate::acp::types::AcpEvent;

fn fixture() -> (SessionState, String) {
    let mut state = SessionState::new(
        "connection".into(),
        AgentType::Hermes,
        None,
        "test".into(),
        None,
    );
    state.external_id = Some("session-A".into());
    state.conversation_id = Some(42);
    state.turn_in_flight = true;
    state.native_steer_acp_v1 = true;
    let host = begin_prompt(&mut state).unwrap()["hostTurnId"]
        .as_str()
        .unwrap()
        .to_string();
    (state, host)
}

fn ready(host: &str, native: &str) -> Value {
    json!({"version":1,"sessionId":"session-A","hostTurnId":host,"nativeTurnId":native,"ready":true})
}

#[test]
fn myclaw_prompt_native_acp_capability_is_distinct_from_legacy_feedback() {
    let legacy = json!({"steering":{"supported":true}})
        .as_object()
        .unwrap()
        .clone();
    assert!(!advertised(AgentType::Hermes, Some(&legacy)));
    let exact = json!({CAPABILITY:{"version":1,"activeOnly":true,"receipts":"consumed"}})
        .as_object()
        .unwrap()
        .clone();
    for agent in [AgentType::OpenClaw, AgentType::Hermes] {
        assert!(advertised(agent, Some(&exact)));
    }
    assert!(!advertised(AgentType::ClaudeCode, Some(&exact)));
    let mut queued = exact;
    queued[CAPABILITY]["receipts"] = json!("queued");
    assert!(!advertised(AgentType::Hermes, Some(&queued)));
    let (state, _) = fixture();
    assert!(
        !state.native_steer_v1_available,
        "handshake is not active-turn readiness"
    );

    // Diagnostic stderr never supplies authority or opaque identifiers. Only
    // the independently validated, data-only vocabulary can reach INFO.
    let prefix = "[codeg-openclaw-native] ";
    for value in [
        json!({"phase":"boot","event":"registered"}),
        json!({"phase":"initialize","result":"failed","elapsedMs":86400000,
            "code":"FORBIDDEN","detailCode":"MISSING_SCOPE","reason":"load_failed"}),
        json!({"phase":"prompt","supported":false,"tracking":false}),
        json!({"phase":"readiness","turn":1000000000,"ready":false,
            "reason":"backend_not_available","elapsedMs":0,"sessionKeyChanged":true,
            "activeRunMatches":true,"controllerMatches":false}),
        json!({"phase":"connection","event":"gateway_disconnect","activeTurns":10000}),
    ] {
        assert_eq!(
            parse_owned_diagnostic(&format!("{prefix}{value}")),
            Some(value)
        );
    }
    for invalid in [
        json!({"phase":"boot","event":"registered","token":"must-not-log"}),
        json!({"phase":"boot","event":"must-not-log"}),
        json!({"phase":"prompt","supported":"true","tracking":false}),
        json!({"phase":"prompt","supported":true}),
        json!({"phase":"prompt","supported":true,"tracking":true,"connection_id":"foreign"}),
        json!({"phase":"initialize","result":"failed","elapsedMs":86400001}),
        json!({"phase":"initialize","result":"failed","elapsedMs":-1}),
        json!({"phase":"initialize","result":"failed","elapsedMs":1.5}),
        json!({"phase":"initialize","result":"failed","elapsedMs":0,"code":"secret"}),
        json!({"phase":"initialize","result":"failed","elapsedMs":0,"detailCode":"secret"}),
        json!({"phase":"readiness","turn":1000000001,"ready":true,"reason":"ready","elapsedMs":0}),
        json!({"phase":"readiness","turn":1,"ready":true,"reason":"foreign","elapsedMs":0}),
        json!({"phase":"connection","event":"shutdown","activeTurns":10001}),
        json!({"phase":"unknown","event":"shutdown"}),
        json!([]),
    ] {
        assert!(parse_owned_diagnostic(&format!("{prefix}{invalid}")).is_none());
    }
    assert!(parse_owned_diagnostic("ordinary stderr must not become INFO").is_none());
    assert!(parse_owned_diagnostic(&format!(" {prefix}{{}}")).is_none());
    assert!(parse_owned_diagnostic(&format!("{prefix}{{}} trailing")).is_none());
    assert!(parse_owned_diagnostic(&format!("{prefix}{}", " ".repeat(2048))).is_none());
}

#[test]
fn myclaw_prompt_native_acp_readiness_cannot_cross_session_host_or_native_turn() {
    let (mut state, host) = fixture();
    apply_state(&mut state, &ready(&host, "native-1"));
    let first = super::super::steering::turn_id(&state);
    assert!(first.starts_with("acp:"));
    assert!(state.native_steer_v1_available);
    let mut wrong = ready(&host, "foreign");
    wrong["sessionId"] = json!("session-B");
    apply_state(&mut state, &wrong);
    apply_state(&mut state, &ready("older-host", "foreign"));
    assert_eq!(super::super::steering::turn_id(&state), first);
    apply_state(&mut state, &ready(&host, "native-2"));
    assert_ne!(super::super::steering::turn_id(&state), first);
    let mut old_end = ready(&host, "native-1");
    old_end["ready"] = json!(false);
    apply_state(&mut state, &old_end);
    assert!(state.native_steer_v1_available);
    let blocks = vec![PromptInputBlock::Text { text: "B".into() }];
    assert!(build_request(&state, "session-A", &first, "submission", &blocks).is_err());
    state.apply_event(&AcpEvent::TurnComplete {
        session_id: "session-A".into(),
        stop_reason: "end_turn".into(),
        agent_type: "hermes".into(),
        duration_ms: None,
    });
    apply_state(&mut state, &ready(&host, "native-2"));
    assert!(!state.native_steer_v1_available);
    assert!(state.native_steer_acp_turn.is_none());
}

#[test]
fn myclaw_prompt_native_acp_request_preserves_scope_submission_and_unicode() {
    let (mut state, host) = fixture();
    apply_state(&mut state, &ready(&host, "native-1"));
    let token = super::super::steering::turn_id(&state);
    let blocks = vec![PromptInputBlock::Text {
        text: "B 调整方向 😀".into(),
    }];
    let request = build_request(&state, "session-A", &token, "submission-1", &blocks).unwrap();
    assert_eq!(request["hostTurnId"], host);
    assert_eq!(request["expectedNativeTurnId"], "native-1");
    assert_eq!(request["submissionId"], "submission-1");
    assert_eq!(request["prompt"][0]["text"], "B 调整方向 😀");
    assert!(build_request(&state, "another-session", &token, "submission-1", &blocks).is_err());
}

#[test]
fn myclaw_prompt_native_acp_only_exact_consumption_or_withdrawal_receipts_settle() {
    let request = json!({"hostTurnId":"host","expectedNativeTurnId":"native","submissionId":"id"});
    let receipt = json!({"version":1,"hostTurnId":"host","nativeTurnId":"native","submissionId":"id","outcome":"injected"});
    assert_eq!(
        parse_result(&receipt, &request).unwrap(),
        SteerOutcome::Injected
    );
    let mut unconsumed = receipt.clone();
    unconsumed["outcome"] = json!("not_injected");
    assert_eq!(
        parse_result(&unconsumed, &request).unwrap(),
        SteerOutcome::PromptRequired
    );
    for (field, value) in [
        ("version", json!(2)),
        ("submissionId", json!("other")),
        ("hostTurnId", json!("other")),
        ("nativeTurnId", json!("other")),
        ("outcome", json!("queued")),
        ("outcome", json!("unconfirmed")),
    ] {
        let mut invalid = receipt.clone();
        invalid[field] = value;
        assert!(parse_result(&invalid, &request).is_err());
    }
}

#[tokio::test]
async fn myclaw_prompt_native_acp_receipt_history_precedes_terminal_and_successor() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().to_string_lossy().to_string();
    temp_env::async_with_vars([("CODEG_HOME", Some(path.as_str()))], async {
        let batch = ReceiptBatch::default();
        let permit = batch.start();
        let (release, waiting) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            waiting.await.unwrap();
            assert!(
                permit
                    .record(
                        AgentType::Hermes,
                        "session-A",
                        &[PromptInputBlock::Text { text: "B".into() },]
                    )
                    .await
            );
        });
        let mut finishing = Box::pin(batch.finish());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut finishing)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        finishing.await;
        writer.await.unwrap();
        super::super::connection::record_turn_end(
            AgentType::Hermes,
            "session-A",
            "end_turn",
            1,
            None,
        )
        .await;
        super::super::connection::record_prompt(
            AgentType::Hermes,
            "session-A",
            &map_prompt_blocks(vec![PromptInputBlock::Text { text: "C".into() }]),
        )
        .await;
        let history = crate::acp_transcript::read_transcript("hermes", "session-A");
        assert_eq!(history.entries.len(), 3);
        assert!(history.entries[0].p.to_string().contains('B'));
        assert_eq!(
            history.entries[1].k,
            crate::acp_transcript::EntryKind::TurnEnd
        );
        assert!(history.entries[2].p.to_string().contains('C'));
    })
    .await;
}

#[tokio::test]
async fn myclaw_prompt_native_acp_dropped_owner_never_appends_a_late_consumed_input() {
    let batch = ReceiptBatch::default();
    let late = batch.start();
    drop(batch);
    // The closed check precedes any transcript path/file access.
    assert!(
        !late
            .record(
                AgentType::Hermes,
                "already-closed-session",
                &[PromptInputBlock::Text {
                    text: "late B must not appear after C".into()
                },]
            )
            .await
    );
}
