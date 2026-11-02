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
        .send_prompt_inner("c", original.clone(), echo.clone(), Some("FIRST\nNEXT"))
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
            .send_prompt_inner("c", hello(), None, value)
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
            .send_prompt_inner("c", hello(), None, value)
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
        .send_prompt_inner("c", hello(), None, Some("THIS_TURN_ONLY"))
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
        .send_prompt_inner("c", hello(), None, Some(&oversized))
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
        .send_prompt_inner("c", hello(), None, Some("VALID"))
        .await
        .unwrap();
    assert!(text_of(&take(&mut rx).await.0).contains("VALID"));
}

#[tokio::test]
async fn myclaw_prompt_busy_and_empty_are_still_rejected() {
    let (manager, mut rx) = fixture().await;
    assert!(manager
        .send_prompt_inner("c", vec![], None, Some("RULES"))
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
            .send_prompt_inner("c", hello(), None, Some("RULES"))
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
            .send_prompt_inner("c", hello(), None, Some("PRIVATE"))
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
        })
        .await
        .unwrap();
    }
    assert!(tokio::time::timeout(
        Duration::from_millis(20),
        manager.send_prompt_inner("c", hello(), None, Some("CANCELLED"))
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
    for _ in 0..4 {
        take(&mut rx).await;
    }
    manager.send_prompt("c", hello()).await.unwrap();
    assert_eq!(take(&mut rx).await.0, hello());
}
