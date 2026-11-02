//! No database, migrations, network, or real agent processes are used here.
use super::*;
use crate::acp::myclaw_prompt::{PromptIdentity, PromptSnapshot, PromptSource};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Source {
    config: std::sync::Mutex<Result<Option<PromptSnapshot>, String>>,
    calls: AtomicUsize,
    identities: std::sync::Mutex<Vec<PromptIdentity>>,
}
#[async_trait::async_trait]
impl PromptSource for Source {
    async fn snapshot(&self, identity: &PromptIdentity) -> Result<Option<PromptSnapshot>, String> {
        self.identities.lock().unwrap().push(identity.clone());
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.config.lock().unwrap().clone()
    }
}
fn config(existing: bool) -> PromptSnapshot {
    PromptSnapshot {
        new_session_prompt: "FIRST_POLICY".into(),
        every_message_prompt: "NEXT_POLICY".into(),
        session_has_messages: existing,
    }
}
async fn fixture(
    existing: bool,
) -> (
    ConnectionManager,
    Arc<Source>,
    tokio::sync::mpsc::Receiver<ConnectionCommand>,
) {
    let manager = ConnectionManager::new();
    let source = Arc::new(Source {
        config: std::sync::Mutex::new(Ok(Some(config(existing)))),
        calls: AtomicUsize::new(0),
        identities: std::sync::Mutex::new(Vec::new()),
    });
    manager.install_internal_prompt_source(source.clone());
    let rx = manager
        .insert_test_connection_live("c", AgentType::ClaudeCode, None, EventEmitter::Noop)
        .await;
    (manager, source, rx)
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
async fn myclaw_prompt_first_then_every_followup_preserves_user_echo_and_reads_updated_config() {
    let (manager, source, mut rx) = fixture(false).await;
    let echo = Some((
        "message-1".into(),
        crate::acp::user_blocks_from_prompt(&hello()),
    ));
    manager
        .send_prompt_inner("c", hello(), echo.clone())
        .await
        .unwrap();
    let (blocks, user_message) = take(&mut rx).await;
    assert!(text_of(&blocks).contains("FIRST_POLICY"));
    assert!(text_of(&blocks).contains("NEXT_POLICY"));
    assert_eq!(user_message, echo);
    assert_eq!(blocks.last(), hello().first());
    for policy in ["SECOND_POLICY", "THIRD_POLICY"] {
        idle(&manager).await;
        source
            .config
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .as_mut()
            .unwrap()
            .every_message_prompt = policy.into();
        // Platform says new until history sync catches up; local admission wins.
        manager.clone_ref().send_prompt("c", hello()).await.unwrap();
        let (blocks, _) = take(&mut rx).await;
        let text = text_of(&blocks);
        assert!(text.contains(policy));
        assert!(!text.contains("FIRST_POLICY"));
        assert_eq!(text.matches("<myclaw_internal_prompt ").count(), 1);
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn myclaw_prompt_resumed_connection_uses_persisted_history_even_without_local_turns() {
    let (manager, _, mut rx) = fixture(true).await;
    manager.send_prompt("c", hello()).await.unwrap();
    let (blocks, _) = take(&mut rx).await;
    assert!(text_of(&blocks).contains("NEXT_POLICY"));
    assert!(!text_of(&blocks).contains("FIRST_POLICY"));
}

#[tokio::test]
async fn myclaw_prompt_failed_config_does_not_enqueue_or_consume_first_phase() {
    let (manager, source, mut rx) = fixture(false).await;
    *source.config.lock().unwrap() = Err("internal_prompt_unavailable".into());
    assert!(manager.send_prompt("c", hello()).await.is_err());
    assert!(rx.try_recv().is_err());
    assert_eq!(
        manager
            .get_state("c")
            .await
            .unwrap()
            .read()
            .await
            .internal_prompt_accepted_scope,
        None
    );
    *source.config.lock().unwrap() = Ok(Some(config(false)));
    manager.send_prompt("c", hello()).await.unwrap();
    let (blocks, _) = take(&mut rx).await;
    assert!(text_of(&blocks).contains("FIRST_POLICY"));
}

#[tokio::test]
async fn myclaw_prompt_busy_and_empty_are_rejected_before_config_reads() {
    let (manager, source, mut rx) = fixture(false).await;
    assert!(manager.send_prompt("c", vec![]).await.is_err());
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    manager
        .get_state("c")
        .await
        .unwrap()
        .write()
        .await
        .turn_in_flight = true;
    assert!(matches!(
        manager.send_prompt("c", hello()).await,
        Err(AcpError::TurnInProgress)
    ));
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn myclaw_prompt_empty_first_config_still_advances_phase() {
    let (manager, source, mut rx) = fixture(false).await;
    source
        .config
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .new_session_prompt
        .clear();
    manager.send_prompt("c", hello()).await.unwrap();
    assert!(text_of(&take(&mut rx).await.0).contains("NEXT_POLICY"));
    idle(&manager).await;
    manager.send_prompt("c", hello()).await.unwrap();
    assert!(text_of(&take(&mut rx).await.0).contains("NEXT_POLICY"));
}

#[tokio::test]
async fn myclaw_prompt_closed_channel_and_cancelled_reserve_do_not_advance_phase() {
    let (manager, _, rx) = fixture(false).await;
    drop(rx);
    assert!(matches!(
        manager.send_prompt("c", hello()).await,
        Err(AcpError::ProcessExited)
    ));
    assert_eq!(
        manager
            .get_state("c")
            .await
            .unwrap()
            .read()
            .await
            .internal_prompt_accepted_scope,
        None
    );
    let (manager, _, _rx) = fixture(false).await;
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
    assert!(
        tokio::time::timeout(Duration::from_millis(20), manager.send_prompt("c", hello()))
            .await
            .is_err()
    );
    let state = manager.get_state("c").await.unwrap();
    let s = state.read().await;
    assert!(!s.turn_in_flight);
    assert_eq!(s.internal_prompt_accepted_scope, None);
}

#[tokio::test]
async fn myclaw_prompt_scope_change_rechecks_phase_and_attachments_are_preserved() {
    let (manager, _, mut rx) = fixture(false).await;
    manager.send_prompt("c", hello()).await.unwrap();
    take(&mut rx).await;
    idle(&manager).await;
    manager
        .get_state("c")
        .await
        .unwrap()
        .write()
        .await
        .conversation_id = Some(42);
    let image = PromptInputBlock::Image {
        data: "test-image".into(),
        mime_type: "image/png".into(),
        uri: None,
    };
    manager.send_prompt("c", vec![image.clone()]).await.unwrap();
    let (blocks, _) = take(&mut rx).await;
    assert!(text_of(&blocks).contains("FIRST_POLICY"));
    assert_eq!(blocks.last(), Some(&image));
}

#[tokio::test]
async fn myclaw_prompt_native_history_evidence_distinguishes_fresh_resume_and_accepted() {
    use crate::acp::myclaw_prompt::HistoryState;
    let (manager, source, mut rx) = fixture(false).await;
    let state = manager.get_state("c").await.unwrap();
    {
        let mut s = state.write().await;
        s.internal_prompt_fresh_session = true;
        s.external_id = Some("new-native".into());
    }
    manager.send_prompt("c", hello()).await.unwrap();
    take(&mut rx).await;
    assert_eq!(
        source.identities.lock().unwrap()[0].history_state,
        HistoryState::Empty
    );
    idle(&manager).await;
    manager.send_prompt("c", hello()).await.unwrap();
    take(&mut rx).await;
    assert_eq!(
        source.identities.lock().unwrap()[1].history_state,
        HistoryState::Present
    );
    let (manager, source, mut rx) = fixture(true).await;
    manager
        .get_state("c")
        .await
        .unwrap()
        .write()
        .await
        .external_id = Some("resumed-native".into());
    manager.send_prompt("c", hello()).await.unwrap();
    take(&mut rx).await;
    assert_eq!(
        source.identities.lock().unwrap()[0].history_state,
        HistoryState::Unknown
    );
    assert_eq!(
        source.identities.lock().unwrap()[0]
            .native_session_id
            .as_deref(),
        Some("resumed-native")
    );
}

#[tokio::test]
async fn myclaw_prompt_native_session_change_clears_fresh_evidence() {
    use crate::acp::types::AcpEvent;
    let (manager, _, _rx) = fixture(false).await;
    let state = manager.get_state("c").await.unwrap();
    let mut s = state.write().await;
    s.internal_prompt_fresh_session = true;
    s.apply_event(&AcpEvent::SessionStarted {
        session_id: "first".into(),
    });
    assert!(s.internal_prompt_fresh_session);
    s.apply_event(&AcpEvent::SessionStarted {
        session_id: "fork-or-resume".into(),
    });
    assert!(!s.internal_prompt_fresh_session);
}
