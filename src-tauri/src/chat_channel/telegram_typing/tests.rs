use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{HEARTBEAT, MIN_REFRESH_INTERVAL, REQUEST_TIMEOUT};
use crate::acp::types::{AcpEvent, ConnectionStatus, EventEnvelope};
use crate::chat_channel::turn_relay::{
    Outcome, RelayInput, RelayTexts, TelegramTurn, TgApi, TgError, TurnReport,
};

#[derive(Clone)]
struct Call {
    method: String,
    body: Value,
    at: Instant,
}

enum Reply {
    Ok,
    RateLimited(u64),
    Unknown,
    Rejected,
    Pending,
}

#[derive(Default)]
struct MockTelegram {
    calls: Mutex<Vec<Call>>,
    typing_replies: Mutex<VecDeque<Reply>>,
    message_replies: Mutex<VecDeque<Reply>>,
    hold_draft: AtomicBool,
    hold_message: AtomicBool,
    release_draft: Notify,
    release_message: Notify,
    active_typing: AtomicUsize,
}

struct ActiveTyping<'a>(&'a AtomicUsize);

impl Drop for ActiveTyping<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn respond(reply: Reply) -> Result<(), TgError> {
    match reply {
        Reply::Ok => Ok(()),
        Reply::RateLimited(seconds) => Err(TgError::RetryAfter(seconds)),
        Reply::Unknown => Err(TgError::Unknown),
        Reply::Rejected => Err(TgError::Other("Forbidden".into())),
        Reply::Pending => std::future::pending().await,
    }
}

#[async_trait]
impl TgApi for MockTelegram {
    async fn call(&self, method: &str, body: Value) -> Result<Option<i64>, TgError> {
        self.calls.lock().unwrap().push(Call {
            method: method.into(),
            body,
            at: Instant::now(),
        });
        match method {
            "sendChatAction" => {
                self.active_typing.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveTyping(&self.active_typing);
                let reply = self
                    .typing_replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Reply::Ok);
                respond(reply).await?;
                Ok(None)
            }
            "sendMessageDraft" => {
                if self.hold_draft.load(Ordering::SeqCst) {
                    self.release_draft.notified().await;
                }
                Ok(None)
            }
            "sendMessage" => {
                if self.hold_message.load(Ordering::SeqCst) {
                    self.release_message.notified().await;
                }
                let reply = self
                    .message_replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Reply::Ok);
                respond(reply).await?;
                Ok(Some(42))
            }
            "editMessageText" => Ok(Some(42)),
            unexpected => panic!("unexpected Telegram API method: {unexpected}"),
        }
    }
}

impl MockTelegram {
    fn calls(&self, method: &str) -> Vec<Call> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.method == method)
            .cloned()
            .collect()
    }

    fn unblock_draft(&self) {
        self.hold_draft.store(false, Ordering::SeqCst);
        self.release_draft.notify_waiters();
    }

    fn unblock_message(&self) {
        self.hold_message.store(false, Ordering::SeqCst);
        self.release_message.notify_waiters();
    }
}

fn event(payload: AcpEvent) -> RelayInput {
    RelayInput::Event(Arc::new(EventEnvelope {
        seq: 1,
        connection_id: "typing-test".into(),
        payload,
    }))
}

fn delta(text: &str) -> RelayInput {
    event(AcpEvent::ContentDelta {
        text: text.into(),
        parent_tool_use_id: None,
    })
}

fn complete(reason: &str) -> RelayInput {
    event(AcpEvent::TurnComplete {
        session_id: "typing-session".into(),
        stop_reason: reason.into(),
        agent_type: "codex".into(),
        duration_ms: None,
    })
}

fn error(terminal: bool) -> RelayInput {
    event(AcpEvent::Error {
        message: "test error".into(),
        agent_type: "codex".into(),
        code: None,
        details: None,
        terminal,
    })
}

fn start(api: Arc<MockTelegram>) -> (mpsc::UnboundedSender<RelayInput>, JoinHandle<TurnReport>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(
        TelegramTurn {
            api,
            chat_id: -10012345,
            draft_base: 77,
            texts: RelayTexts::default(),
        }
        .run(rx),
    );
    (tx, task)
}

async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

async fn advance(duration: Duration) {
    tokio::time::advance(duration).await;
    settle().await;
}

#[tokio::test(start_paused = true)]
async fn typing_starts_and_renews_while_draft_loading_remains() {
    let api = Arc::new(MockTelegram::default());
    let (tx, task) = start(api.clone());
    settle().await;
    let first = api.calls("sendChatAction");
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].body,
        json!({"chat_id": -10012345, "action": "typing"})
    );
    assert_eq!(
        api.calls("sendMessageDraft")[0].body["text"],
        RelayTexts::default().thinking
    );

    advance(HEARTBEAT).await;
    advance(HEARTBEAT).await;
    assert_eq!(api.calls("sendChatAction").len(), 3);
    assert!(api.calls("sendMessageDraft").len() >= 2);

    tx.send(complete("end_turn")).unwrap();
    assert_eq!(task.await.unwrap().outcome, Outcome::Completed);
    let count = api.calls("sendChatAction").len();
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), count);
}

#[tokio::test(start_paused = true)]
async fn every_terminal_stops_typing_even_while_the_initial_draft_is_blocked() {
    let cases = vec![
        (Some(complete("end_turn")), Outcome::Completed),
        (Some(complete("cancelled")), Outcome::Cancelled),
        (Some(complete("error")), Outcome::Failed),
        (Some(error(true)), Outcome::Failed),
        (
            Some(event(AcpEvent::StatusChanged {
                status: ConnectionStatus::Disconnected,
            })),
            Outcome::Failed,
        ),
        (
            Some(event(AcpEvent::StatusChanged {
                status: ConnectionStatus::Error,
            })),
            Outcome::Failed,
        ),
        (Some(RelayInput::Lagged), Outcome::Lagged),
        (Some(RelayInput::SilentComplete), Outcome::Silent),
        (None, Outcome::Aborted),
    ];
    for (terminal, expected) in cases {
        let api = Arc::new(MockTelegram::default());
        api.hold_draft.store(true, Ordering::SeqCst);
        let (tx, task) = start(api.clone());
        let mut tx = Some(tx);
        settle().await;
        advance(HEARTBEAT).await;
        assert_eq!(api.calls("sendChatAction").len(), 2);
        if let Some(terminal) = terminal {
            tx.as_ref().unwrap().send(terminal).unwrap();
        } else {
            drop(tx.take());
        }
        settle().await;
        let count = api.calls("sendChatAction").len();
        advance(HEARTBEAT * 3).await;
        assert_eq!(api.calls("sendChatAction").len(), count, "{expected:?}");
        assert!(
            !task.is_finished(),
            "the deliberately blocked draft must remain blocked"
        );
        api.unblock_draft();
        assert_eq!(task.await.unwrap().outcome, expected);
    }
}

#[tokio::test(start_paused = true)]
async fn slow_body_does_not_block_renewal_or_terminal_stop() {
    let api = Arc::new(MockTelegram::default());
    api.hold_message.store(true, Ordering::SeqCst);
    let (tx, task) = start(api.clone());
    settle().await;
    tx.send(delta("正文仍然流式投递")).unwrap();
    settle().await;
    advance(Duration::from_secs(1)).await;
    assert_eq!(api.calls("sendMessage").len(), 1);
    advance(HEARTBEAT).await;
    assert!(api.calls("sendChatAction").len() >= 2);
    tx.send(complete("cancelled")).unwrap();
    settle().await;
    let count = api.calls("sendChatAction").len();
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), count);
    api.unblock_message();
    let report = task.await.unwrap();
    assert_eq!(report.outcome, Outcome::Cancelled);
    assert!(report.final_text.contains("正文仍然流式投递"));
    assert_eq!(api.calls("sendChatAction").len(), count);
}

#[tokio::test(start_paused = true)]
async fn body_rate_limit_does_not_block_typing_or_terminal_stop() {
    let api = Arc::new(MockTelegram::default());
    api.message_replies
        .lock()
        .unwrap()
        .push_back(Reply::RateLimited(20));
    let (tx, task) = start(api.clone());
    settle().await;
    tx.send(delta("reply")).unwrap();
    settle().await;
    advance(Duration::from_secs(1)).await;
    advance(HEARTBEAT).await;
    assert_eq!(api.calls("sendMessage").len(), 1);
    assert!(api.calls("sendChatAction").len() >= 2);
    tx.send(complete("end_turn")).unwrap();
    settle().await;
    let count = api.calls("sendChatAction").len();
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), count);
    let report = task.await.unwrap();
    assert_eq!(report.final_text, "reply");
    assert!(!report.delivery_failed);
    assert_eq!(api.calls("sendChatAction").len(), count);
}

#[tokio::test(start_paused = true)]
async fn typing_rate_limit_is_respected_without_delaying_body() {
    let api = Arc::new(MockTelegram::default());
    api.typing_replies
        .lock()
        .unwrap()
        .push_back(Reply::RateLimited(9));
    let (tx, task) = start(api.clone());
    settle().await;
    tx.send(delta("hello")).unwrap();
    settle().await;
    advance(Duration::from_secs(1)).await;
    assert_eq!(api.calls("sendMessage").len(), 1);
    advance(Duration::from_secs(7)).await;
    assert_eq!(
        api.calls("sendChatAction").len(),
        1,
        "message refresh must not bypass retry_after"
    );
    advance(Duration::from_secs(1)).await;
    let calls = api.calls("sendChatAction");
    assert_eq!(calls.len(), 2);
    assert!(calls[1].at.duration_since(calls[0].at) >= Duration::from_secs(9));
    tx.send(complete("end_turn")).unwrap();
    assert!(!task.await.unwrap().delivery_failed);
}

#[tokio::test(start_paused = true)]
async fn transient_typing_failure_retries_and_permanent_failure_does_not_fail_delivery() {
    let api = Arc::new(MockTelegram::default());
    api.typing_replies
        .lock()
        .unwrap()
        .extend([Reply::Unknown, Reply::Rejected]);
    let (tx, task) = start(api.clone());
    settle().await;
    advance(HEARTBEAT).await;
    assert_eq!(api.calls("sendChatAction").len(), 2);
    tx.send(delta("hello")).unwrap();
    settle().await;
    advance(Duration::from_secs(1)).await;
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), 2);
    tx.send(complete("end_turn")).unwrap();
    let report = task.await.unwrap();
    assert_eq!(report.final_text, "hello");
    assert!(!report.delivery_failed);
}

#[tokio::test(start_paused = true)]
async fn timed_out_typing_request_is_cancelled_and_retried() {
    let api = Arc::new(MockTelegram::default());
    api.typing_replies.lock().unwrap().push_back(Reply::Pending);
    let (tx, task) = start(api.clone());
    settle().await;
    assert_eq!(api.active_typing.load(Ordering::SeqCst), 1);
    advance(REQUEST_TIMEOUT).await;
    assert_eq!(api.active_typing.load(Ordering::SeqCst), 0);
    advance(HEARTBEAT).await;
    assert_eq!(api.calls("sendChatAction").len(), 2);
    tx.send(complete("end_turn")).unwrap();
    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn terminal_event_cancels_an_in_flight_typing_request() {
    let api = Arc::new(MockTelegram::default());
    api.typing_replies.lock().unwrap().push_back(Reply::Pending);
    let (tx, task) = start(api.clone());
    settle().await;
    assert_eq!(api.active_typing.load(Ordering::SeqCst), 1);
    tx.send(complete("cancelled")).unwrap();
    settle().await;
    assert_eq!(api.active_typing.load(Ordering::SeqCst), 0);
    assert_eq!(task.await.unwrap().outcome, Outcome::Cancelled);
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), 1);
}

#[tokio::test(start_paused = true)]
async fn dropping_turn_future_cancels_typing_and_leaves_no_heartbeat_task() {
    let api = Arc::new(MockTelegram::default());
    api.typing_replies.lock().unwrap().push_back(Reply::Pending);
    let (_tx, task) = start(api.clone());
    settle().await;
    assert_eq!(api.active_typing.load(Ordering::SeqCst), 1);
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(api.active_typing.load(Ordering::SeqCst), 0);
    let count = api.calls("sendChatAction").len();
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), count);
}

#[tokio::test(start_paused = true)]
async fn successful_message_refreshes_typing_and_later_deltas_still_edit_it() {
    let api = Arc::new(MockTelegram::default());
    let (tx, task) = start(api.clone());
    settle().await;
    tx.send(delta("hello")).unwrap();
    settle().await;
    advance(MIN_REFRESH_INTERVAL).await;
    assert_eq!(api.calls("sendMessage").len(), 1);
    let calls = api.calls("sendChatAction");
    assert_eq!(
        calls.len(),
        2,
        "sendMessage clears Telegram typing; renew while generation continues"
    );
    assert!(calls[1].at.duration_since(calls[0].at) >= MIN_REFRESH_INTERVAL);
    assert!(calls[1].at.duration_since(calls[0].at) < HEARTBEAT);
    tx.send(delta(" world")).unwrap();
    settle().await;
    advance(Duration::from_secs(1)).await;
    assert_eq!(
        api.calls("editMessageText").last().unwrap().body["text"],
        "hello world"
    );
    let count = api.calls("sendChatAction").len();
    tx.send(complete("end_turn")).unwrap();
    assert_eq!(task.await.unwrap().final_text, "hello world");
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), count);
}

#[tokio::test(start_paused = true)]
async fn finishing_one_turn_does_not_stop_another_turn_in_the_same_chat() {
    let api = Arc::new(MockTelegram::default());
    let (first_tx, first_task) = start(api.clone());
    let (second_tx, second_task) = start(api.clone());
    settle().await;
    first_tx.send(complete("end_turn")).unwrap();
    first_task.await.unwrap();
    let count = api.calls("sendChatAction").len();
    advance(HEARTBEAT).await;
    assert_eq!(api.calls("sendChatAction").len(), count + 1);
    second_tx.send(complete("end_turn")).unwrap();
    second_task.await.unwrap();
    let count = api.calls("sendChatAction").len();
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), count);
}

#[tokio::test(start_paused = true)]
async fn nonterminal_agent_error_does_not_stop_typing() {
    let api = Arc::new(MockTelegram::default());
    let (tx, task) = start(api.clone());
    settle().await;
    tx.send(error(false)).unwrap();
    settle().await;
    advance(HEARTBEAT).await;
    assert_eq!(api.calls("sendChatAction").len(), 2);
    tx.send(complete("end_turn")).unwrap();
    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn turn_deadline_stops_typing_even_when_delivery_is_blocked() {
    let api = Arc::new(MockTelegram::default());
    api.hold_draft.store(true, Ordering::SeqCst);
    let (_tx, task) = start(api.clone());
    settle().await;
    advance(crate::chat_channel::turn_relay::MAX_TURN).await;
    let count = api.calls("sendChatAction").len();
    advance(HEARTBEAT * 3).await;
    assert_eq!(api.calls("sendChatAction").len(), count);
    assert!(!task.is_finished());
    api.unblock_draft();
    assert_eq!(task.await.unwrap().outcome, Outcome::Failed);
}
