use super::*;
use crate::acp::types::EventEnvelope;
use std::collections::VecDeque;

#[derive(Default)]
struct Fake {
    calls: Mutex<Vec<(String, Value, Instant)>>,
    sends: Mutex<VecDeque<Result<Value, ApiError>>>,
}
#[async_trait]
impl WhatsappApi for Fake {
    async fn call(&self, method: &str, body: Value) -> Result<Value, ApiError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((method.into(), body, Instant::now()));
        let count = calls.len();
        drop(calls);
        if method == "messages" {
            self.sends
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(json!({"messages":[{"id":format!("wamid.{count}")}]})))
        } else {
            Ok(json!({"success":true}))
        }
    }
}
fn turn(api: Arc<Fake>) -> WhatsappTurn {
    WhatsappTurn {
        api,
        agent_id: uuid::Uuid::new_v4().as_u128().to_string(),
        recipient: "user:123".into(),
        inbound_message_id: "wamid.inbound".into(),
        texts: RelayTexts::default(),
    }
}
fn event(payload: AcpEvent) -> RelayInput {
    RelayInput::Event(Arc::new(EventEnvelope {
        seq: 1,
        connection_id: "wa-test".into(),
        payload,
    }))
}
fn content(text: &str) -> RelayInput {
    event(AcpEvent::ContentDelta {
        text: text.into(),
        parent_tool_use_id: None,
    })
}
fn complete(reason: &str) -> RelayInput {
    event(AcpEvent::TurnComplete {
        session_id: "retained-session".into(),
        stop_reason: reason.into(),
        agent_type: "test".into(),
        duration_ms: None,
    })
}
async fn settle() {
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
}
fn message_count(api: &Fake) -> usize {
    api.calls
        .lock()
        .unwrap()
        .iter()
        .filter(|c| c.0 == "messages")
        .count()
}

#[tokio::test(start_paused = true)]
async fn sends_incrementally_before_completion_keeps_exact_text_and_filters_reasoning() {
    let api = Arc::new(Fake::default());
    let (tx, rx) = mpsc::unbounded_channel();
    let worker = tokio::spawn(turn(api.clone()).run(rx));
    settle().await;
    assert_eq!(api.calls.lock().unwrap()[0].0, "statuses");
    let first = format!("{}。\n", "第一段".repeat(32));
    tx.send(content(&first)).unwrap();
    tx.send(event(AcpEvent::Thinking {
        text: "private reasoning".into(),
        parent_tool_use_id: None,
    }))
    .unwrap();
    tx.send(event(AcpEvent::ContentDelta {
        text: "subagent private output".into(),
        parent_tool_use_id: Some("tool".into()),
    }))
    .unwrap();
    settle().await;
    tokio::time::advance(COALESCE).await;
    settle().await;
    assert_eq!(
        message_count(&api),
        1,
        "first paragraph must be sent while agent is still running"
    );
    assert!(!worker.is_finished());
    tx.send(content("第二段尾部")).unwrap();
    tx.send(complete("end_turn")).unwrap();
    let report = worker.await.unwrap();
    assert_eq!(report.outcome, Outcome::Completed);
    assert!(!report.delivery_failed);
    assert_eq!(report.final_text, format!("{first}第二段尾部"));
    assert_eq!(report.segments.len(), 2);
    let calls = api.calls.lock().unwrap();
    let writes: Vec<_> = calls.iter().filter(|c| c.0 == "messages").collect();
    assert!(writes[1].2.duration_since(writes[0].2) >= INTERVAL);
    assert_eq!(writes[0].1["to"], "user:123");
    assert_eq!(writes[0].1["context"]["message_id"], "wamid.inbound");
    assert!(!format!("{calls:?}").contains("private"));
    let compact = crate::chat_channel::relay_result::compact(
        json!({"final_text":report.final_text,"segments":report.segments}),
    )
    .unwrap();
    assert!(compact["segments"][0].get("text").is_none());
    assert!(compact["segments"][0]["message_key"]
        .as_str()
        .unwrap()
        .starts_with("wamid."));
}

#[tokio::test(start_paused = true)]
async fn typing_refreshes_and_stops_after_cancel_or_failure() {
    for reason in ["cancelled", "error"] {
        let api = Arc::new(Fake::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(turn(api.clone()).run(rx));
        settle().await;
        tokio::time::advance(TYPING_REFRESH).await;
        settle().await;
        assert_eq!(
            api.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c.0 == "statuses")
                .count(),
            2
        );
        tx.send(complete(reason)).unwrap();
        let report = worker.await.unwrap();
        assert!(!report.delivery_failed);
        assert_eq!(message_count(&api), 1);
        let before = api.calls.lock().unwrap().len();
        tokio::time::advance(Duration::from_secs(60)).await;
        settle().await;
        assert_eq!(api.calls.lock().unwrap().len(), before);
        assert_eq!(
            report.outcome,
            if reason == "cancelled" {
                Outcome::Cancelled
            } else {
                Outcome::Failed
            }
        );
    }
}

#[tokio::test(start_paused = true)]
async fn unknown_send_is_not_retried_and_does_not_send_a_second_message() {
    let api = Arc::new(Fake::default());
    api.sends.lock().unwrap().push_back(Err(ApiError::Unknown));
    let (tx, rx) = mpsc::unbounded_channel();
    let worker = tokio::spawn(turn(api.clone()).run(rx));
    let first = format!("{}。", "段落".repeat(60));
    tx.send(content(&first)).unwrap();
    settle().await;
    tokio::time::advance(COALESCE).await;
    settle().await;
    assert_eq!(message_count(&api), 1);
    tx.send(content("剩余正文")).unwrap();
    tx.send(complete("end_turn")).unwrap();
    let report = worker.await.unwrap();
    assert!(report.delivery_unknown && report.delivery_failed);
    assert_eq!(message_count(&api), 1);
    assert_eq!(report.final_text, format!("{first}剩余正文"));
}

#[tokio::test(start_paused = true)]
async fn explicit_rejection_retries_only_with_safe_backoff_and_same_body() {
    let api = Fake::default();
    api.sends
        .lock()
        .unwrap()
        .push_back(Err(ApiError::RetryAfter(Duration::from_secs(15))));
    let gate = Limits::default();
    let body = json!({"text":{"body":"reply"}});
    let result = call_limited(
        &api,
        &gate,
        "messages",
        body.clone(),
        &AtomicBool::new(false),
    )
    .await
    .unwrap();
    assert!(result["messages"][0]["id"].is_string());
    let calls = api.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].1, calls[1].1);
    assert!(calls[1].2.duration_since(calls[0].2) >= Duration::from_secs(15));
}

#[tokio::test(start_paused = true)]
async fn same_agent_gate_survives_turn_end_and_serializes_requests() {
    let api = Arc::new(Fake::default());
    let id = uuid::Uuid::new_v4().to_string();
    let first = limits(&id);
    let second = limits(&id);
    assert!(Arc::ptr_eq(&first, &second));
    tokio::time::advance(Duration::from_secs(180)).await;
    call_limited(
        api.as_ref(),
        &first,
        "messages",
        json!({}),
        &AtomicBool::new(false),
    )
    .await
    .unwrap();
    drop(first);
    drop(second);
    call_limited(
        api.as_ref(),
        &limits(&id),
        "messages",
        json!({}),
        &AtomicBool::new(false),
    )
    .await
    .unwrap();
    let calls = api.calls.lock().unwrap();
    assert!(calls[1].2.duration_since(calls[0].2) >= INTERVAL);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_rate_wait_suppresses_unsent_message() {
    let api = Arc::new(Fake::default());
    let gate = Arc::new(Limits::default());
    *gate.messages.lock().await = Some(Instant::now() + Duration::from_secs(30));
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = stopped.clone();
    let calls = api.clone();
    let worker = tokio::spawn(async move {
        call_limited(calls.as_ref(), &gate, "messages", json!({}), &stop).await
    });
    settle().await;
    stopped.store(true, Ordering::Release);
    assert!(matches!(worker.await.unwrap(), Err(ApiError::Suppressed)));
    assert_eq!(message_count(&api), 0);
}

#[test]
fn http_classification_does_not_retry_ambiguous_delivery() {
    assert!(matches!(
        classify(429, None, Value::Null),
        Err(ApiError::RetryAfter(_))
    ));
    assert!(matches!(
        classify(503, None, json!({"error":{"code":131016}})),
        Err(ApiError::RetryAfter(_))
    ));
    for status in [500, 502, 503] {
        assert!(matches!(
            classify(status, None, Value::Null),
            Err(ApiError::Unknown)
        ));
    }
    for status in [400, 401, 403] {
        assert!(matches!(
            classify(status, None, Value::Null),
            Err(ApiError::Rejected)
        ));
    }
}
#[test]
fn chunk_boundaries_preserve_unicode_and_bound_messages() {
    let original = format!("{}\n{}", "🦀".repeat(2100), "tail".repeat(1000));
    let mut pending = original.clone();
    let mut chunks = vec![];
    while !pending.is_empty() {
        let end = chunk_end(&pending, true);
        assert!(end > 0);
        chunks.push(pending.drain(..end).collect::<String>());
    }
    assert_eq!(chunks.concat(), original);
    assert!(chunks.iter().all(|s| s.encode_utf16().count() <= CHUNK));
    assert_eq!(chunk_end("two words", false), 0);
    assert_eq!(chunk_end(&"x".repeat(1000), false), 0);
    assert_eq!(chunk_end(&format!("\n\n{}", "x".repeat(1000)), false), 0);
    assert_eq!(chunk_end(&format!("标题\n{}", "x".repeat(1000)), false), 0);
    assert_eq!(chunk_end(&"x".repeat(4000), false), CHUNK);
    assert!(valid_target("123", "user:456", "wamid.abc", "opaque-token"));
    assert!(!valid_target(
        "123",
        "agent:456",
        "wamid.abc",
        "opaque-token"
    ));
}

#[tokio::test(start_paused = true)]
async fn cancel_pending_stops_typing_and_unknown_runtime_never_flushes_buffer() {
    let api = Arc::new(Fake::default());
    let (tx, rx) = mpsc::unbounded_channel();
    let worker = tokio::spawn(turn(api.clone()).run(rx));
    settle().await;
    tx.send(content("Unfinished buffered answer")).unwrap();
    tx.send(event(AcpEvent::Error {
        message: "cancelling".into(),
        agent_type: "test".into(),
        details: None,
        code: Some("runtime_cancel_pending".into()),
        terminal: false,
    }))
    .unwrap();
    settle().await;
    let calls = api.calls.lock().unwrap().len();
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(api.calls.lock().unwrap().len(), calls);
    tx.send(event(AcpEvent::Error {
        message: "unknown".into(),
        agent_type: "test".into(),
        details: None,
        code: Some("runtime_outcome_unknown".into()),
        terminal: true,
    }))
    .unwrap();
    let report = worker.await.unwrap();
    assert_eq!(report.outcome, Outcome::Unknown);
    assert!(!report.text_complete);
    assert_eq!(report.final_text, "Unfinished buffered answer");
    assert_eq!(message_count(&api), 0);
}

#[tokio::test(start_paused = true)]
async fn normal_final_flush_keeps_typing_until_all_rate_limited_messages_finish() {
    let api = Arc::new(Fake::default());
    let (tx, rx) = mpsc::unbounded_channel();
    let worker = tokio::spawn(turn(api.clone()).run(rx));
    tx.send(content(&"x".repeat(CHUNK * 6 + 10))).unwrap();
    tx.send(complete("end_turn")).unwrap();
    let report = worker.await.unwrap();
    assert!(!report.delivery_failed);
    let calls = api.calls.lock().unwrap().clone();
    let messages: Vec<_> = calls.iter().filter(|c| c.0 == "messages").collect();
    assert_eq!(messages.len(), 7);
    let start = messages.first().unwrap().2;
    let end = messages.last().unwrap().2;
    assert!(end.duration_since(start) > Duration::from_secs(25));
    assert!(calls
        .iter()
        .any(|c| c.0 == "statuses" && c.2.duration_since(start) >= Duration::from_secs(25)));
    assert!(calls
        .iter()
        .filter(|c| c.0 == "statuses")
        .all(|c| c.2 <= end));
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(api.calls.lock().unwrap().len(), calls.len());
}
