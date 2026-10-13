//! Slack native streaming. See docs.slack.dev/reference/methods/chat.startStream
//! and agents.sessions.setStatus. No token, HTTP response body, or URL is logged.
use super::turn_relay::{DeliverySegment, Outcome, RelayInput, RelayTexts, TurnReport};
use crate::acp::types::{AcpEvent, ConnectionStatus};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};

const INTERVAL: Duration = Duration::from_secs(1);
const CHUNK_LIMIT: usize = 10_000;

#[derive(Debug)]
pub(crate) enum SlackError {
    RetryAfter(u64),
    Rejected,
    Unknown,
}
#[async_trait]
#[allow(clippy::double_must_use)] // async-trait adds this to its boxed Future on Rust 1.99.
pub(crate) trait SlackApi: Send + Sync {
    async fn call(&self, method: &str, body: Value) -> Result<Value, SlackError>;
}
pub(crate) struct HttpSlack {
    pub client: reqwest::Client,
    pub token: String,
}
#[async_trait]
impl SlackApi for HttpSlack {
    async fn call(&self, method: &str, body: Value) -> Result<Value, SlackError> {
        let response = self
            .client
            .post(format!("https://slack.com/api/{method}"))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|_| SlackError::Unknown)?;
        let retry = response
            .headers()
            .get("retry-after")
            .and_then(|s| s.to_str().ok())
            .and_then(|s| s.parse().ok());
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(retry
                .map(SlackError::RetryAfter)
                .unwrap_or(SlackError::Rejected));
        }
        if response.status().is_server_error() {
            return Err(SlackError::Unknown);
        }
        let value: Value = response.json().await.map_err(|_| SlackError::Unknown)?;
        if value["ok"] == true {
            return Ok(value);
        }
        match value["error"].as_str().unwrap_or("") {
            "internal_error" | "fatal_error" => Err(SlackError::Unknown),
            "ratelimited" | "rate_limited" => Err(retry
                .map(SlackError::RetryAfter)
                .unwrap_or(SlackError::Rejected)),
            _ => Err(SlackError::Rejected),
        }
    }
}

pub(crate) struct SlackTurn {
    pub session_key: String,
    pub api: Arc<dyn SlackApi>,
    pub channel: String,
    pub thread_ts: String,
    pub recipient_user_id: String,
    pub recipient_team_id: String,
    pub texts: RelayTexts,
}
struct Stream {
    segments: Vec<DeliverySegment>,
    pending: Vec<Value>,
    tools: HashMap<String, String>,
    failed: bool,
    unknown: bool,
    last_write: Option<Instant>,
    open: bool,
}
impl Stream {
    fn new() -> Self {
        Self {
            segments: vec![DeliverySegment::new(0, String::new())],
            pending: vec![],
            tools: HashMap::new(),
            failed: false,
            unknown: false,
            last_write: None,
            open: false,
        }
    }
    fn current(&mut self) -> &mut DeliverySegment {
        self.segments.last_mut().unwrap()
    }
}
impl SlackTurn {
    async fn call(&self, method: &str, body: Value) -> Result<Value, SlackError> {
        for attempt in 0..3 {
            match self.api.call(method, body.clone()).await {
                Err(SlackError::RetryAfter(s)) if attempt < 2 => {
                    tokio::time::sleep(Duration::from_secs(s)).await
                }
                result => return result,
            }
        }
        unreachable!()
    }
    async fn status(&self, status: &str) -> bool {
        self.call(
            "agents.sessions.setStatus",
            json!({"channel_id": self.channel, "thread_ts": self.thread_ts,
            "initiator_user_id": self.recipient_user_id, "status": status}),
        )
        .await
        .is_ok()
    }
    async fn flush(&self, stream: &mut Stream) {
        if stream.failed || stream.pending.is_empty() {
            return;
        }
        if let Some(at) = stream.last_write {
            tokio::time::sleep_until(at + INTERVAL).await;
        }
        let ts = stream.current().message_ts.clone();
        let mut body = json!({"channel": self.channel, "chunks": stream.pending});
        let method = if let Some(ts) = &ts {
            body["ts"] = ts.clone().into();
            "chat.appendStream"
        } else {
            body["thread_ts"] = self.thread_ts.clone().into();
            body["recipient_user_id"] = self.recipient_user_id.clone().into();
            body["recipient_team_id"] = self.recipient_team_id.clone().into();
            body["task_display_mode"] = "timeline".into();
            "chat.startStream"
        };
        let result = self.call(method, body).await;
        stream.last_write = Some(Instant::now());
        match result {
            Ok(value) => {
                if ts.is_none() {
                    if let Some(ts) = value["ts"].as_str() {
                        stream.current().message_ts = Some(ts.into());
                        stream.open = true;
                    } else {
                        stream.failed = true;
                        stream.unknown = true;
                    }
                }
                if !stream.failed {
                    stream.pending.clear();
                }
            }
            Err(SlackError::Unknown) => {
                stream.failed = true;
                stream.unknown = ts.is_none();
            }
            Err(_) => {
                stream.failed = true;
            }
        }
    }
    async fn stop(&self, stream: &mut Stream, final_stop: bool) {
        if !stream.open {
            return;
        }
        let Some(ts) = stream.current().message_ts.clone() else {
            return;
        };
        // No appended text here: retry/consumer cleanup can never append twice.
        let counter = session_counter(&self.session_key);
        let count = counter.lock().await;
        let stopped = self
            .call(
                "chat.stopStream",
                json!({"channel":self.channel,"ts":ts,
            "session_status": if final_stop && *count<=1 { "active" } else { "processing" }}),
            )
            .await
            .is_ok();
        stream.current().delivered = stopped && !stream.failed && stream.pending.is_empty();
        stream.open = false;
        if !stopped {
            stream.failed = true;
        }
    }
    async fn append_text(&self, stream: &mut Stream, mut text: &str) {
        while !text.is_empty() {
            let remaining = CHUNK_LIMIT - stream.current().text.encode_utf16().count();
            if remaining == 0 {
                self.flush(stream).await;
                self.stop(stream, false).await;
                stream
                    .segments
                    .push(DeliverySegment::new(stream.segments.len(), String::new()));
                stream.pending.clear();
            }
            let capacity = CHUNK_LIMIT - stream.current().text.encode_utf16().count();
            let mut units = 0;
            let at = text
                .char_indices()
                .find_map(|(i, c)| {
                    units += c.len_utf16();
                    (units > capacity).then_some(i)
                })
                .unwrap_or(text.len());
            // A remaining one-unit slot cannot contain an emoji. Finalize it
            // before starting the next segment instead of looping on an empty slice.
            if at == 0 {
                self.flush(stream).await;
                self.stop(stream, false).await;
                stream
                    .segments
                    .push(DeliverySegment::new(stream.segments.len(), String::new()));
                stream.pending.clear();
                continue;
            }
            let part = &text[..at];
            stream.current().text.push_str(part);
            if !stream.failed {
                if let Some(last) = stream
                    .pending
                    .last_mut()
                    .filter(|c| c["type"] == "markdown_text")
                {
                    let mut combined = last["text"].as_str().unwrap_or_default().to_owned();
                    combined.push_str(part);
                    last["text"] = combined.into();
                } else {
                    stream
                        .pending
                        .push(json!({"type":"markdown_text","text":part}));
                }
            }
            text = &text[at..];
        }
    }
    fn task(stream: &mut Stream, id: &str, title: &str, state: &str) {
        // Tool IDs/titles are bounded; raw input/output (and reasoning) are never exposed.
        let id = format!("tool-{:x}", super::turn_relay::derive_draft_base(id));
        stream.pending.push(json!({"type":"task_update","id":id,"title":title.chars().take(100).collect::<String>(),"status":state}));
    }
    pub(crate) async fn run(self, mut rx: mpsc::UnboundedReceiver<RelayInput>) -> TurnReport {
        let mut stream = Stream::new();
        let mut full_text = String::new();
        let counter = session_counter(&self.session_key);
        {
            let mut count = counter.lock().await;
            *count += 1;
            self.status("processing").await;
        }
        let deadline = Instant::now() + Duration::from_secs(3 * 60 * 60);
        let mut heartbeat = Instant::now() + Duration::from_secs(30 * 60);
        let outcome = loop {
            let next = if stream.pending.is_empty() || stream.failed {
                deadline
            } else {
                stream.last_write.unwrap_or_else(Instant::now) + INTERVAL
            };
            tokio::select! {
                input = rx.recv() => match input {
                    None => break Outcome::Aborted,
                    Some(RelayInput::Lagged) => break Outcome::Lagged,
                    Some(RelayInput::SilentComplete) => break Outcome::Silent,
                    Some(RelayInput::Event(env)) => match &env.payload {
                        AcpEvent::ContentDelta {text,parent_tool_use_id:None} => {
                            full_text.push_str(text); self.append_text(&mut stream,text).await;
                        }
                        AcpEvent::ToolCall {tool_call_id,title,description,..} if !stream.failed => {
                            let title = description.as_deref().filter(|s| !s.trim().is_empty()).unwrap_or(title).to_owned();
                            stream.tools.insert(tool_call_id.clone(),title.clone()); Self::task(&mut stream,tool_call_id,&title,"in_progress");
                        }
                        AcpEvent::ToolCallUpdate {tool_call_id,title,description,status,..} if !stream.failed => {
                            let label = description.as_ref().or(title.as_ref()).or(stream.tools.get(tool_call_id)).cloned().unwrap_or_else(|| "Tool".into());
                            let state = match status.as_deref() { Some("completed") => "complete", Some("failed") => "error", _ => "in_progress" };
                            Self::task(&mut stream,tool_call_id,&label,state);
                            if state == "in_progress" {stream.tools.insert(tool_call_id.clone(),label);} else { stream.tools.remove(tool_call_id); }
                        }
                        AcpEvent::TurnComplete {stop_reason,..} => break match stop_reason.as_str() {"end_turn"=>Outcome::Completed,"cancelled"=>Outcome::Cancelled,_=>Outcome::Failed},
                        AcpEvent::Error {terminal:true,..} | AcpEvent::StatusChanged {status:ConnectionStatus::Disconnected|ConnectionStatus::Error} => break Outcome::Failed,
                        _=>{}
                    },
                },
                _ = tokio::time::sleep_until(next) => self.flush(&mut stream).await,
                _ = tokio::time::sleep_until(heartbeat) => { self.status("processing").await; heartbeat = Instant::now()+Duration::from_secs(30*60); }
                _ = tokio::time::sleep_until(deadline) => break Outcome::Failed,
            }
            if stream.pending.len() >= 20 {
                self.flush(&mut stream).await;
            }
        };
        if !matches!(outcome, Outcome::Aborted | Outcome::Lagged) {
            let note = match outcome {
                Outcome::Completed if full_text.trim().is_empty() => Some(&self.texts.empty),
                Outcome::Cancelled => Some(&self.texts.cancelled),
                Outcome::Failed => Some(&self.texts.failed),
                _ => None,
            };
            if let Some(note) = note {
                let note = if full_text.is_empty() {
                    note.clone()
                } else {
                    format!("\n\n{note}")
                };
                full_text.push_str(&note);
                self.append_text(&mut stream, &note).await;
            }
            if !stream.failed {
                for (id, title) in std::mem::take(&mut stream.tools) {
                    Self::task(
                        &mut stream,
                        &id,
                        &title,
                        if outcome == Outcome::Completed {
                            "complete"
                        } else {
                            "error"
                        },
                    );
                }
                self.flush(&mut stream).await;
            }
        }
        self.stop(&mut stream, true).await;
        let cleaned = {
            let mut count = counter.lock().await;
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.status("active").await
            } else {
                true
            }
        };
        stream
            .segments
            .retain(|s| !s.text.is_empty() || s.message_ts.is_some());
        let mut report = TurnReport::new(outcome, stream.segments, full_text, stream.unknown);
        report.delivery_failed |= stream.failed || !cleaned;
        report
    }
}

pub(crate) fn session_key(token: &str, channel: &str, thread: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}:{channel}:{thread}", Sha256::digest(token.as_bytes()))
}
fn session_counter(key: &str) -> Arc<tokio::sync::Mutex<usize>> {
    type Counters = std::sync::Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<usize>>>>;
    static COUNTERS: std::sync::OnceLock<Counters> = std::sync::OnceLock::new();
    let mut counters = COUNTERS.get_or_init(Default::default).lock().unwrap();
    counters.retain(|_, v| v.strong_count() > 0);
    if let Some(counter) = counters.get(key).and_then(std::sync::Weak::upgrade) {
        return counter;
    }
    let counter = Arc::new(tokio::sync::Mutex::new(0));
    counters.insert(key.to_owned(), Arc::downgrade(&counter));
    counter
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp::types::EventEnvelope;
    use std::sync::Mutex;
    #[derive(Default)]
    struct FakeSlack {
        calls: Mutex<Vec<(String, Value)>>,
        fail_method: Option<String>,
        unknown: bool,
    }
    #[async_trait]
    impl SlackApi for FakeSlack {
        async fn call(&self, method: &str, body: Value) -> Result<Value, SlackError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((method.into(), body));
            if self.fail_method.as_deref() == Some(method) {
                return Err(if self.unknown {
                    SlackError::Unknown
                } else {
                    SlackError::Rejected
                });
            }
            Ok(json!({"ok":true,"ts":format!("123.{}",calls.len())}))
        }
    }
    fn turn(api: Arc<FakeSlack>, key: &str) -> SlackTurn {
        SlackTurn {
            api,
            session_key: key.into(),
            channel: "C1".into(),
            thread_ts: "1.0".into(),
            recipient_user_id: "U1".into(),
            recipient_team_id: "T1".into(),
            texts: RelayTexts::default(),
        }
    }
    fn event(payload: AcpEvent) -> RelayInput {
        RelayInput::Event(Arc::new(EventEnvelope {
            seq: 0,
            connection_id: "c".into(),
            payload,
        }))
    }
    fn delta(text: &str) -> RelayInput {
        event(AcpEvent::ContentDelta {
            text: text.into(),
            parent_tool_use_id: None,
        })
    }
    fn complete() -> RelayInput {
        event(AcpEvent::TurnComplete {
            session_id: "s".into(),
            stop_reason: "end_turn".into(),
            agent_type: "codex".into(),
            duration_ms: None,
        })
    }
    #[tokio::test(start_paused = true)]
    async fn native_stream_has_loading_deltas_and_final_stop() {
        let api = Arc::new(FakeSlack::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(turn(api.clone(), "native").run(rx));
        tx.send(delta("first")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(delta(" second")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(complete()).unwrap();
        let report = worker.await.unwrap();
        assert!(!report.delivery_failed);
        assert_eq!(report.final_text, "first second");
        assert_eq!(report.segments.len(), 1);
        let calls = api.calls.lock().unwrap();
        assert_eq!(calls[0].0, "agents.sessions.setStatus");
        assert_eq!(calls[0].1["status"], "processing");
        assert_eq!(
            calls
                .iter()
                .find(|(m, _)| m == "chat.startStream")
                .unwrap()
                .1["chunks"][0]["text"],
            "first"
        );
        assert_eq!(
            calls
                .iter()
                .find(|(m, _)| m == "chat.appendStream")
                .unwrap()
                .1["chunks"][0]["text"],
            " second"
        );
        assert!(calls.iter().any(|(m, _)| m == "chat.stopStream"));
        assert_eq!(calls.last().unwrap().1["status"], "active");
    }
    #[tokio::test(start_paused = true)]
    async fn unknown_start_is_not_retried() {
        let api = Arc::new(FakeSlack {
            fail_method: Some("chat.startStream".into()),
            unknown: true,
            ..Default::default()
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(turn(api.clone(), "unknown-start").run(rx));
        tx.send(delta("hello")).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        tx.send(delta(" world")).unwrap();
        tx.send(complete()).unwrap();
        let report = worker.await.unwrap();
        assert!(report.delivery_unknown);
        assert!(report.delivery_failed);
        assert_eq!(report.final_text, "hello world");
        assert_eq!(
            api.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| m == "chat.startStream")
                .count(),
            1
        );
    }
    #[tokio::test(start_paused = true)]
    async fn unknown_append_retains_known_id_for_safe_overwrite() {
        let api = Arc::new(FakeSlack {
            fail_method: Some("chat.appendStream".into()),
            unknown: true,
            ..Default::default()
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(turn(api.clone(), "unknown-append").run(rx));
        tx.send(delta("first")).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        tx.send(delta(" second")).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        tx.send(complete()).unwrap();
        let report = worker.await.unwrap();
        assert!(!report.delivery_unknown);
        assert!(report.delivery_failed);
        assert!(report.segments[0].message_ts.is_some());
        assert_eq!(
            api.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| m == "chat.appendStream")
                .count(),
            1
        );
    }
    #[tokio::test(start_paused = true)]
    async fn long_reply_is_split_without_losing_text() {
        let api = Arc::new(FakeSlack::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let text = "字".repeat(25000);
        tx.send(delta(&text)).unwrap();
        tx.send(complete()).unwrap();
        let report = turn(api, "long").run(rx).await;
        assert!(!report.delivery_failed);
        assert_eq!(report.segments.len(), 3);
        assert_eq!(
            report
                .segments
                .iter()
                .map(|s| s.text.as_str())
                .collect::<String>(),
            text
        );
    }

    #[tokio::test(start_paused = true)]
    async fn emoji_segments_fit_platform_utf16_contract() {
        let api = Arc::new(FakeSlack::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let text = format!("a{}", "😀".repeat(10000));
        tx.send(delta(&text)).unwrap();
        tx.send(complete()).unwrap();
        let report = turn(api, "emoji").run(rx).await;
        assert!(!report.delivery_failed);
        assert!(report
            .segments
            .iter()
            .all(|s| s.text.encode_utf16().count() <= CHUNK_LIMIT));
        assert_eq!(
            report
                .segments
                .iter()
                .map(|s| s.text.as_str())
                .collect::<String>(),
            text
        );
    }
    #[tokio::test(start_paused = true)]
    async fn concurrent_workers_keep_loading_until_last_finishes() {
        let api = Arc::new(FakeSlack::default());
        let (a, ar) = mpsc::unbounded_channel();
        let (b, br) = mpsc::unbounded_channel();
        let wa = tokio::spawn(turn(api.clone(), "shared").run(ar));
        let wb = tokio::spawn(turn(api.clone(), "shared").run(br));
        a.send(delta("a")).unwrap();
        b.send(delta("b")).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        a.send(complete()).unwrap();
        wa.await.unwrap();
        {
            let calls = api.calls.lock().unwrap();
            assert!(!calls
                .iter()
                .any(|(m, v)| m == "agents.sessions.setStatus" && v["status"] == "active"));
            assert_eq!(
                calls
                    .iter()
                    .find(|(m, _)| m == "chat.stopStream")
                    .unwrap()
                    .1["session_status"],
                "processing"
            );
        }
        b.send(complete()).unwrap();
        wb.await.unwrap();
        assert_eq!(
            api.calls.lock().unwrap().last().unwrap().1["status"],
            "active"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn silent_no_action_leaves_no_message() {
        let api = Arc::new(FakeSlack::default());
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(RelayInput::SilentComplete).unwrap();
        let report = turn(api.clone(), "silent").run(rx).await;
        assert!(!report.delivery_failed);
        assert!(report.segments.is_empty());
        assert!(!api
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(m, _)| m.starts_with("chat.")));
    }
}
