//! Official WhatsApp Agent Platform v1. The API has no edit/draft/stream method:
//! completed text chunks become separate messages while generation continues.
//! Reference: whatsapp.com/developer/WhatsApp-Agent-Platform-Developer-Manual.pdf
use super::turn_relay::{DeliverySegment, Outcome, RelayInput, RelayTexts, TurnReport};
use crate::acp::types::{AcpEvent, ConnectionStatus};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch, Notify},
    time::Instant,
};

const INTERVAL: Duration = Duration::from_secs(6); // each endpoint allows 12 / rolling minute
const TYPING_REFRESH: Duration = Duration::from_secs(20); // expires after 25 seconds
const CHUNK: usize = 3800; // conservative UTF-16 bound below 4096 characters
const MIN_CHUNK: usize = 80;
const COALESCE: Duration = Duration::from_millis(500);

pub(crate) fn valid_target(agent: &str, recipient: &str, inbound: &str, token: &str) -> bool {
    !agent.is_empty()
        && agent.len() <= 128
        && agent.bytes().all(|b| b.is_ascii_digit())
        && recipient.starts_with("user:")
        && recipient.len() > 5
        && recipient.len() <= 512
        && !recipient
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        && inbound.starts_with("wamid.")
        && inbound.len() > 6
        && inbound.len() <= 1024
        && !inbound.chars().any(char::is_control)
        && !token.is_empty()
        && token.len() <= 8192
        && !token.chars().any(|c| c.is_whitespace() || c.is_control())
}

#[derive(Debug)]
pub(crate) enum ApiError {
    RetryAfter(Duration),
    Rejected,
    Unknown,
    Suppressed,
}
#[async_trait]
#[allow(clippy::double_must_use)]
pub(crate) trait WhatsappApi: Send + Sync {
    async fn call(&self, method: &str, body: Value) -> Result<Value, ApiError>;
}
pub(crate) struct HttpWhatsapp {
    pub client: reqwest::Client,
    pub token: String,
}
fn classify(status: u16, retry: Option<u64>, body: Value) -> Result<Value, ApiError> {
    if (200..300).contains(&status) {
        return Ok(body);
    }
    if status == 429
        || (status == 503 && body.pointer("/error/code").and_then(Value::as_u64) == Some(131016))
    {
        return Err(ApiError::RetryAfter(Duration::from_secs(
            retry.unwrap_or(6),
        )));
    }
    if status >= 500 {
        Err(ApiError::Unknown)
    } else {
        Err(ApiError::Rejected)
    }
}
#[async_trait]
impl WhatsappApi for HttpWhatsapp {
    async fn call(&self, method: &str, body: Value) -> Result<Value, ApiError> {
        if !matches!(method, "messages" | "statuses") {
            return Err(ApiError::Rejected);
        }
        let response = self
            .client
            .post(format!("https://api.whatsapp.com/agent/v1/{method}"))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(60))
            .json(&body)
            .send()
            .await
            .map_err(|_| ApiError::Unknown)?;
        let status = response.status().as_u16();
        let retry = response
            .headers()
            .get("retry-after")
            .and_then(|s| s.to_str().ok())
            .and_then(|s| s.parse().ok());
        // Do not log request/response bodies, URLs, tokens or transport errors.
        let body = response.json().await.unwrap_or(Value::Null);
        classify(status, retry, body)
    }
}

#[derive(Default)]
struct Limits {
    messages: tokio::sync::Mutex<Option<Instant>>,
    statuses: tokio::sync::Mutex<Option<Instant>>,
    last_used: Mutex<Option<Instant>>,
}
fn limits(agent: &str) -> Arc<Limits> {
    type Entries = HashMap<String, Arc<Limits>>;
    static LIMITS: OnceLock<Mutex<Entries>> = OnceLock::new();
    let mut entries = LIMITS.get_or_init(Default::default).lock().unwrap();
    // Retain quota after long turns too: age is measured from the last API call,
    // not from when the turn originally acquired its limiter.
    entries.retain(|_, gate| {
        Arc::strong_count(gate) > 1
            || gate
                .last_used
                .lock()
                .unwrap()
                .is_some_and(|at| at.elapsed() < Duration::from_secs(120))
    });
    entries
        .entry(agent.into())
        .or_insert_with(|| Arc::new(Limits::default()))
        .clone()
}
async fn call_limited(
    api: &dyn WhatsappApi,
    limits: &Limits,
    method: &str,
    body: Value,
    stopped: &AtomicBool,
) -> Result<Value, ApiError> {
    let gate = if method == "messages" {
        &limits.messages
    } else {
        &limits.statuses
    };
    // Keep the lock through the HTTP call: same Agent/recipient sends never race.
    let mut next = gate.lock().await;
    for attempt in 0..3 {
        if let Some(at) = *next {
            tokio::time::sleep_until(at).await;
        }
        if stopped.load(Ordering::Acquire) {
            return Err(ApiError::Suppressed);
        }
        *next = Some(Instant::now() + INTERVAL);
        *limits.last_used.lock().unwrap() = Some(Instant::now());
        match api.call(method, body.clone()).await {
            Err(ApiError::RetryAfter(delay)) if attempt < 2 => {
                *next = Some(Instant::now() + delay.max(INTERVAL * (1 << attempt)));
            }
            result => return result,
        }
    }
    unreachable!()
}

pub(crate) struct WhatsappTurn {
    pub api: Arc<dyn WhatsappApi>,
    pub agent_id: String,
    pub recipient: String,
    pub inbound_message_id: String,
    pub texts: RelayTexts,
}
// Returns a byte boundary, retaining exactly the generated text for compact receipts.
fn chunk_end(text: &str, final_flush: bool) -> usize {
    if !final_flush && text.trim().is_empty() {
        return 0;
    }
    let mut units = 0;
    let hard = text
        .char_indices()
        .find_map(|(i, c)| {
            units += c.len_utf16();
            (units > CHUNK).then_some(i)
        })
        .unwrap_or(text.len());
    let window = &text[..hard];
    if final_flush && hard == text.len() {
        return hard;
    }
    if window.encode_utf16().count() < MIN_CHUNK {
        return 0;
    }
    let boundary = window
        .char_indices()
        .filter_map(|(i, c)| {
            (matches!(c, '\n' | '。' | '！' | '？')
                || (matches!(c, '.' | '!' | '?')
                    && window[i + c.len_utf8()..]
                        .chars()
                        .next()
                        .is_none_or(char::is_whitespace)))
            .then_some(i + c.len_utf8())
        })
        .next_back()
        .unwrap_or(0);
    if boundary > 0
        && !window[..boundary].trim().is_empty()
        && (final_flush || window[..boundary].encode_utf16().count() >= MIN_CHUNK)
    {
        boundary
    } else if hard < text.len() || final_flush {
        hard
    } else {
        0
    }
}

type WriteFuture = Pin<Box<dyn Future<Output = Result<String, ApiError>> + Send>>;
struct Inflight {
    text: String,
    future: WriteFuture,
}
#[derive(Default)]
struct Delivery {
    segments: Vec<DeliverySegment>,
    failed: bool,
    unknown: bool,
}
impl Delivery {
    fn record(&mut self, text: String, result: Result<String, ApiError>, pending: &mut String) {
        if matches!(result, Err(ApiError::Suppressed)) {
            pending.insert_str(0, &text);
            return;
        }
        let mut segment = DeliverySegment::new(self.segments.len(), text);
        match result {
            Ok(key) => {
                segment.message_key = Some(key);
                segment.delivered = true;
            }
            Err(ApiError::Unknown) => {
                segment.unknown = true;
                self.unknown = true;
                self.failed = true;
            }
            Err(_) => {
                self.failed = true;
            }
        }
        self.segments.push(segment);
    }
}
impl WhatsappTurn {
    fn write(&self, text: String, limits: Arc<Limits>, stopped: Arc<AtomicBool>) -> Inflight {
        let api = self.api.clone();
        let body = json!({"messaging_product":"whatsapp","to":self.recipient,"type":"text",
            "context":{"message_id":self.inbound_message_id},"text":{"body":text,"preview_url":false}});
        Inflight {
            text,
            future: Box::pin(async move {
                let value = call_limited(api.as_ref(), &limits, "messages", body, &stopped).await?;
                value
                    .pointer("/messages/0/id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .ok_or(ApiError::Unknown)
            }),
        }
    }
    fn typing(
        &self,
        limits: Arc<Limits>,
        mut stop: watch::Receiver<bool>,
        refresh: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        let api = self.api.clone();
        let body = json!({"messaging_product":"whatsapp","status":"read","message_id":self.inbound_message_id,"typing_indicator":{"type":"text"}});
        tokio::spawn(async move {
            let never_stopped = AtomicBool::new(false);
            let mut next = Instant::now();
            loop {
                tokio::select! {
                    biased;
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep_until(next) => {
                        tokio::select! {
                            biased;
                            _ = stop.changed() => break,
                            _ = call_limited(api.as_ref(), &limits, "statuses", body.clone(), &never_stopped) => {}
                        }
                        next = Instant::now() + TYPING_REFRESH;
                    }
                    _ = refresh.notified() => { next = Instant::now(); }
                }
            }
        })
    }
    pub(crate) async fn run(self, mut rx: mpsc::UnboundedReceiver<RelayInput>) -> TurnReport {
        let limits = limits(&self.agent_id);
        let stopped = Arc::new(AtomicBool::new(false));
        let (typing_stop, stop_rx) = watch::channel(false);
        let refresh = Arc::new(Notify::new());
        let typing = self.typing(limits.clone(), stop_rx, refresh.clone());
        let mut full = String::new();
        let mut pending = String::new();
        let mut delivery = Delivery::default();
        let mut inflight: Option<Inflight> = None;
        let mut flush_at = Instant::now() + COALESCE;
        let mut cancel_pending = false;
        let deadline = Instant::now() + super::turn_relay::MAX_TURN;
        let outcome = loop {
            let ready = !cancel_pending
                && !delivery.failed
                && inflight.is_none()
                && chunk_end(&pending, false) > 0;
            tokio::select! {
                biased;
                input = rx.recv() => match input {
                    None => break Outcome::Aborted,
                    Some(RelayInput::Lagged) => break Outcome::Lagged,
                    Some(RelayInput::SilentComplete) => break Outcome::Silent,
                    Some(RelayInput::Event(env)) => match &env.payload {
                        AcpEvent::ContentDelta { text, parent_tool_use_id: None } => { full.push_str(text); pending.push_str(text); }
                        AcpEvent::Error { terminal: false, code, .. } if code.as_deref() == Some("runtime_cancel_pending") => {
                            cancel_pending = true;
                            stopped.store(true, Ordering::Release);
                            let _ = typing_stop.send(true);
                        }
                        AcpEvent::TurnComplete { stop_reason, .. } => break match stop_reason.as_str() {
                            "end_turn" => Outcome::Completed, "cancelled" => Outcome::Cancelled, _ => Outcome::Failed,
                        },
                        AcpEvent::Error { terminal: true, code, .. } if code.as_deref() == Some("runtime_outcome_unknown") => break Outcome::Unknown,
                        AcpEvent::Error { terminal: true, .. } | AcpEvent::StatusChanged { status: ConnectionStatus::Disconnected | ConnectionStatus::Error } => break Outcome::Failed,
                        _ => {} // tool input/output and reasoning are never answer text
                    }
                },
                result = async { inflight.as_mut().unwrap().future.as_mut().await }, if inflight.is_some() => {
                    let write = inflight.take().unwrap();
                    if result.is_ok() { refresh.notify_one(); }
                    delivery.record(write.text, result, &mut pending);
                    flush_at = Instant::now() + COALESCE;
                }
                _ = tokio::time::sleep_until(flush_at), if ready => {
                    let end = chunk_end(&pending, false);
                    let text = pending.drain(..end).collect();
                    inflight = Some(self.write(text, limits.clone(), stopped.clone()));
                }
                _ = tokio::time::sleep_until(deadline) => break if cancel_pending { Outcome::Unknown } else { Outcome::Failed },
            }
        };
        // Keep normal-completion loading through rate-limited final delivery.
        // There is no stop-typing API: after refresh stops, a reply clears it or
        // the platform expires it within 25 seconds when delivery is uncertain.
        let keep_typing = matches!(outcome, Outcome::Completed | Outcome::Silent)
            && !cancel_pending
            && !delivery.failed;
        if !keep_typing {
            let _ = typing_stop.send(true);
        }
        stopped.store(true, Ordering::Release);
        if let Some(mut write) = inflight {
            // Never drop/retry an HTTP request whose delivery may already have happened.
            let result = write.future.as_mut().await;
            let sent = result.is_ok();
            delivery.record(write.text, result, &mut pending);
            if delivery.failed {
                let _ = typing_stop.send(true);
            } else if sent && keep_typing && !pending.is_empty() {
                refresh.notify_one();
            }
        }
        let final_delivery = !matches!(
            outcome,
            Outcome::Aborted | Outcome::Lagged | Outcome::Unknown
        );
        if final_delivery {
            let note = match outcome {
                Outcome::Cancelled => Some(&self.texts.cancelled),
                Outcome::Failed => Some(&self.texts.failed),
                Outcome::Completed if full.trim().is_empty() => Some(&self.texts.empty),
                _ => None,
            };
            if let Some(note) = note {
                let text = if full.is_empty() {
                    note.clone()
                } else {
                    format!("\n\n{note}")
                };
                full.push_str(&text);
                pending.push_str(&text);
            }
        }
        while !pending.is_empty() {
            let end = chunk_end(&pending, true);
            let text: String = pending.drain(..end).collect();
            if text.trim().is_empty() {
                continue;
            }
            if final_delivery && !delivery.failed {
                let mut write = self.write(text, limits.clone(), Arc::new(AtomicBool::new(false)));
                let result = write.future.as_mut().await;
                let sent = result.is_ok();
                delivery.record(write.text, result, &mut pending);
                if delivery.failed {
                    let _ = typing_stop.send(true);
                } else if sent && keep_typing && !pending.is_empty() {
                    refresh.notify_one();
                }
            } else {
                delivery
                    .segments
                    .push(DeliverySegment::new(delivery.segments.len(), text));
            }
        }
        let _ = typing_stop.send(true);
        let _ = typing.await;
        TurnReport::new(outcome, delivery.segments, full, delivery.unknown)
    }
}

#[cfg(test)]
mod tests;
