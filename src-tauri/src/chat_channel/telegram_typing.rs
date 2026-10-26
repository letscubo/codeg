//! Best-effort native typing, scoped to one Telegram relay turn.
//! Telegram expires chat actions after at most five seconds; there is no stop
//! action. Stop renewing as soon as a terminal input arrives, even if a draft
//! or body request is still waiting. No detached task survives the relay.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::turn_relay::{RelayInput, TelegramTurn, TgApi, TgError, TurnReport};
use crate::acp::types::{AcpEvent, ConnectionStatus};

const HEARTBEAT: Duration = Duration::from_secs(4);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// A newly sent message clears Telegram's typing indicator. Coalesce requests
/// to restore it while generation is still active, without touching delivery.
struct RefreshAfterMessage {
    inner: Arc<dyn TgApi>,
    refresh: mpsc::Sender<()>,
}

#[async_trait]
impl TgApi for RefreshAfterMessage {
    async fn call(&self, method: &str, body: serde_json::Value) -> Result<Option<i64>, TgError> {
        let result = self.inner.call(method, body).await;
        if method == "sendMessage" && result.is_ok() {
            let _ = self.refresh.try_send(());
        }
        result
    }
}

fn terminal(input: &RelayInput) -> bool {
    match input {
        RelayInput::Lagged | RelayInput::SilentComplete => true,
        RelayInput::Event(event) => matches!(
            event.payload,
            AcpEvent::TurnComplete { .. }
                | AcpEvent::Error { terminal: true, .. }
                | AcpEvent::StatusChanged {
                    status: ConnectionStatus::Disconnected | ConnectionStatus::Error
                }
        ),
    }
}

async fn forward(
    mut input: mpsc::UnboundedReceiver<RelayInput>,
    output: mpsc::UnboundedSender<RelayInput>,
    stopped: oneshot::Sender<()>,
) {
    let mut stopped = Some(stopped);
    while let Some(event) = input.recv().await {
        let done = terminal(&event);
        let cancel_pending = matches!(&event, RelayInput::Event(event)
            if matches!(&event.payload, AcpEvent::Error { terminal: false, code: Some(code), .. }
                if code == "runtime_cancel_pending"));
        if done || cancel_pending {
            // Stop typing immediately on cancellation intent, but continue
            // forwarding until the runtime supplies its real terminal verdict.
            drop(stopped.take());
        }
        if output.send(event).is_err() || done {
            break;
        }
    }
    // Dropping also covers an input channel closing without a terminal event.
    drop(stopped);
}

async fn heartbeat(
    api: Arc<dyn TgApi>,
    chat_id: i64,
    message_thread_id: Option<i64>,
    stopped: oneshot::Receiver<()>,
    mut refresh: mpsc::Receiver<()>,
) {
    let renew = async {
        loop {
            // A refresh received during a request/backoff is covered by this
            // next attempt; it must not cause a second immediate request.
            while refresh.try_recv().is_ok() {}
            let started = Instant::now();
            let mut body = super::turn_relay::telegram_target(chat_id, message_thread_id);
            body["action"] = "typing".into();
            let result =
                tokio::time::timeout(REQUEST_TIMEOUT, api.call("sendChatAction", body)).await;
            match result {
                Ok(Err(TgError::RetryAfter(seconds))) => {
                    // Message notifications must not bypass Telegram's 429.
                    tokio::time::sleep(Duration::from_secs(seconds).max(HEARTBEAT)).await;
                    continue;
                }
                Ok(Err(TgError::Other(_))) => {
                    // Unsupported/forbidden chat actions do not fail delivery.
                    tracing::debug!("[TurnRelay] Telegram typing unavailable for this turn");
                    return;
                }
                // Transient network errors/timeouts retry on the normal beat.
                _ => {}
            }
            tokio::select! {
                _ = tokio::time::sleep_until(started + HEARTBEAT) => {}
                Some(()) = refresh.recv() => {
                    tokio::time::sleep_until(started + MIN_REFRESH_INTERVAL).await;
                }
            }
        }
    };
    tokio::select! {
        biased;
        _ = stopped => {}
        _ = tokio::time::sleep(super::turn_relay::MAX_TURN) => {}
        _ = renew => {}
    }
}

pub(super) async fn run(
    mut turn: TelegramTurn,
    input: mpsc::UnboundedReceiver<RelayInput>,
) -> TurnReport {
    let api = Arc::clone(&turn.api);
    let chat_id = turn.chat_id;
    let message_thread_id = turn.message_thread_id;
    let (refresh_tx, refresh_rx) = mpsc::channel(1);
    turn.api = Arc::new(RefreshAfterMessage {
        inner: Arc::clone(&api),
        refresh: refresh_tx,
    });
    let (forward_tx, forward_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = oneshot::channel();
    let activity = async {
        tokio::join!(
            biased;
            forward(input, forward_tx, stop_tx),
            heartbeat(api, chat_id, message_thread_id, stop_rx, refresh_rx),
        );
    };
    let delivery = turn.run_delivery(forward_rx);
    tokio::pin!(delivery);
    tokio::select! {
        biased;
        // Observe terminal inputs before polling delivery or another renewal.
        _ = activity => delivery.await,
        report = &mut delivery => report,
    }
}

#[cfg(test)]
mod tests;
