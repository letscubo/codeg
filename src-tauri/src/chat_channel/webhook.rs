//! Outbound webhook delivery for the global chat-channel event feed.
//!
//! Webhooks are a channel-agnostic event sink: when an ACP event passes the
//! global event filter (and the bridged-permission suppression), the event
//! subscriber POSTs a structured JSON payload to every configured URL — in
//! addition to the IM channel fan-out. Unlike IM channels, webhooks are NOT
//! debounced and do NOT participate in the per-channel filter; an automation
//! consumer wants the complete event stream.
//!
//! Delivery is fire-and-forget (`tokio::spawn` per URL) so a slow or
//! unreachable endpoint never stalls the event subscriber loop.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::types::RichMessage;

/// Who/where an event came from, as far as the webhook consumer is concerned.
///
/// `conversation_id` is the only REQUIRED field: it is what lets a consumer say
/// which conversation an event belongs to, and every connection has one once
/// linked. The channel fields are `Option` because a connection need not sit
/// behind an IM channel at all — an automation or work-task run has a
/// conversation but no channel, sender or chat. Before those were optional such
/// a run got no context at all, and so no `conversation_id` on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEventContext {
    pub channel_id: Option<i32>,
    pub channel_type: Option<String>,
    pub conversation_id: i32,
    pub sender_id: Option<String>,
    pub chat_id: Option<String>,
    pub thread_key: Option<String>,
    pub thread_kind: Option<String>,
    pub scope: Option<String>,
    /// IANA timezone of the automation that produced this conversation (e.g.
    /// "Asia/Shanghai"); `None` when the conversation isn't an automation's.
    ///
    /// Every other timestamp on the wire is UTC and stays UTC — this is not a
    /// second opinion about `occurred_at`, it is the clock the user wrote the
    /// schedule against, shipped so a consumer can render a run's time as the
    /// user thinks of it. Without it a consumer has to guess: the platform side
    /// stores no user timezone anywhere, leaving it to hardcode one (wrong for
    /// every other tenant) or print UTC (silently 8 hours off in Asia/Shanghai).
    pub timezone: Option<String>,
}

/// One configured webhook sink. Persisted (as a JSON array) under the
/// `chat_event_webhooks` app-metadata key and mirrored on the frontend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    pub enabled: bool,
}

/// Parse the stored webhook config JSON and return the URLs of ENABLED entries
/// only — the set the event subscriber actually delivers to. Unparseable input
/// yields an empty list (treated as "no webhooks").
pub fn enabled_webhook_urls(json: &str) -> Vec<String> {
    serde_json::from_str::<Vec<WebhookConfig>>(json)
        .map(|list| {
            list.into_iter()
                .filter(|w| w.enabled)
                .map(|w| w.url)
                .collect()
        })
        .unwrap_or_default()
}

/// Build the JSON body POSTed to each webhook for one event.
///
/// Pure (no I/O, no clock) so the wire contract is unit-testable. `title`,
/// `body` and the `fields` labels are localized per the chat message-language
/// setting (same text IM channels receive); `event`, `level` and `source` are
/// stable machine-readable values.
pub fn build_webhook_payload(
    event_type: &str,
    connection_id: &str,
    msg: &RichMessage,
) -> serde_json::Value {
    build_webhook_payload_with_context(event_type, connection_id, msg, None)
}

pub fn build_webhook_payload_with_context(
    event_type: &str,
    connection_id: &str,
    msg: &RichMessage,
    context: Option<&WebhookEventContext>,
) -> serde_json::Value {
    let fields: Vec<serde_json::Value> = msg
        .fields
        .iter()
        .map(|(label, value)| serde_json::json!({ "label": label, "value": value }))
        .collect();

    let mut payload = serde_json::json!({
        "event_id": uuid::Uuid::new_v4().to_string(),
        "event": event_type,
        "level": msg.level,
        "title": msg.title,
        "body": msg.body,
        "fields": fields,
        "connection_id": connection_id,
        "occurred_at": chrono::Utc::now().to_rfc3339(),
        "source": "codeg",
    });

    if let Some(context) = context {
        payload["channel_id"] = serde_json::json!(context.channel_id);
        payload["channel_type"] = serde_json::json!(context.channel_type);
        payload["conversation_id"] = serde_json::json!(context.conversation_id);
        payload["sender_id"] = serde_json::json!(context.sender_id);
        payload["chat_id"] = serde_json::json!(context.chat_id);
        payload["thread_key"] = serde_json::json!(context.thread_key);
        payload["thread_kind"] = serde_json::json!(context.thread_kind);
        payload["scope"] = serde_json::json!(context.scope);
        payload["timezone"] = serde_json::json!(context.timezone);
    }

    payload
}

/// Build the shared reqwest client used for webhook delivery. Mirrors the
/// timeout posture of the IM backends (see `backends/telegram.rs`).
pub fn make_webhook_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap_or_default()
}

/// Fan the payload out to every URL on detached tasks. Returns immediately;
/// failures are logged, not surfaced (the event loop must not block on, or be
/// failed by, an unreachable consumer).
pub fn spawn_webhook_delivery(
    client: reqwest::Client,
    urls: Vec<String>,
    payload: serde_json::Value,
) {
    for url in urls {
        let client = client.clone();
        let payload = payload.clone();
        tokio::spawn(async move {
            if let Err(e) = post_one(&client, &url, &payload).await {
                // Redact: webhook URLs often carry secrets in the path/query.
                tracing::error!(
                    "[ChatChannel] webhook delivery to {} failed: {e}",
                    redact_url(&url)
                );
            }
        });
    }
}

/// Reduce a URL to `scheme://host[:port]` for logging, dropping the path,
/// query and any userinfo — webhook URLs frequently embed credentials there
/// (e.g. Slack/Discord tokens) which must not reach logs. Unparseable input
/// collapses to a non-revealing placeholder.
///
/// `pub(crate)` for `myclaw_route`, which logs failures against a URL derived
/// from this same webhook config and therefore carrying the same secret. One
/// implementation rather than two: a second copy is a second thing to forget.
pub(crate) fn redact_url(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(u) => match (u.host_str(), u.port()) {
            (Some(host), Some(port)) => format!("{}://{host}:{port}", u.scheme()),
            (Some(host), None) => format!("{}://{host}", u.scheme()),
            (None, _) => "<webhook>".to_string(),
        },
        Err(_) => "<webhook>".to_string(),
    }
}

/// POST one payload to one URL, mapping transport errors and non-2xx
/// responses to a `String` for logging.
async fn post_one(
    client: &reqwest::Client,
    url: &str,
    payload: &serde_json::Value,
) -> Result<(), String> {
    let resp = client
        .post(url)
        .json(payload)
        .send()
        .await
        // `reqwest::Error`'s Display embeds the request URL ("... for url (...)"),
        // which would re-leak path/query secrets the explicit redaction strips.
        // `without_url()` removes it before stringifying.
        .map_err(|e| e.without_url().to_string())?;

    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat_channel::types::MessageLevel;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn sample_msg() -> RichMessage {
        RichMessage {
            title: Some("Turn Complete".into()),
            body: "Claude Code finished its turn.".into(),
            fields: vec![("Stop Reason".into(), "End Turn".into())],
            level: MessageLevel::Info,
        }
    }

    #[test]
    fn payload_has_stable_envelope_and_localized_text() {
        let payload = build_webhook_payload("turn_complete", "conn-abc", &sample_msg());
        assert_eq!(payload["event"], "turn_complete");
        assert_eq!(payload["level"], "info");
        assert_eq!(payload["title"], "Turn Complete");
        assert_eq!(payload["body"], "Claude Code finished its turn.");
        assert_eq!(payload["connection_id"], "conn-abc");
        assert_eq!(payload["source"], "codeg");
        assert_eq!(payload["fields"][0]["label"], "Stop Reason");
        assert_eq!(payload["fields"][0]["value"], "End Turn");
    }

    #[test]
    fn payload_level_tracks_message_level() {
        let err = RichMessage {
            title: Some("Agent Error".into()),
            body: "boom".into(),
            fields: vec![],
            level: MessageLevel::Error,
        };
        let payload = build_webhook_payload("error", "c", &err);
        assert_eq!(payload["level"], "error");
        assert_eq!(payload["title"], "Agent Error");
        assert!(payload["fields"].as_array().unwrap().is_empty());
    }

    #[test]
    fn payload_includes_chat_session_correlation() {
        let context = WebhookEventContext {
            channel_id: Some(7),
            channel_type: Some("telegram".to_string()),
            conversation_id: 42,
            sender_id: Some("1001".to_string()),
            chat_id: Some("1001".to_string()),
            thread_key: Some("1001".to_string()),
            thread_kind: Some("telegram_direct".to_string()),
            scope: Some("direct".to_string()),
            timezone: None,
        };

        let payload = build_webhook_payload_with_context(
            "turn_complete",
            "conn-abc",
            &sample_msg(),
            Some(&context),
        );

        assert_eq!(payload["channel_id"], 7);
        assert_eq!(payload["channel_type"], "telegram");
        assert_eq!(payload["conversation_id"], 42);
        assert_eq!(payload["chat_id"], "1001");
        assert_eq!(payload["scope"], "direct");
        assert!(payload["event_id"].as_str().is_some());
        assert!(payload["occurred_at"].as_str().is_some());
    }

    /// A run with no IM channel behind it — an automation or work task — still
    /// has to say WHICH conversation the event belongs to. Before the channel
    /// fields were optional such a run produced no context at all, so its
    /// payload carried no `conversation_id` and a consumer could not correlate
    /// the event with anything; a scheduled-run consumer was left unable to act.
    #[test]
    fn payload_carries_conversation_without_a_channel() {
        let context = WebhookEventContext {
            channel_id: None,
            channel_type: None,
            conversation_id: 56,
            sender_id: None,
            chat_id: None,
            thread_key: None,
            thread_kind: None,
            scope: None,
            timezone: Some("Asia/Shanghai".to_string()),
        };

        let payload = build_webhook_payload_with_context(
            "turn_complete",
            "conn-automation",
            &sample_msg(),
            Some(&context),
        );

        assert_eq!(payload["conversation_id"], 56);
        // Present-but-null, not absent: the consumer's channel branch must read
        // "no channel", and `null` is what distinguishes that from "unknown".
        assert!(payload["channel_id"].is_null());
        assert!(payload["channel_type"].is_null());
        assert!(payload["sender_id"].is_null());
    }

    /// The automation's own cron timezone rides along so a consumer can render
    /// the run's time as the user thinks of it. Without it the consumer has
    /// nothing to go on — the platform side stores no user timezone anywhere —
    /// and must either hardcode one (wrong for every other tenant) or print UTC,
    /// which reads 8 hours off for a schedule written in Asia/Shanghai.
    #[test]
    fn payload_carries_the_automations_timezone() {
        let context = WebhookEventContext {
            channel_id: None,
            channel_type: None,
            conversation_id: 57,
            sender_id: None,
            chat_id: None,
            thread_key: None,
            thread_kind: None,
            scope: None,
            timezone: Some("Asia/Shanghai".to_string()),
        };

        let payload = build_webhook_payload_with_context(
            "turn_complete",
            "conn-automation",
            &sample_msg(),
            Some(&context),
        );

        assert_eq!(payload["timezone"], "Asia/Shanghai");
        // `occurred_at` stays UTC — the timezone is a rendering hint about the
        // schedule, never a second opinion about when the event happened.
        assert!(payload["occurred_at"].as_str().unwrap().ends_with("+00:00"));
    }

    /// A conversation no automation produced (every IM-channel and desktop
    /// session) has no schedule, so it has no timezone. Present-but-null, like
    /// the channel fields: the consumer reads "no timezone", not "unknown".
    #[test]
    fn timezone_is_null_for_a_conversation_no_automation_produced() {
        let context = WebhookEventContext {
            channel_id: Some(7),
            channel_type: Some("telegram".to_string()),
            conversation_id: 42,
            sender_id: Some("1001".to_string()),
            chat_id: Some("1001".to_string()),
            thread_key: Some("1001".to_string()),
            thread_kind: Some("telegram_direct".to_string()),
            scope: Some("direct".to_string()),
            timezone: None,
        };

        let payload = build_webhook_payload_with_context(
            "turn_complete",
            "conn-abc",
            &sample_msg(),
            Some(&context),
        );

        assert!(payload["timezone"].is_null());
    }

    #[test]
    fn enabled_webhook_urls_keeps_only_enabled() {
        let json = r#"[
            {"url":"https://a.test/h","enabled":true},
            {"url":"https://b.test/h","enabled":false},
            {"url":"https://c.test/h","enabled":true}
        ]"#;
        assert_eq!(
            enabled_webhook_urls(json),
            vec![
                "https://a.test/h".to_string(),
                "https://c.test/h".to_string()
            ]
        );
        assert!(enabled_webhook_urls("not json").is_empty());
        assert!(enabled_webhook_urls("[]").is_empty());
    }

    #[test]
    fn redact_url_keeps_only_scheme_host_port() {
        assert_eq!(
            redact_url("https://hooks.slack.com/services/T000/B000/XXXXsecret"),
            "https://hooks.slack.com"
        );
        assert_eq!(
            redact_url("http://192.168.1.10:9000/in?token=abc"),
            "http://192.168.1.10:9000"
        );
        // userinfo and path/query are dropped, not surfaced
        assert_eq!(
            redact_url("https://user:pass@host.test/p?q=1"),
            "https://host.test"
        );
        assert_eq!(redact_url("not a url"), "<webhook>");
    }

    #[test]
    fn payload_title_null_when_absent() {
        let msg = RichMessage::info("just a body");
        let payload = build_webhook_payload("permission_request", "c", &msg);
        assert!(payload["title"].is_null());
        assert_eq!(payload["event"], "permission_request");
    }

    /// Read a full HTTP/1.1 request (headers + Content-Length body) from a
    /// loopback connection, then write a minimal 200 so the client's
    /// `send().await` resolves. Returns the raw request text.
    async fn read_request_and_respond(mut stream: tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            // Stop once headers are in and the declared body has arrived.
            if let Some(header_end) = find_header_end(&buf) {
                let len = content_length(&buf[..header_end]);
                if buf.len() >= header_end + len {
                    break;
                }
            }
            let n = stream.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await;
        let _ = stream.flush().await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
    }

    fn content_length(headers: &[u8]) -> usize {
        let text = String::from_utf8_lossy(headers).to_lowercase();
        for line in text.lines() {
            if let Some(v) = line.strip_prefix("content-length:") {
                if let Ok(n) = v.trim().parse::<usize>() {
                    return n;
                }
            }
        }
        0
    }

    #[tokio::test]
    async fn post_one_sends_post_with_json_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            read_request_and_respond(stream).await
        });

        let client = make_webhook_client();
        let payload = build_webhook_payload("turn_complete", "conn-1", &sample_msg());
        post_one(&client, &format!("http://{addr}/hook"), &payload)
            .await
            .expect("post should succeed");

        let request = server.await.unwrap();
        assert!(request.starts_with("POST /hook"), "got: {request}");
        assert!(
            request
                .to_lowercase()
                .contains("content-type: application/json"),
            "missing json content-type: {request}"
        );
        assert!(
            request.contains("\"event\":\"turn_complete\""),
            "got: {request}"
        );
        assert!(
            request.contains("\"connection_id\":\"conn-1\""),
            "got: {request}"
        );
    }

    #[tokio::test]
    async fn post_one_error_does_not_leak_url_secrets() {
        // Bind then drop to obtain a port nothing listens on → connection refused.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let client = make_webhook_client();
        let payload = serde_json::json!({ "event": "error" });
        let url = format!("http://{addr}/services/T0/B0/SECRETPATH?token=SECRETQUERY");
        let err = post_one(&client, &url, &payload)
            .await
            .expect_err("connection should be refused");
        assert!(!err.contains("SECRETPATH"), "leaked path secret: {err}");
        assert!(!err.contains("SECRETQUERY"), "leaked query secret: {err}");
    }

    #[tokio::test]
    async fn post_one_reports_non_2xx() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut chunk = [0u8; 1024];
            let _ = stream.read(&mut chunk).await;
            let _ = stream
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
                .await;
            let _ = stream.flush().await;
        });

        let client = make_webhook_client();
        let payload = serde_json::json!({ "event": "error" });
        let err = post_one(&client, &format!("http://{addr}/"), &payload)
            .await
            .expect_err("non-2xx should error");
        assert!(err.contains("500"), "got: {err}");
    }
}
