//! Per-operation channel destinations. These are never global event subscribers.
//! Only a non-secret immutable origin/VM binding is persisted; credentials stay
//! owned by the existing platform webhook configuration.
use crate::{
    commands::myclaw_platform::{self, PlatformEndpoint},
    db::{service::app_metadata_service, AppDatabase},
};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::Duration;

const PREFIX: &str = "channel_callback_target:";
const INVALID: &str = "invalid_channel_callback_target";
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Target {
    channel_type: String,
    origin: String,
    vm_id: String,
}

fn normalize_origin(value: &str) -> Result<String, &'static str> {
    let url = reqwest::Url::parse(value).map_err(|_| INVALID)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || value.trim() != value
        || value.contains('\\')
        || value.contains('@')
        || value.chars().any(char::is_control)
        || value.strip_prefix("https://").is_none_or(|authority| {
            authority
                .strip_suffix('/')
                .unwrap_or(authority)
                .contains('/')
        })
    {
        return Err(INVALID);
    }
    Ok(url.origin().ascii_serialization())
}
fn target_id(target: &Target) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(target).unwrap()))
}
async fn platform(db: &DatabaseConnection) -> Result<PlatformEndpoint, String> {
    let hooks = app_metadata_service::get_value(db, super::event_subscriber::EVENT_WEBHOOKS_KEY)
        .await
        .map_err(|_| "channel_callback_storage_unavailable")?
        .ok_or(INVALID)?;
    let hooks: Vec<super::webhook::WebhookConfig> =
        serde_json::from_str(&hooks).map_err(|_| INVALID)?;
    myclaw_platform::endpoint_from_hooks(&hooks).ok_or_else(|| INVALID.into())
}
pub async fn register(
    db: &DatabaseConnection,
    channel_type: &str,
    origin: &str,
) -> Result<String, String> {
    if !matches!(channel_type, "slack" | "whatsapp_agent") {
        return Err(INVALID.into());
    }
    let origin = normalize_origin(origin)?;
    let platform = platform(db).await?;
    let target = Target {
        channel_type: channel_type.into(),
        origin,
        vm_id: platform.vm_id,
    };
    let id = target_id(&target);
    let key = format!("{PREFIX}{id}");
    let encoded = serde_json::to_string(&target).unwrap();
    // Same binding reuses its record. Changing origin produces a new immutable
    // ID, so queued operations and retries retain the original destination.
    if let Some(stored) = app_metadata_service::get_value(db, &key)
        .await
        .map_err(|_| "channel_callback_storage_unavailable")?
    {
        if stored != encoded {
            return Err(INVALID.into());
        }
    } else {
        app_metadata_service::upsert_value(db, &key, &encoded)
            .await
            .map_err(|_| "channel_callback_storage_unavailable")?;
    }
    Ok(id)
}
pub(crate) async fn endpoint(
    db: &DatabaseConnection,
    id: &str,
    channel_type: &str,
) -> Result<PlatformEndpoint, String> {
    if !matches!(channel_type, "slack" | "whatsapp_agent") {
        return Err(INVALID.into());
    }
    endpoint_for_target(db, id, Some(channel_type)).await
}
async fn endpoint_for_target(
    db: &DatabaseConnection,
    id: &str,
    expected_channel: Option<&str>,
) -> Result<PlatformEndpoint, String> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(INVALID.into());
    }
    let stored = app_metadata_service::get_value(db, &format!("{PREFIX}{id}"))
        .await
        .map_err(|_| "channel_callback_storage_unavailable")?
        .ok_or(INVALID)?;
    let target: Target = serde_json::from_str(&stored).map_err(|_| INVALID)?;
    let current = platform(db).await?;
    if !matches!(target.channel_type.as_str(), "slack" | "whatsapp_agent")
        || expected_channel.is_some_and(|channel| target.channel_type != channel)
        || target.vm_id != current.vm_id
        || target_id(&target) != id
        || normalize_origin(&target.origin)? != target.origin
    {
        return Err(INVALID.into());
    }
    Ok(PlatformEndpoint {
        base: target.origin,
        ..current
    })
}
pub(crate) async fn post_signed(
    db: &AppDatabase,
    id: Option<&str>,
    channel_type: &str,
    path: &str,
    body: Value,
    timeout: Duration,
) -> Result<Value, String> {
    match id {
        Some(id) => {
            let ep = endpoint(&db.conn, id, channel_type).await?;
            myclaw_platform::post_signed_endpoint(&ep, path, body, timeout).await
        }
        None => myclaw_platform::post_signed(db, path, body, timeout).await,
    }
}
pub(crate) async fn result_url(db: &DatabaseConnection, id: &str) -> Result<String, String> {
    let ep = endpoint_for_target(db, id, None).await?;
    let mut url =
        reqwest::Url::parse(&format!("{}/api/codeg/events", ep.base)).map_err(|_| INVALID)?;
    url.query_pairs_mut()
        .append_pair("vmId", &ep.vm_id)
        .append_pair("s", &ep.secret);
    Ok(url.to_string())
}

const PROMPT_PREFIX: &str = "channel_prompt_callback:";
#[derive(Serialize, Deserialize)]
struct PromptTargets {
    process_id: String,
    first_seq: u64,
    boundaries: Vec<PromptBoundary>,
}
#[derive(Serialize, Deserialize)]
struct PromptBoundary {
    start_seq: u64,
    #[serde(default)]
    reservation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    end_seq: Option<u64>,
    target_id: Option<String>,
}
static PROMPT_WRITES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
fn revoked_pins() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static REVOKED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    REVOKED.get_or_init(Default::default)
}
fn process_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| uuid::Uuid::new_v4().to_string())
}
/// Called with the same prompt lock used to submit the turn. A following normal
/// web/Telegram turn records a None boundary so it cannot inherit Slack routing.
pub(crate) async fn pin_prompt(
    state: &crate::app_state::AppState,
    connection_id: &str,
    target_id: Option<&str>,
    outbound: Option<&super::turn_relay::OutboundSpec>,
) -> Result<Option<String>, String> {
    let target_id = target_id.or(match outbound {
        Some(super::turn_relay::OutboundSpec::Slack {
            result_callback_target_id,
            ..
        })
        | Some(super::turn_relay::OutboundSpec::WhatsappAgent {
            result_callback_target_id,
            ..
        }) => result_callback_target_id.as_deref(),
        _ => None,
    });
    if let Some(id) = target_id {
        match outbound {
            Some(super::turn_relay::OutboundSpec::WhatsappAgent { .. }) => {
                endpoint(&state.db.conn, id, "whatsapp_agent").await?;
            }
            Some(_) => {
                endpoint(&state.db.conn, id, "slack").await?;
            }
            None => {
                endpoint_for_target(&state.db.conn, id, None).await?;
            }
        }
        match outbound {
            Some(super::turn_relay::OutboundSpec::Telegram { .. }) => return Err(INVALID.into()),
            Some(super::turn_relay::OutboundSpec::Slack {
                result_webhook_url: Some(_),
                ..
            })
            | Some(super::turn_relay::OutboundSpec::WhatsappAgent {
                result_webhook_url: Some(_),
                ..
            }) => return Err(INVALID.into()),
            Some(super::turn_relay::OutboundSpec::Slack {
                delivery_version, ..
            })
            | Some(super::turn_relay::OutboundSpec::WhatsappAgent {
                delivery_version, ..
            }) if *delivery_version < 2 => return Err(INVALID.into()),
            Some(super::turn_relay::OutboundSpec::Slack {
                result_callback_target_id: Some(result_id),
                ..
            })
            | Some(super::turn_relay::OutboundSpec::WhatsappAgent {
                result_callback_target_id: Some(result_id),
                ..
            }) if result_id != id => return Err(INVALID.into()),
            _ => {}
        }
    }
    if let Some(spec) = outbound {
        if super::relay_outbox::get(&state.db.conn, spec.turn_id())
            .await?
            .is_some()
        {
            // Preserve the existing duplicate-turn response and never alter its
            // original destination before register_for_prompt rejects it.
            return Ok(None);
        }
    }
    let _write = PROMPT_WRITES.lock().await;
    let key = format!("{PROMPT_PREFIX}{connection_id}");
    let previous = app_metadata_service::get_value(&state.db.conn, &key)
        .await
        .map_err(|_| "channel_callback_storage_unavailable")?;
    if previous.is_none() && target_id.is_none() {
        return Ok(None);
    }
    let session = state
        .connection_manager
        .get_state(connection_id)
        .await
        .ok_or("relay_connection_not_found")?;
    let session = session.read().await;
    if session.turn_in_flight {
        return Err("relay_turn_in_progress".into());
    }
    let start_seq = session.event_seq;
    drop(session);
    let mut targets = match previous {
        Some(value) => serde_json::from_str::<PromptTargets>(&value)
            .map_err(|_| "channel_callback_storage_unavailable")?,
        None => PromptTargets {
            process_id: process_id().into(),
            first_seq: start_seq,
            boundaries: Vec::new(),
        },
    };
    if targets.process_id != process_id() {
        targets = PromptTargets {
            process_id: process_id().into(),
            first_seq: start_seq,
            boundaries: Vec::new(),
        };
    }
    // A rejected prompt may have consumed no sequence number. Reusing this
    // boundary is safe only before a newer event exists.
    targets
        .boundaries
        .retain(|entry| entry.start_seq < start_seq);
    let reservation = uuid::Uuid::new_v4().to_string();
    targets.boundaries.push(PromptBoundary {
        reservation: reservation.clone(),
        start_seq,
        end_seq: None,
        target_id: target_id.map(str::to_owned),
    });
    if targets.boundaries.len() > 64 {
        targets.boundaries.remove(0);
    }
    app_metadata_service::upsert_value(
        &state.db.conn,
        &key,
        &serde_json::to_string(&targets).unwrap(),
    )
    .await
    .map_err(|_| "channel_callback_storage_unavailable".to_string())?;
    Ok(Some(reservation))
}
/// None is an ordinary event; Some(None) suppresses platform delivery when a
/// persisted boundary cannot safely be resolved. This never falls back to the
/// current global endpoint after a restart, corrupt row, or lost boundary.
pub(crate) async fn prompt_target(
    db: &DatabaseConnection,
    connection_id: &str,
    seq: u64,
) -> Option<Option<String>> {
    let stored =
        match app_metadata_service::get_value(db, &format!("{PROMPT_PREFIX}{connection_id}")).await
        {
            Ok(None) => return None,
            Ok(Some(value)) => value,
            Err(_) => return Some(None),
        };
    let Ok(targets) = serde_json::from_str::<PromptTargets>(&stored) else {
        return Some(None);
    };
    if targets.process_id != process_id() {
        return Some(None);
    }
    if seq <= targets.first_seq {
        return None;
    }
    match targets
        .boundaries
        .iter()
        .rev()
        .find(|entry| seq > entry.start_seq)
    {
        Some(entry) if revoked_pins().lock().unwrap().contains(&entry.reservation) => None,
        Some(entry) if entry.end_seq.is_some_and(|end| seq > end) => None,
        Some(entry) => entry.target_id.clone().map(Some),
        None => Some(None),
    }
}
/// Close exactly the terminal event's boundary. Share the write lock with pin
/// and reread before merging so a late old event cannot overwrite a newer turn.
pub(crate) async fn close_prompt(
    db: &DatabaseConnection,
    connection_id: &str,
    seq: u64,
) -> Result<(), String> {
    let _write = PROMPT_WRITES.lock().await;
    let key = format!("{PROMPT_PREFIX}{connection_id}");
    let Some(raw) = app_metadata_service::get_value(db, &key)
        .await
        .map_err(|_| "channel_callback_storage_unavailable")?
    else {
        return Ok(());
    };
    let mut targets: PromptTargets =
        serde_json::from_str(&raw).map_err(|_| "channel_callback_storage_unavailable")?;
    if targets.process_id != process_id() {
        return Ok(());
    }
    let Some(entry) = targets
        .boundaries
        .iter_mut()
        .rev()
        .find(|entry| seq > entry.start_seq)
    else {
        return Ok(());
    };
    if entry.end_seq.is_some() || entry.target_id.is_none() {
        return Ok(());
    }
    entry.end_seq = Some(seq);
    app_metadata_service::upsert_value(db, &key, &serde_json::to_string(&targets).unwrap())
        .await
        .map_err(|_| "channel_callback_storage_unavailable".into())
}
/// An explicit admission error means no turn was accepted. Only revoke the
/// exact reservation; another prompt may already have acquired the lock.
pub(crate) async fn abort_prompt_pin(
    db: &DatabaseConnection,
    connection_id: &str,
    reservation: Option<&str>,
) -> Result<(), String> {
    let Some(reservation) = reservation else {
        return Ok(());
    };
    // Do this before touching SQLite. If the rollback write fails, this process
    // must still never route a later ordinary prompt through the rejected pin.
    revoked_pins()
        .lock()
        .unwrap()
        .insert(reservation.to_owned());
    let _write = PROMPT_WRITES.lock().await;
    let key = format!("{PROMPT_PREFIX}{connection_id}");
    let Some(raw) = app_metadata_service::get_value(db, &key)
        .await
        .map_err(|_| "channel_callback_storage_unavailable")?
    else {
        return Ok(());
    };
    let mut targets: PromptTargets =
        serde_json::from_str(&raw).map_err(|_| "channel_callback_storage_unavailable")?;
    if targets.process_id != process_id() {
        return Ok(());
    }
    let Some(entry) = targets
        .boundaries
        .iter_mut()
        .find(|entry| entry.reservation == reservation)
    else {
        return Ok(());
    };
    if entry.end_seq.is_some() {
        return Ok(());
    }
    entry.end_seq = Some(entry.start_seq);
    app_metadata_service::upsert_value(db, &key, &serde_json::to_string(&targets).unwrap())
        .await
        .map_err(|_| "channel_callback_storage_unavailable".to_string())?;
    revoked_pins().lock().unwrap().remove(reservation);
    Ok(())
}
/// A private callback is an additional explicitly selected sink, never a global
/// subscription. Ordinary hooks keep receiving the same notification payload.
pub(crate) async fn route_prompt_event(
    db: &DatabaseConnection,
    connection_id: &str,
    seq: u64,
    target_id: Option<&str>,
    mut payload: Value,
) -> Result<(), String> {
    let Some(id) = target_id else {
        return Ok(());
    };
    let event_id = format!(
        "channel-event-{:x}",
        Sha256::digest(format!("{}:{connection_id}:{seq}", process_id()))
    );
    payload["callback_target_id"] = id.into();
    payload["event_id"] = event_id.clone().into();
    super::relay_outbox::save_with_target(
        db,
        &event_id,
        payload,
        None,
        Some(id.into()),
        super::relay_result::ResultFormat::Legacy,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    async fn hooks(db: &DatabaseConnection, origin: &str, vm: &str, secret: &str) {
        app_metadata_service::upsert_value(db, super::super::event_subscriber::EVENT_WEBHOOKS_KEY,
            &json!([{"enabled":true,"url":format!("{origin}/api/codeg/events?vmId={vm}&s={secret}")}]).to_string()).await.unwrap();
    }
    #[test]
    fn accepts_only_https_origins_without_credentials_or_paths() {
        assert_eq!(
            normalize_origin("https://slack.example/"),
            Ok("https://slack.example".into())
        );
        for input in [
            "http://slack.example",
            "https://u:p@slack.example",
            "https://@slack.example",
            "https://slack.example/path",
            "https://slack.example/?s=secret",
            "https://slack.example/#fragment",
            "https://slack.example/.",
            "https://slack.example/a/..",
            "https://slack.example///",
            "https://slack.exa\nmple",
            " https://slack.example",
            "https://slack.example\\path",
        ] {
            assert!(normalize_origin(input).is_err(), "{input}");
        }
    }
    #[tokio::test]
    async fn whatsapp_callback_is_independent_and_cannot_use_a_slack_binding() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        hooks(&db.conn, "https://telegram.example", "vm-a", "secret-a").await;
        let wa = register(&db.conn, "whatsapp_agent", "https://wa.example")
            .await
            .unwrap();
        let slack = register(&db.conn, "slack", "https://slack.example")
            .await
            .unwrap();
        assert_eq!(
            endpoint(&db.conn, &wa, "whatsapp_agent")
                .await
                .unwrap()
                .base,
            "https://wa.example"
        );
        assert!(endpoint(&db.conn, &wa, "slack").await.is_err());
        assert!(endpoint(&db.conn, &slack, "whatsapp_agent").await.is_err());
        assert!(result_url(&db.conn, &wa)
            .await
            .unwrap()
            .starts_with("https://wa.example/api/codeg/events?"));
        assert!(result_url(&db.conn, &slack)
            .await
            .unwrap()
            .starts_with("https://slack.example/api/codeg/events?"));
        assert_eq!(
            platform(&db.conn).await.unwrap().base,
            "https://telegram.example"
        );
    }

    #[tokio::test]
    async fn registered_targets_are_immutable_scoped_and_do_not_change_global_hooks() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        hooks(&db.conn, "https://telegram.example", "vm-a", "secret-a").await;
        let before = app_metadata_service::get_value(
            &db.conn,
            super::super::event_subscriber::EVENT_WEBHOOKS_KEY,
        )
        .await
        .unwrap();
        let first = register(&db.conn, "slack", "https://slack-a.example")
            .await
            .unwrap();
        assert_eq!(first.len(), 64);
        assert_eq!(
            first,
            register(&db.conn, "slack", "https://slack-a.example/")
                .await
                .unwrap()
        );
        let second = register(&db.conn, "slack", "https://slack-b.example")
            .await
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(
            before,
            app_metadata_service::get_value(
                &db.conn,
                super::super::event_subscriber::EVENT_WEBHOOKS_KEY
            )
            .await
            .unwrap()
        );
        assert_eq!(
            endpoint(&db.conn, &first, "slack").await.unwrap().base,
            "https://slack-a.example"
        );
        assert_eq!(
            endpoint(&db.conn, &second, "slack").await.unwrap().base,
            "https://slack-b.example"
        );
        let stored = app_metadata_service::get_value(&db.conn, &format!("{PREFIX}{first}"))
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.contains("secret-a"));
        assert!(register(&db.conn, "telegram", "https://slack-a.example")
            .await
            .is_err());
        assert!(endpoint(&db.conn, &first, "telegram").await.is_err());
        assert!(endpoint(&db.conn, &"0".repeat(64), "slack").await.is_err());
        hooks(
            &db.conn,
            "https://new-global.example",
            "vm-a",
            "rotated-secret",
        )
        .await;
        let ep = endpoint(&db.conn, &first, "slack").await.unwrap();
        assert_eq!(ep.base, "https://slack-a.example");
        assert_eq!(ep.secret, "rotated-secret");
        let url = reqwest::Url::parse(&result_url(&db.conn, &first).await.unwrap()).unwrap();
        assert_eq!(url.host_str(), Some("slack-a.example"));
        assert_eq!(url.path(), "/api/codeg/events");
        assert!(url
            .query_pairs()
            .any(|(key, value)| key == "s" && value == "rotated-secret"));
        hooks(&db.conn, "https://new-global.example", "vm-b", "secret-b").await;
        assert!(endpoint(&db.conn, &first, "slack").await.is_err());
    }
    #[tokio::test]
    async fn prompt_boundaries_keep_old_turn_destinations_and_reset_for_non_slack_turns() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let dir = tempfile::tempdir().unwrap();
        let state = crate::app_state::AppState::new_for_test(db, dir.path().to_owned());
        hooks(&state.db.conn, "https://global.example", "vm", "secret").await;
        let first = register(&state.db.conn, "slack", "https://slack-a.example")
            .await
            .unwrap();
        let second = register(&state.db.conn, "slack", "https://slack-b.example")
            .await
            .unwrap();
        state
            .connection_manager
            .insert_test_connection(
                "conn",
                crate::models::agent::AgentType::ClaudeCode,
                None,
                state.emitter.clone(),
            )
            .await;
        pin_prompt(&state, "conn", Some(&first), None)
            .await
            .unwrap();
        assert_eq!(
            prompt_target(&state.db.conn, "conn", 1).await,
            Some(Some(first.clone()))
        );
        let session = state.connection_manager.get_state("conn").await.unwrap();
        session.write().await.event_seq = 10;
        pin_prompt(&state, "conn", Some(&second), None)
            .await
            .unwrap();
        assert_eq!(
            prompt_target(&state.db.conn, "conn", 10).await,
            Some(Some(first))
        );
        assert_eq!(
            prompt_target(&state.db.conn, "conn", 11).await,
            Some(Some(second.clone()))
        );
        close_prompt(&state.db.conn, "conn", 10).await.unwrap();
        assert_eq!(
            prompt_target(&state.db.conn, "conn", 11).await,
            Some(Some(second.clone())),
            "closing an old boundary must preserve a newer pinned turn"
        );
        close_prompt(&state.db.conn, "conn", 15).await.unwrap();
        assert_eq!(
            prompt_target(&state.db.conn, "conn", 16).await,
            None,
            "a later non-HTTP prompt no longer inherits the completed target"
        );

        session.write().await.event_seq = 20;
        pin_prompt(&state, "conn", None, None).await.unwrap();
        assert_eq!(prompt_target(&state.db.conn, "conn", 20).await, None);
        assert_eq!(prompt_target(&state.db.conn, "conn", 21).await, None);
        assert_eq!(prompt_target(&state.db.conn, "ordinary", 1).await, None);
        let key = format!("{PROMPT_PREFIX}conn");
        let mut stored: PromptTargets = serde_json::from_str(
            &app_metadata_service::get_value(&state.db.conn, &key)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        stored.process_id = "previous-process".into();
        app_metadata_service::upsert_value(
            &state.db.conn,
            &key,
            &serde_json::to_string(&stored).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(prompt_target(&state.db.conn, "conn", 100).await, Some(None));
        session.write().await.event_seq = 0;
        pin_prompt(&state, "conn", None, None).await.unwrap();
        assert_eq!(prompt_target(&state.db.conn, "conn", 1).await, None);
    }
    #[tokio::test]
    async fn late_admission_failure_only_revokes_its_exact_reservation() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let dir = tempfile::tempdir().unwrap();
        let state = crate::app_state::AppState::new_for_test(db, dir.path().to_owned());
        hooks(&state.db.conn, "https://global.example", "vm", "secret").await;
        let target = register(&state.db.conn, "slack", "https://slack.example")
            .await
            .unwrap();
        state
            .connection_manager
            .insert_test_connection(
                "pin-failure",
                crate::models::agent::AgentType::ClaudeCode,
                None,
                state.emitter.clone(),
            )
            .await;
        let first = pin_prompt(&state, "pin-failure", Some(&target), None)
            .await
            .unwrap();
        let newer = pin_prompt(&state, "pin-failure", Some(&target), None)
            .await
            .unwrap();
        assert_ne!(first, newer);
        abort_prompt_pin(&state.db.conn, "pin-failure", first.as_deref())
            .await
            .unwrap();
        assert_eq!(
            prompt_target(&state.db.conn, "pin-failure", 1).await,
            Some(Some(target))
        );
        abort_prompt_pin(&state.db.conn, "pin-failure", newer.as_deref())
            .await
            .unwrap();
        assert_eq!(prompt_target(&state.db.conn, "pin-failure", 1).await, None);
    }
    #[tokio::test]
    async fn failed_pin_rollback_write_cannot_route_the_next_ordinary_turn() {
        use sea_orm::ConnectionTrait;
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let dir = tempfile::tempdir().unwrap();
        let state = crate::app_state::AppState::new_for_test(db, dir.path().to_owned());
        hooks(&state.db.conn, "https://global.example", "vm", "secret").await;
        let target = register(&state.db.conn, "slack", "https://slack.example")
            .await
            .unwrap();
        state
            .connection_manager
            .insert_test_connection(
                "rollback-storage-failure",
                crate::models::agent::AgentType::ClaudeCode,
                None,
                state.emitter.clone(),
            )
            .await;
        let reservation = pin_prompt(&state, "rollback-storage-failure", Some(&target), None)
            .await
            .unwrap();
        state.db.conn.execute_unprepared("CREATE TRIGGER reject_pin_rollback BEFORE UPDATE ON app_metadata WHEN OLD.key = 'channel_prompt_callback:rollback-storage-failure' AND NEW.value LIKE '%end_seq%' BEGIN SELECT RAISE(FAIL, 'injected rollback write failure'); END").await.unwrap();
        assert!(abort_prompt_pin(
            &state.db.conn,
            "rollback-storage-failure",
            reservation.as_deref()
        )
        .await
        .is_err());
        assert_eq!(
            prompt_target(&state.db.conn, "rollback-storage-failure", 1).await,
            None
        );
        let key = format!("{PROMPT_PREFIX}rollback-storage-failure");
        let stored: PromptTargets = serde_json::from_str(
            &app_metadata_service::get_value(&state.db.conn, &key)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            stored.boundaries[0].end_seq.is_none(),
            "the test must retain the failed SQLite rollback to exercise the memory guard"
        );
        let newer = pin_prompt(&state, "rollback-storage-failure", Some(&target), None)
            .await
            .unwrap();
        assert_ne!(reservation, newer);
        assert_eq!(
            prompt_target(&state.db.conn, "rollback-storage-failure", 1).await,
            Some(Some(target))
        );
    }
}
