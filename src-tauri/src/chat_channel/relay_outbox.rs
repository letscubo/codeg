//! Durable terminal results. A turn can select exactly one already registered
//! callback URL; only its SHA-256 hash is stored here, never tokens or URLs.
use super::relay_result::{self, ResultFormat};
use crate::db::{entities::app_metadata, service::app_metadata_service};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{sync::OnceLock, time::Duration};
const PREFIX: &str = "channel_relay_result:";
const PREFIX_END: &str = "channel_relay_result;";
static READY: tokio::sync::Notify = tokio::sync::Notify::const_new();
#[derive(Serialize, Deserialize)]
struct Stored {
    result: Value,
    target_hash: Option<String>,
    acknowledged: bool,
    #[serde(default)]
    process_id: String,
    #[serde(default)]
    wire_format: ResultFormat,
}
fn process_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| uuid::Uuid::new_v4().to_string())
}
fn hash(url: &str) -> String {
    format!("{:x}", Sha256::digest(url.as_bytes()))
}
async fn urls(db: &DatabaseConnection) -> Vec<String> {
    app_metadata_service::get_value(db, super::event_subscriber::EVENT_WEBHOOKS_KEY)
        .await
        .ok()
        .flatten()
        .map(|s| super::webhook::enabled_webhook_urls(&s))
        .unwrap_or_default()
}
pub(crate) async fn validated_target(db: &DatabaseConnection, url: Option<&str>) -> Option<String> {
    let url = url?;
    urls(db)
        .await
        .iter()
        .any(|known| known == url)
        .then(|| hash(url))
}
pub(crate) async fn save(
    db: &DatabaseConnection,
    turn_id: &str,
    result: Value,
    target_hash: Option<String>,
    wire_format: ResultFormat,
) -> Result<(), String> {
    let result = relay_result::compact(result)?;
    let terminal = result["delivery_status"] != "running";
    // No selected callback means query-only, so there is no delivery to poll.
    // Running intents stay pending until restart recovery or terminal save.
    let acknowledged = terminal && target_hash.is_none();
    let stored = Stored {
        result,
        target_hash,
        acknowledged,
        process_id: process_id().into(),
        wire_format,
    };
    let key = format!("{PREFIX}{turn_id}");
    let serialized = serde_json::to_string(&stored).unwrap();
    persist_with_retry(|| async {
        app_metadata_service::upsert_value(db, &key, &serialized)
            .await
            .map_err(|error| error.to_string())
    })
    .await?;
    if terminal && !acknowledged {
        READY.notify_one();
    }
    Ok(())
}

/// Retry only local SQLite persistence, with a fixed bound. The caller's
/// immutable payload (including event_id) is reused; this never reruns an agent.
async fn persist_with_retry<F, Fut>(mut persist: F) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    for attempt in 0..3 {
        match persist().await {
            Ok(()) => return Ok(()),
            Err(error) if attempt == 2 => return Err(error),
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    unreachable!()
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("relay callback HTTP client")
}
pub(crate) async fn get(db: &DatabaseConnection, turn_id: &str) -> Result<Option<Value>, String> {
    app_metadata_service::get_value(db, &format!("{PREFIX}{turn_id}"))
        .await
        .map_err(|e| e.to_string())?
        .map(|s| {
            serde_json::from_str::<Stored>(&s)
                .map(|v| v.result)
                .map_err(|_| "invalid relay result".into())
        })
        .transpose()
}
pub(crate) fn start(db: DatabaseConnection) {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return;
    }
    tokio::spawn(async move {
        let client = client();
        run_loop(&READY, || drain(&db, &client)).await;
    });
}

async fn run_loop<F, Fut>(ready: &tokio::sync::Notify, mut poll: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<Duration>, String>>,
{
    loop {
        // Always scan once on startup, including old-process running markers.
        let retry = match poll().await {
            Ok(retry) => retry,
            Err(error) => {
                tracing::warn!("[TurnRelay] outbox failed: {error}");
                Some(Duration::from_secs(5))
            }
        };
        if let Some(delay) = retry {
            tokio::select! {
                _ = ready.notified() => {},
                _ = tokio::time::sleep(delay) => {},
            }
        } else {
            // notify_one retains a permit if a save raced the scan or this wait.
            ready.notified().await;
        }
    }
}

fn pending_query() -> sea_orm::Select<app_metadata::Entity> {
    app_metadata::Entity::find()
        .filter(app_metadata::Column::Key.gte(PREFIX))
        .filter(app_metadata::Column::Key.lt(PREFIX_END))
        .filter(app_metadata::Column::Value.contains("\"acknowledged\":false"))
        .filter(app_metadata::Column::DeletedAt.is_null())
}

async fn drain(
    db: &DatabaseConnection,
    client: &reqwest::Client,
) -> Result<Option<Duration>, String> {
    let records = pending_query().all(db).await.map_err(|e| e.to_string())?;
    let mut configured = None;
    let mut retry = None;
    for record in records {
        let Ok(mut stored) = serde_json::from_str::<Stored>(&record.value) else {
            continue;
        };
        stored.result = match relay_result::compact(stored.result) {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!("[TurnRelay] invalid stored receipt text ranges");
                continue;
            }
        };
        // A previous process cannot resume a token held only in memory. Record
        // uncertainty instead of replaying its agent or sending a second reply.
        if stored.result["delivery_status"] == "running" {
            if stored.process_id == process_id() {
                continue;
            }
            stored.result["delivery_status"] = "unknown".into();
            stored.result["outcome"] = "aborted".into();
            stored.result["failure_code"] = "process_restarted".into();
            stored.acknowledged = stored.target_hash.is_none();
            app_metadata_service::upsert_value(
                db,
                &record.key,
                &serde_json::to_string(&stored).unwrap(),
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        if stored.acknowledged {
            continue;
        }
        let Some(target) = &stored.target_hash else {
            continue;
        };
        if configured.is_none() {
            configured = Some(urls(db).await);
        }
        let Some(url) = configured
            .as_ref()
            .unwrap()
            .iter()
            .find(|url| hash(url) == *target)
        else {
            // A previously selected endpoint may be temporarily disabled. Keep
            // checking configuration; no-target turns remain query-only.
            retry.get_or_insert(Duration::from_secs(60));
            continue;
        };
        let payload = relay_result::for_wire(stored.result.clone(), stored.wire_format)?;
        if client
            .post(url)
            .json(&payload)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            stored.acknowledged = true;
            app_metadata_service::upsert_value(
                db,
                &record.key,
                &serde_json::to_string(&stored).unwrap(),
            )
            .await
            .map_err(|e| e.to_string())?;
        } else {
            retry = Some(Duration::from_secs(5));
        }
    }
    Ok(retry)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn idle_outbox_does_not_poll_and_coalesced_or_racing_saves_are_not_lost() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let ready = Arc::new(tokio::sync::Notify::new());
        let scans = Arc::new(AtomicUsize::new(0));
        let pending = Arc::new(AtomicUsize::new(0));
        let handled = Arc::new(AtomicUsize::new(0));
        let worker = tokio::spawn({
            let (ready, scans, pending, handled) = (
                ready.clone(),
                scans.clone(),
                pending.clone(),
                handled.clone(),
            );
            async move {
                run_loop(&ready, || {
                    let scan = scans.fetch_add(1, Ordering::SeqCst);
                    handled.fetch_add(pending.swap(0, Ordering::SeqCst), Ordering::SeqCst);
                    if scan == 1 {
                        // Another save commits after this scan took its snapshot.
                        pending.fetch_add(1, Ordering::SeqCst);
                        ready.notify_one();
                    }
                    std::future::ready(Ok(None))
                })
                .await;
            }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(scans.load(Ordering::SeqCst), 1, "startup only while idle");
        for _ in 0..3 {
            pending.fetch_add(1, Ordering::SeqCst);
            ready.notify_one();
        }
        tokio::task::yield_now().await;
        assert_eq!(scans.load(Ordering::SeqCst), 3);
        assert_eq!(handled.load(Ordering::SeqCst), 4);
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(scans.load(Ordering::SeqCst), 3);
        worker.abort();
    }
    #[tokio::test(start_paused = true)]
    async fn local_persistence_retries_are_bounded_and_can_recover() {
        let mut calls = 0;
        persist_with_retry(|| {
            calls += 1;
            std::future::ready(if calls < 3 {
                Err("database busy".into())
            } else {
                Ok(())
            })
        })
        .await
        .unwrap();
        assert_eq!(calls, 3);
        let mut failures = 0;
        assert!(persist_with_retry(|| {
            failures += 1;
            std::future::ready(Err("database unavailable".into()))
        })
        .await
        .is_err());
        assert_eq!(failures, 3);
    }
    #[test]
    fn stable_result_without_credentials() {
        let record = Stored {
            result: serde_json::json!({"event_id":"stable","relay_turn_id":"turn"}),
            target_hash: Some(hash("https://host/events?s=SECRET")),
            acknowledged: false,
            process_id: process_id().into(),
            wire_format: ResultFormat::Legacy,
        };
        let json = serde_json::to_string(&record).unwrap();
        assert!(!json.contains("SECRET"));
        assert!(!json.contains("https://"));
        assert_eq!(
            serde_json::from_str::<Stored>(&json).unwrap().result["event_id"],
            "stable"
        );
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    #[tokio::test]
    async fn compact_storage_keeps_one_body_and_reads_both_wire_formats() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let original = serde_json::json!({"event_id":"compact-event","delivery_status":"delivered","final_text":"unique 🦀 body","segments":[{"index":0,"text":"unique 🦀 body","message_ts":"1.0","delivered":true}]});
        save(
            &db.conn,
            "compact",
            original.clone(),
            None,
            ResultFormat::CompactV1,
        )
        .await
        .unwrap();
        let raw = app_metadata_service::get_value(&db.conn, &format!("{PREFIX}compact"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw.matches("unique").count(), 1);
        assert!(
            serde_json::from_str::<Value>(&raw).unwrap()["result"]["segments"][0]
                .get("text")
                .is_none()
        );
        assert_eq!(
            super::super::turn_relay::get_result(&db.conn, "compact", ResultFormat::Legacy)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        assert_eq!(
            super::super::turn_relay::get_result(&db.conn, "compact", ResultFormat::CompactV1)
                .await
                .unwrap()
                .unwrap()["result_format"],
            "compact-v1"
        );
        // Old rows predate wire_format and retain embedded segment text.
        let old = serde_json::json!({"result":original,"target_hash":null,"acknowledged":false,"process_id":process_id()});
        app_metadata_service::upsert_value(&db.conn, &format!("{PREFIX}legacy"), &old.to_string())
            .await
            .unwrap();
        assert_eq!(
            super::super::turn_relay::get_result(&db.conn, "legacy", ResultFormat::Legacy)
                .await
                .unwrap()
                .unwrap(),
            old["result"]
        );
        assert_eq!(
            super::super::turn_relay::get_result(&db.conn, "legacy", ResultFormat::CompactV1)
                .await
                .unwrap()
                .unwrap()["result_format"],
            "compact-v1"
        );
        assert!(
            drain(&db.conn, &client()).await.unwrap().is_none(),
            "query-only results do not poll"
        );
    }

    #[tokio::test]
    async fn temporarily_disabled_target_retries_slowly_and_prefix_uses_key_index() {
        use sea_orm::{ConnectionTrait, DbBackend, QueryTrait, Statement};
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        save(&db.conn,"disabled",serde_json::json!({"delivery_status":"delivered","final_text":"body","segments":[{"text":"body"}]}),Some(hash("https://disabled.invalid/events")),ResultFormat::CompactV1).await.unwrap();
        assert_eq!(
            drain(&db.conn, &client()).await.unwrap(),
            Some(Duration::from_secs(60))
        );
        let sql = pending_query().build(DbBackend::Sqlite).to_string();
        let plans = db
            .conn
            .query_all(Statement::from_string(
                DbBackend::Sqlite,
                format!("EXPLAIN QUERY PLAN {sql}"),
            ))
            .await
            .unwrap();
        let details: Vec<String> = plans
            .iter()
            .map(|p| p.try_get("", "detail").unwrap())
            .collect();
        assert!(
            details
                .iter()
                .any(|p| p.contains("SEARCH app_metadata USING INDEX")
                    && p.contains("key>?")
                    && p.contains("key<?")),
            "{details:?}"
        );
    }
    #[tokio::test]
    async fn restart_converts_only_old_running_intents_to_unknown_without_resending() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        save(
            &db.conn,
            "current",
            serde_json::json!({"delivery_status":"running"}),
            None,
            ResultFormat::Legacy,
        )
        .await
        .unwrap();
        let stored = Stored {
            result: serde_json::json!({"event_id":"stable-old","delivery_status":"running","fallback_allowed":false,"text_complete":false,"final_text":"","segments":[]}),
            target_hash: None,
            acknowledged: false,
            process_id: "previous-process".into(),
            wire_format: ResultFormat::Legacy,
        };
        app_metadata_service::upsert_value(
            &db.conn,
            &format!("{PREFIX}previous"),
            &serde_json::to_string(&stored).unwrap(),
        )
        .await
        .unwrap();
        drain(&db.conn, &client()).await.unwrap();
        let recovered = get(&db.conn, "previous").await.unwrap().unwrap();
        assert_eq!(recovered["delivery_status"], "unknown");
        assert_eq!(recovered["event_id"], "stable-old");
        assert_eq!(recovered["fallback_allowed"], false);
        assert_eq!(
            get(&db.conn, "current").await.unwrap().unwrap()["delivery_status"],
            "running"
        );
        assert!(
            super::super::turn_relay::get_result(&db.conn, "current", ResultFormat::Legacy)
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn result_retries_only_selected_registered_target_with_stable_event_id() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let selected = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let selected_url = format!("http://{}/events?s=SECRET", selected.local_addr().unwrap());
        let other_url = format!("http://{}/events", other.local_addr().unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::<Value>::new()));
        let app = axum::Router::new().route(
            "/events",
            axum::routing::post({
                let count = count.clone();
                let received = received.clone();
                move |axum::Json(payload): axum::Json<Value>| {
                    let count = count.clone();
                    let received = received.clone();
                    async move {
                        received.lock().unwrap().push(payload);
                        if count.fetch_add(1, Ordering::SeqCst) == 0 {
                            axum::http::StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            axum::http::StatusCode::OK
                        }
                    }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(selected, app).await.unwrap() });
        app_metadata_service::upsert_value(&db.conn,super::super::event_subscriber::EVENT_WEBHOOKS_KEY,&serde_json::json!([{"url":selected_url,"enabled":true},{"url":other_url,"enabled":true}]).to_string()).await.unwrap();
        assert!(
            validated_target(&db.conn, Some("https://unregistered.invalid/"))
                .await
                .is_none()
        );
        let target = validated_target(&db.conn, Some(&selected_url))
            .await
            .unwrap();
        save(
            &db.conn,
            "turn-1",
            serde_json::json!({"event_id":"stable-id","final_text":"reply","segments":[{"index":0,"text":"reply","delivered":true}]}),
            Some(target.clone()),
            ResultFormat::Legacy,
        )
        .await
        .unwrap();
        let stored = app_metadata_service::get_value(&db.conn, &format!("{PREFIX}turn-1"))
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.contains("SECRET"));
        let client = super::super::webhook::make_webhook_client();
        drain(&db.conn, &client).await.unwrap();
        drain(&db.conn, &client).await.unwrap();
        drain(&db.conn, &client).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(received
            .lock()
            .unwrap()
            .iter()
            .all(|v| v["event_id"] == "stable-id"));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), other.accept())
                .await
                .is_err()
        );
        assert_eq!(
            get(&db.conn, "turn-1").await.unwrap().unwrap()["final_text"],
            "reply"
        );
        assert!(received.lock().unwrap()[0].get("result_format").is_none());
        assert_eq!(received.lock().unwrap()[0]["segments"][0]["text"], "reply");
        save(&db.conn,"turn-compact",serde_json::json!({"event_id":"compact-id","final_text":"🦀reply","segments":[{"index":0,"text":"🦀reply","delivered":true}]}),Some(target),ResultFormat::CompactV1).await.unwrap();
        drain(&db.conn, &client).await.unwrap();
        let received = received.lock().unwrap();
        assert_eq!(received.last().unwrap()["result_format"], "compact-v1");
        assert_eq!(received.last().unwrap()["segments"][0]["text_end"], 7);
        assert!(received.last().unwrap()["segments"][0]
            .get("text")
            .is_none());
        server.abort();
    }
}
