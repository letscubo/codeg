use super::*;
use axum::{routing::post, Extension, Json, Router};

#[derive(Default)]
struct Platform {
    dispatches: Mutex<Vec<Value>>,
    completions: Mutex<Vec<Value>>,
    blocked_started: Notify,
    blocked_requests: Mutex<HashSet<String>>,
    release: Notify,
    malformed: AtomicBool,
    reject_completion: AtomicBool,
    status_known: AtomicBool,
    resolved_session: Mutex<Option<String>>,
    resolutions: Mutex<Vec<Value>>,
}
async fn callback(
    Extension(platform): Extension<Arc<Platform>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    if body["phase"] == "resolve" {
        let key = platform
            .resolved_session
            .lock()
            .unwrap()
            .clone()
            .map(Value::String)
            .unwrap_or_else(|| body["context"]["sessionKey"].clone());
        platform.resolutions.lock().unwrap().push(body);
        return Json(json!({"code":0,"data":{"kind":"resolved","sessionKey":key}}));
    }
    if body["phase"] == "status" {
        return Json(
            json!({"code":0,"data":{"kind":"status","status":if platform.status_known.load(Ordering::Acquire) { "completed" } else { "unknown" }}}),
        );
    }
    if body["phase"] == "complete" {
        platform.completions.lock().unwrap().push(body);
        return Json(
            json!({"code":if platform.reject_completion.load(Ordering::Acquire) { 1 } else { 0 },"data":{}}),
        );
    }
    let blocked = body["requestId"] == "first"
        || body["requestId"]
            .as_str()
            .is_some_and(|id| platform.blocked_requests.lock().unwrap().contains(id));
    let model = body["parsedCommand"]["name"] == "model";
    platform.dispatches.lock().unwrap().push(body);
    if blocked {
        platform.blocked_started.notify_one();
        platform.release.notified().await;
    }
    let data = if platform.malformed.load(Ordering::Acquire) {
        json!({"kind":"invalid"})
    } else if model {
        json!({"kind":"reply","status":"ignored","text":""})
    } else {
        json!({"kind":"accepted"})
    };
    Json(json!({"code":0,"data":data}))
}
async fn fixture() -> (
    Arc<AppState>,
    Arc<Platform>,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
) {
    let db = crate::db::test_helpers::fresh_in_memory_db().await;
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(AppState::new_for_test(db, dir.path().to_path_buf()));
    let platform = Arc::new(Platform::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route(CALLBACK, post(callback))
        .layer(Extension(platform.clone()));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    crate::commands::chat_channel::set_chat_event_webhooks_core(
        &state.db,
        vec![super::super::webhook::WebhookConfig {
            url: format!("http://{address}/api/codeg/events?vmId=vm&s=test-secret"),
            enabled: true,
        }],
    )
    .await
    .unwrap();
    (state, platform, server, dir)
}
fn request(id: &str, session: &str) -> Request {
    Request {
        request_id: id.into(),
        callback_target_id: None,
        session_ref: session.into(),
        source: Source {
            channel_type: "telegram".into(),
            account_id: "account".into(),
        },
        input: Input::Text {
            text: "private input".into(),
        },
        context: json!({"entry":"managed","jobId":id,"token":"never-persist"}),
    }
}
async fn finished(state: &AppState, id: &str) -> Stored {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(stored) = load(state, id).await.unwrap() {
                if stored.operation.status.terminal() {
                    return stored;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn same_session_order_dedup_and_independent_sessions() {
    let (state, platform, server, _dir) = fixture().await;
    let engine = Arc::new(Engine::new());
    engine
        .submit(state.clone(), request("first", "a"))
        .await
        .unwrap();
    platform.blocked_started.notified().await;
    engine
        .submit(state.clone(), request("second", "a"))
        .await
        .unwrap();
    // Duplicate input returns its durable status without dispatching a second time.
    engine
        .submit(state.clone(), request("first", "a"))
        .await
        .unwrap();
    engine
        .submit(state.clone(), request("other", "b"))
        .await
        .unwrap();
    assert_eq!(
        finished(&state, "other").await.operation.status,
        Status::Completed
    );
    assert_eq!(
        load(&state, "second")
            .await
            .unwrap()
            .unwrap()
            .operation
            .status,
        Status::Queued
    );
    let ids: Vec<_> = platform
        .dispatches
        .lock()
        .unwrap()
        .iter()
        .map(|v| v["requestId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["first", "other"]);
    platform.release.notify_one();
    assert_eq!(
        finished(&state, "second").await.operation.status,
        Status::Completed
    );
    let ids: Vec<_> = platform
        .dispatches
        .lock()
        .unwrap()
        .iter()
        .map(|v| v["requestId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["first", "other", "second"]);
    let stored = serde_json::to_string(&load(&state, "first").await.unwrap().unwrap()).unwrap();
    assert!(!stored.contains("private input"));
    assert!(!stored.contains("never-persist"));
    server.abort();
}
#[tokio::test]
async fn portable_model_is_ordered_between_prompts_for_managed_and_retained_sessions() {
    for (entry, channel, runtime_session) in [
        ("managed", "slack", "codeg:987660"),
        ("legacy_slack", "slack", "agent:main:direct:slack-retained"),
        ("legacy_telegram", "telegram", "hermes:retained-session"),
    ] {
        let (state, platform, server, _dir) = fixture().await;
        let engine = Arc::new(Engine::new());
        platform
            .blocked_requests
            .lock()
            .unwrap()
            .insert("model".into());
        let make_request = |id: &str, text: &str| {
            let mut input = request(id, "stable-target-session");
            input.source.channel_type = channel.into();
            input.input = Input::Text { text: text.into() };
            input.context = json!({
                "entry": entry,
                "jobId": id,
                "sessionKey": runtime_session,
                "chatId": "target-chat",
                "threadId": "target-thread",
                "senderId": "authorized-sender",
            });
            input
        };
        engine
            .submit(state.clone(), make_request("first", "first prompt"))
            .await
            .unwrap();
        platform.blocked_started.notified().await;
        engine
            .submit(
                state.clone(),
                make_request("model", r"\model provider/model"),
            )
            .await
            .unwrap();
        engine
            .submit(state.clone(), make_request("after", "next prompt"))
            .await
            .unwrap();
        assert_eq!(
            load(&state, "model")
                .await
                .unwrap()
                .unwrap()
                .operation
                .status,
            Status::Queued
        );
        assert_eq!(
            load(&state, "after")
                .await
                .unwrap()
                .unwrap()
                .operation
                .status,
            Status::Queued
        );
        assert_eq!(platform.dispatches.lock().unwrap().len(), 1);

        platform.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), platform.blocked_started.notified())
            .await
            .unwrap();
        {
            let dispatches = platform.dispatches.lock().unwrap();
            assert_eq!(dispatches.len(), 2);
            assert_eq!(dispatches[1]["requestId"], "model");
            assert_eq!(
                dispatches[1]["parsedCommand"],
                json!({
                    "name": "model", "arguments": "provider/model",
                })
            );
            assert_eq!(dispatches[1]["sessionRef"], "stable-target-session");
            assert_eq!(dispatches[1]["context"]["sessionKey"], runtime_session);
            assert_eq!(dispatches[1]["context"]["chatId"], "target-chat");
            assert_eq!(dispatches[1]["context"]["threadId"], "target-thread");
            assert_eq!(dispatches[1]["context"]["senderId"], "authorized-sender");
        }
        assert_eq!(
            platform.resolutions.lock().unwrap().len(),
            usize::from(entry == "managed")
        );
        assert_eq!(
            load(&state, "after")
                .await
                .unwrap()
                .unwrap()
                .operation
                .status,
            Status::Queued
        );

        platform.release.notify_one();
        assert_eq!(
            finished(&state, "model").await.operation.status,
            Status::Completed
        );
        assert_eq!(
            finished(&state, "after").await.operation.status,
            Status::Completed
        );
        let ids: Vec<_> = platform
            .dispatches
            .lock()
            .unwrap()
            .iter()
            .map(|body| body["requestId"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ids, ["first", "model", "after"]);
        server.abort();
    }
}
#[tokio::test]
async fn conflicting_duplicate_is_rejected_without_dispatch() {
    let (state, platform, server, _dir) = fixture().await;
    let engine = Arc::new(Engine::new());
    engine
        .submit(state.clone(), request("first", "a"))
        .await
        .unwrap();
    platform.blocked_started.notified().await;
    let mut changed = request("first", "a");
    changed.input = Input::Text {
        text: "different".into(),
    };
    assert_eq!(
        engine.submit(state, changed).await.unwrap_err(),
        "request_id_conflict"
    );
    assert_eq!(platform.dispatches.lock().unwrap().len(), 1);
    platform.release.notify_one();
    server.abort();
}
#[tokio::test]
async fn uncertain_callback_blocks_successors_and_is_never_replayed() {
    let (state, platform, server, _dir) = fixture().await;
    platform.malformed.store(true, Ordering::Release);
    let engine = Arc::new(Engine::new());
    engine
        .submit(state.clone(), request("first", "a"))
        .await
        .unwrap();
    platform.blocked_started.notified().await;
    engine
        .submit(state.clone(), request("second", "a"))
        .await
        .unwrap();
    platform.release.notify_one();
    assert_eq!(
        finished(&state, "first").await.operation.status,
        Status::Unknown
    );
    let second = finished(&state, "second").await;
    assert_eq!(
        second.operation.error_code.as_deref(),
        Some("prior_operation_unknown")
    );
    assert_eq!(
        engine
            .submit(state.clone(), request("first", "a"))
            .await
            .unwrap()
            .status,
        Status::Unknown
    );
    assert_eq!(
        engine
            .submit(state, request("third", "a"))
            .await
            .unwrap_err(),
        "prior_operation_unknown"
    );
    assert_eq!(platform.dispatches.lock().unwrap().len(), 1);
    server.abort();
}
#[tokio::test]
async fn cancelled_waiting_operation_never_calls_platform() {
    let (state, platform, server, _dir) = fixture().await;
    let engine = Arc::new(Engine::new());
    engine
        .submit(state.clone(), request("first", "a"))
        .await
        .unwrap();
    platform.blocked_started.notified().await;
    engine
        .submit(state.clone(), request("second", "a"))
        .await
        .unwrap();
    engine.registry.lock().unwrap().live["second"]
        .cancelled
        .store(true, Ordering::Release);
    platform.release.notify_one();
    assert_eq!(
        finished(&state, "second").await.operation.status,
        Status::Cancelled
    );
    assert_eq!(platform.dispatches.lock().unwrap().len(), 1);
    server.abort();
}
#[tokio::test]
async fn queue_capacity_is_checked_before_persistence() {
    let (state, platform, server, _dir) = fixture().await;
    let engine = Arc::new(Engine::new());
    engine
        .submit(state.clone(), request("first", "a"))
        .await
        .unwrap();
    platform.blocked_started.notified().await;
    for index in 1..MAX_SESSION_PENDING {
        engine
            .submit(state.clone(), request(&format!("queued-{index}"), "a"))
            .await
            .unwrap();
    }
    assert_eq!(
        engine
            .submit(state.clone(), request("overflow", "a"))
            .await
            .unwrap_err(),
        "channel_queue_full"
    );
    assert!(load(&state, "overflow").await.unwrap().is_none());
    platform.release.notify_one();
    server.abort();
}
#[tokio::test]
async fn restart_marks_unfinished_unknown_without_replaying_private_input() {
    let (state, platform, server, _dir) = fixture().await;
    let req = request("old", "a");
    let stored = Stored {
        process_id: "old-process".into(),
        request_hash: fingerprint(&req).unwrap(),
        source: req.source.clone(),
        completion_context: completion_context(&req.context),
        completion_acknowledged: false,
        operation: Operation {
            request_id: "old".into(),
            callback_target_id: None,
            session_ref: "a".into(),
            status: Status::Running,
            phase: "callback".into(),
            turn_id: None,
            connection_id: None,
            error_code: None,
            cancel_requested: false,
            delivery_unknown: false,
            control_session: None,
            control_pending: false,
        },
    };
    persist(&state, &stored).await.unwrap();
    let queued_request = request("old-queued", "a");
    let mut queued = stored.clone();
    queued.request_hash = fingerprint(&queued_request).unwrap();
    queued.operation.request_id = queued_request.request_id.clone();
    queued.operation.status = Status::Queued;
    queued.operation.phase = "waiting".into();
    queued.completion_context = completion_context(&queued_request.context);
    persist(&state, &queued).await.unwrap();
    let engine = Arc::new(Engine::new());
    let result = engine.submit(state.clone(), req).await.unwrap();
    assert_eq!(result.status, Status::Unknown);
    assert_eq!(result.error_code.as_deref(), Some("process_restarted"));
    assert!(platform.dispatches.lock().unwrap().is_empty());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !platform.completions.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let body = platform.completions.lock().unwrap()[0].clone();
    assert_eq!(body["context"]["entry"], "managed");
    assert!(matches!(
        body["context"]["jobId"].as_str(),
        Some("old" | "old-queued")
    ));
    assert!(body.get("input").is_none());
    assert_eq!(engine.registry.lock().unwrap().blocked["a"].len(), 2);
    platform.status_known.store(true, Ordering::Release);
    let mut old = load(&state, "old").await.unwrap().unwrap();
    reconcile_unknown(&engine, &state, &mut old).await.unwrap();
    assert_eq!(engine.registry.lock().unwrap().blocked["a"].len(), 1);
    assert_eq!(
        engine
            .submit(state.clone(), request("new", "a"))
            .await
            .unwrap_err(),
        "prior_operation_unknown",
        "one confirmed operation cannot release another uncertain operation"
    );
    let mut queued = load(&state, "old-queued").await.unwrap().unwrap();
    reconcile_unknown(&engine, &state, &mut queued)
        .await
        .unwrap();
    assert!(!engine.registry.lock().unwrap().blocked.contains_key("a"));
    assert!(platform.dispatches.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn completion_failure_retries_only_notification() {
    let (state, platform, server, _dir) = fixture().await;
    platform.reject_completion.store(true, Ordering::Release);
    let engine = Arc::new(Engine::new());
    engine
        .submit(state.clone(), request("one", "a"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !platform.completions.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !load(&state, "one")
            .await
            .unwrap()
            .unwrap()
            .completion_acknowledged
    );
    platform.reject_completion.store(false, Ordering::Release);
    engine.notify.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if load(&state, "one")
                .await
                .unwrap()
                .unwrap()
                .completion_acknowledged
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(platform.completions.lock().unwrap().len() >= 2);
    assert_eq!(platform.dispatches.lock().unwrap().len(), 1);
    server.abort();
}
#[tokio::test]
async fn model_waits_for_current_turn_then_guards_callback() {
    let (state, platform, server, _dir) = fixture().await;
    state
        .connection_manager
        .insert_test_connection(
            "existing",
            crate::models::agent::AgentType::Codex,
            None,
            state.emitter.clone(),
        )
        .await;
    let session = state
        .connection_manager
        .get_state("existing")
        .await
        .unwrap();
    {
        let mut session = session.write().await;
        session.conversation_id = Some(123);
        session.turn_in_flight = true;
    }
    let engine = Arc::new(Engine::new());
    let mut model = request("first", "channel:stable");
    model.input = Input::Text {
        text: "/model better".into(),
    };
    model.context["sessionKey"] = "codeg:123".into();
    engine.submit(state.clone(), model).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(platform.dispatches.lock().unwrap().is_empty());
    let lock = state
        .connection_manager
        .clone_prompt_lock("existing")
        .await
        .unwrap();
    assert!(
        lock.try_lock().is_ok(),
        "waiting for an active turn must not block cancellation"
    );
    {
        let mut session = session.write().await;
        session.turn_in_flight = false;
        session.turns_completed += 1;
    }
    tokio::time::timeout(Duration::from_secs(2), platform.blocked_started.notified())
        .await
        .unwrap();
    assert!(
        lock.try_lock().is_err(),
        "model callback must exclude ordinary prompt admission"
    );
    assert_eq!(
        platform.dispatches.lock().unwrap()[0]["parsedCommand"],
        json!({"name":"model","arguments":"better"})
    );
    let blocks = || {
        vec![crate::acp::types::PromptInputBlock::Text {
            text: "new turn".into(),
        }]
    };
    let direct = tokio::time::timeout(
        Duration::from_millis(100),
        state.connection_manager.send_prompt("existing", blocks()),
    )
    .await
    .expect("direct send should reject without waiting for the callback");
    assert!(matches!(
        direct,
        Err(crate::acp::error::AcpError::TurnInProgress)
    ));
    let linked = tokio::time::timeout(
        Duration::from_millis(100),
        state.connection_manager.send_prompt_linked(
            &state.db,
            "existing",
            blocks(),
            Some(1),
            None,
            None,
        ),
    )
    .await
    .expect("linked send should reject without waiting for the callback");
    assert!(matches!(
        linked,
        Err(crate::acp::error::AcpError::TurnInProgress)
    ));
    platform.release.notify_one();
    finished(&state, "first").await;
    assert!(
        lock.try_lock().is_ok(),
        "terminal callback must release the prompt lock"
    );
    server.abort();
}
#[tokio::test]
async fn cancel_during_busy_wait_does_not_wait_for_turn_completion() {
    let (state, _platform, server, _dir) = fixture().await;
    state
        .connection_manager
        .insert_test_connection(
            "cancel-existing",
            crate::models::agent::AgentType::Codex,
            None,
            state.emitter.clone(),
        )
        .await;
    state
        .connection_manager
        .get_state("cancel-existing")
        .await
        .unwrap()
        .write()
        .await
        .turn_in_flight = true;
    let cancelled = Arc::new(AtomicBool::new(false));
    let task = {
        let state = state.clone();
        let cancelled = cancelled.clone();
        tokio::spawn(async move {
            acquire_control(&state, Some("cancel-existing"), None, &cancelled).await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(state
        .connection_manager
        .clone_prompt_lock("cancel-existing")
        .await
        .unwrap()
        .try_lock()
        .is_ok());
    cancelled.store(true, Ordering::Release);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err((Status::Cancelled, "cancelled_before_dispatch"))
    ));
    server.abort();
}

#[tokio::test]
async fn status_reconciliation_requires_evidence_and_never_redispatches() {
    let (state, platform, server, _dir) = fixture().await;
    platform.malformed.store(true, Ordering::Release);
    let engine = Arc::new(Engine::new());
    engine
        .submit(state.clone(), request("one", "a"))
        .await
        .unwrap();
    let mut stored = finished(&state, "one").await;
    assert_eq!(stored.operation.status, Status::Unknown);
    reconcile_unknown(&engine, &state, &mut stored)
        .await
        .unwrap();
    assert_eq!(stored.operation.status, Status::Unknown);
    assert!(engine.registry.lock().unwrap().blocked.contains_key("a"));
    platform.status_known.store(true, Ordering::Release);
    reconcile_unknown(&engine, &state, &mut stored)
        .await
        .unwrap();
    assert_eq!(stored.operation.status, Status::Completed);
    assert!(stored.operation.delivery_unknown);
    assert!(!engine.registry.lock().unwrap().blocked.contains_key("a"));
    assert_eq!(
        engine
            .submit(state.clone(), request("one", "a"))
            .await
            .unwrap()
            .status,
        Status::Completed
    );
    assert_eq!(
        platform.dispatches.lock().unwrap().len(),
        1,
        "neither query nor duplicate may run dispatch again"
    );
    server.abort();
}

#[tokio::test]
async fn uncertain_model_holds_admission_until_read_only_confirmation() {
    let (state, platform, server, _dir) = fixture().await;
    for id in ["unknown-model", "reconnected-model"] {
        state
            .connection_manager
            .insert_test_connection(
                id,
                crate::models::agent::AgentType::Codex,
                None,
                state.emitter.clone(),
            )
            .await;
        state
            .connection_manager
            .get_state(id)
            .await
            .unwrap()
            .write()
            .await
            .conversation_id = Some(987654);
    }
    platform.malformed.store(true, Ordering::Release);
    let engine = Arc::new(Engine::new());
    let mut model = request("unknown-command", "unknown-channel");
    model.input = Input::Text {
        text: "/model better".into(),
    };
    model.context["sessionKey"] = "codeg:987654".into();
    engine.submit(state.clone(), model).await.unwrap();
    let mut stored = finished(&state, "unknown-command").await;
    assert_eq!(stored.operation.status, Status::Unknown);
    assert!(stored.operation.control_pending);
    assert!(
        control_active(stored.operation.connection_id.as_deref().unwrap()),
        "durable operation must identify the actual guarded connection"
    );
    for id in ["unknown-model", "reconnected-model"] {
        assert!(control_blocks(&state.connection_manager, id, None).await);
    }
    let cancelled = AtomicBool::new(false);
    assert!(matches!(
        acquire_control(&state, Some("reconnected-model"), None, &cancelled).await,
        Err((Status::Failed, "session_control_in_progress"))
    ));
    assert!(
        control_blocks(&state.connection_manager, "reconnected-model", None).await,
        "another rejected model cannot clear the uncertain model's guard"
    );
    reconcile_unknown(&engine, &state, &mut stored)
        .await
        .unwrap();
    assert!(
        stored.operation.control_pending,
        "unknown status cannot release admission"
    );
    platform.status_known.store(true, Ordering::Release);
    reconcile_unknown(&engine, &state, &mut stored)
        .await
        .unwrap();
    assert!(!stored.operation.control_pending);
    for id in ["unknown-model", "reconnected-model"] {
        assert!(!control_blocks(&state.connection_manager, id, None).await);
    }
    assert_eq!(platform.dispatches.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn disconnected_session_model_blocks_reconnected_web_prompt() {
    let (state, _platform, server, _dir) = fixture().await;
    let cancelled = AtomicBool::new(false);
    let control = acquire_control(&state, None, Some(987655), &cancelled)
        .await
        .unwrap();
    assert!(control_blocks(&state.connection_manager, "new-connection", Some(987655)).await);
    assert!(acquire_control(&state, None, Some(987655), &cancelled)
        .await
        .is_err());
    drop(control);
    assert!(!control_blocks(&state.connection_manager, "new-connection", Some(987655)).await);
    server.abort();
}

#[tokio::test]
async fn model_resolves_new_binding_then_waits_for_its_active_turn() {
    let (state, platform, server, _dir) = fixture().await;
    state
        .connection_manager
        .insert_test_connection(
            "new-backing",
            crate::models::agent::AgentType::Codex,
            None,
            state.emitter.clone(),
        )
        .await;
    let session = state
        .connection_manager
        .get_state("new-backing")
        .await
        .unwrap();
    {
        let mut session = session.write().await;
        session.conversation_id = Some(987656);
        session.turn_in_flight = true;
    }
    *platform.resolved_session.lock().unwrap() = Some("codeg:987656".into());
    let engine = Arc::new(Engine::new());
    let mut request = request("first", "new-channel");
    request.input = Input::Text {
        text: "/model better".into(),
    };
    request.context["sessionKey"] = Value::Null;
    request.context["expectedSessionKey"] = Value::Null;
    engine.submit(state.clone(), request).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(platform.resolutions.lock().unwrap().len(), 1);
    assert!(platform.dispatches.lock().unwrap().is_empty());
    session.write().await.turn_in_flight = false;
    tokio::time::timeout(Duration::from_secs(2), platform.blocked_started.notified())
        .await
        .unwrap();
    {
        let dispatches = platform.dispatches.lock().unwrap();
        let context = &dispatches[0]["context"];
        assert_eq!(context["sessionKey"], "codeg:987656");
        assert_eq!(context["modelSessionKey"], "codeg:987656");
        assert!(context["expectedSessionKey"].is_null());
    }
    assert!(control_blocks(&state.connection_manager, "new-backing", None).await);
    platform.release.notify_one();
    assert_eq!(
        finished(&state, "first").await.operation.status,
        Status::Completed
    );
    server.abort();
}

#[tokio::test]
async fn conversation_gate_closes_disconnected_model_and_new_web_connection_race() {
    let (state, _platform, server, _dir) = fixture().await;
    let initial_gate = conversation_gate(987657).lock_owned().await;
    let model = {
        let state = state.clone();
        tokio::spawn(async move {
            acquire_control(&state, None, Some(987657), &AtomicBool::new(false)).await
        })
    };
    // Let model discovery observe no connection and queue on the session gate.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut commands = state
        .connection_manager
        .insert_test_connection_live(
            "web-reconnected-race",
            crate::models::agent::AgentType::Codex,
            None,
            state.emitter.clone(),
        )
        .await;
    state
        .connection_manager
        .get_state("web-reconnected-race")
        .await
        .unwrap()
        .write()
        .await
        .conversation_id = Some(987657);
    let web = {
        let state = state.clone();
        tokio::spawn(async move {
            state
                .connection_manager
                .send_prompt(
                    "web-reconnected-race",
                    vec![crate::acp::types::PromptInputBlock::Text {
                        text: "next message".into(),
                    }],
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!web.is_finished());
    drop(initial_gate);
    let model_guard = tokio::time::timeout(Duration::from_secs(1), model)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(model_guard.is_some());
    assert!(
        !web.is_finished(),
        "Web passed its early check but must wait for the model's canonical gate"
    );
    assert!(
        commands.try_recv().is_err(),
        "no prompt can be sent during the model operation"
    );
    drop(model_guard);
    tokio::time::timeout(Duration::from_secs(1), web)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(commands.try_recv().is_ok());
    server.abort();
}

#[tokio::test]
async fn model_control_uses_current_connection_after_discovery_reconnect() {
    let (state, _platform, server, _dir) = fixture().await;
    state
        .connection_manager
        .insert_test_connection(
            "current-after-discovery",
            crate::models::agent::AgentType::Codex,
            None,
            state.emitter.clone(),
        )
        .await;
    state
        .connection_manager
        .get_state("current-after-discovery")
        .await
        .unwrap()
        .write()
        .await
        .conversation_id = Some(987658);
    let control = acquire_control(
        &state,
        Some("obsolete-discovery"),
        Some(987658),
        &AtomicBool::new(false),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(control.connection_id, "current-after-discovery");
    assert!(control_active("current-after-discovery"));
    assert!(!control_active("obsolete-discovery"));
    drop(control);
    assert!(!control_blocks(&state.connection_manager, "current-after-discovery", None).await);
    server.abort();
}

#[tokio::test]
async fn callback_target_validation_happens_before_admission_and_legacy_hash_is_unchanged() {
    let (state, platform, server, _dir) = fixture().await;
    let engine = Arc::new(Engine::new());
    let mut req = request("bad-target", "a");
    let legacy = serde_json::to_value(&req).unwrap();
    assert!(legacy.get("callbackTargetId").is_none());
    let expected = format!("{:x}", Sha256::digest(serde_json::to_vec(&req).unwrap()));
    assert_eq!(fingerprint(&req).unwrap(), expected);
    req.callback_target_id = Some("0".repeat(64));
    assert_eq!(
        engine.submit(state.clone(), req.clone()).await.unwrap_err(),
        "invalid_channel_callback_target"
    );
    assert!(load(&state, "bad-target").await.unwrap().is_none());
    let id =
        super::super::channel_callback::register(&state.db.conn, "slack", "https://slack.example")
            .await
            .unwrap();
    req.callback_target_id = Some(id.clone());
    assert_eq!(
        engine.submit(state.clone(), req).await.unwrap_err(),
        "invalid_channel_callback_target"
    );
    assert!(platform.dispatches.lock().unwrap().is_empty());
    // The target lives in the durable operation and survives serde/restart;
    // absent fields in old operations still deserialize as None.
    let old = json!({"requestId":"old","sessionRef":"a","status":"running","phase":"dispatch","cancelRequested":false});
    let mut op: Operation = serde_json::from_value(old).unwrap();
    assert!(op.callback_target_id.is_none());
    op.callback_target_id = Some(id.clone());
    let restored: Operation = serde_json::from_value(serde_json::to_value(op).unwrap()).unwrap();
    assert_eq!(restored.callback_target_id.as_deref(), Some(id.as_str()));
    server.abort();
}
