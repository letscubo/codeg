//! `GET /api/myclaw/activity` —— fork(letscubo)的忙闲探针。
//!
//! 重点守 `active_turns`:它必须读每条连接**实时**的 `state.status`(事件流写入),
//! 而不是 `AgentConnection.status`(建连时的快照,之后不再更新)。平台用它做暂停前的
//! 最后确认,读错一边就会在一轮跑到一半时把实例停掉。

use std::sync::Arc;

use axum_test::TestServer;
use codeg_lib::acp::types::{AcpEvent, ConnectionStatus};
use codeg_lib::app_state::AppState;
use codeg_lib::db::test_helpers::fresh_in_memory_db;
use codeg_lib::models::agent::AgentType;
use codeg_lib::web::event_bridge::emit_with_state;
use codeg_lib::web::router::build_router;
use codeg_lib::web::shutdown::ShutdownSignal;
use serde_json::Value;

const TEST_TOKEN: &str = "activity-test-token";

async fn build_server() -> (
    TestServer,
    Arc<AppState>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let data_dir = tempfile::tempdir().expect("data dir");
    let static_dir = tempfile::tempdir().expect("static dir");
    let db = fresh_in_memory_db().await;
    let state = Arc::new(AppState::new_for_test(db, data_dir.path().to_path_buf()));
    let router = build_router(
        Arc::clone(&state),
        TEST_TOKEN.to_string(),
        static_dir.path().to_path_buf(),
        Arc::new(ShutdownSignal::new()),
    );
    let server = TestServer::builder().build(router).expect("test server");
    (server, state, data_dir, static_dir)
}

async fn activity(server: &TestServer) -> Value {
    let resp = server
        .get("/api/myclaw/activity")
        .add_header("authorization", format!("Bearer {TEST_TOKEN}"))
        .await;
    assert_eq!(resp.status_code(), 200);
    resp.json::<Value>()
}

async fn set_status(state: &Arc<AppState>, conn_id: &str, status: ConnectionStatus) {
    let s = state
        .connection_manager
        .get_state(conn_id)
        .await
        .expect("registered connection");
    // 走生产同一条路:StatusChanged 事件经 emit_with_state 写进 SessionState
    emit_with_state(&s, &state.emitter, AcpEvent::StatusChanged { status }).await;
}

#[tokio::test]
async fn activity_requires_token() {
    let (server, _state, _d, _s) = build_server().await;
    let resp = server.get("/api/myclaw/activity").await;
    assert_eq!(resp.status_code(), 401);
}

#[tokio::test]
async fn active_turns_counts_prompting_connections_only() {
    let (server, state, _d, _s) = build_server().await;

    let body = activity(&server).await;
    assert_eq!(body["active_turns"], 0, "没有连接时为 0");
    assert_eq!(body["ws_clients"], 0);
    assert!(body["collected_at"].is_string());

    for id in ["conn-a", "conn-b", "conn-c"] {
        state
            .connection_manager
            .insert_test_connection(id, AgentType::ClaudeCode, None, state.emitter.clone())
            .await;
    }
    assert_eq!(
        activity(&server).await["active_turns"],
        0,
        "连上了但没在跑一轮,不算"
    );

    set_status(&state, "conn-a", ConnectionStatus::Prompting).await;
    set_status(&state, "conn-b", ConnectionStatus::Prompting).await;
    set_status(&state, "conn-c", ConnectionStatus::Error).await;
    assert_eq!(
        activity(&server).await["active_turns"],
        2,
        "只数 Prompting,Error 不算"
    );

    // 一轮结束:回到 Connected 就不再计入。AgentConnection.status 始终是建连时的
    // Connected —— 读它的话上面那步就会是 0,这条断言守的正是读对了哪一边
    set_status(&state, "conn-a", ConnectionStatus::Connected).await;
    assert_eq!(activity(&server).await["active_turns"], 1);
}
