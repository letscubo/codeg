#![cfg(feature = "test-utils")]

use axum_test::TestServer;
use codeg_lib::acp::connection::ConnectionCommand;
use codeg_lib::acp::session_state::ConnectionTransport;
use codeg_lib::acp::types::{AcpEvent, PermissionOptionInfo};
use codeg_lib::app_state::AppState;
use codeg_lib::db::test_helpers::fresh_in_memory_db;
use codeg_lib::models::AgentType;
use codeg_lib::web::{event_bridge::EventEmitter, router::build_router, shutdown::ShutdownSignal};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn session_permissions_http_reaches_native_cli_approval_but_rejects_stale_answers() {
    let data = tempfile::tempdir().unwrap();
    let assets = tempfile::tempdir().unwrap();
    let state = Arc::new(AppState::new_for_test(
        fresh_in_memory_db().await,
        data.path().into(),
    ));
    let router = build_router(
        state.clone(),
        "test-token".into(),
        assets.path().into(),
        Arc::new(ShutdownSignal::new()),
    );
    let server = TestServer::new(router).unwrap();
    for (id, agent) in [
        ("claude-approval", AgentType::ClaudeCode),
        ("dsh-approval", AgentType::DeepSeek),
    ] {
        let mut commands = state
            .connection_manager
            .insert_test_connection_live(id, agent, None, EventEmitter::Noop)
            .await;
        let session = state.connection_manager.get_state(id).await.unwrap();
        session.write().await.transport = ConnectionTransport::Cli;
        session
            .write()
            .await
            .apply_event(&AcpEvent::PermissionRequest {
                request_id: "pending".into(),
                tool_call: json!({"toolCallId":"call","title":"shell"}),
                queued: 0,
                options: vec![PermissionOptionInfo {
                    option_id: "reject".into(),
                    name: "Reject".into(),
                    kind: "reject_once".into(),
                    meta: None,
                }],
            });
        let wrong = server
            .post("/api/acp_respond_permission")
            .add_header("authorization", "Bearer test-token")
            .json(&json!({"connectionId":id,"requestId":"other-session","optionId":"reject"}))
            .await;
        assert_ne!(wrong.status_code(), 200);
        assert!(commands.try_recv().is_err());
        let response = server
            .post("/api/acp_respond_permission")
            .add_header("authorization", "Bearer test-token")
            .json(&json!({"connectionId":id,"requestId":"pending","optionId":"reject"}))
            .await;
        assert_eq!(response.status_code(), 200);
        assert!(
            matches!(commands.recv().await, Some(ConnectionCommand::RespondPermission { request_id, option_id }) if request_id == "pending" && option_id == "reject")
        );
        // Permission support must not accidentally enable the unsupported CLI set-mode API.
        assert_ne!(
            server
                .post("/api/acp_set_mode")
                .add_header("authorization", "Bearer test-token")
                .json(&json!({"connectionId":id,"modeId":"full"}))
                .await
                .status_code(),
            200
        );
        session.write().await.turn_in_flight = true;
        assert_ne!(
            server
                .post("/api/acp_disconnect_if_idle")
                .add_header("authorization", "Bearer test-token")
                .json(&json!({"connectionId":id}))
                .await
                .status_code(),
            200
        );
        assert!(commands.try_recv().is_err());
    }
}
