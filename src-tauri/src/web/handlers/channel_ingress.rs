//! Thin authenticated HTTP surface for the shared channel coordinator.
use crate::{
    app_error::{AppCommandError, AppErrorCode},
    app_state::AppState,
    chat_channel::ingress,
};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

fn error(code: &'static str) -> AppCommandError {
    match code {
        "request_id_conflict" | "prior_operation_unknown" | "channel_queue_full" => {
            AppCommandError::new(AppErrorCode::TurnInProgress, code)
        }
        "invalid_request" | "request_too_large" => AppCommandError::invalid_input(code),
        _ => AppCommandError::task_execution_failed(code),
    }
}
pub async fn channel_submit(
    Extension(state): Extension<Arc<AppState>>,
    Json(request): Json<ingress::Request>,
) -> Result<Json<ingress::Operation>, AppCommandError> {
    // Once durable admission starts, HTTP cancellation must not leave a queued
    // marker without its worker; the handle is only used to report the result.
    tokio::spawn(ingress::submit(state, request))
        .await
        .map_err(|_| error("submission_interrupted"))?
        .map(Json)
        .map_err(error)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperationRequest {
    pub request_id: String,
}
pub async fn channel_operation(
    Extension(state): Extension<Arc<AppState>>,
    Json(request): Json<OperationRequest>,
) -> Result<Json<Value>, AppCommandError> {
    ingress::get(state, &request.request_id)
        .await
        .map(|operation| Json(json!({"operation":operation})))
        .map_err(error)
}
pub async fn channel_cancel(
    Extension(state): Extension<Arc<AppState>>,
    Json(request): Json<OperationRequest>,
) -> Result<Json<Value>, AppCommandError> {
    ingress::cancel(state, &request.request_id)
        .await
        .map(|operation| Json(json!({"operation":operation})))
        .map_err(error)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectRequest {
    pub kind: String,
    pub platform: String,
}
pub async fn legacy_channel_inspect(
    Json(request): Json<InspectRequest>,
) -> Result<Json<Value>, AppCommandError> {
    if request.platform != "telegram" {
        return Err(AppCommandError::invalid_input("unsupported_legacy_channel"));
    }
    let result = match request.kind.as_str() {
        "hermes" => crate::chat_channel::legacy_runtime::inspect_hermes().await,
        "openclaw" => crate::chat_channel::legacy_openclaw_inspect::inspect().await,
        _ => return Err(AppCommandError::invalid_input("unsupported_legacy_runtime")),
    };
    result
        .map(Json)
        .map_err(|_| error("legacy_inspection_failed"))
}
