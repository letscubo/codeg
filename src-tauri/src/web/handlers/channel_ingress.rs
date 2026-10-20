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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolveRequest {
    pub kind: String,
    pub platform: String,
    pub chat_id: String,
    pub sender_id: String,
    pub sender_name: Option<String>,
    #[serde(default)]
    pub create_session: bool,
}

/// The platform has authenticated the Telegram webhook; Hermes remains the
/// authority for sender grants, pairing and the retained native session.
pub async fn legacy_channel_resolve(
    Json(request): Json<ResolveRequest>,
) -> Result<Json<Value>, AppCommandError> {
    let valid_id = |value: &str| {
        !value.is_empty() && value.len() <= 20 && value.bytes().all(|b| b.is_ascii_digit())
    };
    if request.kind != "hermes"
        || request.platform != "telegram"
        || !valid_id(&request.chat_id)
        || request.chat_id != request.sender_id
        || request
            .sender_name
            .as_ref()
            .is_some_and(|name| name.len() > 256)
    {
        return Err(AppCommandError::invalid_input("unsupported_legacy_channel"));
    }
    crate::chat_channel::legacy_runtime::legacy_hermes_action(
        "resolve",
        &json!({
            "chatId": request.chat_id,
            "senderId": request.sender_id,
            "senderName": request.sender_name,
            "createSession": request.create_session,
        }),
    )
    .await
    .map(Json)
    .map_err(|_| error("legacy_resolution_failed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_resolution_rejects_non_private_or_foreign_identity_before_runtime() {
        for (kind, platform, chat_id, sender_id) in [
            ("hermes", "telegram", "-123", "123"),
            ("hermes", "telegram", "123", "456"),
            ("hermes", "slack", "123", "123"),
            ("openclaw", "telegram", "123", "123"),
        ] {
            let result = legacy_channel_resolve(Json(ResolveRequest {
                kind: kind.into(),
                platform: platform.into(),
                chat_id: chat_id.into(),
                sender_id: sender_id.into(),
                sender_name: None,
                create_session: true,
            }))
            .await;
            assert!(result.is_err());
        }
    }

    #[test]
    fn legacy_resolution_does_not_accept_runtime_or_session_overrides() {
        for key in ["pythonPath", "hermesHome", "sessionId", "apiUrl"] {
            let mut request = json!({
                "kind":"hermes", "platform":"telegram", "chatId":"123", "senderId":"123"
            });
            request[key] = json!("untrusted");
            assert!(serde_json::from_value::<ResolveRequest>(request).is_err());
        }
    }
}
