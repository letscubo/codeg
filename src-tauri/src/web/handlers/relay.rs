//! Platform-only relay capability negotiation and terminal reconciliation.
use crate::app_state::AppState;
use axum::{extract::Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
pub async fn get_relay_capabilities() -> Json<Value> {
    Json(
        json!({"channelIngressVersion":1,"channelCallbackVersion":1,"deliveryVersion":2,"legacyHermesTelegramVersion":1,"kinds":["telegram","slack"],"nativeSlackStreaming":true,"cancelByTurnId":true,"resultFormats":["legacy","compact-v1"]}),
    )
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRelayResult {
    turn_id: String,
    #[serde(default)]
    result_format: crate::chat_channel::turn_relay::ResultFormat,
}
pub async fn get_relay_result(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<GetRelayResult>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    if params.turn_id.is_empty() || params.turn_id.len() > 200 {
        return Err((axum::http::StatusCode::BAD_REQUEST, "invalid turnId".into()));
    }
    let result = crate::chat_channel::turn_relay::get_result(
        &state.db.conn,
        &params.turn_id,
        params.result_format,
    )
    .await
    .map_err(|_| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "relay result unavailable".into(),
        )
    })?;
    Ok(Json(json!({"result":result})))
}

pub async fn cancel_relay_turn(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<GetRelayResult>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    if params.turn_id.is_empty() || params.turn_id.len() > 200 {
        return Err((axum::http::StatusCode::BAD_REQUEST, "invalid turnId".into()));
    }
    let status = crate::chat_channel::turn_relay::cancel_turn(&state, &params.turn_id)
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(json!({"status":status})))
}
