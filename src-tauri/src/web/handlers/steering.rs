//! Native-only steering. All endpoints use the normal authenticated Codeg API.
use crate::acp::steering::{
    NativeSteerInput, NativeSteerLookup, NativeSteerResult, NativeSteeringState,
};
use crate::app_error::{AppCommandError, AppErrorCode};
use crate::app_state::AppState;
use axum::{extract::Extension, Json};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StateInput {
    pub connection_id: String,
}

pub async fn native_steering_state(
    Extension(state): Extension<Arc<AppState>>,
    Json(input): Json<StateInput>,
) -> Json<NativeSteeringState> {
    Json(
        state
            .connection_manager
            .native_steering_state(&input.connection_id)
            .await,
    )
}
pub async fn native_steer(
    Extension(state): Extension<Arc<AppState>>,
    Json(input): Json<NativeSteerInput>,
) -> Result<Json<NativeSteerResult>, AppCommandError> {
    state
        .connection_manager
        .native_steer(input)
        .await
        .map(Json)
        .map_err(|e| AppCommandError::new(AppErrorCode::InvalidInput, e))
}
pub async fn native_steer_result(
    Extension(state): Extension<Arc<AppState>>,
    Json(input): Json<NativeSteerLookup>,
) -> Json<NativeSteerResult> {
    Json(state.connection_manager.native_steer_result(input).await)
}
