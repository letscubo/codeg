//! fork(letscubo)专属: CLI transport (Claude Code, DeepSeek Harness) —
//! `cli_prompt` / `cli_cancel`.
//!
//! `cli_prompt` folds `acp_connect` + `acp_prompt` into one call: without a
//! `connectionId` it registers (or reuses) a CLI connection, then admits the
//! prompt through the same manager path `acp_prompt` uses. Error messages start
//! with a stable machine code (`transport_mismatch: …`) for callers to match.
//! For `deepseek` the first turn has no session id yet (the harness mints
//! `session-<uuid>` when the process starts), so `sessionId` in the result is
//! `null` until the `session_started` event has been applied.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{extract::Extension, Json};
use serde::{Deserialize, Serialize};

use crate::acp::cli::{
    binary, codex_binary, dsh_binary, CliConnectError, CliConnectRequest, CliConnection,
};
use crate::acp::error::AcpError;
use crate::acp::manager::ConnectionManager;
use crate::acp::session_state::ConnectionTransport;
use crate::acp::types::PromptInputBlock;
use crate::app_error::{AppCommandError, AppErrorCode};
use crate::app_state::AppState;
use crate::commands::acp as acp_commands;
use crate::models::agent::AgentType;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliPromptParams {
    #[serde(default)]
    pub connection_id: Option<String>,
    #[serde(default)]
    pub agent_type: Option<AgentType>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    pub folder_id: Option<i32>,
    pub conversation_id: Option<i32>,
    pub blocks: Vec<PromptInputBlock>,
    #[serde(default)]
    pub client_message_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    /// Provider route for `model` (deepseek: the `agent-default-model` patch).
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub runtime_env: Option<BTreeMap<String, String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CliPromptResult {
    pub connection_id: String,
    pub session_id: Option<String>,
    pub conversation_id: Option<i32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliCancelParams {
    pub connection_id: String,
}

pub async fn cli_prompt(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<CliPromptParams>,
) -> Result<Json<CliPromptResult>, AppCommandError> {
    if params.blocks.is_empty() {
        return Err(coded_invalid(
            "invalid_params",
            "blocks must contain at least one content block",
        ));
    }
    if params.conversation_id.is_some() && params.folder_id.is_none() {
        return Err(coded_invalid(
            "invalid_params",
            "conversationId requires folderId",
        ));
    }
    let manager = &state.connection_manager;
    let (connection_id, created) = match params.connection_id.clone() {
        Some(id) => {
            ensure_cli_connection(manager, &id).await?;
            (id, false)
        }
        None => {
            let conn = connect(&state, &params).await?;
            (conn.connection_id, !conn.reused)
        }
    };

    if let Some(model) = params
        .model
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty())
    {
        if let Some(session) = manager.get_state(&connection_id).await {
            session.write().await.cli_model = Some(model.to_string());
        }
    }

    let sent = manager
        .send_prompt_linked_with_message_id(
            &state.db,
            &connection_id,
            params.blocks,
            params.folder_id,
            params.conversation_id,
            None,
            params.client_message_id,
        )
        .await;
    let conversation_id = match sent {
        Ok(conversation_id) => conversation_id,
        Err(err) => {
            if created {
                let _ = manager.disconnect(&connection_id).await;
            }
            return Err(prompt_error(err));
        }
    };
    let session_id = match manager.get_state(&connection_id).await {
        Some(session) => session.read().await.external_id.clone(),
        None => None,
    };
    Ok(Json(CliPromptResult {
        connection_id,
        session_id,
        conversation_id,
    }))
}

pub async fn cli_cancel(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<CliCancelParams>,
) -> Result<Json<()>, AppCommandError> {
    let manager = &state.connection_manager;
    ensure_cli_connection(manager, &params.connection_id).await?;
    let in_flight = match manager.get_state(&params.connection_id).await {
        Some(session) => session.read().await.turn_in_flight,
        None => return Err(connection_not_found(&params.connection_id)),
    };
    // Idempotent: nothing to stop between turns.
    if !in_flight {
        return Ok(Json(()));
    }
    manager
        .cancel(&state.db.conn, &params.connection_id)
        .await
        .map_err(|e| match e {
            AcpError::ConnectionNotFound(_) => connection_not_found(&params.connection_id),
            other => AppCommandError::task_execution_failed(other.to_string()),
        })?;
    Ok(Json(()))
}

/// Guard for `acp_*` handlers: a CLI connection is driven only through `cli_*`.
/// A missing connection passes so the handler reports it as it always has.
pub(crate) async fn reject_cli_connection(
    manager: &ConnectionManager,
    connection_id: &str,
    code: &'static str,
) -> Result<(), AppCommandError> {
    match manager.connection_transport(connection_id).await {
        Some(ConnectionTransport::Cli) => Err(coded_invalid(
            code,
            "this connection uses the CLI transport; use cli_prompt / cli_cancel",
        )),
        _ => Ok(()),
    }
}

async fn connect(
    state: &AppState,
    params: &CliPromptParams,
) -> Result<CliConnection, AppCommandError> {
    let agent_type = params.agent_type.ok_or_else(|| {
        coded_invalid(
            "invalid_params",
            "agentType is required when connectionId is omitted",
        )
    })?;
    if !CLI_TRANSPORT_AGENTS.contains(&agent_type) {
        return Err(coded_invalid(
            "unsupported_agent",
            format!(
                "{agent_type} is not supported by the CLI transport; supported: claude_code, deepseek, codex"
            ),
        ));
    }
    let working_dir = params
        .working_dir
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            coded_invalid(
                "invalid_params",
                "workingDir is required when connectionId is omitted",
            )
        })?;
    if !working_dir.is_dir() {
        return Err(coded_invalid(
            "invalid_params",
            format!("workingDir is not a directory: {}", working_dir.display()),
        ));
    }
    if let Some(session_id) = params.session_id.as_deref() {
        validate_cli_session_id(agent_type, session_id)?;
    }

    let mut runtime_env = acp_commands::build_session_runtime_env(
        &state.db,
        agent_type,
        params.session_id.as_deref(),
        &state.data_dir,
    )
    .await
    .map_err(|e| AppCommandError::configuration_missing(format!("runtime_env_failed: {e}")))?;
    if let Some(extra) = &params.runtime_env {
        runtime_env.extend(extra.clone());
    }
    let executable = match agent_type {
        AgentType::DeepSeek => dsh_binary::resolve_dsh_executable(&runtime_env).await,
        AgentType::Codex => codex_binary::resolve_codex_executable(&runtime_env).await,
        _ => binary::resolve_claude_executable(&runtime_env).await,
    }
    .map_err(|e| AppCommandError::dependency_missing(format!("cli_not_installed: {e}")))?;

    state
        .connection_manager
        .create_or_reuse_cli_connection(
            CliConnectRequest {
                agent_type,
                working_dir,
                session_id: params.session_id.clone(),
                runtime_env,
                executable,
                provider: params.provider.clone(),
                model: params.model.clone(),
                owner_window_label: None,
            },
            state.emitter.clone(),
        )
        .await
        .map_err(|e| match e {
            CliConnectError::SessionLocked { connection_id } => AppCommandError::already_exists(
                format!("session_locked: this session is held by connection {connection_id}"),
            ),
            CliConnectError::ProfileWriteFailed(message) => {
                AppCommandError::configuration_missing(format!("dsh_profile_failed: {message}"))
            }
        })
}

/// Agents the CLI transport can drive. Adding one needs a driver in
/// `acp::cli` and a binary resolver.
const CLI_TRANSPORT_AGENTS: &[AgentType] =
    &[AgentType::ClaudeCode, AgentType::DeepSeek, AgentType::Codex];

/// Claude session ids and Codex thread ids are uuids; the official DeepSeek Harness launcher
/// mints `session-<uuid>` and only continues an id in that exact spelling.
fn validate_cli_session_id(agent_type: AgentType, session_id: &str) -> Result<(), AppCommandError> {
    match agent_type {
        AgentType::DeepSeek => {
            // A bare uuid is a conversation from the retired `deepseek-acp`
            // bridge: the connection continues it in a fresh launcher session.
            if crate::acp::cli::is_dsh_session_id(session_id)
                || uuid::Uuid::parse_str(session_id).is_ok()
            {
                Ok(())
            } else {
                Err(coded_invalid(
                    "invalid_params",
                    "sessionId must be `session-<uuid>` (or a bridge uuid) for deepseek",
                ))
            }
        }
        _ => {
            if uuid::Uuid::parse_str(session_id).is_ok() {
                Ok(())
            } else {
                Err(coded_invalid("invalid_params", "sessionId must be a UUID"))
            }
        }
    }
}

async fn ensure_cli_connection(
    manager: &ConnectionManager,
    connection_id: &str,
) -> Result<(), AppCommandError> {
    match manager.connection_transport(connection_id).await {
        Some(ConnectionTransport::Cli) => Ok(()),
        Some(ConnectionTransport::Acp) => Err(coded_invalid(
            "transport_mismatch",
            "this connection uses the ACP transport; use acp_prompt / acp_cancel",
        )),
        None => Err(connection_not_found(connection_id)),
    }
}

fn prompt_error(err: AcpError) -> AppCommandError {
    let message = err.to_string();
    match err {
        AcpError::TurnInProgress => AppCommandError::new(AppErrorCode::TurnInProgress, message),
        AcpError::ConnectionNotFound(id) => connection_not_found(&id),
        _ => AppCommandError::task_execution_failed(message),
    }
}

fn connection_not_found(connection_id: &str) -> AppCommandError {
    AppCommandError::not_found(format!("connection_not_found: {connection_id}"))
}

fn coded_invalid(code: &str, message: impl std::fmt::Display) -> AppCommandError {
    AppCommandError::invalid_input(format!("{code}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_shape_depends_on_the_agent() {
        let uuid = "55960ec1-365e-4af9-8681-e6576043af2d";
        assert!(validate_cli_session_id(AgentType::ClaudeCode, uuid).is_ok());
        assert!(validate_cli_session_id(AgentType::ClaudeCode, "session-x").is_err());
        assert!(validate_cli_session_id(AgentType::DeepSeek, &format!("session-{uuid}")).is_ok());
        assert!(
            validate_cli_session_id(AgentType::DeepSeek, uuid).is_ok(),
            "a bridge id is continued, not rejected"
        );
        assert!(validate_cli_session_id(AgentType::DeepSeek, "session-nope").is_err());
        // codex thread ids (uuid v7) are uuids
        assert!(
            validate_cli_session_id(AgentType::Codex, "01a0b54e-568c-7310-aa4d-f777c3663fb7")
                .is_ok()
        );
        assert!(validate_cli_session_id(AgentType::Codex, "thread-x").is_err());
    }

    #[test]
    fn cli_transport_allow_list() {
        assert!(CLI_TRANSPORT_AGENTS.contains(&AgentType::ClaudeCode));
        assert!(CLI_TRANSPORT_AGENTS.contains(&AgentType::DeepSeek));
        assert!(CLI_TRANSPORT_AGENTS.contains(&AgentType::Codex));
    }
}
