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

use crate::acp::cli::effort::{self, CliTurnOptions, EffortCapability};
use crate::acp::cli::{
    binary, codex_binary, dsh_binary, pi_binary, CliConnectError, CliConnectRequest, CliConnection,
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
    /// Final internal instructions for this request only; never part of the user echo.
    #[serde(default)]
    pub internal_prompt: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, deserialize_with = "effort::deserialize_effort")]
    pub reasoning_effort: Option<Option<String>>,
    /// Provider route for `model` (deepseek: the `agent-default-model` patch).
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub runtime_env: Option<BTreeMap<String, String>>,
    /// fork(letscubo)专属: 本轮边生成边推到渠道(见 chat_channel::turn_relay)。
    #[serde(default)]
    pub outbound: Option<crate::chat_channel::turn_relay::OutboundSpec>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CliPromptResult {
    pub connection_id: String,
    pub session_id: Option<String>,
    pub conversation_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<Option<String>>,
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
    cli_prompt_impl(state, params).await
}

/// Strict new route: an old server returns 404 instead of silently ignoring effort.
pub async fn cli_prompt_with_reasoning(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<CliPromptParams>,
) -> Result<Json<CliPromptResult>, AppCommandError> {
    if params.reasoning_effort.is_none() {
        return Err(coded_invalid(
            "reasoning_effort_required",
            "reasoningEffort must be present (a level or null)",
        ));
    }
    cli_prompt_impl(state, params).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliCapabilitiesParams {
    pub agent_type: AgentType,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub models: Vec<crate::acp::cli::sdk_effort::ModelRequest>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CliCapabilitiesResult {
    pub reasoning_effort: EffortCapability,
}

pub async fn cli_prompt_capabilities(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<CliCapabilitiesParams>,
) -> Result<Json<CliCapabilitiesResult>, AppCommandError> {
    let mut models = params.models;
    if let Some(model) = params.model {
        models.push(crate::acp::cli::sdk_effort::ModelRequest {
            model,
            provider: params.provider,
        });
    }
    crate::acp::cli::sdk_effort::validate_models(&models)
        .map_err(AppCommandError::invalid_input)?;
    let capability = if !matches!(
        params.agent_type,
        AgentType::ClaudeCode | AgentType::Pi | AgentType::DeepSeek
    ) {
        EffortCapability::unavailable(
            "Per-turn effort is unavailable for this runtime".into(),
            None,
        )
    } else {
        let env = acp_commands::build_session_runtime_env(
            &state.db,
            params.agent_type,
            None,
            &state.data_dir,
        )
        .await
        .map_err(|_| {
            AppCommandError::configuration_missing("Could not inspect Claude Code runtime")
        })?;
        let executable = match params.agent_type {
            AgentType::Pi => pi_binary::resolve_pi_executable(&env).await,
            AgentType::DeepSeek => dsh_binary::resolve_dsh_executable(&env).await,
            _ => binary::resolve_claude_executable(&env).await,
        };
        match executable {
            Ok(executable) if params.agent_type == AgentType::ClaudeCode => {
                effort::probe(&executable).await
            }
            Ok(executable) => {
                crate::acp::cli::sdk_effort::capability(
                    &effort::CliEffortContext {
                        agent_type: params.agent_type,
                        provider: None,
                        executable,
                        working_dir: state.data_dir.clone(),
                        runtime_env: env,
                    },
                    &models,
                )
                .await
            }
            Err(reason) => EffortCapability::unavailable(reason, None),
        }
    };
    Ok(Json(CliCapabilitiesResult {
        reasoning_effort: capability,
    }))
}

async fn cli_prompt_impl(
    state: Arc<AppState>,
    mut params: CliPromptParams,
) -> Result<Json<CliPromptResult>, AppCommandError> {
    crate::acp::myclaw_prompt::validate_internal_prompt(params.internal_prompt.as_deref())
        .map_err(AppCommandError::invalid_input)?;
    effort::validate_level(&params.reasoning_effort)
        .map_err(|reason| coded_invalid("unsupported_reasoning_effort", reason))?;
    if params.reasoning_effort.is_some()
        && params.connection_id.is_none()
        && !matches!(
            params.agent_type,
            Some(AgentType::ClaudeCode | AgentType::Pi | AgentType::DeepSeek | AgentType::Codex)
        )
    {
        return Err(coded_invalid(
            "unsupported_reasoning_effort",
            "Per-turn effort is unavailable for this runtime",
        ));
    }
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

    // Capture model + effort as one immutable turn snapshot. A rejected concurrent
    // request never writes options into the connection's currently running turn.
    let cli_options = if params.reasoning_effort.is_some() {
        let session = manager
            .get_state(&connection_id)
            .await
            .ok_or_else(|| coded_invalid("connection_not_found", "Connection not found"))?;
        let snapshot = session.read().await;
        let model = params.model.clone().or_else(|| snapshot.cli_model.clone());
        let context = snapshot.cli_effort_context.clone().ok_or_else(|| {
            coded_invalid(
                "unsupported_reasoning_effort",
                "Connection does not support per-turn reasoning effort",
            )
        })?;
        drop(snapshot);
        let options = context.turn_options(
            model,
            params.provider.clone(),
            params.reasoning_effort.clone(),
        );
        if let Err(reason) = context
            .resolve_turn(
                options.model.as_deref(),
                options.provider.as_deref(),
                &options.reasoning_effort,
            )
            .await
        {
            if created {
                let _ = manager.disconnect(&connection_id).await;
            }
            return Err(coded_invalid("reasoning_effort_unavailable", reason));
        }
        Some(options)
    } else if params
        .model
        .as_deref()
        .is_some_and(|m| !m.trim().is_empty())
        || params
            .provider
            .as_deref()
            .is_some_and(|p| !p.trim().is_empty())
    {
        // Legacy model changes use the same admission gate. None effort keeps
        // legacy driver behavior without probing or clearing native settings.
        let session = manager
            .get_state(&connection_id)
            .await
            .ok_or_else(|| connection_not_found(&connection_id))?;
        let snapshot = session.read().await;
        Some(CliTurnOptions {
            model: params
                .model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .map(str::to_owned)
                .or_else(|| snapshot.cli_model.clone()),
            provider: params.provider.clone().or_else(|| {
                snapshot
                    .cli_effort_context
                    .as_ref()
                    .and_then(|c| c.provider.clone())
            }),
            reasoning_effort: None,
        })
    } else {
        None
    };

    // 须在提交之前登记,这一轮的事件才不会漏
    let relay_turn = match params.outbound.take() {
        Some(spec) => {
            crate::chat_channel::turn_relay::register_for_prompt(
                &state,
                &connection_id,
                params.conversation_id,
                spec,
            )
            .await
        }
        None => None,
    };
    let sent = manager
        .send_prompt_linked_with_options(
            &state.db,
            &connection_id,
            params.blocks,
            params.folder_id,
            params.conversation_id,
            None,
            params.client_message_id,
            params.internal_prompt,
            cli_options,
        )
        .await;
    let conversation_id = match sent {
        Ok(conversation_id) => conversation_id,
        Err(err) => {
            if let Some(turn_id) = &relay_turn {
                crate::chat_channel::turn_relay::abort(&connection_id, turn_id);
            }
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
        reasoning_effort: params.reasoning_effort,
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
        AgentType::Pi => pi_binary::resolve_pi_executable(&runtime_env).await,
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
const CLI_TRANSPORT_AGENTS: &[AgentType] = &[
    AgentType::ClaudeCode,
    AgentType::DeepSeek,
    AgentType::Codex,
    // pi 没有官方 ACP;之前那座第三方桥会把 provider 失败吞成一句普通回复,
    // 所以 pi 直接走 CLI(见 acp::cli::pi_driver 的模块说明)。
    AgentType::Pi,
];

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
    fn effort_request_and_ack_distinguish_missing_null_and_level() {
        use serde_json::json;
        let parse = |value: serde_json::Value| {
            serde_json::from_value::<CliPromptParams>(value)
                .unwrap()
                .reasoning_effort
        };
        assert_eq!(parse(json!({"blocks":[]})), None);
        assert_eq!(
            parse(json!({"blocks":[],"reasoningEffort":null})),
            Some(None)
        );
        assert_eq!(
            parse(json!({"blocks":[],"reasoningEffort":"xhigh"})),
            Some(Some("xhigh".into()))
        );
        assert!(serde_json::from_value::<CliPromptParams>(
            json!({"blocks":[],"reasoningEffort":3})
        )
        .is_err());
        let ack = |effort| {
            serde_json::to_value(CliPromptResult {
                connection_id: "c".into(),
                session_id: None,
                conversation_id: None,
                reasoning_effort: effort,
            })
            .unwrap()
        };
        assert!(ack(None).get("reasoningEffort").is_none());
        assert!(ack(Some(None))["reasoningEffort"].is_null());
        assert_eq!(ack(Some(Some("high".into())))["reasoningEffort"], "high");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_effort_route_admits_and_acknowledges_the_requested_argv() {
        let _ = crate::acp::cli::test_transcript_root();
        use serde_json::json;
        use std::os::unix::fs::PermissionsExt;
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let dir = tempfile::tempdir().unwrap();
        let folder_id =
            crate::db::test_helpers::seed_folder(&db, &dir.path().display().to_string()).await;
        let state = Arc::new(AppState::new_for_test(db, dir.path().to_path_buf()));
        let executable = dir.path().join("fake-claude");
        std::fs::write(
            &executable,
            r#"#!/bin/sh
case "$1" in
 --version) echo '2.1.276 (Claude Code)'; exit 0;;
 --help) echo 'claude --effort <level>'; exit 0;;
esac
printf '%s\n' "$@" > "$FAKE_DIR/args.txt"
cat >/dev/null
printf '%s\n' '{"type":"result","subtype":"success","is_error":false}'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let params = |effort: serde_json::Value| {
            serde_json::from_value::<CliPromptParams>(json!({
            "agentType":"claude_code", "workingDir":dir.path(), "folderId":folder_id,
            "model":"claude-opus-5", "blocks":[{"type":"text","text":"test"}],
            "runtimeEnv":{"CLAUDE_CODE_EXECUTABLE":executable,"CLAUDE_CONFIG_DIR":dir.path().join("claude"),"FAKE_DIR":dir.path()},
            "reasoningEffort":effort
        })).unwrap()
        };
        let Json(ack) =
            cli_prompt_with_reasoning(Extension(state.clone()), Json(params(json!("xhigh"))))
                .await
                .unwrap();
        assert_eq!(ack.reasoning_effort, Some(Some("xhigh".into())));
        let args_path = dir.path().join("args.txt");
        for _ in 0..500 {
            if args_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let args = std::fs::read_to_string(args_path).unwrap();
        assert!(args.contains("--effort\nxhigh\n"));
        assert!(args.contains("--model\nclaude-opus-5\n"));
        state
            .connection_manager
            .disconnect(&ack.connection_id)
            .await
            .unwrap();
        let mut other_runtime = params(json!("high"));
        other_runtime.agent_type = Some(AgentType::Pi);
        assert!(
            cli_prompt_with_reasoning(Extension(state.clone()), Json(other_runtime))
                .await
                .is_err()
        );
        let mut missing = params(json!("high"));
        missing.reasoning_effort = None;
        assert!(
            cli_prompt_with_reasoning(Extension(state.clone()), Json(missing))
                .await
                .is_err()
        );

        // A settings env override is rejected before prompt admission and startup.
        std::fs::create_dir_all(dir.path().join("claude")).unwrap();
        std::fs::write(
            dir.path().join("claude/settings.json"),
            r#"{"env":{"CLAUDE_CODE_EFFORT_LEVEL":"low"}}"#,
        )
        .unwrap();
        assert!(
            cli_prompt_with_reasoning(Extension(state), Json(params(json!("max"))))
                .await
                .is_err()
        );
    }

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
        assert!(CLI_TRANSPORT_AGENTS.contains(&AgentType::Pi));
    }

    #[test]
    fn a_pi_session_id_is_a_uuid() {
        // pi 铸的是 uuidv7(`01a1271a-…`),走默认那条 uuid 校验。
        assert!(
            validate_cli_session_id(AgentType::Pi, "01a1271a-81fb-7fbd-9bb4-5299598a4dbc").is_ok()
        );
        assert!(validate_cli_session_id(AgentType::Pi, "session-x").is_err());
    }
}
