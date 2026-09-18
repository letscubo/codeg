//! fork(letscubo)专属: CLI transport (Claude Code, DeepSeek Harness).
//!
//! A `ConnectionTransport::Cli` connection is an ordinary entry in the
//! [`ConnectionManager`] map — same `connection_id`, `SessionState`, prompt
//! lock, `turn_in_flight` gate and event pipeline as an ACP connection. The one
//! difference is who consumes its `cmd_tx`: instead of a long-lived ACP adapter
//! loop, a per-turn driver runs one CLI process per prompt — [`driver::CliDriver`]
//! for `claude -p`, [`dsh_driver::DshDriver`] for `dsh --profile headless`. That
//! keeps `send_prompt_linked_with_message_id`, `cancel`, the lifecycle worker,
//! chat-channel webhooks, WS attach and snapshots working unchanged.

pub mod binary;
pub mod driver;
pub mod dsh_binary;
pub mod dsh_driver;
pub mod dsh_profile;
pub mod dsh_prompt;
pub mod dsh_stream;
pub mod dsh_tool_info;
pub mod stream_json;
pub mod tool_info;

#[cfg(all(test, unix))]
mod dsh_tests;
#[cfg(all(test, unix))]
mod tests;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, RwLock};

use crate::acp::connection::AgentConnection;
use crate::acp::manager::ConnectionManager;
use crate::acp::session_state::{ConnectionTransport, SessionState};
use crate::acp::types::{AcpEvent, ConnectionStatus};
use crate::models::agent::AgentType;
use crate::web::event_bridge::{emit_with_state, EventEmitter};

/// Owner label shared with `acp_connect`'s web connections.
const CLI_OWNER_WINDOW: &str = "web";
/// Agent env keys MyClaw pushes for DeepSeek: the provider route and model the
/// harness patch pins as `agent-default-model`.
const DSH_PROVIDER_ENV: &str = "DEEPSEEK_ACP_PROVIDER";
const DSH_MODEL_ENV: &str = "DEEPSEEK_ACP_MODEL";

/// The official DeepSeek launcher only continues ids it minted, spelled
/// `session-<uuid>`. A bare uuid is a `deepseek-acp` (ACP bridge) session id,
/// which the launcher cannot load.
pub fn is_dsh_session_id(session_id: &str) -> bool {
    session_id
        .strip_prefix("session-")
        .is_some_and(|rest| uuid::Uuid::parse_str(rest).is_ok())
}
const COMMAND_CHANNEL_CAPACITY: usize = 16;

pub struct CliConnectRequest {
    pub agent_type: AgentType,
    pub working_dir: PathBuf,
    /// Session to continue (claude: bare uuid; deepseek: `session-<uuid>`);
    /// `None` starts a new session.
    pub session_id: Option<String>,
    pub runtime_env: BTreeMap<String, String>,
    pub executable: PathBuf,
    /// deepseek: provider route + model written into the harness patch as the
    /// default model. Ignored by the claude driver (`cli_model` covers it).
    pub provider: Option<String>,
    pub model: Option<String>,
    /// Owner label for the connection. `None` = the web label `cli_prompt`
    /// uses; codeg's own engines (chat channels, work tasks, automations)
    /// pass theirs so reply routing and `disconnect_by_owner_window` keep
    /// working when `spawn_agent` hands them a CLI connection.
    pub owner_window_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliConnection {
    pub connection_id: String,
    /// `None` for a fresh deepseek connection: the harness mints the id on the
    /// first turn and reports it through `SessionStarted`.
    pub session_id: Option<String>,
    pub reused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliConnectError {
    /// Another live connection — an ACP one, or a CLI one in a different
    /// working dir — already drives this session. Two processes appending to
    /// the same transcript would corrupt it.
    SessionLocked { connection_id: String },
    /// deepseek: the harness home could not be prepared (plugin file or patch).
    ProfileWriteFailed(String),
}

impl ConnectionManager {
    /// Process model behind a live connection; `None` when it doesn't exist.
    pub async fn connection_transport(&self, conn_id: &str) -> Option<ConnectionTransport> {
        let state = self.get_state(conn_id).await?;
        let transport = state.read().await.transport;
        Some(transport)
    }

    /// Reuse the live CLI connection already bound to `(agent_type,
    /// working_dir, session_id)`, or register a new one and start its driver.
    /// No process is spawned here — that happens per prompt.
    pub async fn create_or_reuse_cli_connection(
        &self,
        mut request: CliConnectRequest,
        emitter: EventEmitter,
    ) -> Result<CliConnection, CliConnectError> {
        // deepseek: a conversation started on the retired `deepseek-acp` bridge
        // carries that bridge's bare-uuid session id, which the launcher cannot
        // load. Start a fresh launcher session that CONTINUES it (transcript
        // `continues_from`), so the earlier turns stay in the same history
        // instead of being replaced by the new session's.
        let continues_from = match (request.agent_type, request.session_id.take()) {
            (AgentType::DeepSeek, Some(id)) if !is_dsh_session_id(&id) => {
                tracing::info!(
                    "[CLI][dsh] {id} is a bridge session id; starting a launcher session that continues it"
                );
                Some(id)
            }
            (_, session_id) => {
                request.session_id = session_id;
                None
            }
        };
        let mut connections = self.connections.lock().await;
        if let Some(session_id) = request.session_id.as_deref() {
            for (id, conn) in connections.iter() {
                let state = conn.state.read().await;
                if state.external_id.as_deref() != Some(session_id)
                    || matches!(
                        state.status,
                        ConnectionStatus::Disconnected | ConnectionStatus::Error
                    )
                {
                    continue;
                }
                if state.transport == ConnectionTransport::Cli
                    && conn.agent_type == request.agent_type
                    && state.working_dir.as_deref() == Some(request.working_dir.as_path())
                {
                    return Ok(CliConnection {
                        connection_id: id.clone(),
                        session_id: Some(session_id.to_string()),
                        reused: true,
                    });
                }
                return Err(CliConnectError::SessionLocked {
                    connection_id: id.clone(),
                });
            }
        }

        let connection_id = uuid::Uuid::new_v4().to_string();
        let owner_window_label = request
            .owner_window_label
            .clone()
            .filter(|label| !label.trim().is_empty())
            .unwrap_or_else(|| CLI_OWNER_WINDOW.to_string());
        // deepseek: callers that go through `spawn_agent` carry no explicit
        // route/model; MyClaw pushes them as the agent env
        // (`DEEPSEEK_ACP_PROVIDER` / `DEEPSEEK_ACP_MODEL`), which
        // `build_session_runtime_env` merges into `runtime_env`.
        let (provider, model) = if request.agent_type == AgentType::DeepSeek {
            (
                request
                    .provider
                    .clone()
                    .or_else(|| request.runtime_env.get(DSH_PROVIDER_ENV).cloned()),
                request
                    .model
                    .clone()
                    .or_else(|| request.runtime_env.get(DSH_MODEL_ENV).cloned()),
            )
        } else {
            (request.provider.clone(), request.model.clone())
        };
        // claude accepts any uuid as `--session-id`, so a fresh session can be
        // named up front and the conversation row binds to it on the first
        // prompt. dsh only continues ids it already stored: its first turn
        // mints the id, and the driver announces it when the `session` line
        // arrives.
        let session_id = match (request.agent_type, request.session_id) {
            (_, Some(id)) => Some(id),
            (AgentType::DeepSeek, None) => None,
            (_, None) => Some(uuid::Uuid::new_v4().to_string()),
        };

        // deepseek: the harness home (plugin file, companion token) must be
        // ready before the first turn; failing here leaves nothing registered.
        // Task-engine launches (owner "work_task") carry the task_progress /
        // task_complete tool group, exactly as the ACP injection decides it.
        let tasks_enabled = owner_window_label == "work_task";
        let dsh_profile = if request.agent_type == AgentType::DeepSeek {
            let delegation = self.delegation_snapshot();
            match dsh_profile::prepare_launch_profile(
                &connection_id,
                &request.working_dir,
                &request.runtime_env,
                delegation.as_ref(),
                tasks_enabled,
            )
            .await
            {
                Ok(profile) => Some((profile, delegation)),
                Err(message) => return Err(CliConnectError::ProfileWriteFailed(message)),
            }
        } else {
            None
        };
        // claude: the same `codeg-mcp` companion, handed over per turn with
        // `--mcp-config` (the ACP path injects it into `mcpServers`).
        let (claude_companion, claude_delegation) = if request.agent_type == AgentType::ClaudeCode {
            let delegation = self.delegation_snapshot();
            let companion = match delegation.as_ref() {
                Some(injection) => {
                    crate::acp::connection::companion_launch_spec(
                        injection,
                        &connection_id,
                        &request.working_dir,
                        tasks_enabled,
                        crate::acp::host_tools_policy::HostToolsPolicy::from_env(
                            &request.runtime_env,
                        ),
                        crate::acp::connection::locate_codeg_mcp_binary,
                    )
                    .await
                }
                None => None,
            };
            (companion, delegation)
        } else {
            (None, None)
        };

        let mut session = SessionState::new(
            connection_id.clone(),
            request.agent_type,
            Some(request.working_dir.clone()),
            owner_window_label.clone(),
            None,
        );
        session.transport = ConnectionTransport::Cli;
        session.cli_model = model.filter(|m| !m.trim().is_empty());
        let state = Arc::new(RwLock::new(session));
        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let child_pid = Arc::new(AtomicU32::new(0));
        let fingerprint =
            crate::commands::acp::fingerprint_config(request.agent_type, &request.runtime_env);
        connections.insert(
            connection_id.clone(),
            AgentConnection {
                id: connection_id.clone(),
                agent_type: request.agent_type,
                status: ConnectionStatus::Connected,
                owner_window_label,
                cmd_tx,
                state: Arc::clone(&state),
                emitter: emitter.clone(),
                prompt_lock: Arc::new(Mutex::new(())),
                config_fingerprint: fingerprint.clone(),
                last_observed_fingerprint: fingerprint,
                child_pid: Arc::clone(&child_pid),
            },
        );
        drop(connections);

        match dsh_profile {
            Some((profile, delegation)) => {
                let driver = dsh_driver::DshDriver {
                    connection_id: connection_id.clone(),
                    agent_type: request.agent_type,
                    executable: request.executable,
                    working_dir: request.working_dir,
                    runtime_env: request.runtime_env,
                    state: Arc::clone(&state),
                    emitter: emitter.clone(),
                    child_pid,
                    connections: Arc::clone(&self.connections),
                    profile,
                    provider: provider.filter(|p| !p.trim().is_empty()),
                    delegation,
                    continues_from,
                };
                tokio::spawn(driver.run(cmd_rx));
            }
            None => {
                let driver = driver::CliDriver {
                    connection_id: connection_id.clone(),
                    agent_type: request.agent_type,
                    executable: request.executable,
                    working_dir: request.working_dir,
                    runtime_env: request.runtime_env,
                    state: Arc::clone(&state),
                    emitter: emitter.clone(),
                    child_pid,
                    connections: Arc::clone(&self.connections),
                    companion: claude_companion,
                    delegation: claude_delegation,
                };
                tokio::spawn(driver.run(cmd_rx));
            }
        }

        // Pre-assigning the session id as `external_id` lets the conversation
        // row bind to it when the first prompt links.
        if let Some(session_id) = &session_id {
            emit_with_state(
                &state,
                &emitter,
                AcpEvent::SessionStarted {
                    session_id: session_id.clone(),
                },
            )
            .await;
        }
        emit_with_state(
            &state,
            &emitter,
            AcpEvent::StatusChanged {
                status: ConnectionStatus::Connected,
            },
        )
        .await;
        tracing::info!(
            connection_id = %connection_id,
            session_id = session_id.as_deref().unwrap_or("(pending)"),
            agent_type = %request.agent_type,
            "[CLI] registered CLI connection"
        );
        Ok(CliConnection {
            connection_id,
            session_id,
            reused: false,
        })
    }
}

impl ConnectionManager {
    /// `spawn_agent` for the CLI-only agents (DeepSeek, Claude Code): there is
    /// no ACP process for them any more, so every caller — `acp_connect`, chat
    /// channels, work tasks, automations, delegation — gets a CLI connection
    /// (`dsh --profile headless` / `claude -p`). The returned id works with the
    /// same `send_prompt_linked*` / `cancel` / `disconnect` calls.
    ///
    /// A resume id in the old bridge spelling (bare uuid) becomes a fresh
    /// launcher session that continues it (see `create_or_reuse_cli_connection`).
    pub(crate) async fn spawn_cli_agent(
        &self,
        agent_type: AgentType,
        working_dir: Option<String>,
        session_id: Option<String>,
        runtime_env: BTreeMap<String, String>,
        owner_window_label: String,
        emitter: EventEmitter,
    ) -> Result<String, crate::acp::error::AcpError> {
        use crate::acp::error::AcpError;
        let working_dir = working_dir
            .filter(|d| !d.trim().is_empty())
            .map(PathBuf::from)
            .or_else(dirs::home_dir)
            .ok_or_else(|| AcpError::protocol("a CLI connection needs a working dir"))?;
        // Claude continues any uuid it wrote; anything else starts fresh.
        let session_id = match (agent_type, session_id) {
            (AgentType::ClaudeCode, Some(id)) if uuid::Uuid::parse_str(&id).is_err() => {
                tracing::warn!("[CLI] {id} is not a Claude session id; starting a fresh session");
                None
            }
            (_, session_id) => session_id,
        };
        let executable = match agent_type {
            AgentType::DeepSeek => dsh_binary::resolve_dsh_executable(&runtime_env).await,
            _ => binary::resolve_claude_executable(&runtime_env).await,
        }
        .map_err(|e| AcpError::protocol(format!("cli_not_installed: {e}")))?;
        let conn = self
            .create_or_reuse_cli_connection(
                CliConnectRequest {
                    agent_type,
                    working_dir,
                    session_id,
                    runtime_env,
                    executable,
                    provider: None,
                    model: None,
                    owner_window_label: Some(owner_window_label),
                },
                emitter,
            )
            .await
            .map_err(|e| match e {
                CliConnectError::SessionLocked { connection_id } => AcpError::protocol(format!(
                    "session_locked: this session is held by connection {connection_id}"
                )),
                CliConnectError::ProfileWriteFailed(message) => {
                    AcpError::protocol(format!("dsh_profile_failed: {message}"))
                }
            })?;
        Ok(conn.connection_id)
    }
}
