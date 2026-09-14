//! fork(letscubo)专属: Claude Code CLI transport.
//!
//! A `ConnectionTransport::Cli` connection is an ordinary entry in the
//! [`ConnectionManager`] map — same `connection_id`, `SessionState`, prompt
//! lock, `turn_in_flight` gate and event pipeline as an ACP connection. The one
//! difference is who consumes its `cmd_tx`: instead of a long-lived ACP adapter
//! loop, a [`driver::CliDriver`] runs one `claude -p` process per prompt. That
//! keeps `send_prompt_linked_with_message_id`, `cancel`, the lifecycle worker,
//! chat-channel webhooks, WS attach and snapshots working unchanged.

pub mod binary;
pub mod driver;
pub mod stream_json;
pub mod tool_info;

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
const COMMAND_CHANNEL_CAPACITY: usize = 16;

pub struct CliConnectRequest {
    pub agent_type: AgentType,
    pub working_dir: PathBuf,
    /// Claude session uuid to continue; `None` starts a new session.
    pub session_id: Option<String>,
    pub runtime_env: BTreeMap<String, String>,
    pub executable: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliConnection {
    pub connection_id: String,
    pub session_id: String,
    pub reused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliConnectError {
    /// Another live connection — an ACP one, or a CLI one in a different
    /// working dir — already drives this claude session. Two processes
    /// appending to the same transcript would corrupt it.
    SessionLocked { connection_id: String },
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
        request: CliConnectRequest,
        emitter: EventEmitter,
    ) -> Result<CliConnection, CliConnectError> {
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
                        session_id: session_id.to_string(),
                        reused: true,
                    });
                }
                return Err(CliConnectError::SessionLocked {
                    connection_id: id.clone(),
                });
            }
        }

        let connection_id = uuid::Uuid::new_v4().to_string();
        let session_id = request
            .session_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let mut session = SessionState::new(
            connection_id.clone(),
            request.agent_type,
            Some(request.working_dir.clone()),
            CLI_OWNER_WINDOW.to_string(),
            None,
        );
        session.transport = ConnectionTransport::Cli;
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
                owner_window_label: CLI_OWNER_WINDOW.to_string(),
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
        };
        tokio::spawn(driver.run(cmd_rx));

        // Pre-assigning the claude session id as `external_id` lets the
        // conversation row bind to it when the first prompt links.
        emit_with_state(
            &state,
            &emitter,
            AcpEvent::SessionStarted {
                session_id: session_id.clone(),
            },
        )
        .await;
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
            session_id = %session_id,
            "[CLI] registered Claude Code CLI connection"
        );
        Ok(CliConnection {
            connection_id,
            session_id,
            reused: false,
        })
    }
}
