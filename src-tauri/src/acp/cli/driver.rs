//! fork(letscubo)专属: the per-turn Claude Code CLI driver behind a
//! `ConnectionTransport::Cli` connection.
//!
//! It consumes the connection's `cmd_tx` where an ACP adapter loop would, so the
//! manager's admission path and `manager.cancel` work unchanged. Each `Prompt`
//! spawns one `claude -p` process that reads a single stream-json user message
//! from stdin, then EOF, and exits after its `result` line.
//!
//! Invariant: every turn ends with exactly one `TurnComplete`. It is the only
//! event that clears `turn_in_flight`; a missed one would wedge the connection
//! at `turn_in_progress` for good.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex, RwLock};

use super::stream_json::{
    CliTurnError, LineOutcome, StreamMapper, TurnFinish, STOP_CANCELLED, STOP_UNKNOWN,
};
use crate::acp::connection::{
    map_prompt_blocks, record_prompt, record_transcript_header_continuing,
    record_transcript_update, record_turn_end, record_turn_error_raw, AgentConnection,
    CompanionLaunchSpec, ConnectionCommand, DelegationInjection,
};
use crate::acp::error::AcpError;
use crate::acp::session_state::SessionState;
use crate::acp::types::{AcpEvent, ConnectionStatus, PromptInputBlock, UserMessageBlock};
use crate::models::agent::AgentType;
use crate::web::event_bridge::{emit_with_state, EventEmitter};

pub(super) const STDERR_TAIL_BYTES: usize = 64 * 1024;
pub(super) const MAX_CONSECUTIVE_UNPARSABLE: usize = 50;
pub(super) const PROTOCOL_SAMPLE_BYTES: usize = 2048;
/// How long a CLI that already printed its `result` gets to exit on its own.
pub(super) const EXIT_WAIT: Duration = Duration::from_secs(5);
/// SIGTERM → SIGKILL escalation window. claude exits within ~0.5s of SIGTERM
/// and takes its tool processes with it.
pub(super) const TERMINATE_GRACE: Duration = Duration::from_secs(3);
pub(super) const STDERR_DRAIN_WAIT: Duration = Duration::from_secs(1);
const CLAUDE_CONFIG_DIR_ENV: &str = "CLAUDE_CONFIG_DIR";

const BASE_ARGS: &[&str] = &[
    "-p",
    "--verbose",
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    "--include-partial-messages",
    "--permission-mode",
    "bypassPermissions",
];

pub(crate) struct CliDriver {
    pub connection_id: String,
    pub agent_type: AgentType,
    pub executable: PathBuf,
    pub working_dir: PathBuf,
    pub runtime_env: BTreeMap<String, String>,
    pub state: Arc<RwLock<SessionState>>,
    pub emitter: EventEmitter,
    pub child_pid: Arc<AtomicU32>,
    pub connections: Arc<Mutex<HashMap<String, AgentConnection>>>,
    /// The `codeg-mcp` companion (delegation / feedback / task tools), passed
    /// per turn through `--mcp-config` — the CLI counterpart of the ACP
    /// `mcpServers` injection. `None` when no companion feature is enabled.
    pub companion: Option<CompanionLaunchSpec>,
    pub delegation: Option<DelegationInjection>,
}

enum TurnEnd {
    Finished(TurnFinish),
    Eof,
    Cancelled,
    Disconnected,
    Protocol(String),
}

impl CliDriver {
    pub(crate) async fn run(self, mut commands: mpsc::Receiver<ConnectionCommand>) {
        while let Some(command) = commands.recv().await {
            match command {
                ConnectionCommand::Prompt {
                    blocks,
                    user_message,
                } => {
                    let disconnect = self.run_turn(blocks, user_message, &mut commands).await;
                    if disconnect {
                        break;
                    }
                }
                ConnectionCommand::Disconnect => break,
                // Nothing runs between turns, so there is nothing to stop.
                ConnectionCommand::Cancel => {}
                other => reject_unsupported(other),
            }
        }
        self.emit(AcpEvent::StatusChanged {
            status: ConnectionStatus::Disconnected,
        })
        .await;
        if let (Some(inj), Some(companion)) = (&self.delegation, &self.companion) {
            inj.tokens.revoke(&companion.token).await;
        }
        self.connections.lock().await.remove(&self.connection_id);
        tracing::info!(connection_id = %self.connection_id, "[CLI] driver stopped");
    }

    /// Runs one turn; returns `true` when the connection must shut down.
    async fn run_turn(
        &self,
        blocks: Vec<PromptInputBlock>,
        user_message: Option<(String, Vec<UserMessageBlock>)>,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> bool {
        // Same order as the ACP loop: status first, then the user echo, so the
        // echo's seq precedes every assistant event of the turn.
        self.emit(AcpEvent::StatusChanged {
            status: ConnectionStatus::Prompting,
        })
        .await;
        if let Some((message_id, blocks)) = user_message {
            self.emit(AcpEvent::UserMessage { message_id, blocks })
                .await;
        }

        let started = std::time::Instant::now();
        let (session_id, model) = {
            let s = self.state.read().await;
            (s.external_id.clone(), s.cli_model.clone())
        };
        let (session_id, minted) = match session_id {
            Some(id) => (id, false),
            None => (uuid::Uuid::new_v4().to_string(), true),
        };
        // codeg's own transcript is what the history view reads for Claude
        // Code (`transcript_dir_for`), so a CLI turn records the same header /
        // prompt / updates / turn end an ACP turn does. Header and prompt land
        // before a minted id is announced, so the lifecycle bind never
        // precedes the recorded prompt. A session carried over from the ACP
        // adapter keeps its file: the header write is a no-op once it exists.
        record_transcript_header_continuing(
            self.agent_type,
            &session_id,
            &self.working_dir.display().to_string(),
            None,
        )
        .await;
        record_prompt(
            self.agent_type,
            &session_id,
            &map_prompt_blocks(blocks.clone()),
        )
        .await;
        if minted {
            self.emit(AcpEvent::SessionStarted {
                session_id: session_id.clone(),
            })
            .await;
        }
        // A turn that crashed before claude wrote its transcript leaves nothing
        // to resume, so decide from the file rather than from turn history.
        let resume = transcript_exists(&self.claude_config_dir(), &session_id);
        let mut args = build_args(&session_id, resume, model.as_deref());
        if let Some(config) = self.companion.as_ref().map(mcp_config_json) {
            args.push("--mcp-config".to_string());
            args.push(config);
        }

        let (finish, disconnect) = match self.spawn(&args).await {
            Ok(child) => {
                self.drive(child, build_stdin_line(&blocks), &session_id, commands)
                    .await
            }
            Err(err) => (
                TurnFinish {
                    stop_reason: STOP_UNKNOWN.to_string(),
                    error: Some(CliTurnError {
                        code: "cli_spawn_failed",
                        message: format!("Failed to start Claude Code: {err}"),
                        details: Some(self.executable.display().to_string()),
                    }),
                },
                false,
            ),
        };
        self.child_pid.store(0, Ordering::SeqCst);
        let duration_ms = started.elapsed().as_millis() as u64;
        self.finish_turn(finish, &session_id, duration_ms, model)
            .await;
        if !disconnect {
            self.emit(AcpEvent::StatusChanged {
                status: ConnectionStatus::Connected,
            })
            .await;
        }
        disconnect
    }

    async fn spawn(&self, args: &[String]) -> std::io::Result<Child> {
        let mut command = crate::process::tokio_command(&self.executable);
        command
            .args(args)
            .current_dir(&self.working_dir)
            .envs(&self.runtime_env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Own process group, so cancel reaches the CLI and every tool process
        // it started in one signal.
        #[cfg(unix)]
        command.process_group(0);
        crate::process::spawn_retrying_exec_busy(|| command.spawn()).await
    }

    async fn drive(
        &self,
        mut child: Child,
        stdin_line: String,
        session_id: &str,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> (TurnFinish, bool) {
        let pid = child.id().unwrap_or(0);
        self.child_pid.store(pid, Ordering::SeqCst);
        tracing::info!(connection_id = %self.connection_id, pid, "[CLI] turn started");

        if let Some(mut stdin) = child.stdin.take() {
            tokio::spawn(async move {
                if let Err(e) = stdin.write_all(stdin_line.as_bytes()).await {
                    tracing::warn!("[CLI] failed to write the prompt to stdin: {e}");
                }
                // Dropping stdin sends EOF: the CLI exits after this turn.
            });
        }

        let stderr_tail = Arc::new(std::sync::Mutex::new(OutputTail::default()));
        let mut stderr_task = child.stderr.take().map(|stderr| {
            let tail = Arc::clone(&stderr_tail);
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Ok(mut tail) = tail.lock() {
                        tail.push(line);
                    }
                }
            })
        });

        let end = match child.stdout.take() {
            Some(stdout) => self.pump(stdout, session_id, commands).await,
            None => TurnEnd::Eof,
        };

        match end {
            TurnEnd::Finished(finish) => {
                let status = wait_or_terminate(&mut child, pid).await;
                tracing::info!(connection_id = %self.connection_id, %status, "[CLI] turn finished");
                (finish, false)
            }
            TurnEnd::Cancelled | TurnEnd::Disconnected => {
                terminate(&mut child, pid).await;
                let finish = TurnFinish {
                    stop_reason: STOP_CANCELLED.to_string(),
                    error: None,
                };
                (finish, matches!(end, TurnEnd::Disconnected))
            }
            TurnEnd::Protocol(sample) => {
                terminate(&mut child, pid).await;
                let finish = TurnFinish {
                    stop_reason: STOP_UNKNOWN.to_string(),
                    error: Some(CliTurnError {
                        code: "cli_protocol",
                        message: format!(
                            "Claude Code produced {MAX_CONSECUTIVE_UNPARSABLE} consecutive unparsable output lines"
                        ),
                        details: Some(sample),
                    }),
                };
                (finish, false)
            }
            TurnEnd::Eof => {
                let status = wait_or_terminate(&mut child, pid).await;
                if let Some(task) = stderr_task.take() {
                    let _ = tokio::time::timeout(STDERR_DRAIN_WAIT, task).await;
                }
                let tail = stderr_tail.lock().map(|t| t.text()).unwrap_or_default();
                let finish = TurnFinish {
                    stop_reason: STOP_UNKNOWN.to_string(),
                    error: Some(CliTurnError {
                        code: "cli_exited",
                        message: format!("Claude Code exited before finishing the turn ({status})"),
                        details: (!tail.is_empty()).then_some(tail),
                    }),
                };
                (finish, false)
            }
        }
    }

    /// Forward stdout lines as events until the turn's `result`, EOF, a bad
    /// stream, or a Cancel/Disconnect command.
    async fn pump(
        &self,
        stdout: tokio::process::ChildStdout,
        session_id: &str,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> TurnEnd {
        let mut lines = BufReader::new(stdout).lines();
        let mut mapper =
            StreamMapper::new(Some(session_id.to_string()), Some(self.working_dir.clone()));
        let mut unparsable = 0usize;
        // A `result` printed while a backgrounded task still runs is NOT the
        // end: claude keeps the process alive, feeds the task's notification
        // back and runs a follow-up turn (see `stream_json` module docs). Keep
        // reading; the codeg turn ends on the first `result` with nothing left
        // in the background, or on EOF after one.
        let mut deferred_finish: Option<TurnFinish> = None;
        loop {
            tokio::select! {
                line = lines.next_line() => match line {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match mapper.map_line(&line) {
                            LineOutcome::Events(events) => {
                                unparsable = 0;
                                for event in events {
                                    self.emit_recorded(event, session_id).await;
                                }
                            }
                            LineOutcome::Finished { events, finish } => {
                                unparsable = 0;
                                for event in events {
                                    self.emit_recorded(event, session_id).await;
                                }
                                if mapper.background_pending() {
                                    tracing::info!(
                                        connection_id = %self.connection_id,
                                        "[CLI] result with background work still running; waiting for its follow-up turn"
                                    );
                                    deferred_finish = Some(finish);
                                    continue;
                                }
                                return TurnEnd::Finished(finish);
                            }
                            LineOutcome::Unparsable => {
                                unparsable += 1;
                                tracing::warn!(
                                    connection_id = %self.connection_id,
                                    "[CLI] unparsable stdout line: {}",
                                    truncate(&line, 200)
                                );
                                if unparsable >= MAX_CONSECUTIVE_UNPARSABLE {
                                    return TurnEnd::Protocol(truncate(&line, PROTOCOL_SAMPLE_BYTES));
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        return match deferred_finish.take() {
                            Some(finish) => TurnEnd::Finished(finish),
                            None => TurnEnd::Eof,
                        };
                    }
                    Err(e) => {
                        tracing::warn!(connection_id = %self.connection_id, "[CLI] stdout read failed: {e}");
                        return match deferred_finish.take() {
                            Some(finish) => TurnEnd::Finished(finish),
                            None => TurnEnd::Eof,
                        };
                    }
                },
                command = commands.recv() => match command {
                    Some(ConnectionCommand::Cancel) => return TurnEnd::Cancelled,
                    Some(ConnectionCommand::Disconnect) | None => return TurnEnd::Disconnected,
                    Some(ConnectionCommand::Prompt { .. }) => tracing::warn!(
                        connection_id = %self.connection_id,
                        "[CLI] in-turn Prompt DROPPED — the turn_in_flight gate should have rejected this"
                    ),
                    Some(other) => reject_unsupported(other),
                },
            }
        }
    }

    async fn finish_turn(
        &self,
        finish: TurnFinish,
        session_id: &str,
        duration_ms: u64,
        model: Option<String>,
    ) {
        let agent_type = self.agent_type.to_string();
        if let Some(error) = finish.error {
            tracing::warn!(
                connection_id = %self.connection_id,
                code = error.code,
                "[CLI] turn failed: {}",
                error.message
            );
            record_turn_error_raw(
                self.agent_type,
                session_id,
                error.message.clone(),
                Some(error.code.to_string()),
                false,
            );
            self.emit(AcpEvent::Error {
                message: error.message,
                agent_type: agent_type.clone(),
                code: Some(error.code.to_string()),
                details: error.details,
                terminal: false,
            })
            .await;
        }
        record_turn_end(
            self.agent_type,
            session_id,
            &finish.stop_reason,
            duration_ms,
            model,
        )
        .await;
        let session_id = self
            .state
            .read()
            .await
            .external_id
            .clone()
            .unwrap_or_default();
        self.emit(AcpEvent::TurnComplete {
            session_id,
            stop_reason: finish.stop_reason,
            agent_type,
            duration_ms: Some(duration_ms),
        })
        .await;
    }

    /// Emit one mapped event and record it to codeg's transcript.
    ///
    /// A sub-agent's own prose (`parent_tool_use_id` set) is live-only: the
    /// transcript has no nesting for it, so recording it would re-read as the
    /// main agent talking — and MyClaw's live view drops it for that reason.
    async fn emit_recorded(&self, event: AcpEvent, session_id: &str) {
        let sidechain_prose = matches!(
            &event,
            AcpEvent::ContentDelta {
                parent_tool_use_id: Some(_),
                ..
            } | AcpEvent::Thinking {
                parent_tool_use_id: Some(_),
                ..
            }
        );
        if !sidechain_prose {
            if let Some(update) = super::dsh_driver::transcript_update_for(&event) {
                record_transcript_update(self.agent_type, session_id, &update);
            }
        }
        self.emit(event).await;
    }

    fn claude_config_dir(&self) -> PathBuf {
        self.runtime_env
            .get(CLAUDE_CONFIG_DIR_ENV)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(crate::parsers::claude::resolve_claude_config_dir)
    }

    async fn emit(&self, event: AcpEvent) {
        emit_with_state(&self.state, &self.emitter, event).await;
    }
}

/// `--mcp-config` payload carrying the `codeg-mcp` companion as a stdio server.
/// Added on top of the user's own MCP config (no `--strict-mcp-config`), so the
/// servers MyClaw projects into `CLAUDE_CONFIG_DIR/.claude.json` stay mounted.
pub(crate) fn mcp_config_json(companion: &CompanionLaunchSpec) -> String {
    json!({
        "mcpServers": {
            "codeg-mcp": {
                "type": "stdio",
                "command": companion.command.display().to_string(),
                "args": companion.args,
            }
        }
    })
    .to_string()
}

pub(super) fn reject_unsupported(command: ConnectionCommand) {
    match command {
        ConnectionCommand::GoalControl { reply, .. } => {
            if let Some(reply) = reply {
                let _ = reply.send(false);
            }
        }
        ConnectionCommand::Fork { reply, .. } => {
            let _ = reply.send(Err(unsupported()));
        }
        ConnectionCommand::Steer { reply, .. } => {
            let _ = reply.send(Err(unsupported()));
        }
        ConnectionCommand::StopAsyncTask { reply, .. } => {
            let _ = reply.send(Ok(false));
        }
        ConnectionCommand::SetMode { .. }
        | ConnectionCommand::SetConfigOption { .. }
        | ConnectionCommand::RespondPermission { .. } => {
            tracing::debug!("[CLI] ignoring an ACP-only command on a CLI connection");
        }
        ConnectionCommand::Prompt { .. }
        | ConnectionCommand::Cancel
        | ConnectionCommand::Disconnect => {}
    }
}

fn unsupported() -> AcpError {
    AcpError::Protocol("unsupported_for_cli: not available on the CLI transport".to_string())
}

pub(crate) fn build_args(session_id: &str, resume: bool, model: Option<&str>) -> Vec<String> {
    let mut args: Vec<String> = BASE_ARGS.iter().map(|s| s.to_string()).collect();
    args.push(if resume { "--resume" } else { "--session-id" }.to_string());
    args.push(session_id.to_string());
    if let Some(model) = model.filter(|m| !m.is_empty()) {
        args.push("--model".to_string());
        args.push(model.to_string());
    }
    args
}

/// One `SDKUserMessage` line. Verified minimal shape: no `session_id` or
/// `parent_tool_use_id` needed.
pub(crate) fn build_stdin_line(blocks: &[PromptInputBlock]) -> String {
    let content: Vec<Value> = blocks.iter().map(content_block).collect();
    let mut line = json!({
        "type": "user",
        "message": { "role": "user", "content": content },
    })
    .to_string();
    line.push('\n');
    line
}

fn content_block(block: &PromptInputBlock) -> Value {
    match block {
        PromptInputBlock::Text { text } => json!({ "type": "text", "text": text }),
        PromptInputBlock::Image {
            data, mime_type, ..
        } => image_block(data, mime_type),
        PromptInputBlock::Resource {
            uri,
            mime_type,
            text,
            blob,
        } => match (text, blob, mime_type) {
            (Some(text), _, _) => json!({ "type": "text", "text": format!("[{uri}]\n{text}") }),
            (None, Some(blob), Some(mime)) if mime.starts_with("image/") => image_block(blob, mime),
            _ => json!({ "type": "text", "text": format!("[{uri}]") }),
        },
        PromptInputBlock::ResourceLink { uri, name, .. } => {
            json!({ "type": "text", "text": format!("[{name}]({uri})") })
        }
    }
}

fn image_block(data: &str, mime_type: &str) -> Value {
    json!({
        "type": "image",
        "source": { "type": "base64", "media_type": mime_type, "data": data },
    })
}

pub(crate) fn transcript_exists(claude_config_dir: &Path, session_id: &str) -> bool {
    let file_name = format!("{session_id}.jsonl");
    let Ok(projects) = std::fs::read_dir(claude_config_dir.join("projects")) else {
        return false;
    };
    projects
        .flatten()
        .any(|entry| entry.path().join(&file_name).is_file())
}

pub(super) async fn wait_or_terminate(child: &mut Child, pid: u32) -> String {
    match tokio::time::timeout(EXIT_WAIT, child.wait()).await {
        Ok(Ok(status)) => describe_status(status),
        Ok(Err(e)) => format!("wait failed: {e}"),
        Err(_) => {
            terminate(child, pid).await;
            "did not exit on its own; terminated".to_string()
        }
    }
}

pub(super) async fn terminate(child: &mut Child, pid: u32) {
    #[cfg(unix)]
    signal_group(pid, libc::SIGTERM);
    #[cfg(not(unix))]
    {
        let _ = kill_tree::tokio::kill_tree(pid).await;
        let _ = child.start_kill();
    }
    if tokio::time::timeout(TERMINATE_GRACE, child.wait())
        .await
        .is_err()
    {
        #[cfg(unix)]
        signal_group(pid, libc::SIGKILL);
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    // Tool processes that ignored SIGTERM are still in the group.
    #[cfg(unix)]
    signal_group(pid, libc::SIGKILL);
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: libc::c_int) {
    let Ok(pgid) = libc::pid_t::try_from(pid) else {
        return;
    };
    if pgid <= 0 {
        return;
    }
    // SAFETY: kill(2) on the process group this driver created with
    // `process_group(0)`; no memory is touched.
    unsafe {
        libc::kill(-pgid, signal);
    }
}

pub(super) fn describe_status(status: std::process::ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit code {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {signal}");
        }
    }
    "unknown exit status".to_string()
}

pub(super) fn truncate(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[derive(Default)]
pub(super) struct OutputTail {
    lines: VecDeque<String>,
    bytes: usize,
}

impl OutputTail {
    pub(super) fn push(&mut self, line: String) {
        self.bytes += line.len() + 1;
        self.lines.push_back(line);
        while self.bytes > STDERR_TAIL_BYTES {
            match self.lines.pop_front() {
                Some(old) => self.bytes -= old.len() + 1,
                None => {
                    self.bytes = 0;
                    break;
                }
            }
        }
    }

    pub(super) fn text(&self) -> String {
        self.lines
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("\n")
    }
}
