//! fork(letscubo)专属: the per-turn Codex driver behind a
//! `ConnectionTransport::Cli` connection for `AgentType::Codex`.
//!
//! Each `Prompt` spawns one `codex app-server` process and drives it over its
//! stdio JSON-RPC: `initialize` → `initialized` → `thread/start` (new session)
//! or `thread/resume` (continued session) → `turn/start` → stream notifications
//! until `turn/completed`, then close stdin so the server exits. The thread id
//! is the session id: a new session announces it through `SessionStarted` once
//! `thread/start` answers. Sessions persist under `CODEX_HOME`, so a later turn
//! in a fresh process resumes them (verified on A3 with codex 0.155.0).
//!
//! Why app-server and not `codex exec --json`: exec emits the answer as one
//! `item.completed` block; app-server streams `item/agentMessage/delta` and has
//! a real `turn/interrupt`.
//!
//! Like the dsh driver this one records codeg's own transcript, and every turn
//! ends with exactly one `TurnComplete`.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin};
use tokio::sync::{mpsc, Mutex, RwLock};

use super::codex_stream::CodexStreamMapper;
use super::driver::{
    reject_unsupported, terminate, truncate, wait_or_terminate, OutputTail,
    MAX_CONSECUTIVE_UNPARSABLE, PROTOCOL_SAMPLE_BYTES, STDERR_DRAIN_WAIT,
};
use super::dsh_driver::transcript_update_for;
use super::dsh_prompt::attachment_path;
use super::stream_json::{CliTurnError, LineOutcome, TurnFinish, STOP_CANCELLED, STOP_UNKNOWN};
use crate::acp::connection::{
    map_prompt_blocks, record_prompt, record_transcript_header_continuing,
    record_transcript_update, record_turn_end, record_turn_error_raw, AgentConnection,
    CompanionLaunchSpec, ConnectionCommand, DelegationInjection,
};
use crate::acp::session_state::SessionState;
use crate::acp::types::{AcpEvent, ConnectionStatus, PromptInputBlock, UserMessageBlock};
use crate::models::agent::AgentType;
use crate::web::event_bridge::{emit_with_state, EventEmitter};

/// `initialize` … `turn/start` must answer within this; a server that never
/// gets that far is broken (bad install, config it cannot parse).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);
/// After `turn/interrupt`, how long to wait for `turn/completed{interrupted}`
/// before killing the process (A3: it answers at once).
const INTERRUPT_GRACE: Duration = Duration::from_secs(5);
/// Upper bound for the prompt text; the JSON-RPC line has no hard limit, but a
/// runaway paste should fail with a code the caller can act on.
const MAX_PROMPT_BYTES: usize = 512 * 1024;

/// Our own requests, by JSON-RPC id.
const ID_INITIALIZE: u64 = 1;
const ID_THREAD: u64 = 2;
const ID_TURN: u64 = 3;
const ID_INTERRUPT: u64 = 4;

/// Codex runs inside the MyClaw instance container, which is the sandbox: no
/// approvals, no inner sandbox (bubblewrap is not installed there either).
const APPROVAL_POLICY: &str = "never";
const SANDBOX_MODE: &str = "danger-full-access";

pub(crate) struct CodexDriver {
    pub connection_id: String,
    pub agent_type: AgentType,
    pub executable: PathBuf,
    pub working_dir: PathBuf,
    pub runtime_env: BTreeMap<String, String>,
    pub state: Arc<RwLock<SessionState>>,
    pub emitter: EventEmitter,
    pub child_pid: Arc<AtomicU32>,
    pub connections: Arc<Mutex<HashMap<String, AgentConnection>>>,
    /// The `codeg-mcp` companion, handed over per turn as `-c mcp_servers.*`.
    pub companion: Option<CompanionLaunchSpec>,
    pub delegation: Option<DelegationInjection>,
}

enum TurnEnd {
    Finished(TurnFinish),
    Eof,
    Cancelled,
    Disconnected,
    Protocol(String),
    Failed(CliTurnError),
}

/// What one turn knows about its session id and prompt for recording.
struct TurnRecord {
    session_id: Option<String>,
    prompt_blocks: Vec<PromptInputBlock>,
    prompt_recorded: bool,
}

/// Where the JSON-RPC exchange stands.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Initializing,
    OpeningThread,
    StartingTurn,
    Streaming,
    Interrupting,
}

impl CodexDriver {
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
        tracing::info!(connection_id = %self.connection_id, "[CLI][codex] driver stopped");
    }

    /// Runs one turn; returns `true` when the connection must shut down.
    async fn run_turn(
        &self,
        blocks: Vec<PromptInputBlock>,
        user_message: Option<(String, Vec<UserMessageBlock>)>,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> bool {
        self.emit(AcpEvent::StatusChanged {
            status: ConnectionStatus::Prompting,
        })
        .await;
        if let Some((message_id, blocks)) = user_message {
            self.emit(AcpEvent::UserMessage { message_id, blocks })
                .await;
        }

        let (session_id, model) = {
            let s = self.state.read().await;
            (s.external_id.clone(), s.cli_model.clone())
        };
        let mut record = TurnRecord {
            session_id: session_id.clone(),
            prompt_blocks: blocks.clone(),
            prompt_recorded: false,
        };
        if let Some(sid) = &session_id {
            self.record_turn_start(sid, &record.prompt_blocks).await;
            record.prompt_recorded = true;
        }
        let started = Instant::now();

        let (finish, disconnect) = match build_input(&blocks, &self.working_dir) {
            Err(error) => (
                TurnFinish {
                    stop_reason: STOP_UNKNOWN.to_string(),
                    error: Some(error),
                },
                false,
            ),
            Ok(input) => match self.spawn().await {
                Ok(child) => {
                    self.drive(child, input, &mut record, model.clone(), commands)
                        .await
                }
                Err(err) => (
                    TurnFinish {
                        stop_reason: STOP_UNKNOWN.to_string(),
                        error: Some(CliTurnError {
                            code: "cli_spawn_failed",
                            message: format!("Failed to start Codex: {err}"),
                            details: Some(self.executable.display().to_string()),
                        }),
                    },
                    false,
                ),
            },
        };
        self.child_pid.store(0, Ordering::SeqCst);
        let duration_ms = started.elapsed().as_millis() as u64;
        self.finish_turn(finish, &record, duration_ms, model).await;
        if !disconnect {
            self.emit(AcpEvent::StatusChanged {
                status: ConnectionStatus::Connected,
            })
            .await;
        }
        disconnect
    }

    async fn spawn(&self) -> std::io::Result<Child> {
        let mut command = crate::process::tokio_command(&self.executable);
        command
            .args(app_server_args(self.companion.as_ref()))
            .current_dir(&self.working_dir)
            .envs(&self.runtime_env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        crate::process::spawn_retrying_exec_busy(|| command.spawn()).await
    }

    async fn drive(
        &self,
        mut child: Child,
        input: Vec<Value>,
        record: &mut TurnRecord,
        model: Option<String>,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> (TurnFinish, bool) {
        let pid = child.id().unwrap_or(0);
        self.child_pid.store(pid, Ordering::SeqCst);
        tracing::info!(connection_id = %self.connection_id, pid, "[CLI][codex] turn started");

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

        let mut mapper = CodexStreamMapper::new(Some(self.working_dir.clone()), model.clone());
        let end = match (child.stdin.take(), child.stdout.take()) {
            (Some(stdin), Some(stdout)) => {
                self.pump(stdin, stdout, input, model, &mut mapper, record, commands)
                    .await
            }
            _ => TurnEnd::Eof,
        };

        match end {
            TurnEnd::Finished(finish) => {
                // stdin is closed by now; the server exits on EOF.
                let status = wait_or_terminate(&mut child, pid).await;
                tracing::info!(connection_id = %self.connection_id, %status, "[CLI][codex] turn finished");
                (finish, false)
            }
            TurnEnd::Cancelled | TurnEnd::Disconnected => {
                terminate(&mut child, pid).await;
                (
                    TurnFinish {
                        stop_reason: STOP_CANCELLED.to_string(),
                        error: None,
                    },
                    matches!(end, TurnEnd::Disconnected),
                )
            }
            TurnEnd::Failed(error) => {
                terminate(&mut child, pid).await;
                if let Some(task) = stderr_task.take() {
                    let _ = tokio::time::timeout(STDERR_DRAIN_WAIT, task).await;
                }
                let tail = stderr_tail.lock().map(|t| t.text()).unwrap_or_default();
                let details = error.details.or_else(|| (!tail.is_empty()).then_some(tail));
                (
                    TurnFinish {
                        stop_reason: STOP_UNKNOWN.to_string(),
                        error: Some(CliTurnError { details, ..error }),
                    },
                    false,
                )
            }
            TurnEnd::Protocol(sample) => {
                terminate(&mut child, pid).await;
                (
                    TurnFinish {
                        stop_reason: STOP_UNKNOWN.to_string(),
                        error: Some(CliTurnError {
                            code: "cli_protocol",
                            message: format!(
                                "Codex produced {MAX_CONSECUTIVE_UNPARSABLE} consecutive unparsable output lines"
                            ),
                            details: Some(sample),
                        }),
                    },
                    false,
                )
            }
            TurnEnd::Eof => {
                let status = wait_or_terminate(&mut child, pid).await;
                if let Some(task) = stderr_task.take() {
                    let _ = tokio::time::timeout(STDERR_DRAIN_WAIT, task).await;
                }
                let tail = stderr_tail.lock().map(|t| t.text()).unwrap_or_default();
                (
                    TurnFinish {
                        stop_reason: STOP_UNKNOWN.to_string(),
                        error: Some(CliTurnError {
                            code: "cli_exited",
                            message: format!("Codex exited before finishing the turn ({status})"),
                            details: (!tail.is_empty()).then_some(tail),
                        }),
                    },
                    false,
                )
            }
        }
    }

    /// The JSON-RPC exchange for one turn.
    #[allow(clippy::too_many_arguments)]
    async fn pump(
        &self,
        mut stdin: ChildStdin,
        stdout: tokio::process::ChildStdout,
        input: Vec<Value>,
        model: Option<String>,
        mapper: &mut CodexStreamMapper,
        record: &mut TurnRecord,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> TurnEnd {
        let mut lines = BufReader::new(stdout).lines();
        let mut unparsable = 0usize;
        let mut phase = Phase::Initializing;
        let mut thread_id: Option<String> = None;
        let mut turn_id: Option<String> = None;
        // Handshake budget; once streaming, a silent model / long command is
        // legitimate and only a cancel ends the turn early.
        let mut deadline = Some(tokio::time::Instant::now() + HANDSHAKE_TIMEOUT);

        if let Err(e) = send(&mut stdin, &initialize_request()).await {
            return TurnEnd::Failed(write_failed(e));
        }

        loop {
            let timeout = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                line = lines.next_line() => {
                    let line = match line {
                        Ok(Some(line)) => line,
                        Ok(None) => return TurnEnd::Eof,
                        Err(e) => {
                            tracing::warn!(connection_id = %self.connection_id, "[CLI][codex] stdout read failed: {e}");
                            return TurnEnd::Eof;
                        }
                    };
                    if line.trim().is_empty() {
                        continue;
                    }
                    let Ok(msg) = serde_json::from_str::<Value>(&line) else {
                        unparsable += 1;
                        tracing::warn!(connection_id = %self.connection_id, "[CLI][codex] unparsable stdout line: {}", truncate(&line, 200));
                        if unparsable >= MAX_CONSECUTIVE_UNPARSABLE {
                            return TurnEnd::Protocol(truncate(&line, PROTOCOL_SAMPLE_BYTES));
                        }
                        continue;
                    };
                    unparsable = 0;

                    match classify(&msg) {
                        Message::ServerRequest(id) => {
                            // `approvalPolicy: never` means none should arrive;
                            // refuse anything that does rather than hang the turn.
                            tracing::warn!(connection_id = %self.connection_id, method = msg["method"].as_str().unwrap_or(""), "[CLI][codex] refusing server request");
                            if let Err(e) = send(&mut stdin, &refuse_request(id)).await {
                                return TurnEnd::Failed(write_failed(e));
                            }
                        }
                        Message::Response(id) => {
                            if let Some(error) = msg.get("error") {
                                if id == ID_INTERRUPT {
                                    // Already over; `turn/completed` or EOF follows.
                                    continue;
                                }
                                return TurnEnd::Failed(request_failed(id, error));
                            }
                            let result = &msg["result"];
                            match id {
                                ID_INITIALIZE if phase == Phase::Initializing => {
                                    phase = Phase::OpeningThread;
                                    let open = thread_request(record.session_id.as_deref(), &self.working_dir, model.as_deref());
                                    for m in [json!({ "method": "initialized" }), open] {
                                        if let Err(e) = send(&mut stdin, &m).await {
                                            return TurnEnd::Failed(write_failed(e));
                                        }
                                    }
                                }
                                ID_THREAD if phase == Phase::OpeningThread => {
                                    let Some(id) = result["thread"]["id"].as_str().filter(|s| !s.is_empty()) else {
                                        return TurnEnd::Failed(CliTurnError {
                                            code: "cli_protocol",
                                            message: "Codex opened a thread without an id".to_string(),
                                            details: Some(truncate(&result.to_string(), PROTOCOL_SAMPLE_BYTES)),
                                        });
                                    };
                                    thread_id = Some(id.to_string());
                                    if record.session_id.as_deref() != Some(id) {
                                        self.emit_recorded(AcpEvent::SessionStarted { session_id: id.to_string() }, record).await;
                                    }
                                    phase = Phase::StartingTurn;
                                    if let Err(e) = send(&mut stdin, &turn_request(id, &input, model.as_deref())).await {
                                        return TurnEnd::Failed(write_failed(e));
                                    }
                                }
                                ID_TURN if phase == Phase::StartingTurn => {
                                    if let Some(id) = result["turn"]["id"].as_str() {
                                        turn_id = Some(id.to_string());
                                        mapper.set_turn_id(id);
                                    }
                                    phase = Phase::Streaming;
                                    deadline = None;
                                }
                                _ => {}
                            }
                        }
                        Message::Notification => match mapper.map_notification(&msg) {
                            LineOutcome::Events(events) => {
                                for event in events {
                                    self.emit_recorded(event, record).await;
                                }
                            }
                            LineOutcome::Finished { events, finish } => {
                                for event in events {
                                    self.emit_recorded(event, record).await;
                                }
                                drop(stdin);
                                return TurnEnd::Finished(finish);
                            }
                            LineOutcome::Unparsable => {}
                        },
                        Message::Other => {}
                    }
                }
                _ = timeout => {
                    return match phase {
                        Phase::Interrupting => TurnEnd::Cancelled,
                        _ => TurnEnd::Failed(CliTurnError {
                            code: "cli_handshake_timeout",
                            message: format!("Codex did not start the turn within {}s", HANDSHAKE_TIMEOUT.as_secs()),
                            details: None,
                        }),
                    };
                }
                command = commands.recv(), if phase != Phase::Interrupting => match command {
                    Some(ConnectionCommand::Cancel) => {
                        // A running turn gets a real interrupt (the thread stays
                        // consistent for the next turn); before that, just stop.
                        match (thread_id.as_deref(), turn_id.as_deref()) {
                            (Some(thread), Some(turn)) => {
                                if send(&mut stdin, &interrupt_request(thread, turn)).await.is_err() {
                                    return TurnEnd::Cancelled;
                                }
                                phase = Phase::Interrupting;
                                deadline = Some(tokio::time::Instant::now() + INTERRUPT_GRACE);
                            }
                            _ => return TurnEnd::Cancelled,
                        }
                    }
                    Some(ConnectionCommand::Disconnect) | None => return TurnEnd::Disconnected,
                    Some(ConnectionCommand::Prompt { .. }) => tracing::warn!(
                        connection_id = %self.connection_id,
                        "[CLI][codex] in-turn Prompt DROPPED — the turn_in_flight gate should have rejected this"
                    ),
                    Some(other) => reject_unsupported(other),
                },
            }
        }
    }

    /// Emit one mapped event, recording it to codeg's transcript. A
    /// `SessionStarted` on the first turn lands header + prompt on disk before
    /// it is emitted, so the lifecycle bind never precedes the recorded prompt.
    async fn emit_recorded(&self, event: AcpEvent, record: &mut TurnRecord) {
        if let AcpEvent::SessionStarted { session_id } = &event {
            record.session_id = Some(session_id.clone());
            if !record.prompt_recorded {
                self.record_turn_start(session_id, &record.prompt_blocks)
                    .await;
                record.prompt_recorded = true;
            }
        } else if let (Some(sid), Some(update)) =
            (&record.session_id, transcript_update_for(&event))
        {
            record_transcript_update(self.agent_type, sid, &update);
        }
        self.emit(event).await;
    }

    async fn record_turn_start(&self, session_id: &str, blocks: &[PromptInputBlock]) {
        record_transcript_header_continuing(
            self.agent_type,
            session_id,
            &self.working_dir.display().to_string(),
            None,
        )
        .await;
        record_prompt(
            self.agent_type,
            session_id,
            &map_prompt_blocks(blocks.to_vec()),
        )
        .await;
    }

    async fn finish_turn(
        &self,
        finish: TurnFinish,
        record: &TurnRecord,
        duration_ms: u64,
        model: Option<String>,
    ) {
        let agent_type = self.agent_type.to_string();
        if let Some(error) = finish.error {
            tracing::warn!(
                connection_id = %self.connection_id,
                code = error.code,
                "[CLI][codex] turn failed: {}",
                error.message
            );
            if let Some(sid) = &record.session_id {
                record_turn_error_raw(
                    self.agent_type,
                    sid,
                    error.message.clone(),
                    Some(error.code.to_string()),
                    false,
                );
            }
            self.emit(AcpEvent::Error {
                message: error.message,
                agent_type: agent_type.clone(),
                code: Some(error.code.to_string()),
                details: error.details,
                terminal: false,
            })
            .await;
        }
        if let Some(sid) = &record.session_id {
            record_turn_end(
                self.agent_type,
                sid,
                &finish.stop_reason,
                duration_ms,
                model,
            )
            .await;
        }
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

    async fn emit(&self, event: AcpEvent) {
        emit_with_state(&self.state, &self.emitter, event).await;
    }
}

enum Message {
    /// A response to one of our requests.
    Response(u64),
    /// A request from the server (approval, user input, …).
    ServerRequest(Value),
    Notification,
    Other,
}

fn classify(msg: &Value) -> Message {
    match (msg.get("id"), msg.get("method")) {
        (Some(id), Some(_)) => Message::ServerRequest(id.clone()),
        (Some(id), None) => id.as_u64().map_or(Message::Other, Message::Response),
        (None, Some(_)) => Message::Notification,
        (None, None) => Message::Other,
    }
}

async fn send(stdin: &mut ChildStdin, msg: &Value) -> std::io::Result<()> {
    let mut line = msg.to_string();
    line.push('\n');
    stdin.write_all(line.as_bytes()).await?;
    stdin.flush().await
}

fn write_failed(e: std::io::Error) -> CliTurnError {
    CliTurnError {
        code: "cli_exited",
        message: format!("Codex stopped reading its input: {e}"),
        details: None,
    }
}

fn request_failed(id: u64, error: &Value) -> CliTurnError {
    let message = error["message"]
        .as_str()
        .filter(|m| !m.is_empty())
        .unwrap_or("Codex rejected the request")
        .to_string();
    let code = match id {
        ID_THREAD
            if message.to_ascii_lowercase().contains("not found")
                || message.to_ascii_lowercase().contains("no rollout") =>
        {
            "cli_resume_failed"
        }
        ID_INITIALIZE => "cli_protocol",
        _ => "cli_execution_error",
    };
    CliTurnError {
        code,
        message,
        details: Some(truncate(&error.to_string(), PROTOCOL_SAMPLE_BYTES)),
    }
}

/// `codex app-server` plus the codeg-mcp companion as a stdio MCP server.
pub(crate) fn app_server_args(companion: Option<&CompanionLaunchSpec>) -> Vec<String> {
    let mut args = vec!["app-server".to_string()];
    if let Some(companion) = companion {
        let command = json!(companion.command.display().to_string()).to_string();
        let list = json!(companion.args).to_string();
        args.push("-c".to_string());
        let server = crate::acp::delegation::companion::COMPANION_SERVER_NAME;
        args.push(format!("mcp_servers.{server}.command={command}"));
        args.push("-c".to_string());
        args.push(format!("mcp_servers.{server}.args={list}"));
    }
    args
}

fn initialize_request() -> Value {
    json!({
        "id": ID_INITIALIZE,
        "method": "initialize",
        "params": {
            "clientInfo": {
                "name": crate::acp::delegation::companion::CLIENT_NAME,
                "title": crate::acp::delegation::companion::CLIENT_TITLE,
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": null,
        },
    })
}

pub(crate) fn thread_request(
    session_id: Option<&str>,
    cwd: &std::path::Path,
    model: Option<&str>,
) -> Value {
    let mut params = json!({
        "cwd": cwd.display().to_string(),
        "approvalPolicy": APPROVAL_POLICY,
        "sandbox": SANDBOX_MODE,
    });
    if let Some(model) = model.filter(|m| !m.trim().is_empty()) {
        params["model"] = json!(model);
    }
    match session_id {
        Some(id) => {
            params["threadId"] = json!(id);
            json!({ "id": ID_THREAD, "method": "thread/resume", "params": params })
        }
        None => json!({ "id": ID_THREAD, "method": "thread/start", "params": params }),
    }
}

fn turn_request(thread_id: &str, input: &[Value], model: Option<&str>) -> Value {
    let mut params = json!({ "threadId": thread_id, "input": input });
    if let Some(model) = model.filter(|m| !m.trim().is_empty()) {
        params["model"] = json!(model);
    }
    json!({ "id": ID_TURN, "method": "turn/start", "params": params })
}

fn interrupt_request(thread_id: &str, turn_id: &str) -> Value {
    json!({
        "id": ID_INTERRUPT,
        "method": "turn/interrupt",
        "params": { "threadId": thread_id, "turnId": turn_id },
    })
}

fn refuse_request(id: Value) -> Value {
    json!({
        "id": id,
        "error": { "code": -32601, "message": "not supported by codeg: this session runs without approvals" },
    })
}

/// Prompt blocks → app-server `UserInput[]`: text as `text`, images written
/// to the workspace (or an existing upload) as `localImage`.
pub(crate) fn build_input(
    blocks: &[PromptInputBlock],
    working_dir: &std::path::Path,
) -> Result<Vec<Value>, CliTurnError> {
    let mut input = Vec::new();
    let mut text_bytes = 0usize;
    let mut push_text = |input: &mut Vec<Value>, text: String| {
        text_bytes += text.len();
        input.push(json!({ "type": "text", "text": text, "text_elements": [] }));
    };
    for block in blocks {
        match block {
            PromptInputBlock::Text { text } => {
                if !text.trim().is_empty() {
                    push_text(&mut input, text.clone());
                }
            }
            PromptInputBlock::Image {
                data,
                mime_type,
                uri,
            } => {
                let path = attachment_path(working_dir, data, mime_type, uri.as_deref())?;
                input.push(json!({ "type": "localImage", "path": path.display().to_string() }));
            }
            PromptInputBlock::Resource {
                uri,
                mime_type,
                text,
                blob,
            } => match (text, blob, mime_type) {
                (Some(text), _, _) => push_text(&mut input, format!("[{uri}]\n{text}")),
                (None, Some(blob), Some(mime)) if mime.starts_with("image/") => {
                    let path = attachment_path(working_dir, blob, mime, Some(uri))?;
                    input.push(json!({ "type": "localImage", "path": path.display().to_string() }));
                }
                _ => push_text(&mut input, format!("[{uri}]")),
            },
            PromptInputBlock::ResourceLink { uri, name, .. } => {
                push_text(&mut input, format!("[{name}]({uri})"));
            }
        }
    }
    if input.is_empty() {
        return Err(CliTurnError {
            code: "invalid_params",
            message: "the prompt has no content".to_string(),
            details: None,
        });
    }
    if text_bytes > MAX_PROMPT_BYTES {
        return Err(CliTurnError {
            code: "prompt_too_large",
            message: format!(
                "the prompt is {text_bytes} bytes; Codex takes at most {MAX_PROMPT_BYTES}"
            ),
            details: None,
        });
    }
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn new_session_starts_a_thread_and_a_known_one_resumes_it() {
        let start = thread_request(None, Path::new("/ws"), Some("gpt-5.4-mini"));
        assert_eq!(start["method"], "thread/start");
        assert_eq!(start["params"]["approvalPolicy"], "never");
        assert_eq!(start["params"]["sandbox"], "danger-full-access");
        assert_eq!(start["params"]["model"], "gpt-5.4-mini");
        assert!(start["params"].get("threadId").is_none());

        let resume = thread_request(
            Some("01a0b54e-568c-7310-aa4d-f777c3663fb7"),
            Path::new("/ws"),
            None,
        );
        assert_eq!(resume["method"], "thread/resume");
        assert_eq!(
            resume["params"]["threadId"],
            "01a0b54e-568c-7310-aa4d-f777c3663fb7"
        );
        assert!(resume["params"].get("model").is_none());
    }

    #[test]
    fn companion_is_passed_as_config_overrides() {
        let spec = CompanionLaunchSpec {
            command: PathBuf::from("/opt/codeg/codeg-mcp"),
            args: vec!["--parent".into(), "c1".into()],
            token: "t".into(),
            feedback_available: false,
            delegation_enabled: true,
        };
        let args = app_server_args(Some(&spec));
        assert_eq!(args[0], "app-server");
        assert!(
            args.contains(&r#"mcp_servers.myclaw.command="/opt/codeg/codeg-mcp""#.to_string())
        );
        assert!(args.contains(&r#"mcp_servers.myclaw.args=["--parent","c1"]"#.to_string()));
        assert_eq!(app_server_args(None), vec!["app-server".to_string()]);
    }

    #[test]
    fn prompt_blocks_become_user_input() {
        let tmp = tempfile::tempdir().unwrap();
        let input = build_input(
            &[
                PromptInputBlock::Text { text: "hi".into() },
                PromptInputBlock::ResourceLink {
                    uri: "https://x".into(),
                    name: "x".into(),
                    mime_type: None,
                    description: None,
                },
            ],
            tmp.path(),
        )
        .unwrap();
        assert_eq!(
            input[0],
            json!({"type":"text","text":"hi","text_elements":[]})
        );
        assert_eq!(input[1]["text"], "[x](https://x)");
        assert_eq!(
            build_input(&[], tmp.path()).unwrap_err().code,
            "invalid_params"
        );
    }

    #[test]
    fn server_requests_are_refused_and_responses_classified() {
        assert!(matches!(
            classify(&json!({"id": 9, "method": "item/commandExecution/requestApproval"})),
            Message::ServerRequest(_)
        ));
        assert!(matches!(
            classify(&json!({"id": 3, "result": {}})),
            Message::Response(3)
        ));
        assert!(matches!(
            classify(&json!({"method": "turn/started"})),
            Message::Notification
        ));
        let refusal = refuse_request(json!(9));
        assert_eq!(refusal["id"], 9);
        assert!(refusal["error"]["message"]
            .as_str()
            .unwrap()
            .contains("approvals"));
    }

    #[test]
    fn resume_errors_are_classified() {
        assert_eq!(
            request_failed(ID_THREAD, &json!({"message": "thread not found"})).code,
            "cli_resume_failed"
        );
        assert_eq!(
            request_failed(ID_TURN, &json!({"message": "bad"})).code,
            "cli_execution_error"
        );
    }
}
