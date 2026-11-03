//! fork(letscubo)专属: the per-turn pi CLI driver behind a
//! `ConnectionTransport::Cli` connection for `AgentType::Pi`.
//!
//! Each `Prompt` spawns one `pi --mode rpc` process and keeps stdin open
//! through the owning turn. Native `steer` changes that process's current run;
//! normal queued messages stay host-owned. Close stdin only after
//! `agent_settled` and all steering receipts are reconciled. Session events
//! still use the existing [`super::pi_stream`] mapper and transcript recorder.
//!
//! ## Why not the ACP shim
//!
//! pi has no official ACP support; the third-party `pi-acp` bridge we used
//! before swallowed provider failures — it rendered pi's `auto_retry_*` events
//! as assistant prose and then settled every turn as `end_turn`, so a failed
//! turn reached the user as a reply that looked like the model talking. Talking
//! to pi directly is what makes `TurnFinish.error` carry the real error (see
//! the `pi_stream` module docs).
//!
//! Invariant shared with `driver.rs`: every turn ends with exactly one
//! `TurnComplete`.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex, RwLock};

use super::driver::{
    reject_unsupported, terminate, truncate, wait_or_terminate, OutputTail,
    MAX_CONSECUTIVE_UNPARSABLE, PROTOCOL_SAMPLE_BYTES, STDERR_DRAIN_WAIT,
};
use super::dsh_driver::transcript_update_for;
use super::dsh_prompt::flatten_prompt;
use super::pi_rpc::{user_message_text, PiSteeringReceipts};
use super::pi_stream::PiStreamMapper;
use super::stream_json::{CliTurnError, LineOutcome, TurnFinish, STOP_CANCELLED, STOP_UNKNOWN};
use crate::acp::connection::{
    map_prompt_blocks, record_prompt, record_transcript_header_continuing,
    record_transcript_update, record_turn_end, record_turn_error_raw, AgentConnection,
    ConnectionCommand, DelegationInjection,
};
use crate::acp::error::AcpError;
use crate::acp::session_state::SessionState;
use crate::acp::types::{AcpEvent, ConnectionStatus, PromptInputBlock, UserMessageBlock};
use crate::models::agent::AgentType;
use crate::web::event_bridge::{emit_with_state, EventEmitter};

pub(crate) struct PiDriver {
    pub connection_id: String,
    pub agent_type: AgentType,
    pub executable: PathBuf,
    pub working_dir: PathBuf,
    pub runtime_env: BTreeMap<String, String>,
    pub state: Arc<RwLock<SessionState>>,
    pub emitter: EventEmitter,
    pub child_pid: Arc<AtomicU32>,
    pub connections: Arc<Mutex<HashMap<String, AgentConnection>>>,
    pub provider: Option<String>,
    /// Where pi keeps this agent's sessions. `None` leaves pi's own default
    /// (`$PI_CODING_AGENT_DIR/sessions`), which `runtime_env` already scopes
    /// per agent.
    pub session_dir: Option<PathBuf>,
    /// The conversation's previous session when it cannot be resumed; the new
    /// session links back to it so the recorded history reads as one
    /// conversation. Only a NEW transcript takes it.
    pub continues_from: Option<String>,
    /// fork(letscubo): the delegation registry the session token below lives in
    /// (revoked from it when the driver stops).
    pub delegation: Option<DelegationInjection>,
    /// fork(letscubo): this conversation's token for the HTTP companion
    /// (see [`crate::acp::question::HTTP_SESSION_TOKEN_ENV`]); `None` when
    /// `ask_user_question` is off or there is no delegation registry.
    pub session_token: Option<String>,
}

/// fork(letscubo): mint the per-connection token pi presents to the HTTP
/// companion so `ask_user_question` can be parked on THIS conversation.
///
/// pi only reaches the companion over HTTP (its own MCP reads the projected
/// `mcp.json`; codeg has no per-session MCP wire into `pi --print`), and that
/// endpoint otherwise knows only the agent. The token goes in the same registry
/// the stdio companion's `--token` does, so the endpoint resolves it exactly like
/// the listener's `ask_target`. Only minted while asking is enabled.
pub(crate) async fn mint_session_token(
    injection: Option<&DelegationInjection>,
    connection_id: &str,
    working_dir: &Path,
) -> Option<String> {
    let injection = injection?;
    if !injection.ask.is_enabled().await {
        return None;
    }
    let token = uuid::Uuid::new_v4().to_string();
    injection
        .tokens
        .register(
            token.clone(),
            crate::acp::delegation::listener::TokenEntry {
                parent_connection_id: connection_id.to_string(),
                working_dir: working_dir.to_path_buf(),
            },
        )
        .await;
    Some(token)
}

enum TurnEnd {
    Finished(TurnFinish),
    Eof,
    Cancelled,
    Disconnected,
    Protocol(String),
}

/// What one turn knows about its session id and prompt for recording.
struct TurnRecord {
    session_id: Option<String>,
    prompt_blocks: Vec<PromptInputBlock>,
    prompt_recorded: bool,
}

/// Keep exact session/provider/model selection while using official JSONL RPC.
/// Prompts are protocol records on stdin, never positional arguments.
pub(super) fn build_args(
    session_id: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
    session_dir: Option<&Path>,
) -> Vec<String> {
    let mut args = vec!["--mode".to_string(), "rpc".to_string()];
    if let Some(provider) = provider {
        args.push("--provider".to_string());
        args.push(provider.to_string());
    }
    if let Some(model) = model {
        args.push("--model".to_string());
        args.push(model.to_string());
    }
    if let Some(dir) = session_dir {
        args.push("--session-dir".to_string());
        args.push(dir.display().to_string());
    }
    if let Some(session_id) = session_id {
        // `--session-id` continues an exact session, creating it when missing,
        // so a resumed turn never needs a separate "does it exist" probe.
        args.push("--session-id".to_string());
        args.push(session_id.to_string());
    }
    args
}

impl PiDriver {
    pub(crate) async fn run(self, mut commands: mpsc::Receiver<ConnectionCommand>) {
        while let Some(command) = commands.recv().await {
            match command {
                ConnectionCommand::Prompt {
                    blocks,
                    user_message,
                    ..
                } => {
                    let stop = self.run_turn(blocks, user_message, &mut commands).await;
                    self.reclaim_questions().await;
                    if stop {
                        break;
                    }
                }
                ConnectionCommand::Disconnect => break,
                ConnectionCommand::Cancel => {}
                ConnectionCommand::NativeSteer { reply, .. } => {
                    let _ = reply.send(Err(AcpError::NoActiveTurn));
                }
                other => reject_unsupported(other),
            }
        }
        self.emit(AcpEvent::StatusChanged {
            status: ConnectionStatus::Disconnected,
        })
        .await;
        if let (Some(inj), Some(token)) = (&self.delegation, &self.session_token) {
            inj.tokens.revoke(token).await;
        }
        self.reclaim_questions().await;
        self.connections.lock().await.remove(&self.connection_id);
        tracing::info!(connection_id = %self.connection_id, "[CLI][pi] driver stopped");
    }

    /// fork(letscubo): a question pi parked over the HTTP companion cannot outlive
    /// its turn — the `pi --print` process that was waiting on it has exited (or
    /// was killed by a cancel). Drop it so the card clears and the connection's
    /// one-at-a-time slot frees, instead of trusting the HTTP layer to notice the
    /// client went away.
    async fn reclaim_questions(&self) {
        if self.session_token.is_none() {
            return;
        }
        if let Some(inj) = &self.delegation {
            inj.questions
                .cancel_questions_by_parent(&self.connection_id)
                .await;
        }
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
        // A continued session is known up front: record before the spawn, like
        // the ACP loop records before `session/prompt`.
        if let Some(sid) = &session_id {
            self.record_turn_start(sid, &record.prompt_blocks).await;
            record.prompt_recorded = true;
        }
        let started = Instant::now();
        let use_rpc = self.rpc_supported().await;

        let (finish, disconnect) = match flatten_prompt(&blocks, &self.working_dir) {
            Err(error) => (
                TurnFinish {
                    stop_reason: STOP_UNKNOWN.to_string(),
                    error: Some(error),
                },
                false,
            ),
            Ok(prompt) => {
                let mut args = build_args(
                    session_id.as_deref(),
                    self.provider.as_deref(),
                    model.as_deref(),
                    self.session_dir.as_deref(),
                );
                if !use_rpc {
                    args.splice(0..2, ["--print".into(), "--mode".into(), "json".into()]);
                }
                match self.spawn(&args, &prompt, use_rpc).await {
                    Ok(child) => {
                        self.drive(child, &mut record, model.clone(), commands, use_rpc)
                            .await
                    }
                    Err(err) => (
                        TurnFinish {
                            stop_reason: STOP_UNKNOWN.to_string(),
                            error: Some(CliTurnError {
                                code: "cli_spawn_failed",
                                message: format!("Failed to start pi: {err}"),
                                details: Some(self.executable.display().to_string()),
                            }),
                        },
                        false,
                    ),
                }
            }
        };
        self.state.write().await.native_steer_v1_available = false;
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

    /// Spawn RPC; the pump reads stdout before writing the prompt so large
    /// inputs cannot deadlock against Pi's stdout backpressure.
    async fn spawn(&self, args: &[String], prompt: &str, use_rpc: bool) -> std::io::Result<Child> {
        let mut command = crate::process::tokio_command(&self.executable);
        command
            .args(args)
            .current_dir(&self.working_dir)
            .envs(&self.runtime_env)
            // fork(letscubo): always set, empty when there is no token, so the
            // projected `${MYCLAW_CODEG_SESSION}` header always resolves (an empty
            // header is simply ignored by the endpoint).
            .env(
                crate::acp::question::HTTP_SESSION_TOKEN_ENV,
                self.session_token.as_deref().unwrap_or(""),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = crate::process::spawn_retrying_exec_busy(|| command.spawn()).await?;
        if !use_rpc {
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(prompt.as_bytes()).await?;
                stdin.flush().await?;
            }
        }
        Ok(child)
    }

    /// PATH installs may lag the registry pin. Require an actual stable binary
    /// version whose RPC lifecycle was audited; otherwise preserve print mode.
    async fn rpc_supported(&self) -> bool {
        let mut probe = crate::process::tokio_command(&self.executable);
        probe
            .arg("--version")
            .current_dir(&self.working_dir)
            .envs(&self.runtime_env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        match tokio::time::timeout(Duration::from_secs(5), probe.output()).await {
            Ok(Ok(output)) if output.status.success() => {
                supports_rpc_version(&String::from_utf8_lossy(&output.stdout))
            }
            _ => false,
        }
    }

    async fn drive(
        &self,
        mut child: Child,
        record: &mut TurnRecord,
        _model: Option<String>,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
        use_rpc: bool,
    ) -> (TurnFinish, bool) {
        let pid = child.id().unwrap_or(0);
        self.child_pid.store(pid, Ordering::SeqCst);
        tracing::info!(connection_id = %self.connection_id, pid, "[CLI][pi] turn started");

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

        let mut mapper =
            PiStreamMapper::new(record.session_id.clone(), Some(self.working_dir.clone()));
        let mut stdin = child.stdin.take();
        let end = match (child.stdout.take(), stdin.as_mut(), use_rpc) {
            (Some(stdout), Some(stdin), true) => {
                self.pump(stdout, stdin, &mut mapper, record, commands)
                    .await
            }
            (Some(stdout), _, false) => {
                self.pump_print(stdout, &mut mapper, record, commands).await
            }
            _ => TurnEnd::Eof,
        };
        self.state.write().await.native_steer_v1_available = false;
        drop(stdin); // RPC orderly shutdown, still one subprocess per turn.

        match end {
            TurnEnd::Finished(finish) => {
                let status = wait_or_terminate(&mut child, pid).await;
                tracing::info!(connection_id = %self.connection_id, %status, "[CLI][pi] turn finished");
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
            TurnEnd::Protocol(sample) => {
                terminate(&mut child, pid).await;
                (
                    TurnFinish {
                        stop_reason: STOP_UNKNOWN.to_string(),
                        error: Some(CliTurnError {
                            code: "cli_protocol",
                            message: format!(
                                "pi produced {MAX_CONSECUTIVE_UNPARSABLE} consecutive unparsable output lines"
                            ),
                            details: Some(sample),
                        }),
                    },
                    false,
                )
            }
            TurnEnd::Eof => {
                // `agent_settled` never arrived. Pi prints its startup warnings
                // on stdout before the first record (a bad `--model` prints
                // `Warning: Model "x" not found...`), so the stderr tail is not
                // always the whole story — but it is what a crash leaves.
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
                            message: format!("pi exited before finishing the turn ({status})"),
                            details: (!tail.is_empty()).then_some(tail),
                        }),
                    },
                    false,
                )
            }
        }
    }

    async fn pump(
        &self,
        stdout: tokio::process::ChildStdout,
        stdin: &mut tokio::process::ChildStdin,
        mapper: &mut PiStreamMapper,
        record: &mut TurnRecord,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> TurnEnd {
        let mut lines = BufReader::new(stdout).lines();
        let mut receipts = PiSteeringReceipts::default();
        let mut unparsable = 0usize;
        let mut started = false;
        let mut initial_user_seen = false;
        let mut initial_prompt: Option<String> = None;
        let mut settled: Option<TurnFinish> = None;
        let mut settle_deadline: Option<tokio::time::Instant> = None;
        let startup_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        if write_rpc(stdin, &json!({"id":"codeg-state", "type":"get_state"}))
            .await
            .is_err()
        {
            return TurnEnd::Eof;
        }
        loop {
            if settled.is_some() {
                if receipts.is_empty() {
                    return TurnEnd::Finished(settled.take().unwrap());
                }
                if let Some(command) = receipts.clear_after_settle() {
                    if write_rpc(stdin, &command).await.is_err() {
                        receipts.unconfirmed();
                        return TurnEnd::Finished(settled.take().unwrap());
                    }
                }
            }
            let deadline = settle_deadline;
            tokio::select! {
                // Drain available stdout before taking a steering command: a
                // queued agent_settled must disable admission first.
                biased;
                line = lines.next_line() => match line {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() { continue; }
                        let value = serde_json::from_str::<Value>(&line).ok();
                        if let Some(value) = &value {
                            if value["type"] == "response" && value["id"] == "codeg-state" {
                                let Some(session_id) = value["data"]["sessionId"].as_str().filter(|id| !id.is_empty()) else {
                                    return rpc_failure("Pi did not report an exact session identity");
                                };
                                if value["success"] != true || record.session_id.as_deref().is_some_and(|id| id != session_id) {
                                    return rpc_failure("Pi refused the requested session identity");
                                }
                                let session = json!({"type":"session", "id":session_id}).to_string();
                                if let LineOutcome::Events(events) = mapper.map_line(&session) {
                                    for event in events { self.emit_recorded(event, record).await; }
                                }
                                let prompt = match flatten_prompt(&record.prompt_blocks, &self.working_dir) {
                                    Ok(prompt) => prompt,
                                    Err(_) => return rpc_failure("Pi prompt preparation failed"),
                                };
                                initial_prompt = Some(prompt.clone());
                                if write_rpc(stdin, &json!({"id":"codeg-prompt", "type":"prompt", "message":prompt})).await.is_err() { return TurnEnd::Eof; }
                                continue;
                            }
                            if value["type"] == "response" && value["id"] == "codeg-prompt" {
                                if value["success"] != true { return rpc_failure(value["error"].as_str().unwrap_or("Pi rejected the prompt")); }
                                if value["data"]["disposition"] == "handled" && !started {
                                    return TurnEnd::Finished(TurnFinish { stop_reason: super::stream_json::STOP_END_TURN.into(), error: None });
                                }
                            }
                            if value["type"] == "agent_start" { started = true; }
                            if !initial_user_seen && initial_prompt.is_some()
                                && user_message_text(value).as_deref() == initial_prompt.as_deref() {
                                initial_user_seen = true;
                            } else {
                                for blocks in receipts.observe(value) {
                                    if let Some(session_id) = &record.session_id {
                                        record_prompt(self.agent_type, session_id, &map_prompt_blocks(blocks)).await;
                                    }
                                }
                            }
                            if receipts.observe_clear(value) && settled.is_some() {
                                return TurnEnd::Finished(settled.take().unwrap());
                            }
                            self.state.write().await.native_steer_v1_available = started && initial_user_seen && settled.is_none();
                        }
                        match mapper.map_line(&line) {
                            LineOutcome::Events(events) => {
                                unparsable = 0;
                                for event in events { self.emit_recorded(event, record).await; }
                            }
                            LineOutcome::Finished { events, finish } => {
                                for event in events { self.emit_recorded(event, record).await; }
                                self.state.write().await.native_steer_v1_available = false;
                                settled = Some(finish);
                                settle_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(10));
                            }
                            LineOutcome::Unparsable => {
                                unparsable += 1;
                                tracing::warn!(connection_id = %self.connection_id, "[CLI][pi] unparsable stdout: {}", truncate(&line, 200));
                                if unparsable >= MAX_CONSECUTIVE_UNPARSABLE { return TurnEnd::Protocol(truncate(&line, PROTOCOL_SAMPLE_BYTES)); }
                            }
                        }
                    }
                    Ok(None) => return settled.map(TurnEnd::Finished).unwrap_or(TurnEnd::Eof),
                    Err(_) => return TurnEnd::Eof,
                },
                _ = async move {
                    match deadline { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending::<()>().await }
                } => { receipts.unconfirmed(); return TurnEnd::Finished(settled.take().unwrap()); },
                _ = tokio::time::sleep_until(startup_deadline), if !started => {
                    return rpc_failure("Pi RPC did not start the requested turn");
                },
                command = commands.recv() => match command {
                    Some(ConnectionCommand::Cancel) => return TurnEnd::Cancelled,
                    Some(ConnectionCommand::Disconnect) | None => return TurnEnd::Disconnected,
                    Some(ConnectionCommand::NativeSteer { expected_turn_id, blocks, reply, .. }) => {
                        let current = self.state.read().await;
                        let allowed = started && initial_user_seen && settled.is_none()
                            && crate::acp::steering::matches_turn(&current, &expected_turn_id);
                        drop(current);
                        if !allowed { let _ = reply.send(Err(AcpError::NoActiveTurn)); continue; }
                        if let Some(command) = receipts.enqueue(blocks, reply) {
                            if write_rpc(stdin, &command).await.is_err() { return TurnEnd::Eof; }
                        }
                    }
                    Some(ConnectionCommand::Prompt { .. }) => tracing::warn!(connection_id = %self.connection_id, "[CLI][pi] rejected in-turn Prompt"),
                    Some(other) => reject_unsupported(other),
                },
            }
        }
    }

    async fn pump_print(
        &self,
        stdout: tokio::process::ChildStdout,
        mapper: &mut PiStreamMapper,
        record: &mut TurnRecord,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> TurnEnd {
        let mut lines = BufReader::new(stdout).lines();
        let mut unparsable = 0usize;
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
                                    self.emit_recorded(event, record).await;
                                }
                            }
                            LineOutcome::Finished { events, finish } => {
                                for event in events {
                                    self.emit_recorded(event, record).await;
                                }
                                return TurnEnd::Finished(finish);
                            }
                            LineOutcome::Unparsable => {
                                // pi prints human warnings on stdout before the
                                // first record; those are not a protocol fault
                                // unless they never stop.
                                unparsable += 1;
                                tracing::warn!(
                                    connection_id = %self.connection_id,
                                    "[CLI][pi] unparsable stdout line: {}",
                                    truncate(&line, 200)
                                );
                                if unparsable >= MAX_CONSECUTIVE_UNPARSABLE {
                                    return TurnEnd::Protocol(truncate(&line, PROTOCOL_SAMPLE_BYTES));
                                }
                            }
                        }
                    }
                    Ok(None) => return TurnEnd::Eof,
                    Err(e) => {
                        tracing::warn!(connection_id = %self.connection_id, "[CLI][pi] stdout read failed: {e}");
                        return TurnEnd::Eof;
                    }
                },
                command = commands.recv() => match command {
                    Some(ConnectionCommand::Cancel) => return TurnEnd::Cancelled,
                    Some(ConnectionCommand::Disconnect) | None => return TurnEnd::Disconnected,
                    Some(ConnectionCommand::Prompt { .. }) => tracing::warn!(
                        connection_id = %self.connection_id,
                        "[CLI][pi] in-turn Prompt DROPPED — the turn_in_flight gate should have rejected this"
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
        // 工具结果的图片先落盘:自录线路只记路径,历史靠这份文件取图(与 claude 线路 driver.rs 同一套)。
        if let AcpEvent::ToolCallUpdate {
            images: Some(images),
            ..
        } = &event
        {
            crate::acp::connection::persist_tool_images(images, Some(&self.working_dir));
        }
        if let AcpEvent::SessionStarted { session_id } = &event {
            record.session_id = Some(session_id.clone());
            if !record.prompt_recorded {
                self.record_turn_start(session_id, &record.prompt_blocks)
                    .await;
                record.prompt_recorded = true;
            }
        } else if let (Some(sid), Some(update)) = (
            &record.session_id,
            transcript_update_for(&event, Some(&self.working_dir)),
        ) {
            record_transcript_update(self.agent_type, sid, &update);
        }
        self.emit(event).await;
    }

    async fn record_turn_start(&self, session_id: &str, blocks: &[PromptInputBlock]) {
        record_transcript_header_continuing(
            self.agent_type,
            session_id,
            &self.working_dir.display().to_string(),
            self.continues_from.as_deref(),
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
                "[CLI][pi] turn failed: {}",
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

async fn write_rpc(stdin: &mut tokio::process::ChildStdin, value: &Value) -> std::io::Result<()> {
    let mut line = value.to_string();
    line.push('\n');
    stdin.write_all(line.as_bytes()).await?;
    stdin.flush().await
}

fn supports_rpc_version(version: &str) -> bool {
    let version = version.trim();
    let parts: Option<Vec<u64>> = version
        .strip_prefix('v')
        .unwrap_or(version)
        .split('.')
        .map(|part| part.parse().ok())
        .collect();
    matches!(parts.as_deref(), Some([major, minor, patch]) if (*major, *minor, *patch) >= (1, 3, 0))
}

fn rpc_failure(message: &str) -> TurnEnd {
    TurnEnd::Finished(TurnFinish {
        stop_reason: STOP_UNKNOWN.into(),
        error: Some(CliTurnError {
            code: "cli_protocol",
            message: message.into(),
            details: None,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_turn_has_no_session() {
        let args = build_args(None, Some("myclaw"), Some("GLM-5.3-FlashX"), None);
        assert_eq!(
            args,
            [
                "--mode",
                "rpc",
                "--provider",
                "myclaw",
                "--model",
                "GLM-5.3-FlashX",
            ]
        );
    }

    #[test]
    fn a_continued_turn_passes_the_session_id_and_dir() {
        let dir = PathBuf::from("/home/ubuntu/.myclaw/agents/a/pi-home/sessions");
        let args = build_args(Some("01a1271a"), None, None, Some(&dir));
        assert_eq!(
            args,
            [
                "--mode",
                "rpc",
                "--session-dir",
                "/home/ubuntu/.myclaw/agents/a/pi-home/sessions",
                "--session-id",
                "01a1271a",
            ]
        );
    }

    #[test]
    fn the_prompt_is_never_an_argument() {
        // 0.84.2 不认 `--`(`Error: Unknown option: --`),所以提示词一律走 stdin;
        // 参数里出现任何一段提示词都是回归。
        for args in [
            build_args(None, None, None, None),
            build_args(Some("s"), Some("p"), Some("m"), Some(Path::new("/d"))),
        ] {
            assert!(!args.iter().any(|a| a == "--"), "{args:?}");
            assert_eq!(args[0], "--mode");
            assert_eq!(args[1], "rpc");
            assert!(
                args.iter().all(|a| a.starts_with('-') || !a.contains(' ')),
                "{args:?}"
            );
        }
    }

    #[test]
    fn myclaw_prompt_pi_rpc_version_is_fail_closed() {
        assert!(supports_rpc_version("1.3.0\n"));
        assert!(supports_rpc_version("v1.4.0"));
        for version in ["0.84.2", "1.2.9", "", "1.3.0-alpha.1", "unknown", "vv1.3.0"] {
            assert!(!supports_rpc_version(version), "{version}");
        }
    }
}

#[cfg(all(test, unix))]
#[path = "pi_driver_rpc_tests.rs"]
mod rpc_tests;
