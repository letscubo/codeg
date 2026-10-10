//! fork(letscubo)专属: the per-turn pi CLI driver behind a
//! `ConnectionTransport::Cli` connection for `AgentType::Pi`.
//!
//! Each `Prompt` spawns one `pi --print --mode json` process with the prompt as
//! its positional argument; the process prints NDJSON session events and exits
//! after `agent_settled`. Mapping lives in [`super::pi_stream`].
//!
//! ## Why not `--mode rpc`
//!
//! Pi also speaks a long-lived JSONL RPC with steering, queueing and `abort`.
//! It is the richer transport, but it is a different lifecycle than every other
//! CLI driver here (one process per turn), and the two modes emit the *same*
//! session events — so the mapper is shared and moving to RPC later changes
//! only this file. Per-turn first keeps the admission path, `manager.cancel`
//! and the `turn_in_flight` gate working exactly as they do for claude and dsh.
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
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex, RwLock};

use super::driver::{
    reject_unsupported, terminate, truncate, wait_or_terminate, OutputTail,
    MAX_CONSECUTIVE_UNPARSABLE, PROTOCOL_SAMPLE_BYTES, STDERR_DRAIN_WAIT,
};
use super::dsh_driver::transcript_update_for;
use super::dsh_prompt::flatten_prompt;
use super::pi_stream::PiStreamMapper;
use super::stream_json::{CliTurnError, LineOutcome, TurnFinish, STOP_CANCELLED, STOP_UNKNOWN};
use crate::acp::connection::{
    map_prompt_blocks, record_prompt, record_transcript_header_continuing,
    record_transcript_update, record_turn_end, record_turn_error_raw, AgentConnection,
    ConnectionCommand,
};
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

/// `pi [options] [--] [messages...]`. The `--` matters: a prompt that starts
/// with `-` would otherwise be parsed as a flag.
pub(super) fn build_args(
    session_id: Option<&str>,
    provider: Option<&str>,
    model: Option<&str>,
    session_dir: Option<&Path>,
    prompt: &str,
) -> Vec<String> {
    let mut args = vec![
        "--print".to_string(),
        "--mode".to_string(),
        "json".to_string(),
    ];
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
    args.push("--".to_string());
    args.push(prompt.to_string());
    args
}

impl PiDriver {
    pub(crate) async fn run(self, mut commands: mpsc::Receiver<ConnectionCommand>) {
        while let Some(command) = commands.recv().await {
            match command {
                ConnectionCommand::Prompt {
                    blocks,
                    user_message,
                } => {
                    if self.run_turn(blocks, user_message, &mut commands).await {
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
        self.connections.lock().await.remove(&self.connection_id);
        tracing::info!(connection_id = %self.connection_id, "[CLI][pi] driver stopped");
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

        let (finish, disconnect) = match flatten_prompt(&blocks, &self.working_dir) {
            Err(error) => (
                TurnFinish {
                    stop_reason: STOP_UNKNOWN.to_string(),
                    error: Some(error),
                },
                false,
            ),
            Ok(prompt) => {
                let args = build_args(
                    session_id.as_deref(),
                    self.provider.as_deref(),
                    model.as_deref(),
                    self.session_dir.as_deref(),
                    &prompt,
                );
                match self.spawn(&args).await {
                    Ok(child) => self.drive(child, &mut record, model.clone(), commands).await,
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

    async fn spawn(&self, args: &[String]) -> std::io::Result<Child> {
        let mut command = crate::process::tokio_command(&self.executable);
        command
            .args(args)
            .current_dir(&self.working_dir)
            .envs(&self.runtime_env)
            // `--print` takes the prompt from argv; nothing is fed on stdin.
            .stdin(Stdio::null())
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
        record: &mut TurnRecord,
        _model: Option<String>,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
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
        let end = match child.stdout.take() {
            Some(stdout) => self.pump(stdout, &mut mapper, record, commands).await,
            None => TurnEnd::Eof,
        };

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
        if let AcpEvent::SessionStarted { session_id } = &event {
            record.session_id = Some(session_id.clone());
            if !record.prompt_recorded {
                self.record_turn_start(session_id, &record.prompt_blocks)
                    .await;
                record.prompt_recorded = true;
            }
        } else if let (Some(sid), Some(update)) = (&record.session_id, transcript_update_for(&event))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_turn_has_no_session_and_the_prompt_is_last() {
        let args = build_args(None, Some("myclaw"), Some("GLM-5.3-FlashX"), None, "hello");
        assert_eq!(
            args,
            [
                "--print",
                "--mode",
                "json",
                "--provider",
                "myclaw",
                "--model",
                "GLM-5.3-FlashX",
                "--",
                "hello",
            ]
        );
    }

    #[test]
    fn a_continued_turn_passes_the_session_id_and_dir() {
        let dir = PathBuf::from("/home/ubuntu/.myclaw/agents/a/pi-home/sessions");
        let args = build_args(Some("01a1271a"), None, None, Some(&dir), "go on");
        assert_eq!(
            args,
            [
                "--print",
                "--mode",
                "json",
                "--session-dir",
                "/home/ubuntu/.myclaw/agents/a/pi-home/sessions",
                "--session-id",
                "01a1271a",
                "--",
                "go on",
            ]
        );
    }

    #[test]
    fn a_prompt_that_looks_like_a_flag_stays_a_prompt() {
        // Without the `--` separator pi would parse this as an option.
        let args = build_args(None, None, None, None, "--help me debug this");
        assert_eq!(args.last().unwrap(), "--help me debug this");
        assert_eq!(args[args.len() - 2], "--");
    }
}
