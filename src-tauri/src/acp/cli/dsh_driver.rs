//! fork(letscubo)专属: the per-turn DeepSeek Harness CLI driver behind a
//! `ConnectionTransport::Cli` connection for `AgentType::DeepSeek`.
//!
//! Each `Prompt` spawns one `dsh --profile headless --json` process with the
//! prompt as its positional task; the process prints NDJSON and exits after
//! `final`. The first turn has no session id — dsh mints `session-<uuid>` and
//! reports it in the `session` event, which becomes this connection's
//! `external_id` (and the id `--session-id` continues on later turns).
//!
//! Unlike the claude driver this one records codeg's own transcript
//! (`acp_transcript`), because `commands::conversations` prefers that
//! transcript for DeepSeek history; a CLI turn that recorded nothing would
//! read back as an empty reply.
//!
//! Invariant shared with `driver.rs`: every turn ends with exactly one
//! `TurnComplete`.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex, RwLock};

use super::driver::{
    reject_unsupported, terminate, truncate, wait_or_terminate, OutputTail,
    MAX_CONSECUTIVE_UNPARSABLE, PROTOCOL_SAMPLE_BYTES, STDERR_DRAIN_WAIT,
};
use super::dsh_profile::{remove_turn_patch, write_turn_patch, DshLaunchProfile};
use super::dsh_prompt::{build_args, flatten_prompt};
use super::dsh_stream::DshStreamMapper;
use super::stream_json::{CliTurnError, LineOutcome, TurnFinish, STOP_CANCELLED, STOP_UNKNOWN};
use crate::acp::connection::{
    map_prompt_blocks, record_prompt, record_transcript_header_continuing,
    record_transcript_update, record_turn_end, record_turn_error_raw, AgentConnection,
    ConnectionCommand, DelegationInjection,
};
use crate::acp::session_state::SessionState;
use crate::acp::types::{AcpEvent, ConnectionStatus, PromptInputBlock, UserMessageBlock};
use crate::models::agent::AgentType;
use crate::web::event_bridge::{emit_with_state, EventEmitter};

pub(crate) struct DshDriver {
    pub connection_id: String,
    pub agent_type: AgentType,
    pub executable: PathBuf,
    pub working_dir: PathBuf,
    pub runtime_env: BTreeMap<String, String>,
    pub state: Arc<RwLock<SessionState>>,
    pub emitter: EventEmitter,
    pub child_pid: Arc<AtomicU32>,
    pub connections: Arc<Mutex<HashMap<String, AgentConnection>>>,
    pub profile: DshLaunchProfile,
    pub provider: Option<String>,
    pub delegation: Option<DelegationInjection>,
    /// The conversation's previous session when it cannot be resumed (an id
    /// minted by the retired `deepseek-acp` bridge). The session the launcher
    /// mints on the first turn links back to it, so the recorded history reads
    /// as one conversation. Only a NEW transcript takes it: the header write is
    /// a no-op once the file exists.
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

impl DshDriver {
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
        remove_turn_patch(&self.profile, &self.connection_id);
        if let (Some(inj), Some(companion)) = (&self.delegation, &self.profile.companion) {
            inj.tokens.revoke(&companion.token).await;
        }
        self.connections.lock().await.remove(&self.connection_id);
        tracing::info!(connection_id = %self.connection_id, "[CLI][dsh] driver stopped");
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

        let prepared = flatten_prompt(&blocks, &self.working_dir).and_then(|prompt| {
            write_turn_patch(
                &self.profile,
                &self.connection_id,
                self.provider.as_deref(),
                model.as_deref(),
            )
            .map(|patch| build_args(&patch, session_id.as_deref(), &prompt))
            .map_err(|message| CliTurnError {
                code: "dsh_profile_failed",
                message,
                details: None,
            })
        });

        let (finish, disconnect) = match prepared {
            Err(error) => (
                TurnFinish {
                    stop_reason: STOP_UNKNOWN.to_string(),
                    error: Some(error),
                },
                false,
            ),
            Ok(args) => match self.spawn(&args).await {
                Ok(child) => {
                    self.drive(child, &mut record, model.clone(), commands)
                        .await
                }
                Err(err) => (
                    TurnFinish {
                        stop_reason: STOP_UNKNOWN.to_string(),
                        error: Some(CliTurnError {
                            code: "cli_spawn_failed",
                            message: format!("Failed to start DeepSeek Harness: {err}"),
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

    async fn spawn(&self, args: &[String]) -> std::io::Result<Child> {
        let mut command = crate::process::tokio_command(&self.executable);
        command
            .args(args)
            .current_dir(&self.working_dir)
            .envs(&self.runtime_env)
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
        model: Option<String>,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> (TurnFinish, bool) {
        let pid = child.id().unwrap_or(0);
        self.child_pid.store(pid, Ordering::SeqCst);
        tracing::info!(connection_id = %self.connection_id, pid, "[CLI][dsh] turn started");

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

        let mut mapper = DshStreamMapper::new(
            record.session_id.clone(),
            Some(self.working_dir.clone()),
            model,
        );
        let end = match child.stdout.take() {
            Some(stdout) => self.pump(stdout, &mut mapper, record, commands).await,
            None => TurnEnd::Eof,
        };

        match end {
            TurnEnd::Finished(finish) => {
                let status = wait_or_terminate(&mut child, pid).await;
                tracing::info!(connection_id = %self.connection_id, %status, "[CLI][dsh] turn finished");
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
                                "DeepSeek Harness produced {MAX_CONSECUTIVE_UNPARSABLE} consecutive unparsable output lines"
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
                // `turn_end` seen but `final` lost: the turn still has an outcome.
                if let Some(finish) = mapper.take_pending_finish() {
                    return (finish, false);
                }
                let tail = stderr_tail.lock().map(|t| t.text()).unwrap_or_default();
                (
                    TurnFinish {
                        stop_reason: STOP_UNKNOWN.to_string(),
                        error: Some(CliTurnError {
                            code: "cli_exited",
                            message: format!(
                                "DeepSeek Harness exited before finishing the turn ({status})"
                            ),
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
        mapper: &mut DshStreamMapper,
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
                                unparsable += 1;
                                tracing::warn!(
                                    connection_id = %self.connection_id,
                                    "[CLI][dsh] unparsable stdout line: {}",
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
                        tracing::warn!(connection_id = %self.connection_id, "[CLI][dsh] stdout read failed: {e}");
                        return TurnEnd::Eof;
                    }
                },
                command = commands.recv() => match command {
                    Some(ConnectionCommand::Cancel) => return TurnEnd::Cancelled,
                    Some(ConnectionCommand::Disconnect) | None => return TurnEnd::Disconnected,
                    Some(ConnectionCommand::Prompt { .. }) => tracing::warn!(
                        connection_id = %self.connection_id,
                        "[CLI][dsh] in-turn Prompt DROPPED — the turn_in_flight gate should have rejected this"
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
        // Durable before `SessionStarted` is emitted: the session-binding guard
        // reads the link to tell a continuation from an unrelated session.
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
                "[CLI][dsh] turn failed: {}",
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

/// The `session/update` shape the history projection reads back for one live
/// event; `None` for events the projection never records.
/// 真工具名记进 ACP 报文的 `_meta` 扩展位 —— ACP 本身没有工具名通道(`title` 是给人看的),
/// 而读回时无从重算,所以必须落盘。键名带 `codeg.` 前缀,避免和别家的 meta 撞。
fn set_tool_name_meta(v: &mut Value, tool_name: Option<&str>) {
    let Some(name) = tool_name.filter(|n| !n.is_empty()) else {
        return;
    };
    v["_meta"]["codeg.toolName"] = Value::String(name.to_string());
}

pub(crate) fn transcript_update_for(event: &AcpEvent) -> Option<sacp::schema::SessionUpdate> {
    let value = match event {
        AcpEvent::ContentDelta { text, .. } => json!({
            "sessionUpdate": "agent_message_chunk",
            "content": { "type": "text", "text": text },
        }),
        AcpEvent::Thinking { text, .. } => json!({
            "sessionUpdate": "agent_thought_chunk",
            "content": { "type": "text", "text": text },
        }),
        AcpEvent::ToolCall {
            tool_call_id,
            title,
            kind,
            status,
            tool_name,
            raw_input,
            locations,
            ..
        } => {
            let raw_input: Option<Value> = raw_input
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok());
            let mut v = json!({
                "sessionUpdate": "tool_call",
                "toolCallId": tool_call_id,
                "title": title,
                "kind": kind,
                "status": status,
            });
            if let Some(input) = raw_input {
                v["rawInput"] = input;
            }
            if let Some(locations) = locations {
                v["locations"] = locations.clone();
            }
            set_tool_name_meta(&mut v, tool_name.as_deref());
            v
        }
        AcpEvent::ToolCallUpdate {
            tool_call_id,
            title,
            status,
            tool_name,
            raw_input,
            raw_output,
            ..
        } => {
            let mut v = json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": tool_call_id,
            });
            if let Some(status) = status {
                v["status"] = Value::String(status.clone());
            }
            /*
             * title / rawInput 也要记:CLI 通道里工具刚出现时参数还没到,第一条 tool_call
             * 记下的是占位标题("Terminal")且没有参数;参数齐了才发这条更新。以前这里用 `..`
             * 把两者吞掉,于是历史里永远是那份占位 —— 实时看得到命令,刷新后只剩 "Terminal"
             * 且参数为空(2026-09-19 实测 A3 会话 152)。
             */
            if let Some(title) = title {
                v["title"] = Value::String(title.clone());
            }
            if let Some(input) = raw_input
                .as_deref()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
            {
                v["rawInput"] = input;
            }
            if let Some(raw) = raw_output
                .as_deref()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
            {
                v["rawOutput"] = raw;
            }
            set_tool_name_meta(&mut v, tool_name.as_deref());
            v
        }
        AcpEvent::UsageUpdate { used, size } => json!({
            "sessionUpdate": "usage_update",
            "used": used,
            "size": size,
        }),
        _ => return None,
    };
    serde_json::from_value(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_events_round_trip_into_recorded_session_updates() {
        let events = vec![
            AcpEvent::ContentDelta {
                text: "hi".into(),
                parent_tool_use_id: None,
            },
            AcpEvent::Thinking {
                text: "hmm".into(),
                parent_tool_use_id: None,
            },
            AcpEvent::ToolCall {
                tool_name: None,
                description: None,
                tool_call_id: "c1".into(),
                title: "app-notion: notion-fetch".into(),
                kind: "other".into(),
                status: "in_progress".into(),
                content: None,
                raw_input: Some(r#"{"id":"self"}"#.into()),
                raw_output: None,
                locations: None,
                meta: None,
                images: None,
            },
            AcpEvent::ToolCallUpdate {
                tool_name: None,
                description: None,
                tool_call_id: "c1".into(),
                title: None,
                status: Some("completed".into()),
                content: None,
                raw_input: None,
                raw_output: Some("\"ok\"".into()),
                raw_output_append: None,
                locations: None,
                meta: None,
                images: None,
            },
            AcpEvent::UsageUpdate {
                used: 10,
                size: 1000,
            },
        ];
        for event in &events {
            let update = transcript_update_for(event).expect("recorded");
            assert!(crate::parsers::acp_native::is_recorded_update(&update));
        }
        assert!(transcript_update_for(&AcpEvent::StatusChanged {
            status: ConnectionStatus::Connected
        })
        .is_none());
    }

    #[test]
    fn tool_call_update_carries_status_and_raw_output() {
        let update = transcript_update_for(&AcpEvent::ToolCallUpdate {
            tool_name: None,
            description: None,
            tool_call_id: "c9".into(),
            title: None,
            status: Some("failed".into()),
            content: None,
            raw_input: None,
            raw_output: Some("\"boom\"".into()),
            raw_output_append: None,
            locations: None,
            meta: None,
            images: None,
        })
        .unwrap();
        let v = serde_json::to_value(&update).unwrap();
        assert_eq!(v["toolCallId"], "c9");
        assert_eq!(v["status"], "failed");
        assert_eq!(v["rawOutput"], "boom");
    }

    /// CLI 通道(claude_code / codex / dsh 共用本函数)里,工具刚出现时参数还没到:
    /// 第一条 `tool_call` 记的是占位标题与空参数,参数齐了才发更新。更新必须把 title 与
    /// rawInput 一并记下,否则历史永远停在占位 —— 实时看得到命令,刷新后只剩 "Terminal"。
    #[test]
    fn tool_call_update_carries_title_and_raw_input() {
        let update = transcript_update_for(&AcpEvent::ToolCallUpdate {
            tool_name: None,
            description: None,
            tool_call_id: "c1".into(),
            title: Some("officecli save a.pptx".into()),
            status: Some("in_progress".into()),
            content: None,
            raw_input: Some(
                "{\"command\":\"officecli save a.pptx\",\"description\":\"保存 PPT\"}".into(),
            ),
            raw_output: None,
            raw_output_append: None,
            locations: None,
            meta: None,
            images: None,
        })
        .unwrap();
        let v = serde_json::to_value(&update).unwrap();
        assert_eq!(v["title"], "officecli save a.pptx");
        assert_eq!(v["rawInput"]["command"], "officecli save a.pptx");
        assert_eq!(v["rawInput"]["description"], "保存 PPT");
    }

    /// 真工具名要随转写落盘:ACP 没有工具名通道(`title` 早被各家换成人话),读回时无从重算。
    /// 界面按工具类型分形态(命令一种、读写文件另一种)全靠它。
    #[test]
    fn tool_name_is_recorded_in_meta() {
        let update = transcript_update_for(&AcpEvent::ToolCall {
            tool_name: Some("Edit".into()),
            description: None,
            tool_call_id: "c3".into(),
            title: "Edit a.rs".into(),
            kind: "edit".into(),
            status: "in_progress".into(),
            content: None,
            raw_input: Some("{\"file_path\":\"/a.rs\"}".into()),
            raw_output: None,
            locations: None,
            meta: None,
            images: None,
        })
        .unwrap();
        let v = serde_json::to_value(&update).unwrap();
        assert_eq!(v["_meta"]["codeg.toolName"], "Edit");
        // 更新事件同样带得上(首帧没带时由它补)
        let update = transcript_update_for(&AcpEvent::ToolCallUpdate {
            tool_name: Some("Bash".into()),
            description: None,
            tool_call_id: "c4".into(),
            title: None,
            status: Some("completed".into()),
            content: None,
            raw_input: None,
            raw_output: None,
            raw_output_append: None,
            locations: None,
            meta: None,
            images: None,
        })
        .unwrap();
        let v = serde_json::to_value(&update).unwrap();
        assert_eq!(v["_meta"]["codeg.toolName"], "Bash");
    }

    /// 没有工具名(ACP 通道拿不到)就不要凭空写出 `_meta`。
    #[test]
    fn missing_tool_name_writes_no_meta() {
        let update = transcript_update_for(&AcpEvent::ToolCallUpdate {
            tool_name: None,
            description: None,
            tool_call_id: "c5".into(),
            title: None,
            status: Some("completed".into()),
            content: None,
            raw_input: None,
            raw_output: None,
            raw_output_append: None,
            locations: None,
            meta: None,
            images: None,
        })
        .unwrap();
        let v = serde_json::to_value(&update).unwrap();
        assert!(
            v.get("_meta")
                .is_none_or(|m| m.get("codeg.toolName").is_none()),
            "{v}"
        );
    }

    /// 只带状态的更新(最常见)不要凭空写出 title / rawInput 字段。
    #[test]
    fn status_only_update_adds_no_title_or_input() {
        let update = transcript_update_for(&AcpEvent::ToolCallUpdate {
            tool_name: None,
            description: None,
            tool_call_id: "c2".into(),
            title: None,
            status: Some("completed".into()),
            content: None,
            raw_input: None,
            raw_output: None,
            raw_output_append: None,
            locations: None,
            meta: None,
            images: None,
        })
        .unwrap();
        let v = serde_json::to_value(&update).unwrap();
        assert!(v.get("title").is_none() || v["title"].is_null(), "{v}");
        assert!(
            v.get("rawInput").is_none() || v["rawInput"].is_null(),
            "{v}"
        );
    }
}
