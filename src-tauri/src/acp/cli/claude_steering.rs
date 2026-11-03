//! Claude Code 2.1.287+ native, context-only stdin steering.
//!
//! Public SDK contract: https://code.claude.com/docs/en/agent-sdk/typescript
//! SDKUserMessage `priority: next` reaches the active loop at a tool boundary;
//! `shouldQuery: false` cannot start a new assistant turn on the idle race.
//! Installed Anthropic CLI 2.1.287 also verifies the internal msg_lifecycle_v1
//! and cancel_async_message wire used below. These are compatibility points,
//! not claims that an undocumented wire is a stable public API.
use super::*;
use crate::acp::connection::SteerOutcome;
use tokio::process::ChildStdin;
use tokio::sync::oneshot;

const RECEIPT_GRACE: Duration = Duration::from_secs(2);
type Reply = oneshot::Sender<Result<SteerOutcome, AcpError>>;
struct Pending {
    blocks: Vec<PromptInputBlock>,
    reply: Reply,
    cancelling: bool,
}
struct Receipts {
    session: String,
    initial_uuid: String,
    pending: BTreeMap<String, Pending>,
    sent_ids: std::collections::HashSet<String>,
}
impl Receipts {
    fn new(session: &str, initial_uuid: &str) -> Self {
        Self {
            session: session.into(),
            initial_uuid: initial_uuid.into(),
            pending: BTreeMap::new(),
            sent_ids: std::collections::HashSet::new(),
        }
    }
    fn enqueue(&mut self, blocks: Vec<PromptInputBlock>, reply: Reply) -> Option<Value> {
        if blocks.is_empty()
            || blocks
                .iter()
                .any(|b| !matches!(b, PromptInputBlock::Text { .. }))
        {
            let _ = reply.send(Err(AcpError::NoActiveTurn));
            return None;
        }
        let id = uuid::Uuid::new_v4().to_string();
        let input = json!({"type":"user","uuid":id,"session_id":self.session,
            "parent_tool_use_id":null,"origin":{"kind":"human"},"client_composed":true,
            "priority":"next","shouldQuery":false,
            "message":{"role":"user","content":blocks.iter().map(content_block).collect::<Vec<_>>()}});
        self.sent_ids.insert(id.clone());
        self.pending.insert(
            id,
            Pending {
                blocks,
                reply,
                cancelling: false,
            },
        );
        Some(input)
    }
    /// Replay and queued/started lifecycle notifications do not prove that the
    /// active model turn answered B. Only its attributed reply/result listing
    /// BOTH A and B proves same-turn consumption. Match UUIDs, never text.
    fn observe(&mut self, event: &Value) -> Vec<(Vec<PromptInputBlock>, Reply)> {
        if event["session_id"].as_str() != Some(&self.session)
            || !matches!(
                event["type"].as_str(),
                Some("result" | "assistant" | "stream_event")
            )
            || event["parent_tool_use_id"].as_str().is_some()
            || event.get("agent_id").is_some()
        {
            return Vec::new();
        }
        let ids = event["user_message_uuids"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if !ids.iter().any(|id| id.as_str() == Some(&self.initial_uuid)) {
            return Vec::new();
        }
        let consumed: Vec<_> = self
            .pending
            .keys()
            .filter(|id| ids.iter().any(|v| v.as_str() == Some(id.as_str())))
            .cloned()
            .collect();
        let mut records = Vec::new();
        for id in consumed {
            if let Some(pending) = self.pending.remove(&id) {
                records.push((pending.blocks, pending.reply));
            }
        }
        records
    }
    fn cancel_unconsumed(&mut self) -> Vec<Value> {
        self.pending.iter_mut().filter_map(|(id,pending)| {
            if pending.cancelling { return None; }
            pending.cancelling = true;
            Some(json!({"type":"control_request","request_id":format!("codeg-native-cancel-{id}"),
                "request":{"subtype":"cancel_async_message","message_uuid":id}}))
        }).collect()
    }
    fn observe_cancel(&mut self, event: &Value) {
        if event["type"] != "control_response" {
            return;
        }
        let response = &event["response"];
        let Some(id) = response["request_id"]
            .as_str()
            .and_then(|id| id.strip_prefix("codeg-native-cancel-"))
        else {
            return;
        };
        let Some(pending) = self.pending.remove(id) else {
            return;
        };
        let outcome = if pending.cancelling
            && response["subtype"] == "success"
            && response["response"]["cancelled"] == true
        {
            // The verified CLI removes a UUID from its queue and acknowledges
            // true only if it was still withdrawable, before model consumption.
            Ok(SteerOutcome::PromptRequired)
        } else {
            Err(AcpError::protocol("claude_native_delivery_unconfirmed"))
        };
        let _ = pending.reply.send(outcome);
    }
    fn context_only_result(&self, event: &Value) -> bool {
        event["type"] == "result"
            && event["num_turns"].as_u64() == Some(0)
            && !event["user_message_uuids"]
                .as_array()
                .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(&self.initial_uuid)))
            && (event["user_message_uuid"]
                .as_str()
                .is_some_and(|id| self.sent_ids.contains(id))
                || event["user_message_uuids"].as_array().is_some_and(|ids| {
                    ids.iter()
                        .filter_map(Value::as_str)
                        .any(|id| self.sent_ids.contains(id))
                }))
    }
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    fn unconfirmed(&mut self) {
        for (_, pending) in std::mem::take(&mut self.pending) {
            let _ = pending.reply.send(Err(AcpError::protocol(
                "claude_native_delivery_unconfirmed",
            )));
        }
    }
}
impl Drop for Receipts {
    fn drop(&mut self) {
        self.unconfirmed();
    }
}

fn audited_version(raw: &str) -> bool {
    let Some(value) = raw.split_whitespace().next() else {
        return false;
    };
    let Ok(version) = semver::Version::parse(value) else {
        return false;
    };
    version.major == 2 && version.pre.is_empty() && version >= semver::Version::new(2, 1, 287)
}
fn supports_init(event: &Value, session: &str) -> bool {
    event["type"] == "system"
        && event["subtype"] == "init"
        && event["session_id"].as_str() == Some(session)
        && event["claude_code_version"]
            .as_str()
            .is_some_and(audited_version)
        && event["capabilities"]
            .as_array()
            .is_some_and(|caps| caps.iter().any(|cap| cap == "msg_lifecycle_v1"))
}
async fn write_input(stdin: &mut Option<ChildStdin>, value: &Value) -> std::io::Result<()> {
    let stdin = stdin.as_mut().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Claude stdin is closed")
    })?;
    let line = format!("{value}\n");
    stdin.write_all(line.as_bytes()).await?;
    stdin.flush().await
}

impl CliDriver {
    pub(super) async fn supports_native_steering(&self) -> bool {
        let mut command = crate::process::tokio_command(&self.executable);
        command
            .arg("--version")
            .current_dir(&self.working_dir)
            .envs(&self.runtime_env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        match tokio::time::timeout(Duration::from_secs(3), command.output()).await {
            Ok(Ok(output)) if output.status.success() => {
                audited_version(&String::from_utf8_lossy(&output.stdout))
            }
            _ => false,
        }
    }
    pub(super) async fn set_native_ready(&self, ready: bool, initial_uuid: &str) {
        let mut state = self.state.write().await;
        state.native_steer_v1_available = ready;
        state.native_steer_v1_turn_id = ready.then(|| {
            format!(
                "{}:{}:host:claude:{initial_uuid}",
                self.connection_id,
                state.conversation_id.unwrap_or_default()
            )
        });
    }
    pub(super) async fn pump_native(
        &self,
        stdout: tokio::process::ChildStdout,
        stdin: ChildStdin,
        session_id: &str,
        initial_uuid: &str,
        commands: &mut mpsc::Receiver<ConnectionCommand>,
    ) -> TurnEnd {
        let mut stdin = Some(stdin);
        let mut lines = BufReader::new(stdout).lines();
        let mut mapper = StreamMapper::new(Some(session_id.into()), Some(self.working_dir.clone()));
        let mut receipts = Receipts::new(session_id, initial_uuid);
        let mut initial_started = false;
        let mut compatible = false;
        let mut initial_terminal = false;
        let mut final_terminal_ready = false;
        let mut unparsable = 0;
        let mut deferred_finish: Option<TurnFinish> = None;
        let mut receipt_deadline: Option<tokio::time::Instant> = None;
        let mut background_deadline: Option<tokio::time::Instant> = None;
        loop {
            let deadline = match (receipt_deadline, background_deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                _ => None,
            };
            tokio::select! {
                _ = async move { match deadline { Some(at)=>tokio::time::sleep_until(at).await, None=>std::future::pending::<()>().await } } => {
                    receipts.unconfirmed();
                    receipt_deadline = None;
                    if background_deadline.is_some_and(|at| at <= tokio::time::Instant::now()) {
                        let mut finish = deferred_finish.take().unwrap_or(TurnFinish {stop_reason:STOP_UNKNOWN.into(),error:None});
                        finish.error = Some(CliTurnError {code:"cli_background_timeout",message:"Background tasks were still running and were stopped".into(),details:None});
                        return TurnEnd::Finished(finish);
                    }
                    if final_terminal_ready {
                        if let Some(finish)=deferred_finish.take() { return TurnEnd::Finished(finish); }
                    }
                }
                line = lines.next_line() => match line {
                    Ok(Some(line)) => {
                        let Ok(value)=serde_json::from_str::<Value>(&line) else {
                            unparsable += 1;
                            if unparsable >= MAX_CONSECUTIVE_UNPARSABLE { return TurnEnd::Protocol(truncate(&line, PROTOCOL_SAMPLE_BYTES)); }
                            continue;
                        };
                        unparsable = 0;
                        if value["session_id"].as_str().is_some_and(|sid| sid != session_id) {
                            if value["type"] == "system" && value["subtype"] == "init" {
                                compatible=false;
                                self.set_native_ready(false, initial_uuid).await;
                                // ChildStdin::shutdown is a no-op on Tokio's
                                // Unix pipe. Dropping the handle delivers EOF.
                                drop(stdin.take());
                            }
                            continue;
                        }
                        if value["type"] == "command_lifecycle" && value["command_uuid"] == initial_uuid && value["state"] == "started" { initial_started=true; }
                        if value["type"] == "system" && value["subtype"] == "init" {
                            compatible=stdin.is_some() && supports_init(&value,session_id);
                            if !compatible { drop(stdin.take()); }
                        }
                        self.set_native_ready(compatible && initial_started && !initial_terminal, initial_uuid).await;
                        for (blocks,reply) in receipts.observe(&value) {
                            record_prompt(self.agent_type,session_id,&map_prompt_blocks(blocks)).await;
                            let _=reply.send(Ok(SteerOutcome::Injected));
                        }
                        receipts.observe_cancel(&value);
                        if receipt_deadline.is_some() && receipts.is_empty() {
                            receipt_deadline=None;
                            if final_terminal_ready {
                                if let Some(finish)=deferred_finish.take() { return TurnEnd::Finished(finish); }
                            }
                        }
                        match mapper.map_line(&line) {
                            LineOutcome::Events(events) => for event in events { self.emit_recorded(event,session_id).await; },
                            LineOutcome::Finished {events,finish} => {
                                if receipts.context_only_result(&value) { continue; }
                                for event in events { self.emit_recorded(event,session_id).await; }
                                // An input with shouldQuery:false can emit an
                                // empty context-only result after A settled.
                                // Keep A's real terminal result in that case.
                                if !initial_terminal || mapper.background_pending() || value["num_turns"].as_u64().unwrap_or(0)>0 { deferred_finish=Some(finish); }
                                initial_terminal=true;
                                self.set_native_ready(false, initial_uuid).await;
                                if !receipts.is_empty() {
                                    for request in receipts.cancel_unconsumed() {
                                        if write_input(&mut stdin,&request).await.is_err() { receipts.unconfirmed(); }
                                    }
                                    receipt_deadline=Some(tokio::time::Instant::now()+RECEIPT_GRACE);
                                }
                                final_terminal_ready = !mapper.background_pending();
                                if !final_terminal_ready {
                                    background_deadline.get_or_insert(tokio::time::Instant::now()+self.background_wait());
                                    continue;
                                }
                                if receipts.is_empty() { return TurnEnd::Finished(deferred_finish.take().expect("result has terminal finish")); }
                            }
                            LineOutcome::Unparsable => {}
                        }
                    }
                    Ok(None)|Err(_) => return deferred_finish.take().map_or(TurnEnd::Eof,TurnEnd::Finished),
                },
                command=commands.recv()=>match command {
                    Some(ConnectionCommand::NativeSteer {expected_turn_id,blocks,reply,..})=>{
                        let ready=compatible && initial_started && !initial_terminal && crate::acp::steering::matches_turn(&*self.state.read().await,&expected_turn_id);
                        if !ready { let _=reply.send(Err(AcpError::NoActiveTurn));continue; }
                        if let Some(input)=receipts.enqueue(blocks,reply) {
                            if write_input(&mut stdin,&input).await.is_err() { receipts.unconfirmed(); }
                        }
                    }
                    Some(ConnectionCommand::Cancel)=>return TurnEnd::Cancelled,
                    Some(ConnectionCommand::Disconnect)|None=>return TurnEnd::Disconnected,
                    Some(ConnectionCommand::Prompt {..})=>tracing::warn!("[CLI][claude] rejected in-turn Prompt; admission gate must prevent it"),
                    Some(other)=>reject_unsupported(other),
                },
            }
        }
    }
}

#[cfg(all(test, unix))]
#[path = "claude_steering_tests.rs"]
mod tests;
