//! End-to-end driver tests against a fake `claude` shell script.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::broadcast;

use super::{CliConnectError, CliConnectRequest};
use crate::acp::connection::ConnectionCommand;
use crate::acp::manager::ConnectionManager;
use crate::acp::session_state::ConnectionTransport;
use crate::acp::types::{EventEnvelope, PromptInputBlock};
use crate::models::agent::AgentType;
use crate::web::event_bridge::EventEmitter;

/// Records argv and stdin, replays `stdout.jsonl`, then exits / sleeps as told.
const FAKE_CLI: &str = r#"#!/bin/sh
printf '%s\n' "$@" > "$FAKE_DIR/args.txt"
cat > "$FAKE_DIR/stdin.txt"
cat "$FAKE_DIR/stdout.jsonl" 2>/dev/null
if [ -n "$FAKE_STDERR" ]; then echo "$FAKE_STDERR" >&2; fi
if [ -n "$FAKE_SLEEP" ]; then exec sleep "$FAKE_SLEEP"; fi
exit "${FAKE_EXIT:-0}"
"#;

struct Harness {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    manager: ConnectionManager,
    connection_id: String,
    session_id: String,
}

impl Harness {
    async fn new(env: &[(&str, &str)]) -> Self {
        // The driver records codeg's transcript: keep it in a temp CODEG_HOME.
        let _ = super::dsh_tests::transcript_root();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let executable = dir.join("fake-claude");
        std::fs::write(&executable, FAKE_CLI).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert("FAKE_DIR".to_string(), dir.display().to_string());
        runtime_env.insert(
            "CLAUDE_CONFIG_DIR".to_string(),
            dir.join("claude").display().to_string(),
        );
        for (k, v) in env {
            runtime_env.insert(k.to_string(), v.to_string());
        }
        let manager = ConnectionManager::new();
        let conn = manager
            .create_or_reuse_cli_connection(
                CliConnectRequest {
                    agent_type: AgentType::ClaudeCode,
                    working_dir: dir.clone(),
                    session_id: None,
                    runtime_env,
                    executable,
                    provider: None,
                    model: None,
                    owner_window_label: None,
                },
                EventEmitter::Noop,
            )
            .await
            .unwrap();
        Self {
            _tmp: tmp,
            dir,
            manager,
            connection_id: conn.connection_id,
            session_id: conn
                .session_id
                .expect("claude connections pre-assign a session id"),
        }
    }

    fn stdout(&self, lines: &[Value]) {
        let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(self.dir.join("stdout.jsonl"), body).unwrap();
    }

    fn args(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.join("args.txt"))
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect()
    }

    async fn subscribe(&self) -> broadcast::Receiver<Arc<EventEnvelope>> {
        let state = self.manager.get_state(&self.connection_id).await.unwrap();
        let rx = state.read().await.event_stream().subscribe();
        rx
    }

    async fn prompt(&self, text: &str) {
        self.manager
            .send_prompt(
                &self.connection_id,
                vec![PromptInputBlock::Text {
                    text: text.to_string(),
                }],
            )
            .await
            .unwrap();
    }

    async fn turn_in_flight(&self) -> bool {
        let state = self.manager.get_state(&self.connection_id).await.unwrap();
        let in_flight = state.read().await.turn_in_flight;
        in_flight
    }
}

fn init_line(session_id: &str) -> Value {
    json!({"type":"system","subtype":"init","session_id":session_id,"model":"claude-opus-5"})
}

/// Collect events up to and including the first one matching `until`.
async fn collect_until(
    rx: &mut broadcast::Receiver<Arc<EventEnvelope>>,
    until: impl Fn(&Value) -> bool,
) -> Vec<Value> {
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let envelope = rx.recv().await.expect("event stream closed");
            let value = serde_json::to_value(envelope.as_ref()).unwrap();
            let done = until(&value);
            out.push(value);
            if done {
                break;
            }
        }
    })
    .await
    .expect("timed out waiting for events");
    out
}

fn is_status(status: &'static str) -> impl Fn(&Value) -> bool {
    move |v| v["type"] == "status_changed" && v["status"] == status
}

fn types(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|e| e["type"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_turn_streams_events_and_returns_to_connected() {
    let h = Harness::new(&[]).await;
    h.stdout(&[
        init_line(&h.session_id),
        json!({"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"parent_tool_use_id":null}),
        json!({"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"pong"}},"parent_tool_use_id":null}),
        json!({"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"pong"}]},"parent_tool_use_id":null}),
        json!({"type":"result","subtype":"success","is_error":false,"stop_reason":"end_turn","result":"pong",
            "usage":{"input_tokens":10,"output_tokens":2},"modelUsage":{"claude-opus-5":{"contextWindow":200000}}}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    let events = collect_until(&mut rx, is_status("connected")).await;

    assert_eq!(
        types(&events),
        vec![
            "status_changed",
            "content_delta",
            "usage_update",
            "turn_complete",
            "status_changed"
        ]
    );
    assert_eq!(events[0]["status"], "prompting");
    assert_eq!(events[3]["stop_reason"], "end_turn");
    assert_eq!(events[3]["session_id"], h.session_id.as_str());
    assert!(!h.turn_in_flight().await);

    let args = h.args();
    assert!(args
        .windows(2)
        .any(|w| w == ["--session-id", h.session_id.as_str()]));
    assert!(args
        .windows(2)
        .any(|w| w == ["--permission-mode", "bypassPermissions"]));
    assert!(!args.iter().any(|a| a == "--resume"));
    let stdin: Value =
        serde_json::from_str(&std::fs::read_to_string(h.dir.join("stdin.txt")).unwrap()).unwrap();
    assert_eq!(
        stdin,
        json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}})
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_transcript_is_resumed_and_the_model_is_passed() {
    let h = Harness::new(&[]).await;
    let project = h.dir.join("claude/projects/-tmp-x");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join(format!("{}.jsonl", h.session_id)), "{}\n").unwrap();
    h.manager
        .get_state(&h.connection_id)
        .await
        .unwrap()
        .write()
        .await
        .cli_model = Some("claude-sonnet-5".to_string());
    h.stdout(&[json!({"type":"result","subtype":"success","is_error":false})]);
    let mut rx = h.subscribe().await;
    h.prompt("again").await;
    collect_until(&mut rx, |v| v["type"] == "turn_complete").await;

    let args = h.args();
    assert!(args
        .windows(2)
        .any(|w| w == ["--resume", h.session_id.as_str()]));
    assert!(args.windows(2).any(|w| w == ["--model", "claude-sonnet-5"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_early_exit_reports_cli_exited_and_frees_the_turn_gate() {
    let h = Harness::new(&[("FAKE_EXIT", "3"), ("FAKE_STDERR", "boom")]).await;
    h.stdout(&[init_line(&h.session_id)]);
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    let events = collect_until(&mut rx, is_status("connected")).await;

    let error = events
        .iter()
        .find(|e| e["type"] == "error")
        .expect("error event");
    assert_eq!(error["code"], "cli_exited");
    assert!(error["message"].as_str().unwrap().contains("exit code 3"));
    assert!(error["details"].as_str().unwrap().contains("boom"));
    let complete = events
        .iter()
        .find(|e| e["type"] == "turn_complete")
        .unwrap();
    assert_eq!(complete["stop_reason"], "unknown");
    assert!(!h.turn_in_flight().await);

    // The gate is free again: a second prompt is admitted.
    h.prompt("retry").await;
    collect_until(&mut rx, |v| v["type"] == "turn_complete").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resume_failure_maps_to_cli_resume_failed() {
    let h = Harness::new(&[("FAKE_EXIT", "1")]).await;
    h.stdout(&[
        json!({"type":"result","subtype":"error_during_execution","is_error":true,
        "errors":["No conversation found with session ID: x"]}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    let events = collect_until(&mut rx, |v| v["type"] == "turn_complete").await;
    let error = events.iter().find(|e| e["type"] == "error").unwrap();
    assert_eq!(error["code"], "cli_resume_failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_kills_the_running_cli_and_completes_the_turn() {
    let h = Harness::new(&[("FAKE_SLEEP", "30")]).await;
    h.stdout(&[init_line(&h.session_id)]);
    let mut rx = h.subscribe().await;
    h.prompt("long").await;
    collect_until(&mut rx, is_status("prompting")).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let cmd_tx = h
        .manager
        .connections
        .lock()
        .await
        .get(&h.connection_id)
        .unwrap()
        .cmd_tx
        .clone();
    let started = std::time::Instant::now();
    cmd_tx.send(ConnectionCommand::Cancel).await.unwrap();
    let events = collect_until(&mut rx, is_status("connected")).await;

    let complete = events
        .iter()
        .find(|e| e["type"] == "turn_complete")
        .unwrap();
    assert_eq!(complete["stop_reason"], "cancelled");
    assert!(!events.iter().any(|e| e["type"] == "error"));
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(!h.turn_in_flight().await);
    let pid = h
        .manager
        .connections
        .lock()
        .await
        .get(&h.connection_id)
        .unwrap()
        .child_pid
        .load(Ordering::SeqCst);
    assert_eq!(pid, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_stops_the_driver_and_drops_the_connection() {
    let h = Harness::new(&[]).await;
    let mut rx = h.subscribe().await;
    h.manager.disconnect(&h.connection_id).await.unwrap();
    collect_until(&mut rx, is_status("disconnected")).await;
    assert_eq!(h.manager.connection_transport(&h.connection_id).await, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_session_is_reused_in_place_and_locked_elsewhere() {
    let h = Harness::new(&[]).await;
    assert_eq!(
        h.manager.connection_transport(&h.connection_id).await,
        Some(ConnectionTransport::Cli)
    );
    let request = |dir: PathBuf| CliConnectRequest {
        agent_type: AgentType::ClaudeCode,
        working_dir: dir,
        session_id: Some(h.session_id.clone()),
        runtime_env: BTreeMap::new(),
        executable: PathBuf::from("/bin/false"),
        provider: None,
        model: None,
        owner_window_label: None,
    };

    let reused = h
        .manager
        .create_or_reuse_cli_connection(request(h.dir.clone()), EventEmitter::Noop)
        .await
        .unwrap();
    assert!(reused.reused);
    assert_eq!(reused.connection_id, h.connection_id);

    let locked = h
        .manager
        .create_or_reuse_cli_connection(request(PathBuf::from("/elsewhere")), EventEmitter::Noop)
        .await;
    assert_eq!(
        locked,
        Err(CliConnectError::SessionLocked {
            connection_id: h.connection_id.clone()
        })
    );
}

/// Claude Code's history is read from codeg's own transcript, so a CLI turn
/// must record header, prompt, the streamed reply and the turn end — and
/// report its duration like the ACP path does.
#[tokio::test]
async fn a_turn_is_recorded_to_codegs_transcript() {
    let h = Harness::new(&[]).await;
    h.stdout(&[
        init_line(&h.session_id),
        json!({"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"parent_tool_use_id":null}),
        json!({"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"pong"}},"parent_tool_use_id":null}),
        json!({"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"pong"}]},"parent_tool_use_id":null}),
        json!({"type":"result","subtype":"success","is_error":false,"stop_reason":"end_turn","result":"pong",
            "usage":{"input_tokens":10,"output_tokens":2},"modelUsage":{"claude-opus-5":{"contextWindow":200000}}}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("ping").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    assert!(events.last().unwrap()["duration_ms"].as_u64().is_some());
    assert!(
        !h.args().iter().any(|a| a == "--mcp-config"),
        "no companion without delegation"
    );

    let transcript = crate::acp_transcript::read_transcript_in(
        &crate::paths::codeg_acp_transcripts_root(),
        crate::acp::registry::registry_id_for(AgentType::ClaudeCode),
        &h.session_id,
    );
    assert!(transcript.header.is_some(), "header recorded");
    let kinds: Vec<crate::acp_transcript::EntryKind> =
        transcript.entries.iter().map(|e| e.k).collect();
    assert_eq!(
        kinds.first(),
        Some(&crate::acp_transcript::EntryKind::Prompt)
    );
    assert!(kinds.contains(&crate::acp_transcript::EntryKind::Update));
    assert_eq!(
        kinds.last(),
        Some(&crate::acp_transcript::EntryKind::TurnEnd)
    );
    assert!(transcript.entries[0].p.to_string().contains("ping"));
}

/// `spawn_agent` (acp_connect, channels, work tasks, automations) must give
/// Claude Code a CLI connection that keeps the caller's owner label.
#[tokio::test]
async fn spawn_agent_gives_claude_code_a_cli_connection() {
    let _ = super::dsh_tests::transcript_root();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let executable = dir.join("fake-claude");
    std::fs::write(&executable, FAKE_CLI).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut runtime_env = BTreeMap::new();
    runtime_env.insert(
        "CLAUDE_CODE_EXECUTABLE".to_string(),
        executable.display().to_string(),
    );
    let manager = ConnectionManager::new();
    let resumed = "0b0f7c1e-2b44-4e8e-9a51-0c6c3f0b7d21";
    let id = manager
        .spawn_agent(
            AgentType::ClaudeCode,
            Some(dir.display().to_string()),
            Some(resumed.to_string()),
            runtime_env,
            "automation".to_string(),
            EventEmitter::Noop,
            None,
            BTreeMap::new(),
        )
        .await
        .expect("claude connects through the CLI transport");
    assert_eq!(
        manager.connection_transport(&id).await,
        Some(ConnectionTransport::Cli)
    );
    let state = manager.get_state(&id).await.unwrap();
    assert_eq!(
        state.read().await.external_id.as_deref(),
        Some(resumed),
        "an ACP-era Claude session id is the same id the CLI resumes"
    );
    {
        let conns = manager.connections.lock().await;
        assert_eq!(conns[&id].owner_window_label, "automation");
    }
    manager.disconnect(&id).await.unwrap();
}

/// Background work keeps `claude -p` alive past the model's turn: it prints a
/// `result`, waits for the task, feeds the notification back, runs a follow-up
/// turn and prints a second `result` (claude 2.1.276 on A3). The codeg turn must
/// span both — one `turn_complete`, after the follow-up's text — instead of
/// ending at the first `result` and killing the process with its background task.
#[tokio::test]
async fn a_background_task_keeps_the_turn_open_until_its_follow_up() {
    let h = Harness::new(&[]).await;
    let tool = "toolu_bg";
    h.stdout(&[
        init_line(&h.session_id),
        json!({"type":"assistant","message":{"id":"m1","content":[{"type":"tool_use","id":tool,"name":"Agent","input":{"description":"sleep","prompt":"sleep 25","run_in_background":true}}]},"parent_tool_use_id":null}),
        json!({"type":"system","subtype":"background_tasks_changed","tasks":[{"task_id":"a01"}]}),
        json!({"type":"system","subtype":"task_started","task_id":"a01","tool_use_id":tool,"is_backgrounded":true}),
        json!({"type":"user","message":{"role":"user","content":[{"tool_use_id":tool,"type":"tool_result","content":"Async agent launched successfully."}]},"parent_tool_use_id":null}),
        json!({"type":"assistant","message":{"id":"m2","content":[{"type":"text","text":"started"}]},"parent_tool_use_id":null}),
        json!({"type":"result","subtype":"success","is_error":false,"stop_reason":"end_turn","result":"started"}),
        // The sub-agent talking to itself: live-only, never transcript.
        json!({"type":"assistant","message":{"id":"s1","content":[{"type":"text","text":"SIDECHAIN"}]},"parent_tool_use_id":tool}),
        json!({"type":"system","subtype":"background_tasks_changed","tasks":[]}),
        json!({"type":"system","subtype":"task_notification","task_id":"a01","tool_use_id":tool,"status":"completed","summary":"SUBAGENT_DONE"}),
        json!({"type":"system","subtype":"init","session_id":h.session_id,"model":"claude-opus-5"}),
        json!({"type":"assistant","message":{"id":"m3","content":[{"type":"text","text":"done"}]},"parent_tool_use_id":null}),
        json!({"type":"result","subtype":"success","is_error":false,"stop_reason":"end_turn","result":"done"}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("go").await;
    let events = collect_until(&mut rx, is_status("connected")).await;

    let completes: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "turn_complete")
        .collect();
    assert_eq!(completes.len(), 1, "one codeg turn for both claude turns");
    let texts: Vec<&str> = events
        .iter()
        .filter(|e| e["type"] == "content_delta")
        .filter_map(|e| e["text"].as_str())
        .collect();
    assert!(
        texts.contains(&"started") && texts.contains(&"done"),
        "{texts:?}"
    );
    let done_at = events.iter().position(|e| e["text"] == "done").unwrap();
    let complete_at = events
        .iter()
        .position(|e| e["type"] == "turn_complete")
        .unwrap();
    assert!(done_at < complete_at, "the follow-up lands inside the turn");
    let card = events
        .iter()
        .filter(|e| e["type"] == "tool_call_update" && e["tool_call_id"] == tool)
        .map(|e| e["status"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(card.last(), Some(&"completed"), "{card:?}");
    assert!(card.contains(&"in_progress"), "{card:?}");

    let transcript = crate::acp_transcript::read_transcript_in(
        &crate::paths::codeg_acp_transcripts_root(),
        crate::acp::registry::registry_id_for(AgentType::ClaudeCode),
        &h.session_id,
    );
    let recorded =
        serde_json::to_string(&transcript.entries.iter().map(|e| &e.p).collect::<Vec<_>>())
            .unwrap();
    assert!(recorded.contains("done"));
    assert!(
        !recorded.contains("SIDECHAIN"),
        "sub-agent prose is live-only"
    );
    let turn_ends = transcript
        .entries
        .iter()
        .filter(|e| e.k == crate::acp_transcript::EntryKind::TurnEnd)
        .count();
    assert_eq!(turn_ends, 1);
}

/// If the process exits after a `result` while the mapper still thought work
/// was pending (e.g. a task it never heard settle), that result ends the turn —
/// not a spurious `cli_exited`.
#[tokio::test]
async fn eof_after_a_deferred_result_finishes_normally() {
    let h = Harness::new(&[]).await;
    h.stdout(&[
        init_line(&h.session_id),
        json!({"type":"system","subtype":"task_started","task_id":"a01","tool_use_id":"toolu_bg","is_backgrounded":true}),
        json!({"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"started"}]},"parent_tool_use_id":null}),
        json!({"type":"result","subtype":"success","is_error":false,"stop_reason":"end_turn","result":"started"}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("go").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    assert_eq!(events.last().unwrap()["stop_reason"], "end_turn");
    assert!(!events.iter().any(|e| e["type"] == "error"));
}
