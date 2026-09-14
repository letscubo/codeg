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
            session_id: conn.session_id,
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
