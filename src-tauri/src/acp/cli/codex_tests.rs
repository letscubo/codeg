//! End-to-end Codex CLI driver tests against a fake `codex app-server` shell
//! script: it logs every request line and answers by method, replaying the
//! canned `thread.json` / `turn.jsonl` the test wrote.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::broadcast;

use super::dsh_tests::transcript_root;
use super::CliConnectRequest;
use crate::acp::connection::ConnectionCommand;
use crate::acp::manager::ConnectionManager;
use crate::acp::types::{EventEnvelope, PromptInputBlock};
use crate::acp_transcript::EntryKind;
use crate::models::agent::AgentType;
use crate::web::event_bridge::EventEmitter;

/// serde_json writes compact objects with sorted keys, so `"method":"<m>"`
/// matches each request without a JSON parser in sh.
const FAKE_CODEX: &str = r#"#!/bin/sh
printf '%s\n' "$@" > "$FAKE_DIR/args.txt"
: > "$FAKE_DIR/requests.jsonl"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$FAKE_DIR/requests.jsonl"
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"userAgent":"fake-codex"}}' ;;
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*) cat "$FAKE_DIR/thread.json" ;;
    *'"method":"turn/start"'*) cat "$FAKE_DIR/turn.jsonl" ;;
    *'"method":"turn/interrupt"'*)
      echo '{"id":4,"result":{}}'
      echo '{"method":"turn/completed","params":{"threadId":"TH","turn":{"id":"T1","status":"interrupted","error":null}}}' ;;
  esac
done
"#;

const THREAD_ID: &str = "01a0b54e-568c-7310-aa4d-f777c3663fb7";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Harness {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    manager: ConnectionManager,
    connection_id: String,
}

impl Harness {
    async fn new(session_id: Option<&str>) -> Self {
        let _ = transcript_root();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let executable = dir.join("fake-codex");
        std::fs::write(&executable, FAKE_CODEX).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert("FAKE_DIR".to_string(), dir.display().to_string());
        runtime_env.insert(
            "CODEX_HOME".to_string(),
            dir.join("codex-home").display().to_string(),
        );
        let manager = ConnectionManager::new();
        let conn = manager
            .create_or_reuse_cli_connection(
                CliConnectRequest {
                    agent_type: AgentType::Codex,
                    working_dir: dir.clone(),
                    session_id: session_id.map(str::to_string),
                    runtime_env,
                    executable,
                    provider: None,
                    model: Some("gpt-5.4-mini".to_string()),
                    owner_window_label: None,
                },
                EventEmitter::Noop,
            )
            .await
            .unwrap();
        if session_id.is_none() {
            assert_eq!(
                conn.session_id, None,
                "codex mints the thread id on the first turn"
            );
        }
        Self {
            _tmp: tmp,
            dir,
            manager,
            connection_id: conn.connection_id,
        }
    }

    fn thread(&self, response: Value) {
        std::fs::write(self.dir.join("thread.json"), format!("{response}\n")).unwrap();
    }

    fn turn(&self, lines: &[Value]) {
        let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(self.dir.join("turn.jsonl"), body).unwrap();
    }

    fn requests(&self) -> Vec<Value> {
        std::fs::read_to_string(self.dir.join("requests.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
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

    async fn external_id(&self) -> Option<String> {
        let state = self.manager.get_state(&self.connection_id).await.unwrap();
        let id = state.read().await.external_id.clone();
        id
    }

    async fn child_pid(&self) -> u32 {
        let conns = self.manager.connections.lock().await;
        conns[&self.connection_id].child_pid.load(Ordering::SeqCst)
    }
}

fn thread_ok() -> Value {
    json!({"id":2,"result":{"thread":{"id":THREAD_ID,"status":{"type":"idle"}}}})
}

fn happy_turn(text: &str) -> Vec<Value> {
    let t = |method: &str, params: Value| json!({ "method": method, "params": params });
    let mut lines = vec![
        json!({"id":3,"result":{"turn":{"id":"T1","status":"inProgress"}}}),
        t(
            "turn/started",
            json!({"threadId":THREAD_ID,"turn":{"id":"T1"}}),
        ),
        t(
            "item/started",
            json!({"turnId":"T1","item":{"type":"commandExecution","id":"c1","command":"/bin/bash -lc 'ls'","commandActions":[{"command":"ls"}],"cwd":"/ws","status":"inProgress"}}),
        ),
        t(
            "item/completed",
            json!({"turnId":"T1","item":{"type":"commandExecution","id":"c1","command":"/bin/bash -lc 'ls'","commandActions":[{"command":"ls"}],"cwd":"/ws","status":"completed","aggregatedOutput":"a\n","exitCode":0}}),
        ),
        t(
            "item/started",
            json!({"turnId":"T1","item":{"type":"agentMessage","id":"m1","text":""}}),
        ),
    ];
    for chunk in text.chars().collect::<Vec<_>>().chunks(2) {
        lines.push(t(
            "item/agentMessage/delta",
            json!({"turnId":"T1","itemId":"m1","delta":chunk.iter().collect::<String>()}),
        ));
    }
    lines.extend([
        t("item/completed", json!({"turnId":"T1","item":{"type":"agentMessage","id":"m1","text":text}})),
        t("thread/tokenUsage/updated", json!({"turnId":"T1","tokenUsage":{"last":{"inputTokens":10000,"outputTokens":50},"modelContextWindow":258400}})),
        t("turn/completed", json!({"threadId":THREAD_ID,"turn":{"id":"T1","status":"completed","error":null}})),
    ]);
    lines
}

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

fn types(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| e["type"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn first_turn_starts_a_thread_streams_and_binds_the_thread_id() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(None).await;
    h.thread(thread_ok());
    h.turn(&happy_turn("done!"));
    let mut rx = h.subscribe().await;
    h.prompt("hello").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;

    // Protocol order and parameters.
    let requests = h.requests();
    let methods: Vec<&str> = requests
        .iter()
        .filter_map(|r| r["method"].as_str())
        .collect();
    assert_eq!(
        methods,
        vec!["initialize", "initialized", "thread/start", "turn/start"]
    );
    let start = &requests[2]["params"];
    assert_eq!(start["approvalPolicy"], "never");
    assert_eq!(start["sandbox"], "danger-full-access");
    assert_eq!(start["model"], "gpt-5.4-mini");
    let turn = &requests[3]["params"];
    assert_eq!(turn["threadId"], THREAD_ID);
    assert_eq!(
        turn["input"][0],
        json!({"type":"text","text":"hello","text_elements":[]})
    );
    assert_eq!(h.args(), vec!["app-server".to_string()]);

    let kinds = types(&events);
    assert!(kinds.contains(&"session_started".to_string()));
    assert!(kinds.contains(&"tool_call".to_string()));
    assert!(kinds.contains(&"tool_call_update".to_string()));
    let deltas = kinds.iter().filter(|k| *k == "content_delta").count();
    assert!(deltas >= 3, "text streams as several deltas, got {deltas}");
    assert!(kinds.contains(&"usage_update".to_string()));
    let complete = events.last().unwrap();
    assert_eq!(complete["stop_reason"], "end_turn");
    assert_eq!(complete["session_id"], THREAD_ID);
    assert_eq!(h.external_id().await.as_deref(), Some(THREAD_ID));
    assert_eq!(h.child_pid().await, 0);

    // Recorded transcript: prompt + updates + turn end with the model.
    let transcript = crate::acp_transcript::read_transcript_in(
        &crate::paths::codeg_acp_transcripts_root(),
        crate::acp::registry::registry_id_for(AgentType::Codex),
        THREAD_ID,
    );
    let kinds: Vec<EntryKind> = transcript.entries.iter().map(|e| e.k).collect();
    assert_eq!(kinds[0], EntryKind::Prompt);
    assert!(kinds.contains(&EntryKind::Update));
    assert_eq!(*kinds.last().unwrap(), EntryKind::TurnEnd);
    assert_eq!(
        transcript.entries.last().unwrap().p["model"],
        "gpt-5.4-mini"
    );
}

#[tokio::test]
async fn continued_session_resumes_the_thread() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(Some(THREAD_ID)).await;
    h.thread(thread_ok());
    h.turn(&happy_turn("again"));
    let mut rx = h.subscribe().await;
    h.prompt("second").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let requests = h.requests();
    assert_eq!(requests[2]["method"], "thread/resume");
    assert_eq!(requests[2]["params"]["threadId"], THREAD_ID);
    assert!(
        !types(&events).contains(&"session_started".to_string()),
        "same id, no re-announce"
    );
    assert_eq!(events.last().unwrap()["stop_reason"], "end_turn");
}

#[tokio::test]
async fn resume_failure_is_surfaced_before_turn_complete() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(Some(THREAD_ID)).await;
    h.thread(
        json!({"id":2,"error":{"code":-32600,"message":"thread not found: no rollout for id"}}),
    );
    h.turn(&[]);
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let error = events
        .iter()
        .find(|e| e["type"] == "error")
        .expect("error event");
    assert_eq!(error["code"], "cli_resume_failed");
    assert_eq!(events.last().unwrap()["stop_reason"], "unknown");
}

#[tokio::test]
async fn failed_turn_reports_the_upstream_message() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(None).await;
    h.thread(thread_ok());
    h.turn(&[
        json!({"id":3,"result":{"turn":{"id":"T1","status":"inProgress"}}}),
        json!({"method":"error","params":{"turnId":"T1","willRetry":false,"error":{"message":"{\"error\":{\"message\":\"The encrypted content could not be verified.\"}}"}}}),
        json!({"method":"turn/completed","params":{"turn":{"id":"T1","status":"failed","error":null}}}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let error = events
        .iter()
        .find(|e| e["type"] == "error")
        .expect("error event");
    assert_eq!(error["code"], "cli_api_error");
    assert_eq!(
        error["message"],
        "The encrypted content could not be verified."
    );
}

#[tokio::test]
async fn cancel_interrupts_the_running_turn() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(None).await;
    h.thread(thread_ok());
    // The turn starts and streams, but never completes on its own.
    h.turn(&[
        json!({"id":3,"result":{"turn":{"id":"T1","status":"inProgress"}}}),
        json!({"method":"item/agentMessage/delta","params":{"turnId":"T1","itemId":"m1","delta":"partial"}}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("long task").await;
    collect_until(&mut rx, |e| e["type"] == "content_delta").await;
    h.manager.connections.lock().await[&h.connection_id]
        .cmd_tx
        .send(ConnectionCommand::Cancel)
        .await
        .unwrap();
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    assert_eq!(events.last().unwrap()["stop_reason"], "cancelled");
    let interrupt = h
        .requests()
        .into_iter()
        .find(|r| r["method"] == "turn/interrupt")
        .expect("turn/interrupt sent");
    assert_eq!(
        interrupt["params"],
        json!({"threadId": THREAD_ID, "turnId": "T1"})
    );
}

impl Harness {
    fn args(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.join("args.txt"))
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect()
    }
}
