//! End-to-end DeepSeek CLI driver tests against a fake `dsh` shell script.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::broadcast;

use super::CliConnectRequest;
use crate::acp::connection::ConnectionCommand;
use crate::acp::manager::ConnectionManager;
use crate::acp::types::{EventEnvelope, PromptInputBlock};
use crate::acp_transcript::EntryKind;
use crate::models::agent::AgentType;
use crate::web::event_bridge::EventEmitter;

/// Records argv, replays `stdout.jsonl`, then sleeps / exits as told.
const FAKE_DSH: &str = r#"#!/bin/sh
printf '%s\n' "$@" > "$FAKE_DIR/args.txt"
cat "$FAKE_DIR/stdout.jsonl" 2>/dev/null
if [ -n "$FAKE_STDERR" ]; then echo "$FAKE_STDERR" >&2; fi
if [ -n "$FAKE_SLEEP" ]; then exec sleep "$FAKE_SLEEP"; fi
exit "${FAKE_EXIT:-0}"
"#;

const SID: &str = "session-11c2b268-f4fc-4eba-805a-ec1a49c08a8f";

/// The transcript root is process env (`CODEG_HOME`), not launch env, so the
/// driver tests point it at one temp dir for the whole process and run one at
/// a time.
static TRANSCRIPT_ROOT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) fn transcript_root() -> &'static PathBuf {
    TRANSCRIPT_ROOT.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("codeg-dsh-tests-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("CODEG_HOME", &dir);
        dir
    })
}

struct Harness {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    manager: ConnectionManager,
    connection_id: String,
}

impl Harness {
    async fn new(env: &[(&str, &str)]) -> Self {
        Self::resuming(env, None).await
    }

    async fn resuming(env: &[(&str, &str)], session_id: Option<&str>) -> Self {
        let _ = transcript_root();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let executable = dir.join("fake-dsh");
        std::fs::write(&executable, FAKE_DSH).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert("FAKE_DIR".to_string(), dir.display().to_string());
        runtime_env.insert(
            "DSH_HOME".to_string(),
            dir.join("dsh-home").display().to_string(),
        );
        for (k, v) in env {
            runtime_env.insert(k.to_string(), v.to_string());
        }
        let manager = ConnectionManager::new();
        let conn = manager
            .create_or_reuse_cli_connection(
                CliConnectRequest {
                    agent_type: AgentType::DeepSeek,
                    working_dir: dir.clone(),
                    session_id: session_id.map(str::to_string),
                    runtime_env,
                    executable,
                    provider: Some("myclaw".to_string()),
                    model: Some("kimi-k3".to_string()),
                    owner_window_label: None,
                },
                EventEmitter::Noop,
            )
            .await
            .unwrap();
        assert_eq!(conn.session_id, None, "dsh mints the id on the first turn");
        Self {
            _tmp: tmp,
            dir,
            manager,
            connection_id: conn.connection_id,
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

    async fn prompt_blocks(&self, blocks: Vec<PromptInputBlock>) {
        self.manager
            .send_prompt(&self.connection_id, blocks)
            .await
            .unwrap();
    }

    async fn prompt(&self, text: &str) {
        self.prompt_blocks(vec![PromptInputBlock::Text {
            text: text.to_string(),
        }])
        .await;
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

fn happy_turn(text: &str) -> Vec<Value> {
    vec![
        json!({"type":"session","sessionId":SID,"cwd":"/ws"}),
        json!({"type":"status","phase":"turn_start","turn":1}),
        json!({"type":"thinking","text":"thinking"}),
        json!({"type":"tool_call","callId":"c1","tool":"mcp__app-notion__notion-fetch","input":{"id":"self"}}),
        json!({"type":"tool_result","callId":"c1","status":"completed","result":"{\"ok\":true}"}),
        json!({"type":"status","phase":"step_end","turn":1,"step":1,"usage":{"inputTokens":8000,"outputTokens":100,"totalTokens":8100}}),
        json!({"type":"text","text":text}),
        json!({"type":"status","phase":"turn_end","turn":1,"reason":{"kind":"completed"}}),
        json!({"type":"final","text":text}),
    ]
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

fn types(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| e["type"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn first_turn_has_no_session_flag_and_binds_the_minted_id() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(&[]).await;
    h.stdout(&happy_turn("done"));
    let mut rx = h.subscribe().await;
    h.prompt("hello").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;

    let args = h.args();
    assert_eq!(&args[..2], &["--profile", "headless"]);
    assert_eq!(args[2], "--patch");
    assert!(args[3].ends_with(&format!("codeg-{}.patch.yml", h.connection_id)));
    assert_eq!(args[4], "--json");
    assert!(!args.contains(&"--session-id".to_string()));
    assert_eq!(&args[args.len() - 2..], &["--", "hello"]);

    let kinds = types(&events);
    assert!(kinds.contains(&"session_started".to_string()));
    assert!(kinds.contains(&"thinking".to_string()));
    assert!(kinds.contains(&"tool_call".to_string()));
    assert!(kinds.contains(&"tool_call_update".to_string()));
    assert!(kinds.contains(&"content_delta".to_string()));
    let complete = events.last().unwrap();
    assert_eq!(complete["stop_reason"], "end_turn");
    assert_eq!(complete["session_id"], SID);
    assert!(complete["duration_ms"].as_u64().is_some());
    assert_eq!(h.external_id().await.as_deref(), Some(SID));

    // Recorded transcript: header + prompt + updates + turn end.
    let transcript = crate::acp_transcript::read_transcript_in(
        &crate::paths::codeg_acp_transcripts_root(),
        "deepseek-acp",
        SID,
    );
    let kinds: Vec<EntryKind> = transcript.entries.iter().map(|e| e.k).collect();
    assert_eq!(kinds[0], EntryKind::Prompt);
    assert!(kinds.contains(&EntryKind::Update));
    assert_eq!(*kinds.last().unwrap(), EntryKind::TurnEnd);
    let turn_end = transcript.entries.last().unwrap();
    assert_eq!(turn_end.p["stopReason"], "end_turn");
    assert_eq!(turn_end.p["model"], "kimi-k3");

    // The patch file names the provider/model and the plugin.
    let patch = std::fs::read_to_string(&args[3]).unwrap();
    assert!(patch.contains("agent-default-model"));
    assert!(patch.contains("kimi-k3"));
    assert!(patch.contains("codeg-tool-search.mjs"));
    assert!(h
        .dir
        .join("dsh-home/plugins/codeg-tool-search.mjs")
        .is_file());
}

#[tokio::test]
async fn second_turn_continues_the_session() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(&[]).await;
    h.stdout(&happy_turn("one"));
    let mut rx = h.subscribe().await;
    h.prompt("first").await;
    collect_until(&mut rx, |e| e["type"] == "turn_complete").await;

    h.stdout(&happy_turn("two"));
    h.prompt("second").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let args = h.args();
    assert!(args.windows(2).any(|w| w == ["--session-id", SID]));
    assert!(
        !types(&events).contains(&"session_started".to_string()),
        "same id, no re-announce"
    );
    assert_eq!(events.last().unwrap()["stop_reason"], "end_turn");
}

#[tokio::test]
async fn image_blocks_are_written_to_the_workspace_and_referenced() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(&[]).await;
    h.stdout(&happy_turn("saw it"));
    let mut rx = h.subscribe().await;
    h.prompt_blocks(vec![
        PromptInputBlock::Text {
            text: "what is this".into(),
        },
        PromptInputBlock::Image {
            data: "aGVsbG8=".into(),
            mime_type: "image/png".into(),
            uri: None,
        },
    ])
    .await;
    collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let args = h.args();
    let sep = args.iter().position(|a| a == "--").unwrap();
    let prompt = args[sep + 1..].join("\n");
    assert!(prompt.starts_with("what is this\n\n[附件: "));
    let path = prompt
        .trim_start_matches("what is this\n\n[附件: ")
        .trim_end_matches(']');
    assert_eq!(std::fs::read(path).unwrap(), b"hello");
    assert!(path.starts_with(h.dir.join(".codeg/attachments").to_str().unwrap()));
}

#[tokio::test]
async fn runner_error_is_surfaced_before_turn_complete() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(&[("FAKE_EXIT", "1")]).await;
    h.stdout(&[
        json!({"type":"error","message":"unknown session id session-x: no stored Session"}),
    ]);
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let error = events
        .iter()
        .find(|e| e["type"] == "error")
        .expect("error event");
    assert_eq!(error["code"], "cli_resume_failed");
    assert_eq!(events.last().unwrap()["stop_reason"], "unknown");
    assert_eq!(h.child_pid().await, 0);
}

#[tokio::test]
async fn silent_exit_is_cli_exited_with_the_stderr_tail() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(&[("FAKE_EXIT", "1"), ("FAKE_STDERR", "dsh: boom")]).await;
    h.stdout(&[]);
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let error = events
        .iter()
        .find(|e| e["type"] == "error")
        .expect("error event");
    assert_eq!(error["code"], "cli_exited");
    assert!(error["details"].as_str().unwrap().contains("dsh: boom"));
}

#[tokio::test]
async fn cancel_terminates_the_process_and_ends_with_cancelled() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(&[("FAKE_SLEEP", "30")]).await;
    h.stdout(&[json!({"type":"session","sessionId":SID,"cwd":"/ws"})]);
    let mut rx = h.subscribe().await;
    h.prompt("hang").await;
    collect_until(&mut rx, |e| e["type"] == "session_started").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(h.child_pid().await > 0);
    h.manager.connections.lock().await[&h.connection_id]
        .cmd_tx
        .send(ConnectionCommand::Cancel)
        .await
        .unwrap();
    let events = collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    assert_eq!(events.last().unwrap()["stop_reason"], "cancelled");
    assert!(!types(&events).contains(&"error".to_string()));
    assert_eq!(h.child_pid().await, 0);
    let transcript = crate::acp_transcript::read_transcript_in(
        &crate::paths::codeg_acp_transcripts_root(),
        "deepseek-acp",
        SID,
    );
    assert_eq!(
        transcript.entries.last().unwrap().p["stopReason"],
        "cancelled"
    );
}

#[tokio::test]
async fn disconnect_removes_the_patch_file_and_the_connection() {
    let _guard = SERIAL.lock().await;
    let h = Harness::new(&[]).await;
    h.stdout(&happy_turn("x"));
    let mut rx = h.subscribe().await;
    h.prompt("hi").await;
    collect_until(&mut rx, |e| e["type"] == "turn_complete").await;
    let patch = PathBuf::from(&h.args()[3]);
    assert!(patch.is_file());
    h.manager.disconnect(&h.connection_id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.manager.get_state(&h.connection_id).await.is_some() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while patch.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("patch file removed on disconnect");
}

/// `spawn_agent` is the entry every codeg engine uses (acp_connect, chat
/// channels, work tasks, automations, delegation). DeepSeek has no ACP process
/// any more, so it must come back as a CLI connection that keeps the caller's
/// owner label, takes model + route from the agent env MyClaw pushes, and
/// treats an old bridge session id (bare uuid) as a fresh session.
#[tokio::test]
async fn spawn_agent_gives_deepseek_a_cli_connection() {
    let _serial = SERIAL.lock().await;
    let _ = transcript_root();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let executable = dir.join("fake-dsh");
    std::fs::write(
        &executable,
        "#!/bin/sh\ncase \" $* \" in *\" --help \"*) echo '--json --session-id'; exit 0;; esac\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut runtime_env = BTreeMap::new();
    runtime_env.insert(
        "DSH_EXECUTABLE".to_string(),
        executable.display().to_string(),
    );
    runtime_env.insert(
        "DSH_HOME".to_string(),
        dir.join("dsh-home").display().to_string(),
    );
    runtime_env.insert("DEEPSEEK_ACP_PROVIDER".to_string(), "myclaw".to_string());
    runtime_env.insert("DEEPSEEK_ACP_MODEL".to_string(), "kimi-k3".to_string());

    let manager = ConnectionManager::new();
    let id = manager
        .spawn_agent(
            AgentType::DeepSeek,
            Some(dir.display().to_string()),
            Some("11c2b268-f4fc-4eba-805a-ec1a49c08a8f".to_string()),
            runtime_env,
            "work_task".to_string(),
            EventEmitter::Noop,
            Some("plan".to_string()),
            BTreeMap::new(),
        )
        .await
        .expect("deepseek connects through the CLI transport");

    assert_eq!(
        manager.connection_transport(&id).await,
        Some(crate::acp::session_state::ConnectionTransport::Cli)
    );
    let state = manager.get_state(&id).await.unwrap();
    {
        let state = state.read().await;
        assert_eq!(state.external_id, None, "a bridge id is not resumed");
        assert_eq!(state.cli_model.as_deref(), Some("kimi-k3"));
    }
    {
        let conns = manager.connections.lock().await;
        assert_eq!(conns[&id].owner_window_label, "work_task");
    }
    assert!(
        dir.join("dsh-home/plugins/codeg-tool-search.mjs").is_file(),
        "the harness home is prepared at connect time"
    );
    manager.disconnect(&id).await.unwrap();
}

/// A conversation started on the retired `deepseek-acp` bridge has a bare-uuid
/// session id the launcher cannot load. The connection starts a fresh launcher
/// session whose transcript continues the bridge one, so reading the history
/// back yields the old turns first and the new ones after — not a replacement.
#[tokio::test]
async fn a_bridge_session_is_continued_not_replaced() {
    let _guard = SERIAL.lock().await;
    let bridge = "b4cbcfb7-641e-4b03-be6b-f248082166c9";
    let _ = transcript_root();
    let root = crate::paths::codeg_acp_transcripts_root();
    // The bridge conversation's recorded turn (what an ACP connection wrote).
    crate::acp_transcript::append_line_in(
        &root,
        "deepseek-acp",
        bridge,
        &serde_json::to_string(&crate::acp_transcript::TranscriptHeader::new(
            "deepseek", bridge, "/ws", 1,
        ))
        .unwrap(),
    );
    crate::acp_transcript::append_line_in(
        &root,
        "deepseek-acp",
        bridge,
        r#"{"t":2,"k":"prompt","p":[{"type":"text","text":"old question"}]}"#,
    );

    let h = Harness::resuming(&[], Some(bridge)).await;
    assert_eq!(h.external_id().await, None, "the bridge id is not resumed");
    // Its own minted id: headers are written once per file, and SID is shared.
    let minted = "session-5d0f7c1e-2b44-4e8e-9a51-0c6c3f0b7d21";
    let mut turn = happy_turn("new answer");
    turn[0] = json!({"type":"session","sessionId":minted,"cwd":"/ws"});
    h.stdout(&turn);
    let mut rx = h.subscribe().await;
    h.prompt("new question").await;
    collect_until(&mut rx, |e| e["type"] == "turn_complete").await;

    assert!(!h.args().contains(&"--session-id".to_string()));
    assert_eq!(h.external_id().await.as_deref(), Some(minted));
    let header = crate::acp_transcript::read_header_in(&root, "deepseek-acp", minted).unwrap();
    assert_eq!(header.continues_from.as_deref(), Some(bridge));
    let chain = crate::acp_transcript::read_chain_in(&root, "deepseek-acp", minted);
    let prompts: Vec<String> = chain
        .entries
        .iter()
        .filter(|e| e.k == crate::acp_transcript::EntryKind::Prompt)
        .map(|e| e.p.to_string())
        .collect();
    assert_eq!(prompts.len(), 2, "{prompts:?}");
    assert!(prompts[0].contains("old question"));
    assert!(prompts[1].contains("new question"));
}
