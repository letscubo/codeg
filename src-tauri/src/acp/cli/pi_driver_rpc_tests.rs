//! Database-free lifecycle fixtures: the real Pi driver talks JSONL to a local
//! fake process; SessionState, Noop emitter and transcript files only. No
//! ConnectionManager, AppState, SQL, network, provider or real agent is used.
use super::*;
use crate::acp::types::EventEnvelope;
use std::os::unix::fs::PermissionsExt;
use tokio::sync::{broadcast, oneshot};

const SID: &str = "11111111-2222-4333-8444-555555555555";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const FAKE_PI: &str = r#"#!/usr/bin/env python3
import json, os, sys
if sys.argv[1:] == ['--version']:
    print(os.environ.get('FAKE_VERSION', '1.3.0'))
    sys.exit(0)
root = os.environ['FAKE_DIR']
with open(root + '/args.json', 'w') as f: json.dump(sys.argv[1:], f)
def out(value):
    print(json.dumps(value), flush=True)
def event(kind, **fields): out(dict(type=kind, **fields))
def finish():
    event('agent_end', willRetry=False, messages=[{'role':'assistant','stopReason':'stop'}])
    event('agent_settled', aborted=False)
mode = os.environ.get('FAKE_MODE', 'wait')
pending = []
if '--print' in sys.argv:
    prompt = sys.stdin.read()
    with open(root + '/requests.jsonl', 'a') as f: f.write(json.dumps({'type':'legacy_prompt','message':prompt}) + '\n')
    event('session', id=os.environ['FAKE_SESSION'])
    event('message_update', assistantMessageEvent={'type':'text_delta','delta':'legacy answer'})
    finish()
    sys.exit(0)
for line in sys.stdin:
    command = json.loads(line)
    with open(root + '/requests.jsonl', 'a') as f: f.write(json.dumps(command) + '\n')
    kind, ident = command['type'], command['id']
    if kind == 'get_state':
        out({'type':'response','id':ident,'command':kind,'success':True,'data':{'sessionId':os.environ['FAKE_SESSION']}})
    elif kind == 'prompt':
        if mode == 'handled':
            out({'type':'response','id':ident,'command':kind,'success':True,'data':{'disposition':'handled'}})
            continue
        out({'type':'response','id':ident,'command':kind,'success':True,'data':{'disposition':'started'}})
        event('agent_start')
        event('message_start', message={'role':'user','content':[{'type':'text','text':command['message']}]})
        event('message_update', assistantMessageEvent={'type':'text_delta','delta':'before steer'})
        event('tool_execution_start', toolCallId='tool-1', toolName='bash', args={'command':'echo ok'})
        if mode == 'error':
            event('agent_end', willRetry=False, messages=[{'role':'assistant','stopReason':'error','errorMessage':'provider quota exhausted'}])
            event('agent_settled', aborted=False)
        elif mode == 'happy': finish()
    elif kind == 'steer':
        pending.append(command['message'])
        if mode == 'race':
            finish()
            out({'type':'response','id':ident,'command':kind,'success':True,'data':{'disposition':'queued'}})
        else:
            out({'type':'response','id':ident,'command':kind,'success':True,'data':{'disposition':'queued'}})
            event('message_start', message={'role':'user','content':[{'type':'text','text':command['message']}]})
            pending.clear()
            event('tool_execution_end', toolCallId='tool-1', toolName='bash', result={'content':[{'type':'text','text':'ok'}]}, isError=False)
            event('message_update', assistantMessageEvent={'type':'text_delta','delta':'after steer'})
            finish()
    elif kind == 'clear_queue':
        out({'type':'response','id':ident,'command':kind,'success':True,'data':{'steering':pending,'followUp':[]}})
        pending = []
"#;

struct Harness {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    state: Arc<RwLock<SessionState>>,
    tx: mpsc::Sender<ConnectionCommand>,
    events: broadcast::Receiver<Arc<EventEnvelope>>,
    task: tokio::task::JoinHandle<()>,
    pid: Arc<AtomicU32>,
}

impl Harness {
    fn new(mode: &str, resumed: bool) -> Self {
        let _ = crate::acp::cli::dsh_tests::transcript_root();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let executable = dir.join("fake-pi");
        std::fs::write(&executable, FAKE_PI).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut session = SessionState::new(
            "pi-test".into(),
            AgentType::Pi,
            Some(dir.clone()),
            "test".into(),
            None,
        );
        session.transport = crate::acp::session_state::ConnectionTransport::Cli;
        session.cli_model = Some("fixture-model".into());
        if resumed {
            session.external_id = Some(SID.into());
        }
        let events = session.event_stream().subscribe();
        let state = Arc::new(RwLock::new(session));
        let pid = Arc::new(AtomicU32::new(0));
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert("FAKE_DIR".into(), dir.display().to_string());
        runtime_env.insert("FAKE_MODE".into(), mode.into());
        runtime_env.insert("FAKE_SESSION".into(), SID.into());
        if mode == "legacy" {
            runtime_env.insert("FAKE_VERSION".into(), "0.84.2".into());
        }
        let driver = PiDriver {
            connection_id: "pi-test".into(),
            agent_type: AgentType::Pi,
            executable,
            working_dir: dir.clone(),
            runtime_env,
            state: state.clone(),
            emitter: EventEmitter::Noop,
            child_pid: pid.clone(),
            connections: Arc::new(Mutex::new(HashMap::new())),
            provider: Some("fixture".into()),
            session_dir: Some(dir.join("sessions")),
            continues_from: None,
            delegation: None,
            session_token: None,
        };
        let (tx, rx) = mpsc::channel(16);
        let task = tokio::spawn(driver.run(rx));
        Self {
            _tmp: tmp,
            dir,
            state,
            tx,
            events,
            task,
            pid,
        }
    }

    async fn prompt(&self) {
        self.state.write().await.turn_in_flight = true;
        self.tx
            .send(ConnectionCommand::Prompt {
                blocks: vec![PromptInputBlock::Text {
                    text: "initial".into(),
                }],
                user_message: None,
                cli_options: None,
            })
            .await
            .unwrap();
    }

    async fn ready(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.state.read().await.native_steer_v1_available {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("RPC did not become ready");
    }

    async fn steer(
        &self,
        expected: String,
    ) -> oneshot::Receiver<Result<crate::acp::connection::SteerOutcome, AcpError>> {
        let (reply, receiver) = oneshot::channel();
        self.tx
            .send(ConnectionCommand::NativeSteer {
                submission_id: uuid::Uuid::new_v4().to_string(),
                expected_turn_id: expected,
                blocks: vec![PromptInputBlock::Text {
                    text: "change direction".into(),
                }],
                reply,
            })
            .await
            .unwrap();
        receiver
    }

    async fn turn(&self) -> String {
        crate::acp::steering::turn_id(&*self.state.read().await)
    }

    async fn completed(&mut self) -> Vec<AcpEvent> {
        tokio::time::timeout(Duration::from_secs(6), async {
            let mut events = Vec::new();
            loop {
                let event = self.events.recv().await.unwrap().payload.clone();
                let complete = matches!(event, AcpEvent::TurnComplete { .. });
                events.push(event);
                if complete {
                    return events;
                }
            }
        })
        .await
        .expect("turn did not settle")
    }

    fn requests(&self) -> Vec<Value> {
        std::fs::read_to_string(self.dir.join("requests.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    async fn close(self) {
        self.tx.send(ConnectionCommand::Disconnect).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn myclaw_prompt_pi_rpc_driver_steers_one_turn_preserving_tools_and_finish() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("wait", false);
    let transcript_root = crate::paths::codeg_acp_transcripts_root();
    let before = crate::acp_transcript::read_transcript_in(&transcript_root, "pi-acp", SID)
        .entries
        .len();
    h.prompt().await;
    h.ready().await;
    let result = h.steer(h.turn().await).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), result)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        crate::acp::connection::SteerOutcome::Injected
    );
    let events = h.completed().await;
    assert!(events.iter().any(
        |event| matches!(event, AcpEvent::ToolCall { tool_call_id, .. } if tool_call_id == "tool-1")
    ));
    assert!(events.iter().any(
        |event| matches!(event, AcpEvent::ToolCallUpdate { status, .. } if status.as_deref() == Some("completed"))
    ));
    assert!(
        matches!(events.last(), Some(AcpEvent::TurnComplete { stop_reason, .. }) if stop_reason == "end_turn")
    );
    for expected in ["before steer", "after steer"] {
        assert!(events
            .iter()
            .any(|event| matches!(event, AcpEvent::ContentDelta { text, .. } if text == expected)));
    }
    let transcript = crate::acp_transcript::read_transcript_in(&transcript_root, "pi-acp", SID);
    let new_entries = &transcript.entries[before..];
    assert_eq!(
        new_entries
            .iter()
            .filter(|entry| entry.k == crate::acp_transcript::EntryKind::Prompt)
            .count(),
        2
    );
    assert!(new_entries
        .iter()
        .any(|entry| entry.k == crate::acp_transcript::EntryKind::Prompt
            && entry.p.to_string().contains("change direction")));
    assert_eq!(new_entries.last().unwrap().p["model"], "fixture-model");
    let requests = h.requests();
    assert_eq!(requests.iter().filter(|r| r["type"] == "prompt").count(), 1);
    assert_eq!(requests.iter().filter(|r| r["type"] == "steer").count(), 1);
    assert!(!requests.iter().any(|r| r["type"] == "abort"));
    assert!(!h.state.read().await.native_steer_v1_available);
    assert!(
        !h.state.read().await.native_steering_available,
        "legacy pull-feedback gate stays independent"
    );
    h.close().await;
}

#[tokio::test]
async fn myclaw_prompt_pi_rpc_driver_idle_and_stale_turn_never_write_steer() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("wait", true);
    assert!(matches!(
        h.steer("idle".into()).await.await.unwrap(),
        Err(AcpError::NoActiveTurn)
    ));
    assert_eq!(h.pid.load(Ordering::SeqCst), 0);
    h.prompt().await;
    h.ready().await;
    assert!(matches!(
        h.steer("wrong-turn".into()).await.await.unwrap(),
        Err(AcpError::NoActiveTurn)
    ));
    assert!(!h.requests().iter().any(|r| r["type"] == "steer"));
    h.tx.send(ConnectionCommand::Cancel).await.unwrap();
    let events = h.completed().await;
    assert!(
        matches!(events.last(), Some(AcpEvent::TurnComplete { stop_reason, .. }) if stop_reason == "cancelled")
    );
    assert!(!h.state.read().await.native_steer_v1_available);
    h.close().await;
}

#[tokio::test]
async fn myclaw_prompt_pi_rpc_driver_settlement_race_clears_unconsumed_input() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("race", true);
    h.prompt().await;
    h.ready().await;
    let result = h.steer(h.turn().await).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), result)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        crate::acp::connection::SteerOutcome::PromptRequired
    );
    h.completed().await;
    let requests = h.requests();
    assert_eq!(requests.iter().filter(|r| r["type"] == "prompt").count(), 1);
    assert!(requests.iter().any(|r| r["type"] == "clear_queue"));
    h.close().await;
}

#[tokio::test]
async fn myclaw_prompt_pi_rpc_driver_provider_error_remains_error() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("error", false);
    h.prompt().await;
    let events = h.completed().await;
    assert!(events.iter().any(|event| matches!(event, AcpEvent::Error { message, .. } if message == "provider quota exhausted")));
    assert!(
        matches!(events.last(), Some(AcpEvent::TurnComplete { stop_reason, .. }) if stop_reason == "unknown")
    );
    assert!(!h.state.read().await.native_steer_v1_available);
    h.close().await;
}

#[tokio::test]
async fn myclaw_prompt_pi_rpc_driver_resume_keeps_session_and_handles_local_commands() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("handled", true);
    h.prompt().await;
    let events = h.completed().await;
    assert!(!events
        .iter()
        .any(|event| matches!(event, AcpEvent::SessionStarted { .. })));
    assert_eq!(h.state.read().await.external_id.as_deref(), Some(SID));
    let args: Vec<String> =
        serde_json::from_str(&std::fs::read_to_string(h.dir.join("args.json")).unwrap()).unwrap();
    assert!(args
        .windows(2)
        .any(|pair| pair[0] == "--session-id" && pair[1] == SID));
    assert!(args.windows(2).any(|pair| pair == ["--mode", "rpc"]));
    assert!(!args.iter().any(|arg| arg == "--print"));
    h.close().await;
}

#[tokio::test]
async fn myclaw_prompt_pi_rpc_driver_old_binary_preserves_print_transport() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("legacy", true);
    h.prompt().await;
    let events = h.completed().await;
    assert!(
        matches!(events.last(), Some(AcpEvent::TurnComplete { stop_reason, .. }) if stop_reason == "end_turn")
    );
    assert_eq!(h.requests()[0]["type"], "legacy_prompt");
    assert_eq!(h.requests()[0]["message"], "initial");
    let args: Vec<String> =
        serde_json::from_str(&std::fs::read_to_string(h.dir.join("args.json")).unwrap()).unwrap();
    assert!(args.iter().any(|arg| arg == "--print"));
    assert!(!h.state.read().await.native_steer_v1_available);
    h.close().await;
}
