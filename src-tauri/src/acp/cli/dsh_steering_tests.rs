//! No database/provider/model: real DshDriver with a local fake JSONL process,
//! SessionState, Noop emitter and transcript files only.
use super::*;
use crate::acp::types::EventEnvelope;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};

const SID: &str = "session-dsh-native-fixture";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const FAKE: &str = r#"#!/usr/bin/env python3
import json, os, sys, time
root, mode = os.environ['FAKE_DIR'], os.environ['FAKE_MODE']
sid = 'session-dsh-native-fixture'
def out(v): print(json.dumps(v), flush=True)
def control(kind, **fields): out(dict(type=kind, protocol=1, version='0.1.6-alpha.2', sessionId=sid, **fields))
def finish():
    control('codeg_native_state', turn=1, supported=False)
    out(dict(type='status', phase='turn_end', reason=dict(kind='completed')))
    out(dict(type='final', text='done'))
with open(root + '/args.json', 'w') as f: json.dump(sys.argv[1:], f)
out(dict(type='session', sessionId=sid))
out(dict(type='text', text='before'))
if mode == 'unknown':
    out(dict(type='codeg_native_state', protocol=1, version='unknown', sessionId=sid, turn=1, supported=True))
    time.sleep(.15); finish(); sys.exit(0)
if mode == 'stock':
    control('codeg_native_state', turn=1, supported=True)
    time.sleep(.15); finish(); sys.exit(0)
control('codeg_native_state', turn=1, supported=True)
pending = []
for line in sys.stdin:
    command = json.loads(line)
    with open(root + '/requests.jsonl', 'a') as f: f.write(json.dumps(command) + '\n')
    if command['type'] != 'codeg_native_steer': raise Exception('unexpected cancel/prompt')
    pending.append(command)
    if mode == 'late':
        finish(); time.sleep(.05)
        control('codeg_native_result', id=command['id'], turn=1, status='injected')
        break
    if mode == 'lost': finish(); break
    if mode == 'pair' and len(pending) < 2: continue
    # Queued is deliberately not an accepted consumption receipt.
    control('codeg_native_queued', id=command['id'], turn=1)
    time.sleep(.15)
    for item in reversed(pending):
        control('codeg_native_result', id=item['id'], turn=1, status='injected')
    out(dict(type='text', text='after'))
    finish(); break
"#;

struct Harness {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    state: Arc<RwLock<SessionState>>,
    tx: mpsc::Sender<ConnectionCommand>,
    events: broadcast::Receiver<Arc<EventEnvelope>>,
    task: tokio::task::JoinHandle<()>,
}

impl Harness {
    fn new(mode: &str) -> Self {
        let _ = crate::acp::cli::dsh_tests::transcript_root();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let executable = dir.join("fake-dsh");
        std::fs::write(&executable, FAKE).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut session = SessionState::new(
            "dsh-native-fixture".into(),
            AgentType::DeepSeek,
            Some(dir.clone()),
            "test".into(),
            None,
        );
        session.cli_model = Some("fixture-model".into());
        let events = session.event_stream().subscribe();
        let state = Arc::new(RwLock::new(session));
        let driver = DshDriver {
            connection_id: "dsh-native-fixture".into(),
            agent_type: AgentType::DeepSeek,
            executable,
            working_dir: dir.clone(),
            runtime_env: BTreeMap::from([
                ("FAKE_DIR".into(), dir.display().to_string()),
                ("FAKE_MODE".into(), mode.into()),
            ]),
            state: state.clone(),
            emitter: EventEmitter::Noop,
            child_pid: Arc::new(AtomicU32::new(0)),
            connections: Arc::new(Mutex::new(HashMap::new())),
            profile: DshLaunchProfile {
                dsh_home: dir.join("home"),
                plugin_path: dir.join("plugin.mjs"),
                companion: None,
                tool_search_mode: "off".into(),
                runner_path: (mode != "stock").then(|| dir.join("runner.mjs")),
                background_wait_secs: 1,
            },
            provider: None,
            delegation: None,
            continues_from: None,
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
        }
    }

    async fn prompt(&self) {
        self.state.write().await.turn_in_flight = true;
        self.tx
            .send(ConnectionCommand::Prompt {
                blocks: vec![PromptInputBlock::Text { text: "A".into() }],
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
        .expect("dsh bridge not ready");
    }

    async fn steer(
        &self,
    ) -> oneshot::Receiver<Result<crate::acp::connection::SteerOutcome, AcpError>> {
        let (reply, rx) = oneshot::channel();
        let expected_turn_id = crate::acp::steering::turn_id(&*self.state.read().await);
        assert!(expected_turn_id.ends_with(":dsh:1"));
        self.tx
            .send(ConnectionCommand::NativeSteer {
                submission_id: uuid::Uuid::new_v4().to_string(),
                expected_turn_id,
                blocks: vec![PromptInputBlock::Text {
                    text: "same B".into(),
                }],
                reply,
            })
            .await
            .unwrap();
        rx
    }

    async fn completed(&mut self) -> Vec<AcpEvent> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut events = Vec::new();
            loop {
                let payload = self.events.recv().await.unwrap().payload.clone();
                let done = matches!(payload, AcpEvent::TurnComplete { .. });
                events.push(payload);
                if done {
                    return events;
                }
            }
        })
        .await
        .expect("fake dsh did not finish")
    }

    fn requests(&self) -> Vec<Value> {
        std::fs::read_to_string(self.dir.join("requests.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    async fn close(self) {
        assert!(!self.state.read().await.native_steer_v1_available);
        assert!(self.state.read().await.native_steer_v1_turn_id.is_none());
        self.tx.send(ConnectionCommand::Disconnect).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn myclaw_prompt_dsh_native_driver_waits_for_consumption_and_records_b() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("wait");
    let root = crate::paths::codeg_acp_transcripts_root();
    let before = crate::acp_transcript::read_transcript_in(&root, "deepseek-acp", SID)
        .entries
        .len();
    h.prompt().await;
    h.ready().await;
    let mut result = h.steer().await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(
        result.await.unwrap().unwrap(),
        crate::acp::connection::SteerOutcome::Injected
    );
    let events = h.completed().await;
    assert!(events
        .iter()
        .any(|event| matches!(event, AcpEvent::ContentDelta { text, .. } if text == "after")));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AcpEvent::TurnComplete { .. }))
            .count(),
        1
    );
    let transcript = crate::acp_transcript::read_transcript_in(&root, "deepseek-acp", SID);
    assert_eq!(
        transcript.entries[before..]
            .iter()
            .filter(|entry| entry.k == crate::acp_transcript::EntryKind::Prompt)
            .count(),
        2
    );
    assert_eq!(h.requests().len(), 1);
    assert_eq!(h.requests()[0]["type"], "codeg_native_steer");
    h.close().await;
}

#[tokio::test]
async fn myclaw_prompt_dsh_native_driver_equal_text_ids_are_independent() {
    let _guard = SERIAL.lock().await;
    let mut h = Harness::new("pair");
    h.prompt().await;
    h.ready().await;
    let first = h.steer().await;
    let second = h.steer().await;
    assert_eq!(
        second.await.unwrap().unwrap(),
        crate::acp::connection::SteerOutcome::Injected
    );
    assert_eq!(
        first.await.unwrap().unwrap(),
        crate::acp::connection::SteerOutcome::Injected
    );
    h.completed().await;
    let requests = h.requests();
    assert_eq!(requests.len(), 2);
    assert_ne!(requests[0]["id"], requests[1]["id"]);
    h.close().await;
}

#[tokio::test]
async fn myclaw_prompt_dsh_native_driver_final_before_or_without_receipt_is_unknown() {
    let _guard = SERIAL.lock().await;
    for mode in ["late", "lost"] {
        let mut h = Harness::new(mode);
        h.prompt().await;
        h.ready().await;
        assert!(
            h.steer().await.await.is_err(),
            "{mode}: final cannot substitute for a consumption receipt"
        );
        h.completed().await;
        assert_eq!(h.requests().len(), 1);
        h.close().await;
    }
}

#[tokio::test]
async fn myclaw_prompt_dsh_native_driver_stock_and_unknown_version_stay_disabled() {
    let _guard = SERIAL.lock().await;
    for mode in ["stock", "unknown"] {
        let mut h = Harness::new(mode);
        h.prompt().await;
        h.completed().await;
        assert!(h.requests().is_empty());
        h.close().await;
    }
}
