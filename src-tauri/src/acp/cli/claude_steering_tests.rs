//! Reviewed fake-process tests: temporary Python script, JSONL, SessionState,
//! Noop emitter and temporary transcript files only. No real CLI or database.
use super::*;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::AtomicU32;

const FAKE: &str = r#"#!/usr/bin/python3
import json, os, sys, time
root=os.environ['CLAUDE_NATIVE_FIXTURE'];mode=os.environ.get('CLAUDE_NATIVE_MODE','normal')
if '--version' in sys.argv:
 print('2.1.286 (Claude Code)' if mode=='old' else '2.1.287 (Claude Code)');sys.exit(0)
def out(value):print(json.dumps(value),flush=True)
def wait(name):
 while not os.path.exists(root+'/'+name):time.sleep(.005)
def finish(ids,text='finished',n=1):
 out({'type':'assistant','session_id':sid,'message':{'id':'answer','role':'assistant','content':[{'type':'text','text':text}]},'user_message_uuid':a['uuid'] if 'uuid' in a else '', 'user_message_uuids':ids})
 out({'type':'result','subtype':'success','session_id':sid,'num_turns':n,'is_error':False,'result':text,'terminal_reason':'completed','user_message_uuid':ids[-1] if ids else a.get('uuid',''),'user_message_uuids':ids,'usage':{'input_tokens':10,'output_tokens':2}})
a=json.loads(sys.stdin.readline());sid=sys.argv[sys.argv.index('--session-id')+1]
cap=[] if mode=='missing_cap' else ['msg_lifecycle_v1']
out({'type':'system','subtype':'init','claude_code_version':'2.1.287','capabilities':cap,'session_id':'another-session' if mode=='wrong_init' else sid})
if mode in ['old','missing_cap','wrong_init']:
 sys.stdin.read();open(root+'/eof','w').write('closed');finish([a.get('uuid','')]);sys.exit(0)
out({'type':'command_lifecycle','command_uuid':a['uuid'],'state':'started','session_id':sid})
out({'type':'assistant','session_id':sid,'message':{'id':'initial','role':'assistant','content':[{'type':'text','text':'initial progress'}]},'user_message_uuid':a['uuid'],'user_message_uuids':[a['uuid']]})
open(root+'/ready','w').write('ready')
if mode=='stale':
 wait('finish');finish([a['uuid']]);sys.stdin.read();sys.exit(0)
b=json.loads(sys.stdin.readline());open(root+'/input.json','w').write(json.dumps(b))
assert b['type']=='user' and b['priority']=='next' and b['shouldQuery'] is False
assert b['client_composed'] is True and b['origin']['kind']=='human' and b['uuid']!=a['uuid']
out({'type':'user','uuid':b['uuid'],'isReplay':True,'session_id':sid,'message':b['message']})
out({'type':'command_lifecycle','command_uuid':b['uuid'],'state':'started','session_id':sid})
open(root+'/queued','w').write('queued')
if mode in ['race','race_false','lost_cancel','background_race','background_race_lost']:
 if mode.startswith('background_race'):out({'type':'system','subtype':'task_started','task_id':'bg','tool_use_id':'tool-bg','is_backgrounded':True,'session_id':sid})
 finish([a['uuid']],'A original terminal')
 c=json.loads(sys.stdin.readline());open(root+'/cancel.json','w').write(json.dumps(c))
 assert c['request']['subtype']=='cancel_async_message' and c['request']['message_uuid']==b['uuid']
 if mode=='race_false':
  # A context-only result for B is not proof B joined original A.
  finish([b['uuid']],'context-only',0)
 if mode.startswith('background_race'):
  out({'type':'system','subtype':'task_notification','task_id':'bg','status':'completed','session_id':sid})
  finish([b['uuid']],'context-only',0)
 if mode not in ['lost_cancel','background_race_lost']:out({'type':'control_response','response':{'subtype':'success','request_id':c['request_id'],'response':{'cancelled':mode in ['race','background_race']}}})
 if mode.startswith('background_race'):
  wait('finish_background');finish([a['uuid']],'actual background terminal')
 sys.stdin.read();sys.exit(0)
if mode=='wrong_session':
 out({'type':'result','subtype':'success','session_id':'another-session','user_message_uuids':[a['uuid'],b['uuid']]})
wait('release')
if mode=='background':out({'type':'system','subtype':'task_started','task_id':'bg','tool_use_id':'tool-bg','is_backgrounded':True,'session_id':sid})
finish([a['uuid'],b['uuid']],'B adjusted the original task')
if mode=='background':
 wait('finish_background')
 out({'type':'system','subtype':'task_notification','task_id':'bg','status':'completed','session_id':sid})
 finish([a['uuid'],b['uuid']],'background continuation finished')
sys.stdin.read()
"#;

struct Harness {
    _temp: tempfile::TempDir,
    path: PathBuf,
    state: Arc<RwLock<SessionState>>,
    commands: mpsc::Sender<ConnectionCommand>,
    task: tokio::task::JoinHandle<()>,
    session: String,
}
impl Harness {
    async fn new(mode: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().to_path_buf();
        let executable = path.join("fake-claude");
        std::fs::write(&executable, FAKE).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let session = uuid::Uuid::new_v4().to_string();
        let mut state = SessionState::new(
            "claude-c".into(),
            AgentType::ClaudeCode,
            Some(path.clone()),
            "test".into(),
            None,
        );
        state.conversation_id = Some(42);
        state.external_id = Some(session.clone());
        state.turn_in_flight = true;
        let state = Arc::new(RwLock::new(state));
        let mut runtime_env = BTreeMap::new();
        runtime_env.insert("CLAUDE_NATIVE_FIXTURE".into(), path.display().to_string());
        runtime_env.insert("CLAUDE_NATIVE_MODE".into(), mode.into());
        runtime_env.insert(
            "CLAUDE_CONFIG_DIR".into(),
            path.join("config").display().to_string(),
        );
        let driver = CliDriver {
            connection_id: "claude-c".into(),
            agent_type: AgentType::ClaudeCode,
            executable,
            working_dir: path.clone(),
            runtime_env,
            state: state.clone(),
            emitter: EventEmitter::Noop,
            child_pid: Arc::new(AtomicU32::new(0)),
            connections: Arc::new(Mutex::new(HashMap::new())),
            companion: None,
            delegation: None,
        };
        let (commands, rx) = mpsc::channel(8);
        let task = tokio::spawn(driver.run(rx));
        commands
            .send(ConnectionCommand::Prompt {
                cli_options: None,
                blocks: vec![PromptInputBlock::Text {
                    text: "A initial".into(),
                }],
                user_message: None,
            })
            .await
            .unwrap();
        Self {
            _temp: temp,
            path,
            state,
            commands,
            task,
            session,
        }
    }
    async fn ready(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.state.read().await.native_steer_v1_available {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn file(&self, name: &str) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.path.join(name).exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn steer(
        &self,
        expected: Option<String>,
    ) -> oneshot::Receiver<Result<SteerOutcome, AcpError>> {
        let expected_turn_id = expected.unwrap_or_else(|| "".into());
        let expected_turn_id = if expected_turn_id.is_empty() {
            crate::acp::steering::turn_id(&*self.state.read().await)
        } else {
            expected_turn_id
        };
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(ConnectionCommand::NativeSteer {
                submission_id: uuid::Uuid::new_v4().to_string(),
                expected_turn_id,
                blocks: vec![PromptInputBlock::Text {
                    text: "B change course".into(),
                }],
                reply,
            })
            .await
            .unwrap();
        rx
    }
    async fn finished(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.state.read().await.turn_in_flight {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let state = self.state.read().await;
        assert_eq!(state.turns_completed, 1);
        assert!(!state.native_steer_v1_available);
        assert!(state.native_steer_v1_turn_id.is_none());
    }
    async fn close(self) {
        self.commands
            .send(ConnectionCommand::Disconnect)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
async fn isolated<F, Fut>(mode: &str, run: F)
where
    F: FnOnce(Harness) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_string_lossy().to_string();
    temp_env::async_with_vars([("CODEG_HOME", Some(path.as_str()))], async {
        let h = Harness::new(mode).await;
        run(h).await;
    })
    .await;
}

#[tokio::test]
async fn myclaw_prompt_claude_native_same_turn_proof_and_transcript() {
    isolated("normal", |h| async move {
        h.ready().await;
        let mut reply = h.steer(None).await;
        h.file("queued").await;
        assert!(
            matches!(reply.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "replay+started alone is not consumption"
        );
        std::fs::write(h.path.join("release"), "").unwrap();
        assert!(matches!(reply.await.unwrap(), Ok(SteerOutcome::Injected)));
        h.finished().await;
        let transcript = crate::acp_transcript::read_transcript(
            crate::acp::registry::registry_id_for(AgentType::ClaudeCode),
            &h.session,
        );
        assert_eq!(
            transcript
                .entries
                .iter()
                .filter(|entry| entry.k == crate::acp_transcript::EntryKind::Prompt
                    && entry.p.to_string().contains("B change course"))
                .count(),
            1
        );
        let sent: Value =
            serde_json::from_slice(&std::fs::read(h.path.join("input.json")).unwrap()).unwrap();
        assert_eq!(sent["shouldQuery"], false);
        assert!(!h.path.join("cancel.json").exists());
        h.close().await;
    })
    .await;
}
#[tokio::test]
async fn myclaw_prompt_claude_native_late_input_requires_real_queue_cancellation() {
    isolated("race", |h| async move {
        h.ready().await;
        let reply = h.steer(None).await;
        assert!(matches!(
            reply.await.unwrap(),
            Ok(SteerOutcome::PromptRequired)
        ));
        h.finished().await;
        h.file("cancel.json").await;
        h.close().await;
    })
    .await;
}
#[tokio::test]
async fn myclaw_prompt_claude_native_late_consumption_or_lost_cancel_stays_unknown() {
    for mode in ["race_false", "lost_cancel"] {
        isolated(mode, |h| async move {
            h.ready().await;
            let reply = h.steer(None).await;
            assert!(reply.await.unwrap().is_err());
            h.finished().await;
            h.close().await;
        })
        .await;
    }
}
#[tokio::test]
async fn myclaw_prompt_claude_native_stale_turn_never_writes_input() {
    isolated("stale", |h| async move {
        h.ready().await;
        let reply = h.steer(Some("another-turn".into())).await;
        assert!(matches!(reply.await.unwrap(), Err(AcpError::NoActiveTurn)));
        assert!(!h.path.join("input.json").exists());
        std::fs::write(h.path.join("finish"), "").unwrap();
        h.finished().await;
        h.close().await;
    })
    .await;
}
#[tokio::test]
async fn myclaw_prompt_claude_native_other_session_cannot_confirm_input() {
    isolated("wrong_session", |h| async move {
        h.ready().await;
        let mut reply = h.steer(None).await;
        h.file("queued").await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(reply.try_recv().is_err());
        std::fs::write(h.path.join("release"), "").unwrap();
        assert!(matches!(reply.await.unwrap(), Ok(SteerOutcome::Injected)));
        h.finished().await;
        h.close().await;
    })
    .await;
}
#[tokio::test]
async fn myclaw_prompt_claude_native_old_or_unadvertised_cli_preserves_eof() {
    for mode in ["old", "missing_cap", "wrong_init"] {
        isolated(mode, |h| async move {
            h.finished().await;
            assert!(
                h.path.join("eof").exists(),
                "{mode}: child must receive EOF"
            );
            assert!(!h.path.join("input.json").exists());
            h.close().await;
        })
        .await;
    }
}
#[tokio::test]
async fn myclaw_prompt_claude_native_background_retains_one_host_terminal() {
    isolated("background", |h| async move {
        h.ready().await;
        let reply = h.steer(None).await;
        std::fs::write(h.path.join("release"), "").unwrap();
        assert!(matches!(reply.await.unwrap(), Ok(SteerOutcome::Injected)));
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(h.state.read().await.turn_in_flight);
        assert!(!h.state.read().await.native_steer_v1_available);
        std::fs::write(h.path.join("finish_background"), "").unwrap();
        h.finished().await;
        h.close().await;
    })
    .await;
}
#[test]
fn myclaw_prompt_claude_native_version_and_duplicate_text_keep_identity() {
    assert!(audited_version("2.1.287 (Claude Code)"));
    for value in ["2.1.286", "2.1.287-beta.1", "3.0.0", "unknown"] {
        assert!(!audited_version(value));
    }
    let mut receipts = Receipts::new("s", "a");
    let blocks = vec![PromptInputBlock::Text {
        text: "same".into(),
    }];
    let (p1, r1) = oneshot::channel();
    let (p2, mut r2) = oneshot::channel();
    let b1 = receipts.enqueue(blocks.clone(), p1).unwrap();
    let b2 = receipts.enqueue(blocks, p2).unwrap();
    assert_ne!(b1["uuid"], b2["uuid"]);
    let proof = json!({"type":"result","session_id":"s","user_message_uuids":["a",b1["uuid"]]});
    let consumed = receipts.observe(&proof);
    assert_eq!(consumed.len(), 1);
    for (_, reply) in consumed {
        reply.send(Ok(SteerOutcome::Injected)).unwrap();
    }
    assert!(matches!(
        r1.blocking_recv().unwrap(),
        Ok(SteerOutcome::Injected)
    ));
    assert!(matches!(
        r2.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(receipts);
    assert!(r2.blocking_recv().unwrap().is_err());
}

#[tokio::test]
async fn myclaw_prompt_claude_native_receipt_after_background_notification_waits_for_final_result()
{
    for mode in ["background_race", "background_race_lost"] {
        isolated(mode, |h| async move {
            h.ready().await;
            let outcome = h.steer(None).await.await.unwrap();
            if mode == "background_race" {
                assert!(matches!(outcome, Ok(SteerOutcome::PromptRequired)));
            } else {
                assert!(outcome.is_err());
            }
            assert!(
                h.state.read().await.turn_in_flight,
                "a task notification and receipt cannot substitute the final result"
            );
            assert_eq!(h.state.read().await.turns_completed, 0);
            std::fs::write(h.path.join("finish_background"), "").unwrap();
            h.finished().await;
            h.close().await;
        })
        .await;
    }
}
