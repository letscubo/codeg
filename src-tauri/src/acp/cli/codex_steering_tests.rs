//! Drive the production Codex JSONL pump against an in-memory fake app-server.
//! Only temporary transcript files, SessionState, Noop and duplex IO are used.
//! No database, real agent, network or API credentials are involved.
use super::*;
use tokio::io::DuplexStream;

#[derive(Clone, Copy)]
enum Receipt {
    Injected,
    TerminalFirst,
    NoActiveTurn,
    Unknown,
    Lost,
}

async fn read_message(
    lines: &mut tokio::io::Lines<BufReader<tokio::io::ReadHalf<DuplexStream>>>,
) -> Value {
    serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
}

async fn protocol_roundtrip(receipt: Receipt, depth: Option<Option<String>>) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().to_string_lossy().to_string();
    temp_env::async_with_vars([("CODEG_HOME", Some(root.as_str()))], async {
        let sid = "native-steering-fixture";
        let mut state = SessionState::new("connection-a".into(), AgentType::Codex,
            Some(temp.path().to_path_buf()), "test".into(), None);
        state.conversation_id = Some(42);
        state.external_id = Some(sid.into());
        state.turn_in_flight = true;
        let state = Arc::new(RwLock::new(state));
        let driver = CodexDriver {
            connection_id:"connection-a".into(), agent_type:AgentType::Codex,
            executable:PathBuf::from("unused-fake-binary"), working_dir:temp.path().to_path_buf(),
            runtime_env:BTreeMap::new(), state:state.clone(), emitter:EventEmitter::Noop,
            child_pid:Arc::new(AtomicU32::new(0)), connections:Arc::new(Mutex::new(HashMap::new())),
            companion:None, delegation:None,
        };
        let initial = vec![PromptInputBlock::Text { text:"A initial task".into() }];
        driver.record_turn_start(sid, &initial).await;
        let mut record = TurnRecord { session_id:Some(sid.into()), prompt_blocks:initial, prompt_recorded:true };
        let mut mapper = CodexStreamMapper::new(Some(temp.path().to_path_buf()), None);
        let (host, peer) = tokio::io::duplex(16 * 1024);
        let (host_read, host_write) = tokio::io::split(host);
        let (peer_read, mut peer_write) = tokio::io::split(peer);
        let (commands, mut command_rx) = mpsc::channel(4);
        let fake_server = async {
            let mut lines = BufReader::new(peer_read).lines();
            let mut methods = Vec::new();
            let initialize = read_message(&mut lines).await;
            methods.push(initialize["method"].as_str().unwrap().to_string());
            send(&mut peer_write, &json!({"id":ID_INITIALIZE,"result":{}})).await.unwrap();
            let initialized = read_message(&mut lines).await;
            methods.push(initialized["method"].as_str().unwrap().to_string());
            if let Some(depth) = &depth {
                let list = read_message(&mut lines).await;
                assert_eq!(list["method"], "model/list");
                send(&mut peer_write, &json!({"id":ID_MODELS,"result":{"data":[{"id":"fixture-model","defaultReasoningEffort":"medium","supportedReasoningEfforts":[{"reasoningEffort":"low"},{"reasoningEffort":"medium"},{"reasoningEffort":"high"}]}]}})).await.unwrap();
                assert!(depth.as_deref().is_none_or(|s| s == "low" || s == "high"));
            }
            let thread = read_message(&mut lines).await;
            methods.push(thread["method"].as_str().unwrap().to_string());
            send(&mut peer_write, &json!({"id":ID_THREAD,"result":{"thread":{"id":sid}}})).await.unwrap();
            let start = read_message(&mut lines).await;
            methods.push(start["method"].as_str().unwrap().to_string());
            assert_eq!(start["params"]["input"][0]["text"], "A initial task");
            if let Some(depth) = &depth { assert_eq!(start["params"]["effort"], depth.as_deref().unwrap_or("medium")); }
            else { assert!(start["params"].get("effort").is_none()); }
            send(&mut peer_write, &json!({"id":ID_TURN,"result":{"turn":{"id":"turn-a","status":"inProgress"}}})).await.unwrap();
            while !state.read().await.native_steer_v1_available { tokio::task::yield_now().await; }
            let expected = crate::acp::steering::turn_id(&*state.read().await);
            let (reply, reply_rx) = tokio::sync::oneshot::channel();
            commands.send(ConnectionCommand::NativeSteer { submission_id:uuid::Uuid::new_v4().to_string(), expected_turn_id:expected,
                blocks:vec![PromptInputBlock::Text { text:"B adjust direction".into() }], reply }).await.unwrap();
            let steer = read_message(&mut lines).await;
            methods.push(steer["method"].as_str().unwrap().to_string());
            assert_eq!(steer["method"], "turn/steer");
            assert_eq!(steer["params"]["expectedTurnId"], "turn-a");
            assert_eq!(steer["params"]["threadId"], sid);
            assert_eq!(steer["params"]["input"][0]["text"], "B adjust direction");
            if matches!(receipt, Receipt::Lost) {
                peer_write.shutdown().await.unwrap();
                drop(peer_write);
                assert!(reply_rx.await.is_err(), "lost reply remains unconfirmed");
                return methods;
            }
            if matches!(receipt, Receipt::TerminalFirst) {
                send(&mut peer_write, &json!({"method":"turn/completed","params":{"turn":{"id":"turn-a","status":"completed","error":null}}})).await.unwrap();
            }
            let response = match receipt {
                Receipt::Injected | Receipt::TerminalFirst => json!({"id":steer["id"],"result":{"turnId":"turn-a"}}),
                Receipt::NoActiveTurn => json!({"id":steer["id"],"error":{"code":-32600,"message":"no active turn to steer"}}),
                Receipt::Unknown => json!({"id":steer["id"],"error":{"code":-32000,"message":"unexpected server failure"}}),
                Receipt::Lost => unreachable!(),
            };
            send(&mut peer_write, &response).await.unwrap();
            let outcome = reply_rx.await.unwrap();
            match receipt {
                Receipt::Injected | Receipt::TerminalFirst => assert!(matches!(outcome, Ok(SteerOutcome::Injected))),
                Receipt::NoActiveTurn => assert!(matches!(outcome, Ok(SteerOutcome::PromptRequired))),
                Receipt::Unknown => assert!(outcome.is_err()),
                Receipt::Lost => unreachable!(),
            }
            assert!(state.read().await.turn_in_flight, "steering must not finish the original turn");
            assert_eq!(state.read().await.turns_completed, 0);
            if !matches!(receipt, Receipt::TerminalFirst) {
                send(&mut peer_write, &json!({"method":"item/agentMessage/delta","params":{"turnId":"turn-a","itemId":"message-a","delta":"Original turn completed with adjusted direction"}})).await.unwrap();
                send(&mut peer_write, &json!({"method":"turn/completed","params":{"turn":{"id":"turn-a","status":"completed","error":null}}})).await.unwrap();
            }
            assert!(lines.next_line().await.unwrap().is_none(), "no hidden turn/start or turn/interrupt may follow");
            methods
        };
        let pump = driver.pump(host_write, host_read, vec![json!({"type":"text","text":"A initial task"})], depth.as_ref().map(|_| "fixture-model".into()), depth.clone(), &mut mapper, &mut record, &mut command_rx);
        let (end, methods) = tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(pump, fake_server) }).await.unwrap();
        assert_eq!(methods.iter().filter(|m| m.as_str() == "turn/start").count(), 1);
        assert!(!methods.iter().any(|m| m == "turn/interrupt"));
        match end {
            TurnEnd::Finished(finish) => {
                assert!(!matches!(receipt, Receipt::Lost));
                driver.finish_turn(finish, &record, 1, None).await;
                assert!(!state.read().await.turn_in_flight);
                assert_eq!(state.read().await.turns_completed, 1);
            }
            TurnEnd::Eof => assert!(matches!(receipt, Receipt::Lost)),
            _ => panic!("unexpected pump terminal state"),
        }
        let transcript = crate::acp_transcript::read_transcript_in(&temp.path().join("acp-transcripts"), crate::acp::registry::registry_id_for(AgentType::Codex), sid);
        let prompts = transcript.entries.iter().filter(|entry| matches!(entry.k, crate::acp_transcript::EntryKind::Prompt)).map(|entry| entry.p.to_string()).collect::<Vec<_>>();
        assert!(prompts.iter().any(|text| text.contains("A initial task")));
        assert_eq!(prompts.iter().filter(|text| text.contains("B adjust direction")).count(), usize::from(matches!(receipt, Receipt::Injected | Receipt::TerminalFirst)), "only confirmed native input is recorded once");
    }).await;
}

#[tokio::test]
async fn myclaw_prompt_codex_native_driver_turn_steer_finish_and_transcript() {
    protocol_roundtrip(Receipt::Injected, None).await;
}
#[tokio::test]
async fn myclaw_prompt_codex_native_driver_no_active_turn_is_unconsumed() {
    protocol_roundtrip(Receipt::NoActiveTurn, None).await;
}
#[tokio::test]
async fn myclaw_prompt_codex_native_driver_unknown_or_lost_reply_is_unconfirmed() {
    protocol_roundtrip(Receipt::Unknown, None).await;
    protocol_roundtrip(Receipt::Lost, None).await;
}

#[tokio::test]
async fn myclaw_prompt_codex_native_driver_terminal_before_ack_keeps_receipt_and_transcript() {
    protocol_roundtrip(Receipt::TerminalFirst, None).await;
}

#[tokio::test]
async fn myclaw_prompt_codex_automation_depth_survives_resume_and_restores_native_default() {
    for depth in [Some("low".into()), Some("high".into()), None] {
        protocol_roundtrip(Receipt::Injected, Some(depth)).await;
    }
}
