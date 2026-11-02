//! Codeg owns the turn, pacing and typing. The platform proxy holds WAHA credentials.
use super::{
    classify, tool_status, Composer, Outcome, PublicEvent, RelayInput, TurnReport, MAX_TURN,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, net::IpAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
const UPDATE: Duration = Duration::from_secs(2);
const TYPING: Duration = Duration::from_secs(4);
const EDIT_WINDOW: Duration = Duration::from_secs(14 * 60);
const MAX_BYTES: usize = 256 * 1024;
const STATUS_DELAY: Duration = Duration::from_secs(15);

/// Opt-in negotiated by the platform; old proxies continue using the v2 presentation.
#[derive(Clone, Debug, Deserialize)]
pub struct Presentation {
    pub working: String,
    pub waiting: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum Operation {
    Send {
        key: String,
        text: String,
    },
    Edit {
        key: String,
        text: String,
        #[serde(rename = "messageId")]
        message_id: String,
    },
    Typing {
        active: bool,
    },
    Reaction {
        state: String,
    },
    Finish {
        outcome: String,
        operations: Vec<String>,
    },
}
pub(super) fn endpoint(raw: &str, turn_id: &str) -> Result<reqwest::Url, &'static str> {
    let id = uuid::Uuid::parse_str(turn_id).map_err(|_| "invalid turn id")?;
    let url = reqwest::Url::parse(raw).map_err(|_| "invalid relay endpoint")?;
    let host = url.host_str().ok_or("relay endpoint host required")?;
    let normalized_host = host.to_ascii_lowercase();
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != format!("/api/channels/whatsapp/relay/{id}")
    {
        return Err("invalid relay endpoint");
    }
    if normalized_host == "localhost"
        || normalized_host.ends_with(".localhost")
        || normalized_host.ends_with(".local")
        || host.parse::<IpAddr>().is_ok()
    {
        return Err("relay endpoint must use a public hostname");
    }
    Ok(url)
}
#[derive(Debug)]
pub(super) enum BrokerError {
    Retryable,
    Rejected,
}
#[async_trait]
pub(super) trait Broker: Send + Sync {
    async fn execute(&self, operation: &Operation) -> Result<Option<String>, BrokerError>;
}
pub(super) struct HttpBroker {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    grant: String,
}
impl HttpBroker {
    pub(super) fn new(endpoint: reqwest::Url, grant: String) -> Result<Self, &'static str> {
        if grant.is_empty() || grant.len() > 4096 {
            return Err("invalid relay grant");
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "proxy client unavailable")?;
        Ok(Self {
            client,
            endpoint,
            grant,
        })
    }
}
#[async_trait]
impl Broker for HttpBroker {
    async fn execute(&self, op: &Operation) -> Result<Option<String>, BrokerError> {
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.grant)
            .json(op)
            .send()
            .await
            .map_err(|_| BrokerError::Retryable)?;
        if response.status().is_server_error() || response.status().as_u16() == 429 {
            return Err(BrokerError::Retryable);
        }
        if !response.status().is_success() {
            return Err(BrokerError::Rejected);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| BrokerError::Retryable)? {
            if bytes.len() + chunk.len() > 4096 {
                return Err(BrokerError::Rejected);
            }
            bytes.extend_from_slice(&chunk);
        }
        let receipt: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| BrokerError::Retryable)?;
        if receipt["data"]["accepted"] != true {
            return Err(BrokerError::Retryable);
        }
        match op {
            Operation::Send { .. } | Operation::Edit { .. } => receipt["data"]["messageId"]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() < 512)
                .map(|s| Some(s.to_owned()))
                .ok_or(BrokerError::Retryable),
            Operation::Finish { outcome, .. }
                if outcome != "handoff" && receipt["data"]["finished"] != true =>
            {
                Err(BrokerError::Retryable)
            }
            _ => Ok(None),
        }
    }
}
async fn call(broker: &Arc<dyn Broker>, op: &Operation) -> Result<Option<String>, BrokerError> {
    for attempt in 0..3 {
        match broker.execute(op).await {
            Err(BrokerError::Retryable) if attempt < 2 => {
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await
            }
            result => return result,
        }
    }
    Err(BrokerError::Retryable)
}
async fn typing_pump(broker: Arc<dyn Broker>, mut active: watch::Receiver<bool>) {
    loop {
        let enabled = *active.borrow_and_update();
        let _ = broker.execute(&Operation::Typing { active: enabled }).await;
        tokio::select! {
            changed=active.changed()=>if changed.is_err(){let _=broker.execute(&Operation::Typing{active:false}).await;break;},
            _=tokio::time::sleep(TYPING), if enabled=>{},
        }
    }
}
// One in-flight status request; coalesce rapid tool updates. Cosmetic failures never
// interrupt answer delivery, and no tool arguments/reasoning enter this channel.
async fn reaction_pump(broker: Arc<dyn Broker>, mut state: watch::Receiver<Option<&'static str>>) {
    if state.borrow().is_some() {
        let _ = broker
            .execute(&Operation::Reaction {
                state: "received".into(),
            })
            .await;
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    let mut last = None;
    loop {
        let current = *state.borrow_and_update();
        if current != last || current.is_none() {
            let _ = broker
                .execute(&Operation::Reaction {
                    state: current.unwrap_or("clear").into(),
                })
                .await;
            last = current;
        }
        if current.is_none() {
            break;
        }
        if state.changed().await.is_err() {
            let _ = broker
                .execute(&Operation::Reaction {
                    state: "clear".into(),
                })
                .await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
}

/// Carry fenced-code context across long-message boundaries. Only final chunks
/// advance the context; preview edits may render the same source repeatedly.
fn fenced_chunk(text: &str, opened: &mut Option<String>) -> String {
    let mut result = opened
        .as_ref()
        .map(|marker| format!("{marker}\n"))
        .unwrap_or_default();
    result.push_str(text);
    for line in text.lines() {
        let line = line.trim_start();
        let Some(ch) = line.chars().next().filter(|c| *c == '`' || *c == '~') else {
            continue;
        };
        let width = line.chars().take_while(|c| *c == ch).count();
        if !(3..=16).contains(&width) {
            continue;
        }
        match opened.as_ref() {
            Some(marker)
                if marker.starts_with(ch)
                    && width >= marker.len()
                    && line[width..].trim().is_empty() =>
            {
                *opened = None
            }
            None => *opened = Some(ch.to_string().repeat(width)),
            _ => {}
        }
    }
    if let Some(marker) = opened.as_ref() {
        result.push_str(&format!("\n{marker}"));
    }
    result
}
struct Output {
    broker: Arc<dyn Broker>,
    live: Option<String>,
    created: Instant,
    shown: String,
    index: usize,
    edit: usize,
    finals: Vec<String>,
    presentation: bool,
    code_fence: Option<String>,
}
impl Output {
    async fn show(&mut self, text: String, final_part: bool) -> bool {
        let mut next_fence = self.code_fence.clone();
        let text = if self.presentation {
            fenced_chunk(&text, &mut next_fence)
        } else {
            text
        };
        if !final_part
            && (self.shown == text || self.live.is_some() && self.created.elapsed() >= EDIT_WINDOW)
        {
            return true;
        }
        self.edit += 1;
        let key = if final_part {
            format!("final:{}", self.index)
        } else if self.live.is_some() {
            format!("edit:{}", self.edit)
        } else {
            format!("message:{}", self.index)
        };
        let op = match &self.live {
            Some(id) if self.created.elapsed() < EDIT_WINDOW => Operation::Edit {
                key: key.clone(),
                text: text.clone(),
                message_id: id.clone(),
            },
            _ => Operation::Send {
                key: key.clone(),
                text: text.clone(),
            },
        };
        match call(&self.broker, &op).await {
            Ok(Some(id)) => {
                if self.live.as_ref() != Some(&id) {
                    self.created = Instant::now();
                }
                self.live = Some(id);
                self.shown = text;
                if final_part {
                    self.code_fence = next_fence;
                    self.finals.push(key);
                    self.live = None;
                    self.shown.clear();
                    self.index += 1;
                }
                true
            }
            _ => false,
        }
    }
}
pub(super) struct WhatsappTurn {
    pub broker: Arc<dyn Broker>,
    pub presentation: Option<Presentation>,
}
impl WhatsappTurn {
    pub(super) async fn run(self, mut rx: mpsc::UnboundedReceiver<RelayInput>) -> TurnReport {
        let (typing, receiver) = watch::channel(true);
        let pulse = tokio::spawn(typing_pump(self.broker.clone(), receiver));
        let rich = self.presentation.is_some();
        let reaction = if rich {
            let (tx, rx) = watch::channel(Some("thinking"));
            Some((tx, tokio::spawn(reaction_pump(self.broker.clone(), rx))))
        } else {
            None
        };
        let mut out = Output {
            broker: self.broker.clone(),
            live: None,
            created: Instant::now(),
            shown: String::new(),
            index: 0,
            edit: 0,
            finals: vec![],
            presentation: rich,
            code_fence: None,
        };
        let mut composer = Composer::default();
        let mut bytes = 0usize;
        let mut tools = BTreeSet::new();
        let mut waiting = BTreeSet::new();
        let mut status = "💭".to_owned();
        let mut dirty = false;
        let mut last = Instant::now();
        let fallback_at = last + STATUS_DELAY;
        let mut fallback_shown = false;
        let mut after_tool = false;
        let deadline = Instant::now() + MAX_TURN;
        let mut delivery_failed = !rich && !out.show(status.clone(), false).await;
        let outcome = if delivery_failed {
            Outcome::Failed
        } else {
            loop {
                let next = if dirty { last + UPDATE } else { deadline };
                tokio::select! {
                    input=rx.recv()=>match input {
                        None=>break Outcome::Aborted,
                        Some(RelayInput::Lagged)=>break Outcome::Lagged,
                        Some(RelayInput::Event(env))=>{
                            match classify(&env.payload){
                                PublicEvent::Text(text)=>{
                                    bytes+=text.len();if bytes>MAX_BYTES{break Outcome::Lagged;}
                                    if rich && after_tool && !text.is_empty() {
                                        if !composer.text().is_empty() && !composer.text().ends_with("\n\n") {
                                            for head in composer.push_whatsapp("\n\n") { if !out.show(head,true).await { delivery_failed=true;break; } }
                                        }
                                        after_tool=false;
                                    }
                                    if delivery_failed { break Outcome::Failed; }
                                    for head in composer.push_whatsapp(text){if !out.show(head,true).await{delivery_failed=true;break;}}
                                    if delivery_failed{break Outcome::Failed;}
                                }
                                PublicEvent::ToolStarted{id,name,..}=>{tools.insert(id.to_owned());after_tool=true;status=tool_status(None,name.unwrap_or("Tool")).unwrap_or_default().chars().take(80).collect();}
                                PublicEvent::ToolUpdated{id,name,state,..}=>{
                                    if matches!(state,Some("completed"|"failed"|"cancelled")){tools.remove(id);}else if tools.contains(id)&&name.is_some(){status=tool_status(None,name.unwrap()).unwrap_or_default().chars().take(80).collect();}
                                }
                                PublicEvent::AwaitingUser(kind,id)=>{waiting.insert((kind.to_owned(),id.to_owned()));}
                                PublicEvent::Resumed(kind,id)=>{waiting.remove(&(kind.to_owned(),id.to_owned()));}
                                PublicEvent::Finished(reason)=>break match reason{"end_turn"=>Outcome::Completed,"cancelled"=>Outcome::Cancelled,_=>Outcome::Failed},
                                PublicEvent::Failed=>break Outcome::Failed,
                                PublicEvent::Ignore=>continue,
                            }
                            let _=typing.send(waiting.is_empty());dirty=true;
                            if let Some((tx,_))=&reaction {
                                let next=Some(if !waiting.is_empty(){"waiting"}else if !tools.is_empty(){"tool"}else if composer.text().is_empty(){"thinking"}else{"writing"});
                                tx.send_if_modified(|current| { if *current==next {false} else {*current=next;true} });
                            }
                        }
                    },
                    _=tokio::time::sleep_until(next), if dirty=>{
                        let text = if let Some(p)=&self.presentation {
                            if !waiting.is_empty() { Some(format!("{}\n\n{}",composer.text(),p.waiting.chars().take(120).collect::<String>()).trim().to_owned()) }
                            else if !composer.text().trim().is_empty() && (out.live.is_some() || composer.text().chars().count() >= 20) { Some(composer.text().to_owned()) }
                            else if out.live.is_some() { Some(p.working.chars().take(120).collect()) }
                            else { None }
                        } else {
                            let note=if !waiting.is_empty(){"⏸"}else if !tools.is_empty(){&status}else if composer.text().is_empty(){"💭"}else{"✍️"};
                            Some(if composer.text().is_empty(){note.to_owned()}else{format!("{}\n\n{note}",composer.text())})
                        };
                        if let Some(text)=text { if !out.show(text,false).await{delivery_failed=true;break Outcome::Failed;} }
                        dirty=false;last=Instant::now();
                    }
                    _=tokio::time::sleep_until(fallback_at), if rich && !fallback_shown=>{
                        fallback_shown=true;
                        if out.live.is_none() && composer.text().trim().is_empty() {
                            let text=self.presentation.as_ref().unwrap().working.chars().take(120).collect();
                            if !out.show(text,false).await {delivery_failed=true;break Outcome::Failed;}
                        }
                    }
                    _=tokio::time::sleep_until(deadline)=>break Outcome::Failed,
                }
            }
        };
        let _ = typing.send(false);
        drop(typing);
        let _ = pulse.await;
        let handoff = delivery_failed || matches!(outcome, Outcome::Lagged | Outcome::Aborted);
        if !handoff {
            let text = match outcome {
                Outcome::Completed => {
                    if composer.text().trim().is_empty() {
                        "(No reply was generated.)".to_owned()
                    } else {
                        composer.text().to_owned()
                    }
                }
                Outcome::Cancelled => format!("{}\n\n⏹ Stopped.", composer.text()),
                _ => "⚠️ Something went wrong while generating this reply. Please try again."
                    .to_owned(),
            };
            delivery_failed = !out.show(text, true).await;
        }
        let label = if handoff || delivery_failed {
            "handoff"
        } else {
            match outcome {
                Outcome::Completed => "completed",
                Outcome::Cancelled => "cancelled",
                _ => "failed",
            }
        };
        delivery_failed |= call(
            &self.broker,
            &Operation::Finish {
                outcome: label.to_owned(),
                operations: out.finals,
            },
        )
        .await
        .is_err();
        if let Some((tx, worker)) = reaction {
            let _ = tx.send(None);
            drop(tx);
            let _ = worker.await;
        }
        TurnReport {
            outcome,
            delivery_failed,
            message_ids: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };
    use tokio::sync::Notify;
    #[derive(Default)]
    struct FakeBroker {
        calls: Mutex<Vec<Operation>>,
        fail_once: AtomicBool,
        fail_all: AtomicBool,
        fail_reactions: AtomicBool,
        block_once: AtomicBool,
        started: Notify,
        release: Notify,
    }
    #[async_trait]
    impl Broker for FakeBroker {
        async fn execute(&self, op: &Operation) -> Result<Option<String>, BrokerError> {
            self.calls.lock().unwrap().push(op.clone());
            if matches!(op, Operation::Reaction { .. })
                && self.fail_reactions.load(Ordering::SeqCst)
            {
                return Err(BrokerError::Rejected);
            }
            if matches!(op, Operation::Send { .. } | Operation::Edit { .. }) {
                if self.block_once.swap(false, Ordering::SeqCst) {
                    self.started.notify_one();
                    self.release.notified().await;
                }
                if self.fail_all.load(Ordering::SeqCst)
                    || self.fail_once.swap(false, Ordering::SeqCst)
                {
                    return Err(BrokerError::Retryable);
                }
            }
            Ok(match op {
                Operation::Send { key, .. } => Some(format!("true_group_{key}")),
                Operation::Edit { message_id, .. } => Some(message_id.clone()),
                _ => None,
            })
        }
    }
    #[test]
    fn explicit_relay_endpoint_is_exact_and_operation_has_no_recipient_credentials() {
        let id = "11111111-1111-4111-8111-111111111111";
        let raw = format!("https://platform.invalid/api/channels/whatsapp/relay/{id}");
        assert_eq!(endpoint(&raw, id).unwrap().as_str(), raw);
        assert!(endpoint(
            &format!("http://platform.invalid/api/channels/whatsapp/relay/{id}"),
            id
        )
        .is_err());
        let value = serde_json::to_value(Operation::Edit {
            key: "final:0".into(),
            text: "hello".into(),
            message_id: "id".into(),
        })
        .unwrap();
        assert_eq!(value["messageId"], "id");
        assert_eq!(value.as_object().unwrap().len(), 4);
    }
    #[test]
    fn relay_endpoint_rejects_other_turns_credentials_and_local_hosts() {
        let id = "11111111-1111-4111-8111-111111111111";
        assert!(endpoint(
            &format!("https://user:secret@platform.invalid/api/channels/whatsapp/relay/{id}"),
            id
        )
        .is_err());
        assert!(endpoint(
            &format!("https://platform.invalid/api/channels/whatsapp/relay/{id}?s=x#fragment"),
            id
        )
        .is_err());
        assert!(endpoint(
            "https://platform.invalid/api/channels/whatsapp/relay/22222222-2222-4222-8222-222222222222",
            id
        )
        .is_err());
        assert!(endpoint(
            &format!("https://localhost/api/channels/whatsapp/relay/{id}"),
            id
        )
        .is_err());
        assert!(endpoint(
            &format!("https://127.0.0.1/api/channels/whatsapp/relay/{id}"),
            id
        )
        .is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn completion_requires_final_operations_and_filters_private_events() {
        use super::super::tests::{complete, delta, env};
        let broker = Arc::new(FakeBroker::default());
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(env(crate::acp::types::AcpEvent::Thinking {
            text: "SECRET".into(),
            parent_tool_use_id: None,
        }))
        .unwrap();
        tx.send(delta("hello")).unwrap();
        tx.send(complete("end_turn")).unwrap();
        let report = WhatsappTurn {
            broker: broker.clone(),
            presentation: None,
        }
        .run(rx)
        .await;
        assert_eq!(report.outcome, Outcome::Completed);
        assert!(!report.delivery_failed);
        let calls = broker.calls.lock().unwrap();
        assert!(!serde_json::to_string(&*calls).unwrap().contains("SECRET"));
        assert!(calls.iter().any(
            |op| matches!(op,Operation::Edit{key,text,..} if key=="final:0" && text=="hello")
        ));
        assert!(
            matches!(calls.last(),Some(Operation::Finish{outcome,operations}) if outcome=="completed" && operations==&vec!["final:0".to_owned()])
        );
    }
    #[tokio::test(start_paused = true)]
    async fn large_unicode_reply_is_split_without_exceeding_whatsapp_limit() {
        use super::super::tests::{complete, delta};
        let broker = Arc::new(FakeBroker::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let text = "🚀".repeat(2200);
        tx.send(delta(&text)).unwrap();
        tx.send(complete("end_turn")).unwrap();
        WhatsappTurn {
            broker: broker.clone(),
            presentation: None,
        }
        .run(rx)
        .await;
        let calls = broker.calls.lock().unwrap();
        let mut final_text = String::new();
        for op in calls.iter() {
            match op {
                Operation::Send { key, text } | Operation::Edit { key, text, .. } => {
                    assert!(text.encode_utf16().count() <= 4000);
                    if key.starts_with("final:") {
                        final_text.push_str(text);
                    }
                }
                _ => {}
            }
        }
        assert_eq!(final_text, text);
    }
    #[tokio::test(start_paused = true)]
    async fn transport_retry_reuses_the_identical_operation_key() {
        let broker = Arc::new(FakeBroker::default());
        broker.fail_once.store(true, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(super::super::tests::complete("end_turn")).unwrap();
        let result = WhatsappTurn {
            broker: broker.clone(),
            presentation: None,
        }
        .run(rx)
        .await;
        assert!(!result.delivery_failed);
        let sends: Vec<_> = broker
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|op| matches!(op, Operation::Send { .. }))
            .cloned()
            .collect();
        assert_eq!(sends[0], sends[1]);
    }
    #[tokio::test(start_paused = true)]
    async fn failed_delivery_never_reports_successful_completion() {
        let broker = Arc::new(FakeBroker::default());
        broker.fail_all.store(true, Ordering::SeqCst);
        let (_tx, rx) = mpsc::unbounded_channel();
        WhatsappTurn {
            broker: broker.clone(),
            presentation: None,
        }
        .run(rx)
        .await;
        assert!(
            matches!(broker.calls.lock().unwrap().last(),Some(Operation::Finish{outcome,..}) if outcome=="handoff")
        );
    }
    #[tokio::test(start_paused = true)]
    async fn typing_pulses_while_message_request_is_blocked() {
        let broker = Arc::new(FakeBroker::default());
        broker.block_once.store(true, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(super::super::tests::complete("end_turn")).unwrap();
        let task = tokio::spawn(
            WhatsappTurn {
                broker: broker.clone(),
                presentation: None,
            }
            .run(rx),
        );
        broker.started.notified().await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(8)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            broker
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|op| matches!(op, Operation::Typing { active: true }))
                .count()
                >= 2
        );
        broker.release.notify_one();
        assert!(!task.await.unwrap().delivery_failed);
        assert!(broker
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|op| matches!(op, Operation::Typing { active: false })));
    }
    fn quiet_turn(broker: Arc<FakeBroker>) -> WhatsappTurn {
        WhatsappTurn {
            broker,
            presentation: Some(Presentation {
                working: "正在处理你的请求…".into(),
                waiting: "请在 MyClaw 中确认。".into(),
            }),
        }
    }
    async fn settle_tasks() {
        for _ in 0..12 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn quick_replies_have_no_placeholder_or_cursor_bubble() {
        use super::super::tests::{complete, delta};
        let b = Arc::new(FakeBroker::default());
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(delta("收到")).unwrap();
        tx.send(complete("end_turn")).unwrap();
        let report = quiet_turn(b.clone()).run(rx).await;
        assert!(!report.delivery_failed);
        let calls = b.calls.lock().unwrap();
        let bodies: Vec<_> = calls
            .iter()
            .filter_map(|op| match op {
                Operation::Send { text, .. } | Operation::Edit { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(bodies, vec!["收到"]);
        assert!(matches!(calls.last(),Some(Operation::Reaction{state}) if state=="clear"));
    }
    #[tokio::test(start_paused = true)]
    async fn slow_turn_uses_one_delayed_text_then_replaces_it_with_the_answer() {
        use super::super::tests::{complete, delta};
        let b = Arc::new(FakeBroker::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(quiet_turn(b.clone()).run(rx));
        settle_tasks().await;
        tokio::time::advance(Duration::from_secs(14)).await;
        settle_tasks().await;
        assert!(!b
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|op| matches!(op, Operation::Send { .. })));
        tokio::time::advance(Duration::from_secs(2)).await;
        settle_tasks().await;
        assert!(b
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|op| matches!(op,Operation::Send{text,..} if text=="正在处理你的请求…")));
        tx.send(delta("实际结果")).unwrap();
        tx.send(complete("end_turn")).unwrap();
        assert!(!task.await.unwrap().delivery_failed);
        let calls = b.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|op| matches!(op, Operation::Send { .. }))
                .count(),
            1
        );
        assert!(calls.iter().any(
            |op| matches!(op,Operation::Edit{key,text,..} if key=="final:0" && text=="实际结果")
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn tool_lifecycle_changes_small_reactions_without_leaking_tool_details() {
        use super::super::tests::{complete, delta, tool_call, tool_done};
        let b = Arc::new(FakeBroker::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(quiet_turn(b.clone()).run(rx));
        settle_tasks().await;
        tx.send(tool_call("one", "private command", Some("SECRET_PATH")))
            .unwrap();
        settle_tasks().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        settle_tasks().await;
        assert!(b
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|op| matches!(op,Operation::Reaction{state} if state=="tool")));
        tx.send(tool_done("one")).unwrap();
        tx.send(delta("A sufficiently long public reply for streaming"))
            .unwrap();
        settle_tasks().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        settle_tasks().await;
        tx.send(complete("end_turn")).unwrap();
        assert!(!task.await.unwrap().delivery_failed);
        let calls = b.calls.lock().unwrap();
        let text = serde_json::to_string(&*calls).unwrap();
        assert!(!text.contains("SECRET_PATH"));
        assert!(!text.contains("private command"));
        assert!(!text.contains("💭"));
        assert!(calls
            .iter()
            .any(|op| matches!(op,Operation::Reaction{state} if state=="writing")));
        let finish = calls
            .iter()
            .position(|op| matches!(op,Operation::Finish{outcome,..} if outcome=="completed"))
            .unwrap();
        let clear = calls
            .iter()
            .rposition(|op| matches!(op,Operation::Reaction{state} if state=="clear"))
            .unwrap();
        assert!(clear > finish);
    }
    #[tokio::test(start_paused = true)]
    async fn unavailable_reactions_do_not_fail_or_duplicate_the_reply() {
        use super::super::tests::{complete, delta, tool_call};
        let b = Arc::new(FakeBroker::default());
        b.fail_reactions.store(true, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(delta("先检查")).unwrap();
        tx.send(tool_call("t", "Bash", None)).unwrap();
        tx.send(delta("检查结束")).unwrap();
        tx.send(complete("end_turn")).unwrap();
        assert!(!quiet_turn(b.clone()).run(rx).await.delivery_failed);
        let calls = b.calls.lock().unwrap();
        assert!(calls.iter().any(|op|matches!(op,Operation::Send{text,key} if key=="final:0" && text=="先检查\n\n检查结束")));
        assert_eq!(
            calls
                .iter()
                .filter(|op| matches!(op, Operation::Send { .. }))
                .count(),
            1
        );
    }
    #[test]
    fn fenced_code_is_closed_for_each_preview_and_carried_to_the_next_chunk() {
        let mut fence = None;
        assert_eq!(
            fenced_chunk("```python\nprint('**literal**')", &mut fence),
            "```python\nprint('**literal**')\n```"
        );
        let saved = fence.clone();
        let mut preview = fence.clone();
        assert_eq!(fenced_chunk("next()", &mut preview), "```\nnext()\n```");
        assert_eq!(fence, saved);
        assert_eq!(
            fenced_chunk("last()\n```\n\n**done**", &mut fence),
            "```\nlast()\n```\n\n**done**"
        );
        assert!(fence.is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn approval_pauses_typing_and_resume_replaces_the_waiting_notice() {
        use super::super::tests::{complete, delta, env};
        use crate::acp::types::AcpEvent;
        let b = Arc::new(FakeBroker::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(quiet_turn(b.clone()).run(rx));
        settle_tasks().await;
        tx.send(env(AcpEvent::QuestionRequest {
            question_id: "question".into(),
            questions: vec![],
            form: None,
        }))
        .unwrap();
        settle_tasks().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        settle_tasks().await;
        assert!(b
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|op| matches!(op,Operation::Send{text,..} if text=="请在 MyClaw 中确认。")));
        assert!(b
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|op| matches!(op, Operation::Typing { active: false })));
        tx.send(env(AcpEvent::QuestionResolved {
            question_id: "question".into(),
        }))
        .unwrap();
        settle_tasks().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        settle_tasks().await;
        assert!(b
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|op| matches!(op,Operation::Edit{text,..} if text=="正在处理你的请求…")));
        tx.send(delta("完成")).unwrap();
        tx.send(complete("end_turn")).unwrap();
        assert!(!task.await.unwrap().delivery_failed);
    }
}
