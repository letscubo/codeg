//! Codeg owns the turn, pacing and typing. The platform proxy holds WAHA credentials.
use super::{
    classify, tool_status, Composer, Outcome, PublicEvent, RelayInput, TurnReport, MAX_TURN,
};
use async_trait::async_trait;
use serde::Serialize;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
const UPDATE: Duration = Duration::from_secs(2);
const TYPING: Duration = Duration::from_secs(4);
const EDIT_WINDOW: Duration = Duration::from_secs(14 * 60);
const MAX_BYTES: usize = 256 * 1024;

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
    Finish {
        outcome: String,
        operations: Vec<String>,
    },
}
pub(super) fn endpoint(urls: &[String], turn_id: &str) -> Result<reqwest::Url, &'static str> {
    let id = uuid::Uuid::parse_str(turn_id).map_err(|_| "invalid turn id")?;
    let origins: BTreeSet<_> = urls
        .iter()
        .filter_map(|raw| {
            let url = reqwest::Url::parse(raw).ok()?;
            (url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/api/codeg/events"
                && url.fragment().is_none())
            .then(|| url.origin().ascii_serialization())
        })
        .collect();
    if origins.len() != 1 {
        return Err("one trusted webhook origin required");
    }
    // Existing platform webhooks carry vmId/s in their query. Derive only the
    // trusted origin; never copy those credentials to the relay endpoint.
    reqwest::Url::parse(&format!(
        "{}/api/channels/whatsapp/relay/{id}",
        origins.first().unwrap()
    ))
    .map_err(|_| "invalid proxy origin")
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
struct Output {
    broker: Arc<dyn Broker>,
    live: Option<String>,
    created: Instant,
    shown: String,
    index: usize,
    edit: usize,
    finals: Vec<String>,
}
impl Output {
    async fn show(&mut self, text: String, final_part: bool) -> bool {
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
}
impl WhatsappTurn {
    pub(super) async fn run(self, mut rx: mpsc::UnboundedReceiver<RelayInput>) -> TurnReport {
        let (typing, receiver) = watch::channel(true);
        let pulse = tokio::spawn(typing_pump(self.broker.clone(), receiver));
        let mut out = Output {
            broker: self.broker.clone(),
            live: None,
            created: Instant::now(),
            shown: String::new(),
            index: 0,
            edit: 0,
            finals: vec![],
        };
        let mut composer = Composer::default();
        let mut bytes = 0usize;
        let mut tools = BTreeSet::new();
        let mut waiting = BTreeSet::new();
        let mut status = "💭".to_owned();
        let mut dirty = false;
        let mut last = Instant::now();
        let deadline = Instant::now() + MAX_TURN;
        let mut delivery_failed = !out.show(status.clone(), false).await;
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
                                    for head in composer.push_whatsapp(text){if !out.show(head,true).await{delivery_failed=true;break;}}
                                    if delivery_failed{break Outcome::Failed;}
                                }
                                PublicEvent::ToolStarted{id,name,..}=>{tools.insert(id.to_owned());status=tool_status(None,name.unwrap_or("Tool")).unwrap_or_default().chars().take(80).collect();}
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
                        }
                    },
                    _=tokio::time::sleep_until(next), if dirty=>{
                        let note=if !waiting.is_empty(){"⏸"}else if !tools.is_empty(){&status}else if composer.text().is_empty(){"💭"}else{"✍️"};
                        let text=if composer.text().is_empty(){note.to_owned()}else{format!("{}\n\n{note}",composer.text())};
                        if !out.show(text,false).await{delivery_failed=true;break Outcome::Failed;}dirty=false;last=Instant::now();
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
        block_once: AtomicBool,
        started: Notify,
        release: Notify,
    }
    #[async_trait]
    impl Broker for FakeBroker {
        async fn execute(&self, op: &Operation) -> Result<Option<String>, BrokerError> {
            self.calls.lock().unwrap().push(op.clone());
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
    fn proxy_origin_is_fixed_https_and_operation_has_no_recipient_credentials() {
        let id = "11111111-1111-4111-8111-111111111111";
        assert!(endpoint(&["https://platform.invalid/api/codeg/events".into()], id).is_ok());
        assert!(endpoint(&["http://platform.invalid/api/codeg/events".into()], id).is_err());
        assert!(endpoint(
            &[
                "https://a.invalid/api/codeg/events".into(),
                "https://b.invalid/api/codeg/events".into()
            ],
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
    fn legacy_webhook_query_is_supported_but_never_forwarded() {
        let id = "11111111-1111-4111-8111-111111111111";
        let url = endpoint(
            &[
                "https://platform.invalid/api/codeg/events?vmId=test-vm&s=private-test-secret"
                    .into(),
            ],
            id,
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            format!("https://platform.invalid/api/channels/whatsapp/relay/{id}")
        );
        assert!(url.query().is_none());
        assert!(!url.as_str().contains("private-test-secret"));
        assert!(endpoint(
            &["https://user:secret@platform.invalid/api/codeg/events?vmId=test&s=x".into()],
            id
        )
        .is_err());
        assert!(endpoint(
            &["https://platform.invalid/api/codeg/events?s=x#fragment".into()],
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
}
