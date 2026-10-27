//! Authenticated platform ingress. Inputs and credentials never enter SQLite.
//! The platform re-authorizes the durable job on every signed dispatch callback.
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::Duration;

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use super::turn_relay::{self, OutboundSpec, ResultFormat};
use crate::{
    app_state::AppState,
    db::{entities::app_metadata, service::app_metadata_service},
};

const CALLBACK: &str = "/api/codeg/channel-input";
const PREFIX: &str = "channel_input_operation:";
const PREFIX_END: &str = "channel_input_operation;";
const MAX_PENDING: usize = 256;
const MAX_SESSION_PENDING: usize = 32;
const MAX_INPUT_BYTES: usize = 1_048_576;
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(180);
const TURN_TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60 + 120);

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Source {
    pub channel_type: String,
    pub account_id: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Input {
    Text {
        text: String,
    },
    Command {
        name: String,
        arguments: String,
        #[serde(default)]
        recipient: Option<String>,
    },
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Request {
    pub request_id: String,
    pub session_ref: String,
    pub source: Source,
    pub input: Input,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_target_id: Option<String>,
    #[serde(default)]
    pub context: Value,
}
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ParsedCommand {
    pub name: &'static str,
    pub arguments: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipient: Option<String>,
}
/// Only an entire command message is recognized. `\model` is the portable text
/// form; `/model` and Telegram recipients remain compatible. A sentence mentioning
/// either form remains conversation. Keep the grammar aligned with MyClaw adapters.
pub fn parse_command(input: &Input) -> Option<ParsedCommand> {
    match input {
        Input::Command {
            name,
            arguments,
            recipient,
        } => {
            let name = name
                .strip_prefix('/')
                .or_else(|| name.strip_prefix('\\'))
                .unwrap_or(name);
            name.eq_ignore_ascii_case("model").then(|| ParsedCommand {
                name: "model",
                arguments: arguments.trim().into(),
                recipient: recipient.as_ref().map(|r| r.to_ascii_lowercase()),
            })
        }
        Input::Text { text } => {
            static RE: OnceLock<regex::Regex> = OnceLock::new();
            let re = RE.get_or_init(|| {
                regex::Regex::new(
                    r"(?is)^\s*(?:/model(?:@([A-Za-z0-9_]+))?|\\model)(?:\s+([\s\S]*))?\s*$",
                )
                .unwrap()
            });
            let caps = re.captures(text)?;
            Some(ParsedCommand {
                name: "model",
                arguments: caps.get(2).map_or("", |m| m.as_str()).trim().into(),
                recipient: caps.get(1).map(|m| m.as_str().to_ascii_lowercase()),
            })
        }
    }
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}
impl Status {
    fn terminal(self) -> bool {
        !matches!(self, Self::Queued | Self::Running)
    }
}
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub request_id: String,
    pub session_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_target_id: Option<String>,
    pub status: Status,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    pub cancel_requested: bool,
    #[serde(default)]
    pub delivery_unknown: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_session: Option<i32>,
    #[serde(default)]
    pub control_pending: bool,
}
#[derive(Clone, Deserialize, Serialize)]
struct Stored {
    process_id: String,
    request_hash: String,
    operation: Operation,
    source: Source,
    // Only platform entry and durable job IDs; never the original opaque context.
    completion_context: Value,
    #[serde(default)]
    completion_acknowledged: bool,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum Dispatch {
    #[serde(rename_all = "camelCase")]
    Turn {
        turn_id: String,
        #[serde(default)]
        connection_id: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Reply {
        text: String,
        status: String,
        #[serde(default)]
        outbound: Option<OutboundSpec>,
        #[serde(default)]
        response_url: Option<String>,
    },
    Runtime {
        runtime: Value,
        outbound: OutboundSpec,
    },
    Accepted {},
}
struct Pending {
    request: Request,
    stored: Stored,
    cancelled: Arc<AtomicBool>,
}
#[derive(Clone)]
struct Live {
    stored: Stored,
    cancelled: Arc<AtomicBool>,
}
#[derive(Default)]
struct Registry {
    queues: HashMap<String, VecDeque<Pending>>,
    workers: HashSet<String>,
    live: HashMap<String, Live>,
    blocked: HashMap<String, HashSet<String>>,
}
struct Engine {
    admission: tokio::sync::Mutex<()>,
    registry: Mutex<Registry>,
    completions: Mutex<HashMap<String, Stored>>,
    initialized: AtomicBool,
    notify: Notify,
    process_id: String,
}
impl Engine {
    fn new() -> Self {
        Self {
            admission: tokio::sync::Mutex::new(()),
            registry: Mutex::new(Registry::default()),
            completions: Mutex::new(HashMap::new()),
            initialized: AtomicBool::new(false),
            notify: Notify::new(),
            process_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}
fn engine() -> &'static Arc<Engine> {
    static ENGINE: OnceLock<Arc<Engine>> = OnceLock::new();
    ENGINE.get_or_init(|| Arc::new(Engine::new()))
}
/// Includes queued commands and legacy turns; platform decides which VM kinds
/// use this liveness signal. This does not enable sleeping on legacy instances.
pub fn active_operations() -> usize {
    let registry = engine().registry.lock().unwrap();
    registry.live.len() + registry.blocked.values().map(HashSet::len).sum::<usize>()
}
fn completion_context(value: &Value) -> Value {
    let mut safe = serde_json::Map::new();
    for key in ["entry", "jobId"] {
        if let Some(value) = value
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| s.len() <= 200)
        {
            safe.insert(key.into(), value.into());
        }
    }
    Value::Object(safe)
}
fn fingerprint(request: &Request) -> Result<String, &'static str> {
    let bytes = serde_json::to_vec(request).map_err(|_| "invalid_request")?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err("request_too_large");
    }
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn validate(request: &Request) -> Result<(), &'static str> {
    for (value, max) in [
        (&request.request_id, 200),
        (&request.session_ref, 500),
        (&request.source.channel_type, 50),
        (&request.source.account_id, 200),
    ] {
        if value.trim().is_empty() || value.len() > max || value.chars().any(char::is_control) {
            return Err("invalid_request");
        }
    }
    Ok(())
}
async fn persist(state: &AppState, stored: &Stored) -> Result<(), &'static str> {
    let json = serde_json::to_string(stored).map_err(|_| "operation_storage_unavailable")?;
    app_metadata_service::upsert_value(
        &state.db.conn,
        &format!("{PREFIX}{}", stored.operation.request_id),
        &json,
    )
    .await
    .map_err(|_| "operation_storage_unavailable")
}
async fn load(state: &AppState, id: &str) -> Result<Option<Stored>, &'static str> {
    app_metadata_service::get_value(&state.db.conn, &format!("{PREFIX}{id}"))
        .await
        .map_err(|_| "operation_storage_unavailable")?
        .map(|s| serde_json::from_str(&s).map_err(|_| "operation_storage_unavailable"))
        .transpose()
}
async fn records(state: &AppState) -> Result<Vec<Stored>, &'static str> {
    app_metadata::Entity::find()
        .filter(app_metadata::Column::Key.gte(PREFIX))
        .filter(app_metadata::Column::Key.lt(PREFIX_END))
        .filter(app_metadata::Column::DeletedAt.is_null())
        .all(&state.db.conn)
        .await
        .map_err(|_| "operation_storage_unavailable")?
        .into_iter()
        .map(|row| serde_json::from_str(&row.value).map_err(|_| "operation_storage_unavailable"))
        .collect()
}
impl Engine {
    /// Called under admission lock. Recovery never replays input or a callback.
    async fn initialize(self: &Arc<Self>, state: Arc<AppState>) -> Result<(), &'static str> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        for mut record in records(&state).await? {
            if !record.operation.status.terminal() {
                record.operation.status = Status::Unknown;
                record.operation.error_code = Some("process_restarted".into());
                record.operation.phase = "interrupted".into();
                record.completion_acknowledged = false;
                persist(&state, &record).await?;
            }
            if record.operation.status == Status::Unknown {
                if record.operation.control_pending {
                    if let Some(id) = record.operation.control_session {
                        controlled_conversations().lock().unwrap().insert(id);
                    }
                    if let Some(connection) = &record.operation.connection_id {
                        controlled_connections()
                            .lock()
                            .unwrap()
                            .insert(connection.clone());
                    }
                }
                self.registry
                    .lock()
                    .unwrap()
                    .blocked
                    .entry(record.operation.session_ref.clone())
                    .or_default()
                    .insert(record.operation.request_id.clone());
            }
            self.queue_completion(&record);
        }
        self.initialized.store(true, Ordering::Release);
        let this = self.clone();
        tokio::spawn(async move {
            this.deliver_completions(state).await;
        });
        Ok(())
    }
    async fn submit(
        self: &Arc<Self>,
        state: Arc<AppState>,
        request: Request,
    ) -> Result<Operation, &'static str> {
        validate(&request)?;
        if let Some(id) = request.callback_target_id.as_deref() {
            super::channel_callback::endpoint(&state.db.conn, id, &request.source.channel_type)
                .await
                .map_err(|_| "invalid_channel_callback_target")?;
        }
        let hash = fingerprint(&request)?;
        let _admission = self.admission.lock().await;
        self.initialize(state.clone()).await?;
        if let Some(existing) = load(&state, &request.request_id).await? {
            if existing.request_hash != hash {
                return Err("request_id_conflict");
            }
            return Ok(existing.operation);
        }
        {
            let registry = self.registry.lock().unwrap();
            if registry.blocked.contains_key(&request.session_ref) {
                return Err("prior_operation_unknown");
            }
            if registry.live.len() >= MAX_PENDING
                || registry
                    .live
                    .values()
                    .filter(|live| live.stored.operation.session_ref == request.session_ref)
                    .count()
                    >= MAX_SESSION_PENDING
            {
                return Err("channel_queue_full");
            }
        }
        let operation = Operation {
            request_id: request.request_id.clone(),
            session_ref: request.session_ref.clone(),
            callback_target_id: request.callback_target_id.clone(),
            status: Status::Queued,
            phase: "waiting".into(),
            turn_id: None,
            connection_id: None,
            error_code: None,
            cancel_requested: false,
            delivery_unknown: false,
            control_session: None,
            control_pending: false,
        };
        let stored = Stored {
            process_id: self.process_id.clone(),
            request_hash: hash,
            operation: operation.clone(),
            source: request.source.clone(),
            completion_context: completion_context(&request.context),
            completion_acknowledged: false,
        };
        persist(&state, &stored).await?;
        let session = request.session_ref.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let start = {
            let mut registry = self.registry.lock().unwrap();
            registry.live.insert(
                request.request_id.clone(),
                Live {
                    stored: stored.clone(),
                    cancelled: cancelled.clone(),
                },
            );
            registry
                .queues
                .entry(session.clone())
                .or_default()
                .push_back(Pending {
                    request,
                    stored,
                    cancelled,
                });
            registry.workers.insert(session.clone())
        };
        if start {
            let this = self.clone();
            tokio::spawn(async move {
                this.worker(state, session).await;
            });
        }
        Ok(operation)
    }
    fn queue_completion(&self, stored: &Stored) {
        if stored.operation.status.terminal() && !stored.completion_acknowledged {
            self.completions
                .lock()
                .unwrap()
                .insert(stored.operation.request_id.clone(), stored.clone());
        }
    }
    async fn update(&self, state: &AppState, pending: &mut Pending) -> Result<(), &'static str> {
        pending.stored.operation.cancel_requested = pending.cancelled.load(Ordering::Acquire);
        persist(state, &pending.stored).await?;
        self.queue_completion(&pending.stored);
        if let Some(live) = self
            .registry
            .lock()
            .unwrap()
            .live
            .get_mut(&pending.request.request_id)
        {
            live.stored = pending.stored.clone();
        }
        Ok(())
    }
    async fn worker(self: Arc<Self>, state: Arc<AppState>, session: String) {
        loop {
            let next = {
                let mut registry = self.registry.lock().unwrap();
                let item = registry
                    .queues
                    .get_mut(&session)
                    .and_then(VecDeque::pop_front);
                if item.is_none() {
                    registry.workers.remove(&session);
                    registry.queues.remove(&session);
                }
                item
            };
            let Some(mut pending) = next else {
                break;
            };
            let blocked = self.registry.lock().unwrap().blocked.contains_key(&session);
            let result = if blocked {
                Err((Status::Failed, "prior_operation_unknown"))
            } else if pending.cancelled.load(Ordering::Acquire) {
                Err((Status::Cancelled, "cancelled_before_dispatch"))
            } else {
                self.execute(&state, &mut pending).await
            };
            match result {
                Ok(status) => pending.stored.operation.status = status,
                Err((status, code)) => {
                    pending.stored.operation.status = status;
                    pending.stored.operation.error_code = Some(code.into());
                }
            }
            pending.stored.operation.phase = "finished".into();
            if self.update(&state, &mut pending).await.is_err() {
                pending.stored.operation.status = Status::Unknown;
                pending.stored.operation.error_code = Some("operation_storage_unavailable".into());
                let _ = self.update(&state, &mut pending).await;
            }
            {
                let mut registry = self.registry.lock().unwrap();
                if pending.stored.operation.status == Status::Unknown {
                    registry
                        .blocked
                        .entry(session.clone())
                        .or_default()
                        .insert(pending.request.request_id.clone());
                }
                registry.live.remove(&pending.request.request_id);
            }
            self.notify.notify_one();
        }
    }
    async fn execute(
        &self,
        state: &Arc<AppState>,
        pending: &mut Pending,
    ) -> Result<Status, (Status, &'static str)> {
        let parsed = parse_command(&pending.request.input);
        pending.stored.operation.status = Status::Running;
        self.update(state, pending)
            .await
            .map_err(|e| (Status::Failed, e))?;
        if parsed.is_some()
            && matches!(
                pending.request.context["entry"].as_str(),
                Some("managed" | "native_model")
            )
        {
            // A prior queued message may have created a backing session. The
            // platform verifies the durable creator/target before resolving it.
            let mut body = serde_json::to_value(&pending.request)
                .map_err(|_| (Status::Failed, "invalid_request"))?;
            body["phase"] = "resolve".into();
            body["parsedCommand"] =
                serde_json::to_value(&parsed).map_err(|_| (Status::Failed, "invalid_request"))?;
            let response = super::channel_callback::post_signed(
                &state.db,
                pending.stored.operation.callback_target_id.as_deref(),
                &pending.stored.source.channel_type,
                CALLBACK,
                body,
                Duration::from_secs(30),
            )
            .await
            .map_err(|_| (Status::Failed, "session_resolution_failed"))?;
            let session = response.get("sessionKey").filter(|value| {
                value.is_null()
                    || value
                        .as_str()
                        .is_some_and(|key| !key.is_empty() && key.len() <= 512)
            });
            if response["kind"] != "resolved" || session.is_none() {
                return Err((Status::Failed, "invalid_session_resolution"));
            }
            let session = session.unwrap().clone();
            pending.request.context["sessionKey"] = session.clone();
            pending.request.context["modelSessionKey"] = session;
        }
        let runtime_session = pending
            .request
            .context
            .get("sessionKey")
            .and_then(Value::as_str)
            .unwrap_or(&pending.request.session_ref);
        let connection = wait_existing(state, runtime_session, &pending.cancelled).await?;
        // Do not hold this lock while waiting for a turn: cancellation needs it.
        // The model callback may set configuration/disconnect, but never prompts.
        let is_command = parsed.is_some();
        let mut control = if is_command {
            acquire_control(
                state,
                connection.as_deref(),
                runtime_session
                    .strip_prefix("codeg:")
                    .and_then(|id| id.parse::<i32>().ok()),
                &pending.cancelled,
            )
            .await?
        } else {
            None
        };
        if pending.cancelled.load(Ordering::Acquire) {
            return Err((Status::Cancelled, "cancelled_before_dispatch"));
        }
        pending.stored.operation.control_pending = control.is_some();
        pending.stored.operation.control_session =
            control.as_ref().and_then(|guard| guard.conversation_id);
        if is_command {
            // Discovery can race a reconnect; persist the actual guarded
            // connection so read-only reconciliation releases that marker.
            pending.stored.operation.connection_id = control
                .as_ref()
                .and_then(|guard| {
                    (!guard.connection_id.is_empty()).then(|| guard.connection_id.clone())
                })
                .or(connection.clone());
        }
        pending.stored.operation.phase = "callback".into();
        self.update(state, pending)
            .await
            .map_err(|e| (Status::Failed, e))?;
        let mut body = serde_json::to_value(&pending.request)
            .map_err(|_| (Status::Failed, "invalid_request"))?;
        body["parsedCommand"] =
            serde_json::to_value(parsed).map_err(|_| (Status::Failed, "invalid_request"))?;
        body["phase"] = "dispatch".into();
        let response = super::channel_callback::post_signed(
            &state.db,
            pending.stored.operation.callback_target_id.as_deref(),
            &pending.stored.source.channel_type,
            CALLBACK,
            body,
            CALLBACK_TIMEOUT,
        )
        .await;
        let dispatch = response
            .ok()
            .and_then(|response| serde_json::from_value::<Dispatch>(response).ok());
        let valid_command_reply = !is_command
            || matches!(&dispatch, Some(Dispatch::Reply { status, .. }) if matches!(status.as_str(), "completed" | "failed" | "ignored"));
        let Some(dispatch) = dispatch.filter(|_| valid_command_reply) else {
            // The platform may still be applying a model after our HTTP timeout.
            // Release the mutex for cancellation, but keep admission suppressed
            // until a read-only status query proves the operation's outcome.
            if let Some(guard) = control.as_mut() {
                guard.retain_uncertainty = true;
            }
            return Err((Status::Unknown, "platform_dispatch_unknown"));
        };
        pending.stored.operation.control_session = None;
        pending.stored.operation.control_pending = false;
        drop(control);
        match dispatch {
            Dispatch::Turn {
                turn_id,
                connection_id,
            } => {
                if turn_id.trim().is_empty() || turn_id.len() > 200 {
                    return Err((Status::Unknown, "invalid_platform_response"));
                }
                pending.stored.operation.turn_id = Some(turn_id.clone());
                pending.stored.operation.connection_id = connection_id;
                pending.stored.operation.phase = "turn".into();
                self.update(state, pending)
                    .await
                    .map_err(|e| (Status::Unknown, e))?;
                wait_turn(state, &turn_id, &pending.cancelled).await
            }
            Dispatch::Runtime { runtime, outbound } => {
                pending.stored.operation.turn_id = Some(outbound.turn_id().to_string());
                pending.stored.operation.phase = "turn".into();
                self.update(state, pending)
                    .await
                    .map_err(|e| (Status::Unknown, e))?;
                let result = super::legacy_runtime::execute(
                    state.clone(),
                    runtime,
                    outbound,
                    pending.cancelled.clone(),
                )
                .await
                .map_err(|reason| {
                    if reason == "cancelled_before_dispatch" {
                        (Status::Cancelled, "cancelled_before_dispatch")
                    } else if super::legacy_runtime::is_known_preflight_failure(&reason) {
                        (Status::Failed, "runtime_preflight_rejected")
                    } else {
                        (Status::Unknown, "runtime_outcome_unknown")
                    }
                })?;
                Ok(receipt_status(&result))
            }
            Dispatch::Reply {
                text,
                status,
                outbound,
                response_url,
            } => {
                let intended = match status.as_str() {
                    "completed" | "ignored" => Status::Completed,
                    "failed" => Status::Failed,
                    _ => return Err((Status::Unknown, "invalid_platform_response")),
                };
                if status == "ignored" {
                    return Ok(intended);
                }
                pending.stored.operation.phase = "reply".into();
                if let Some(outbound) = outbound {
                    pending.stored.operation.turn_id = Some(outbound.turn_id().to_string());
                    self.update(state, pending)
                        .await
                        .map_err(|e| (Status::Unknown, e))?;
                    let result = turn_relay::deliver_reply(state, outbound, text)
                        .await
                        .map_err(|_| (Status::Unknown, "reply_outcome_unknown"))?;
                    let delivered = receipt_status(&result);
                    if delivered != Status::Completed {
                        return Ok(delivered);
                    }
                } else if let Some(url) = response_url {
                    self.update(state, pending)
                        .await
                        .map_err(|e| (Status::Unknown, e))?;
                    send_slack_response(&url, &text).await?;
                } else {
                    return Err((Status::Failed, "reply_target_missing"));
                }
                Ok(intended)
            }
            Dispatch::Accepted {} => {
                // Group fan-out is owned by its platform job; this acknowledges
                // dispatch only, not completion of all independently relayed turns.
                Ok(Status::Completed)
            }
        }
    }
    async fn deliver_completions(self: Arc<Self>, state: Arc<AppState>) {
        loop {
            let mut retry = false;
            // Rebuild this retry set once on startup; do not re-read
            // every historical operation whenever one turn completes.
            let records: Vec<Stored> = self.completions.lock().unwrap().values().cloned().collect();
            for stored in records {
                if !stored.operation.status.terminal() || stored.completion_acknowledged {
                    continue;
                }
                let mut outcome = json!({"status":stored.operation.status,"errorCode":stored.operation.error_code,"turnId":stored.operation.turn_id,"deliveryUnknown":stored.operation.delivery_unknown});
                if let Some(turn) = &stored.operation.turn_id {
                    if let Ok(Some(receipt)) =
                        turn_relay::get_result(&state.db.conn, turn, ResultFormat::CompactV1).await
                    {
                        outcome["result"] = receipt;
                    }
                }
                let mut body = json!({"phase":"complete","requestId":stored.operation.request_id,"sessionRef":stored.operation.session_ref,"source":stored.source,"context":stored.completion_context,"outcome":outcome});
                if let Some(id) = &stored.operation.callback_target_id {
                    body["callbackTargetId"] = id.clone().into();
                }
                if super::channel_callback::post_signed(
                    &state.db,
                    stored.operation.callback_target_id.as_deref(),
                    &stored.source.channel_type,
                    CALLBACK,
                    body,
                    Duration::from_secs(30),
                )
                .await
                .is_ok()
                {
                    // A status reconciliation may have advanced this
                    // record while its older notification was in flight.
                    let _admission = self.admission.lock().await;
                    match load(&state, &stored.operation.request_id).await {
                        Ok(Some(mut current)) if current.operation == stored.operation => {
                            current.completion_acknowledged = true;
                            if persist(&state, &current).await.is_err() {
                                retry = true;
                            } else {
                                self.completions
                                    .lock()
                                    .unwrap()
                                    .remove(&stored.operation.request_id);
                            }
                        }
                        _ => {
                            retry = true;
                        }
                    }
                } else {
                    retry = true;
                }
            }
            if retry {
                tokio::select! { _ = self.notify.notified() => {}, _ = tokio::time::sleep(Duration::from_secs(30)) => {} }
            } else {
                self.notify.notified().await;
            }
        }
    }
}
fn receipt_status(receipt: &Value) -> Status {
    if receipt["delivery_status"] == "unknown" || receipt["execution_status"] == "unknown" {
        return Status::Unknown;
    }
    if receipt["outcome"] == "cancelled" {
        return Status::Cancelled;
    }
    if receipt["delivery_status"] == "delivered" && receipt["outcome"] == "completed" {
        Status::Completed
    } else {
        Status::Failed
    }
}
async fn wait_turn(
    state: &AppState,
    turn: &str,
    cancelled: &AtomicBool,
) -> Result<Status, (Status, &'static str)> {
    let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
    let mut cancellation_sent = false;
    let mut events = super::relay_outbox::subscribe_results();
    let mut query = true;
    loop {
        if query {
            if let Some(receipt) =
                turn_relay::get_result(&state.db.conn, turn, ResultFormat::CompactV1)
                    .await
                    .map_err(|_| (Status::Unknown, "relay_storage_unavailable"))?
            {
                return Ok(receipt_status(&receipt));
            }
            query = false;
        }
        if cancelled.load(Ordering::Acquire) && !cancellation_sent {
            turn_relay::cancel_turn(state, turn)
                .await
                .map_err(|_| (Status::Unknown, "cancel_outcome_unknown"))?;
            cancellation_sent = true;
        }
        tokio::select! {
            changed = events.changed() => { if changed.is_err() { return Err((Status::Unknown,"relay_event_stream_closed")); } query = true; }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            _ = tokio::time::sleep_until(deadline) => return Err((Status::Unknown, "relay_result_timeout")),
        }
    }
}
async fn session_connection(state: &AppState, session: &str) -> Option<String> {
    let id = session.strip_prefix("codeg:")?.parse::<i32>().ok()?;
    state
        .connection_manager
        .find_connection_by_conversation_id(id)
        .await
}
async fn wait_existing(
    state: &AppState,
    session: &str,
    cancelled: &AtomicBool,
) -> Result<Option<String>, (Status, &'static str)> {
    let mut events = state.acp_event_bus.subscribe();
    let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err((Status::Cancelled, "cancelled_before_dispatch"));
        }
        if session
            .strip_prefix("codeg:")
            .and_then(|id| id.parse::<i32>().ok())
            .is_some_and(|id| controlled_conversations().lock().unwrap().contains(&id))
        {
            return Err((Status::Failed, "session_control_in_progress"));
        }
        let connection = session_connection(state, session).await;
        let Some(id) = connection else {
            return Ok(None);
        };
        let busy = match state.connection_manager.get_state(&id).await {
            Some(s) => s.read().await.turn_in_flight,
            None => false,
        };
        if !busy {
            return Ok(Some(id));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err((Status::Failed, "session_wait_timeout"));
        }
        // Broadcast lag/closure is reconciled from authoritative session state.
        tokio::select! { _ = events.recv() => {}, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
    }
}
type ConversationGate = (i32, tokio::sync::OwnedMutexGuard<()>);
fn conversation_gate(id: i32) -> Arc<tokio::sync::Mutex<()>> {
    type Gates = Mutex<HashMap<i32, std::sync::Weak<tokio::sync::Mutex<()>>>>;
    static GATES: OnceLock<Gates> = OnceLock::new();
    let mut gates = GATES.get_or_init(Default::default).lock().unwrap();
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(&id).and_then(std::sync::Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    gates.insert(id, Arc::downgrade(&gate));
    gate
}
/// Lock order is ALWAYS connection prompt_lock, then conversation gate. The
/// latter closes the race across reconnects with different connection IDs.
/// Callers without a connection take only the conversation gate and never
/// acquire a prompt_lock while holding it.
pub(crate) async fn prompt_admission_gate(
    manager: &crate::acp::manager::ConnectionManager,
    connection: &str,
    supplied: Option<i32>,
) -> Option<ConversationGate> {
    let linked = match manager.get_state(connection).await {
        Some(state) => state.read().await.conversation_id,
        None => None,
    };
    let id = linked.or(supplied)?;
    Some((id, conversation_gate(id).lock_owned().await))
}
async fn conversation_busy(manager: &crate::acp::manager::ConnectionManager, id: i32) -> bool {
    let states: Vec<_> = manager
        .connections
        .lock()
        .await
        .values()
        .map(|connection| connection.state.clone())
        .collect();
    for state in states {
        let state = state.read().await;
        if state.conversation_id == Some(id) && state.turn_in_flight {
            return true;
        }
    }
    false
}
async fn control_lock(
    lock: Arc<tokio::sync::Mutex<()>>,
    cancelled: &AtomicBool,
    deadline: tokio::time::Instant,
) -> Result<tokio::sync::OwnedMutexGuard<()>, (Status, &'static str)> {
    let acquire = lock.lock_owned();
    tokio::pin!(acquire);
    loop {
        tokio::select! {
            guard = &mut acquire => return Ok(guard),
            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                if cancelled.load(Ordering::Acquire) { return Err((Status::Cancelled, "cancelled_before_dispatch")); }
                if tokio::time::Instant::now() >= deadline { return Err((Status::Failed, "session_wait_timeout")); }
            }
        }
    }
}
fn controlled_connections() -> &'static Mutex<HashSet<String>> {
    static CONTROLS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    CONTROLS.get_or_init(|| Mutex::new(HashSet::new()))
}
fn controlled_conversations() -> &'static Mutex<HashSet<i32>> {
    static CONTROLS: OnceLock<Mutex<HashSet<i32>>> = OnceLock::new();
    CONTROLS.get_or_init(|| Mutex::new(HashSet::new()))
}
/// Covers reconnects whose connection ID changed while the same native codeg
/// conversation has an uncertain model operation.
pub(crate) async fn control_blocks(
    manager: &crate::acp::manager::ConnectionManager,
    connection: &str,
    conversation: Option<i32>,
) -> bool {
    if control_active(connection)
        || conversation.is_some_and(|id| controlled_conversations().lock().unwrap().contains(&id))
    {
        return true;
    }
    let Some(state) = manager.get_state(connection).await else {
        return false;
    };
    let conversation = state.read().await.conversation_id;
    conversation.is_some_and(|id| controlled_conversations().lock().unwrap().contains(&id))
}
/// A cheap admission hint for existing Web/ACP callers. The prompt lock remains
/// the authority; this permits a caller to reject early instead of waiting on a
/// platform model callback. It is independent from instance sleep policy.
pub(crate) fn control_active(connection_id: &str) -> bool {
    controlled_connections()
        .lock()
        .unwrap()
        .contains(connection_id)
}
struct ControlGuard {
    connection_id: String,
    conversation_id: Option<i32>,
    retain_uncertainty: bool,
    _prompt_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    _conversation_guard: Option<ConversationGate>,
}
impl Drop for ControlGuard {
    fn drop(&mut self) {
        if self.retain_uncertainty {
            return;
        }
        controlled_connections()
            .lock()
            .unwrap()
            .remove(&self.connection_id);
        if let Some(id) = self.conversation_id {
            controlled_conversations().lock().unwrap().remove(&id);
        }
    }
}
async fn acquire_control(
    state: &AppState,
    initial_connection: Option<&str>,
    conversation: Option<i32>,
    cancelled: &AtomicBool,
) -> Result<Option<ControlGuard>, (Status, &'static str)> {
    let mut events = state.acp_event_bus.subscribe();
    let deadline = tokio::time::Instant::now() + TURN_TIMEOUT;
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err((Status::Cancelled, "cancelled_before_dispatch"));
        }
        let connection = if let Some(id) = conversation {
            state
                .connection_manager
                .find_connection_by_conversation_id(id)
                .await
        } else {
            initial_connection.map(str::to_owned)
        };
        if conversation.is_some_and(|id| controlled_conversations().lock().unwrap().contains(&id)) {
            return Err((Status::Failed, "session_control_in_progress"));
        }
        if let Some(connection) = &connection {
            if control_blocks(&state.connection_manager, connection, conversation).await {
                return Err((Status::Failed, "session_control_in_progress"));
            }
        }
        let prompt_guard = if let Some(connection) = &connection {
            match state.connection_manager.clone_prompt_lock(connection).await {
                Ok(lock) => Some(control_lock(lock, cancelled, deadline).await?),
                Err(_) => None,
            }
        } else {
            None
        };
        let (busy, conversation_id) = if let Some(connection) = &connection {
            match state.connection_manager.get_state(connection).await {
                Some(session) => {
                    let session = session.read().await;
                    (
                        session.turn_in_flight,
                        session.conversation_id.or(conversation),
                    )
                }
                None => (false, conversation),
            }
        } else {
            (false, conversation)
        };
        if connection.is_none() && conversation_id.is_none() {
            return Ok(None);
        }
        let conversation_guard = if let Some(id) = conversation_id {
            Some((
                id,
                control_lock(conversation_gate(id), cancelled, deadline).await?,
            ))
        } else {
            None
        };
        // A reconnect may have admitted a prompt before this gate was acquired.
        // Check every connection for the exact conversation under the gate.
        let busy = busy
            || match conversation_id {
                Some(id) => conversation_busy(&state.connection_manager, id).await,
                None => false,
            };
        if !busy {
            if let Some(id) = conversation_id {
                if !controlled_conversations().lock().unwrap().insert(id) {
                    return Err((Status::Failed, "session_control_in_progress"));
                }
            }
            if let Some(connection) = &connection {
                if !controlled_connections()
                    .lock()
                    .unwrap()
                    .insert(connection.clone())
                {
                    if let Some(id) = conversation_id {
                        controlled_conversations().lock().unwrap().remove(&id);
                    }
                    return Err((Status::Failed, "session_control_in_progress"));
                }
            }
            return Ok(Some(ControlGuard {
                connection_id: connection.unwrap_or_default(),
                conversation_id,
                retain_uncertainty: false,
                _prompt_guard: prompt_guard,
                _conversation_guard: conversation_guard,
            }));
        }
        drop(conversation_guard);
        drop(prompt_guard);
        if tokio::time::Instant::now() >= deadline {
            return Err((Status::Failed, "session_wait_timeout"));
        }
        tokio::select! { _ = events.recv() => {}, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
    }
}
fn valid_slack_response_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && matches!(
                url.host_str(),
                Some("hooks.slack.com" | "hooks.slack-gov.com")
            )
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && url.path().starts_with("/commands/")
            && url.fragment().is_none()
    })
}
async fn send_slack_response(url: &str, text: &str) -> Result<(), (Status, &'static str)> {
    if !valid_slack_response_url(url) {
        return Err((Status::Failed, "invalid_slack_response_url"));
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| (Status::Failed, "http_client_unavailable"))?;
    let response = client
        .post(url)
        .json(&json!({"response_type":"ephemeral","replace_original":false,"text":text}))
        .send()
        .await
        .map_err(|_| (Status::Unknown, "reply_outcome_unknown"))?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err((Status::Unknown, "reply_outcome_unknown"))
    }
}
/// Recover unfinished operations and retry safe completion notifications on boot.
/// Original inputs/credentials are unavailable after restart and never replayed.
pub async fn start(state: Arc<AppState>) -> Result<(), &'static str> {
    let this = engine();
    let _admission = this.admission.lock().await;
    this.initialize(state).await
}
pub async fn submit(state: Arc<AppState>, request: Request) -> Result<Operation, &'static str> {
    engine().submit(state, request).await
}
pub async fn get(state: Arc<AppState>, id: &str) -> Result<Option<Operation>, &'static str> {
    if id.is_empty() || id.len() > 200 {
        return Err("invalid_request");
    }
    let this = engine();
    let _admission = this.admission.lock().await;
    this.initialize(state.clone()).await?;
    let Some(mut stored) = load(&state, id).await? else {
        return Ok(None);
    };
    if stored.operation.status == Status::Unknown {
        reconcile_unknown(this, &state, &mut stored).await?;
    }
    Ok(Some(stored.operation))
}

async fn reconcile_unknown(
    this: &Engine,
    state: &AppState,
    stored: &mut Stored,
) -> Result<(), &'static str> {
    let mut resolved = None;
    if let Some(turn) = &stored.operation.turn_id {
        if let Some(result) = turn_relay::get_result(&state.db.conn, turn, ResultFormat::CompactV1)
            .await
            .map_err(|_| "relay_storage_unavailable")?
        {
            let status = receipt_status(&result);
            if status != Status::Unknown {
                resolved = Some((status, false));
            }
        }
    }
    if resolved.is_none() {
        let mut body = json!({"phase":"status","requestId":stored.operation.request_id,"sessionRef":stored.operation.session_ref,"source":stored.source,"context":stored.completion_context});
        if let Some(id) = &stored.operation.callback_target_id {
            body["callbackTargetId"] = id.clone().into();
        }
        if let Ok(response) = super::channel_callback::post_signed(
            &state.db,
            stored.operation.callback_target_id.as_deref(),
            &stored.source.channel_type,
            CALLBACK,
            body,
            Duration::from_secs(15),
        )
        .await
        {
            if response["kind"] == "status" {
                if let Some(turn) = response["turnId"]
                    .as_str()
                    .filter(|s| !s.is_empty() && s.len() <= 200)
                {
                    stored.operation.turn_id = Some(turn.to_string());
                    persist(state, stored).await?;
                    if let Some(result) =
                        turn_relay::get_result(&state.db.conn, turn, ResultFormat::CompactV1)
                            .await
                            .map_err(|_| "relay_storage_unavailable")?
                    {
                        let status = receipt_status(&result);
                        if status != Status::Unknown {
                            resolved = Some((status, false));
                        }
                    }
                }
                if resolved.is_none() {
                    resolved = match response["status"].as_str() {
                        Some("completed") => Some((Status::Completed, true)),
                        Some("failed") => Some((Status::Failed, true)),
                        _ => None,
                    };
                }
            }
        }
    }
    if let Some((status, delivery_unknown)) = resolved {
        stored.operation.status = status;
        stored.operation.delivery_unknown = delivery_unknown;
        stored.operation.error_code = delivery_unknown.then(|| "delivery_outcome_unknown".into());
        let control_session = stored.operation.control_session.take();
        let control_pending = std::mem::take(&mut stored.operation.control_pending);
        stored.operation.phase = "reconciled".into();
        stored.completion_acknowledged = false;
        persist(state, stored).await?;
        this.queue_completion(stored);
        if control_pending {
            if let Some(id) = control_session {
                controlled_conversations().lock().unwrap().remove(&id);
            }
            if let Some(connection) = &stored.operation.connection_id {
                controlled_connections().lock().unwrap().remove(connection);
            }
        }
        let mut registry = this.registry.lock().unwrap();
        if let Some(blocked) = registry.blocked.get_mut(&stored.operation.session_ref) {
            blocked.remove(&stored.operation.request_id);
            if blocked.is_empty() {
                registry.blocked.remove(&stored.operation.session_ref);
            }
        }
        drop(registry);
        this.notify.notify_one();
    }
    Ok(())
}
pub async fn cancel(state: Arc<AppState>, id: &str) -> Result<Option<Operation>, &'static str> {
    let _admission = engine().admission.lock().await;
    engine().initialize(state.clone()).await?;
    let live = engine().registry.lock().unwrap().live.get(id).cloned();
    if let Some(mut live) = live {
        live.cancelled.store(true, Ordering::Release);
        live.stored.operation.cancel_requested = true;
        // Worker owns durable phase/status; do not race its terminal persistence.
        return Ok(Some(live.stored.operation));
    }
    Ok(load(&state, id).await?.map(|s| s.operation))
}

#[cfg(test)]
mod queue_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_commands_only() {
        for text in [
            "/model",
            "/MODEL@My_Bot provider/model",
            r"\model",
            r"\MODEL provider/model",
            r" \model list 2 ",
        ] {
            assert!(parse_command(&Input::Text { text: text.into() }).is_some());
        }
        for text in [
            "我想知道可不可以切换模型，比如/model命令",
            "What is /model?",
            r"我想知道能否用 \model 切换模型",
            "Explain\n\\model provider/model",
            r"`\model`",
            "```\n\\model provider/model\n```",
            r"\models",
            r"\model.json",
            r"\model@My_Bot",
            r"\\model",
            r"\new",
            "!model",
            "!model provider/model",
            "!new",
            "`/model`",
            "/models",
            "/new",
            "/model.json",
            "/model@wrong-bot",
        ] {
            assert!(
                parse_command(&Input::Text { text: text.into() }).is_none(),
                "{text}"
            );
        }
        assert_eq!(
            parse_command(&Input::Text {
                text: "/model@My_Bot a/b".into()
            })
            .unwrap(),
            ParsedCommand {
                name: "model",
                arguments: "a/b".into(),
                recipient: Some("my_bot".into())
            }
        );
        assert_eq!(
            parse_command(&Input::Text {
                text: " \\model list 2 \n".into()
            }),
            Some(ParsedCommand {
                name: "model",
                arguments: "list 2".into(),
                recipient: None,
            })
        );
    }
    #[test]
    fn native_and_text_have_same_command() {
        let native = Input::Command {
            name: "/model".into(),
            arguments: " a/b ".into(),
            recipient: Some("My_Bot".into()),
        };
        assert_eq!(
            parse_command(&native),
            parse_command(&Input::Text {
                text: "/model@My_Bot a/b".into()
            })
        );
    }
    #[test]
    fn portable_text_and_compatible_inputs_have_the_same_command() {
        let expected = ParsedCommand {
            name: "model",
            arguments: "provider/model".into(),
            recipient: None,
        };
        for text in [r"\model provider/model", "/model provider/model"] {
            assert_eq!(
                parse_command(&Input::Text { text: text.into() }),
                Some(expected.clone()),
            );
        }
        for name in ["model", r"\model", r"\MODEL", "/model"] {
            assert_eq!(
                parse_command(&Input::Command {
                    name: name.into(),
                    arguments: " provider/model ".into(),
                    recipient: None,
                }),
                Some(expected.clone()),
            );
        }
        for name in [
            "models",
            r"\models",
            r"\\model",
            r"\model@My_Bot",
            "!model",
            "!models",
            "/new",
            "model provider/model",
        ] {
            assert!(parse_command(&Input::Command {
                name: name.into(),
                arguments: String::new(),
                recipient: None,
            })
            .is_none());
        }
    }
    #[test]
    fn completion_context_cannot_store_credentials() {
        assert_eq!(
            completion_context(
                &json!({"entry":"legacy_slack","jobId":"j","token":"secret","responseUrl":"secret","message":"private"})
            ),
            json!({"entry":"legacy_slack","jobId":"j"})
        );
    }
    #[test]
    fn slack_reply_is_strictly_scoped() {
        assert!(valid_slack_response_url(
            "https://hooks.slack.com/commands/T/B/token"
        ));
        for url in [
            "http://hooks.slack.com/commands/a",
            "https://hooks.slack.com.evil.test/commands/a",
            "https://evil.test/commands/a",
            "https://user@hooks.slack.com/commands/a",
            "https://hooks.slack.com:444/commands/a",
            "https://hooks.slack.com/services/a",
            "https://hooks.slack.com/commands/a#x",
        ] {
            assert!(!valid_slack_response_url(url), "{url}");
        }
    }
    #[test]
    fn failed_or_uncertain_delivery_is_not_success() {
        assert_eq!(
            receipt_status(&json!({"delivery_status":"unknown","outcome":"completed"})),
            Status::Unknown
        );
        assert_eq!(
            receipt_status(&json!({"delivery_status":"failed","outcome":"completed"})),
            Status::Failed
        );
        assert_eq!(
            receipt_status(&json!({"delivery_status":"delivered","outcome":"cancelled"})),
            Status::Cancelled
        );
        assert_eq!(
            receipt_status(&json!({"delivery_status":"delivered","outcome":"completed"})),
            Status::Completed
        );
    }
}
