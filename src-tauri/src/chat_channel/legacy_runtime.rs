//! Existing runtime adapters. Native session identities and configuration stay owned by
//! their runtime; this module only translates execution events into the channel relay.
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::turn_relay::{self, OutboundSpec, ResultFormat};
use crate::acp::types::{AcpEvent, ConnectionStatus, EventEnvelope, PromptInputBlock};
use crate::app_state::AppState;
use crate::models::agent::AgentType;

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum RuntimeRequest {
    #[serde(rename = "legacy_openclaw", rename_all = "camelCase")]
    OpenClaw {
        session_key: String,
        message: String,
        working_dir: Option<String>,
        #[serde(default)]
        runtime_env: BTreeMap<String, String>,
        #[serde(default = "yes")]
        require_existing: bool,
    },
    #[serde(rename = "legacy_hermes_runs")]
    HermesRuns(HermesRunsRequest),
    #[serde(rename = "legacy_hermes_native")]
    HermesNative(HermesNativeRequest),
}

fn yes() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HermesRunsRequest {
    api_url: String,
    api_key: String,
    proxy_token: Option<String>,
    session_key: String,
    message: String,
    #[serde(default)]
    history: Vec<HistoryMessage>,
    system_prompt: Option<String>,
    model: String,
    provider: String,
    runtime_version: String,
}

#[derive(Deserialize)]
struct HistoryMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HermesNativeRequest {
    hermes_home: String,
    python_path: String,
    session_id: String,
    #[serde(default)]
    session_key: Option<String>,
    #[serde(default)]
    transport: Option<String>,
    message: String,
    model: Option<String>,
    provider: Option<String>,
    #[serde(default)]
    native_delivery_disabled: bool,
}

pub async fn execute(
    state: Arc<AppState>,
    request: Value,
    outbound: OutboundSpec,
    cancelled: Arc<AtomicBool>,
) -> Result<Value, String> {
    let version = match &outbound {
        OutboundSpec::Telegram {
            delivery_version, ..
        }
        | OutboundSpec::Slack {
            delivery_version, ..
        }
        | OutboundSpec::WhatsappAgent {
            delivery_version, ..
        } => *delivery_version,
    };
    if version < 2 {
        return Err("legacy_runtime_requires_delivery_v2".into());
    }
    let request: RuntimeRequest = serde_json::from_value(request)
        .map_err(|_| "invalid_legacy_runtime_request".to_string())?;
    if cancelled.load(Ordering::Acquire) {
        return Err("cancelled_before_dispatch".into());
    }
    match request {
        RuntimeRequest::OpenClaw {
            session_key,
            message,
            working_dir,
            mut runtime_env,
            require_existing,
        } => {
            if session_key.trim().is_empty() || message.trim().is_empty() {
                return Err("invalid_legacy_runtime_request".into());
            }
            let original_cli = std::env::var("CODEG_LEGACY_OPENCLAW_CLI").unwrap_or_default();
            if !std::path::Path::new(&original_cli).is_absolute()
                || !std::path::Path::new(&original_cli).is_file()
            {
                return Err("legacy_openclaw_cli_not_configured".into());
            }
            runtime_env.insert("OPENCLAW_ACP_COMMAND".into(), original_cli);
            runtime_env.insert("CODEG_LEGACY_OPENCLAW_BRIDGE".into(), "1".into());
            // The official `openclaw acp --session` bridge retains the native key.
            // Never allow inherited agent settings to reset that transcript.
            runtime_env.insert("OPENCLAW_SESSION_KEY".into(), session_key);
            runtime_env.insert("OPENCLAW_RESET_SESSION".into(), "0".into());
            runtime_env.insert(
                "OPENCLAW_REQUIRE_EXISTING".into(),
                if require_existing { "1" } else { "0" }.into(),
            );
            let connection_id = state
                .connection_manager
                .spawn_agent(
                    AgentType::OpenClaw,
                    working_dir,
                    None,
                    runtime_env,
                    "legacy-channel".into(),
                    state.emitter.clone(),
                    None,
                    BTreeMap::new(),
                )
                .await
                .map_err(|_| "legacy_runtime_spawn_failed".to_string())?;
            let result =
                execute_openclaw(&state, &connection_id, message, outbound, &cancelled).await;
            let _ = state.connection_manager.disconnect(&connection_id).await;
            result
        }
        RuntimeRequest::HermesRuns(request) => {
            execute_hermes(&state, request, outbound, &cancelled).await
        }
        RuntimeRequest::HermesNative(request) => {
            execute_native(&state, request, outbound, &cancelled).await
        }
    }
}

async fn execute_openclaw(
    state: &AppState,
    connection_id: &str,
    message: String,
    outbound: OutboundSpec,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    let start = tokio::time::Instant::now();
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err("cancelled_before_dispatch".into());
        }
        let session = state
            .connection_manager
            .get_state(connection_id)
            .await
            .ok_or("legacy_runtime_connection_lost")?;
        let session = session.read().await;
        match session.status {
            ConnectionStatus::Connected => break,
            ConnectionStatus::Disconnected | ConnectionStatus::Error => {
                return Err("legacy_runtime_connect_failed".into())
            }
            _ => {}
        }
        drop(session);
        if start.elapsed() > Duration::from_secs(90) {
            return Err("legacy_runtime_connect_timeout".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let turn_id = outbound.turn_id().to_owned();
    turn_relay::register_for_prompt(state, connection_id, None, outbound).await?;
    if let Err(error) = state
        .connection_manager
        .send_prompt(
            connection_id,
            vec![PromptInputBlock::Text { text: message }],
        )
        .await
    {
        turn_relay::abort(connection_id, &turn_id);
        return Err(error.to_string());
    }
    let mut stop_sent = false;
    let mut results = super::relay_outbox::subscribe_results();
    let mut changed = true;
    loop {
        if cancelled.load(Ordering::Acquire) && !stop_sent {
            state
                .connection_manager
                .cancel(&state.db.conn, connection_id)
                .await
                .map_err(|e| e.to_string())?;
            stop_sent = true;
        }
        if changed {
            if let Some(result) =
                turn_relay::get_result(&state.db.conn, &turn_id, ResultFormat::Legacy).await?
            {
                return Ok(result);
            }
        }
        changed = tokio::select! {
            _ = results.changed() => true,
            _ = tokio::time::sleep(Duration::from_millis(100)) => false,
        };
    }
}

fn supports_request_model(version: &str) -> bool {
    let numbers: Vec<u32> = version
        .trim()
        .trim_start_matches('v')
        .split(['.', '-', '+'])
        .take(3)
        .map(str::parse)
        .collect::<Result<_, _>>()
        .unwrap_or_default();
    matches!(numbers.as_slice(), [0, minor, patch] if (*minor, *patch) >= (19, 1))
        || matches!(numbers.as_slice(), [year, month, day] if *year >= 2026 && (*year, *month, *day) >= (2026, 7, 30))
}

fn validate_api_url(raw: &str) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(raw).map_err(|_| "invalid_hermes_api_url")?;
    // Authenticated platform supplies the endpoint, but credentials must never be
    // forwarded through a redirect or embedded URL username/password.
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("invalid_hermes_api_url".into());
    }
    Ok(url)
}

fn run_input(request: &HermesRunsRequest) -> Result<String, String> {
    let mut lines = Vec::new();
    let mut size = 0;
    for message in request.history.iter().rev() {
        if !matches!(message.role.as_str(), "user" | "assistant") {
            return Err("invalid_history_role".into());
        }
        size += message.content.len();
        if size > 120_000 {
            break;
        }
        lines.push(format!("{}: {}", message.role, message.content));
    }
    lines.reverse();
    if lines.is_empty() {
        return Ok(request.message.clone());
    }
    Ok(format!("[Conversation so far — context only; continue it naturally and do NOT re-answer previous turns]\n{}\n[End of conversation history]\n\n{}", lines.join("\n"), request.message))
}

struct RelayEvents<'a> {
    state: &'a AppState,
    connection: String,
    seq: u64,
    text: String,
    segment: String,
    tool_seq: u64,
    tools: BTreeMap<String, String>,
}
impl RelayEvents<'_> {
    fn emit(&mut self, payload: AcpEvent) {
        self.seq += 1;
        self.state.acp_event_bus.send(Arc::new(EventEnvelope {
            seq: self.seq,
            connection_id: self.connection.clone(),
            payload,
        }));
    }
    fn text(&mut self, text: &str) {
        self.text.push_str(text);
        self.segment.push_str(text);
        self.emit(AcpEvent::ContentDelta {
            text: text.into(),
            parent_tool_use_id: None,
        });
    }
    fn event(&mut self, value: &Value) -> Result<Option<&'static str>, String> {
        match value["event"].as_str().unwrap_or("") {
            "message.delta" => {
                if let Some(text) = value["delta"].as_str() {
                    self.text(text);
                }
            }
            "message.interim" if value["already_streamed"].as_bool() != Some(true) => {
                if let Some(text) = value["text"].as_str() {
                    self.text(text);
                }
            }
            // Actual reasoning is never relayed as answer content.
            "reasoning.available" => self.emit(AcpEvent::Thinking {
                text: String::new(),
                parent_tool_use_id: None,
            }),
            "tool.started" => {
                self.segment.clear();
                self.tool_seq += 1;
                let tool = value["tool"].as_str().unwrap_or("tool");
                let id = value["tool_id"]
                    .as_str()
                    .or_else(|| value["tool_call_id"].as_str())
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("{}-{}", self.connection, self.tool_seq));
                self.tools.insert(id.clone(), tool.into());
                self.emit(AcpEvent::ToolCall {
                    tool_call_id: id,
                    title: tool.into(),
                    kind: "other".into(),
                    status: "in_progress".into(),
                    tool_name: Some(tool.into()),
                    description: None,
                    content: None,
                    raw_input: None,
                    raw_output: None,
                    locations: None,
                    meta: None,
                    images: None,
                });
            }
            "tool.completed" => {
                let id = value["tool_id"]
                    .as_str()
                    .or_else(|| value["tool_call_id"].as_str())
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .or_else(|| {
                        self.tools
                            .iter()
                            .find(|(_, name)| value["tool"].as_str() == Some(name.as_str()))
                            .map(|(id, _)| id.clone())
                    });
                if let Some(id) = id {
                    self.tools.remove(&id);
                    self.emit(AcpEvent::ToolCallUpdate {
                        tool_call_id: id,
                        title: None,
                        tool_name: None,
                        description: None,
                        status: Some(
                            if value["failed"].as_bool() == Some(true)
                                || value["error"].as_bool() == Some(true)
                            {
                                "failed"
                            } else {
                                "completed"
                            }
                            .into(),
                        ),
                        content: None,
                        raw_input: None,
                        raw_output: None,
                        raw_output_append: None,
                        locations: None,
                        meta: None,
                        images: None,
                    });
                }
            }
            "run.completed" => {
                self.final_text(value["output"].as_str().unwrap_or(""))?;
                return Ok(Some("end_turn"));
            }
            "run.cancelled" => return Ok(Some("cancelled")),
            "run.failed" => return Err("hermes_run_failed".into()),
            "run.interrupted" => return Err("hermes_run_interrupted".into()),
            "replay.truncated" => return Err("hermes_event_replay_gap".into()),
            _ => {}
        }
        Ok(None)
    }
    fn final_text(&mut self, output: &str) -> Result<(), String> {
        if output.is_empty() || self.text.ends_with(output) {
            return Ok(());
        }
        if let Some(rest) = output.strip_prefix(&self.text) {
            let rest = rest.to_owned();
            self.text(&rest);
            return Ok(());
        }
        if let Some(rest) = output.strip_prefix(&self.segment) {
            let rest = rest.to_owned();
            self.text(&rest);
            return Ok(());
        }
        // Never append a whole final snapshot to already streamed content.
        if output
            .to_ascii_lowercase()
            .contains("response truncated due to output length limit")
            && self.text.len() > output.len()
        {
            return Ok(());
        }
        Err("hermes_final_snapshot_changed".into())
    }
}

async fn execute_hermes(
    state: &AppState,
    request: HermesRunsRequest,
    outbound: OutboundSpec,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    if !supports_request_model(&request.runtime_version) {
        return Err("hermes_request_model_unsupported".into());
    }
    if request.api_key.is_empty()
        || request.model.trim().is_empty()
        || request.provider.trim().is_empty()
        || request.session_key.trim().is_empty()
        || request.message.trim().is_empty()
    {
        return Err("invalid_hermes_runtime_request".into());
    }
    validate_api_url(&request.api_url)?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| "hermes_client_failed")?;
    let base = request.api_url.trim_end_matches('/');
    let auth = |builder: reqwest::RequestBuilder| {
        let builder = builder
            .bearer_auth(&request.api_key)
            .header("X-Hermes-Session-Key", &request.session_key);
        match &request.proxy_token {
            Some(token) => builder.header("X-Hermes-Proxy-Token", token),
            None => builder,
        }
    };
    let capabilities = auth(client.get(format!("{base}/v1/capabilities")))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|_| "hermes_capabilities_unavailable")?
        .error_for_status()
        .map_err(|_| "hermes_capabilities_unavailable")?
        .json::<Value>()
        .await
        .map_err(|_| "hermes_capabilities_invalid")?;
    if !["run_submission", "run_events_sse", "run_status", "run_stop"]
        .iter()
        .all(|flag| capabilities["features"][flag].as_bool() == Some(true))
    {
        return Err("hermes_runs_unsupported".into());
    }
    let text = run_input(&request)?;
    let turn_id = outbound.turn_id().to_owned();
    let connection = format!("legacy-hermes-{turn_id}");
    turn_relay::register_external(state, &connection, outbound).await?;
    let mut events = RelayEvents {
        state,
        connection,
        seq: 0,
        text: String::new(),
        segment: String::new(),
        tool_seq: 0,
        tools: BTreeMap::new(),
    };
    let execution: Result<String, String> = async {
        if cancelled.load(Ordering::Acquire) { return Ok("cancelled".into()); }
        let response = auth(client.post(format!("{base}/v1/runs")))
            .header("Idempotency-Key", &turn_id).timeout(Duration::from_secs(30))
            .json(&json!({"input":text,"model":request.model,"provider":request.provider,"instructions":request.system_prompt}))
            .send().await.map_err(|_| "hermes_admission_unknown")?;
        if !response.status().is_success() {
            return Err(if response.status().is_client_error() {"hermes_admission_rejected"} else {"hermes_admission_unknown"}.into());
        }
        let response = response.json::<Value>().await.map_err(|_| "hermes_admission_unknown")?;
        let run_id = response["run_id"].as_str().filter(|id| !id.is_empty()).ok_or("hermes_admission_unknown")?;
        let run_url = format!("{base}/v1/runs/{}", url_segment(run_id));
        let mut stopped = false;
        if let Ok(Ok(response)) = tokio::time::timeout(Duration::from_secs(30),
            auth(client.get(format!("{run_url}/events"))).send()).await {
            if response.status().is_success() {
                let mut stream = response.bytes_stream();
                let mut pending = Vec::new();
                let mut last_bytes = tokio::time::Instant::now();
                loop {
                    tokio::select! {
                        next = stream.next() => {
                            let Some(Ok(bytes)) = next else { break; };
                            last_bytes = tokio::time::Instant::now();
                            pending.extend_from_slice(&bytes);
                            if pending.len() > 2 * 1024 * 1024 { break; }
                            while let Some(index) = pending.iter().position(|byte| *byte == b'\n') {
                                let line: Vec<u8> = pending.drain(..=index).collect();
                                let Ok(line) = std::str::from_utf8(&line) else { return Err("hermes_invalid_sse_utf8".into()); };
                                if let Some(data) = line.trim().strip_prefix("data:") {
                                    let value: Value = serde_json::from_str(data.trim()).map_err(|_| "hermes_invalid_sse_event")?;
                                    if let Some(outcome) = events.event(&value)? { return Ok(outcome.into()); }
                                }
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {
                            // Official Runs SSE emits keepalive comments every 10 s.
                            // A silent transport must fall back to observing the admitted run.
                            if cancelled.load(Ordering::Acquire) || last_bytes.elapsed() > Duration::from_secs(45) { break; }
                        }
                    }
                }
            }
        }
        // A detached SSE stream does not stop Hermes. Observe the accepted run;
        // never submit a replacement and never infer completion from EOF.
        let mut last_observed = tokio::time::Instant::now();
        loop {
            if cancelled.load(Ordering::Acquire) && !stopped {
                auth(client.post(format!("{run_url}/stop"))).timeout(Duration::from_secs(10))
                    .send().await.map_err(|_| "hermes_stop_unknown")?
                    .error_for_status().map_err(|_| "hermes_stop_rejected")?;
                stopped = true;
            }
            let status = auth(client.get(&run_url)).timeout(Duration::from_secs(10)).send().await;
            if let Ok(response) = status {
                if response.status().is_success() {
                    if let Ok(mut value) = response.json::<Value>().await {
                        let status = value["status"].as_str().unwrap_or("").to_owned();
                        if status == "unknown" { return Err("hermes_execution_unknown".into()); }
                        if matches!(status.as_str(), "queued"|"running"|"waiting_for_approval"|"stopping"|"completed"|"cancelled"|"interrupted"|"failed") {
                            last_observed = tokio::time::Instant::now();
                        }
                        value["event"] = json!(format!("run.{status}"));
                        if let Some(outcome) = events.event(&value)? { return Ok(outcome.into()); }
                    }
                }
            }
            if last_observed.elapsed() > Duration::from_secs(60) {return Err("hermes_execution_unknown".into());}
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }.await;
    match execution {
        Ok(stop_reason) => events.emit(AcpEvent::TurnComplete {
            session_id: request.session_key,
            stop_reason,
            agent_type: "hermes".into(),
            duration_ms: None,
        }),
        Err(code) => events.emit(runtime_error(code)),
    }
    wait_result(state, &turn_id).await
}

async fn wait_result(state: &AppState, turn_id: &str) -> Result<Value, String> {
    let mut results = super::relay_outbox::subscribe_results();
    loop {
        if let Some(result) =
            turn_relay::get_result(&state.db.conn, turn_id, ResultFormat::Legacy).await?
        {
            return Ok(result);
        }
        results
            .changed()
            .await
            .map_err(|_| "relay_result_unavailable")?;
    }
}

const HERMES_GATEWAY_BRIDGE: &str = include_str!("legacy_hermes_gateway.py");
const HERMES_HTTP_BRIDGE: &str = include_str!("legacy_hermes_http.py");

fn hermes_bridge_source() -> String {
    format!("{HERMES_HTTP_BRIDGE}\n{HERMES_GATEWAY_BRIDGE}")
}

/// Inspect/resolve use deployment-owned paths. No caller-selected runtime or credentials.
pub async fn legacy_hermes_action(action: &str, params: &Value) -> Result<Value, String> {
    if !matches!(action, "inspect" | "resolve") {
        return Err("hermes_invalid_action".into());
    }
    let home = std::env::var("CODEG_LEGACY_HERMES_HOME").unwrap_or_default();
    let python = std::env::var("CODEG_LEGACY_HERMES_PYTHON").unwrap_or_default();
    if !std::path::Path::new(&home).is_absolute() || !std::path::Path::new(&python).is_absolute() {
        return Err("hermes_native_paths_not_configured".into());
    }
    let mut payload = params.as_object().cloned().unwrap_or_default();
    payload.insert("action".into(), json!(action));
    payload.insert("hermesHome".into(), json!(&home));
    payload.insert("pythonPath".into(), json!(&python));
    let mut child = tokio::process::Command::new(&python)
        .args(["-u", "-c"])
        .arg(hermes_bridge_source())
        .env("HERMES_HOME", &home)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| "hermes_python_unavailable")?;
    let mut input = child.stdin.take().ok_or("hermes_bridge_unavailable")?;
    input
        .write_all(format!("{}\n", Value::Object(payload)).as_bytes())
        .await
        .map_err(|_| "hermes_bridge_unavailable")?;
    drop(input);
    let mut lines = BufReader::new(child.stdout.take().ok_or("hermes_bridge_unavailable")?).lines();
    let result = match tokio::time::timeout(Duration::from_secs(60), lines.next_line()).await {
        Ok(Ok(Some(line))) if line.len() <= 8 * 1024 * 1024 => {
            match serde_json::from_str::<Value>(&line) {
                Ok(frame) if frame["event"] == action => Ok(frame["result"].clone()),
                Ok(frame) => Err(bridge_error(&frame)),
                Err(_) => Err("hermes_invalid_bridge_frame".into()),
            }
        }
        _ => Err("hermes_native_inspection_unavailable".into()),
    };
    let _ = child.kill().await;
    result
}

pub async fn inspect_hermes() -> Result<Value, String> {
    match legacy_hermes_action("inspect", &json!({})).await {
        Ok(value) => Ok(value),
        Err(reason) => Ok(
            json!({"kind":"hermes","transport":"hermes_http", "compatible":false,
            "ready":false,"nativeDeliveryDisabled":false,"reasonCodes":[reason],"sessions":[]}),
        ),
    }
}

async fn execute_native(
    state: &AppState,
    request: HermesNativeRequest,
    outbound: OutboundSpec,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    if !request.native_delivery_disabled {
        return Err("hermes_native_delivery_not_disabled".into());
    }
    if !std::path::Path::new(&request.python_path).is_absolute()
        || !std::path::Path::new(&request.hermes_home).is_absolute()
        || [&request.session_id, &request.message]
            .iter()
            .any(|v| v.trim().is_empty())
    {
        return Err("invalid_hermes_native_request".into());
    }
    let turn_id = outbound.turn_id().to_owned();
    let mut child = tokio::process::Command::new(&request.python_path)
        .args(["-u", "-c"])
        .arg(hermes_bridge_source())
        .env("HERMES_HOME", &request.hermes_home)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        // Runtime imports/logging may contain config or credentials; never retain it.
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| "hermes_python_unavailable")?;
    let mut input = child.stdin.take().ok_or("hermes_bridge_unavailable")?;
    let mut output =
        BufReader::new(child.stdout.take().ok_or("hermes_bridge_unavailable")?).lines();
    let initial = native_bridge_request(&request, &turn_id);
    input
        .write_all(format!("{initial}\n").as_bytes())
        .await
        .map_err(|_| "hermes_bridge_unavailable")?;
    let ready = tokio::time::timeout(Duration::from_secs(60), output.next_line())
        .await
        .map_err(|_| "hermes_gateway_preflight_timeout")?
        .map_err(|_| "hermes_bridge_unavailable")?
        .ok_or("hermes_canonical_gateway_unsupported")?;
    let ready: Value = serde_json::from_str(&ready).map_err(|_| "hermes_invalid_bridge_frame")?;
    if ready["event"] != "ready" {
        return Err(bridge_error(&ready));
    }
    if cancelled.load(Ordering::Acquire) {
        return Err("cancelled_before_dispatch".into());
    }
    let connection = format!("legacy-hermes-native-{turn_id}");
    turn_relay::register_external(state, &connection, outbound).await?;
    let mut events = RelayEvents {
        state,
        connection,
        seq: 0,
        text: String::new(),
        segment: String::new(),
        tool_seq: 0,
        tools: BTreeMap::new(),
    };
    let execution: Result<String,String> = async {
        input.write_all(b"{\"action\":\"submit\"}\n").await.map_err(|_|"hermes_admission_unknown")?;
        let mut stopped = false;
        loop {
            tokio::select! {
                line = output.next_line() => {
                    let line = line.map_err(|_|"hermes_execution_unknown")?.ok_or("hermes_execution_unknown")?;
                    if line.len() > 8*1024*1024 {return Err("hermes_invalid_bridge_frame".into());}
                    let frame: Value = serde_json::from_str(&line).map_err(|_|"hermes_invalid_bridge_frame")?;
                    if frame["event"] == "bridge.error" {return Err(bridge_error(&frame));}
                    if frame["event"] == "run.accepted" {
                        let run_id = frame["run_id"].as_str().filter(|id| !id.is_empty() && id.len() <= 200)
                            .ok_or("hermes_invalid_bridge_frame")?;
                        let native_id = frame["session_id"].as_str().filter(|id| !id.is_empty() && id.len() <= 512)
                            .ok_or("hermes_invalid_bridge_frame")?;
                        // One checkpoint per accepted run, no tokens or message bodies.
                        crate::db::service::app_metadata_service::upsert_value(&state.db.conn,
                            &format!("hermes_http_run:{turn_id}"),
                            &json!({"runId":run_id,"nativeSessionId":native_id}).to_string()).await
                            .map_err(|_| "hermes_run_checkpoint_failed")?;
                        continue;
                    }
                    if let Some(outcome) = events.event(&frame)? {return Ok(outcome.into());}
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    if cancelled.load(Ordering::Acquire) && !stopped {
                        input.write_all(b"{\"action\":\"cancel\"}\n").await.map_err(|_|"hermes_stop_unknown")?;
                        stopped = true;
                    }
                }
            }
        }
    }.await;
    match execution {
        Ok(stop_reason) => events.emit(AcpEvent::TurnComplete {
            session_id: request.session_id,
            stop_reason,
            agent_type: "hermes".into(),
            duration_ms: None,
        }),
        Err(code) => events.emit(runtime_error(code)),
    }
    // Completion comes from the runtime terminal event/observed status, never child process exit.
    let _ = child.kill().await;
    wait_result(state, &turn_id).await
}

fn native_bridge_request(request: &HermesNativeRequest, turn_id: &str) -> Value {
    json!({"hermesHome":request.hermes_home,"pythonPath":request.python_path,
        "sessionId":request.session_id,"message":request.message,
        "model":request.model,"provider":request.provider,"sessionKey":request.session_key,
        "transport":request.transport,"nativeDeliveryDisabled":request.native_delivery_disabled,
        "turnId":turn_id})
}

fn bridge_error(frame: &Value) -> String {
    frame["code"]
        .as_str()
        .filter(|code| {
            code.starts_with("hermes_")
                && code.len() <= 80
                && code.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
        })
        .unwrap_or("hermes_gateway_failed")
        .into()
}

fn runtime_error(reason: String) -> AcpEvent {
    let code = if matches!(
        reason.as_str(),
        "hermes_run_failed" | "hermes_run_interrupted" | "hermes_admission_rejected"
    ) {
        reason.clone()
    } else {
        // A transport error never proves that the accepted runtime stopped.
        "runtime_outcome_unknown".into()
    };
    AcpEvent::Error {
        message: reason,
        agent_type: "hermes".into(),
        code: Some(code),
        details: None,
        terminal: true,
    }
}

/// These errors can escape `execute` only before runtime prompt admission.
/// Errors after registration are converted to a durable relay receipt instead.
pub(crate) fn is_known_preflight_failure(reason: &str) -> bool {
    matches!(
        reason,
        "legacy_runtime_requires_delivery_v2"
            | "invalid_legacy_runtime_request"
            | "cancelled_before_dispatch"
            | "legacy_runtime_spawn_failed"
            | "legacy_openclaw_cli_not_configured"
            | "legacy_runtime_connection_lost"
            | "legacy_runtime_connect_failed"
            | "legacy_runtime_connect_timeout"
            | "hermes_request_model_unsupported"
            | "invalid_hermes_runtime_request"
            | "invalid_hermes_api_url"
            | "hermes_client_failed"
            | "hermes_capabilities_unavailable"
            | "hermes_capabilities_invalid"
            | "hermes_runs_unsupported"
            | "invalid_history_role"
            | "hermes_native_delivery_not_disabled"
            | "invalid_hermes_native_request"
            | "hermes_python_unavailable"
            | "hermes_bridge_unavailable"
            | "hermes_gateway_preflight_timeout"
            | "hermes_canonical_gateway_unsupported"
            | "hermes_http_bridge_unsupported"
            | "hermes_invalid_bridge_frame"
            | "hermes_gateway_failed"
            | "hermes_home_unavailable"
            | "hermes_gateway_not_ready"
            | "hermes_authority_changed"
            | "hermes_session_not_found"
            | "hermes_native_adapter_still_present"
            | "hermes_status_unverified"
            | "hermes_status_timeout"
            | "hermes_invalid_status"
            | "hermes_api_not_configured"
            | "hermes_api_credential_unavailable"
            | "hermes_http_request_rejected"
            | "hermes_http_unavailable"
            | "hermes_invalid_http_response"
            | "hermes_channel_not_ready"
            | "hermes_session_identity_changed"
    )
}

fn url_segment(value: &str) -> String {
    // Use the URL library's path encoder rather than allowing a runtime ID to
    // introduce a different endpoint/query/fragment.
    let mut url = reqwest::Url::parse("http://localhost/").expect("literal URL");
    url.path_segments_mut().expect("base URL").push(value);
    url.path().trim_start_matches('/').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn hermes_stream_filters_reasoning_and_keeps_final_tool_segment_once() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let directory = tempfile::tempdir().unwrap();
        let state = AppState::new_for_test(db, directory.path().to_owned());
        let mut bus = state.acp_event_bus.subscribe();
        let mut events = RelayEvents {
            state: &state,
            connection: "fixture".into(),
            seq: 0,
            text: String::new(),
            segment: String::new(),
            tool_seq: 0,
            tools: BTreeMap::new(),
        };
        for frame in [
            json!({"event":"message.delta","delta":"Checking. "}),
            json!({"event":"reasoning.available","text":"private thought"}),
            json!({"event":"message.interim","text":"Checking. ","already_streamed":true}),
            json!({"event":"tool.started","tool":"shell","tool_call_id":"call"}),
            json!({"event":"tool.completed","tool":"shell","tool_call_id":"call","error":true}),
            json!({"event":"message.delta","delta":"Answ"}),
        ] {
            events.event(&frame).unwrap();
        }
        assert_eq!(
            events
                .event(&json!({"event":"run.completed","output":"Answer"}))
                .unwrap(),
            Some("end_turn")
        );
        let mut body = String::new();
        let mut lifecycle = Vec::new();
        while let Ok(frame) = bus.try_recv() {
            match &frame.payload {
                AcpEvent::ContentDelta { text, .. } => body.push_str(text),
                AcpEvent::Thinking { text, .. } => assert!(text.is_empty()),
                AcpEvent::ToolCall { tool_call_id, .. } => lifecycle.push(tool_call_id.clone()),
                AcpEvent::ToolCallUpdate {
                    tool_call_id,
                    status,
                    ..
                } => {
                    lifecycle.push(tool_call_id.clone());
                    assert_eq!(status.as_deref(), Some("failed"));
                }
                _ => {}
            }
        }
        assert_eq!(body, "Checking. Answer");
        assert_eq!(lifecycle, vec!["call", "call"]);
        assert!(events
            .event(&json!({"event":"run.completed","output":"Answer"}))
            .is_ok());
        assert!(
            bus.try_recv().is_err(),
            "duplicate final snapshot must not emit body"
        );
        assert!(events.event(&json!({"event":"replay.truncated"})).is_err());
    }
    #[test]
    fn transport_uncertainty_is_not_a_known_execution_failure() {
        let error = runtime_error("hermes_stop_unknown".into());
        assert!(
            matches!(error,AcpEvent::Error{code:Some(ref code),terminal:true,..} if code=="runtime_outcome_unknown")
        );
        assert!(!is_known_preflight_failure("hermes_stop_unknown"));
        assert!(is_known_preflight_failure(
            "hermes_canonical_gateway_unsupported"
        ));
    }
    #[test]
    fn model_override_gate_rejects_old_and_unknown_versions() {
        for version in ["", "0.19.0", "2026.7.29", "unknown"] {
            assert!(!supports_request_model(version));
        }
        for version in ["v0.19.1", "0.21.3", "v2026.7.30"] {
            assert!(supports_request_model(version));
        }
    }
    #[test]
    fn native_hermes_cannot_silently_fall_back_to_new_api_session() {
        assert!(serde_json::from_value::<RuntimeRequest>(
            json!({"kind":"legacy_hermes_native","sessionKey":"original"})
        )
        .is_err());
    }

    #[test]
    fn native_http_bridge_request_contains_runtime_paths_and_original_identity() {
        let request: HermesNativeRequest = serde_json::from_value(json!({
            "hermesHome":"/fixture/home", "pythonPath":"/fixture/venv/bin/python",
            "sessionId":"original-id", "sessionKey":"agent:main:telegram:dm:42",
            "transport":"hermes_http", "message":"hello", "model":"selected",
            "provider":"provider", "nativeDeliveryDisabled":true,
        }))
        .unwrap();
        let payload = native_bridge_request(&request, "stable-turn");
        assert_eq!(payload["pythonPath"], "/fixture/venv/bin/python");
        assert_eq!(payload["hermesHome"], "/fixture/home");
        assert_eq!(payload["sessionId"], "original-id");
        assert_eq!(payload["sessionKey"], "agent:main:telegram:dm:42");
        assert_eq!(payload["transport"], "hermes_http");
        assert_eq!(payload["turnId"], "stable-turn");
        assert_eq!(payload["nativeDeliveryDisabled"], true);
    }
    #[test]
    fn run_identifier_cannot_escape_its_path_segment() {
        assert_eq!(url_segment("a/b?c#d"), "a%2Fb%3Fc%23d");
    }
    #[test]
    fn endpoint_never_accepts_embedded_credentials_or_queries() {
        assert!(validate_api_url("http://127.0.0.1:8642").is_ok());
        for url in [
            "file:///etc/passwd",
            "https://secret@example.com",
            "https://example.com/?token=x",
        ] {
            assert!(validate_api_url(url).is_err());
        }
    }
}
