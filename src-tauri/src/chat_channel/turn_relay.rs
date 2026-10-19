//! Per-turn Telegram and Slack delivery. Tokens live only in worker memory.
//! `relay_turn_id` is a delivery claim, never a success acknowledgement. V2
//! consumers wait for the durable `relay_result` after the worker stops writing.
//! Unknown non-idempotent writes are never blindly replayed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::acp::internal_bus::InternalEventBus;
use crate::acp::types::{AcpEvent, ConnectionStatus, EventEnvelope};
use crate::db::service::app_metadata_service;

pub use super::relay_result::ResultFormat;
use super::tg_html;

/// 两次更新(草稿或编辑正文)的最小间隔。实测草稿 1/s 稳定、1.5/s 触发 429;
/// editMessageText 与它共享限额,同样按 1/s 节流(业界常见取 200–800ms,这里保守些)。
const MIN_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// 没有新字时续草稿的间隔。草稿不更新约 10 秒就消失(见文件头),取一半留余量。
const DRAFT_HEARTBEAT: Duration = Duration::from_secs(5);
/// 一轮最长跟多久;超过按失败收尾,防止连接异常没有结束事件时 worker 常驻。
pub(super) const MAX_TURN: Duration = Duration::from_secs(3 * 60 * 60);
/// 认领记录保留多久(给 event_subscriber 取;正常在 turn_complete 时就取走了)。
const CLAIM_TTL: Duration = Duration::from_secs(6 * 60 * 60);
/// 草稿 / 单条消息的字数上限。Telegram 是 4096(按 UTF-16 计),留余量给 emoji 之类双单元字符。
const TG_CHUNK: usize = 3800;
/// 最终消息 429 时的重试次数；每次完整遵守平台 retry_after。
const SEND_RETRIES: usize = 3;

// ── 入参 ────────────────────────────────────────────────────────────────────

/// `acp_prompt` / `cli_prompt` 的 `outbound` 参数。
#[derive(Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum OutboundSpec {
    #[serde(rename_all = "camelCase")]
    Telegram {
        /// 平台生成的本轮 id,原样写回 webhook(`relay_turn_id`)。
        turn_id: String,
        #[serde(default)]
        delivery_version: u8,
        #[serde(default)]
        result_format: ResultFormat,
        #[serde(default)]
        result_webhook_url: Option<String>,
        #[serde(flatten)]
        presentation: super::relay_input::Presentation,
        bot_token: String,
        chat_id: i64,
        /// 草稿 id 基数(非 0);不给就由 turn_id 派生。每段正文用 `基数 + 段号`。
        #[serde(default)]
        draft_id: Option<i64>,
        #[serde(default)]
        texts: RelayTexts,
    },
    #[serde(rename_all = "camelCase")]
    Slack {
        turn_id: String,
        #[serde(default)]
        delivery_version: u8,
        #[serde(default)]
        result_format: ResultFormat,
        #[serde(default)]
        result_webhook_url: Option<String>,
        #[serde(flatten)]
        presentation: super::relay_input::Presentation,
        bot_token: String,
        channel: String,
        thread_ts: String,
        recipient_user_id: String,
        recipient_team_id: String,
        #[serde(default)]
        texts: RelayTexts,
    },
}

impl std::fmt::Debug for OutboundSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutboundSpec::Telegram {
                turn_id, chat_id, ..
            } => f
                .debug_struct("Telegram")
                .field("turn_id", turn_id)
                .field("chat_id", chat_id)
                .field("bot_token", &"<redacted>")
                .finish(),
            OutboundSpec::Slack {
                turn_id,
                channel,
                thread_ts,
                ..
            } => f
                .debug_struct("Slack")
                .field("turn_id", turn_id)
                .field("channel", channel)
                .field("thread_ts", thread_ts)
                .field("bot_token", &"<redacted>")
                .finish(),
        }
    }
}

impl OutboundSpec {
    pub fn turn_id(&self) -> &str {
        match self {
            OutboundSpec::Telegram { turn_id, .. } | OutboundSpec::Slack { turn_id, .. } => turn_id,
        }
    }
}

/// 状态 / 收尾提示语。平台按用户语言给;没给用英文。
#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase", default)]
pub struct RelayTexts {
    /// 还没有正文、也没在跑工具时草稿显示的状态。
    pub thinking: String,
    pub failed: String,
    pub cancelled: String,
    pub empty: String,
}

impl Default for RelayTexts {
    fn default() -> Self {
        Self {
            thinking: "💭 Thinking…".into(),
            failed: "⚠️ Something went wrong while generating this reply. Please try again.".into(),
            cancelled: "⏹ Stopped.".into(),
            empty: "(No reply was generated.)".into(),
        }
    }
}

// ── 正文分段(纯逻辑,可测)─────────────────────────────────────────────────

/// 本轮正文。一条 Telegram 消息承载当前这块正文,**超长才另起一条**;工具调用不再断句
/// (状态行挂在这条消息末尾,见 `TelegramTurn::display`)。
#[derive(Default, Debug)]
pub(crate) struct Composer {
    text: String,
    index: u32,
}

impl Composer {
    /// 第几条消息(0 起)。
    #[cfg(test)]
    pub(crate) fn index(&self) -> u32 {
        self.index
    }

    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// 追加正文。超长时把要**定稿**的头部切出来返回(调用方把当前消息改成它,再开新消息),
    /// 余下留作新消息的开头。
    pub(crate) fn push(&mut self, delta: &str) -> Vec<String> {
        self.text.push_str(delta);
        let mut out = Vec::new();
        while self.text.encode_utf16().count() > TG_CHUNK {
            let (head, tail) = split_head(&self.text, TG_CHUNK);
            out.push(head);
            self.text = tail;
            self.index += 1;
        }
        out
    }
}

/// 按字符数切出不超过 `max` 的头部:优先在最后一个换行处切,没有换行就硬切。
fn split_head(text: &str, max: usize) -> (String, String) {
    let mut units = 0;
    let cut = text
        .char_indices()
        .find_map(|(i, c)| {
            units += c.len_utf16();
            (units > max).then_some(i)
        })
        .unwrap_or(text.len());
    let window = &text[..cut];
    let at = match window.rfind('\n') {
        Some(i) if i > 0 => i,
        _ => cut,
    };
    let head = text[..at].to_string();
    let tail = text[at..].to_string();
    (head, tail)
}

/// 由 turn_id 派生草稿 id 基数(1..2^30),留出空间给 `+ 段号`。
pub(crate) fn derive_draft_base(turn_id: &str) -> i64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in turn_id.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    ((h % (1 << 30)) as i64).max(1)
}

// ── Telegram 调用 ────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub(crate) enum TgError {
    /// 429:按 Telegram 给的秒数等。
    #[error("rate limited for {0}s")]
    RetryAfter(u64),
    #[error("{0}")]
    Other(String),
    #[error("delivery outcome unknown")]
    Unknown,
}

#[async_trait]
pub(crate) trait TgApi: Send + Sync {
    /// 成功时返回 `result.message_id`(草稿之类没有就是 `None`)。
    async fn call(&self, method: &str, body: serde_json::Value) -> Result<Option<i64>, TgError>;
}

struct HttpTg {
    client: reqwest::Client,
    token: String,
}

#[async_trait]
impl TgApi for HttpTg {
    async fn call(&self, method: &str, body: serde_json::Value) -> Result<Option<i64>, TgError> {
        let url = format!("https://api.telegram.org/bot{}/{}", self.token, method);
        let res = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            // reqwest 的错误文本会带上 URL(含 token),只留类别
            .map_err(|_| TgError::Unknown)?;
        if res.status().is_server_error() {
            return Err(TgError::Unknown);
        }
        let v: serde_json::Value = res.json().await.map_err(|_| TgError::Unknown)?;
        if v.get("ok").and_then(|o| o.as_bool()) == Some(true) {
            return Ok(v.pointer("/result/message_id").and_then(|m| m.as_i64()));
        }
        if let Some(ra) = v
            .pointer("/parameters/retry_after")
            .and_then(|r| r.as_u64())
        {
            return Err(TgError::RetryAfter(ra));
        }
        Err(TgError::Other(
            v.get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("unknown error")
                .replace(&self.token, "<redacted>"),
        ))
    }
}

// ── 一轮的 worker ────────────────────────────────────────────────────────────

pub(crate) enum RelayInput {
    Event(Arc<EventEnvelope>),
    /// 事件总线落后:正文可能缺字,停止推送,交回平台。
    Lagged,
    SilentComplete,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Completed,
    #[serde(rename = "completed")]
    Silent,
    Cancelled,
    Failed,
    /// Runtime ownership was lost; neither completion nor cancellation is proved.
    Unknown,
    /// 连接被新一轮顶替 / 提交失败被撤回:什么都不发。
    Aborted,
    /// 事件总线落后:什么都不发,由平台补。
    Lagged,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DeliverySegment {
    pub index: usize,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_ts: Option<String>,
    pub delivered: bool,
    #[serde(skip)]
    pub unknown: bool,
}

impl DeliverySegment {
    pub(crate) fn new(index: usize, text: String) -> Self {
        Self {
            index,
            text,
            message_id: None,
            message_ts: None,
            delivered: false,
            unknown: false,
        }
    }
}

pub(crate) struct TurnReport {
    pub outcome: Outcome,
    pub delivery_failed: bool,
    pub message_ids: Vec<i64>,
    pub segments: Vec<DeliverySegment>,
    pub final_text: String,
    pub text_complete: bool,
    pub delivery_unknown: bool,
}

impl TurnReport {
    pub(crate) fn new(
        outcome: Outcome,
        mut segments: Vec<DeliverySegment>,
        final_text: String,
        delivery_unknown: bool,
    ) -> Self {
        for (index, segment) in segments.iter_mut().enumerate() {
            segment.index = index;
        }
        let text_complete = !matches!(
            outcome,
            Outcome::Lagged | Outcome::Aborted | Outcome::Unknown
        );
        Self {
            delivery_failed: segments.iter().any(|s| !s.delivered) || !text_complete,
            message_ids: segments.iter().filter_map(|s| s.message_id).collect(),
            delivery_unknown: delivery_unknown || segments.iter().any(|s| s.unknown),
            outcome,
            segments,
            final_text,
            text_complete,
        }
    }
}

/// 工具状态行最长多少字(工具说明是模型写的,可能很长)。
const STATUS_MAX: usize = 120;

/// 工具调用的状态行:优先用一句话说明,没有就用标题。
fn tool_status(description: Option<&str>, title: &str) -> Option<String> {
    let label = description
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or_else(|| title.trim());
    if label.is_empty() {
        return None;
    }
    let mut chars = label.chars();
    let mut clipped: String = chars.by_ref().take(STATUS_MAX).collect();
    if chars.next().is_some() {
        clipped.push('…');
    }
    Some(format!("🔧 {clipped}"))
}

pub(crate) struct TelegramTurn {
    pub api: Arc<dyn TgApi>,
    pub chat_id: i64,
    pub draft_base: i64,
    pub texts: RelayTexts,
}

impl TelegramTurn {
    pub(crate) async fn run(self, rx: mpsc::UnboundedReceiver<RelayInput>) -> TurnReport {
        super::telegram_typing::run(self, rx).await
    }

    pub(super) async fn run_delivery(
        self,
        mut rx: mpsc::UnboundedReceiver<RelayInput>,
    ) -> TurnReport {
        let deadline = Instant::now() + MAX_TURN;
        let mut body = Composer::default();
        let mut full_text = String::new();
        let mut segments = vec![DeliverySegment::new(0, String::new())];
        let mut fence = None;
        let mut draft_shown = None;
        let mut last_update = None;
        let mut blocked_until = None;
        let mut shown = String::new();
        let mut status = self.texts.thinking.clone();
        let mut status_tool = None;
        let mut cancel_pending = false;
        self.draft(
            &status,
            &mut draft_shown,
            &mut last_update,
            &mut blocked_until,
        )
        .await;
        let outcome = loop {
            let want = self.display(&body, &status, status_tool.is_some());
            let current = segments.last().unwrap();
            let has_message = current.message_id.is_some();
            let dirty = if has_message {
                shown != want
            } else {
                draft_shown.as_deref() != Some(&want)
            };
            let base = last_update.unwrap_or_else(Instant::now);
            let mut next = base
                + if dirty {
                    MIN_UPDATE_INTERVAL
                } else if has_message {
                    MAX_TURN
                } else {
                    DRAFT_HEARTBEAT
                };
            if let Some(b) = blocked_until {
                next = next.max(b);
            }
            if current.unknown && current.message_id.is_none() {
                next = deadline;
            }
            tokio::select! {
                // Pending cancellation/terminal events beat a due refresh.
                biased;
                input = rx.recv() => match input {
                    None => break Outcome::Aborted,
                    Some(RelayInput::Lagged) => break Outcome::Lagged,
                    Some(RelayInput::SilentComplete) => break Outcome::Silent,
                    Some(RelayInput::Event(env)) => match &env.payload {
                        AcpEvent::ContentDelta { text, parent_tool_use_id: None } => {
                            full_text.push_str(text);
                            for head in body.push(text) {
                                let segment = segments.last_mut().unwrap();
                                segment.text = head.clone();
                                if !cancel_pending {
                                    self.write(&head, segment, &fence, &mut shown, &mut last_update, &mut blocked_until).await;
                                }
                                // Only finalized chunks advance the cross-message fence.
                                tg_html::render_chunk(&head, &mut fence);
                                segments.push(DeliverySegment::new(segments.len(), String::new()));
                                shown.clear();
                            }
                            { let segment = segments.last_mut().unwrap(); segment.text = body.text().to_owned(); segment.delivered = false; }
                        }
                        AcpEvent::ToolCall { tool_call_id, title, description, .. } => {
                            if let Some(line) = tool_status(description.as_deref(), title) {
                                status = line; status_tool = Some(tool_call_id.clone());
                            }
                        }
                        AcpEvent::ToolCallUpdate { tool_call_id, title, description, status: tool_state, .. }
                            if status_tool.as_deref() == Some(tool_call_id.as_str()) => {
                            if matches!(tool_state.as_deref(), Some("completed" | "failed")) {
                                status = self.texts.thinking.clone(); status_tool = None;
                            } else if let Some(line) = tool_status(description.as_deref(), title.as_deref().unwrap_or("")) {
                                status = line;
                            }
                        }
                        AcpEvent::Error { terminal: false, code, .. } if code.as_deref() == Some("runtime_cancel_pending") => {
                            cancel_pending = true;
                        }
                        AcpEvent::TurnComplete { stop_reason, .. } => break match stop_reason.as_str() {
                            "end_turn" => Outcome::Completed, "cancelled" => Outcome::Cancelled, _ => Outcome::Failed,
                        },
                        AcpEvent::Error { terminal: true, code, .. } if code.as_deref() == Some("runtime_outcome_unknown") => break Outcome::Unknown,
                        AcpEvent::Error { terminal: true, .. } |
                        AcpEvent::StatusChanged { status: ConnectionStatus::Disconnected | ConnectionStatus::Error } => break Outcome::Failed,
                        _ => {}
                    }
                },
                _ = tokio::time::sleep_until(next), if !cancel_pending => {
                    if body.text().trim().is_empty() {
                        self.draft(&want, &mut draft_shown, &mut last_update, &mut blocked_until).await;
                    } else {
                        self.write(&want, segments.last_mut().unwrap(), &fence, &mut shown, &mut last_update, &mut blocked_until).await;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break if cancel_pending { Outcome::Unknown } else { Outcome::Failed },
            }
        };
        if matches!(
            outcome,
            Outcome::Aborted | Outcome::Lagged | Outcome::Unknown
        ) {
            // Only repair the already known message. A missing ID must never
            // become a new send while handing an incomplete turn back.
            if let Some(segment) = segments.last().filter(|_| !cancel_pending) {
                if let Some(id) = segment.message_id {
                    if !segment.text.trim().is_empty() && shown != segment.text {
                        let mut local_fence = fence.clone();
                        let html = tg_html::render_chunk(&segment.text, &mut local_fence);
                        let _ = self.send_with_plain_fallback("editMessageText",
                            serde_json::json!({"chat_id":self.chat_id,"message_id":id,"text":html,"parse_mode":"HTML"}),
                            serde_json::json!({"chat_id":self.chat_id,"message_id":id,"text":segment.text}),
                        ).await;
                    }
                }
            }
            segments.retain(|s| !s.text.is_empty() || s.message_id.is_some() || s.unknown);
            return TurnReport::new(outcome, segments, full_text, false);
        }
        segments.last_mut().unwrap().text = body.text().to_owned();
        segments.retain(|s| !s.text.trim().is_empty() || s.message_id.is_some() || s.unknown);
        let note = match outcome {
            Outcome::Completed if full_text.trim().is_empty() => Some(&self.texts.empty),
            Outcome::Cancelled => Some(&self.texts.cancelled),
            Outcome::Failed => Some(&self.texts.failed),
            _ => None,
        };
        if let Some(note) = note {
            if !full_text.is_empty() {
                full_text.push_str("\n\n");
            }
            full_text.push_str(note);
            segments.push(DeliverySegment::new(segments.len(), note.clone()));
        }
        // Reconcile all final segments, including a failed earlier split. A
        // transient failed edit does not latch failure after successful repair.
        let mut fence = None;
        for segment in &mut segments {
            let text = segment.text.clone();
            if segment.delivered {
                tg_html::render_chunk(&text, &mut fence);
                continue;
            }
            let mut final_shown = String::new();
            self.write(
                &text,
                segment,
                &fence,
                &mut final_shown,
                &mut last_update,
                &mut blocked_until,
            )
            .await;
            tg_html::render_chunk(&text, &mut fence);
        }
        TurnReport::new(outcome, segments, full_text, false)
    }

    fn display(&self, body: &Composer, status: &str, tool_running: bool) -> String {
        let text = body.text().trim_end();
        if text.trim().is_empty() {
            return status.to_string();
        }
        if tool_running {
            format!("{text}\n\n{status}")
        } else {
            text.to_string()
        }
    }

    async fn write(
        &self,
        text: &str,
        segment: &mut DeliverySegment,
        fence: &Option<String>,
        shown: &mut String,
        last_update: &mut Option<Instant>,
        blocked_until: &mut Option<Instant>,
    ) {
        if text.trim().is_empty() {
            return;
        }
        if shown == text {
            segment.delivered = text == segment.text;
            return;
        }
        // A lost sendMessage response may have created a message. Never create
        // another one without its ID. Edits with known IDs remain repairable.
        if segment.unknown && segment.message_id.is_none() {
            return;
        }
        let mut next = last_update
            .map(|at| at + MIN_UPDATE_INTERVAL)
            .unwrap_or_else(Instant::now);
        if let Some(blocked) = blocked_until {
            next = next.max(*blocked);
        }
        tokio::time::sleep_until(next).await;
        *last_update = Some(Instant::now());
        let mut local_fence = fence.clone();
        let html = tg_html::render_chunk(text, &mut local_fence);
        loop {
            let method = if segment.message_id.is_some() {
                "editMessageText"
            } else {
                "sendMessage"
            };
            let mut body =
                serde_json::json!({"chat_id": self.chat_id, "text": html, "parse_mode": "HTML"});
            let mut plain = serde_json::json!({"chat_id": self.chat_id, "text": text});
            if let Some(id) = segment.message_id {
                body["message_id"] = id.into();
                plain["message_id"] = id.into();
            }
            match self.send_with_plain_fallback(method, body, plain).await {
                Ok(id) => {
                    if segment.message_id.is_none() {
                        let Some(id) = id else {
                            segment.unknown = true;
                            segment.delivered = false;
                            break;
                        };
                        segment.message_id = Some(id);
                    }
                    segment.delivered = text == segment.text;
                    segment.unknown = false;
                    *shown = text.to_owned();
                    break;
                }
                Err(TgError::Other(e))
                    if segment.message_id.is_some() && e.contains("not modified") =>
                {
                    segment.delivered = text == segment.text;
                    segment.unknown = false;
                    *shown = text.to_owned();
                    break;
                }
                Err(TgError::Other(e))
                    if segment.message_id.is_some() && e.contains("message to edit not found") =>
                {
                    segment.message_id = None;
                    segment.unknown = false;
                }
                Err(TgError::Unknown) => {
                    segment.delivered = false;
                    segment.unknown = segment.message_id.is_none();
                    break;
                }
                Err(TgError::RetryAfter(s)) => {
                    *blocked_until = Some(Instant::now() + Duration::from_secs(s));
                    segment.delivered = false;
                    break;
                }
                Err(TgError::Other(_)) => {
                    segment.delivered = false;
                    break;
                }
            }
        }
        *last_update = Some(Instant::now());
    }

    async fn draft(
        &self,
        text: &str,
        draft_shown: &mut Option<String>,
        last_update: &mut Option<Instant>,
        blocked_until: &mut Option<Instant>,
    ) {
        *last_update = Some(Instant::now());
        match self
            .api
            .call(
                "sendMessageDraft",
                serde_json::json!({"chat_id":self.chat_id,"draft_id":self.draft_base,"text":text}),
            )
            .await
        {
            Ok(_) => {
                *draft_shown = Some(text.to_owned());
                *blocked_until = None;
            }
            Err(TgError::RetryAfter(s)) => {
                *blocked_until = Some(Instant::now() + Duration::from_secs(s));
            }
            Err(_) => {
                *draft_shown = Some(text.to_owned());
            }
        }
    }

    async fn send_with_plain_fallback(
        &self,
        method: &str,
        html: serde_json::Value,
        plain: serde_json::Value,
    ) -> Result<Option<i64>, TgError> {
        match self.call_with_retry(method, html).await {
            Err(TgError::Other(e))
                if e.contains("can't parse entities")
                    || e.contains("can't find end")
                    || e.contains("unsupported start tag") =>
            {
                self.call_with_retry(method, plain).await
            }
            result => result,
        }
    }

    async fn call_with_retry(
        &self,
        method: &str,
        body: serde_json::Value,
    ) -> Result<Option<i64>, TgError> {
        for attempt in 0..SEND_RETRIES {
            match self.api.call(method, body.clone()).await {
                Err(TgError::RetryAfter(s)) if attempt + 1 < SEND_RETRIES => {
                    tokio::time::sleep(Duration::from_secs(s)).await
                }
                result => return result,
            }
        }
        unreachable!()
    }
}

// ── 全局登记 ─────────────────────────────────────────────────────────────────

struct Worker {
    turn_id: String,
    generation: u64,
    start_seq: u64,
    tx: mpsc::UnboundedSender<RelayInput>,
}

struct Claim {
    turn_id: String,
    start_seq: u64,
    at: std::time::Instant,
}

struct Relay {
    admission: tokio::sync::Mutex<()>,
    db: DatabaseConnection,
    client: reqwest::Client,
    workers: StdMutex<HashMap<String, Worker>>,
    claims: StdMutex<HashMap<String, Vec<Claim>>>,
}

static RELAY: OnceLock<Arc<Relay>> = OnceLock::new();

/// 首次使用时建好登记表并挂上总线订阅(订阅在返回前完成,之后 send 的事件不会漏)。
fn relay(bus: &Arc<InternalEventBus>, db: &DatabaseConnection) -> Arc<Relay> {
    Arc::clone(RELAY.get_or_init(|| {
        let relay = Arc::new(Relay {
            admission: tokio::sync::Mutex::new(()),
            db: db.clone(),
            client: super::webhook::make_webhook_client(),
            workers: StdMutex::new(HashMap::new()),
            claims: StdMutex::new(HashMap::new()),
        });
        let rx = bus.subscribe();
        tokio::spawn(dispatch(Arc::clone(&relay), rx));
        relay
    }))
}

async fn dispatch(relay: Arc<Relay>, mut rx: tokio::sync::broadcast::Receiver<Arc<EventEnvelope>>) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(env) => {
                let workers = relay.workers.lock().unwrap();
                if let Some(w) = workers.get(&env.connection_id) {
                    if env.seq > w.start_seq {
                        let _ = w.tx.send(RelayInput::Event(env));
                    }
                }
            }
            Err(RecvError::Lagged(n)) => {
                tracing::warn!("[TurnRelay] event bus lagged by {n}; handing active turns back");
                let mut workers = relay.workers.lock().unwrap();
                // Keep claims: incomplete output must not race platform fallback.
                for (_, w) in workers.drain() {
                    let _ = w.tx.send(RelayInput::Lagged);
                }
            }
            Err(RecvError::Closed) => break,
        }
    }
}

/// 提交 prompt 之前调:登记本轮直推,返回 turn_id(提交失败时用它调 [`abort`] 撤回)。
///
/// 这条连接上一轮还在跑时**不登记**:这次提交必然被拒(`TurnInProgress`),而登记会顶掉
/// 正在推送的上一轮(连发两条消息时就会这样)。
pub async fn register_for_prompt(
    state: &crate::app_state::AppState,
    connection_id: &str,
    conversation_id: Option<i32>,
    spec: OutboundSpec,
) -> Result<Option<String>, String> {
    register_with_state(state, connection_id, conversation_id, spec, false).await
}

/// External runtimes keep their original sessions outside ConnectionManager.
/// Require durable v2 receipts so ingress can wait for actual delivery completion.
pub async fn register_external(
    state: &crate::app_state::AppState,
    connection_id: &str,
    spec: OutboundSpec,
) -> Result<(), String> {
    let version = match &spec {
        OutboundSpec::Telegram {
            delivery_version, ..
        }
        | OutboundSpec::Slack {
            delivery_version, ..
        } => *delivery_version,
    };
    if version < 2 {
        return Err("external_relay_requires_v2".into());
    }
    register_with_state(state, connection_id, None, spec, true)
        .await
        .map(|_| ())
}

async fn register_with_state(
    state: &crate::app_state::AppState,
    connection_id: &str,
    conversation_id: Option<i32>,
    spec: OutboundSpec,
    external: bool,
) -> Result<Option<String>, String> {
    let relay = relay(&state.acp_event_bus, &state.db.conn);
    let _admission = relay.admission.lock().await;
    if spec.turn_id().is_empty() || spec.turn_id().len() > 200 {
        return Err("invalid_relay_turn_id".into());
    }
    if super::relay_outbox::get(&state.db.conn, spec.turn_id())
        .await
        .map_err(|_| "relay_storage_unavailable".to_string())?
        .is_some()
    {
        return Err("relay_turn_already_exists".into());
    }
    if relay.workers.lock().unwrap().contains_key(connection_id) {
        return Err("relay_turn_in_progress".into());
    }
    let (generation, start_seq) = if external {
        (0, 0)
    } else if let Some(session) = state.connection_manager.get_state(connection_id).await {
        let session = session.read().await;
        if session.turn_in_flight {
            return Err("relay_turn_in_progress".into());
        }
        (session.turns_completed, session.event_seq)
    } else {
        return Err("relay_connection_not_found".into());
    };
    let (version, kind, target, callback, format) = match &spec {
        OutboundSpec::Telegram {
            delivery_version,
            chat_id,
            result_webhook_url,
            result_format,
            ..
        } => (
            *delivery_version,
            "telegram",
            serde_json::json!({"chat_id":chat_id}),
            result_webhook_url.as_deref(),
            *result_format,
        ),
        OutboundSpec::Slack {
            delivery_version,
            channel,
            thread_ts,
            result_webhook_url,
            result_format,
            ..
        } => {
            if channel.is_empty() || thread_ts.is_empty() || thread_ts == "0" {
                return Err("invalid_slack_relay_target".into());
            }
            (
                *delivery_version,
                "slack",
                serde_json::json!({"channel":channel,"thread_ts":thread_ts}),
                result_webhook_url.as_deref(),
                *result_format,
            )
        }
    };
    let callback_hash = if version >= 2 {
        super::relay_outbox::validated_target(&state.db.conn, callback).await
    } else {
        None
    };
    if version >= 2 {
        super::relay_outbox::save(&state.db.conn, spec.turn_id(), serde_json::json!({
            "event_id":uuid::Uuid::new_v4().to_string(), "event":"relay_result", "relay_version":2,
            "relay_turn_id":spec.turn_id(), "relay_kind":kind, "connection_id":connection_id,
            "conversation_id":conversation_id, "delivery_status":"running",
            "fallback_allowed":false, "text_complete":false, "final_text":"", "segments":[],
            "target":target, "occurred_at":chrono::Utc::now().to_rfc3339(), "source":"codeg"
        }), callback_hash.clone(), format).await.map_err(|_| "relay_storage_unavailable".to_string())?;
    }
    register(
        &state.acp_event_bus,
        &state.db.conn,
        connection_id,
        conversation_id,
        spec,
        (generation, start_seq),
        callback_hash,
    )
    .map(Some)
}

/// 登记一轮直推。须在提交 prompt **之前**调用(之后的事件才不会漏)。
fn register(
    bus: &Arc<InternalEventBus>,
    db: &DatabaseConnection,
    connection_id: &str,
    conversation_id: Option<i32>,
    spec: OutboundSpec,
    (generation, start_seq): (u64, u64),
    callback_hash: Option<String>,
) -> Result<String, String> {
    let relay = relay(bus, db);
    let turn_id = spec.turn_id().to_string();
    let (tx, rx) = mpsc::unbounded_channel();
    {
        let mut workers = relay.workers.lock().unwrap();
        insert_worker(
            &mut workers,
            connection_id,
            Worker {
                turn_id: turn_id.clone(),
                generation,
                start_seq,
                tx,
            },
        )?;
        let mut claims = relay.claims.lock().unwrap();
        claims.retain(|_, entries| {
            entries.retain(|c| c.at.elapsed() < CLAIM_TTL);
            !entries.is_empty()
        });
        claims
            .entry(connection_id.to_string())
            .or_default()
            .push(Claim {
                turn_id: turn_id.clone(),
                start_seq,
                at: std::time::Instant::now(),
            });
    }

    let (version, kind, target) = match &spec {
        OutboundSpec::Telegram {
            delivery_version,
            chat_id,
            ..
        } => (
            *delivery_version,
            "telegram",
            serde_json::json!({"chat_id":chat_id}),
        ),
        OutboundSpec::Slack {
            delivery_version,
            channel,
            thread_ts,
            ..
        } => (
            *delivery_version,
            "slack",
            serde_json::json!({"channel":channel,"thread_ts":thread_ts}),
        ),
    };
    let (presentation, result_format) = match &spec {
        OutboundSpec::Telegram {
            presentation,
            result_format,
            ..
        }
        | OutboundSpec::Slack {
            presentation,
            result_format,
            ..
        } => (presentation.clone(), *result_format),
    };
    let rx = super::relay_input::wrap(rx, presentation);
    let connection_id = connection_id.to_string();
    let tid = turn_id.clone();
    tokio::spawn(async move {
        let report = match spec {
            OutboundSpec::Telegram {
                bot_token,
                chat_id,
                draft_id,
                texts,
                ..
            } => {
                TelegramTurn {
                    api: Arc::new(HttpTg {
                        client: relay.client.clone(),
                        token: bot_token,
                    }),
                    chat_id,
                    draft_base: draft_id
                        .filter(|d| *d > 0)
                        .unwrap_or_else(|| derive_draft_base(&tid)),
                    texts,
                }
                .run(rx)
                .await
            }
            OutboundSpec::Slack {
                bot_token,
                channel,
                thread_ts,
                recipient_user_id,
                recipient_team_id,
                texts,
                ..
            } => {
                super::slack_relay::SlackTurn {
                    session_key: super::slack_relay::session_key(&bot_token, &channel, &thread_ts),
                    api: Arc::new(super::slack_relay::HttpSlack {
                        client: relay.client.clone(),
                        token: bot_token,
                    }),
                    channel,
                    thread_ts,
                    recipient_user_id,
                    recipient_team_id,
                    texts,
                }
                .run(rx)
                .await
            }
        };
        tracing::info!(
            "[TurnRelay] turn {tid} on {connection_id}: {:?} (delivery_failed={})",
            report.outcome,
            report.delivery_failed
        );
        {
            let mut workers = relay.workers.lock().unwrap();
            if workers.get(&connection_id).map(|w| w.turn_id == tid) == Some(true) {
                workers.remove(&connection_id);
            }
        }
        if version >= 2 {
            let status = if report.delivery_unknown || report.outcome == Outcome::Unknown {
                "unknown"
            } else if report.delivery_failed {
                "failed"
            } else {
                "delivered"
            };
            let payload = serde_json::json!({
                "event_id": uuid::Uuid::new_v4().to_string(), "event": "relay_result", "relay_version": 2,
                "relay_turn_id": tid, "relay_kind": kind, "connection_id": connection_id,
                "conversation_id": conversation_id, "outcome": report.outcome,
                "delivery_status": status,
                "execution_status": if report.outcome == Outcome::Unknown { "unknown" } else { "terminal" },
                "fallback_allowed": report.delivery_failed && !report.delivery_unknown && report.text_complete,
                "text_complete": report.text_complete, "final_text": report.final_text,
                "segments": report.segments, "target": target,
                "failure_code": if report.delivery_unknown { Some("write_outcome_unknown") } else if !report.text_complete { Some("incomplete_event_stream") } else if report.delivery_failed { Some("platform_write_failed") } else { None },
                "occurred_at": chrono::Utc::now().to_rfc3339(), "source": "codeg",
            });
            if super::relay_outbox::save(&relay.db, &tid, payload, callback_hash, result_format)
                .await
                .is_err()
            {
                tracing::error!("[TurnRelay] terminal result persistence failed");
            }
        } else if report.delivery_failed && !report.delivery_unknown && report.text_complete {
            report_failure(
                &relay,
                &connection_id,
                conversation_id,
                &tid,
                &report.message_ids,
            )
            .await;
        }
    });
    Ok(turn_id)
}

fn insert_worker(
    workers: &mut HashMap<String, Worker>,
    connection_id: &str,
    worker: Worker,
) -> Result<(), String> {
    match workers.entry(connection_id.to_owned()) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(worker);
            Ok(())
        }
        std::collections::hash_map::Entry::Occupied(_) => Err("relay_turn_in_progress".into()),
    }
}

/// 提交 prompt 失败时撤回登记(只撤本轮的,不误伤后来者)。
pub fn abort(connection_id: &str, turn_id: &str) {
    let Some(relay) = RELAY.get() else { return };
    let mut workers = relay.workers.lock().unwrap();
    if workers.get(connection_id).map(|w| w.turn_id == turn_id) == Some(true) {
        workers.remove(connection_id); // 丢掉 tx → worker 按 Aborted 退出
    }
    let mut claims = relay.claims.lock().unwrap();
    if let Some(entries) = claims.get_mut(connection_id) {
        entries.retain(|c| c.turn_id != turn_id);
    }
}

/// webhook 用:这条连接的当前轮是否由本模块直推。`take` = 取走(turn_complete 时)。
pub fn relay_turn_id_for(connection_id: &str, seq: u64, take: bool) -> Option<String> {
    let relay = RELAY.get()?;
    let mut claims = relay.claims.lock().unwrap();
    let entries = claims.get_mut(connection_id)?;
    claim_for_event(entries, seq, take)
}

fn claim_for_event(entries: &mut Vec<Claim>, seq: u64, take: bool) -> Option<String> {
    entries.retain(|c| c.at.elapsed() < CLAIM_TTL);
    let index = entries.iter().rposition(|c| c.start_seq < seq)?;
    if take {
        Some(entries.remove(index).turn_id)
    } else {
        Some(entries[index].turn_id.clone())
    }
}

/// 内容没写出去 → 发 `relay_failed` webhook,平台用落库的正文兜底(带上已写的消息 id)。
async fn report_failure(
    relay: &Relay,
    connection_id: &str,
    conversation_id: Option<i32>,
    turn_id: &str,
    message_ids: &[i64],
) {
    let urls =
        app_metadata_service::get_value(&relay.db, super::event_subscriber::EVENT_WEBHOOKS_KEY)
            .await
            .ok()
            .flatten()
            .map(|json| super::webhook::enabled_webhook_urls(&json))
            .unwrap_or_default();
    if urls.is_empty() {
        return;
    }
    let payload = serde_json::json!({
        "event_id": uuid::Uuid::new_v4().to_string(),
        "event": "relay_failed",
        "connection_id": connection_id,
        "conversation_id": conversation_id,
        "relay_turn_id": turn_id,
        // 已经写出去的消息:平台兜底时**编辑**它们(第 n 条对应正文的第 n 块),
        // 只有多出来的块才新发 —— 编辑幂等,不会重复。空数组 = 一个字都没发出去
        "message_ids": message_ids,
        "occurred_at": chrono::Utc::now().to_rfc3339(),
        "source": "codeg",
    });
    super::webhook::spawn_webhook_delivery(relay.client.clone(), urls, payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composer_keeps_growing_until_the_length_cap() {
        // 工具调用不再断句:正文一直往同一条消息里长
        let mut c = Composer::default();
        assert!(c.push("先说一句").is_empty());
        assert!(c.push(",再说一句").is_empty());
        assert_eq!(c.text(), "先说一句,再说一句");
        assert_eq!(c.index(), 0);
    }

    #[test]
    fn composer_overflow_finalizes_head_and_keeps_tail() {
        let mut c = Composer::default();
        let line = "字".repeat(1000);
        let text = format!("{line}\n{line}\n{line}\n{line}\n尾巴");
        let out = c.push(&text);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], format!("{line}\n{line}\n{line}"));
        assert_eq!(c.text(), format!("\n{line}\n尾巴"));
        assert_eq!(c.index(), 1);
    }

    #[test]
    fn split_without_newlines_hard_cuts_by_chars() {
        let text = "长".repeat(TG_CHUNK + 5);
        let (head, tail) = split_head(&text, TG_CHUNK);
        assert_eq!(head.chars().count(), TG_CHUNK);
        assert_eq!(tail.chars().count(), 5);
    }

    #[test]
    fn draft_base_is_stable_and_positive() {
        let a = derive_draft_base("turn-1");
        assert_eq!(a, derive_draft_base("turn-1"));
        assert!(a > 0 && a < (1 << 30));
        assert_ne!(a, derive_draft_base("turn-2"));
    }

    #[test]
    fn outbound_spec_parses_and_redacts_token() {
        let spec: OutboundSpec = serde_json::from_value(serde_json::json!({
            "kind": "telegram",
            "turnId": "t1",
            "botToken": "123:SECRET",
            "chatId": 42,
        }))
        .unwrap();
        assert_eq!(spec.turn_id(), "t1");
        let dbg = format!("{spec:?}");
        assert!(!dbg.contains("SECRET"));
        assert!(dbg.contains("redacted"));
    }

    // ── worker(假 Telegram,记录调用)──

    #[derive(Default)]
    struct FakeTg {
        calls: StdMutex<Vec<(String, String)>>,
        next_id: StdMutex<i64>,
        fail_messages: bool,
        /// 模拟 Telegram 拒收 HTML(can't parse entities)
        reject_html: bool,
        /// 模拟消息被用户删掉,编辑报错
        edit_gone: bool,
    }

    #[async_trait]
    impl TgApi for FakeTg {
        async fn call(
            &self,
            method: &str,
            body: serde_json::Value,
        ) -> Result<Option<i64>, TgError> {
            let text = body["text"].as_str().unwrap_or_default().to_string();
            self.calls.lock().unwrap().push((method.to_string(), text));
            if self.fail_messages && method == "sendMessage" {
                return Err(TgError::Other("Forbidden".into()));
            }
            if self.edit_gone && method == "editMessageText" {
                return Err(TgError::Other(
                    "Bad Request: message to edit not found".into(),
                ));
            }
            if self.reject_html && body.get("parse_mode").is_some() {
                return Err(TgError::Other("Bad Request: can't parse entities".into()));
            }
            if method == "sendMessage" {
                let mut id = self.next_id.lock().unwrap();
                *id += 1;
                return Ok(Some(*id));
            }
            Ok(None)
        }
    }

    fn env(payload: AcpEvent) -> RelayInput {
        RelayInput::Event(Arc::new(EventEnvelope {
            seq: 0,
            connection_id: "c1".into(),
            payload,
        }))
    }

    fn delta(t: &str) -> RelayInput {
        env(AcpEvent::ContentDelta {
            text: t.into(),
            parent_tool_use_id: None,
        })
    }

    fn complete(reason: &str) -> RelayInput {
        env(AcpEvent::TurnComplete {
            session_id: "s".into(),
            stop_reason: reason.into(),
            agent_type: "claude_code".into(),
            duration_ms: None,
        })
    }

    fn tool_call(id: &str, title: &str, description: Option<&str>) -> RelayInput {
        env(AcpEvent::ToolCall {
            tool_call_id: id.into(),
            title: title.into(),
            kind: "execute".into(),
            status: "in_progress".into(),
            tool_name: Some("Bash".into()),
            description: description.map(str::to_string),
            content: None,
            raw_input: None,
            raw_output: None,
            locations: None,
            meta: None,
            images: None,
        })
    }

    fn tool_done(id: &str) -> RelayInput {
        env(AcpEvent::ToolCallUpdate {
            tool_call_id: id.into(),
            title: None,
            tool_name: None,
            description: None,
            status: Some("completed".into()),
            content: None,
            raw_input: None,
            raw_output: None,
            raw_output_append: None,
            locations: None,
            meta: None,
            images: None,
        })
    }

    fn turn_with(api: Arc<FakeTg>) -> TelegramTurn {
        TelegramTurn {
            api: api as Arc<dyn TgApi>,
            chat_id: 1,
            draft_base: 100,
            texts: RelayTexts::default(),
        }
    }

    async fn run_with(api: Arc<FakeTg>, inputs: Vec<RelayInput>) -> TurnReport {
        let (tx, rx) = mpsc::unbounded_channel();
        for i in inputs {
            tx.send(i).unwrap();
        }
        // 输入发完就关:没有结束事件的用例据此走 Aborted,而不是一直等到 MAX_TURN
        drop(tx);
        turn_with(api).run(rx).await
    }

    fn calls_of(api: &FakeTg, method: &str) -> Vec<String> {
        api.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == method)
            .map(|(_, t)| t.clone())
            .collect()
    }

    fn messages(api: &FakeTg) -> Vec<String> {
        calls_of(api, "sendMessage")
    }

    fn edits(api: &FakeTg) -> Vec<String> {
        calls_of(api, "editMessageText")
    }

    fn drafts(api: &FakeTg) -> Vec<String> {
        calls_of(api, "sendMessageDraft")
    }

    #[tokio::test(start_paused = true)]
    async fn a_turn_lands_in_one_message_even_with_tool_calls() {
        let api = Arc::new(FakeTg::default());
        let r = run_with(
            Arc::clone(&api),
            vec![
                delta("我先看看"),
                tool_call("t", "Bash", None),
                delta("结论是 "),
                delta("42"),
                complete("end_turn"),
            ],
        )
        .await;
        assert_eq!(r.outcome, Outcome::Completed);
        assert!(!r.delivery_failed);
        // 工具调用不再把正文切成两条
        assert_eq!(messages(&api), vec!["我先看看结论是 42"]);
        assert_eq!(r.message_ids, vec![1]);
        // 第一次调用就是「思考中」状态,不是空草稿
        assert_eq!(
            drafts(&api).first().unwrap(),
            &RelayTexts::default().thinking
        );
    }

    #[tokio::test(start_paused = true)]
    async fn growing_text_edits_the_same_message() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(turn_with(Arc::clone(&api)).run(rx));

        tx.send(delta("第一段")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(messages(&api), vec!["第一段"], "第一段正文新发一条消息");

        tx.send(delta("、第二段")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(edits(&api), vec!["第一段、第二段"], "之后只编辑同一条");
        assert_eq!(messages(&api).len(), 1, "不会再发新消息");

        tx.send(complete("end_turn")).unwrap();
        let r = handle.await.unwrap();
        assert_eq!(r.message_ids, vec![1]);
        // 定稿内容与最后一次编辑相同 → 不重复调用
        assert_eq!(edits(&api).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn tool_status_rides_at_the_end_of_the_message() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(turn_with(Arc::clone(&api)).run(rx));

        tx.send(delta("正文")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(tool_call("t1", "df -h", Some("查看磁盘使用情况")))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(edits(&api).last().unwrap(), "正文\n\n🔧 查看磁盘使用情况");

        tx.send(tool_done("t1")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(edits(&api).last().unwrap(), "正文", "工具跑完状态行去掉");

        tx.send(complete("end_turn")).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn status_shows_in_a_draft_until_the_first_text() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(turn_with(Arc::clone(&api)).run(rx));

        tx.send(tool_call("t1", "df -h", Some("查看磁盘使用情况")))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(drafts(&api).last().unwrap(), "🔧 查看磁盘使用情况");
        assert!(messages(&api).is_empty(), "还没有正文就不占消息历史");

        tx.send(tool_done("t1")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(
            drafts(&api).last().unwrap(),
            &RelayTexts::default().thinking
        );

        tx.send(complete("end_turn")).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn empty_and_cancelled_and_failed_turns_leave_a_note() {
        let api = Arc::new(FakeTg::default());
        run_with(Arc::clone(&api), vec![complete("end_turn")]).await;
        assert_eq!(messages(&api), vec![RelayTexts::default().empty]);

        let api = Arc::new(FakeTg::default());
        run_with(
            Arc::clone(&api),
            vec![delta("写了一半"), complete("cancelled")],
        )
        .await;
        assert_eq!(
            messages(&api),
            vec!["写了一半".to_string(), RelayTexts::default().cancelled]
        );

        let api = Arc::new(FakeTg::default());
        let r = run_with(Arc::clone(&api), vec![complete("max_tokens")]).await;
        assert_eq!(r.outcome, Outcome::Failed);
        assert_eq!(messages(&api), vec![RelayTexts::default().failed]);
    }

    #[tokio::test(start_paused = true)]
    async fn lagged_and_aborted_send_nothing_final() {
        let api = Arc::new(FakeTg::default());
        let r = run_with(Arc::clone(&api), vec![delta("半截"), RelayInput::Lagged]).await;
        assert_eq!(r.outcome, Outcome::Lagged);
        assert!(messages(&api).is_empty());

        let api = Arc::new(FakeTg::default());
        let r = run_with(Arc::clone(&api), vec![delta("半截")]).await; // tx 已丢 → 关闭
        assert_eq!(r.outcome, Outcome::Aborted);
        assert!(messages(&api).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn uncertain_runtime_never_finalizes_or_allows_fallback() {
        let api = Arc::new(FakeTg::default());
        let report = run_with(
            api.clone(),
            vec![
                delta("partial"),
                env(AcpEvent::Error {
                    message: "lost owner".into(),
                    agent_type: "legacy".into(),
                    code: Some("runtime_outcome_unknown".into()),
                    details: None,
                    terminal: true,
                }),
            ],
        )
        .await;
        assert_eq!(report.outcome, Outcome::Unknown);
        assert!(!report.text_complete);
        assert!(messages(&api).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn markdown_is_sent_as_html_and_falls_back_to_plain() {
        let api = Arc::new(FakeTg::default());
        run_with(
            Arc::clone(&api),
            vec![delta("**总结**:磁盘 `/` 充裕"), complete("end_turn")],
        )
        .await;
        assert_eq!(messages(&api), vec!["<b>总结</b>:磁盘 <code>/</code> 充裕"]);

        let api = Arc::new(FakeTg {
            reject_html: true,
            ..Default::default()
        });
        let r = run_with(
            Arc::clone(&api),
            vec![delta("**总结**"), complete("end_turn")],
        )
        .await;
        assert!(!r.delivery_failed);
        // 先试 HTML 被拒,再以原文纯文本发出
        assert_eq!(messages(&api), vec!["<b>总结</b>", "**总结**"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_deleted_message_is_replaced_by_a_new_one() {
        let api = Arc::new(FakeTg {
            edit_gone: true,
            ..Default::default()
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(turn_with(Arc::clone(&api)).run(rx));
        tx.send(delta("第一段")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(delta("、第二段")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(complete("end_turn")).unwrap();
        let r = handle.await.unwrap();
        assert!(!r.delivery_failed);
        // 编辑失败(消息已删)→ 另起一条,内容完整
        assert_eq!(messages(&api).last().unwrap(), "第一段、第二段");
        assert_eq!(r.message_ids, vec![2]);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_delivery_is_reported() {
        let api = Arc::new(FakeTg {
            fail_messages: true,
            ..Default::default()
        });
        let r = run_with(Arc::clone(&api), vec![delta("hi"), complete("end_turn")]).await;
        assert!(r.delivery_failed);
        assert!(r.message_ids.is_empty(), "一条都没发出去 → 平台整条补发");
    }

    #[tokio::test(start_paused = true)]
    async fn lagged_turn_removes_tool_status_only_from_its_known_message() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(turn_with(api.clone()).run(rx));
        tx.send(delta("partial body")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(tool_call("t", "Running tool", None)).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(RelayInput::Lagged).unwrap();
        let report = handle.await.unwrap();
        assert!(!report.text_complete);
        assert_eq!(messages(&api).len(), 1);
        assert_eq!(edits(&api).last().unwrap(), "partial body");
    }

    #[tokio::test(start_paused = true)]
    async fn updates_are_throttled_and_the_draft_heartbeats() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(turn_with(Arc::clone(&api)).run(rx));
        // 没有正文:草稿每 5 秒续一次(草稿 ~10 秒过期)
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(drafts(&api).len(), 2);

        // 连续来字:距上次更新已超过 1 秒,第一个字立刻发出;余下攒到下一次一起写
        for t in ["a", "b", "c", "d"] {
            tx.send(delta(t)).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        // 距上次更新已超过 1 秒,先到的字立刻发出(select 先取事件还是先触发计时器不确定,
        // 所以只断言「发了一条、内容是 abcd 的前缀」),余下攒到下一次一起写
        assert_eq!(messages(&api).len(), 1);
        assert!(
            "abcd".starts_with(messages(&api)[0].as_str()),
            "{:?}",
            messages(&api)
        );
        // The select loop may consume the whole batch before its first write;
        // then the original send already has abcd and no edit is needed.
        assert_eq!(
            edits(&api).pop().or_else(|| messages(&api).pop()).unwrap(),
            "abcd"
        );

        // 正文消息不会过期 → 没有新字就不再写正文/草稿，typing 独立续期。
        let body_calls = || messages(&api).len() + edits(&api).len() + drafts(&api).len();
        let before = body_calls();
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(body_calls(), before, "静默时不重写正文或草稿");

        tx.send(complete("end_turn")).unwrap();
        let r = handle.await.unwrap();
        assert_eq!(r.outcome, Outcome::Completed);
        assert_eq!(messages(&api).len(), 1, "全程只发了一条消息");
        assert_eq!(
            edits(&api).pop().or_else(|| messages(&api).pop()).unwrap(),
            "abcd"
        );
    }
}

/// Deliver a command reply through the same formatting, throttling and durable
/// receipts as normal turns. Synthetic events are sent ONLY to this worker,
/// never to the ACP/global bus or an agent/transcript.
pub async fn deliver_reply(
    state: &crate::app_state::AppState,
    spec: OutboundSpec,
    text: String,
) -> Result<serde_json::Value, String> {
    let turn_id = spec.turn_id().to_string();
    let connection_id = format!("channel-reply:{turn_id}");
    register_external(state, &connection_id, spec).await?;
    let relay = relay(&state.acp_event_bus, &state.db.conn);
    let tx = relay
        .workers
        .lock()
        .unwrap()
        .get(&connection_id)
        .map(|worker| worker.tx.clone())
        .ok_or("reply_worker_missing")?;
    for (seq, payload) in [
        (
            1,
            AcpEvent::ContentDelta {
                text,
                parent_tool_use_id: None,
            },
        ),
        (
            2,
            AcpEvent::TurnComplete {
                session_id: String::new(),
                stop_reason: "end_turn".into(),
                agent_type: "channel-command".into(),
                duration_ms: None,
            },
        ),
    ] {
        tx.send(RelayInput::Event(Arc::new(EventEnvelope {
            seq,
            connection_id: connection_id.clone(),
            payload,
        })))
        .map_err(|_| "reply_worker_closed")?;
    }
    let mut results = super::relay_outbox::subscribe_results();
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(result) =
                get_result(&state.db.conn, &turn_id, ResultFormat::CompactV1).await?
            {
                return Ok(result);
            }
            results
                .changed()
                .await
                .map_err(|_| "relay_result_stream_closed".to_string())?;
        }
    })
    .await
    .map_err(|_| "reply_outcome_unknown".to_string())?
}

pub async fn get_result(
    db: &DatabaseConnection,
    turn_id: &str,
    format: ResultFormat,
) -> Result<Option<serde_json::Value>, String> {
    super::relay_outbox::get(db, turn_id)
        .await?
        .filter(|result| result["delivery_status"] != "running")
        .map(|result| super::relay_result::for_wire(result, format))
        .transpose()
}

pub(crate) fn admission_error(message: String) -> crate::app_error::AppCommandError {
    use crate::app_error::{AppCommandError, AppErrorCode};
    match message.as_str() {
        "relay_turn_in_progress" | "relay_turn_already_exists" => {
            AppCommandError::new(AppErrorCode::TurnInProgress, message)
        }
        "relay_storage_unavailable" => AppCommandError::task_execution_failed(message),
        "relay_connection_not_found" => AppCommandError::not_found("connection_not_found"),
        _ => AppCommandError::invalid_input(message),
    }
}

/// Execute without an await while the exact worker identity remains locked.
pub(crate) fn enqueue_if_current(
    connection_id: &str,
    turn_id: &str,
    enqueue: impl FnOnce(),
) -> bool {
    let Some(relay) = RELAY.get() else {
        return false;
    };
    let workers = relay.workers.lock().unwrap();
    if workers
        .get(connection_id)
        .is_some_and(|w| w.turn_id == turn_id)
    {
        enqueue();
        true
    } else {
        false
    }
}

pub async fn cancel_turn(
    state: &crate::app_state::AppState,
    turn_id: &str,
) -> Result<&'static str, String> {
    let worker = RELAY.get().and_then(|relay| {
        relay
            .workers
            .lock()
            .unwrap()
            .iter()
            .find(|(_, w)| w.turn_id == turn_id)
            .map(|(id, w)| (id.clone(), w.generation))
    });
    if let Some((connection_id, generation)) = worker {
        return state
            .connection_manager
            .cancel_relay_turn(&state.db.conn, &connection_id, turn_id, generation)
            .await
            .map(|cancelled| if cancelled { "cancelled" } else { "not_found" })
            .map_err(|_| "relay cancel failed".into());
    }
    Ok(
        match super::relay_outbox::get(&state.db.conn, turn_id).await? {
            Some(result) if result["delivery_status"] != "running" => "terminal",
            _ => "not_found",
        },
    )
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use std::collections::VecDeque;
    #[test]
    fn parallel_registrations_cannot_replace_the_winning_worker() {
        let workers = Arc::new(StdMutex::new(HashMap::new()));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let tasks: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|id| {
                let workers = workers.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let (tx, _rx) = mpsc::unbounded_channel();
                    barrier.wait();
                    insert_worker(
                        &mut workers.lock().unwrap(),
                        "connection",
                        Worker {
                            turn_id: id.into(),
                            generation: 0,
                            start_seq: 0,
                            tx,
                        },
                    )
                    .map(|_| id)
                })
            })
            .collect();
        let winners: Vec<_> = tasks
            .into_iter()
            .filter_map(|t| t.join().unwrap().ok())
            .collect();
        assert_eq!(winners.len(), 1);
        assert_eq!(workers.lock().unwrap()["connection"].turn_id, winners[0]);
    }

    #[test]
    fn delayed_completion_takes_only_its_own_claim() {
        let mut entries = vec![
            Claim {
                turn_id: "old".into(),
                start_seq: 10,
                at: std::time::Instant::now(),
            },
            Claim {
                turn_id: "new".into(),
                start_seq: 20,
                at: std::time::Instant::now(),
            },
        ];
        assert_eq!(
            claim_for_event(&mut entries, 20, true).as_deref(),
            Some("old")
        );
        assert_eq!(
            claim_for_event(&mut entries, 21, false).as_deref(),
            Some("new")
        );
        assert!(claim_for_event(&mut entries, 19, true).is_none());
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn persisted_turn_id_rejects_resubmission_before_any_agent_or_http_work() {
        let db = crate::db::test_helpers::fresh_in_memory_db().await;
        let directory = tempfile::tempdir().unwrap();
        let state = crate::app_state::AppState::new_for_test(db, directory.path().to_owned());
        super::super::relay_outbox::save(
            &state.db.conn,
            "already-done",
            serde_json::json!({"delivery_status":"delivered"}),
            None,
            ResultFormat::Legacy,
        )
        .await
        .unwrap();
        let spec:OutboundSpec=serde_json::from_value(serde_json::json!({"kind":"telegram","turnId":"already-done","deliveryVersion":2,"botToken":"unused","chatId":1})).unwrap();
        assert_eq!(
            register_for_prompt(&state, "missing-connection", None, spec)
                .await
                .unwrap_err(),
            "relay_turn_already_exists"
        );
        let spec:OutboundSpec=serde_json::from_value(serde_json::json!({"kind":"telegram","turnId":"not-admitted","deliveryVersion":2,"botToken":"unused","chatId":1})).unwrap();
        assert_eq!(
            register_for_prompt(&state, "missing-connection", None, spec)
                .await
                .unwrap_err(),
            "relay_connection_not_found"
        );
        assert!(
            get_result(&state.db.conn, "not-admitted", ResultFormat::Legacy)
                .await
                .unwrap()
                .is_none()
        );
    }
    #[derive(Default)]
    struct Scripted {
        replies: StdMutex<VecDeque<Result<Option<i64>, TgError>>>,
        calls: StdMutex<Vec<(String, serde_json::Value)>>,
    }
    #[async_trait]
    impl TgApi for Scripted {
        async fn call(
            &self,
            method: &str,
            body: serde_json::Value,
        ) -> Result<Option<i64>, TgError> {
            self.calls.lock().unwrap().push((method.into(), body));
            if matches!(method, "sendMessageDraft" | "sendChatAction") {
                return Ok(None);
            }
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Some(42)))
        }
    }
    fn turn(api: Arc<Scripted>) -> TelegramTurn {
        TelegramTurn {
            api,
            chat_id: 1,
            draft_base: 1,
            texts: RelayTexts::default(),
        }
    }
    fn delta(text: &str) -> RelayInput {
        input(AcpEvent::ContentDelta {
            text: text.into(),
            parent_tool_use_id: None,
        })
    }
    fn input(payload: AcpEvent) -> RelayInput {
        RelayInput::Event(Arc::new(EventEnvelope {
            seq: 0,
            connection_id: "c".into(),
            payload,
        }))
    }
    fn done() -> RelayInput {
        input(AcpEvent::TurnComplete {
            session_id: "s".into(),
            stop_reason: "end_turn".into(),
            agent_type: "codex".into(),
            duration_ms: None,
        })
    }
    #[tokio::test(start_paused = true)]
    async fn unknown_send_is_never_replayed_as_plain_or_next_tick() {
        let api = Arc::new(Scripted::default());
        api.replies.lock().unwrap().push_back(Err(TgError::Unknown));
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(turn(api.clone()).run(rx));
        tx.send(delta("hello")).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        tx.send(delta(" world")).unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        tx.send(done()).unwrap();
        let report = worker.await.unwrap();
        assert!(report.delivery_unknown);
        assert_eq!(report.final_text, "hello world");
        assert_eq!(
            api.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| m == "sendMessage")
                .count(),
            1
        );
    }
    #[tokio::test(start_paused = true)]
    async fn temporary_edit_failure_is_cleared_by_successful_final_edit() {
        let api = Arc::new(Scripted::default());
        api.replies.lock().unwrap().extend([
            Ok(Some(7)),
            Err(TgError::Other("Bad Request".into())),
            Ok(None),
        ]);
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(turn(api).run(rx));
        tx.send(delta("one")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(delta(" two")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(done()).unwrap();
        let report = worker.await.unwrap();
        assert!(!report.delivery_failed);
        assert_eq!(report.message_ids, vec![7]);
    }
    #[tokio::test(start_paused = true)]
    async fn repeated_full_text_render_does_not_advance_fence() {
        let api = Arc::new(Scripted::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(turn(api.clone()).run(rx));
        tx.send(delta("```rust\nfn a() {")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(delta("}\n```")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tx.send(done()).unwrap();
        worker.await.unwrap();
        let calls = api.calls.lock().unwrap();
        let last = calls
            .iter()
            .rev()
            .find(|(m, _)| m == "editMessageText")
            .unwrap();
        assert_eq!(
            last.1["text"],
            "<pre><code class=\"language-rust\">fn a() {}</code></pre>"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn retry_after_over_thirty_seconds_is_respected() {
        let api = Arc::new(Scripted::default());
        api.replies
            .lock()
            .unwrap()
            .extend([Err(TgError::RetryAfter(45)), Ok(Some(8))]);
        let start = Instant::now();
        assert_eq!(
            turn(api)
                .call_with_retry("sendMessage", serde_json::json!({"text":"x"}))
                .await
                .unwrap(),
            Some(8)
        );
        assert!(start.elapsed() >= Duration::from_secs(45));
    }
    #[test]
    fn utf16_split_and_segment_join_preserve_the_reply() {
        let mut composer = Composer::default();
        let text = format!("{}\n{}", "😀".repeat(1900), "尾".repeat(40));
        let heads = composer.push(&text);
        assert!(heads.iter().all(|s| s.encode_utf16().count() <= TG_CHUNK));
        assert_eq!(heads.join("") + composer.text(), text);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_whitespace_only_segments_keeps_contiguous_result_indices() {
        let api = Arc::new(Scripted::default());
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(delta(&format!("{}reply", " ".repeat(TG_CHUNK))))
            .unwrap();
        tx.send(done()).unwrap();
        let report = turn(api).run(rx).await;
        assert_eq!(report.segments.len(), 1);
        assert_eq!(report.segments[0].index, 0);
        assert_eq!(report.segments[0].text, "reply");
        let compact = super::super::relay_result::compact(
            serde_json::json!({"final_text":report.final_text,"segments":report.segments}),
        )
        .unwrap();
        assert_eq!(compact["segments"][0]["text_start"], TG_CHUNK);
    }

    #[tokio::test(start_paused = true)]
    async fn telegram_terminal_note_and_emoji_compact_without_changing_segment_body() {
        for stop_reason in ["cancelled", "refusal"] {
            let (tx, rx) = mpsc::unbounded_channel();
            tx.send(delta("🦀回答")).unwrap();
            tx.send(input(AcpEvent::TurnComplete {
                session_id: "s".into(),
                stop_reason: stop_reason.into(),
                agent_type: "codex".into(),
                duration_ms: None,
            }))
            .unwrap();
            let report = turn(Arc::new(Scripted::default())).run(rx).await;
            let original =
                serde_json::json!({"final_text":report.final_text,"segments":report.segments});
            let compact = super::super::relay_result::compact(original.clone()).unwrap();
            assert_eq!(compact["segments"][0]["text_end"], 4);
            assert_eq!(compact["segments"][1]["text_start"], 6);
            assert_eq!(
                super::super::relay_result::for_wire(compact, ResultFormat::Legacy).unwrap(),
                original
            );
        }
    }
}
