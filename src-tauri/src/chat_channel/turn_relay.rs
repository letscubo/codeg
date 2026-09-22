//! fork(letscubo)专属: 渠道回合直推(turn relay)。
//!
//! MyClaw 的 Telegram 托管 bot(一个 agent 一个 bot)收到消息后,由平台调 `acp_prompt` /
//! `cli_prompt` 驱动这一轮,并在参数里带上 `outbound`(bot token + 私聊 chat_id)。本模块
//! 按连接订阅这一轮的事件,**边生成边推到 Telegram**:
//!
//! - 还没有正文时,草稿显示一行状态:开始是「💭 思考中…」(平台按用户语言给),调用工具时换成
//!   「🔧 <工具的一句话说明>」,工具结束回到「思考中」。空草稿在 Telegram 桌面端只是个「…」气泡,
//!   看不出 agent 在干什么(2026-09-21 用户反馈)
//! - 第一段正文到达:`sendMessage` 发出**一条**消息并记下 message_id,之后正文增长一律
//!   `editMessageText` 改这条消息(Slack 的 chat.startStream / Multica 的 Telegram 出站都是
//!   这个形态:一条消息逐渐长出来)。只有超过单条长度上限才定稿当前这条、另起一条
//! - 工具调用**不再断句**:正在跑工具时状态行挂在这条消息末尾,跑完去掉
//! - 结束:最后编辑一次,写入定稿正文(去掉状态行);取消 / 失败补一句提示
//! - 草稿里的正文和正式消息一样把 Markdown 转成 Telegram HTML(见 `tg_html`):转换器只给成对
//!   闭合的标记加格式,写到一半的 `**` 原样留着,没闭合的代码块自动补上,所以半截内容也是合法
//!   HTML。草稿与正式消息同样排版 —— 某些客户端正式消息到了草稿还会多挂几秒(2026-09-21 用户
//!   反馈),挂着的也不是一堆 `##` / `**`。Telegram 报解析错误就退回纯文本重发。状态行是纯文本
//!
//! ## 节奏(2026-09-21 实测,A1-HM 托管 bot)
//! - 草稿每秒 1 次连续 90 秒不限流;每秒 1.5 次会 429 → 两次更新至少间隔 1 秒。编辑与草稿
//!   共享限额,同样按这个节流(⚠️ editMessageText 的具体额度官方无明文,待实测再调)。
//!   来字就推、推送在路上时新字攒着(本 worker 串行发请求,天然单飞),不设固定计时器
//! - 草稿没有更新约 **10 秒**就消失(文档说 30 秒;2026-09-22 在 Telegram Web 里挂 DOM 监听实测,
//!   两次都是最后一次更新后 ~10 秒消失)。消失后再发同 id 的草稿,客户端当新草稿重建并把整段
//!   文字重新「打字」一遍 —— 正文写完、这一轮迟迟不结束时就会反复「消失 → 重打 → 消失」。
//!   所以没有新字时每 5 秒重发一次续上,赶在过期前。**正文消息不会过期**,没有新内容就不动它
//!
//! ## 和平台的分工
//! - 平台照旧靠 `turn_complete` webhook 同步历史。本模块在该 webhook 里写上
//!   `relay_turn_id`(见 [`relay_turn_id_for`]),平台看到它就**不再**自己发回复,避免两份
//! - 本模块写失败时,另发一个 `relay_failed` webhook,**带上已写出的 message_id**:平台编辑
//!   这些消息写入落库正文,而不是重发一遍。编辑幂等 —— 这就是不用「投递账本」也不会重复的原因
//! - 事件总线落后(`Lagged`)时丢掉认领、停止推送:turn_complete 不带 relay_turn_id,
//!   平台照旧自己发 —— 拼接的正文可能缺字,宁可交回平台
//!
//! ## token
//! 只在本进程内存里,这一轮结束即丢;不落盘、不进日志(`Debug` 已脱敏)、**不进 agent 进程
//! 的环境变量**(agent 能执行命令)。
//!
//! 目前只接 Telegram;Slack 的流式接口(chat.startStream / appendStream)另行接入。

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::acp::internal_bus::InternalEventBus;
use crate::acp::types::{AcpEvent, ConnectionStatus, EventEnvelope};
use crate::db::service::app_metadata_service;

use super::tg_html;

/// 两次更新(草稿或编辑正文)的最小间隔。实测草稿 1/s 稳定、1.5/s 触发 429;
/// editMessageText 与它共享限额,同样按 1/s 节流(业界常见取 200–800ms,这里保守些)。
const MIN_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// 没有新字时续草稿的间隔。草稿不更新约 10 秒就消失(见文件头),取一半留余量。
const DRAFT_HEARTBEAT: Duration = Duration::from_secs(5);
/// 一轮最长跟多久;超过按失败收尾,防止连接异常没有结束事件时 worker 常驻。
const MAX_TURN: Duration = Duration::from_secs(3 * 60 * 60);
/// 认领记录保留多久(给 event_subscriber 取;正常在 turn_complete 时就取走了)。
const CLAIM_TTL: Duration = Duration::from_secs(6 * 60 * 60);
/// 草稿 / 单条消息的字数上限。Telegram 是 4096(按 UTF-16 计),留余量给 emoji 之类双单元字符。
const TG_CHUNK: usize = 3800;
/// 最终消息 429 时的重试次数与单次等待上限。
const SEND_RETRIES: usize = 3;
const MAX_RETRY_WAIT: Duration = Duration::from_secs(30);

// ── 入参 ────────────────────────────────────────────────────────────────────

/// `acp_prompt` / `cli_prompt` 的 `outbound` 参数。
#[derive(Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum OutboundSpec {
    #[serde(rename_all = "camelCase")]
    Telegram {
        /// 平台生成的本轮 id,原样写回 webhook(`relay_turn_id`)。
        turn_id: String,
        bot_token: String,
        chat_id: i64,
        /// 草稿 id 基数(非 0);不给就由 turn_id 派生。每段正文用 `基数 + 段号`。
        #[serde(default)]
        draft_id: Option<i64>,
        #[serde(default)]
        texts: RelayTexts,
    },
}

impl std::fmt::Debug for OutboundSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutboundSpec::Telegram { turn_id, chat_id, .. } => f
                .debug_struct("Telegram")
                .field("turn_id", turn_id)
                .field("chat_id", chat_id)
                .field("bot_token", &"<redacted>")
                .finish(),
        }
    }
}

impl OutboundSpec {
    pub fn turn_id(&self) -> &str {
        match self {
            OutboundSpec::Telegram { turn_id, .. } => turn_id,
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
        while self.text.chars().count() > TG_CHUNK {
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
    let cut = text
        .char_indices()
        .nth(max)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    let window = &text[..cut];
    let at = match window.rfind('\n') {
        Some(i) if i > 0 => i,
        _ => cut,
    };
    let head = text[..at].to_string();
    let tail = text[at..].trim_start_matches('\n').to_string();
    (head, tail)
}

/// 由 turn_id 派生草稿 id 基数(1..2^30),留出空间给 `+ 段号`。
fn derive_draft_base(turn_id: &str) -> i64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in turn_id.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    ((h % (1 << 30)) as i64).max(1)
}

// ── Telegram 调用 ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) enum TgError {
    /// 429:按 Telegram 给的秒数等。
    RetryAfter(u64),
    Other(String),
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
            .map_err(|e| TgError::Other(format!("request failed (timeout={})", e.is_timeout())))?;
        let v: serde_json::Value = res
            .json()
            .await
            .map_err(|_| TgError::Other("unreadable response".into()))?;
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
                .to_string(),
        ))
    }
}

// ── 一轮的 worker ────────────────────────────────────────────────────────────

pub(crate) enum RelayInput {
    Event(Arc<EventEnvelope>),
    /// 事件总线落后:正文可能缺字,停止推送,交回平台。
    Lagged,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Completed,
    Cancelled,
    Failed,
    /// 连接被新一轮顶替 / 提交失败被撤回:什么都不发。
    Aborted,
    /// 事件总线落后:什么都不发,由平台补。
    Lagged,
}

pub(crate) struct TurnReport {
    pub outcome: Outcome,
    /// 有内容没写出去(平台需要兜底)。
    pub delivery_failed: bool,
    /// 这一轮写过的 Telegram 消息 id,按顺序。平台兜底时**编辑**这些消息,不重发。
    pub message_ids: Vec<i64>,
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

/// 正在写的那条 Telegram 消息。
struct Live {
    message_id: i64,
    /// 这条消息当前显示的文字(相同就不必再编辑)。
    shown: String,
}

impl TelegramTurn {
    pub(crate) async fn run(self, mut rx: mpsc::UnboundedReceiver<RelayInput>) -> TurnReport {
        let deadline = Instant::now() + MAX_TURN;
        let mut body = Composer::default();
        let mut live: Option<Live> = None;
        // 这一轮用到的消息 id,按顺序;失败时交给平台,让它编辑同一条而不是重发
        let mut message_ids: Vec<i64> = Vec::new();
        let mut delivery_failed = false;
        // 还没有正文时草稿显示过什么(避免同样的状态重复推)
        let mut draft_shown: Option<String> = None;
        let mut last_update: Option<Instant> = None;
        let mut blocked_until: Option<Instant> = None;
        // 长回复被切成多条时,跨条的未闭合代码块(见 tg_html::render_chunk)
        let mut fence: Option<String> = None;
        let mut status = self.texts.thinking.clone();
        let mut status_tool: Option<String> = None;

        // 立刻给出状态(还没有正文,走草稿:不占消息历史)
        self.draft(&status, &mut draft_shown, &mut last_update, &mut blocked_until)
            .await;

        let outcome = loop {
            let want = self.display(&body, &status, status_tool.is_some());
            let dirty = match &live {
                Some(l) => l.shown != want,
                None => draft_shown.as_deref() != Some(want.as_str()),
            };
            let base = last_update.unwrap_or_else(Instant::now);
            // 正文消息不会过期,没有新内容就不用动;草稿约 10 秒消失,要心跳续上
            let mut next = if dirty {
                base + MIN_UPDATE_INTERVAL
            } else if live.is_some() {
                base + MAX_TURN
            } else {
                base + DRAFT_HEARTBEAT
            };
            if let Some(b) = blocked_until {
                next = next.max(b);
            }

            tokio::select! {
                input = rx.recv() => match input {
                    None => break Outcome::Aborted,
                    Some(RelayInput::Lagged) => break Outcome::Lagged,
                    Some(RelayInput::Event(env)) => match &env.payload {
                        AcpEvent::ContentDelta { text, parent_tool_use_id: None } => {
                            // 超长:当前这条消息定稿成 head,之后的正文另起一条
                            for head in body.push(text) {
                                delivery_failed |= !self
                                    .write(&head, &mut live, &mut fence, &mut message_ids, &mut last_update, &mut blocked_until)
                                    .await;
                                live = None;
                            }
                        }
                        AcpEvent::ToolCall { tool_call_id, title, description, .. } => {
                            if let Some(line) = tool_status(description.as_deref(), title) {
                                status = line;
                                status_tool = Some(tool_call_id.clone());
                            }
                        }
                        AcpEvent::ToolCallUpdate { tool_call_id, title, description, status: tool_state, .. }
                            if status_tool.as_deref() == Some(tool_call_id.as_str()) =>
                        {
                            if matches!(tool_state.as_deref(), Some("completed" | "failed")) {
                                status = self.texts.thinking.clone();
                                status_tool = None;
                            } else if let Some(line) = description
                                .as_deref()
                                .map(|d| tool_status(Some(d), ""))
                                .or_else(|| title.as_deref().map(|t| tool_status(None, t)))
                                .flatten()
                            {
                                // 首帧没带说明、更新时才补上的
                                status = line;
                            }
                        }
                        AcpEvent::TurnComplete { stop_reason, .. } => {
                            break match stop_reason.as_str() {
                                "end_turn" => Outcome::Completed,
                                "cancelled" => Outcome::Cancelled,
                                _ => Outcome::Failed,
                            };
                        }
                        AcpEvent::Error { terminal: true, .. } => break Outcome::Failed,
                        AcpEvent::StatusChanged {
                            status: ConnectionStatus::Disconnected | ConnectionStatus::Error,
                        } => break Outcome::Failed,
                        _ => {}
                    },
                },
                _ = tokio::time::sleep_until(next) => {
                    let want = self.display(&body, &status, status_tool.is_some());
                    if body.text().trim().is_empty() {
                        self.draft(&want, &mut draft_shown, &mut last_update, &mut blocked_until).await;
                    } else {
                        delivery_failed |= !self
                            .write(&want, &mut live, &mut fence, &mut message_ids, &mut last_update, &mut blocked_until)
                            .await;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break Outcome::Failed,
            }
        };

        if matches!(outcome, Outcome::Aborted | Outcome::Lagged) {
            return TurnReport {
                outcome,
                delivery_failed: false,
                message_ids,
            };
        }

        // 收尾:正文定稿(去掉状态行)
        let final_text = body.text().trim().to_string();
        if !final_text.is_empty() {
            delivery_failed |= !self
                .write(&final_text, &mut live, &mut fence, &mut message_ids, &mut last_update, &mut blocked_until)
                .await;
        }
        let note = match outcome {
            Outcome::Completed if final_text.is_empty() && message_ids.is_empty() => Some(&self.texts.empty),
            Outcome::Cancelled => Some(&self.texts.cancelled),
            Outcome::Failed => Some(&self.texts.failed),
            _ => None,
        };
        if let Some(note) = note {
            match self
                .send(serde_json::json!({ "chat_id": self.chat_id, "text": note }))
                .await
            {
                Ok(Some(id)) => message_ids.push(id),
                Ok(None) => {}
                Err(_) => delivery_failed = true,
            }
        }
        // 草稿只在还没有正文时用过;正文消息发出后它会自行消失
        TurnReport {
            outcome,
            delivery_failed,
            message_ids,
        }
    }

    /// 这条消息该显示什么:正文,外加正在跑工具时末尾的一行状态。
    /// 还没有正文时就只有状态行(那时走草稿)。
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

    /// 写正文:没有活动消息就新发一条并记下 id,有就编辑它。
    ///
    /// 编辑天然幂等 —— 重试同一条内容结果一样,不会像「发多条」那样重复。Telegram 说
    /// 「内容没变」按成功算;消息被用户删了就重新发一条,接着在新消息上写。
    /// 返回是否写成功。
    async fn write(
        &self,
        text: &str,
        live: &mut Option<Live>,
        fence: &mut Option<String>,
        message_ids: &mut Vec<i64>,
        last_update: &mut Option<Instant>,
        blocked_until: &mut Option<Instant>,
    ) -> bool {
        if text.trim().is_empty() {
            return true;
        }
        if live.as_ref().is_some_and(|l| l.shown == text) {
            return true;
        }
        *last_update = Some(Instant::now());
        // 跨消息的未闭合代码块:本条用一份副本,定稿时才落回真实状态
        let mut local_fence = fence.clone();
        let html = tg_html::render_chunk(text, &mut local_fence);
        let ok = match live.as_ref().map(|l| l.message_id) {
            None => {
                let body = serde_json::json!({ "chat_id": self.chat_id, "text": html, "parse_mode": "HTML" });
                let plain = serde_json::json!({ "chat_id": self.chat_id, "text": text });
                match self.send_with_plain_fallback("sendMessage", body, plain).await {
                    Ok(id) => {
                        if let Some(id) = id {
                            message_ids.push(id);
                            *live = Some(Live { message_id: id, shown: text.to_string() });
                        }
                        true
                    }
                    Err(e) => {
                        tracing::warn!("[TurnRelay] telegram sendMessage failed: {e}");
                        false
                    }
                }
            }
            Some(id) => {
                let body = serde_json::json!({
                    "chat_id": self.chat_id, "message_id": id, "text": html, "parse_mode": "HTML"
                });
                let plain = serde_json::json!({ "chat_id": self.chat_id, "message_id": id, "text": text });
                match self.send_with_plain_fallback("editMessageText", body, plain).await {
                    Ok(_) => {
                        if let Some(l) = live.as_mut() {
                            l.shown = text.to_string();
                        }
                        true
                    }
                    Err(e) if e.contains("not modified") => true,
                    Err(e) if e.contains("not found") || e.contains("can't be edited") => {
                        // 用户把消息删了:另起一条接着写
                        tracing::warn!("[TurnRelay] telegram message {id} no longer editable ({e}); sending a new one");
                        *live = None;
                        Box::pin(self.write(text, live, fence, message_ids, last_update, blocked_until)).await
                    }
                    Err(e) => {
                        tracing::warn!("[TurnRelay] telegram editMessageText failed: {e}");
                        false
                    }
                }
            }
        };
        *fence = local_fence;
        *blocked_until = None;
        ok
    }

    /// 还没有正文时的状态草稿(不占消息历史)。纯展示:失败只记日志,429 记下冷却时间。
    async fn draft(
        &self,
        text: &str,
        draft_shown: &mut Option<String>,
        last_update: &mut Option<Instant>,
        blocked_until: &mut Option<Instant>,
    ) {
        let body = serde_json::json!({
            "chat_id": self.chat_id, "draft_id": self.draft_base, "text": text
        });
        *last_update = Some(Instant::now());
        match self.api.call("sendMessageDraft", body).await {
            Ok(_) => {
                *draft_shown = Some(text.to_string());
                *blocked_until = None;
            }
            Err(TgError::RetryAfter(s)) => {
                *blocked_until = Some(Instant::now() + Duration::from_secs(s));
            }
            Err(TgError::Other(e)) => {
                tracing::warn!("[TurnRelay] telegram draft failed: {e}");
                // 当作已显示,免得每个 tick 都重试同一句;有新内容会再发
                *draft_shown = Some(text.to_string());
            }
        }
    }

    /// 先按 HTML 发;Telegram 拒收(解析错误等)就退回纯文本再发一次。
    async fn send_with_plain_fallback(
        &self,
        method: &str,
        html: serde_json::Value,
        plain: serde_json::Value,
    ) -> Result<Option<i64>, String> {
        match self.call_with_retry(method, html).await {
            Ok(id) => Ok(id),
            Err(e) if e.contains("not modified") || e.contains("not found") || e.contains("can't be edited") => Err(e),
            Err(e) => {
                tracing::warn!("[TurnRelay] telegram HTML rejected ({e}); retrying as plain text");
                self.call_with_retry(method, plain).await
            }
        }
    }

    /// 429 按 Telegram 给的秒数等待后重试(重试同一条编辑是幂等的)。
    async fn call_with_retry(
        &self,
        method: &str,
        body: serde_json::Value,
    ) -> Result<Option<i64>, String> {
        for _ in 0..SEND_RETRIES {
            match self.api.call(method, body.clone()).await {
                Ok(id) => return Ok(id),
                Err(TgError::RetryAfter(s)) => {
                    tokio::time::sleep(Duration::from_secs(s).min(MAX_RETRY_WAIT)).await;
                }
                Err(TgError::Other(e)) => return Err(e),
            }
        }
        Err("rate limited".into())
    }

    /// 发一条独立消息(收尾提示语)。
    async fn send(&self, body: serde_json::Value) -> Result<Option<i64>, String> {
        self.call_with_retry("sendMessage", body).await
    }
}

// ── 全局登记 ─────────────────────────────────────────────────────────────────

struct Worker {
    turn_id: String,
    tx: mpsc::UnboundedSender<RelayInput>,
}

struct Claim {
    turn_id: String,
    at: std::time::Instant,
}

struct Relay {
    db: DatabaseConnection,
    client: reqwest::Client,
    workers: StdMutex<HashMap<String, Worker>>,
    claims: StdMutex<HashMap<String, Claim>>,
}

static RELAY: OnceLock<Arc<Relay>> = OnceLock::new();

/// 首次使用时建好登记表并挂上总线订阅(订阅在返回前完成,之后 send 的事件不会漏)。
fn relay(bus: &Arc<InternalEventBus>, db: &DatabaseConnection) -> Arc<Relay> {
    Arc::clone(RELAY.get_or_init(|| {
        let relay = Arc::new(Relay {
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

async fn dispatch(
    relay: Arc<Relay>,
    mut rx: tokio::sync::broadcast::Receiver<Arc<EventEnvelope>>,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(env) => {
                let workers = relay.workers.lock().unwrap();
                if let Some(w) = workers.get(&env.connection_id) {
                    let _ = w.tx.send(RelayInput::Event(env));
                }
            }
            Err(RecvError::Lagged(n)) => {
                tracing::warn!("[TurnRelay] event bus lagged by {n}; handing active turns back");
                let mut workers = relay.workers.lock().unwrap();
                let mut claims = relay.claims.lock().unwrap();
                for (conn, w) in workers.drain() {
                    let _ = w.tx.send(RelayInput::Lagged);
                    claims.remove(&conn);
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
) -> Option<String> {
    if let Some(session) = state.connection_manager.get_state(connection_id).await {
        if session.read().await.turn_in_flight {
            return None;
        }
    }
    Some(register(
        &state.acp_event_bus,
        &state.db.conn,
        connection_id,
        conversation_id,
        spec,
    ))
}

/// 登记一轮直推。须在提交 prompt **之前**调用(之后的事件才不会漏)。
fn register(
    bus: &Arc<InternalEventBus>,
    db: &DatabaseConnection,
    connection_id: &str,
    conversation_id: Option<i32>,
    spec: OutboundSpec,
) -> String {
    let relay = relay(bus, db);
    let turn_id = spec.turn_id().to_string();
    let (tx, rx) = mpsc::unbounded_channel();
    {
        let mut workers = relay.workers.lock().unwrap();
        // 同一连接上一轮还登记着 → 替换掉(旧 worker 的 rx 关闭,按 Aborted 退出)
        workers.insert(
            connection_id.to_string(),
            Worker {
                turn_id: turn_id.clone(),
                tx,
            },
        );
        let mut claims = relay.claims.lock().unwrap();
        claims.retain(|_, c| c.at.elapsed() < CLAIM_TTL);
        claims.insert(
            connection_id.to_string(),
            Claim {
                turn_id: turn_id.clone(),
                at: std::time::Instant::now(),
            },
        );
    }

    let OutboundSpec::Telegram {
        bot_token,
        chat_id,
        draft_id,
        texts,
        ..
    } = spec;
    let turn = TelegramTurn {
        api: Arc::new(HttpTg {
            client: relay.client.clone(),
            token: bot_token,
        }),
        chat_id,
        draft_base: draft_id.filter(|d| *d > 0).unwrap_or_else(|| derive_draft_base(&turn_id)),
        texts,
    };
    let connection_id = connection_id.to_string();
    let tid = turn_id.clone();
    tokio::spawn(async move {
        let report = turn.run(rx).await;
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
        if report.delivery_failed {
            report_failure(&relay, &connection_id, conversation_id, &tid, &report.message_ids).await;
        }
    });
    turn_id
}

/// 提交 prompt 失败时撤回登记(只撤本轮的,不误伤后来者)。
pub fn abort(connection_id: &str, turn_id: &str) {
    let Some(relay) = RELAY.get() else { return };
    let mut workers = relay.workers.lock().unwrap();
    if workers.get(connection_id).map(|w| w.turn_id == turn_id) == Some(true) {
        workers.remove(connection_id); // 丢掉 tx → worker 按 Aborted 退出
    }
    let mut claims = relay.claims.lock().unwrap();
    if claims.get(connection_id).map(|c| c.turn_id == turn_id) == Some(true) {
        claims.remove(connection_id);
    }
}

/// webhook 用:这条连接的当前轮是否由本模块直推。`take` = 取走(turn_complete 时)。
pub fn relay_turn_id_for(connection_id: &str, take: bool) -> Option<String> {
    let relay = RELAY.get()?;
    let mut claims = relay.claims.lock().unwrap();
    let fresh = claims
        .get(connection_id)
        .is_some_and(|c| c.at.elapsed() < CLAIM_TTL);
    if !fresh {
        claims.remove(connection_id);
        return None;
    }
    if take {
        claims.remove(connection_id).map(|c| c.turn_id)
    } else {
        claims.get(connection_id).map(|c| c.turn_id.clone())
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
    let urls = app_metadata_service::get_value(&relay.db, super::event_subscriber::EVENT_WEBHOOKS_KEY)
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
        assert_eq!(c.text(), format!("{line}\n尾巴"));
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
        async fn call(&self, method: &str, body: serde_json::Value) -> Result<Option<i64>, TgError> {
            let text = body["text"].as_str().unwrap_or_default().to_string();
            self.calls.lock().unwrap().push((method.to_string(), text));
            if self.fail_messages && method == "sendMessage" {
                return Err(TgError::Other("Forbidden".into()));
            }
            if self.edit_gone && method == "editMessageText" {
                return Err(TgError::Other("Bad Request: message to edit not found".into()));
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
            vec![delta("我先看看"), tool_call("t", "Bash", None), delta("结论是 "), delta("42"), complete("end_turn")],
        )
        .await;
        assert_eq!(r.outcome, Outcome::Completed);
        assert!(!r.delivery_failed);
        // 工具调用不再把正文切成两条
        assert_eq!(messages(&api), vec!["我先看看结论是 42"]);
        assert_eq!(r.message_ids, vec![1]);
        // 第一次调用就是「思考中」状态,不是空草稿
        assert_eq!(drafts(&api).first().unwrap(), &RelayTexts::default().thinking);
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
        tx.send(tool_call("t1", "df -h", Some("查看磁盘使用情况"))).unwrap();
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

        tx.send(tool_call("t1", "df -h", Some("查看磁盘使用情况"))).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(drafts(&api).last().unwrap(), "🔧 查看磁盘使用情况");
        assert!(messages(&api).is_empty(), "还没有正文就不占消息历史");

        tx.send(tool_done("t1")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(drafts(&api).last().unwrap(), &RelayTexts::default().thinking);

        tx.send(complete("end_turn")).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn empty_and_cancelled_and_failed_turns_leave_a_note() {
        let api = Arc::new(FakeTg::default());
        run_with(Arc::clone(&api), vec![complete("end_turn")]).await;
        assert_eq!(messages(&api), vec![RelayTexts::default().empty]);

        let api = Arc::new(FakeTg::default());
        run_with(Arc::clone(&api), vec![delta("写了一半"), complete("cancelled")]).await;
        assert_eq!(messages(&api), vec!["写了一半".to_string(), RelayTexts::default().cancelled]);

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
    async fn markdown_is_sent_as_html_and_falls_back_to_plain() {
        let api = Arc::new(FakeTg::default());
        run_with(Arc::clone(&api), vec![delta("**总结**:磁盘 `/` 充裕"), complete("end_turn")]).await;
        assert_eq!(messages(&api), vec!["<b>总结</b>:磁盘 <code>/</code> 充裕"]);

        let api = Arc::new(FakeTg {
            reject_html: true,
            ..Default::default()
        });
        let r = run_with(Arc::clone(&api), vec![delta("**总结**"), complete("end_turn")]).await;
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
        assert_eq!(r.message_ids, vec![1, 2]);
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
        assert!("abcd".starts_with(messages(&api)[0].as_str()), "{:?}", messages(&api));
        assert_eq!(edits(&api).last().unwrap(), "abcd");

        // 正文消息不会过期 → 没有新字就不再调用
        let before = api.calls.lock().unwrap().len();
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(api.calls.lock().unwrap().len(), before, "静默时不打扰 Telegram");

        tx.send(complete("end_turn")).unwrap();
        let r = handle.await.unwrap();
        assert_eq!(r.outcome, Outcome::Completed);
        assert_eq!(messages(&api).len(), 1, "全程只发了一条消息");
        assert_eq!(edits(&api).last().unwrap(), "abcd");
    }
}
