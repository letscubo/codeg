//! fork(letscubo)专属: 渠道回合直推(turn relay)。
//!
//! MyClaw 的 Telegram 托管 bot(一个 agent 一个 bot)收到消息后,由平台调 `acp_prompt` /
//! `cli_prompt` 驱动这一轮,并在参数里带上 `outbound`(bot token + 私聊 chat_id)。本模块
//! 按连接订阅这一轮的事件,**边生成边推到 Telegram**:
//!
//! - 还没有正文时,草稿显示一行状态:开始是「💭 思考中…」(平台按用户语言给),调用工具时换成
//!   「🔧 <工具的一句话说明>」,工具结束回到「思考中」。空草稿在 Telegram 桌面端只是个「…」气泡,
//!   看不出 agent 在干什么(2026-09-21 用户反馈)
//! - 有新正文:草稿换成正文(`sendMessageDraft`,同一 draft_id 动画续写)
//! - 插入工具调用:工具前那段正文先 `sendMessage` 正式发出,工具后的正文进新草稿
//!   (与平台「一块一行」的历史一致)
//! - 结束:剩余正文 `sendMessage` 正式发出(草稿随之消失);取消 / 失败补一句提示
//! - 草稿里的正文和正式消息一样把 Markdown 转成 Telegram HTML(见 `tg_html`):转换器只给成对
//!   闭合的标记加格式,写到一半的 `**` 原样留着,没闭合的代码块自动补上,所以半截内容也是合法
//!   HTML。草稿与正式消息同样排版 —— 某些客户端正式消息到了草稿还会多挂几秒(2026-09-21 用户
//!   反馈),挂着的也不是一堆 `##` / `**`。Telegram 报解析错误就退回纯文本重发。状态行是纯文本
//!
//! ## 节奏(2026-09-21 实测,A1-HM 托管 bot)
//! - 草稿每秒 1 次连续 90 秒不限流;每秒 1.5 次会 429 → 两次草稿至少间隔 1 秒。
//!   来字就推、推送在路上时新字攒着(本 worker 串行发请求,天然单飞),不设固定计时器
//! - 草稿没有更新约 **10 秒**就消失(文档说 30 秒;2026-09-22 在 Telegram Web 里挂 DOM 监听实测,
//!   两次都是最后一次更新后 ~10 秒消失)。消失后再发同 id 的草稿,客户端当新草稿重建并把整段
//!   文字重新「打字」一遍 —— 正文写完、这一轮迟迟不结束时就会反复「消失 → 重打 → 消失」。
//!   所以没有新字时每 5 秒重发一次续上,赶在过期前
//!
//! ## 和平台的分工
//! - 平台照旧靠 `turn_complete` webhook 同步历史。本模块在该 webhook 里写上
//!   `relay_turn_id`(见 [`relay_turn_id_for`]),平台看到它就**不再**自己发回复,避免两份
//! - 本模块最终消息发送失败时,另发一个 `relay_failed` webhook,由平台用落库的正文兜底补发
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

/// 两次草稿最小间隔(实测 1/s 稳定,1.5/s 触发 429)。
const MIN_DRAFT_INTERVAL: Duration = Duration::from_secs(1);
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

/// 本轮正文按「段」管理:一段 = 一条 Telegram 消息的内容。草稿显示当前段。
#[derive(Default, Debug)]
pub(crate) struct Segmenter {
    text: String,
    segment: u32,
}

impl Segmenter {
    pub(crate) fn segment(&self) -> u32 {
        self.segment
    }

    pub(crate) fn draft_text(&self) -> &str {
        &self.text
    }

    /// 追加正文。当前段超长时,把能发的部分切出来作为正式消息返回,余下留在新段。
    pub(crate) fn push(&mut self, delta: &str) -> Vec<String> {
        self.text.push_str(delta);
        let mut out = Vec::new();
        while self.text.chars().count() > TG_CHUNK {
            let (head, tail) = split_head(&self.text, TG_CHUNK);
            out.push(head);
            self.text = tail;
            self.segment += 1;
        }
        out
    }

    /// 工具调用插进来:当前段(非空白)整段收成正式消息,开新段。
    pub(crate) fn break_segment(&mut self) -> Vec<String> {
        if self.text.trim().is_empty() {
            self.text.clear();
            return Vec::new();
        }
        let out = split_for_telegram(&std::mem::take(&mut self.text));
        self.segment += 1;
        out
    }

    /// 收尾:剩下的正文。
    pub(crate) fn finish(&mut self) -> Vec<String> {
        self.break_segment()
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

/// 整段正文切成若干条(每条 ≤ TG_CHUNK 字),空白段不发。
pub(crate) fn split_for_telegram(text: &str) -> Vec<String> {
    let mut rest = text.trim().to_string();
    let mut out = Vec::new();
    while rest.chars().count() > TG_CHUNK {
        let (head, tail) = split_head(&rest, TG_CHUNK);
        if !head.trim().is_empty() {
            out.push(head);
        }
        rest = tail;
    }
    if !rest.trim().is_empty() {
        out.push(rest);
    }
    out
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
    async fn call(&self, method: &str, body: serde_json::Value) -> Result<(), TgError>;
}

struct HttpTg {
    client: reqwest::Client,
    token: String,
}

#[async_trait]
impl TgApi for HttpTg {
    async fn call(&self, method: &str, body: serde_json::Value) -> Result<(), TgError> {
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
            return Ok(());
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
    /// 有正式消息没发出去(平台需要兜底)。
    pub delivery_failed: bool,
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
    pub(crate) async fn run(self, mut rx: mpsc::UnboundedReceiver<RelayInput>) -> TurnReport {
        let deadline = Instant::now() + MAX_TURN;
        let mut seg = Segmenter::default();
        let mut sent_any = false;
        let mut delivery_failed = false;
        // 上一次草稿显示的(段号, 文字);不同即「有新内容」
        let mut shown: Option<(u32, String)> = None;
        let mut last_draft: Option<Instant> = None;
        let mut blocked_until: Option<Instant> = None;
        // 长回复被切成多条时,跨条的未闭合代码块(见 tg_html::render_chunk)
        let mut fence: Option<String> = None;
        // 没有正文时草稿显示的状态行,及它对应的工具(工具结束时回到「思考中」)
        let mut status = self.texts.thinking.clone();
        let mut status_tool: Option<String> = None;

        // 立刻给出状态
        self.draft(&seg, &status, &fence, &mut shown, &mut last_draft, &mut blocked_until)
            .await;

        let outcome = loop {
            let dirty = shown.as_ref() != Some(&(seg.segment(), self.display(&seg, &status)));
            let base = last_draft.unwrap_or_else(Instant::now);
            let mut next = if dirty {
                base + MIN_DRAFT_INTERVAL
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
                            for m in seg.push(text) {
                                sent_any = true;
                                delivery_failed |= !self.message(&m, &mut fence).await;
                            }
                        }
                        AcpEvent::ToolCall { tool_call_id, title, description, .. } => {
                            for m in seg.break_segment() {
                                sent_any = true;
                                delivery_failed |= !self.message(&m, &mut fence).await;
                            }
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
                    self.draft(&seg, &status, &fence, &mut shown, &mut last_draft, &mut blocked_until).await;
                }
                _ = tokio::time::sleep_until(deadline) => break Outcome::Failed,
            }
        };

        if matches!(outcome, Outcome::Aborted | Outcome::Lagged) {
            return TurnReport {
                outcome,
                delivery_failed: false,
            };
        }

        for m in seg.finish() {
            sent_any = true;
            delivery_failed |= !self.message(&m, &mut fence).await;
        }
        let note = match outcome {
            Outcome::Completed if !sent_any => Some(&self.texts.empty),
            Outcome::Cancelled => Some(&self.texts.cancelled),
            Outcome::Failed => Some(&self.texts.failed),
            _ => None,
        };
        if let Some(note) = note {
            delivery_failed |= !self.send_message(serde_json::json!({ "chat_id": self.chat_id, "text": note })).await.is_ok();
        }
        TurnReport {
            outcome,
            delivery_failed,
        }
    }

    /// 草稿该显示什么:当前段有正文就显示正文,否则显示状态行。
    fn display(&self, seg: &Segmenter, status: &str) -> String {
        if seg.draft_text().trim().is_empty() {
            status.to_string()
        } else {
            seg.draft_text().to_string()
        }
    }

    /// 更新草稿。纯展示:失败只记日志,429 记下冷却时间。
    ///
    /// 正文按 Markdown 转 HTML 发(`fence` 是之前已发出那几段留下的未闭合代码块,克隆一份用,
    /// 不改真实状态);状态行纯文本。HTML 被拒就当场改发纯文本。
    async fn draft(
        &self,
        seg: &Segmenter,
        status: &str,
        fence: &Option<String>,
        shown: &mut Option<(u32, String)>,
        last_draft: &mut Option<Instant>,
        blocked_until: &mut Option<Instant>,
    ) {
        let segment = seg.segment();
        let text = self.display(seg, status);
        let draft_id = self.draft_base + i64::from(segment);
        let plain = serde_json::json!({ "chat_id": self.chat_id, "draft_id": draft_id, "text": text });
        let body = if seg.draft_text().trim().is_empty() {
            plain.clone()
        } else {
            let mut f = fence.clone();
            serde_json::json!({
                "chat_id": self.chat_id,
                "draft_id": draft_id,
                "text": tg_html::render_chunk(&text, &mut f),
                "parse_mode": "HTML",
            })
        };
        *last_draft = Some(Instant::now());
        let mut result = self.api.call("sendMessageDraft", body.clone()).await;
        if matches!(result, Err(TgError::Other(_))) && body.get("parse_mode").is_some() {
            result = self.api.call("sendMessageDraft", plain).await;
        }
        match result {
            Ok(()) => {
                *shown = Some((segment, text));
                *blocked_until = None;
            }
            Err(TgError::RetryAfter(s)) => {
                *blocked_until = Some(Instant::now() + Duration::from_secs(s));
            }
            Err(TgError::Other(e)) => {
                tracing::warn!("[TurnRelay] telegram draft failed: {e}");
                // 当作已显示,免得每个 tick 都重试同一段;有新字会再发
                *shown = Some((segment, text));
            }
        }
    }

    /// 发一条正式消息:Markdown 转 Telegram HTML 发;Telegram 拒收(解析错误等)就退回纯文本
    /// 再发一次。返回是否发出。
    async fn message(&self, md: &str, fence: &mut Option<String>) -> bool {
        let html = tg_html::render_chunk(md, fence);
        let formatted = serde_json::json!({ "chat_id": self.chat_id, "text": html, "parse_mode": "HTML" });
        match self.send_message(formatted).await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("[TurnRelay] telegram HTML message rejected ({e}); retrying as plain text");
                let plain = serde_json::json!({ "chat_id": self.chat_id, "text": md });
                match self.send_message(plain).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!("[TurnRelay] telegram sendMessage failed: {e}");
                        false
                    }
                }
            }
        }
    }

    /// sendMessage;429 按 Telegram 给的秒数等待后重试。
    async fn send_message(&self, body: serde_json::Value) -> Result<(), String> {
        for _ in 0..SEND_RETRIES {
            match self.api.call("sendMessage", body.clone()).await {
                Ok(()) => return Ok(()),
                Err(TgError::RetryAfter(s)) => {
                    tokio::time::sleep(Duration::from_secs(s).min(MAX_RETRY_WAIT)).await;
                }
                Err(TgError::Other(e)) => return Err(e),
            }
        }
        Err("rate limited".into())
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
            report_failure(&relay, &connection_id, conversation_id, &tid).await;
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

/// 最终消息没发出去 → 发 `relay_failed` webhook,平台用落库的正文补发。
async fn report_failure(
    relay: &Relay,
    connection_id: &str,
    conversation_id: Option<i32>,
    turn_id: &str,
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
        "occurred_at": chrono::Utc::now().to_rfc3339(),
        "source": "codeg",
    });
    super::webhook::spawn_webhook_delivery(relay.client.clone(), urls, payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segmenter_breaks_on_tool_and_keeps_order() {
        let mut s = Segmenter::default();
        assert!(s.push("先说一句").is_empty());
        assert_eq!(s.draft_text(), "先说一句");
        assert_eq!(s.break_segment(), vec!["先说一句".to_string()]);
        assert_eq!(s.segment(), 1);
        assert_eq!(s.draft_text(), "");
        // 空白段不发、不占段号
        s.push("  \n");
        assert!(s.break_segment().is_empty());
        assert_eq!(s.segment(), 1);
        s.push("工具之后");
        assert_eq!(s.finish(), vec!["工具之后".to_string()]);
    }

    #[test]
    fn segmenter_overflow_emits_head_and_keeps_tail() {
        let mut s = Segmenter::default();
        let line = "字".repeat(1000);
        let text = format!("{line}\n{line}\n{line}\n{line}\n尾巴");
        let out = s.push(&text);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], format!("{line}\n{line}\n{line}"));
        assert_eq!(s.draft_text(), format!("{line}\n尾巴"));
        assert_eq!(s.segment(), 1);
    }

    #[test]
    fn split_without_newlines_hard_cuts_by_chars() {
        let text = "长".repeat(TG_CHUNK * 2 + 5);
        let parts = split_for_telegram(&text);
        assert_eq!(parts.len(), 3);
        assert!(parts.iter().all(|p| p.chars().count() <= TG_CHUNK));
        assert_eq!(parts.concat(), text);
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
        fail_messages: bool,
        /// 模拟 Telegram 拒收 HTML(can't parse entities)
        reject_html: bool,
    }

    #[async_trait]
    impl TgApi for FakeTg {
        async fn call(&self, method: &str, body: serde_json::Value) -> Result<(), TgError> {
            let text = body["text"].as_str().unwrap_or_default().to_string();
            self.calls.lock().unwrap().push((method.to_string(), text));
            if self.fail_messages && method == "sendMessage" {
                return Err(TgError::Other("Forbidden".into()));
            }
            if self.reject_html && body.get("parse_mode").is_some() {
                return Err(TgError::Other("Bad Request: can't parse entities".into()));
            }
            Ok(())
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

    fn tool() -> RelayInput {
        env(AcpEvent::ToolCall {
            tool_call_id: "t".into(),
            title: "Bash".into(),
            kind: "execute".into(),
            status: "pending".into(),
            tool_name: None,
            description: None,
            content: None,
            raw_input: None,
            raw_output: None,
            locations: None,
            meta: None,
            images: None,
        })
    }

    async fn run_with(api: Arc<FakeTg>, inputs: Vec<RelayInput>) -> TurnReport {
        let (tx, rx) = mpsc::unbounded_channel();
        for i in inputs {
            tx.send(i).unwrap();
        }
        // 输入发完就关:没有结束事件的用例据此走 Aborted,而不是一直等到 MAX_TURN
        drop(tx);
        let turn = TelegramTurn {
            api,
            chat_id: 1,
            draft_base: 100,
            texts: RelayTexts::default(),
        };
        turn.run(rx).await
    }

    fn messages(api: &FakeTg) -> Vec<String> {
        api.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == "sendMessage")
            .map(|(_, t)| t.clone())
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn completed_turn_sends_segments_in_order() {
        let api = Arc::new(FakeTg::default());
        let r = run_with(
            Arc::clone(&api),
            vec![delta("我先看看"), tool(), delta("结论是 "), delta("42"), complete("end_turn")],
        )
        .await;
        assert_eq!(r.outcome, Outcome::Completed);
        assert!(!r.delivery_failed);
        assert_eq!(messages(&api), vec!["我先看看", "结论是 42"]);
        // 第一次调用就是「思考中」状态,不是空草稿
        assert_eq!(api.calls.lock().unwrap()[0], ("sendMessageDraft".into(), RelayTexts::default().thinking));
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
    async fn tool_status_shows_while_running_then_back_to_thinking() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let turn = TelegramTurn {
            api: Arc::clone(&api) as Arc<dyn TgApi>,
            chat_id: 1,
            draft_base: 100,
            texts: RelayTexts::default(),
        };
        let handle = tokio::spawn(turn.run(rx));
        let drafts = |api: &FakeTg| {
            api.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| m == "sendMessageDraft")
                .map(|(_, t)| t.clone())
                .collect::<Vec<_>>()
        };
        tx.send(env(AcpEvent::ToolCall {
            tool_call_id: "t1".into(),
            title: "df -h".into(),
            kind: "execute".into(),
            status: "in_progress".into(),
            tool_name: Some("Bash".into()),
            description: Some("查看磁盘使用情况".into()),
            content: None,
            raw_input: None,
            raw_output: None,
            locations: None,
            meta: None,
            images: None,
        }))
        .unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(drafts(&api).last().unwrap(), "🔧 查看磁盘使用情况");
        tx.send(env(AcpEvent::ToolCallUpdate {
            tool_call_id: "t1".into(),
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
        }))
        .unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(drafts(&api).last().unwrap(), &RelayTexts::default().thinking);
        tx.send(complete("end_turn")).unwrap();
        handle.await.unwrap();
    }

    #[test]
    fn tool_status_prefers_description_and_clips() {
        assert_eq!(tool_status(Some("读取 a.txt"), "cat a.txt").unwrap(), "🔧 读取 a.txt");
        assert_eq!(tool_status(None, "cat a.txt").unwrap(), "🔧 cat a.txt");
        assert_eq!(tool_status(Some("  "), "").as_deref(), None);
        let long = "x".repeat(200);
        assert_eq!(tool_status(Some(&long), "").unwrap().chars().count(), 2 + STATUS_MAX + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn draft_body_is_formatted_like_the_final_message() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let turn = TelegramTurn {
            api: Arc::clone(&api) as Arc<dyn TgApi>,
            chat_id: 1,
            draft_base: 100,
            texts: RelayTexts::default(),
        };
        let handle = tokio::spawn(turn.run(rx));
        tx.send(delta("## 总结\n**内存**:充裕,**磁")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let last_draft = api
            .calls
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(m, _)| m == "sendMessageDraft")
            .map(|(_, t)| t.clone())
            .unwrap();
        assert_eq!(last_draft, "<b>总结</b>\n<b>内存</b>:充裕,**磁");
        tx.send(complete("end_turn")).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_html_draft_falls_back_to_plain() {
        let api = Arc::new(FakeTg {
            reject_html: true,
            ..Default::default()
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let turn = TelegramTurn {
            api: Arc::clone(&api) as Arc<dyn TgApi>,
            chat_id: 1,
            draft_base: 100,
            texts: RelayTexts::default(),
        };
        let handle = tokio::spawn(turn.run(rx));
        tx.send(delta("**粗**")).unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let drafts: Vec<String> = api
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == "sendMessageDraft")
            .map(|(_, t)| t.clone())
            .collect();
        assert_eq!(drafts[drafts.len() - 2..], ["<b>粗</b>".to_string(), "**粗**".to_string()]);
        tx.send(complete("end_turn")).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_delivery_is_reported() {
        let api = Arc::new(FakeTg {
            fail_messages: true,
            ..Default::default()
        });
        let r = run_with(Arc::clone(&api), vec![delta("hi"), complete("end_turn")]).await;
        assert!(r.delivery_failed);
    }

    #[tokio::test(start_paused = true)]
    async fn drafts_are_throttled_and_heartbeat_keeps_them_alive() {
        let api = Arc::new(FakeTg::default());
        let (tx, rx) = mpsc::unbounded_channel();
        let turn = TelegramTurn {
            api: Arc::clone(&api) as Arc<dyn TgApi>,
            chat_id: 1,
            draft_base: 100,
            texts: RelayTexts::default(),
        };
        let handle = tokio::spawn(turn.run(rx));
        // 连续来字:1 秒内只应再发一次草稿
        for t in ["a", "b", "c", "d"] {
            tx.send(delta(t)).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let drafts = |api: &FakeTg| {
            api.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| m == "sendMessageDraft")
                .map(|(_, t)| t.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(drafts(&api), vec![RelayTexts::default().thinking, "abcd".to_string()]);
        // 没有新字:5 秒后续一次同样的草稿(草稿 ~10 秒过期)
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(drafts(&api).last().unwrap(), "abcd");
        assert_eq!(drafts(&api).len(), 3);
        tx.send(complete("end_turn")).unwrap();
        let r = handle.await.unwrap();
        assert_eq!(r.outcome, Outcome::Completed);
        assert_eq!(messages(&api), vec!["abcd"]);
    }
}
