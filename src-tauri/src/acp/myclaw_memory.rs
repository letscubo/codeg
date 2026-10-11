//! fork(letscubo)专属: 统一记忆注入 —— 每条对话送达运行时前,在最前面加上平台给的 `<myclaw_memory>` 块。
//!
//! ## 为什么在这里
//!
//! 网页、Telegram / Slack 渠道、定时任务、agent 互相派活,发给运行时的每一轮最后都汇到
//! `ConnectionManager::send_prompt_inner`。在这一处注入,六个运行时、所有入口一次覆盖,
//! 不碰任何运行时自己的文件。记忆本身在平台(`agent_memories`),这里只取快照、缓存、插块。
//!
//! ## 取哪个 agent 的记忆
//!
//! 连接建立时(`spawn_agent`)从平台下发的 runtime env 里认出 agent:各运行时的 per-agent
//! 目录都在 `/home/ubuntu/.myclaw/agents/<vm_agent_id>/…`(HERMES_HOME、CLAUDE_CONFIG_DIR、
//! PI_CODING_AGENT_DIR、CODEX_HOME、DSH_HOME),openclaw 的 `OPENCLAW_SESSION_KEY` 里带
//! `va-<vm_agent_id>`。取快照时把它和 conversation id 一并交给平台,平台据此定 agent、
//! 会话与群上下文。
//!
//! ## 什么时候注入
//!
//! 每条连接第一轮注入一次;之后只有平台那边的版本变了(记忆被改过)才再注入。平台改完记忆
//! 调 `/api/myclaw/memory/invalidate`,这里把缓存标脏,下一轮重新取。
//!
//! ## 绝不卡对话
//!
//! 取快照超时 4 秒(只发生在每个会话的第一轮 / 记忆改过之后;平台那边要连查几次库,实测约 1.3–1.6 秒);失败(平台不可达、不是平台托管、老平台没有这条接口)就跳过本次注入,
//! 只记日志。失败结果缓存 30 秒,别每一轮都去撞。
//!
//! 只改发给运行时的 `blocks`,不动回显给界面的 user message —— 实时界面看不到注入块;
//! 历史里的那份由平台同步时去掉(`stripMemoryBlock`)。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::Mutex;

use crate::acp::types::PromptInputBlock;
use crate::db::AppDatabase;

pub const SNAPSHOT_PATH: &str = "/api/codeg/memory/snapshot";
const FETCH_TIMEOUT: Duration = Duration::from_secs(4);
/// 成功的快照最长留多久(兜底:平台的失效通知没送到时,最晚这么久后也会重新取)。
const TTL_OK: Duration = Duration::from_secs(600);
/// 取不到时多久内不再试。
const TTL_ERR: Duration = Duration::from_secs(30);

const AGENTS_ROOT_MARK: &str = "/.myclaw/agents/";
const AGENT_HOME_ENV_KEYS: [&str; 5] = [
    "HERMES_HOME",
    "CLAUDE_CONFIG_DIR",
    "PI_CODING_AGENT_DIR",
    "CODEX_HOME",
    "DSH_HOME",
];

fn is_uuid(s: &str) -> bool {
    s.len() == 36
        && s.chars().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// 从平台下发的 runtime env 认出这条连接属于哪个 vm agent。认不出 = None(只注入「关于你」)。
pub fn agent_key_from_env(env: &BTreeMap<String, String>) -> Option<String> {
    if let Some(id) = env.get("MYCLAW_VM_AGENT_ID").map(|s| s.trim()) {
        if is_uuid(id) {
            return Some(id.to_ascii_lowercase());
        }
    }
    for key in AGENT_HOME_ENV_KEYS {
        if let Some(v) = env.get(key) {
            if let Some(pos) = v.find(AGENTS_ROOT_MARK) {
                let rest = &v[pos + AGENTS_ROOT_MARK.len()..];
                let id = rest.get(..36).unwrap_or("");
                if is_uuid(id) {
                    return Some(id.to_ascii_lowercase());
                }
            }
        }
    }
    if let Some(v) = env.get("OPENCLAW_SESSION_KEY") {
        let mut search = v.as_str();
        while let Some(pos) = search.find("va-") {
            let id = search[pos + 3..].get(..36).unwrap_or("");
            if is_uuid(id) {
                return Some(id.to_ascii_lowercase());
            }
            search = &search[pos + 3..];
        }
    }
    None
}

/// 平台给的快照。`text` 为空 = 没有记忆,不插块。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySnapshot {
    pub version: String,
    pub text: String,
}

struct Cached {
    generation: u64,
    at: Instant,
    snapshot: Option<MemorySnapshot>,
}

static GENERATION: AtomicU64 = AtomicU64::new(0);

fn cache() -> &'static Mutex<HashMap<String, Cached>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Cached>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn db_cell() -> &'static OnceLock<AppDatabase> {
    static DB: OnceLock<AppDatabase> = OnceLock::new();
    &DB
}

/// 服务端启动时交给这里一份库句柄(读平台 webhook 配置要用)。没装 = 不注入(桌面版即如此)。
pub fn install_db(db: AppDatabase) {
    if db_cell().set(db).is_ok() {
        tracing::info!("[MyclawMemory] memory injection enabled");
    }
}

/// 平台改了记忆:所有缓存作废,每条连接下一轮重新取、版本变了就重新注入。
pub async fn invalidate_all() {
    GENERATION.fetch_add(1, Ordering::SeqCst);
    cache().lock().await.clear();
}

fn cache_key(conversation_id: Option<i32>, agent_key: Option<&str>) -> String {
    format!(
        "{}|{}",
        conversation_id.map(|c| c.to_string()).unwrap_or_default(),
        agent_key.unwrap_or("")
    )
}

/// 取这条连接此刻应带的记忆。走缓存;取不到返回 None(调用方跳过注入)。
pub async fn snapshot_for(
    conversation_id: Option<i32>,
    agent_key: Option<&str>,
) -> Option<MemorySnapshot> {
    if conversation_id.is_none() && agent_key.is_none() {
        return None;
    }
    let Some(db) = db_cell().get() else {
        tracing::debug!("[MyclawMemory] no database handle installed; memory injection is off");
        return None;
    };
    let key = cache_key(conversation_id, agent_key);
    let generation = GENERATION.load(Ordering::SeqCst);
    {
        let guard = cache().lock().await;
        if let Some(c) = guard.get(&key) {
            let ttl = if c.snapshot.is_some() {
                TTL_OK
            } else {
                TTL_ERR
            };
            if c.generation == generation && c.at.elapsed() < ttl {
                return c.snapshot.clone();
            }
        }
    }
    let identity = json!({ "conversationId": conversation_id, "agentKey": agent_key });
    let fetched = match crate::commands::myclaw_platform::post_signed(
        db,
        SNAPSHOT_PATH,
        json!({ "identity": identity }),
        FETCH_TIMEOUT,
    )
    .await
    {
        Ok(data) => {
            let version = data
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let text = data
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Some(MemorySnapshot { version, text })
        }
        Err(e) => {
            tracing::info!(
                "[MyclawMemory] snapshot unavailable, skipping injection this turn: {e}"
            );
            None
        }
    };
    cache().lock().await.insert(
        key,
        Cached {
            generation,
            at: Instant::now(),
            snapshot: fetched.clone(),
        },
    );
    fetched
}

/// 要不要注入、注入后连接上该记的版本。纯函数,供 manager 与测试共用。
///
/// - 快照为空串:不插块,但记下版本(之后记忆从无到有时版本会变,再注入);
/// - 已注入过同一版本:不重复插;
/// - 其余:插在最前面。
pub fn plan_injection(
    injected_version: Option<&str>,
    snapshot: &MemorySnapshot,
) -> (Option<PromptInputBlock>, String) {
    if injected_version == Some(snapshot.version.as_str()) || snapshot.text.trim().is_empty() {
        return (None, snapshot.version.clone());
    }
    (
        Some(PromptInputBlock::Text {
            text: format!("{}\n\n", snapshot.text.trim_end()),
        }),
        snapshot.version.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "6ca6b0b8-cf83-46dc-9e6f-004d20837493";

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn agent_key_from_per_agent_home_dirs() {
        for key in AGENT_HOME_ENV_KEYS {
            let home = format!("/home/ubuntu/.myclaw/agents/{ID}/x-home");
            assert_eq!(
                agent_key_from_env(&env(&[(key, &home)])).as_deref(),
                Some(ID),
                "{key}"
            );
        }
    }

    #[test]
    fn agent_key_from_openclaw_session_key_and_explicit_var() {
        let sk = format!("agent:va-{ID}:codeg:12");
        assert_eq!(
            agent_key_from_env(&env(&[("OPENCLAW_SESSION_KEY", &sk)])).as_deref(),
            Some(ID)
        );
        assert_eq!(
            agent_key_from_env(&env(&[("MYCLAW_VM_AGENT_ID", ID)])).as_deref(),
            Some(ID)
        );
    }

    #[test]
    fn agent_key_absent_or_malformed() {
        assert_eq!(agent_key_from_env(&env(&[])), None);
        assert_eq!(
            agent_key_from_env(&env(&[("HERMES_HOME", "/home/ubuntu/.hermes")])),
            None
        );
        assert_eq!(
            agent_key_from_env(&env(&[(
                "CODEX_HOME",
                "/home/ubuntu/.myclaw/agents/not-a-uuid/c"
            )])),
            None
        );
        assert_eq!(
            agent_key_from_env(&env(&[("OPENCLAW_SESSION_KEY", "agent:main:codeg:1")])),
            None
        );
    }

    fn snap(version: &str, text: &str) -> MemorySnapshot {
        MemorySnapshot {
            version: version.into(),
            text: text.into(),
        }
    }

    #[test]
    fn injects_first_time_and_on_version_change_only() {
        let s = snap(
            "v1",
            "<myclaw_memory version=\"v1\">\n- x\n</myclaw_memory>",
        );
        let (block, v) = plan_injection(None, &s);
        assert!(
            matches!(block, Some(PromptInputBlock::Text { ref text }) if text.starts_with("<myclaw_memory") && text.ends_with("\n\n"))
        );
        assert_eq!(v, "v1");
        assert!(plan_injection(Some("v1"), &s).0.is_none());
        assert!(plan_injection(Some("v0"), &s).0.is_some());
    }

    #[test]
    fn empty_snapshot_inserts_nothing_but_records_version() {
        let (block, v) = plan_injection(None, &snap("v2", ""));
        assert!(block.is_none());
        assert_eq!(v, "v2");
    }

    #[tokio::test]
    async fn invalidate_bumps_generation_and_clears_cache() {
        cache().lock().await.insert(
            "k".into(),
            Cached {
                generation: GENERATION.load(Ordering::SeqCst),
                at: Instant::now(),
                snapshot: None,
            },
        );
        let before = GENERATION.load(Ordering::SeqCst);
        invalidate_all().await;
        assert!(GENERATION.load(Ordering::SeqCst) > before);
        assert!(cache().lock().await.is_empty());
    }
}
