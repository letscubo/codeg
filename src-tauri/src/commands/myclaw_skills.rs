//! MyClaw 平台下发的技能 —— 定时**主动拉取**同步到中心库并链接到各 agent。
//!
//! ## 与 experts/science 的关系
//!
//! 那两个包是 `include_dir!` 编译期嵌进二进制的只读内容,内容与二进制版本绑死。
//! 本模块是它们的替代:同样落在中心库 `~/.codeg/skills/<slug>/`,同样复用
//! experts 的链接引擎,唯一的区别是**内容来自 HTTP 而不是二进制**,因此平台改一
//! 句文案就能全网生效,不必发版。
//!
//! ## 拉取方向
//!
//! 容器主动请求平台,平台不知道也不需要知道哪台实例在线。拉失败就保持现状、下一
//! 轮再来。这与 MyClaw 侧「容器拥有自己的状态,平台只提供数据源」的原则一致。
//!
//! ## 三向哈希(照搬 experts 的语义)
//!
//! 每个 slug 同时看三个值:
//!   · `remote.hash`      平台说现在应该是什么
//!   · `manifest.hash`    我们上次装进去的是什么
//!   · `on_disk_hash`     磁盘上现在是什么
//!
//!   remote == on_disk                → 跳过
//!   on_disk == manifest(未被改过)    → 直接覆盖成 remote
//!   on_disk != manifest(用户改过)    → **先备份**成 `<slug>.user-backup-<时间>`
//!                                       再写入,并标记 pending_user_review
//!
//! ## 为什么必须有独立 manifest
//!
//! 中心库里同时住着**用户在 MyClaw 面板自建的技能**。删除判据若写成「中心库里有
//! 但平台清单里没有」,第一次同步就会把用户自建的全删光。所以删除只看
//! `.myclaw-manifest.json` —— 只有本模块装过的 slug 才会被本模块删。用户自建的
//! 从不进这份 manifest,因此永远安全。
//!
//! ## 失败语义(重要)
//!
//! 拉取失败(网络、5xx、解析错)一律**保持现状直接返回**。空清单只有在平台明确
//! 回 200 且 `skills: []` 时才被当作「平台把技能都删了」。把网络抖动解释成删除
//! 指令,会让全网实例在一次平台故障中把技能卸干净。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::commands::experts::{central_experts_dir, create_link_raw, path_is_symlink};
use crate::chat_channel::webhook::WebhookConfig;
use crate::db::AppDatabase;
use crate::models::agent::{AgentType, BUILTIN_AGENT_TYPES};
use crate::commands::acp::preferred_scope_skill_dir;
use crate::acp::types::AgentSkillScope;

/// 本模块自己的 manifest —— 与 experts 的 `.manifest.json` 分开,互不干扰。
const MANIFEST_FILE: &str = ".myclaw-manifest.json";

/// 同步周期:10~15 分钟之间随机取,避免全网实例对齐到同一秒形成尖峰。
const SYNC_MIN_SECS: u64 = 600;
const SYNC_MAX_SECS: u64 = 900;

/// 平台地址与凭证由 agent 在 CREATE 时写进 `~/.myclaw/codeg.env`,
/// codeg 启动时 source 它 —— 与 webhook 地址同一次写入、同一个 origin。
/// 平台地址的**唯一来源**是 codeg 自己已配置的出站 webhook —— 开通流程配的那条
/// `{origin}/api/codeg/events?vmId=…&s=…`,origin / vmId / secret 三样都在里面。
///
/// 刻意不再引入 MYCLAW_SYNC_URL / MYCLAW_SYNC_SECRET 这类专用 env:那等于给一份
/// 已经存在的配置再造一份副本,两份还会各自漂移;而且存量实例的 webhook 早就配好,
/// 走这条路它们**升级后立刻可用**,不必逐台补 env。
///
/// 代价:同步能力与 webhook 配置绑定 —— webhook 被清空或改地址,同步跟着失效或
/// 跟着走。这是「统一逻辑」的必然结果,已接受。
const EVENTS_PATH: &str = "/api/codeg/events";
const SKILLS_PATH: &str = "/api/codeg/skills";

/// 同一时刻只允许一个同步在跑(启动那次与定时那次可能撞上)。
fn sync_lock() -> &'static Mutex<()> {
    static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Deserialize)]
struct RemoteSkill {
    slug: String,
    hash: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct RemoteEnvelope {
    code: i64,
    #[serde(default)]
    data: Option<RemoteData>,
    #[serde(default)]
    msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RemoteData {
    #[serde(default)]
    skills: Vec<RemoteSkill>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct MyclawManifest {
    #[serde(default)]
    skills: BTreeMap<String, ManifestEntry>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ManifestEntry {
    /// 我们上次写进磁盘的内容哈希 —— 用来区分「平台更新了」和「用户改过了」。
    hash: String,
    installed_at: String,
    #[serde(default)]
    pending_user_review: bool,
}

#[derive(Debug, Default)]
pub struct SyncReport {
    pub installed: usize,
    pub updated: usize,
    pub removed: usize,
    pub backed_up: Vec<String>,
    pub errors: Vec<String>,
}

fn manifest_path() -> PathBuf {
    central_experts_dir().join(MANIFEST_FILE)
}

fn skill_dir(slug: &str) -> PathBuf {
    central_experts_dir().join(slug)
}

fn load_manifest() -> MyclawManifest {
    fs::read_to_string(manifest_path())
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_manifest(m: &MyclawManifest) -> std::io::Result<()> {
    let path = manifest_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, serde_json::to_string_pretty(m).unwrap_or_else(|_| "{}".into()))
}

fn hash_str(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    format!("{:x}", h.finalize())
}

/// 磁盘上那份 SKILL.md 的哈希;文件不存在返回 None。
fn on_disk_hash(slug: &str) -> Option<String> {
    fs::read_to_string(skill_dir(slug).join("SKILL.md")).ok().map(|s| hash_str(&s))
}

/// slug 同时是目录名 —— 必须挡住路径穿越,内容来自网络。
fn slug_ok(slug: &str) -> bool {
    !slug.is_empty()
        && slug.len() <= 63
        && slug
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !slug.starts_with('-')
        && !slug.contains("..")
}

/// 写一个技能到中心库。用户改过的话先备份,不静默覆盖。
fn write_skill(slug: &str, content: &str, entry: Option<&ManifestEntry>) -> std::io::Result<bool> {
    let dir = skill_dir(slug);
    let mut backed_up = false;

    if dir.exists() {
        let disk = on_disk_hash(slug).unwrap_or_default();
        let known = entry.map(|e| e.hash.clone()).unwrap_or_default();
        // known 为空 = 这个目录不是我们装的(用户自建且撞名),同样按「用户的东西」对待
        if known.is_empty() || disk != known {
            let backup = central_experts_dir()
                .join(format!("{slug}.user-backup-{}", Utc::now().format("%Y%m%d-%H%M%S")));
            fs::rename(&dir, &backup)?;
            backed_up = true;
        }
    }

    fs::create_dir_all(&dir)?;
    fs::write(dir.join("SKILL.md"), content)?;
    Ok(backed_up)
}

/// 删除真身与它在所有 agent 下的链接。
fn remove_skill(slug: &str) -> std::io::Result<()> {
    for agent in BUILTIN_AGENT_TYPES.iter().copied() {
        if let Some(link) = agent_link(agent, slug) {
            if path_is_symlink(&link) || link.exists() {
                let _ = fs::remove_dir_all(&link).or_else(|_| fs::remove_file(&link));
            }
        }
    }
    let dir = skill_dir(slug);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    Ok(())
}

fn agent_link(agent: AgentType, slug: &str) -> Option<PathBuf> {
    preferred_scope_skill_dir(agent, AgentSkillScope::Global, None)
        .ok()
        .map(|d| d.join(slug))
}

/// 把 slug 链接到所有**目录能解析出来**的 agent。
///
/// 不去判断"这个 agent 装没装" —— 解析不出目录的 agent 自然会被 `agent_link`
/// 过滤掉;而目录能解析、agent 还没装的情况下先建好链接也无害,agent 装上就能看见。
fn link_everywhere(slug: &str, report: &mut SyncReport) {
    let truth = skill_dir(slug);
    for agent in BUILTIN_AGENT_TYPES.iter().copied() {
        let Some(link) = agent_link(agent, slug) else { continue };
        if path_is_symlink(&link) {
            continue; // 已是链接:真身路径没变,不必重建
        }
        if link.exists() {
            // 那里是个真实目录(旧版直写的产物或用户手放的)—— 不碰,记一笔
            report
                .errors
                .push(format!("{slug}: {} is a real directory, left as-is", link.display()));
            continue;
        }
        if let Some(parent) = link.parent() {
            if fs::create_dir_all(parent).is_err() {
                continue;
            }
        }
        if let Err(e) = create_link_raw(&truth, &link) {
            report.errors.push(format!("{slug}: link {} failed: {e}", link.display()));
        }
    }
}

/// 从已配置的 webhook 列表里推导出技能清单端点。没有匹配的 webhook = 这台实例
/// 不参与平台下发(返回 None)。
///
/// 只换 path、**query 原样保留** —— `vmId` 和 `s` 本来就在里面,逐个解析再重拼
/// 只会多出几处可能写错的地方。
///
/// 按 path 精确匹配而不是取列表第一条:用户可能自己加了别的 webhook,取第一条
/// 会把技能清单请求发到不相干的地址去。
fn skills_endpoint(hooks: &[WebhookConfig]) -> Option<String> {
    hooks
        .iter()
        .filter(|w| w.enabled)
        .find(|w| w.url.contains(EVENTS_PATH))
        .map(|w| w.url.replacen(EVENTS_PATH, SKILLS_PATH, 1))
}

/// 读回本机已配置的 webhook 并推导端点。db 出错时返回 None(保持现状,不动磁盘)。
async fn sync_config(db: &AppDatabase) -> Option<String> {
    let hooks = crate::commands::chat_channel::get_chat_event_webhooks_core(db)
        .await
        .ok()?;
    skills_endpoint(&hooks)
}

/// 拉一次并同步。**任何拉取失败都保持现状**,只有平台明确返回 200 才会动磁盘。
pub async fn sync_once(db: &AppDatabase) -> SyncReport {
    let _guard = sync_lock().lock().await;
    let mut report = SyncReport::default();

    let Some(endpoint) = sync_config(db).await else {
        return report; // 没有可用 webhook = 这台实例不参与平台下发,静默跳过
    };

    // endpoint 由 webhook URL 换 path 得来,`vmId` 与 `s` 原样带着,这里不再拼接
    // 任何 query —— 少一处拼接就少一处能拼错的地方。
    let resp = match reqwest::Client::new()
        .get(&endpoint)
        .timeout(Duration::from_secs(30))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            report.errors.push(format!("fetch failed: {e}"));
            return report; // 保持现状
        }
    };

    if !resp.status().is_success() {
        report.errors.push(format!("fetch HTTP {}", resp.status()));
        return report; // 保持现状 —— 5xx 不是「平台删光了技能」
    }

    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            report.errors.push(format!("read body failed: {e}"));
            return report;
        }
    };
    let env: RemoteEnvelope = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            report.errors.push(format!("parse failed: {e}"));
            return report;
        }
    };
    if env.code != 0 {
        report
            .errors
            .push(format!("platform error: {}", env.msg.unwrap_or_default()));
        return report;
    }
    let remote = env.data.map(|d| d.skills).unwrap_or_default();

    let mut manifest = load_manifest();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for r in &remote {
        if !slug_ok(&r.slug) {
            report.errors.push(format!("{}: rejected slug", r.slug));
            continue;
        }
        // 内容与平台声明的哈希对不上 = 传输被截断或平台算错,不写
        if hash_str(&r.content) != r.hash {
            report.errors.push(format!("{}: content/hash mismatch", r.slug));
            continue;
        }
        seen.insert(r.slug.clone());

        let entry = manifest.skills.get(&r.slug).cloned();
        let disk = on_disk_hash(&r.slug);
        if disk.as_deref() == Some(r.hash.as_str()) {
            // 磁盘已经是目标内容 —— 补齐 manifest 后跳过(可能是上次写完崩在记账前)
            if entry.as_ref().map(|e| e.hash.as_str()) != Some(r.hash.as_str()) {
                manifest.skills.insert(
                    r.slug.clone(),
                    ManifestEntry { hash: r.hash.clone(), installed_at: Utc::now().to_rfc3339(), pending_user_review: false },
                );
            }
            link_everywhere(&r.slug, &mut report);
            continue;
        }

        let existed = disk.is_some();
        match write_skill(&r.slug, &r.content, entry.as_ref()) {
            Ok(backed_up) => {
                if backed_up {
                    report.backed_up.push(r.slug.clone());
                }
                if existed {
                    report.updated += 1;
                } else {
                    report.installed += 1;
                }
                manifest.skills.insert(
                    r.slug.clone(),
                    ManifestEntry {
                        hash: r.hash.clone(),
                        installed_at: Utc::now().to_rfc3339(),
                        pending_user_review: backed_up,
                    },
                );
                link_everywhere(&r.slug, &mut report);
            }
            Err(e) => report.errors.push(format!("{}: write failed: {e}", r.slug)),
        }
    }

    // 删除:**只看 manifest** —— 用户自建的技能从不在这里,因此永远不会被删。
    let stale: Vec<String> = manifest.skills.keys().filter(|k| !seen.contains(*k)).cloned().collect();
    for slug in stale {
        match remove_skill(&slug) {
            Ok(()) => {
                manifest.skills.remove(&slug);
                report.removed += 1;
            }
            Err(e) => report.errors.push(format!("{slug}: remove failed: {e}")),
        }
    }

    if let Err(e) = save_manifest(&manifest) {
        report.errors.push(format!("manifest save failed: {e}"));
    }
    report
}

/// 启动时跑一次,之后每 10~15 分钟一次(随机,避免全网对齐成尖峰)。
pub fn spawn_sync_loop(db: AppDatabase) {
    tokio::spawn(async move {
        // 配置检查放进循环体而不是启动时一次性判断:webhook 是开通流程写的,
        // 容器可能先于它启动;一次性判断会让这台实例直到下次重启都不参与下发。
        if sync_config(&db).await.is_none() {
            tracing::info!(
                "[MyclawSkills] no enabled webhook pointing at {EVENTS_PATH} — platform skill sync idle until one is configured"
            );
        }
        loop {
            let r = sync_once(&db).await;
            if r.installed + r.updated + r.removed > 0 || !r.errors.is_empty() {
                tracing::info!(
                    "[MyclawSkills] sync: installed={} updated={} removed={} backed_up={:?} errors={:?}",
                    r.installed, r.updated, r.removed, r.backed_up, r.errors
                );
            }
            let secs = {
                use rand::Rng;
                rand::thread_rng().gen_range(SYNC_MIN_SECS..=SYNC_MAX_SECS)
            };
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
    });
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn hook(url: &str, enabled: bool) -> WebhookConfig {
        WebhookConfig { url: url.into(), enabled }
    }

    #[test]
    fn derives_skills_endpoint_keeping_query() {
        // vmId 与 s 必须原样带过来 —— 平台就靠这两个参数认实例和鉴权
        let hooks = vec![hook(
            "https://myclaw.ai/api/codeg/events?vmId=abc-123&s=sekret",
            true,
        )];
        assert_eq!(
            skills_endpoint(&hooks).as_deref(),
            Some("https://myclaw.ai/api/codeg/skills?vmId=abc-123&s=sekret")
        );
    }

    #[test]
    fn ignores_disabled_hooks() {
        let hooks = vec![hook("https://myclaw.ai/api/codeg/events?vmId=a&s=b", false)];
        assert!(skills_endpoint(&hooks).is_none());
    }

    #[test]
    fn picks_by_path_not_by_position() {
        // 用户自己加的 webhook 排在前面时,不能把技能清单请求发到他那儿去
        let hooks = vec![
            hook("https://hooks.example.com/whatever", true),
            hook("https://myclaw.ai/api/codeg/events?vmId=a&s=b", true),
        ];
        assert_eq!(
            skills_endpoint(&hooks).as_deref(),
            Some("https://myclaw.ai/api/codeg/skills?vmId=a&s=b")
        );
    }

    #[test]
    fn no_matching_hook_means_not_participating() {
        let hooks = vec![hook("https://hooks.example.com/whatever", true)];
        assert!(skills_endpoint(&hooks).is_none());
        assert!(skills_endpoint(&[]).is_none());
    }

    #[test]
    fn replaces_only_the_first_occurrence() {
        // 目录名里再出现一次同样的字串时,只换 path 那一处
        let hooks = vec![hook(
            "https://myclaw.ai/api/codeg/events?redirect=/api/codeg/events",
            true,
        )];
        assert_eq!(
            skills_endpoint(&hooks).as_deref(),
            Some("https://myclaw.ai/api/codeg/skills?redirect=/api/codeg/events")
        );
    }
}
