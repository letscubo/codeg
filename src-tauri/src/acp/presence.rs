//! fork(letscubo)专属:实例「最近一次有人用」的两个时刻,给 `GET /api/myclaw/activity` 读。
//!
//! ## 为什么要它
//!
//! 平台判忙闲原先只有两个即时量:`ws_clients`(有几个页面连着)和 `active_turns`(此刻有几轮
//! 在跑)。「页面连着」不等于「有人在用」—— 后台挂着的标签页、开着就走开了的电脑都会一直连着,
//! 实例就永远停不下来(2026-10-16~18 线上实测:近 48 小时的「忙」里 61% 只是页面开着)。
//! 主流云开发环境(Codespaces / Gitpod / Coder)都按「人有没有在操作」判,不看连接。
//!
//! 所以这里记两个时刻,平台按「距今多久」自己定规则(规则会变,不写进 codeg):
//!
//! - **用户最近一次在页面上操作** —— 前端在页面可见且有键盘 / 鼠标 / 触摸时,经 `/ws/events`
//!   发 `{"action":"user_activity"}`(见 `web/ws.rs`)
//! - **最近一轮结束** —— 任何一条连接从「正在跑一轮」变成别的状态(见
//!   `session_state.rs` 的 `apply_event`)。网页、渠道、定时任务、CLI 通道的轮都算
//!
//! ## 用墙钟、落盘(2026-10-19 改)
//!
//! 最初用进程内单调时钟、只存内存,还没记过的按「进程起点」算。两处都在线上算错了:
//!
//! - **快照挂起期间单调时钟不走**:C10 10-19 实测,06:26 冷启动后到 16:16 墙钟过了约 9 小时 50 分,
//!   uptime 只有 3.9 小时 —— 被挂起的约 6 小时不计。恢复后报的「距今」比实际短得多,看起来像刚有人用过
//! - **重启 = 刚有人用过**:A2 从 10-06 起一直暂停,10-19 升级 codeg 重启后报「18 分钟前有人操作」,
//!   平台按 30 分钟宽限判成忙,多开、多计费。每次升级、冷启动、崩溃重启都会这样
//!
//! 现在存 Unix 毫秒(墙钟),写进 `paths::codeg_presence_file()`,进程起来时读回。快照恢复后容器
//! 墙钟是对的(C10 10-19 无对时进程,与平台相差约 1 秒),挂起的时间照常计入。墙钟往回跳时距今按 0 算
//! (偏「刚用过」,方向是少停)。
//!
//! **从没记过** 报 [`NEVER_AGO_S`](约 10 年)而不是 null:平台把 null 当成老版 codeg、退回
//! 「页面连着就算忙」。重启后不会被立刻误停 —— 记录读得回来;平台刚叫醒的实例 10 分钟内不停;
//! 页面重连后会补报最近的操作。

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Once;

/// 从没记过时报的「距今秒数」—— 当成很久以前(约 10 年)。
pub const NEVER_AGO_S: u64 = 10 * 365 * 24 * 3600;

/// Unix 毫秒;0 = 从没记过。
static LAST_USER_ACTIVITY_MS: AtomicI64 = AtomicI64::new(0);
static LAST_TURN_ENDED_MS: AtomicI64 = AtomicI64::new(0);
static LOADED: Once = Once::new();

#[derive(serde::Serialize, serde::Deserialize, Default, Debug, PartialEq)]
struct Persisted {
    #[serde(default)]
    user_activity_ms: i64,
    #[serde(default)]
    turn_ended_ms: i64,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 第一次读写前把落盘的时刻读回来(测试里不碰磁盘)。
fn ensure_loaded() {
    LOADED.call_once(|| {
        #[cfg(not(test))]
        if let Some(p) = load(&crate::paths::codeg_presence_file()) {
            LAST_USER_ACTIVITY_MS.fetch_max(p.user_activity_ms, Ordering::Relaxed);
            LAST_TURN_ENDED_MS.fetch_max(p.turn_ended_ms, Ordering::Relaxed);
        }
    });
}

fn load(path: &Path) -> Option<Persisted> {
    let raw = std::fs::read_to_string(path).ok()?;
    match serde_json::from_str(&raw) {
        Ok(p) => Some(p),
        Err(e) => {
            tracing::warn!("[presence] {} unreadable, ignored: {e}", path.display());
            None
        }
    }
}

/// 先写临时文件再改名,读的人不会读到半截。写失败只打日志 —— 只是重启后少了这一条记录。
fn save(path: &Path, p: &Persisted) {
    let result = (|| -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec(p).unwrap_or_default())?;
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = result {
        tracing::warn!("[presence] could not persist to {}: {e}", path.display());
    }
}

fn persist() {
    #[cfg(not(test))]
    save(
        &crate::paths::codeg_presence_file(),
        &Persisted {
            user_activity_ms: LAST_USER_ACTIVITY_MS.load(Ordering::Relaxed),
            turn_ended_ms: LAST_TURN_ENDED_MS.load(Ordering::Relaxed),
        },
    );
}

fn note(slot: &AtomicI64) {
    ensure_loaded();
    slot.fetch_max(now_ms(), Ordering::Relaxed);
    persist();
}

/// 记一次「用户在页面上操作了」。前端每条连接每分钟最多报一次,这里每次都落盘也不多。
pub fn note_user_activity() {
    note(&LAST_USER_ACTIVITY_MS);
}

/// 记一次「有一轮结束了」。
pub fn note_turn_ended() {
    note(&LAST_TURN_ENDED_MS);
}

/// 距 `last_ms` 过了几秒:从没记过 = [`NEVER_AGO_S`];墙钟往回跳(last 在未来)= 0。
fn ago_s_at(now_ms: i64, last_ms: i64) -> u64 {
    if last_ms <= 0 {
        return NEVER_AGO_S;
    }
    (now_ms.saturating_sub(last_ms).max(0) / 1000) as u64
}

/// 距上次用户操作过了几秒(从没有过 = [`NEVER_AGO_S`])。
pub fn user_active_ago_s() -> u64 {
    ensure_loaded();
    ago_s_at(now_ms(), LAST_USER_ACTIVITY_MS.load(Ordering::Relaxed))
}

/// 距上一轮结束过了几秒(从没有过 = [`NEVER_AGO_S`])。
pub fn turn_ended_ago_s() -> u64 {
    ensure_loaded();
    ago_s_at(now_ms(), LAST_TURN_ENDED_MS.load(Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_noted_reports_a_very_long_time_not_process_uptime() {
        assert_eq!(ago_s_at(1_000_000, 0), NEVER_AGO_S);
        assert!(NEVER_AGO_S > 365 * 24 * 3600, "要远大于平台任何宽限");
    }

    #[test]
    fn ago_is_wall_clock_seconds_and_never_negative() {
        assert_eq!(ago_s_at(1_000_000 + 90_500, 1_000_000), 90);
        // 墙钟往回跳:last 在未来 → 0(偏「刚用过」,少停)
        assert_eq!(ago_s_at(1_000_000, 1_005_000), 0);
    }

    #[test]
    fn noting_resets_the_clock_and_an_earlier_time_cannot_win() {
        note_user_activity();
        assert_eq!(user_active_ago_s(), 0);
        note_turn_ended();
        assert_eq!(turn_ended_ago_s(), 0);
        // fetch_max:更早的时刻写不回去
        LAST_USER_ACTIVITY_MS.fetch_max(1, Ordering::Relaxed);
        assert_eq!(user_active_ago_s(), 0);
    }

    #[test]
    fn persisted_times_round_trip_and_bad_files_are_ignored() {
        let dir = std::env::temp_dir().join(format!("codeg-presence-{}", std::process::id()));
        let path = dir.join("myclaw-presence.json");
        let p = Persisted { user_activity_ms: 1_760_000_000_000, turn_ended_ms: 1_760_000_100_000 };
        save(&path, &p);
        assert_eq!(load(&path), Some(p));
        // 旧文件少字段:缺的按 0(从没记过)
        std::fs::write(&path, r#"{"user_activity_ms":5}"#).unwrap();
        assert_eq!(load(&path), Some(Persisted { user_activity_ms: 5, turn_ended_ms: 0 }));
        std::fs::write(&path, "not json").unwrap();
        assert_eq!(load(&path), None);
        assert_eq!(load(&dir.join("missing.json")), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
