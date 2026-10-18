//! fork(letscubo)专属:实例「最近一次有人用」的两个时钟,给 `GET /api/myclaw/activity` 读。
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
//!   发 `{"action":"user_activity"}`(见 `web/ws_attach.rs` 的 `ClientMsg::UserActivity`)
//! - **最近一轮结束** —— 任何一条连接从「正在跑一轮」变成别的状态(见
//!   `session_state.rs` 的 `apply_event`)。网页、渠道、定时任务、CLI 通道的轮都算
//!
//! ## 为什么报「过了几秒」而不是时刻
//!
//! EKS 实例从内存快照恢复后容器时钟会漂(2026-10-15 a358a274 实测启动时刻 14:56:51 漂成
//! 14:57:06),拿容器时刻和平台时刻直接比会算错。这里只用进程内单调时钟算间隔。
//!
//! ⚠️ 未验证:快照挂起期间单调时钟是否前进。不前进的话,恢复后算出的间隔不含挂起的那段 ——
//! 只会把「距今」算短(更像刚用过),方向是少停,不会误停。
//!
//! ## 进程重启
//!
//! 两个时钟只在内存里。还没记过的时钟按「进程起点」算:刚重启的 codeg 报的是「距启动多久」,
//! 而不是一个很大的数 —— 重启不等于闲了很久,否则一重启就会被立刻判闲停掉。进程起点取第一次
//! 读写本模块的时刻(`STARTED` 懒初始化),最多比真实启动晚一次采集间隔,方向同样是少停。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

static STARTED: OnceLock<Instant> = OnceLock::new();
/// 距 `STARTED` 的毫秒数;0 = 还没记过(按进程起点算)。
static LAST_USER_ACTIVITY_MS: AtomicU64 = AtomicU64::new(0);
static LAST_TURN_ENDED_MS: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    STARTED.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// 记一次「用户在页面上操作了」。只存最新时刻,重复调用无副作用 —— 不需要限流。
pub fn note_user_activity() {
    LAST_USER_ACTIVITY_MS.fetch_max(now_ms(), Ordering::Relaxed);
}

/// 记一次「有一轮结束了」。
pub fn note_turn_ended() {
    LAST_TURN_ENDED_MS.fetch_max(now_ms(), Ordering::Relaxed);
}

fn ago_s(slot: &AtomicU64) -> u64 {
    now_ms().saturating_sub(slot.load(Ordering::Relaxed)) / 1000
}

/// 距上次用户操作过了几秒(还没有过 = 距进程起点)。
pub fn user_active_ago_s() -> u64 {
    ago_s(&LAST_USER_ACTIVITY_MS)
}

/// 距上一轮结束过了几秒(还没有过 = 距进程起点)。
pub fn turn_ended_ago_s() -> u64 {
    ago_s(&LAST_TURN_ENDED_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noting_resets_the_clock_and_ago_never_goes_negative() {
        note_user_activity();
        assert_eq!(user_active_ago_s(), 0);
        note_turn_ended();
        assert_eq!(turn_ended_ago_s(), 0);
        // fetch_max:更早的时刻写不回去
        LAST_USER_ACTIVITY_MS.fetch_max(0, Ordering::Relaxed);
        assert_eq!(user_active_ago_s(), 0);
    }
}
