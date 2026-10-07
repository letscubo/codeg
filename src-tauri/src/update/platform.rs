//! fork(letscubo):按平台下发的目标版本闲时自动升级(设计文档「闲时自动升级 codeg」)。
//!
//! ## 分工
//!
//! 平台决定**升到哪个版本**(`GET /api/codeg/target-version`:testing 只给测试名单,
//! stable 给所有实例,单台可钉版本;目标比当前低就是回退)。codeg 决定**什么时候换**:
//!
//! 1. 每小时问一次目标版本(启动后先等几分钟,别和开机时的其他事抢)。
//! 2. 目标与当前不同 → 走与「手动升级」同一条下载 / 验签 / 替换路径,只是按版本号下载
//!    固定 tag(`ReleaseSource::Pinned`)。替换完文件就已在磁盘上 —— **此后任何一次
//!    重启都会直接起新版**:ovh 实例闲置暂停再恢复、宿主机重启、运维重启都算,不用等。
//! 3. 同时本地等一个真正没人用的时刻再主动重启:没有 WS 连接、没有正在跑的一轮、接下来
//!    15 分钟没有定时任务,并且这种状态连续 10 分钟。比平台每分钟采一次准,没有采样空档。
//!
//! 新版没撑过试运行就自动退回旧版并记下失败版本(`install::reexec_boot_guard`);同一个
//! 目标版本之后不再自动装,直到平台换了目标。
//!
//! 只有平台托管的实例(有指向平台的事件 webhook)才会跑起来;`CODEG_PLATFORM_AUTO_UPDATE=0`
//! 关掉。手动升级(面板按钮)照旧可用;但平台托管时目标版本以平台为准,手动装的其他版本
//! 会在下一次检查时被换回平台的目标版本。

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::app_state::AppState;
use crate::update::install::{self, ReleaseSource};
use crate::update::state::{self as update_state, AppUpdateLifecycle};
use crate::update::version::trim_v_prefix;

pub const ENV_DISABLE: &str = "CODEG_PLATFORM_AUTO_UPDATE";

const FIRST_CHECK_DELAY: Duration = Duration::from_secs(3 * 60);
const CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const IDLE_POLL: Duration = Duration::from_secs(60);
/// 连续闲这么久才主动重启。
const IDLE_REQUIRED: Duration = Duration::from_secs(10 * 60);
/// 接下来这么久内有定时任务要跑,就不算闲。
const AUTOMATION_LOOKAHEAD_MINUTES: i64 = 15;
/// 等下载 / 替换完成的上限。
const STAGE_WAIT_MAX: Duration = Duration::from_secs(20 * 60);
/// 等闲时的上限;过了就交给下一轮检查(文件已替换,自然重启也会起新版)。
const IDLE_WAIT_MAX: Duration = Duration::from_secs(24 * 60 * 60);

fn disabled() -> bool {
    std::env::var(ENV_DISABLE)
        .map(|v| v.trim() == "0" || v.trim().eq_ignore_ascii_case("false"))
        .unwrap_or(false)
}

/// 在服务启动后调用一次。桌面版、不支持自更新的平台、显式关闭时什么都不做。
pub fn spawn(state: Arc<AppState>) {
    if disabled() {
        tracing::info!("[update/platform] disabled by {ENV_DISABLE}");
        return;
    }
    if install::asset_basename().is_none() || cfg!(target_os = "windows") {
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(FIRST_CHECK_DELAY).await;
        loop {
            tick(&state).await;
            tokio::time::sleep(CHECK_INTERVAL).await;
        }
    });
}

/// 要不要升、升到哪个版本。纯函数,便于测试。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// 平台没给版本 / 已是目标版本 / 这个目标刚试运行失败过。
    Stay,
    /// 装 `version`;`clear_failed` = 平台已换了目标,先清掉上一次的失败记录。
    Install { version: String, clear_failed: bool },
}

pub fn decide(current: &str, target: Option<&str>, failed: Option<&str>) -> Decision {
    let Some(target) = target
        .map(|t| trim_v_prefix(t.trim()))
        .filter(|t| !t.is_empty())
    else {
        return Decision::Stay;
    };
    if target == trim_v_prefix(current) {
        return Decision::Stay;
    }
    let failed = failed.map(|f| trim_v_prefix(f.trim()));
    if failed == Some(target) {
        return Decision::Stay;
    }
    Decision::Install {
        version: target.to_string(),
        clear_failed: failed.is_some(),
    }
}

async fn tick(state: &Arc<AppState>) {
    let current = env!("CARGO_PKG_VERSION");
    let failed = install::failed_upgrade_version();
    let target = match crate::commands::myclaw_platform::target_version(
        &state.db,
        current,
        failed.as_deref(),
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!("[update/platform] target version unavailable: {e}");
            return;
        }
    };
    let version = match decide(current, target.as_deref(), failed.as_deref()) {
        Decision::Stay => {
            if let (Some(t), Some(f)) = (target.as_deref(), failed.as_deref()) {
                if trim_v_prefix(t) == trim_v_prefix(f) {
                    tracing::warn!("[update/platform] target v{t} failed its trial before; not retrying until the platform changes the target");
                }
            }
            return;
        }
        Decision::Install {
            version,
            clear_failed,
        } => {
            if clear_failed {
                install::clear_failed_upgrade_version();
            }
            version
        }
    };

    match update_state::snapshot(&state.update_state).status {
        AppUpdateLifecycle::ReadyToRestart => {}
        AppUpdateLifecycle::Idle | AppUpdateLifecycle::Error => {
            // 上一个进程替换完、这个进程还在试运行期:标记没清之前不能再替换(会毁掉 `.bak`)。
            if install::upgrade_staged() {
                return;
            }
            tracing::info!("[update/platform] v{current} → v{version}: downloading");
            if let Err(e) = crate::web::handlers::app_update::start_perform(
                state.clone(),
                ReleaseSource::Pinned(version.clone()),
            )
            .await
            {
                tracing::warn!("[update/platform] could not start update to v{version}: {e}");
                return;
            }
            if !wait_until_staged(state).await {
                return;
            }
        }
        // 手动升级正在进行 / 正在重启:这一轮不插手。
        _ => return,
    }

    tracing::info!("[update/platform] v{version} staged; waiting for a quiet moment to restart");
    wait_idle_then_restart(state).await;
}

async fn wait_until_staged(state: &Arc<AppState>) -> bool {
    let deadline = Instant::now() + STAGE_WAIT_MAX;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(5)).await;
        match update_state::snapshot(&state.update_state).status {
            AppUpdateLifecycle::ReadyToRestart => return true,
            AppUpdateLifecycle::Error => {
                tracing::warn!(
                    "[update/platform] update failed: {}",
                    update_state::snapshot(&state.update_state)
                        .error
                        .unwrap_or_default()
                );
                return false;
            }
            _ => {}
        }
    }
    tracing::warn!("[update/platform] update did not finish staging in time");
    false
}

/// 此刻是不是没人在用:没有页面连着、没有正在跑的一轮、接下来一段时间没有定时任务。
async fn is_quiet(state: &Arc<AppState>) -> bool {
    if crate::web::ws::ws_client_count() > 0 {
        return false;
    }
    if crate::web::handlers::myclaw::activity::count_active_turns(&state.connection_manager).await
        > 0
    {
        return false;
    }
    let horizon = chrono::Utc::now() + chrono::Duration::minutes(AUTOMATION_LOOKAHEAD_MINUTES);
    match crate::db::service::automation_service::list_due(&state.db.conn, horizon).await {
        Ok(due) => due.is_empty(),
        // 读不到就当有任务:宁可晚点升,不打断定时任务。
        Err(_) => false,
    }
}

async fn wait_idle_then_restart(state: &Arc<AppState>) {
    let deadline = Instant::now() + IDLE_WAIT_MAX;
    let mut quiet_since: Option<Instant> = None;
    while Instant::now() < deadline {
        if update_state::snapshot(&state.update_state).status != AppUpdateLifecycle::ReadyToRestart
        {
            return;
        }
        if is_quiet(state).await {
            let since = *quiet_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= IDLE_REQUIRED {
                tracing::info!(
                    "[update/platform] quiet for {}s; restarting into the staged version",
                    since.elapsed().as_secs()
                );
                if let Err(e) = crate::web::handlers::app_update::restart_impl(state.clone()) {
                    tracing::warn!("[update/platform] restart refused: {e}");
                }
                return;
            }
        } else {
            quiet_since = None;
        }
        tokio::time::sleep(IDLE_POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_without_target_or_on_target() {
        assert_eq!(decide("0.30.10-43", None, None), Decision::Stay);
        assert_eq!(decide("0.30.10-43", Some(""), None), Decision::Stay);
        assert_eq!(
            decide("0.30.10-43", Some("0.30.10-43"), None),
            Decision::Stay
        );
        assert_eq!(
            decide("0.30.10-43", Some("v0.30.10-43"), None),
            Decision::Stay
        );
    }

    #[test]
    fn installs_newer_and_older_targets() {
        assert_eq!(
            decide("0.30.10-43", Some("0.30.10-44"), None),
            Decision::Install {
                version: "0.30.10-44".into(),
                clear_failed: false
            }
        );
        // 目标比当前低 = 平台要求回退
        assert_eq!(
            decide("0.30.10-43", Some("0.30.10-31"), None),
            Decision::Install {
                version: "0.30.10-31".into(),
                clear_failed: false
            }
        );
    }

    #[test]
    fn does_not_retry_a_version_that_failed_its_trial() {
        assert_eq!(
            decide("0.30.10-43", Some("0.30.10-44"), Some("0.30.10-44")),
            Decision::Stay
        );
        // 平台换了目标 → 装新目标,并清掉旧的失败记录
        assert_eq!(
            decide("0.30.10-43", Some("0.30.10-45"), Some("0.30.10-44")),
            Decision::Install {
                version: "0.30.10-45".into(),
                clear_failed: true
            }
        );
    }
}
