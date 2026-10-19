//! `GET /api/myclaw/activity` — fork(letscubo)专属:这台实例「当前有没有人在用」。
//!
//! ## 为什么要它
//!
//! 平台要判断实例忙/闲(空闲就停、有人用就别停)。已有的判据都不够用:
//!
//! - **消息与工具调用是整轮结束后才批量落库的**。一轮跑两小时,库里这两小时一片
//!   空白 —— 单看数据库会把正在满负荷跑的实例判成闲。定时任务同理。
//! - 渠道(Telegram / Slack)驱动的轮次连网页都不经过,更看不见。
//!
//! 而浏览器打开页面就连上 `/ws/events` 旁听**整个实例**的事件流(attach_all),
//! 这条连接在整轮期间一直开着。所以它同时回答了两件事:「有人在看吗」和
//! 「是不是正在跑一轮」。
//!
//! ## 为什么不并进 `myclaw/metrics`
//!
//! 那条是给面板负荷环用的:每次调用都要采 CPU(与上次采样比,还持有进程内的
//! `LAST_CPU`)、读内存与 `statvfs`。平台判忙/闲是**按实例定时全量扫**的,不该为拿
//! 一个计数去做这些系统调用,也不该干扰负荷环的采样窗口。这条只读一个原子量。
//!
//! ## 口径
//!
//! `ws_clients` 一条 WS 连接算一个,与它内部开了几个 attach 订阅无关。计数在
//! `web/ws.rs`,用 RAII 保证进出配对。
//!
//! `active_turns` 是此刻**正在跑一轮**的会话连接数(状态为 `Prompting`)。网页对话、
//! Telegram / Slack 驱动的轮、定时任务起的轮、CLI 通道(claude / deepseek / codex 驱动)
//! 都在同一个连接管理器里,一并计入;等权限 / 等提问回答时这一轮没结束,也仍算在内。
//! 平台用它做「暂停前的最后确认」:`ws_clients` 为 0 只说明没人在看,一轮可以在
//! 没人看的时候跑很久(定时任务、渠道消息),这时暂停就会把它打断 —— 容器重启时
//! codeg 会把这一轮判为失败(`boot_reconcile_interrupted`),不会重跑。
//!
//! ⚠️ 读的是每条连接 `state.status`(事件流实时写入),不是 `AgentConnection.status`
//! —— 后者是建连时的快照,之后不再更新,`acp_list_connections` 返回的就是它。
//!
//! ## 「最近有没有人用」(2026-10-18)
//!
//! `ws_clients` 只说明有页面连着,后台挂着的标签页也算 —— 平台若拿它挡停机,实例就永远停不
//! 下来。所以再报三个「距今多久」(只报事实,规则由平台定):
//!
//! - `user_active_ago_s`:距上次用户在页面上操作(前端发 `user_activity`)
//! - `turn_ended_ago_s`:距上一轮结束
//! - `next_automation_in_s`:距最近一次定时任务到点(null = 没有或读不到)
//!
//! 前两个按墙钟算、落盘(重启读回);从没有过报约 10 年(`presence::NEVER_AGO_S`),不报 null。见 `acp/presence.rs`。
//!
//! ## 维护中
//!
//! `maintenance`:平台触发的维护正在进行(目前只有备份,见 `handlers/myclaw/backup.rs`)。
//! 平台据此别暂停实例;codeg 自己的闲时自动升级也不在这时重启。
//!
//! 鉴权与 `/api/myclaw/exec` 同:受保护路由组内,Bearer 或 WS 子协议。

use std::sync::Arc;

use axum::{Extension, Json};
use serde::Serialize;

use crate::acp::manager::ConnectionManager;
use crate::acp::types::ConnectionStatus;
use crate::app_state::AppState;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ActivityResponse {
    /// 当前挂在 `/ws/events` 上的客户端数。0 = 没有任何页面在旁听这台实例。
    pub ws_clients: usize,
    /// 此刻正在跑一轮的会话连接数。0 = 没有任何一轮在进行(网页、渠道、定时任务都算)。
    pub active_turns: usize,
    /// 采集时刻(RFC3339),调用方据此判断新鲜度。
    pub collected_at: String,
    /// 距上次「用户在页面上操作」过了几秒(前端经 `/ws/events` 发 `user_activity`)。
    /// 从没有过 = `presence::NEVER_AGO_S`(约 10 年)。见 `acp/presence.rs`。
    pub user_active_ago_s: u64,
    /// 距上一轮结束(任何连接从「正在跑」变成别的状态)过了几秒;从没有过 = `presence::NEVER_AGO_S`。
    pub turn_ended_ago_s: u64,
    /// 最近一次定时任务还有几秒到点(已过点还没被认领的报 0);没有启用的定时任务、或读库
    /// 失败时为 null —— 调用方应把 null 当「不知道」,而不是「没有任务」去放心停机。
    pub next_automation_in_s: Option<i64>,
    /// 正在做的平台维护(目前只有 `"backup"`,见 `handlers/myclaw/backup.rs`);没有为 null。
    /// 非 null 时平台应当成「忙」、别暂停 —— 暂停会把维护打断。
    pub maintenance: Option<&'static str>,
}

pub async fn activity(Extension(state): Extension<Arc<AppState>>) -> Json<ActivityResponse> {
    let now = chrono::Utc::now();
    let next_automation_in_s =
        match crate::db::service::automation_service::next_scheduled_run(&state.db.conn).await {
            Ok(at) => at.map(|at| (at - now).num_seconds().max(0)),
            Err(e) => {
                tracing::warn!("[myclaw/activity] next_scheduled_run failed: {e}");
                None
            }
        };
    Json(ActivityResponse {
        ws_clients: crate::web::ws::ws_client_count(),
        active_turns: count_active_turns(&state.connection_manager).await,
        collected_at: now.to_rfc3339(),
        user_active_ago_s: crate::acp::presence::user_active_ago_s(),
        turn_ended_ago_s: crate::acp::presence::turn_ended_ago_s(),
        next_automation_in_s,
        maintenance: super::backup::maintenance(),
    })
}

/// 状态为 `Prompting` 的连接数。先在连接表的锁里把各连接的状态句柄拷出来、放锁,
/// 再逐个读 —— 不在持有连接表锁的时候等每条连接的读锁。
pub async fn count_active_turns(manager: &ConnectionManager) -> usize {
    let states: Vec<_> = {
        let connections = manager.connections.lock().await;
        connections.values().map(|c| c.state.clone()).collect()
    };
    let mut n = 0;
    for state in states {
        if state.read().await.status == ConnectionStatus::Prompting {
            n += 1;
        }
    }
    n
}
