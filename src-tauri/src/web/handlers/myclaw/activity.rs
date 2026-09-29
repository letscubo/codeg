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
//! 鉴权与 `/api/myclaw/exec` 同:受保护路由组内,Bearer 或 WS 子协议。

use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ActivityResponse {
    /// 当前挂在 `/ws/events` 上的客户端数。0 = 没有任何页面在旁听这台实例。
    pub ws_clients: usize,
    /// 采集时刻(RFC3339),调用方据此判断新鲜度。
    pub collected_at: String,
}

pub async fn activity() -> Json<ActivityResponse> {
    Json(ActivityResponse {
        ws_clients: crate::web::ws::ws_client_count(),
        collected_at: chrono::Utc::now().to_rfc3339(),
    })
}
