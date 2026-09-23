//! MyClaw fork ext (letscubo) —— WS 连上就主动下发一次实例快照。
//!
//! 面板顶栏要显示「版本 / CPU / 内存 / 磁盘」。原先这两类各自一次请求(版本走平台路由转
//! `check_app_update`、指标读平台的 vm_idle),打开页面要等用户悬停才开始拉。既然页面本来
//! 就连着 `/ws/events`,连上时由服务端直接推一帧,页面不用问。
//!
//! **只推本地能读到的东西**:版本、自更新能力、CPU/内存/磁盘 —— 全是毫秒级的本地读。
//! 「有没有新版」要访问 GitHub(`latest.json`),几百毫秒到数秒,而且每条连接都查一次
//! 等于每开一次页面给上游打一发;它**不进快照**,改由面板真的展开时现查
//! (`check_app_update`)。
//!
//! 字段用驼峰(与 codeg 既有的 check_app_update 响应一致);metrics 段沿用 snake_case。
//! 帧形状沿用旧的全局帧 `{channel, payload}`(与安装日志 `app://agent-install`、终端输出
//! `terminal://output/<id>` 同类),channel = `myclaw://snapshot`。客户端按 channel 订阅。

use std::sync::Arc;

use serde_json::json;
use tokio::sync::mpsc;

use super::handlers::myclaw::metrics;
use super::ws_attach::ServerMsg;
use crate::update::runtime;

/// 客户端按这个 channel 订阅(web: `lib/codeg/event-hub.ts`)。
pub const SNAPSHOT_CHANNEL: &str = "myclaw://snapshot";

/// 连上后推快照。一帧,全部本地读 —— 调用方 spawn 它,不阻塞 WS 主循环。
pub async fn push_snapshot(outbound_tx: mpsc::Sender<ServerMsg>) {
    let m = metrics::collect().await;
    let payload = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "capability": runtime::capability(),
        "runtime": runtime::runtime_label(),
        "restartDelayMs": runtime::restart_delay_ms(),
        "metrics": m,
    });
    let _ = outbound_tx.send(channel_frame(payload)).await;
}

fn channel_frame(payload: serde_json::Value) -> ServerMsg {
    ServerMsg::Channel {
        channel: SNAPSHOT_CHANNEL.to_string(),
        payload: Arc::new(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_carries_version_and_metrics_without_network() {
        let (tx, mut rx) = mpsc::channel::<ServerMsg>(8);
        tokio::spawn(push_snapshot(tx));
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("快照必须在不联网的前提下发出")
            .expect("有帧");
        match frame {
            ServerMsg::Channel { channel, payload } => {
                assert_eq!(channel, SNAPSHOT_CHANNEL);
                assert_eq!(payload["version"], env!("CARGO_PKG_VERSION"));
                assert!(payload["metrics"]["collected_at"].is_string());
                // 「有没有新版」要联网,不进快照 —— 面板展开时再 check_app_update
                assert!(payload.get("updateAvailable").is_none());
                assert!(payload.get("latestVersion").is_none());
                assert!(payload.get("updateChecked").is_none());
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    /// 只发一帧:第二帧(查 GitHub)已经去掉,别再悄悄加回来。
    #[tokio::test]
    async fn snapshot_is_a_single_frame() {
        let (tx, mut rx) = mpsc::channel::<ServerMsg>(8);
        push_snapshot(tx).await;
        assert!(rx.recv().await.is_some(), "第一帧");
        assert!(rx.recv().await.is_none(), "不该有第二帧");
    }
}
