//! MyClaw fork ext (letscubo) —— WS 连上就主动下发一次实例快照。
//!
//! 面板顶栏要显示「版本 / 有没有新版 / CPU / 内存 / 磁盘」。原先这三类各自一次请求
//! (版本走平台路由转 `check_app_update`、指标读平台的 vm_idle),打开页面要等用户悬停
//! 才开始拉。既然页面本来就连着 `/ws/events`,连上时由服务端直接推一帧,页面不用问。
//!
//! 两帧,因为两类数据的代价差了三个数量级:
//!   ① 连上立刻:版本 + 自更新能力 + CPU/内存/磁盘 —— 全是本地读,毫秒级;
//!   ② 随后补发:`update_available` / `latest_version` —— 要访问 GitHub(`latest.json`),
//!      几百毫秒到数秒。取不到就不发第二帧(面板保持「无新版」的默认显示)。
//!
//! 字段用驼峰(与 codeg 既有的 check_app_update 响应一致);metrics 段沿用 snake_case。
//! 帧形状沿用旧的全局帧 `{channel, payload}`(与安装日志 `app://agent-install`、终端输出
//! `terminal://output/<id>` 同类),channel = `myclaw://snapshot`。客户端按 channel 订阅。

use std::sync::Arc;

use serde_json::json;
use tokio::sync::mpsc;

use super::handlers::myclaw::metrics;
use super::ws_attach::ServerMsg;
use crate::update::{runtime, version};

/// 客户端按这个 channel 订阅(web: `lib/codeg/event-hub.ts`)。
pub const SNAPSHOT_CHANNEL: &str = "myclaw://snapshot";

/// 第二帧(查 GitHub)的等待上限:超了就不发,不拖着任务不放。
const UPDATE_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// 连上后推快照。两帧之间不阻塞 WS 主循环 —— 调用方 spawn 它。
pub async fn push_snapshot(outbound_tx: mpsc::Sender<ServerMsg>) {
    let m = metrics::collect().await;
    let first = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "capability": runtime::capability(),
        "runtime": runtime::runtime_label(),
        "restartDelayMs": runtime::restart_delay_ms(),
        "updateChecked": false,
        "metrics": m,
    });
    if outbound_tx.send(channel_frame(first)).await.is_err() {
        return; // WS 已关
    }

    // ② 有没有新版:要联网,慢,单独一帧
    let manifest = match tokio::time::timeout(UPDATE_CHECK_TIMEOUT, version::fetch_latest_manifest()).await {
        Ok(Ok(m)) => m,
        _ => return, // 查不到(离线 / 限流 / 超时)就不发:面板保持默认
    };
    let current = env!("CARGO_PKG_VERSION");
    let newer = version::is_newer(&manifest.version, current);
    let second = json!({
        "version": current,
        "updateChecked": true,
        "updateAvailable": newer,
        "latestVersion": if newer { Some(version::trim_v_prefix(&manifest.version).to_string()) } else { None },
        "releaseNotes": if newer { manifest.notes.clone() } else { None },
        "releaseDate": if newer { manifest.pub_date.clone() } else { None },
    });
    let _ = outbound_tx.send(channel_frame(second)).await;
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
    async fn first_frame_carries_version_and_metrics_without_network() {
        let (tx, mut rx) = mpsc::channel::<ServerMsg>(8);
        tokio::spawn(push_snapshot(tx));
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("第一帧必须在联网之前就发出")
            .expect("有帧");
        match frame {
            ServerMsg::Channel { channel, payload } => {
                assert_eq!(channel, SNAPSHOT_CHANNEL);
                assert_eq!(payload["version"], env!("CARGO_PKG_VERSION"));
                assert_eq!(payload["updateChecked"], false);
                assert!(payload["metrics"]["collected_at"].is_string());
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}
