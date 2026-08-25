//! fork(letscubo)专属:命令执行核心 —— `/api/myclaw/exec`(同步)与
//! `/api/myclaw/task`(异步)共用同一套 spawn / 流式捕获 / 超时 / 截断逻辑。
//!
//! 设计要点:输出**边跑边累积**进 `Arc<Mutex<RunState>>`。同步的 exec 直接
//! `run().await` 等它跑到终态再一次性投影;异步的 task 把同一个 `run` 丢进
//! `tokio::spawn` 后台跑,GET 轮询时读 `RunState` 当前快照 —— 于是运行中也能
//! 拿到「到此刻为止」的 stdout/stderr,而不是完成后才有输出。

use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

/// stdout / stderr 各自的累积上限(16 MiB),超出即停止累积并置 truncated;
/// 仍继续读并丢弃,避免子进程写满管道后阻塞。
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
/// `timeoutMs` 缺省值(60s)与硬上限(30min)。
pub const DEFAULT_TIMEOUT_MS: u64 = 60_000;
pub const MAX_TIMEOUT_MS: u64 = 30 * 60_000;

/// exec 与 task 的 POST body 共用同一份输入契约(camelCase)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecRequest {
    /// 要执行的命令,经 `bash -c` 运行(支持管道 / 重定向 / glob / heredoc)。
    pub command: String,
    /// 可选工作目录;缺省继承 codeg-server 进程的 cwd。
    #[serde(default)]
    pub cwd: Option<String>,
    /// 可选:写入子进程 stdin(如 `cat > /path` 写文件)。应保持适度大小。
    #[serde(default)]
    pub stdin: Option<String>,
    /// 可选附加环境变量,叠加在继承的进程环境之上。
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// 可选超时(毫秒),缺省 60s,上限 30min。
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl ExecRequest {
    /// 命令非空校验 —— 两个 handler 提交前都调。
    pub fn is_blank(&self) -> bool {
        self.command.trim().is_empty()
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(
            self.timeout_ms
                .unwrap_or(DEFAULT_TIMEOUT_MS)
                .clamp(1, MAX_TIMEOUT_MS),
        )
    }
}

/// 任务/执行的生命周期阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    /// 进程在跑(输出仍在累积)。
    Running,
    /// 进程已终止(正常退出 / 超时被杀 / wait 失败)。
    Completed,
    /// 连 spawn 都没成功(bash 缺失、cwd 不存在等)。
    Failed,
}

/// 共享可变的执行状态。exec / task 都从这里投影出各自的响应形状。
pub struct RunState {
    pub status: RunStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    /// 进程退出码;超时被杀 / 被信号终止 / 未完成时为 None。
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    /// spawn 失败时的原因(status == Failed 时有值)。
    pub spawn_error: Option<String>,
    started_at: Instant,
    /// 终态时长(ms);运行中为 None。
    pub duration_ms: Option<u64>,
}

impl RunState {
    pub fn new_running() -> Self {
        Self {
            status: RunStatus::Running,
            stdout: Vec::new(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
            exit_code: None,
            timed_out: false,
            spawn_error: None,
            started_at: Instant::now(),
            duration_ms: None,
        }
    }

    /// 任一路输出被截断。
    pub fn truncated(&self) -> bool {
        self.stdout_truncated || self.stderr_truncated
    }

    /// 运行中给「至今耗时」,终态给最终耗时。
    pub fn elapsed_ms(&self) -> u64 {
        self.duration_ms
            .unwrap_or_else(|| self.started_at.elapsed().as_millis() as u64)
    }

    /// 共享构造:新建一个处于 Running 的状态句柄。
    pub fn shared() -> Arc<Mutex<RunState>> {
        Arc::new(Mutex::new(RunState::new_running()))
    }
}

enum Stream {
    Out,
    Err,
}

/// 把一路输出流持续读进 `RunState`,受 `MAX_OUTPUT_BYTES` 上限约束。
/// 即使达到上限也继续读(丢弃),避免子进程因管道写满而挂起。
async fn pump<R>(mut reader: R, state: Arc<Mutex<RunState>>, which: Stream)
where
    R: AsyncRead + Unpin,
{
    let mut buf = [0u8; 8192];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let mut guard = state.lock().unwrap();
                // 先一次性解引用成 &mut RunState:否则经 MutexGuard 的 DerefMut
                // 分别取两个字段会各触发一次 &mut *guard,被判为重复可变借用。
                let s: &mut RunState = &mut guard;
                let (target, trunc) = match which {
                    Stream::Out => (&mut s.stdout, &mut s.stdout_truncated),
                    Stream::Err => (&mut s.stderr, &mut s.stderr_truncated),
                };
                let room = MAX_OUTPUT_BYTES.saturating_sub(target.len());
                if room == 0 {
                    *trunc = true;
                } else {
                    let take = n.min(room);
                    target.extend_from_slice(&buf[..take]);
                    if take < n {
                        *trunc = true;
                    }
                }
            }
            Err(_) => break,
        }
    }
}

/// 执行核心。跑到进程终态(或 spawn 失败)才返回;全程把输出累积进 `state`。
/// 由 exec 直接 await,或由 task 丢进 `tokio::spawn` 后台驱动。
pub async fn run(req: ExecRequest, state: Arc<Mutex<RunState>>) {
    let timeout = req.timeout();

    let mut cmd = Command::new("bash");
    cmd.arg("-c").arg(&req.command);
    if let Some(cwd) = req.cwd.as_deref().filter(|s| !s.is_empty()) {
        cmd.current_dir(cwd);
    }
    for (k, v) in &req.env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let mut s = state.lock().unwrap();
            s.status = RunStatus::Failed;
            s.spawn_error = Some(e.to_string());
            s.duration_ms = Some(s.started_at.elapsed().as_millis() as u64);
            return;
        }
    };

    // 关闭 stdin(写入可选内容后 drop => EOF),否则读 stdin 的命令会一直阻塞。
    if let Some(mut sink) = child.stdin.take() {
        if let Some(input) = req.stdin.as_deref() {
            let _ = sink.write_all(input.as_bytes()).await;
        }
    }

    // stdout/stderr 各起一个后台读任务,边跑边累积。
    let out_handle = child
        .stdout
        .take()
        .map(|out| tokio::spawn(pump(out, state.clone(), Stream::Out)));
    let err_handle = child
        .stderr
        .take()
        .map(|err| tokio::spawn(pump(err, state.clone(), Stream::Err)));

    let (exit_code, timed_out) = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => (status.code(), false),
        Ok(Err(_)) => (None, false),
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            (None, true)
        }
    };

    // 等两路读任务把剩余管道内容排空,快照才完整。
    if let Some(h) = out_handle {
        let _ = h.await;
    }
    if let Some(h) = err_handle {
        let _ = h.await;
    }

    let mut s = state.lock().unwrap();
    s.exit_code = exit_code;
    s.timed_out = timed_out;
    s.status = RunStatus::Completed;
    s.duration_ms = Some(s.started_at.elapsed().as_millis() as u64);
}
