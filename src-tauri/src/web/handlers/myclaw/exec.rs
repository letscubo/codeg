//! `POST /api/myclaw/exec` — fork(letscubo)专属:一次性 shell 命令执行。
//!
//! 业务端(MyClaw 平台)通过这个接口在实例容器内跑一条命令,主要用于读写系统
//! 文件/目录等运维动作,省去上游 `terminal_*`(spawn/write/resize/read)那套 PTY
//! 三步交互 —— 一个请求进,`{stdout, stderr, exitCode}` 出。
//!
//! ## 鉴权 / 信任边界
//!
//! 与所有 `/api/*` 一样过 `web::auth::require_token`(Bearer,或 WS 子协议)。
//! codeg 的单 token 本就等价于容器内全权限(`terminal_spawn` 能起任意 shell、
//! `git_*` 能跑 git、`files`/`workspace_files` 能读写文件),**本接口不新增任何
//! 信任边界**,只是把 one-shot 运维 exec 收敛成一次调用。因此它必须挂在受
//! 保护路由组内(router.rs 已保证),绝不放进 public_api。
//!
//! ## 安全约束(本文件内自持,不依赖调用方自觉)
//!
//! - stdout/stderr 各自捕获上限 `MAX_OUTPUT_BYTES`,超出截断并置 `truncated=true`,
//!   防止一条 `cat 大文件` 把进程内存打爆。
//! - `timeoutMs` 有默认值与硬上限,超时 `kill_on_drop` 杀直接子进程(bash)。
//!   注意:命令若 fork 出后台孙进程,孙进程可能残留 —— 运维一次性命令一般不涉及。

use std::collections::BTreeMap;
use std::process::Stdio;
use std::time::{Duration, Instant};

use axum::Json;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// stdout / stderr 各自的捕获上限(16 MiB),超出即截断。
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
/// `timeoutMs` 缺省值(60s)与硬上限(30min)。
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 30 * 60_000;

use crate::app_error::AppCommandError;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecParams {
    /// 要执行的命令,经 `bash -c` 运行(支持管道 / 重定向 / glob / heredoc)。
    pub command: String,
    /// 可选工作目录;缺省继承 codeg-server 进程的 cwd。
    #[serde(default)]
    pub cwd: Option<String>,
    /// 可选:写入子进程 stdin(如 `cat > /path` 写文件)。应保持适度大小 ——
    /// 写在读 stdout 之前完成,超大 stdin 撑满管道可能自阻塞。
    #[serde(default)]
    pub stdin: Option<String>,
    /// 可选附加环境变量,叠加在继承的进程环境之上。
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// 可选超时(毫秒),缺省 60s,上限 30min。
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    /// 进程退出码;被信号终止或超时杀死时为 `null`。
    pub exit_code: Option<i32>,
    /// 是否因超时被杀。
    pub timed_out: bool,
    /// stdout 或 stderr 是否因超过 `MAX_OUTPUT_BYTES` 被截断。
    pub truncated: bool,
    pub duration_ms: u64,
}

/// 把捕获到的字节流按上限截断,返回(文本, 是否截断)。用 lossy 解码,保证
/// 二进制输出也能安全返回而不是整个请求失败。
fn clamp_output(bytes: Vec<u8>) -> (String, bool) {
    if bytes.len() > MAX_OUTPUT_BYTES {
        let text = String::from_utf8_lossy(&bytes[..MAX_OUTPUT_BYTES]).into_owned();
        (text, true)
    } else {
        (String::from_utf8_lossy(&bytes).into_owned(), false)
    }
}

pub async fn exec(Json(params): Json<ExecParams>) -> Result<Json<ExecResult>, AppCommandError> {
    if params.command.trim().is_empty() {
        return Err(AppCommandError::invalid_input("command must not be empty"));
    }

    let timeout = Duration::from_millis(
        params
            .timeout_ms
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .clamp(1, MAX_TIMEOUT_MS),
    );

    let mut cmd = Command::new("bash");
    cmd.arg("-c").arg(&params.command);
    if let Some(cwd) = params.cwd.as_deref().filter(|s| !s.is_empty()) {
        cmd.current_dir(cwd);
    }
    for (k, v) in &params.env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // 超时时 future 被 drop,连带 drop child -> 杀掉直接子进程(bash)。
        .kill_on_drop(true);

    let started = Instant::now();
    let mut child = cmd.spawn().map_err(|e| {
        AppCommandError::io_error("failed to spawn command").with_detail(e.to_string())
    })?;

    // 无论是否提供 stdin,都必须关闭它:否则读到 stdin 的命令会一直阻塞等输入,
    // 直到超时才被杀。take() 后 drop 即发送 EOF。
    if let Some(mut sink) = child.stdin.take() {
        if let Some(input) = params.stdin.as_deref() {
            sink.write_all(input.as_bytes()).await.map_err(|e| {
                AppCommandError::io_error("failed to write stdin").with_detail(e.to_string())
            })?;
        }
        // drop(sink) 关闭写端 => 子进程收到 EOF。
    }

    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => {
            let (stdout, out_trunc) = clamp_output(output.stdout);
            let (stderr, err_trunc) = clamp_output(output.stderr);
            Ok(Json(ExecResult {
                stdout,
                stderr,
                exit_code: output.status.code(),
                timed_out: false,
                truncated: out_trunc || err_trunc,
                duration_ms: started.elapsed().as_millis() as u64,
            }))
        }
        Ok(Err(e)) => Err(AppCommandError::io_error("failed to collect command output")
            .with_detail(e.to_string())),
        Err(_elapsed) => Ok(Json(ExecResult {
            stdout: String::new(),
            stderr: format!("command timed out after {}ms", timeout.as_millis()),
            exit_code: None,
            timed_out: true,
            truncated: false,
            duration_ms: started.elapsed().as_millis() as u64,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_command_and_captures_stdout() {
        let params = ExecParams {
            command: "printf 'hello'".into(),
            cwd: None,
            stdin: None,
            env: BTreeMap::new(),
            timeout_ms: None,
        };
        let res = exec(Json(params)).await.unwrap().0;
        assert_eq!(res.stdout, "hello");
        assert_eq!(res.exit_code, Some(0));
        assert!(!res.timed_out);
    }

    #[tokio::test]
    async fn propagates_nonzero_exit_and_stderr() {
        let params = ExecParams {
            command: "echo oops >&2; exit 3".into(),
            cwd: None,
            stdin: None,
            env: BTreeMap::new(),
            timeout_ms: None,
        };
        let res = exec(Json(params)).await.unwrap().0;
        assert_eq!(res.exit_code, Some(3));
        assert_eq!(res.stderr.trim(), "oops");
    }

    #[tokio::test]
    async fn writes_stdin_to_child() {
        let params = ExecParams {
            command: "cat".into(),
            cwd: None,
            stdin: Some("piped-input".into()),
            env: BTreeMap::new(),
            timeout_ms: None,
        };
        let res = exec(Json(params)).await.unwrap().0;
        assert_eq!(res.stdout, "piped-input");
    }

    #[tokio::test]
    async fn empty_command_is_rejected() {
        let params = ExecParams {
            command: "   ".into(),
            cwd: None,
            stdin: None,
            env: BTreeMap::new(),
            timeout_ms: None,
        };
        assert!(exec(Json(params)).await.is_err());
    }

    #[tokio::test]
    async fn honors_env_and_cwd() {
        let params = ExecParams {
            command: "echo \"$MYCLAW_TEST_VAR $(pwd)\"".into(),
            cwd: Some("/tmp".into()),
            stdin: None,
            env: BTreeMap::from([("MYCLAW_TEST_VAR".to_string(), "ok".to_string())]),
            timeout_ms: None,
        };
        let res = exec(Json(params)).await.unwrap().0;
        assert!(res.stdout.contains("ok"));
        assert!(res.stdout.contains("/tmp"));
    }

    #[tokio::test]
    async fn times_out_long_command() {
        let params = ExecParams {
            command: "sleep 5".into(),
            cwd: None,
            stdin: None,
            env: BTreeMap::new(),
            timeout_ms: Some(200),
        };
        let res = exec(Json(params)).await.unwrap().0;
        assert!(res.timed_out);
        assert_eq!(res.exit_code, None);
    }
}
