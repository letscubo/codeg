//! `POST /api/myclaw/exec` — fork(letscubo)专属:**同步**一次性 shell 执行。
//!
//! 业务端(MyClaw 平台)通过这个接口在实例容器内跑一条命令,主要用于读写系统
//! 文件/目录等运维动作,省去上游 `terminal_*`(spawn/write/resize/read)那套 PTY
//! 三步交互 —— 一个请求进,`{stdout, stderr, exitCode}` 出,等命令跑完才返回。
//!
//! 需要「不阻塞、可轮询」的长命令走异步孪生接口 `/api/myclaw/task`(见 task.rs);
//! 两者共用 [`super::runner`] 的执行核心。
//!
//! ## 鉴权 / 信任边界
//!
//! 与所有 `/api/*` 一样过 `web::auth::require_token`(Bearer,或 WS 子协议)。
//! codeg 的单 token 本就等价于容器内全权限(`terminal_spawn` 能起任意 shell、
//! `git_*` 能跑 git、`files`/`workspace_files` 能读写文件),**本接口不新增任何
//! 信任边界**,只是把 one-shot 运维 exec 收敛成一次调用。必须挂在受保护路由组
//! 内(router.rs 已保证),绝不放进 public_api。

use axum::Json;
use serde::Serialize;

use super::runner::{self, ExecRequest, RunStatus};
use crate::app_error::AppCommandError;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    /// 进程退出码;被信号终止或超时杀死时为 `null`。
    pub exit_code: Option<i32>,
    /// 是否因超时被杀。
    pub timed_out: bool,
    /// stdout 或 stderr 是否因超过上限被截断。
    pub truncated: bool,
    pub duration_ms: u64,
}

pub async fn exec(Json(req): Json<ExecRequest>) -> Result<Json<ExecResult>, AppCommandError> {
    if req.is_blank() {
        return Err(AppCommandError::invalid_input("command must not be empty"));
    }

    // 同步:建一个 RunState,原地跑到终态,再投影成响应。
    let state = runner::RunState::shared();
    runner::run(req, state.clone()).await;

    let s = state.lock().unwrap();
    if s.status == RunStatus::Failed {
        return Err(AppCommandError::io_error("failed to spawn command")
            .with_detail(s.spawn_error.clone().unwrap_or_default()));
    }
    Ok(Json(ExecResult {
        stdout: String::from_utf8_lossy(&s.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&s.stderr).into_owned(),
        exit_code: s.exit_code,
        timed_out: s.timed_out,
        truncated: s.truncated(),
        duration_ms: s.elapsed_ms(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn req(command: &str) -> ExecRequest {
        ExecRequest {
            command: command.into(),
            cwd: None,
            stdin: None,
            env: BTreeMap::new(),
            timeout_ms: None,
        }
    }

    #[tokio::test]
    async fn runs_command_and_captures_stdout() {
        let res = exec(Json(req("printf 'hello'"))).await.unwrap().0;
        assert_eq!(res.stdout, "hello");
        assert_eq!(res.exit_code, Some(0));
        assert!(!res.timed_out);
    }

    #[tokio::test]
    async fn propagates_nonzero_exit_and_stderr() {
        let res = exec(Json(req("echo oops >&2; exit 3"))).await.unwrap().0;
        assert_eq!(res.exit_code, Some(3));
        assert_eq!(res.stderr.trim(), "oops");
    }

    #[tokio::test]
    async fn writes_stdin_to_child() {
        let mut r = req("cat");
        r.stdin = Some("piped-input".into());
        let res = exec(Json(r)).await.unwrap().0;
        assert_eq!(res.stdout, "piped-input");
    }

    #[tokio::test]
    async fn empty_command_is_rejected() {
        assert!(exec(Json(req("   "))).await.is_err());
    }

    #[tokio::test]
    async fn honors_env_and_cwd() {
        let mut r = req("echo \"$MYCLAW_TEST_VAR $(pwd)\"");
        r.cwd = Some("/tmp".into());
        r.env = BTreeMap::from([("MYCLAW_TEST_VAR".to_string(), "ok".to_string())]);
        let res = exec(Json(r)).await.unwrap().0;
        assert!(res.stdout.contains("ok"));
    }

    #[tokio::test]
    async fn times_out_long_command() {
        let mut r = req("sleep 5");
        r.timeout_ms = Some(200);
        let res = exec(Json(r)).await.unwrap().0;
        assert!(res.timed_out);
        assert_eq!(res.exit_code, None);
    }
}
