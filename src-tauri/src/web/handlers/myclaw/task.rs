//! `POST /api/myclaw/task` + `GET /api/myclaw/task/{taskId}` — fork(letscubo)专属:
//! **异步**命令执行,exec 的孪生接口。
//!
//! - POST 提交一条命令(body 契约与 exec 完全相同,见 [`super::runner::ExecRequest`]),
//!   立即返回 `{taskId}`,命令丢进 `tokio::spawn` 后台跑,不阻塞请求。
//! - GET 用 taskId 查**实时**快照:运行中返回「到此刻为止」累积的 stdout/stderr +
//!   `status:"running"`;跑完返回完整输出 + `status:"completed"|"failed"` + exitCode。
//!   业务端提交后轮询这个端点直到 status 非 running。
//!
//! 执行核心与 exec 共用 [`super::runner`]。
//!
//! ## 存储:进程内内存(重启即丢)
//!
//! 任务态存在一个进程内 `static` map(不碰上游 AppState,fork 隔离最干净)。
//! **codeg-server 重启 / 升级后 taskId 全部失效** —— 业务端须把任务视为
//! ephemeral,重启后按 404 处理并重投。为防内存无限增长,map 到达
//! `MAX_TASKS` 时驱逐最老的**已完成**任务(运行中的永不驱逐)。
//!
//! ## 鉴权 / 信任边界
//!
//! 与 exec 相同:过 `require_token`,单 token 等价容器全权限,不新增信任边界,
//! 两个路由都必须在受保护组内(router.rs 已保证)。

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use axum::extract::Path;
use axum::Json;
use serde::Serialize;

use super::runner::{self, ExecRequest, RunState, RunStatus};
use crate::app_error::AppCommandError;

/// 内存中保留的任务上限;超出时驱逐最老的已完成任务。
const MAX_TASKS: usize = 2000;

/// 一条任务的存档:执行状态 + 入队时刻(仅用于驱逐排序)。
struct TaskEntry {
    state: Arc<Mutex<RunState>>,
    created_at: Instant,
}

/// 进程内任务表。`static` 而非挂 AppState —— fork 专属,不改上游结构。
static STORE: LazyLock<Mutex<HashMap<String, TaskEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitResult {
    pub task_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSnapshot {
    pub task_id: String,
    /// "running" | "completed" | "failed"。
    pub status: &'static str,
    /// 到此刻为止累积的 stdout(运行中即为部分输出)。
    pub stdout: String,
    pub stderr: String,
    /// 进程退出码;运行中 / 超时 / 信号终止时为 `null`。
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub truncated: bool,
    /// spawn 失败原因(status=="failed" 时有值)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 运行中=至今耗时;终态=最终耗时(ms)。
    pub duration_ms: u64,
}

fn status_str(s: RunStatus) -> &'static str {
    match s {
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
    }
}

/// 从共享 RunState 投影出对外快照(短临界区内 clone 出数据,不跨 await 持锁)。
fn snapshot(task_id: String, state: &Arc<Mutex<RunState>>) -> TaskSnapshot {
    let s = state.lock().unwrap();
    TaskSnapshot {
        task_id,
        status: status_str(s.status),
        stdout: String::from_utf8_lossy(&s.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&s.stderr).into_owned(),
        exit_code: s.exit_code,
        timed_out: s.timed_out,
        truncated: s.truncated(),
        error: s.spawn_error.clone(),
        duration_ms: s.elapsed_ms(),
    }
}

/// 插入前的容量守护:超过 MAX_TASKS 时,按入队时间从老到新驱逐**已完成**任务,
/// 直到回落到上限内。运行中的任务永不驱逐(否则丢失正在进行的结果)。
fn evict_if_needed(map: &mut HashMap<String, TaskEntry>) {
    if map.len() < MAX_TASKS {
        return;
    }
    let mut done: Vec<(String, Instant)> = map
        .iter()
        .filter(|(_, e)| e.state.lock().unwrap().status != RunStatus::Running)
        .map(|(id, e)| (id.clone(), e.created_at))
        .collect();
    done.sort_by_key(|(_, t)| *t);
    for (id, _) in done {
        if map.len() < MAX_TASKS {
            break;
        }
        map.remove(&id);
    }
}

pub async fn submit(Json(req): Json<ExecRequest>) -> Result<Json<SubmitResult>, AppCommandError> {
    if req.is_blank() {
        return Err(AppCommandError::invalid_input("command must not be empty"));
    }

    let task_id = uuid::Uuid::new_v4().to_string();
    let state = RunState::shared();

    {
        let mut map = STORE.lock().unwrap();
        evict_if_needed(&mut map);
        map.insert(
            task_id.clone(),
            TaskEntry {
                state: state.clone(),
                created_at: Instant::now(),
            },
        );
    }

    // 后台驱动:run 把输出累积进同一个 state,GET 随时可读。
    tokio::spawn(async move {
        runner::run(req, state).await;
    });

    Ok(Json(SubmitResult { task_id }))
}

pub async fn get(Path(task_id): Path<String>) -> Result<Json<TaskSnapshot>, AppCommandError> {
    let state = {
        let map = STORE.lock().unwrap();
        map.get(&task_id).map(|e| e.state.clone())
    };
    match state {
        Some(state) => Ok(Json(snapshot(task_id, &state))),
        None => Err(AppCommandError::not_found(format!(
            "task {task_id} not found (unknown id, or lost to a server restart)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

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
    async fn submit_returns_id_and_get_reaches_completed() {
        let id = submit(Json(req("printf done"))).await.unwrap().0.task_id;
        assert!(!id.is_empty());
        // 轮询到终态(短命令,几十 ms 内完成)。
        let mut snap = get(Path(id.clone())).await.unwrap().0;
        for _ in 0..100 {
            if snap.status != "running" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            snap = get(Path(id.clone())).await.unwrap().0;
        }
        assert_eq!(snap.status, "completed");
        assert_eq!(snap.stdout, "done");
        assert_eq!(snap.exit_code, Some(0));
    }

    #[tokio::test]
    async fn unknown_task_is_404() {
        assert!(get(Path("no-such-task".into())).await.is_err());
    }

    #[tokio::test]
    async fn running_task_reports_accumulated_output() {
        // 先打印一行再睡,提交后立刻查应看到 running + 已有部分输出。
        let id = submit(Json(req("echo first; sleep 2")))
            .await
            .unwrap()
            .0
            .task_id;
        // 给它一点时间打印首行但远不到睡醒。
        tokio::time::sleep(Duration::from_millis(300)).await;
        let snap = get(Path(id)).await.unwrap().0;
        assert_eq!(snap.status, "running");
        assert!(snap.stdout.contains("first"));
        assert_eq!(snap.exit_code, None);
    }

    #[tokio::test]
    async fn blank_command_is_rejected() {
        assert!(submit(Json(req("  "))).await.is_err());
    }
}
