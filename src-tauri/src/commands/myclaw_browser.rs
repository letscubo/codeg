//! fork(letscubo)专属: 内置浏览器工具 —— `browser_exec` / `browser_screenshot`,挂在伴生服务 `myclaw`
//! 的 `browser` 组上,每个会话自动就有(不是应用、不要安装授权)。
//!
//! ## 为什么做成内置、而且浏览器归 codeg
//!
//! 以前是应用市场的 myclaw-browser:运行时把它当 stdio MCP 拉起,脚本再拉起 Chrome。openclaw
//! 2026.10.5 每轮结束回收 MCP,并清掉它的整棵子孙进程(`ownProcessTree`),Chrome 跟着每轮被杀 ——
//! 2026-10-19 麻豆传媒实例:12 次 MCP 启动对应 12 次 Chrome 启动,标签页、刚过的验证全丢,
//! 浏览器悬浮窗两轮之间一直是 "The browser is closed"。
//!
//! 这里两样都由 codeg 持有,不在任何运行时的进程树里:
//!
//!   · **浏览器**:`web/browser_launch.sh` 起完就退出,Chrome 被 init 收养;有 Xvfb 时有头运行
//!     (无头的 UA 带 HeadlessChrome,Cloudflare 一类人机验证过不去)。
//!   · **执行进程**:browser-use 的 `CLIMCPServer`(`python -m browser_use.mcp.cli_mcp`)。它把
//!     `browser_exec` 的变量放在**自己进程里**(`exec(code, self._namespace)`,venv 内源码核实),所以
//!     要跨调用保状态就得常驻 —— 这里养一个,父进程是 codeg,codeg 当它的 MCP 客户端转发调用。
//!
//! ## 一个实例一份
//!
//! 浏览器、登录态、执行进程整个实例共用一份(key 固定 [`BROWSER_KEY`]),哪个 agent、哪条运行时
//! 调都是同一个;悬浮窗不管从哪个 agent 打开也看这一个。调用串行(一把锁)。
//!
//! ## 唯一一份实现
//!
//! stdio 伴生(经 UDS 交给父进程 listener)与 HTTP 端点(openclaw / pi)都调 [`call`],返回完整的
//! MCP `CallToolResult`,两边原样转出;截图是 `image` 块,codeg 收到工具结果时自己落盘。
//!
//! ## Python 环境
//!
//! 镜像预装在 [`IMAGE_VENV`];老镜像没有就用 / 装 `~/.myclaw-browser/venv`(与浏览器应用同一个位置、
//! 同一个 `.ready` 标记、同一把安装锁,装过应用的实例直接复用)。装的那半分钟工具回「正在准备」,
//! 不让调用干等。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout};

use crate::commands::acp::resolve_command_on_path;
use crate::process::tokio_command;

/// 两个工具的 MCP 定义。伴生与 HTTP 端点的 `tools/list` 都从这里取。
pub const BROWSER_TOOL_SCHEMA_JSON: &str = include_str!("myclaw_browser_tools.json");

/// 整个实例共用的那一份浏览器的目录名:`~/.myclaw-browser/instance/`。
pub const BROWSER_KEY: &str = "instance";

/// 页面助手 `elements()` / `act()`:browser-harness 启动时把 `$BH_AGENT_WORKSPACE/agent_helpers.py`
/// 的公开函数并进 `browser_exec` 的命名空间(原在 MyClaw 仓库的浏览器应用里,随 codeg 发布)。
const AGENT_HELPERS_PY: &str = include_str!("myclaw_browser_helpers.py");

/// 起浏览器的脚本(悬浮窗与工具共用)。
const LAUNCH_SCRIPT: &str = include_str!("../web/browser_launch.sh");

/// 镜像预装的 venv。
pub const IMAGE_VENV: &str = "/opt/myclaw-browser/venv";
/// 运行时安装用的版本 —— 与镜像预装一致(实例上验证过的组合)。
const BROWSER_USE_PIN: &str = "browser-use==0.13.11";
const BROWSER_HARNESS_PIN: &str = "browser-harness==0.1.13";

/// 启动脚本最多等 25 秒浏览器起来,再给排队拿锁留点余量。
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(45);
/// 执行进程冷启动:导入 browser-use、握手。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);
/// 单次工具调用的上限;超时把执行进程重起(变量丢失,浏览器和标签页还在)。
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
/// 排队等前一个调用的上限 —— 浏览器整实例共用,别的 agent 正在用时不无限等。
const QUEUE_TIMEOUT: Duration = Duration::from_secs(150);
/// 闲置多久回收执行进程(浏览器不回收)。可用 `CODEG_BROWSER_WORKER_IDLE_SECS` 覆盖。
const DEFAULT_IDLE: Duration = Duration::from_secs(30 * 60);
/// 返回文本的字符上限(`browser_exec` 打印的东西可能很大)。
const MAX_TEXT_CHARS: usize = 120_000;

const PREPARING: &str = "The browser is being set up on this instance (first use, about 30 seconds). \
Try the same call again in a little while; if it still says this after a few tries, tell the user the \
browser is not available yet. Do not fall back to installing your own browser tooling.";

// ─── 工具定义 ───────────────────────────────────────────────────────────

fn schema_tools() -> &'static [Value] {
    static TOOLS: OnceLock<Vec<Value>> = OnceLock::new();
    TOOLS.get_or_init(|| {
        serde_json::from_str::<Value>(BROWSER_TOOL_SCHEMA_JSON)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
    })
}

/// 两个工具的定义(给 `embedded_tools` 合并)。
pub fn tools() -> Vec<Value> {
    schema_tools().to_vec()
}

pub fn is_browser_tool_name(name: &str) -> bool {
    schema_tools()
        .iter()
        .any(|t| t.get("name").and_then(Value::as_str) == Some(name))
}

// ─── 能不能用 ───────────────────────────────────────────────────────────

/// 这台机器上能不能开浏览器组:有 chromium、有 python3(建 venv 用)。只在 Linux 容器里为真。
pub fn browser_available() -> bool {
    cfg!(target_os = "linux") && find_chromium().is_some() && resolve_command_on_path("python3").is_some()
}

/// 与启动脚本同一个查找顺序。
fn find_chromium() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("MYCLAW_CHROME").map(PathBuf::from) {
        if p.is_file() {
            return Some(p);
        }
    }
    let mut roots = vec![PathBuf::from("/opt/pw-browsers")];
    if let Some(custom) = std::env::var_os("PLAYWRIGHT_BROWSERS_PATH") {
        roots.insert(0, PathBuf::from(custom));
    }
    for root in roots {
        if let Ok(entries) = std::fs::read_dir(&root) {
            for e in entries.flatten() {
                if e.file_name().to_string_lossy().starts_with("chromium-") {
                    let chrome = e.path().join("chrome-linux64").join("chrome");
                    if chrome.is_file() {
                        return Some(chrome);
                    }
                }
            }
        }
    }
    ["/usr/bin/google-chrome-stable", "/usr/bin/chromium"]
        .iter()
        .map(PathBuf::from)
        .find(|p| p.is_file())
}

fn browser_root() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".myclaw-browser"))
}

// ─── 浏览器 ─────────────────────────────────────────────────────────────

/// 把 `key` 这份浏览器起起来(已在跑就直接回端口)。悬浮窗(`ws_browser`)也用它。
pub(crate) async fn launch_browser(key: &str) -> Result<u16, String> {
    let run = tokio::process::Command::new("bash")
        .arg("-c")
        .arg(LAUNCH_SCRIPT)
        .arg("myclaw-browser-launch")
        .env("MYCLAW_AGENT_ID", key)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output();
    let out = tokio::time::timeout(LAUNCH_TIMEOUT, run)
        .await
        .map_err(|_| "starting the browser timed out".to_string())?
        .map_err(|e| format!("cannot run the browser start script: {e}"))?;
    if out.status.success() {
        let stdout = String::from_utf8_lossy(&out.stdout);
        return stdout
            .lines()
            .rev()
            .find_map(|l| l.trim().parse::<u16>().ok())
            .ok_or_else(|| "the browser start script printed no port".to_string());
    }
    let err = String::from_utf8_lossy(&out.stderr);
    let line = err.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("");
    Err(if line.is_empty() {
        format!("the browser did not start (exit {})", out.status.code().unwrap_or(-1))
    } else {
        line.trim().to_string()
    })
}

// ─── Python 环境 ────────────────────────────────────────────────────────

/// 装好的 venv 里的 python:镜像预装的优先,其次 `~/.myclaw-browser/venv`。都以 `.ready` 为准
/// (半截安装没有它)。
fn ready_python(roots: &[PathBuf]) -> Option<PathBuf> {
    roots
        .iter()
        .filter(|v| v.join(".ready").is_file())
        .map(|v| v.join("bin").join("python"))
        .find(|p| p.is_file())
}

fn venv_roots() -> Vec<PathBuf> {
    let mut roots = vec![PathBuf::from(IMAGE_VENV)];
    if let Some(root) = browser_root() {
        roots.push(root.join("venv"));
    }
    roots
}

static INSTALLING: AtomicBool = AtomicBool::new(false);

/// 后台装 `~/.myclaw-browser/venv`(与浏览器应用同一把锁、同一个标记;别人正在装就排队,装好就跳过)。
fn start_install() {
    if INSTALLING.swap(true, Ordering::SeqCst) {
        return;
    }
    let script = format!(
        r#"set -u
H="$HOME/.myclaw-browser"; V="$H/venv"; mkdir -p "$H"
exec 9>"$H/.install.lock"; flock 9
[ -f "$V/.ready" ] && exit 0
{{
  echo "=== $(date -Is) codeg: 安装内置浏览器的 Python 环境 ==="
  rm -rf "$V"
  python3 -m venv "$V" \
    && "$V/bin/pip" install -q "{BROWSER_USE_PIN}" "{BROWSER_HARNESS_PIN}" \
    && "$V/bin/python" -c "import browser_use.mcp.cli_mcp" \
    && touch "$V/.ready" \
    && echo "=== $(date -Is) 安装完成 ==="
}} >>"$H/setup.log" 2>&1
"#
    );
    match tokio_command("bash")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(mut child) => {
            tokio::spawn(async move {
                let _ = child.wait().await;
                INSTALLING.store(false, Ordering::SeqCst);
            });
        }
        Err(e) => {
            tracing::warn!("[myclaw-browser] cannot start the python setup: {e}");
            INSTALLING.store(false, Ordering::SeqCst);
        }
    }
}

// ─── 执行进程 ───────────────────────────────────────────────────────────

struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: u64,
    last_used: Instant,
    python: PathBuf,
    port: u16,
}

static WORKER: LazyLock<tokio::sync::Mutex<Option<Worker>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));

impl Worker {
    fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    async fn send(&mut self, msg: &Value) -> Result<(), String> {
        let mut line = msg.to_string();
        line.push('\n');
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| format!("browser session pipe closed: {e}"))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| format!("browser session pipe closed: {e}"))
    }

    /// 发一个请求,读到同 id 的回应为止(通知和别的 id 跳过 —— 被取消的上一次调用的迟到回应也在此丢掉)。
    async fn request(&mut self, method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?;
        let read = async {
            loop {
                let line = self
                    .stdout
                    .next_line()
                    .await
                    .map_err(|e| format!("reading the browser session failed: {e}"))?
                    .ok_or_else(|| "the browser session exited".to_string())?;
                let Ok(msg) = serde_json::from_str::<Value>(line.trim()) else {
                    continue;
                };
                if msg.get("id").and_then(Value::as_u64) != Some(id) {
                    continue;
                }
                if let Some(err) = msg.get("error") {
                    let text = err.get("message").and_then(Value::as_str).unwrap_or("error");
                    return Err(format!("browser session error: {text}"));
                }
                return Ok(msg.get("result").cloned().unwrap_or(Value::Null));
            }
        };
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| format!("the browser call took longer than {}s", timeout.as_secs()))?
    }
}

fn reap(mut child: Child) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(async move {
                let _ = child.kill().await;
            });
        }
        Err(_) => {
            let _ = child.start_kill();
        }
    }
}

/// 执行进程的环境:运行时目录、名字都落在 instance 目录下(harness 的 daemon 默认一台机器一个,
/// 分开放才不会和老的应用进程串);临时目录用短路径且不归会话管。
fn worker_env(base: &Path, port: u16) -> Vec<(String, String)> {
    let p = |s: &str| base.join(s).to_string_lossy().into_owned();
    vec![
        ("BU_CDP_URL".into(), format!("http://127.0.0.1:{port}")),
        ("BH_CLIENT".into(), "myclaw".into()),
        ("BH_HOME".into(), p("harness")),
        ("BH_RUNTIME_DIR".into(), p("run")),
        ("BH_AGENT_WORKSPACE".into(), p("workspace")),
        ("BU_NAME".into(), BROWSER_KEY.into()),
        ("TMPDIR".into(), format!("/tmp/mcb-{BROWSER_KEY}-w")),
        ("BH_TELEMETRY".into(), "0".into()),
        ("BH_UPDATE_CHECK".into(), "0".into()),
        ("BH_OPEN_LIVE_URL".into(), "0".into()),
        ("DO_NOT_TRACK".into(), "1".into()),
    ]
}

async fn spawn_worker(python: &Path, port: u16) -> Result<Worker, String> {
    let base = browser_root()
        .ok_or_else(|| "HOME is not set".to_string())?
        .join(BROWSER_KEY);
    for dir in ["harness", "run", "workspace"] {
        std::fs::create_dir_all(base.join(dir)).map_err(|e| format!("cannot prepare the browser directory: {e}"))?;
    }
    let tmp = PathBuf::from(format!("/tmp/mcb-{BROWSER_KEY}-w"));
    let _ = std::fs::create_dir_all(&tmp);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o700));
    }
    std::fs::write(base.join("workspace").join("agent_helpers.py"), AGENT_HELPERS_PY)
        .map_err(|e| format!("cannot write the page helpers: {e}"))?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(base.join("worker.log"))
        .map(Stdio::from)
        .unwrap_or_else(|_| Stdio::null());

    let mut cmd = tokio_command(python);
    cmd.args(["-m", "browser_use.mcp.cli_mcp"])
        .current_dir(base.join("workspace"))
        .envs(worker_env(&base, port))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(log)
        .kill_on_drop(true);
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("cannot start the browser session: {e}"))?;
    let stdin = child.stdin.take().ok_or("browser session has no stdin")?;
    let stdout = child.stdout.take().ok_or("browser session has no stdout")?;
    let mut w = Worker {
        child,
        stdin,
        stdout: BufReader::new(stdout).lines(),
        next_id: 0,
        last_used: Instant::now(),
        python: python.to_path_buf(),
        port,
    };
    let init = w
        .request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "codeg", "version": env!("CARGO_PKG_VERSION") },
            }),
            HANDSHAKE_TIMEOUT,
        )
        .await;
    if let Err(e) = init {
        reap(w.child);
        return Err(format!("{e} (see ~/.myclaw-browser/{BROWSER_KEY}/worker.log)"));
    }
    w.send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))
        .await?;
    Ok(w)
}

// ─── 调用 ───────────────────────────────────────────────────────────────

/// `browser_exec` / `browser_screenshot`。返回完整的 MCP `CallToolResult`。
pub async fn call(name: &str, arguments: &Value) -> Value {
    if !is_browser_tool_name(name) {
        return error_result(format!("unknown browser tool: {name}"));
    }
    if name == "browser_exec" {
        let ok = arguments
            .get("code")
            .and_then(Value::as_str)
            .is_some_and(|c| !c.trim().is_empty());
        if !ok {
            return error_result("browser_exec needs `code`: the Python to run.".into());
        }
    }
    let Some(python) = ready_python(&venv_roots()) else {
        start_install();
        return error_result(PREPARING.into());
    };
    let port = match launch_browser(BROWSER_KEY).await {
        Ok(p) => p,
        Err(e) => return error_result(format!("The browser could not be started: {e}")),
    };
    let Ok(mut guard) = tokio::time::timeout(QUEUE_TIMEOUT, WORKER.lock()).await else {
        return error_result(
            "The browser is busy with another task on this instance. Try again shortly.".into(),
        );
    };
    let stale = match guard.as_mut() {
        Some(w) => !w.alive() || w.port != port || w.python != python,
        None => true,
    };
    if stale {
        if let Some(old) = guard.take() {
            reap(old.child);
        }
        match spawn_worker(&python, port).await {
            Ok(w) => *guard = Some(w),
            Err(e) => return error_result(format!("The browser session could not be started: {e}")),
        }
    }
    let Some(worker) = guard.as_mut() else {
        return error_result("The browser session could not be started.".into());
    };
    let res = worker
        .request(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
            CALL_TIMEOUT,
        )
        .await;
    match res {
        Ok(result) => {
            worker.last_used = Instant::now();
            normalize(result)
        }
        Err(e) => {
            if let Some(w) = guard.take() {
                reap(w.child);
            }
            error_result(format!(
                "{e}. The browser session was restarted: variables from earlier browser_exec calls are gone; \
the browser itself, its tabs and sign-ins are kept."
            ))
        }
    }
}

/// 执行进程的结果原样转出,只把超长文本截断。
fn normalize(result: Value) -> Value {
    let Some(mut obj) = result.as_object().cloned() else {
        return error_result("the browser session returned no result".into());
    };
    if let Some(Value::Array(content)) = obj.get_mut("content") {
        for block in content.iter_mut() {
            if block.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(Value::String(text)) = block.get_mut("text") {
                    if text.chars().count() > MAX_TEXT_CHARS {
                        let cut: String = text.chars().take(MAX_TEXT_CHARS).collect();
                        *text = format!("{cut}\n… (output truncated; print less at a time)");
                    }
                }
            }
        }
    } else {
        return error_result("the browser session returned no content".into());
    }
    Value::Object(obj)
}

fn error_result(message: String) -> Value {
    json!({ "content": [{ "type": "text", "text": message }], "isError": true })
}

// ─── 回收 ───────────────────────────────────────────────────────────────

fn idle_timeout() -> Duration {
    std::env::var("CODEG_BROWSER_WORKER_IDLE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_IDLE)
}

/// 闲置的执行进程收掉(正在调用的拿着锁,`try_lock` 拿不到就跳过这一轮)。浏览器不动。
pub fn sweep_browser_worker(idle: Duration) -> bool {
    let Ok(mut guard) = WORKER.try_lock() else {
        return false;
    };
    let idle_out = guard.as_ref().is_some_and(|w| w.last_used.elapsed() >= idle);
    if idle_out {
        if let Some(w) = guard.take() {
            reap(w.child);
        }
    }
    idle_out
}

/// 每分钟看一次(codeg-server 启动时 spawn)。
pub async fn browser_worker_idle_sweep_task() {
    let idle = idle_timeout();
    let mut ticker = tokio::time::interval(Duration::from_secs(60));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if sweep_browser_worker(idle) {
            tracing::info!("[myclaw-browser] idle browser session reaped");
        }
    }
}

/// 退出时收掉执行进程(浏览器常驻,不在这里关)。
pub fn stop_browser_worker() {
    if let Ok(mut guard) = WORKER.try_lock() {
        if let Some(mut w) = guard.take() {
            let _ = w.child.start_kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_the_two_tools() {
        let names: Vec<_> = tools()
            .iter()
            .filter_map(|t| t.get("name").and_then(Value::as_str).map(str::to_string))
            .collect();
        assert_eq!(names, ["browser_exec", "browser_screenshot"]);
        assert!(is_browser_tool_name("browser_exec"));
        assert!(!is_browser_tool_name("upload_file"));
    }

    #[test]
    fn ready_python_prefers_the_image_venv_and_needs_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("image");
        let home = dir.path().join("home");
        for v in [&image, &home] {
            std::fs::create_dir_all(v.join("bin")).unwrap();
            std::fs::write(v.join("bin").join("python"), "").unwrap();
        }
        let roots = vec![image.clone(), home.clone()];
        assert_eq!(ready_python(&roots), None, "no .ready = half-installed");
        std::fs::write(home.join(".ready"), "").unwrap();
        assert_eq!(ready_python(&roots), Some(home.join("bin").join("python")));
        std::fs::write(image.join(".ready"), "").unwrap();
        assert_eq!(ready_python(&roots), Some(image.join("bin").join("python")));
    }

    #[test]
    fn normalize_truncates_long_text_and_keeps_images() {
        let long = "x".repeat(MAX_TEXT_CHARS + 10);
        let out = normalize(json!({
            "content": [
                { "type": "text", "text": long },
                { "type": "image", "data": "AAAA", "mimeType": "image/png" }
            ]
        }));
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.ends_with("print less at a time)"));
        assert_eq!(out["content"][1]["data"], "AAAA");
        assert_eq!(normalize(json!({}))["isError"], true);
    }

    #[test]
    fn worker_env_points_at_the_instance_browser() {
        let env: std::collections::HashMap<_, _> =
            worker_env(Path::new("/h/.myclaw-browser/instance"), 9230).into_iter().collect();
        assert_eq!(env["BU_CDP_URL"], "http://127.0.0.1:9230");
        assert_eq!(env["BU_NAME"], "instance");
        assert_eq!(env["BH_AGENT_WORKSPACE"], "/h/.myclaw-browser/instance/workspace");
        assert!(env["TMPDIR"].len() < 40, "chromium unix socket paths must stay short");
    }

    /// 假的执行进程(python 写的最小 MCP 服务器)走一遍握手、转发、变量跨调用保留。
    #[tokio::test]
    async fn worker_round_trip_with_a_fake_server() {
        let Some(py) = resolve_command_on_path("python3") else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("fake.py");
        std::fs::write(
            &script,
            r#"
import json, sys
ns = {}
for line in sys.stdin:
    m = json.loads(line)
    if 'id' not in m: continue
    if m['method'] == 'initialize':
        r = {'protocolVersion': '2025-06-18', 'capabilities': {'tools': {}}, 'serverInfo': {'name': 'f', 'version': '1'}}
    else:
        a = m['params']['arguments']
        exec(a.get('code', ''), ns)
        r = {'content': [{'type': 'text', 'text': str(ns.get('x'))}]}
    print(json.dumps({'jsonrpc': '2.0', 'id': 'noise'}), flush=True)
    print(json.dumps({'jsonrpc': '2.0', 'id': m['id'], 'result': r}), flush=True)
"#,
        )
        .unwrap();
        let mut child = tokio_command(&py)
            .arg(&script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut w = Worker {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            next_id: 0,
            last_used: Instant::now(),
            python: py.clone(),
            port: 9230,
        };
        w.request("initialize", json!({}), Duration::from_secs(10)).await.unwrap();
        w.request("tools/call", json!({"name": "browser_exec", "arguments": {"code": "x = 41"}}), Duration::from_secs(10))
            .await
            .unwrap();
        let r = w
            .request("tools/call", json!({"name": "browser_exec", "arguments": {"code": "x += 1"}}), Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(r["content"][0]["text"], "42");
        assert!(w.alive());
    }
}
