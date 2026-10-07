//! MyClaw fork ext (letscubo) —— agent 浏览器的实时画面与接管。
//!
//! MyClaw 的浏览器应用给每个 agent 起一个自己的 headless chromium(启动脚本见 MyClaw 仓库
//! `apps/myclaw-browser/skills/myclaw-browser/bin/browser-mcp.sh`),调试端口记在
//! `~/.myclaw-browser/<agent>/port`。页面想看 agent 正在浏览器里干什么、必要时自己伸手点
//! (登录、验证码),就连这条 `/ws/browser?agent=<id>`:
//!
//! ```text
//! 网页 ──WS(本路由,与 /ws/events 同一把 token)──> codeg ──CDP──> 127.0.0.1:<port>
//!   ← frame / tabs / status                          ← Page.screencastFrame
//!   → mouse / wheel / key / text / select / navigate  → Input.dispatch* / Target.* / Page.navigate
//!   → resize / close_tab / close_browser               → Browser.setWindowBounds / Target.closeTarget / Browser.close
//!
//! 标签页的网站图标:定时读 `/json/list` 的 faviconUrl,在容器里下载成 data: 地址推给页面
//! (`{"type":"icon","targetId","data"}`;这页没有图标时 `data` 为 null,页面据此清掉旧图标)。
//!
//! 浏览器不在(从没用过 / 被关掉)时页面在地址栏输网址:用 codeg 自带的启动脚本
//! (`browser_launch.sh`,与应用脚本同一套约定)把这个 agent 的浏览器起起来,再在新标签页里打开 ——
//! 和装没装浏览器应用无关;
//! 浏览器在但一个标签页都没有时同样开新标签页。
//! ```
//!
//! 画面用 `Page.startScreencast`:页面不动就不出帧,出一帧回一个 ack(不回就停推)。
//! 2026-10-05 在 C6(2 核)实测:滚动约 10 帧/秒、1280×800 质量 60 每帧 30–120KB、
//! 容器内延迟 30–45ms、推画面本身只多占 ~16MB。
//!
//! 跟哪个标签页:默认跟**最新打开的页面**(agent 多半在那上面干活),页面可以用 `select`
//! 指定。跟上之后 `Page.bringToFront` 一次 —— headless 下不在前台的页面不合成,一帧都不出
//! (和 harness 截图首次超时是同一个原因)。
//!
//! 窗口大小跟着面板:用户在页面上改完悬浮窗大小,发一条 `resize`(想要的网页可视区,
//! CSS 像素),这里把 agent 浏览器的**窗口**改成对应大小(`Browser.setWindowBounds`),网页随之
//! 重新排版 —— 和真浏览器一样,而不是把一张固定大小的画面放大缩小。上限 1280×800(截图更大
//! 会让模型费用变高)。所有标签页在同一个窗口里,新开的页面也是这个大小;agent 看到的就是这个
//! 大小。改完记进 `~/.myclaw-browser/<agent>/window-size`(窗口外框,浏览器重启时启动脚本按它
//! 开窗口)和 `view-size`(想要的可视区)。窗口比可视区多出来的那圈不是定值(`--no-sandbox` 的
//! 警告条就占 56px,C9 实测),所以面板每次连上、出第一帧时对一次 `view-size`,差了就再改一次。
//! 坐标仍由页面按帧里带的 `w`/`h`(CSS 像素)换算。
//!
//! 鉴权与 `/ws/events` 完全一致(挂同一个 `require_token`)。能连上这里的人本来就能让 agent
//! 做任何事,接管浏览器没有扩大暴露面。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::Query;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message as CdpMessage;

/// 调试端口的合法范围:启动脚本从 9230 起挑(9222 留给手工调试)。只连这个范围里的本机端口,
/// 端口文件被改写也连不到别的服务上去。
const PORT_RANGE: std::ops::RangeInclusive<u16> = 9222..=9400;
const SCREENCAST: &str =
    r#"{"format":"jpeg","quality":60,"maxWidth":1280,"maxHeight":800,"everyNthFrame":1}"#;
/// 浏览器还没起来(agent 没用过浏览器)时多久再看一眼。
const WAIT_BROWSER: Duration = Duration::from_secs(2);
/// 刚连上时逐个问标签页的等待上限 —— 卡死的页面不回话,不能让面板一直等它。
const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
/// 启动脚本起浏览器的等待上限:脚本自己最多等 25 秒,再给排队拿锁留点余量。
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(45);
/// 网页可视区的上下限(CSS 像素)。上限与截图费用挂钩,下限再小网页就没法用了。
const VIEW_MIN: (f64, f64) = (320.0, 200.0);
const VIEW_MAX: (f64, f64) = (1280.0, 800.0);

#[derive(Deserialize)]
pub struct BrowserQuery {
    agent: String,
}

pub async fn ws_browser_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<BrowserQuery>,
) -> impl IntoResponse {
    ws.protocols([super::auth::WS_EVENT_PROTOCOL])
        .on_upgrade(move |socket| run(socket, q.agent))
}

/// agent id 只允许 uuid 一类的字符 —— 它要拼进文件路径。
pub(crate) fn valid_agent_id(agent: &str) -> bool {
    !agent.is_empty()
        && agent.len() <= 64
        && agent
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn agent_file(agent: &str, name: &str) -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(
        PathBuf::from(home)
            .join(".myclaw-browser")
            .join(agent)
            .join(name),
    )
}

fn port_file(agent: &str) -> Option<PathBuf> {
    agent_file(agent, "port")
}

/// 隔多久看一次各标签页的网站图标
const ICON_SCAN_EVERY: Duration = Duration::from_secs(3);
/// 一次最多新下载几个图标、每个最大多少字节(大图标多半不是 favicon,不要)
const ICON_FETCH_PER_SCAN: usize = 8;
const ICON_MAX_BYTES: usize = 100 * 1024;

struct IconScan {
    /// 标签页 → 它此刻的网站图标地址
    by_target: Vec<(String, String)>,
    /// 这次新下载的:图标地址 → data: 地址(下不到为 None,记下来不再重试)
    fetched: Vec<(String, Option<String>)>,
}

async fn scan_icons(port: u16, known: HashSet<String>) -> IconScan {
    let mut out = IconScan {
        by_target: Vec::new(),
        fetched: Vec::new(),
    };
    let Ok(local) = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .no_proxy()
        .build()
    else {
        return out;
    };
    let Ok(resp) = local
        .get(format!("http://127.0.0.1:{port}/json/list"))
        .send()
        .await
    else {
        return out;
    };
    let list: Vec<Value> = resp.json().await.unwrap_or_default();
    for t in &list {
        if t.get("type").and_then(Value::as_str) != Some("page") {
            continue;
        }
        let Some(id) = t.get("id").and_then(Value::as_str) else {
            continue;
        };
        // 没有 faviconUrl = 这页没有图标(或还没加载出来):记成空串,好让页面清掉旧图标
        let icon = t.get("faviconUrl").and_then(Value::as_str).unwrap_or("");
        out.by_target.push((id.to_string(), icon.to_string()));
    }
    let Ok(web) = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0 Safari/537.36")
        .build()
    else {
        return out;
    };
    let mut wanted: Vec<String> = out
        .by_target
        .iter()
        .map(|(_, u)| u.clone())
        .filter(|u| !u.is_empty() && !known.contains(u))
        .collect();
    wanted.sort();
    wanted.dedup();
    for url in wanted.into_iter().take(ICON_FETCH_PER_SCAN) {
        let data = fetch_icon(&web, &url).await;
        out.fetched.push((url, data));
    }
    out
}

/// 图标地址 → data: 地址。只要图片,太大的不要。
async fn fetch_icon(client: &reqwest::Client, url: &str) -> Option<String> {
    use base64::Engine as _;
    if url.starts_with("data:image/") {
        return (url.len() <= ICON_MAX_BYTES * 2).then(|| url.to_string());
    }
    if !allowed_url(url) {
        return None;
    }
    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let header_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        });
    let bytes = resp.bytes().await.ok()?;
    if bytes.is_empty() || bytes.len() > ICON_MAX_BYTES {
        return None;
    }
    let mime = icon_mime(header_type.as_deref(), url)?;
    Some(format!(
        "data:{mime};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    ))
}

/// 图标的类型:响应头说是图片就信它;没说或说成二进制流就按扩展名猜;说是别的(网页等)就不要。
pub(crate) fn icon_mime(header_type: Option<&str>, url: &str) -> Option<String> {
    match header_type {
        Some(t) if t.starts_with("image/") => return Some(t.to_string()),
        Some(t) if !t.is_empty() && t != "application/octet-stream" => return None,
        _ => {}
    }
    let path = url
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let mime = if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".gif") {
        "image/gif"
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        "image/jpeg"
    } else if path.ends_with(".webp") {
        "image/webp"
    } else {
        "image/x-icon"
    };
    Some(mime.to_string())
}

/// 浏览器还没起来时页面发来的消息:只认地址栏的 `navigate`(http/https),返回网址。
pub(crate) fn navigate_url(text: &str) -> Option<String> {
    let v: Value = serde_json::from_str(text).ok()?;
    if v.get("type").and_then(Value::as_str) != Some("navigate") {
        return None;
    }
    v.get("url")
        .and_then(Value::as_str)
        .filter(|u| allowed_url(u))
        .map(str::to_string)
}

/// codeg 自带的启动脚本(见 `browser_launch.sh`):和浏览器应用的启动脚本同一套目录、端口、锁,
/// 所以不装应用也能打开,装了应用 agent 在对话里用的也是同一个浏览器。
const LAUNCH_SCRIPT: &str = include_str!("browser_launch.sh");

/// 把这个 agent 的浏览器起起来(同一份资料、同一个端口、记住的窗口大小)。
async fn launch_browser(agent: &str) -> Result<(), String> {
    let run = tokio::process::Command::new("bash")
        .arg("-c")
        .arg(LAUNCH_SCRIPT)
        .arg("myclaw-browser-launch")
        .env("MYCLAW_AGENT_ID", agent)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    let out = tokio::time::timeout(LAUNCH_TIMEOUT, run)
        .await
        .map_err(|_| "starting the browser timed out".to_string())?
        .map_err(|e| format!("cannot run the browser start script: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    let err = String::from_utf8_lossy(&out.stderr);
    let line = err
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("");
    Err(if line.is_empty() {
        format!(
            "the browser did not start (exit {})",
            out.status.code().unwrap_or(-1)
        )
    } else {
        line.trim().to_string()
    })
}

/// `view-size` 文件("宽,高")→ 可视区。
pub(crate) fn parse_view(text: &str) -> Option<(f64, f64)> {
    let (w, h) = text.trim().split_once(',')?;
    clamp_view(w.trim().parse().ok()?, h.trim().parse().ok()?)
}

/// 想要的网页可视区 → 夹到上下限内、取整。
pub(crate) fn clamp_view(w: f64, h: f64) -> Option<(f64, f64)> {
    if !w.is_finite() || !h.is_finite() || w <= 0.0 || h <= 0.0 {
        return None;
    }
    Some((
        w.clamp(VIEW_MIN.0, VIEW_MAX.0).round(),
        h.clamp(VIEW_MIN.1, VIEW_MAX.1).round(),
    ))
}

/// 可视区 → 窗口大小。窗口比可视区多出来的那圈(headless 下宽 0、高 143,版本不同可能不同)
/// 按改之前的「窗口 − 可视区」现算,不写死。
pub(crate) fn window_for_view(
    view: (f64, f64),
    bounds: (f64, f64),
    current_view: (f64, f64),
) -> (u32, u32) {
    let extra_w = (bounds.0 - current_view.0).max(0.0);
    let extra_h = (bounds.1 - current_view.1).max(0.0);
    (
        (view.0 + extra_w).round() as u32,
        (view.1 + extra_h).round() as u32,
    )
}

/// 端口文件 → 端口。只认范围内的数字。
pub(crate) fn parse_port(text: &str) -> Option<u16> {
    text.trim()
        .parse::<u16>()
        .ok()
        .filter(|p| PORT_RANGE.contains(p))
}

async fn browser_ws_url(port: u16) -> Option<String> {
    let url = format!("http://127.0.0.1:{port}/json/version");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .no_proxy()
        .build()
        .ok()?;
    let v: Value = client.get(url).send().await.ok()?.json().await.ok()?;
    v.get("webSocketDebuggerUrl")?.as_str().map(str::to_string)
}

async fn send_json(socket: &mut WebSocket, v: Value) -> bool {
    socket
        .send(Message::Text(v.to_string().into()))
        .await
        .is_ok()
}

async fn run(mut socket: WebSocket, agent: String) {
    if !valid_agent_id(&agent) {
        let _ = send_json(
            &mut socket,
            json!({"type": "status", "state": "error", "message": "invalid agent id"}),
        )
        .await;
        return;
    }
    // 等浏览器出现:agent 第一次用浏览器之前端口文件不存在;用户在面板里关掉浏览器后端口
    // 文件还在、浏览器不在。页面开着面板等就行 —— 或者直接在地址栏输网址:这里用浏览器应用
    // 的启动脚本(`--launch`)把这个 agent 的浏览器起起来,连上后在新标签页里打开它。
    let mut announced = false;
    let mut pending_url: Option<String> = None;
    let (port, ws_url) = loop {
        let port = match port_file(&agent) {
            Some(path) => tokio::fs::read_to_string(path)
                .await
                .ok()
                .and_then(|t| parse_port(&t)),
            None => None,
        };
        if let Some(port) = port {
            if let Some(url) = browser_ws_url(port).await {
                break (port, url);
            }
        }
        if !announced {
            // 有端口文件 = 开过、后来没了(被关掉 / 崩了 / 实例重启);没有 = 从没用过
            let reason = if port.is_some() { "closed" } else { "never" };
            if !send_json(
                &mut socket,
                json!({"type": "status", "state": "no_browser", "reason": reason}),
            )
            .await
            {
                return;
            }
            announced = true;
        }
        let url = tokio::select! {
            _ = tokio::time::sleep(WAIT_BROWSER) => None,
            msg = socket.recv() => match msg {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(Message::Text(text))) => navigate_url(&text),
                _ => None,
            },
        };
        if let Some(url) = url {
            let _ = send_json(&mut socket, json!({"type": "status", "state": "launching"})).await;
            match launch_browser(&agent).await {
                // 起来了下一圈就连上(连不上的话重新告诉页面「没有浏览器」)
                Ok(()) => {
                    pending_url = Some(url);
                    announced = false;
                }
                // 没起来:报错留在页面上,不再发「没有浏览器」把它冲掉;页面可以再输网址重试
                Err(message) => {
                    let _ = send_json(
                        &mut socket,
                        json!({"type": "status", "state": "error", "message": message}),
                    )
                    .await;
                }
            }
        }
    };

    let (cdp, _) = match tokio_tungstenite::connect_async(ws_url.as_str()).await {
        Ok(c) => c,
        Err(e) => {
            let _ = send_json(
                &mut socket,
                json!({"type": "status", "state": "error", "message": format!("cdp: {e}")}),
            )
            .await;
            return;
        }
    };
    let mut bridge = Bridge::new(cdp, agent);
    // 先挑 agent 正在用的那页,再开始盯新标签页 —— 顺序反过来的话,已有的每个标签页都会
    // 被当成「新开的」挨个跟一遍,最后停在哪页全看上报顺序
    bridge.pick_initial().await;
    bridge
        .call("Target.setDiscoverTargets", json!({"discover": true}), None)
        .await;
    let _ = send_json(&mut socket, json!({"type": "status", "state": "connected"})).await;
    if let Some(url) = pending_url {
        // 新标签页打开;它一出现就被当成「新开的页面」跟过去
        bridge
            .call("Target.createTarget", json!({"url": url}), None)
            .await;
    }

    // 标签页的网站图标:每隔几秒问一次浏览器(`/json/list` 带 faviconUrl,CDP 事件里没有),
    // 新出现的图标在容器里下载、转成 data: 地址推给页面 —— 页面不直接去各网站拉:有的是
    // http、有的防盗链,容器里打开的内网 / 本机地址用户那边也根本连不到。
    let mut icon_tick = tokio::time::interval(ICON_SCAN_EVERY);
    let (icon_tx, mut icon_rx) = tokio::sync::mpsc::channel::<IconScan>(2);
    let mut icon_scanning = false;
    let mut favicon_of: HashMap<String, String> = HashMap::new();
    let mut icons: HashMap<String, Option<String>> = HashMap::new();

    loop {
        tokio::select! {
            _ = icon_tick.tick(), if !icon_scanning => {
                icon_scanning = true;
                let known: HashSet<String> = icons.keys().cloned().collect();
                let tx = icon_tx.clone();
                tokio::spawn(async move {
                    let _ = tx.send(scan_icons(port, known).await).await;
                });
            }
            Some(scan) = icon_rx.recv() => {
                icon_scanning = false;
                let fresh: HashSet<String> = scan.fetched.iter().map(|(u, _)| u.clone()).collect();
                icons.extend(scan.fetched);
                for (target, url) in scan.by_target {
                    let changed = favicon_of.get(&target) != Some(&url) || fresh.contains(&url);
                    if !changed {
                        continue;
                    }
                    // 没有图标 / 下不到也要说:同一个标签页跳到别的网站后,不能还挂着上一个网站的
                    // 图标(2026-10-06 C9:example.org 页挂着之前那页的 Google 图标)
                    let data = icons.get(&url).cloned().flatten();
                    let msg = json!({"type": "icon", "targetId": target, "data": data});
                    if !send_json(&mut socket, msg).await {
                        bridge.stop().await;
                        return;
                    }
                    favicon_of.insert(target, url);
                }
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        bridge.handle_client(v).await;
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            },
            msg = bridge.cdp.next() => match msg {
                Some(Ok(CdpMessage::Text(text))) => {
                    let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                    for out in bridge.handle_cdp(v).await {
                        if !send_json(&mut socket, out).await {
                            bridge.stop().await;
                            return;
                        }
                    }
                }
                Some(Ok(_)) => {}
                // 浏览器没了(被杀 / 崩了):告诉页面,页面重连时会重新等浏览器
                Some(Err(_)) | None => {
                    let _ = send_json(&mut socket, json!({"type": "status", "state": "browser_gone"})).await;
                    return;
                }
            },
        }
    }
    bridge.stop().await;
}

type CdpStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
struct Tab {
    id: String,
    url: String,
    title: String,
}

struct Bridge {
    cdp: CdpStream,
    agent: String,
    /// 用户在面板上定的可视区(`view-size`);连上后第一帧对一次,不对就改回来
    desired_view: Option<(f64, f64)>,
    healed: bool,
    next_id: u64,
    /// 打开的页面,按出现先后
    tabs: Vec<Tab>,
    /// 当前在看的页面与它的 CDP session
    current: Option<(String, String)>,
    /// 想看的页面。attach 是异步的:连上时已有好几个标签页会接连触发跟随,回包回来时
    /// 不是它的就立刻 detach,免得后台标签页一直推帧
    want: Option<String>,
    /// 用户手动选过 —— 之后新开的页面不再自动跟过去
    pinned: bool,
    /// 等回包的请求:id → 用途
    pending: HashMap<u64, Pending>,
    /// 最近一帧的视口尺寸(CSS 像素),输入坐标按它换算
    viewport: (f64, f64),
}

enum Pending {
    Attach(String),
    /// 问窗口位置是为了改大小:想要的网页可视区
    Resize(f64, f64),
    Ignore,
}

impl Bridge {
    fn new(cdp: CdpStream, agent: String) -> Self {
        let desired_view = agent_file(&agent, "view-size")
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|t| parse_view(&t));
        Self {
            cdp,
            agent,
            desired_view,
            healed: false,
            next_id: 0,
            tabs: Vec::new(),
            current: None,
            want: None,
            pinned: false,
            pending: HashMap::new(),
            viewport: (0.0, 0.0),
        }
    }

    /// 发一个请求并等它的回包(只在开始盯事件之前用:这期间来的事件直接丢掉)。
    async fn rpc(&mut self, method: &str, params: Value, session: Option<&str>) -> Option<Value> {
        let id = self.call(method, params, session).await;
        self.pending.remove(&id);
        let wait = async {
            while let Some(Ok(msg)) = self.cdp.next().await {
                let CdpMessage::Text(text) = msg else {
                    continue;
                };
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                if v.get("id").and_then(Value::as_u64) == Some(id) {
                    return v.get("result").cloned();
                }
            }
            None
        };
        tokio::time::timeout(PROBE_TIMEOUT, wait)
            .await
            .ok()
            .flatten()
    }

    /// 刚连上时挑哪页:**最近一次跳转**最晚的那页,就是 agent 正在用的。
    ///
    /// 「最后上报的标签页」「有没有别人连着」都靠不住(2026-10-05 预览站实测:面板停在上一轮
    /// 留下、已经卡死的 BBC 页;残留的 harness 进程也会连着别的页)。页面的
    /// `performance.timeOrigin` 就是它最近一次导航的时刻;卡死的页面不回话,超时跳过。
    async fn pick_initial(&mut self) {
        let Some(targets) = self.rpc("Target.getTargets", json!({}), None).await else {
            return;
        };
        let mut best: Option<(f64, String)> = None;
        for info in targets
            .get("targetInfos")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            if info.get("type").and_then(Value::as_str) != Some("page") {
                continue;
            }
            let tab = tab_of(&info);
            if tab.id.is_empty() {
                continue;
            }
            self.tabs.push(tab.clone());
            let Some(att) = self
                .rpc(
                    "Target.attachToTarget",
                    json!({"targetId": tab.id, "flatten": true}),
                    None,
                )
                .await
            else {
                continue;
            };
            let Some(session) = att
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            let origin = self
                .rpc(
                    "Runtime.evaluate",
                    json!({"expression": "performance.timeOrigin", "returnByValue": true}),
                    Some(&session),
                )
                .await
                .and_then(|r| r.pointer("/result/value").and_then(Value::as_f64));
            self.call(
                "Target.detachFromTarget",
                json!({"sessionId": session}),
                None,
            )
            .await;
            if let Some(origin) = origin {
                if best.as_ref().is_none_or(|(b, _)| origin > *b) {
                    best = Some((origin, tab.id.clone()));
                }
            }
        }
        let pick = best
            .map(|(_, id)| id)
            .or_else(|| self.tabs.last().map(|t| t.id.clone()));
        if let Some(id) = pick {
            self.follow(&id).await;
        }
    }

    async fn call(&mut self, method: &str, params: Value, session: Option<&str>) -> u64 {
        self.call_for(method, params, session, Pending::Ignore)
            .await
    }

    async fn call_for(
        &mut self,
        method: &str,
        params: Value,
        session: Option<&str>,
        purpose: Pending,
    ) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        let mut body = json!({"id": id, "method": method, "params": params});
        if let Some(s) = session {
            body["sessionId"] = json!(s);
        }
        self.pending.insert(id, purpose);
        let _ = self
            .cdp
            .send(CdpMessage::Text(body.to_string().into()))
            .await;
        id
    }

    fn session(&self) -> Option<String> {
        self.current.as_ref().map(|(_, s)| s.clone())
    }

    fn tabs_msg(&self) -> Value {
        let current = self.current.as_ref().map(|(t, _)| t.as_str());
        json!({
            "type": "tabs",
            "current": current,
            "tabs": self.tabs.iter().map(|t| json!({"id": t.id, "url": t.url, "title": t.title})).collect::<Vec<_>>(),
        })
    }

    async fn follow(&mut self, target_id: &str) {
        self.want = Some(target_id.to_string());
        if self.current.as_ref().is_some_and(|(t, _)| t == target_id) {
            return;
        }
        if let Some(session) = self.session() {
            self.call("Page.stopScreencast", json!({}), Some(&session))
                .await;
            self.call(
                "Target.detachFromTarget",
                json!({"sessionId": session}),
                None,
            )
            .await;
        }
        self.current = None;
        self.call_for(
            "Target.attachToTarget",
            json!({"targetId": target_id, "flatten": true}),
            None,
            Pending::Attach(target_id.to_string()),
        )
        .await;
    }

    async fn stop(&mut self) {
        if let Some(session) = self.session() {
            self.call("Page.stopScreencast", json!({}), Some(&session))
                .await;
            self.call(
                "Target.detachFromTarget",
                json!({"sessionId": session}),
                None,
            )
            .await;
        }
        let _ = self.cdp.close(None).await;
    }

    /// CDP 来的一条消息 → 要推给页面的消息(可能没有)。
    async fn handle_cdp(&mut self, v: Value) -> Vec<Value> {
        let mut out = Vec::new();
        if let Some(id) = v.get("id").and_then(Value::as_u64) {
            if let Some(Pending::Resize(w, h)) = self.pending.get(&id) {
                let view = (*w, *h);
                self.pending.remove(&id);
                self.apply_resize(&v, view).await;
                return out;
            }
            if let Some(Pending::Attach(target)) = self.pending.remove(&id) {
                if let Some(session) = v.pointer("/result/sessionId").and_then(Value::as_str) {
                    let session = session.to_string();
                    // 回包回来时已经想看别的页面了(连上时接连触发的跟随):这个不要了
                    if self.want.as_deref() != Some(target.as_str()) {
                        self.call(
                            "Target.detachFromTarget",
                            json!({"sessionId": session}),
                            None,
                        )
                        .await;
                        return out;
                    }
                    self.current = Some((target, session.clone()));
                    self.call("Page.enable", json!({}), Some(&session)).await;
                    self.call("Page.bringToFront", json!({}), Some(&session))
                        .await;
                    let params: Value = serde_json::from_str(SCREENCAST).unwrap_or_default();
                    self.call("Page.startScreencast", params, Some(&session))
                        .await;
                    out.push(self.tabs_msg());
                }
            }
            return out;
        }
        let method = v.get("method").and_then(Value::as_str).unwrap_or("");
        let params = v.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "Page.screencastFrame" => {
                let session = v
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if let Some(frame_session) = params.get("sessionId").and_then(Value::as_u64) {
                    self.call(
                        "Page.screencastFrameAck",
                        json!({"sessionId": frame_session}),
                        Some(&session),
                    )
                    .await;
                }
                if self.session().as_deref() != Some(session.as_str()) {
                    return out;
                }
                let meta = params.get("metadata").cloned().unwrap_or(Value::Null);
                let w = meta
                    .get("deviceWidth")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                let h = meta
                    .get("deviceHeight")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                self.viewport = (w, h);
                if !self.healed && w > 0.0 && h > 0.0 {
                    self.healed = true;
                    self.heal_view().await;
                }
                out.push(json!({"type": "frame", "data": params.get("data"), "w": w, "h": h}));
            }
            "Target.targetCreated" | "Target.targetInfoChanged" => {
                let info = params.get("targetInfo").cloned().unwrap_or(Value::Null);
                if info.get("type").and_then(Value::as_str) != Some("page") {
                    return out;
                }
                let tab = tab_of(&info);
                if tab.id.is_empty() {
                    return out;
                }
                let old_url = self
                    .tabs
                    .iter()
                    .find(|t| t.id == tab.id)
                    .map(|t| t.url.clone());
                match self.tabs.iter_mut().find(|t| t.id == tab.id) {
                    Some(existing) => *existing = tab.clone(),
                    None => self.tabs.push(tab.clone()),
                }
                // 跟着 agent 走:新开的页面,或者**刚跳转过的页面**。agent 常在已有的
                // 标签页里直接 goto_url,只跟「最新打开的」会停在一个它早就不用的页面上
                // (2026-10-05 预览站实测:面板停在上一轮的 BBC 页,agent 在 GitHub 那页干活)。
                // 只认地址变化,标题变化不算 —— 后台页面改标题不该把画面抢走。
                // 用户手动选过就都不跟了。
                if !self.pinned && should_follow(old_url.as_deref(), &tab.url) {
                    self.follow(&tab.id).await;
                } else if self.current.is_none() && self.want.is_none() {
                    // 刚连上时一个都没挑出来(全都不回话):先随便看一个
                    self.follow(&tab.id).await;
                }
                out.push(self.tabs_msg());
            }
            "Target.targetDestroyed" => {
                let id = params.get("targetId").and_then(Value::as_str).unwrap_or("");
                self.tabs.retain(|t| t.id != id);
                if self.current.as_ref().is_some_and(|(t, _)| t == id) {
                    self.current = None;
                    self.pinned = false;
                    if let Some(last) = self.tabs.last().map(|t| t.id.clone()) {
                        self.follow(&last).await;
                    }
                }
                out.push(self.tabs_msg());
            }
            _ => {}
        }
        out
    }

    /// 当前可视区和用户定的差了(浏览器重启后窗口外框变了)就再改一次。
    async fn heal_view(&mut self) {
        let Some(view) = self.desired_view else {
            return;
        };
        if (view.0 - self.viewport.0).abs() <= 2.0 && (view.1 - self.viewport.1).abs() <= 2.0 {
            return;
        }
        let Some((target, _)) = self.current.clone() else {
            return;
        };
        self.call_for(
            "Browser.getWindowForTarget",
            json!({"targetId": target}),
            None,
            Pending::Resize(view.0, view.1),
        )
        .await;
    }

    /// `Browser.getWindowForTarget` 回来了:按「窗口 − 可视区」算出新窗口大小,改掉并记下来。
    async fn apply_resize(&mut self, reply: &Value, view: (f64, f64)) {
        let Some(window_id) = reply.pointer("/result/windowId").and_then(Value::as_u64) else {
            return;
        };
        let num = |k: &str| {
            reply
                .pointer(&format!("/result/bounds/{k}"))
                .and_then(Value::as_f64)
        };
        let (Some(bw), Some(bh)) = (num("width"), num("height")) else {
            return;
        };
        let (w, h) = window_for_view(view, (bw, bh), self.viewport);
        self.call(
            "Browser.setWindowBounds",
            json!({"windowId": window_id, "bounds": {"windowState": "normal"}}),
            None,
        )
        .await;
        self.call(
            "Browser.setWindowBounds",
            json!({"windowId": window_id, "bounds": {"width": w, "height": h}}),
            None,
        )
        .await;
        // 浏览器重启后按这个大小开窗口(启动脚本读);写不进去不影响这次
        if let Some(path) = agent_file(&self.agent, "window-size") {
            let _ = tokio::fs::write(path, format!("{w},{h}\n")).await;
        }
        if let Some(path) = agent_file(&self.agent, "view-size") {
            let _ = tokio::fs::write(path, format!("{},{}\n", view.0, view.1)).await;
        }
        self.desired_view = Some(view);
    }

    /// 页面发来的一条消息 → CDP 调用。坐标是 CSS 像素(页面按帧的 w/h 换算好)。
    async fn handle_client(&mut self, v: Value) {
        let kind = v.get("type").and_then(Value::as_str).unwrap_or("");
        if kind == "resize" {
            let num = |k: &str| v.get(k).and_then(Value::as_f64).unwrap_or(0.0);
            // 还没出过画面就不知道窗口比可视区多多少,不改
            let (Some(view), Some((target, _))) =
                (clamp_view(num("w"), num("h")), self.current.clone())
            else {
                return;
            };
            if self.viewport.0 <= 0.0 || self.viewport.1 <= 0.0 {
                return;
            }
            self.call_for(
                "Browser.getWindowForTarget",
                json!({"targetId": target}),
                None,
                Pending::Resize(view.0, view.1),
            )
            .await;
            return;
        }
        if kind == "select" {
            if let Some(id) = v.get("targetId").and_then(Value::as_str) {
                if self.tabs.iter().any(|t| t.id == id) {
                    self.pinned = true;
                    self.follow(id).await;
                }
            }
            return;
        }
        if kind == "close_tab" {
            // 只关这个浏览器里认得的页面;关的是正在看的,targetDestroyed 会换到别的页
            if let Some(id) = v.get("targetId").and_then(Value::as_str) {
                if self.tabs.iter().any(|t| t.id == id) {
                    let id = id.to_string();
                    self.call("Target.closeTarget", json!({"targetId": id}), None)
                        .await;
                }
            }
            return;
        }
        if kind == "close_browser" {
            // 整个浏览器退出(省内存 / 卡死时重来)。CDP 连接随之断开,页面收到 browser_gone
            // 后重连等待;agent 下次用浏览器时启动脚本按同一份资料重开,登录状态还在。
            // 先把标签页一个个关掉:直接 Browser.close 的话下次启动会把它们全恢复回来
            // (C9 实测;先关光再退出,重开只剩一个空白页)
            let ids: Vec<String> = self.tabs.iter().map(|t| t.id.clone()).collect();
            for id in ids {
                self.call("Target.closeTarget", json!({"targetId": id}), None)
                    .await;
            }
            // 关标签页是异步的,等它们真的关掉再退出,否则会话里还记着它们
            tokio::time::sleep(Duration::from_millis(600)).await;
            self.call("Browser.close", json!({}), None).await;
            return;
        }
        if kind == "follow_latest" {
            self.pinned = false;
            if let Some(last) = self.tabs.last().map(|t| t.id.clone()) {
                self.follow(&last).await;
            }
            return;
        }
        let Some(session) = self.session() else {
            // 一个标签页都没有(最后一个被关掉了):在地址栏输网址就开一个新的
            if kind == "navigate" {
                if let Some(url) = v
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|u| allowed_url(u))
                {
                    self.call("Target.createTarget", json!({"url": url}), None)
                        .await;
                }
            }
            return;
        };
        let num = |k: &str| v.get(k).and_then(Value::as_f64).unwrap_or(0.0);
        let (w, h) = self.viewport;
        let clamp = |x: f64, max: f64| {
            if max > 0.0 {
                x.clamp(0.0, max)
            } else {
                x.max(0.0)
            }
        };
        match kind {
            "mouse" => {
                let event = v.get("event").and_then(Value::as_str).unwrap_or("");
                if !matches!(event, "mousePressed" | "mouseReleased" | "mouseMoved") {
                    return;
                }
                let button = v.get("button").and_then(Value::as_str).unwrap_or("left");
                let params = json!({
                    "type": event,
                    "x": clamp(num("x"), w),
                    "y": clamp(num("y"), h),
                    "button": if event == "mouseMoved" { "none" } else { button },
                    "clickCount": v.get("clickCount").and_then(Value::as_u64).unwrap_or(1),
                    "modifiers": v.get("modifiers").and_then(Value::as_u64).unwrap_or(0),
                });
                self.call("Input.dispatchMouseEvent", params, Some(&session))
                    .await;
            }
            "wheel" => {
                let params = json!({
                    "type": "mouseWheel",
                    "x": clamp(num("x"), w),
                    "y": clamp(num("y"), h),
                    "deltaX": num("dx"),
                    "deltaY": num("dy"),
                });
                self.call("Input.dispatchMouseEvent", params, Some(&session))
                    .await;
            }
            "key" => {
                let event = v.get("event").and_then(Value::as_str).unwrap_or("");
                if !matches!(event, "keyDown" | "keyUp" | "rawKeyDown" | "char") {
                    return;
                }
                let mut params = json!({"type": event, "modifiers": v.get("modifiers").and_then(Value::as_u64).unwrap_or(0)});
                for k in ["key", "code", "text"] {
                    if let Some(s) = v.get(k).and_then(Value::as_str) {
                        params[k] = json!(s);
                    }
                }
                if let Some(n) = v.get("keyCode").and_then(Value::as_u64) {
                    params["windowsVirtualKeyCode"] = json!(n);
                }
                self.call("Input.dispatchKeyEvent", params, Some(&session))
                    .await;
            }
            "text" => {
                if let Some(text) = v.get("text").and_then(Value::as_str) {
                    self.call("Input.insertText", json!({"text": text}), Some(&session))
                        .await;
                }
            }
            "navigate" => {
                // 只放行 http(s),防止页面把浏览器带去 file:// 读容器里的文件
                if let Some(url) = v
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|u| allowed_url(u))
                {
                    self.call("Page.navigate", json!({"url": url}), Some(&session))
                        .await;
                }
            }
            "back" | "forward" => {
                let expr = if kind == "back" {
                    "history.back()"
                } else {
                    "history.forward()"
                };
                self.call(
                    "Runtime.evaluate",
                    json!({"expression": expr}),
                    Some(&session),
                )
                .await;
            }
            "reload" => {
                self.call("Page.reload", json!({}), Some(&session)).await;
            }
            _ => {}
        }
    }
}

fn tab_of(info: &Value) -> Tab {
    let field = |k: &str| {
        info.get(k)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    Tab {
        id: field("targetId"),
        url: field("url"),
        title: field("title"),
    }
}

/// 页面的地址变化要不要把画面跟过去:新出现的页面要跟;已有页面只在地址真的变了时跟。
pub(crate) fn should_follow(old_url: Option<&str>, new_url: &str) -> bool {
    match old_url {
        None => true,
        Some(old) => old != new_url && !new_url.is_empty(),
    }
}

pub(crate) fn allowed_url(url: &str) -> bool {
    let lower = url.trim().to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_ids_that_could_escape_the_directory_are_rejected() {
        assert!(valid_agent_id("051dcf22-3c81-4a4c-ade8-e268a2173c60"));
        assert!(!valid_agent_id("../root"));
        assert!(!valid_agent_id("a/b"));
        assert!(!valid_agent_id(""));
        assert!(!valid_agent_id(&"a".repeat(65)));
    }

    #[test]
    fn only_debugging_ports_in_range_are_used() {
        assert_eq!(parse_port("9230\n"), Some(9230));
        assert_eq!(parse_port("22"), None, "端口文件被改写也连不到别的服务");
        assert_eq!(parse_port("9401"), None);
        assert_eq!(parse_port("abc"), None);
    }

    #[test]
    fn the_view_follows_new_tabs_and_tabs_that_just_navigated() {
        assert!(should_follow(None, "about:blank"), "新开的页面");
        assert!(
            should_follow(Some("https://a.test/"), "https://b.test/"),
            "agent 在老标签页里跳转"
        );
        assert!(
            !should_follow(Some("https://a.test/"), "https://a.test/"),
            "只改了标题"
        );
        assert!(!should_follow(Some("https://a.test/"), ""));
    }

    #[test]
    fn the_view_size_is_kept_within_limits() {
        assert_eq!(clamp_view(1000.4, 600.6), Some((1000.0, 601.0)));
        assert_eq!(
            clamp_view(5000.0, 5000.0),
            Some((1280.0, 800.0)),
            "上限 1280×800"
        );
        assert_eq!(clamp_view(10.0, 10.0), Some((320.0, 200.0)));
        assert_eq!(clamp_view(0.0, 600.0), None);
        assert_eq!(clamp_view(f64::NAN, 600.0), None);
    }

    #[test]
    fn the_saved_view_size_is_read_back_within_limits() {
        assert_eq!(parse_view("1280,729\n"), Some((1280.0, 729.0)));
        assert_eq!(parse_view(" 900 , 600 "), Some((900.0, 600.0)));
        assert_eq!(parse_view("9999,9999"), Some((1280.0, 800.0)));
        assert_eq!(parse_view("abc"), None);
        assert_eq!(parse_view(""), None);
    }

    #[test]
    fn the_window_adds_what_it_had_beyond_the_view() {
        // C9 实测:窗口 780×580 时可视区 780×437(headless 顶上多 143)
        assert_eq!(
            window_for_view((1000.0, 507.0), (780.0, 580.0), (780.0, 437.0)),
            (1000, 650)
        );
        assert_eq!(
            window_for_view((800.0, 500.0), (800.0, 500.0), (800.0, 500.0)),
            (800, 500),
            "没有多出来的就原样"
        );
    }

    #[test]
    fn only_address_bar_navigation_starts_a_closed_browser() {
        assert_eq!(
            navigate_url(r#"{"type":"navigate","url":"https://example.com"}"#),
            Some("https://example.com".to_string())
        );
        assert_eq!(
            navigate_url(r#"{"type":"navigate","url":"file:///etc/passwd"}"#),
            None
        );
        assert_eq!(navigate_url(r#"{"type":"reload"}"#), None);
        assert_eq!(navigate_url("not json"), None);
    }

    #[test]
    fn site_icons_are_accepted_only_as_images() {
        assert_eq!(
            icon_mime(Some("image/png"), "https://a.test/x"),
            Some("image/png".into())
        );
        assert_eq!(
            icon_mime(Some("text/html"), "https://a.test/favicon.ico"),
            None,
            "被重定向到网页"
        );
        assert_eq!(
            icon_mime(None, "https://a.test/favicon.ico?v=2"),
            Some("image/x-icon".into())
        );
        assert_eq!(
            icon_mime(Some("application/octet-stream"), "https://a.test/i.svg"),
            Some("image/svg+xml".into())
        );
    }

    #[test]
    fn navigation_is_limited_to_web_pages() {
        assert!(allowed_url("https://example.com"));
        assert!(allowed_url("HTTP://example.com"));
        assert!(!allowed_url("file:///etc/passwd"));
        assert!(!allowed_url("javascript:alert(1)"));
        assert!(!allowed_url("chrome://settings"));
    }
}
