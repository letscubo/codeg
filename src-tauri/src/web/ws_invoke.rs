//! MyClaw fork ext (letscubo) —— 在 `/ws/events` 上调用 HTTP API。
//!
//! ## 为什么
//!
//! 网页终端每敲一批键都是一趟 `POST /api/terminal_write`;经平台转发时每批 2~3s(鉴权 +
//! 查库 + 转发),浏览器直连也要为每批付一次 HTTP 往返。页面本来就连着 `/ws/events`
//! (已用同一 token 鉴过权),在这条连接上发调用即可省掉逐次的 HTTP 开销。
//!
//! ## 怎么做
//!
//! 不另写一套命令表:router 在挂鉴权中间件**之前**把受保护的 `/api/*` 路由留一份
//! (`ApiDispatch`),这里把 `invoke` 合成一个 `POST /<name>` 请求派发进去 —— 同样的
//! handler、同样的报错、同样的 body 限制,与 HTTP 调用逐字等价。public 路由不在其中。
//!
//! ## 顺序
//!
//! 每条 WS 一个队列、一个 worker **按到达顺序串行**执行:终端按键不乱序,也不阻塞
//! 事件推送(worker 与主循环分开)。代价是长耗时调用(`myclaw/exec`、`acp_prompt` 之类)
//! 会挡住后面的调用 —— 这类请继续走 HTTP。
//!
//! 协议:
//! - 客户端 → `{"action":"invoke","request_id":"…","name":"terminal_write","body":{…}}`
//! - 服务端 → `{"type":"invoke_result","request_id":"…","status":200,"ok":true,"data":…}`
//!   `data` = handler 的 JSON 响应体(失败时即错误体);非 JSON 响应体原样作字符串。

use axum::body::Body;
use axum::http::{header, Method, Request};
use axum::Router;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tower::ServiceExt;

use super::ws_attach::ServerMsg;

/// 受保护的 `/api/*` 路由(未挂鉴权层,已挂 AppState 等 Extension)。只由 router 构造。
#[derive(Clone)]
pub struct ApiDispatch(pub Router);

/// 单条 WS 上排队中的调用上限。满了主循环的 send 会等(背压),不丢。
const INVOKE_QUEUE: usize = 256;
/// 响应体上限:与最大的 JSON 响应同量级,防止一次调用把内存打爆。
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub struct InvokeJob {
    pub request_id: String,
    pub name: String,
    pub body: Value,
}

/// `name` 只能是路由路径段:字母数字 `_ - /`,不以 `/` 开头,不含 `..` / `//`。
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('/')
        && !name.contains("..")
        && !name.contains("//")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '/')
}

/// 把一次调用合成 `POST /<name>` 派发进 API 路由,返回 (HTTP 状态码, 响应体 JSON)。
pub async fn dispatch(router: &Router, name: &str, body: Value) -> (u16, Value) {
    let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"null".to_vec());
    let req = match Request::builder()
        .method(Method::POST)
        .uri(format!("/{name}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
    {
        Ok(r) => r,
        Err(e) => return (400, Value::String(format!("invalid invoke request: {e}"))),
    };
    // Router 的 Error = Infallible
    let resp = match router.clone().oneshot(req).await {
        Ok(r) => r,
        Err(never) => match never {},
    };
    let status = resp.status().as_u16();
    let data = match axum::body::to_bytes(resp.into_body(), MAX_RESPONSE_BYTES).await {
        Ok(b) if b.is_empty() => Value::Null,
        Ok(b) => serde_json::from_slice(&b)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into_owned())),
        Err(e) => return (500, Value::String(format!("invoke response body: {e}"))),
    };
    (status, data)
}

/// 起这条 WS 的调用 worker:按到达顺序逐个执行,结果推回 outbound。
pub fn spawn_worker(
    router: Router,
    outbound_tx: mpsc::Sender<ServerMsg>,
) -> (mpsc::Sender<InvokeJob>, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<InvokeJob>(INVOKE_QUEUE);
    let handle = tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let (status, data) = if valid_name(&job.name) {
                dispatch(&router, &job.name, job.body).await
            } else {
                (400, Value::String(format!("invalid invoke name: {}", job.name)))
            };
            let msg = ServerMsg::InvokeResult {
                request_id: job.request_id,
                status,
                ok: (200..300).contains(&status),
                data,
            };
            if outbound_tx.send(msg).await.is_err() {
                break; // WS 已关
            }
        }
    });
    (tx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json};
    use std::sync::{Arc, Mutex};

    #[test]
    fn name_must_be_a_plain_route_path() {
        assert!(valid_name("terminal_write"));
        assert!(valid_name("myclaw/exec"));
        assert!(!valid_name(""));
        assert!(!valid_name("/terminal_write"));
        assert!(!valid_name("../health"));
        assert!(!valid_name("a//b"));
        assert!(!valid_name("a?b=1"));
        assert!(!valid_name("a b"));
    }

    fn echo_router(log: Arc<Mutex<Vec<String>>>) -> Router {
        Router::new()
            .route(
                "/echo",
                post(move |Json(v): Json<Value>| {
                    let log = log.clone();
                    async move {
                        log.lock().unwrap().push(v["k"].as_str().unwrap_or("").to_string());
                        Json(v)
                    }
                }),
            )
            .route(
                "/boom",
                post(|| async { (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"nope"}))) }),
            )
    }

    #[tokio::test]
    async fn dispatch_returns_handler_json_and_status() {
        let r = echo_router(Arc::new(Mutex::new(vec![])));
        let (s, d) = dispatch(&r, "echo", serde_json::json!({"k":"v"})).await;
        assert_eq!(s, 200);
        assert_eq!(d, serde_json::json!({"k":"v"}));
        let (s, d) = dispatch(&r, "boom", Value::Null).await;
        assert_eq!(s, 400);
        assert_eq!(d, serde_json::json!({"error":"nope"}));
        let (s, _) = dispatch(&r, "missing", Value::Null).await;
        assert_eq!(s, 404);
    }

    #[tokio::test]
    async fn worker_runs_in_arrival_order_and_reports_each() {
        let log = Arc::new(Mutex::new(vec![]));
        let (out_tx, mut out_rx) = mpsc::channel::<ServerMsg>(64);
        let (tx, _h) = spawn_worker(echo_router(log.clone()), out_tx);
        for i in 0..20 {
            tx.send(InvokeJob {
                request_id: format!("r{i}"),
                name: "echo".into(),
                body: serde_json::json!({"k": i.to_string()}),
            })
            .await
            .unwrap();
        }
        tx.send(InvokeJob { request_id: "bad".into(), name: "/x".into(), body: Value::Null })
            .await
            .unwrap();
        let mut ids = vec![];
        for _ in 0..21 {
            match out_rx.recv().await.unwrap() {
                ServerMsg::InvokeResult { request_id, status, ok, .. } => {
                    if request_id == "bad" {
                        assert_eq!(status, 400);
                        assert!(!ok);
                    } else {
                        assert!(ok);
                    }
                    ids.push(request_id);
                }
                _ => panic!("unexpected frame"),
            }
        }
        let expected: Vec<String> = (0..20).map(|i| format!("r{i}")).chain(["bad".to_string()]).collect();
        assert_eq!(ids, expected);
        let expected_log: Vec<String> = (0..20).map(|i| i.to_string()).collect();
        assert_eq!(*log.lock().unwrap(), expected_log);
    }
}
