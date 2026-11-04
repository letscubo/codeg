//! Whole-port HTTP/WebSocket proxy. No database, no subprocess, no arbitrary upstream URL.
//! Native MCP prepares local forwarding, then registers with MyClaw. The unchanged
//! Worker forwards its existing X-Preview-Target header; no new Worker protocol.
use axum::{
    body::Body,
    extract::{
        ws::{rejection::WebSocketUpgradeRejection, CloseFrame, Message},
        FromRequestParts, Request, State, WebSocketUpgrade,
    },
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
#[cfg(test)]
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, OnceLock, RwLock},
    time::Duration,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message as UpMessage},
};

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Registration {
    pub port: u16,
    pub capability: String,
    #[serde(skip)]
    host: String,
}
#[derive(Default)]
pub struct PortPreviews {
    entries: RwLock<HashMap<u16, Registration>>,
    prepare_lock: tokio::sync::Mutex<()>,
    control_port: u16,
}
impl PortPreviews {
    pub fn new(control_port: u16) -> Self {
        Self {
            control_port,
            ..Self::default()
        }
    }
}
pub(crate) fn shared() -> Arc<PortPreviews> {
    static STATE: OnceLock<Arc<PortPreviews>> = OnceLock::new();
    STATE
        .get_or_init(|| {
            Arc::new(PortPreviews::new(
                std::env::var("CODEG_PORT")
                    .ok()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(3080),
            ))
        })
        .clone()
}
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("loopback client")
});

pub(crate) fn validate_port(port: u32, control_port: u16) -> Result<u16, &'static str> {
    if (9230..=9330).contains(&port)
        || !(1024..=65535).contains(&port)
        || port == u32::from(control_port)
        || [3080, 18789, 18790, 9222, 9223].contains(&port)
    {
        return Err("Invalid or reserved application port");
    }
    Ok(port as u16)
}
pub(crate) fn valid_host(port: u16, host: &str) -> bool {
    let Some(vm) = host
        .strip_prefix(&format!("p-{port}-"))
        .and_then(|s| s.strip_suffix(".myclaw.build"))
    else {
        return false;
    };
    uuid::Uuid::parse_str(vm).is_ok_and(|id| id.to_string() == vm)
}
pub(crate) async fn prepare(
    state: &PortPreviews,
    port: u32,
    host: String,
) -> Result<Registration, String> {
    let port = validate_port(port, state.control_port)?;
    if !valid_host(port, &host) {
        return Err("Invalid preview host".into());
    }
    let _lock = state.prepare_lock.lock().await;
    {
        let entries = state.entries.read().unwrap_or_else(|e| e.into_inner());
        if entries.len() >= 64 && !entries.contains_key(&port) {
            return Err("Too many registered preview ports".into());
        }
    }
    if !matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            CLIENT.head(format!("http://127.0.0.1:{port}/")).send()
        )
        .await,
        Ok(Ok(_))
    ) {
        return Err("No HTTP service is responding on this port".into());
    }
    if let Some(entry) = state
        .entries
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&port)
    {
        if entry.host != host {
            return Err("Preview identity conflict".into());
        }
        return Ok(entry.clone());
    }
    let registration = Registration {
        port,
        host,
        capability: format!(
            "cp1.{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        ),
    };
    state
        .entries
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(port, registration.clone());
    Ok(registration)
}
/// Runs outside the normal API/CORS router. App /api/* requests stay app requests.
pub async fn worker_proxy(
    State(state): State<Arc<PortPreviews>>,
    req: Request,
    next: Next,
) -> Response {
    let target = req
        .headers()
        .get("x-preview-target")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    // Ordinary Codeg host ingress already uses numeric IP:port in this header.
    if !target.starts_with("cp1.") {
        return next.run(req).await;
    }
    let entry = state
        .entries
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .find(|e| e.capability == target)
        .map(|e| e.clone());
    match entry {
        Some(entry) => forward_registered(entry, req).await,
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn forward_registered(entry: Registration, req: Request) -> Response {
    let (mut parts, body) = req.into_parts();
    let ws = WebSocketUpgrade::from_request_parts(&mut parts, &()).await;
    proxy(entry, ws, Request::from_parts(parts, body)).await
}
#[cfg(test)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterPort {
    port: u32,
    host: String,
}
#[cfg(test)]
async fn register(
    axum::Extension(state): axum::Extension<Arc<PortPreviews>>,
    axum::Json(input): axum::Json<RegisterPort>,
) -> Response {
    match prepare(&state, input.port, input.host).await {
        Ok(registration) => axum::Json(registration).into_response(),
        Err(_) => (StatusCode::BAD_REQUEST, "Invalid preview").into_response(),
    }
}
fn clean_headers(original: &HeaderMap, websocket: bool) -> HeaderMap {
    let mut headers = original.clone();
    let nominated: Vec<String> = original
        .get_all("connection")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(',').map(|v| v.trim().to_ascii_lowercase()))
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "upgrade",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "x-myclaw-preview-cap",
        "x-myclaw-preview-host",
        "x-preview-secret",
        "x-preview-target",
        "forwarded",
    ] {
        headers.remove(name);
    }
    let private: Vec<_> = headers
        .keys()
        .filter(|k| {
            k.as_str().starts_with("x-forwarded-") || k.as_str().starts_with("x-myclaw-preview-")
        })
        .cloned()
        .collect();
    for key in private {
        headers.remove(key);
    }
    if websocket {
        for name in [
            "host",
            "sec-websocket-key",
            "sec-websocket-version",
            "sec-websocket-extensions",
        ] {
            headers.remove(name);
        }
    }
    headers
}
fn rewrite_location(value: &HeaderValue, port: u16, host: &str) -> HeaderValue {
    if let Ok(s) = value.to_str() {
        if let Ok(mut url) = reqwest::Url::parse(&if s.starts_with("//") {
            format!("http:{s}")
        } else {
            s.to_owned()
        }) {
            if matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
                && url.port_or_known_default() == Some(port)
            {
                let _ = url.set_scheme("https");
                let _ = url.set_host(Some(host));
                let _ = url.set_port(None);
                if let Ok(v) = HeaderValue::from_str(url.as_str()) {
                    return v;
                }
            }
        }
    }
    value.clone()
}
async fn proxy(
    entry: Registration,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    req: Request,
) -> Response {
    let port = entry.port;
    // Old Worker forwards the application URI directly. Never trust new/private
    // raw-URI headers from the public request; an ingress must preserve its URI.
    let path = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/")
        .to_owned();
    if let Ok(ws) = ws {
        let mut upstream = match format!("ws://127.0.0.1:{port}{path}").into_client_request() {
            Ok(r) => r,
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        };
        for (name, value) in &clean_headers(req.headers(), true) {
            upstream.headers_mut().append(name, value.clone());
        }
        upstream
            .headers_mut()
            .insert("host", HeaderValue::from_str(&entry.host).unwrap());
        upstream.headers_mut().insert(
            "x-forwarded-host",
            HeaderValue::from_str(&entry.host).unwrap(),
        );
        upstream
            .headers_mut()
            .insert("x-forwarded-proto", HeaderValue::from_static("https"));
        let (up, response) =
            match tokio::time::timeout(Duration::from_secs(5), connect_async(upstream)).await {
                Ok(Ok(v)) => v,
                _ => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        "Preview WebSocket connection failed",
                    )
                        .into_response()
                }
            };
        let selected = response
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let ws = if let Some(protocol) = selected {
            ws.protocols([protocol])
        } else {
            ws
        };
        return ws.on_upgrade(move |down| async move { bridge(down, up).await });
    }
    let mut headers = clean_headers(req.headers(), false);
    headers.insert("host", HeaderValue::from_str(&entry.host).unwrap());
    headers.insert(
        "x-forwarded-host",
        HeaderValue::from_str(&entry.host).unwrap(),
    );
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
    let method = req.method().clone();
    let response = CLIENT
        .request(method, format!("http://127.0.0.1:{port}{path}"))
        .headers(headers)
        .body(reqwest::Body::wrap_stream(
            req.into_body().into_data_stream(),
        ))
        .send()
        .await;
    let upstream = match response {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_GATEWAY, "Preview service unavailable").into_response(),
    };
    let status = upstream.status();
    let mut headers = clean_headers(upstream.headers(), false);
    if let Some(location) = headers.get("location").cloned() {
        headers.insert("location", rewrite_location(&location, port, &entry.host));
    }
    let mut out = Response::new(Body::from_stream(upstream.bytes_stream()));
    *out.status_mut() = status;
    *out.headers_mut() = headers;
    out.headers_mut()
        .insert("x-myclaw-port-preview", HeaderValue::from_static("v1"));
    out
}
async fn bridge(
    down: axum::extract::ws::WebSocket,
    up: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let (mut dt, mut dr) = down.split();
    let (mut ut, mut ur) = up.split();
    let outbound = async {
        while let Some(Ok(msg)) = dr.next().await {
            let closing = matches!(msg, Message::Close(_));
            let msg = match msg {
                Message::Text(v) => UpMessage::Text(v.to_string().into()),
                Message::Binary(v) => UpMessage::Binary(v),
                Message::Ping(v) => UpMessage::Ping(v),
                Message::Pong(v) => UpMessage::Pong(v),
                Message::Close(v) => UpMessage::Close(v.map(|f| {
                    tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: f.code.into(),
                        reason: f.reason.to_string().into(),
                    }
                })),
            };
            if ut.send(msg).await.is_err() || closing {
                break;
            }
        }
    };
    let inbound = async {
        while let Some(Ok(msg)) = ur.next().await {
            let closing = matches!(msg, UpMessage::Close(_));
            let msg = match msg {
                UpMessage::Text(v) => Message::Text(v.to_string().into()),
                UpMessage::Binary(v) => Message::Binary(v),
                UpMessage::Ping(v) => Message::Ping(v),
                UpMessage::Pong(v) => Message::Pong(v),
                UpMessage::Close(v) => Message::Close(v.map(|f| CloseFrame {
                    code: f.code.into(),
                    reason: f.reason.to_string().into(),
                })),
                UpMessage::Frame(_) => continue,
            };
            if dt.send(msg).await.is_err() || closing {
                break;
            }
        }
    };
    tokio::select! {_ = outbound=>{},_ = inbound=>{}}
}

#[cfg(test)]
mod tests;
