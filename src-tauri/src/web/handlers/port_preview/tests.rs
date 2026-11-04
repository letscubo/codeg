// Reviewed database-free tests. Only in-memory routers and ephemeral loopback fake servers.
use super::*;
use axum::{
    extract::ws::WebSocketUpgrade,
    http::Request as HttpRequest,
    routing::{get, post},
};
use axum::{Extension, Router};
use tokio::net::TcpListener;
use tower::ServiceExt;
const VM: &str = "d550dff7-1678-470c-93fd-093e9c09faab";
async fn spawn(router: Router) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (port, task)
}
struct AbortTasks(Vec<tokio::task::JoinHandle<()>>);
impl Drop for AbortTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}
fn test_router(state: Arc<PortPreviews>) -> Router {
    let api = Router::new()
        .route("/api/port_preview/register", post(register))
        .layer(Extension(state.clone()))
        .layer(axum::middleware::from_fn(|req, next| {
            crate::web::auth::require_token(req, next, "master-test".to_owned())
        }));
    api.layer(axum::middleware::from_fn_with_state(state, worker_proxy))
}
async fn register_test(router: &Router, port: u16) -> (String, String) {
    let host = format!("p-{port}-{VM}.myclaw.build");
    let response = router
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/api/port_preview/register")
                .header("authorization", "Bearer master-test")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"port":port,"host":host}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let data: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    (host, data["capability"].as_str().unwrap().to_owned())
}
#[test]
fn ports_and_hosts_are_scoped() {
    assert!(validate_port(36221, 3080).is_ok());
    for p in [0, 80, 3080, 18789, 18790, 9222, 9223, 9230, 9330, 65536] {
        assert!(validate_port(p, 3080).is_err());
    }
    assert!(validate_port(4200, 4200).is_err());
    assert!(valid_host(36221, &format!("p-36221-{VM}.myclaw.build")));
    assert!(!valid_host(36221, &format!("p-3000-{VM}.myclaw.build")));
    assert!(!valid_host(36221, "36221-anything.myclaw.build"));
    assert!(serde_json::from_value::<RegisterPort>(
        serde_json::json!({"port":3000,"host":"x","path":"/"})
    )
    .is_err());
}
#[tokio::test]
async fn auth_repeated_registration_and_restart() {
    let (port, task) = spawn(Router::new().fallback(|| async { "ok" })).await;
    let _tasks = AbortTasks(vec![task]);
    let state = Arc::new(PortPreviews::new(3080));
    let router = test_router(state);
    let unauthorized = router
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/api/port_preview/register")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let (host, cap) = register_test(&router, port).await;
    assert_eq!(
        register_test(&router, port).await,
        (host.clone(), cap.clone())
    );
    for (h, c) in [
        (host.clone(), "cp1.wrong".to_owned()),
        (host.clone(), "cp1.invalid".to_owned()),
    ] {
        let r = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/")
                    .header("x-myclaw-preview-host", h)
                    .header("x-preview-target", c)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), 404);
    }
    let restarted = test_router(Arc::new(PortPreviews::new(3080)));
    let r = restarted
        .oneshot(
            HttpRequest::builder()
                .uri("/")
                .header("x-myclaw-preview-host", host)
                .header("x-preview-target", cap)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}
#[tokio::test]
async fn http_preserves_path_range_headers_and_stream() {
    let fake = Router::new().fallback(|req: Request| async move {
        if req.method() == "HEAD" {
            return StatusCode::METHOD_NOT_ALLOWED.into_response();
        }
        assert_eq!(
            req.uri().path_and_query().unwrap().as_str(),
            "/a%2Fb?x=1&x=2"
        );
        assert_eq!(req.headers()["range"], "bytes=2-4");
        assert_eq!(req.headers()["cookie"], "app=x");
        assert_eq!(req.headers()["authorization"], "Basic app");
        assert!(!req.headers().contains_key("x-preview-target"));
        assert!(!req.headers().contains_key("x-leak"));
        assert_eq!(req.headers()["x-forwarded-proto"], "https");
        let bytes = axum::body::to_bytes(req.into_body(), 100).await.unwrap();
        assert_eq!(&bytes[..], b"hello");
        (
            StatusCode::PARTIAL_CONTENT,
            [
                ("content-range", "bytes 2-4/9"),
                ("set-cookie", "session=x; Path=/"),
                ("content-type", "video/mp4"),
            ],
            "234",
        )
            .into_response()
    });
    let (port, task) = spawn(fake).await;
    let _tasks = AbortTasks(vec![task]);
    let router = test_router(Arc::new(PortPreviews::new(3080)));
    let (host, cap) = register_test(&router, port).await;
    let r = router
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/a%2Fb?x=1&x=2")
                .header("x-preview-target", cap)
                .header("x-myclaw-preview-host", host)
                .header("range", "bytes=2-4")
                .header("cookie", "app=x")
                .header("authorization", "Basic app")
                .header("connection", "x-leak")
                .header("x-leak", "secret")
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), 206);
    assert_eq!(r.headers()["content-range"], "bytes 2-4/9");
    assert_eq!(r.headers()["set-cookie"], "session=x; Path=/");
    assert_eq!(r.headers()["x-myclaw-port-preview"], "v1");
    assert_eq!(
        &axum::body::to_bytes(r.into_body(), 100).await.unwrap()[..],
        b"234"
    );
}
#[tokio::test]
async fn websocket_text_binary_and_subprotocol() {
    let fake = Router::new()
        .route(
            "/socket",
            get(|ws: WebSocketUpgrade| async {
                ws.protocols(["chat"]).on_upgrade(|mut socket| async move {
                    while let Some(Ok(msg)) = socket.recv().await {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                })
            }),
        )
        .route("/", get(|| async { "ok" }));
    let (port, task) = spawn(fake).await;
    let router = test_router(Arc::new(PortPreviews::new(3080)));
    let (host, cap) = register_test(&router, port).await;
    let (proxy_port, proxy_task) = spawn(router).await;
    let _tasks = AbortTasks(vec![task, proxy_task]);
    let mut req = format!("ws://127.0.0.1:{proxy_port}/socket?q=1")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("x-preview-target", cap.parse().unwrap());
    req.headers_mut()
        .insert("x-myclaw-preview-host", host.parse().unwrap());
    req.headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static("chat"));
    let (mut client, response) = connect_async(req).await.unwrap();
    assert_eq!(response.headers()["sec-websocket-protocol"], "chat");
    for msg in [
        UpMessage::Text("你好 preview".into()),
        UpMessage::Binary(vec![0, 1, 255].into()),
    ] {
        client.send(msg.clone()).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result, msg);
    }
    client.close(None).await.unwrap();
}
#[test]
fn redirects_preserve_public_origin() {
    let host = format!("p-36221-{VM}.myclaw.build");
    assert_eq!(
        rewrite_location(
            &HeaderValue::from_static("http://127.0.0.1:36221/login?q=1#f"),
            36221,
            &host
        )
        .to_str()
        .unwrap(),
        format!("https://{host}/login?q=1#f")
    );
    assert_eq!(
        rewrite_location(&HeaderValue::from_static("/login"), 36221, &host),
        "/login"
    );
    assert_eq!(
        rewrite_location(
            &HeaderValue::from_static("https://example.com/login"),
            36221,
            &host
        ),
        "https://example.com/login"
    );
}

#[tokio::test]
async fn encoded_paths_are_not_decoded_by_router() {
    let (port, task) =
        spawn(Router::new().fallback(|req: Request| async move { req.uri().to_string() })).await;
    let _tasks = AbortTasks(vec![task]);
    let router = test_router(Arc::new(PortPreviews::new(3080)));
    let (host, cap) = register_test(&router, port).await;
    for path in [
        "/file%FF.bin?q=%FE",
        "/a%2Fb?q=1&q=2",
        "/%E4%B8%AD%E6%96%87",
    ] {
        let response = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri(path)
                    .header("x-preview-target", &cap)
                    .header("x-myclaw-preview-host", &host)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            &axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap()[..],
            path.as_bytes()
        );
    }
}

#[tokio::test]
async fn legacy_worker_uses_actual_uri_and_strips_private_headers() {
    let (port, task) = spawn(Router::new().fallback(|req: Request| async move {
        assert!(!req.headers().contains_key("x-preview-target"));
        assert!(!req.headers().contains_key("x-preview-secret"));
        assert!(!req.headers().contains_key("x-myclaw-preview-uri"));
        req.uri().to_string()
    }))
    .await;
    let _tasks = AbortTasks(vec![task]);
    let router = test_router(Arc::new(PortPreviews::new(3080)));
    let (_, cap) = register_test(&router, port).await;
    let response = router
        .oneshot(
            HttpRequest::builder()
                .uri("/api/app?q=1")
                .header("x-preview-target", cap)
                .header("x-preview-secret", "global-worker-secret")
                .header("x-myclaw-preview-uri", "/spoofed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        &axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap()[..],
        b"/api/app?q=1"
    );
}

#[tokio::test]
async fn native_preview_is_embedded_and_rejects_extra_arguments() {
    use crate::acp::delegation::companion::{CompanionFeatures, TOOL_SCHEMA_JSON};
    let schema: serde_json::Value = serde_json::from_str(TOOL_SCHEMA_JSON).unwrap();
    let tool = schema
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "expose_preview")
        .expect("native tool must exist");
    assert_eq!(tool["inputSchema"]["required"], serde_json::json!(["port"]));
    assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    assert!(CompanionFeatures::parse(Some("platform")).allows_tool("expose_preview"));
    assert!(!CompanionFeatures::parse(Some("uploads")).allows_tool("expose_preview"));
}

#[tokio::test]
async fn native_flow_prepares_before_registration_and_verifies_before_return() {
    use crate::commands::myclaw_preview::{expose_with, parse_arguments, PreviewPublisher};
    use std::sync::Mutex;
    struct Publisher {
        events: Mutex<Vec<&'static str>>,
        fail: bool,
    }
    #[async_trait::async_trait]
    impl PreviewPublisher for Publisher {
        async fn register(&self, p: &Registration) -> Result<String, String> {
            self.events.lock().unwrap().push("register");
            assert!(p.capability.starts_with("cp1."));
            assert!(valid_host(p.port, &p.host));
            if self.fail {
                return Err("registration failed".into());
            }
            Ok(format!("https://{}", p.host))
        }
        async fn verify(&self, _url: &str) -> Result<(), String> {
            self.events.lock().unwrap().push("verify");
            Ok(())
        }
    }
    let (port, task) = spawn(Router::new().fallback(|| async { "application" })).await;
    let _tasks = AbortTasks(vec![task]);
    let state = PortPreviews::new(3080);
    let publisher = Publisher {
        events: Mutex::new(vec![]),
        fail: false,
    };
    let result = expose_with(&state, VM, serde_json::json!({"port":port}), &publisher)
        .await
        .unwrap();
    let body: serde_json::Value =
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(body["url"], format!("https://p-{port}-{VM}.myclaw.build"));
    assert_eq!(
        *publisher.events.lock().unwrap(),
        vec!["register", "verify"]
    );
    let failed = Publisher {
        events: Mutex::new(vec![]),
        fail: true,
    };
    assert!(
        expose_with(&state, VM, serde_json::json!({"port":port}), &failed)
            .await
            .is_err()
    );
    assert_eq!(*failed.events.lock().unwrap(), vec!["register"]);
    for args in [
        serde_json::json!({"port":"3000"}),
        serde_json::json!({"port":3000,"path":"/"}),
        serde_json::json!({"port":3080}),
    ] {
        assert!(parse_arguments(args).is_err());
    }
}

#[tokio::test]
async fn http_companion_uses_native_handler_and_never_platform_tools_call() {
    use crate::commands::myclaw_upload::ArtifactUploadAccess;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Fake(AtomicUsize);
    #[async_trait::async_trait]
    impl ArtifactUploadAccess for Fake {
        async fn upload(&self, _: &str) -> Result<String, String> {
            panic!("not upload")
        }
        async fn platform_call(&self, _: &str, _: Value, _: Value) -> Result<Value, String> {
            panic!("must not call platform tools")
        }
        async fn expose_preview(&self, args: Value) -> Result<Value, String> {
            assert_eq!(args, json!({"port":36221}));
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"content":[{"type":"text","text":"native result"}]}))
        }
    }
    let fake = Fake(AtomicUsize::new(0));
    let result =
        crate::web::handlers::myclaw::mcp::preview_call_for_test(&fake, json!({"port":36221}))
            .await;
    assert_eq!(result["result"]["content"][0]["text"], "native result");
    let rejected = crate::web::handlers::myclaw::mcp::preview_call_for_test(
        &fake,
        json!({"port":36221,"path":"/"}),
    )
    .await;
    assert_eq!(rejected["error"]["code"], -32602);
    assert_eq!(fake.0.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn stdio_companion_dispatches_native_preview_and_validates_arguments() {
    use crate::acp::delegation::companion::{
        dispatch_line, CompanionContext, CompanionFeatures, InflightCalls, LineAction,
    };
    use serde_json::json;
    let ctx = CompanionContext {
        parent_connection_id: "test".into(),
        socket_path: "/nonexistent/native-preview-test.sock".into(),
        token: "test-token".into(),
        features: CompanionFeatures::parse(Some("platform")),
        custom_agents: vec![],
        disabled_agents: vec![],
    };
    let invalid = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"expose_preview","arguments":{"port":36221,"path":"/"}}});
    match dispatch_line(&ctx, Arc::new(InflightCalls::new()), &invalid.to_string()).await {
        LineAction::Respond(r) => assert_eq!(r.error.unwrap().code, -32602),
        _ => panic!("must reject before broker I/O"),
    };
    let valid = json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"expose_preview","arguments":{"port":36221}}});
    assert!(matches!(
        dispatch_line(&ctx, Arc::new(InflightCalls::new()), &valid.to_string()).await,
        LineAction::Spawn(_)
    ));
}
