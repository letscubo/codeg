//! `GET /api/myclaw/file` — fork(letscubo)专属:**真流式**读取实例里的一个文件。
//!
//! ## 为什么要它
//!
//! 平台要把会话产物(PPT / Word / 图片 / 日志)交到用户手里,此前只有两条路:
//!
//!   · `/api/myclaw/exec` + `base64` —— 整个文件先进内存、再膨胀 4/3 倍、再进
//!     JSON 字符串,20MB 的 pptx 变成 27MB 的一行字符串,无法续传、无法预览。
//!   · 上游 `POST /api/download_workspace_file` —— 已经是流式,但要求 `root_path`
//!     是一个**已打开的工作区**并过 `ensure_user_navigable_path`(那是给桌面端文件树
//!     的右键菜单用的边界),会话目录不在其中;而且它 POST + JSON、只回
//!     `application/octet-stream`、不认 `Range`、不认条件请求。
//!
//! 这条接口照 cube 沙箱里 envd 的 `/files` 做:一个 GET、路径在 query、body 就是
//! 文件原始字节。2026-09-27 对 C6 的 envd 实测(完整 200 / `Range: bytes=0-99` → 206
//! `content-range: bytes 0-99/20768` / 无 token → 401)确认了那套形态可用,这里把它
//! 补齐了 envd 缺的两项:**主动通告 `Accept-Ranges: bytes`**,并带 `Content-Length`
//! 与 `Last-Modified`(envd 是 chunked、不回 `accept-ranges`)。
//!
//! ## 为什么不自己写 Range 解析
//!
//! 整个响应交给 `tower_http::services::ServeFile`(`fs` feature 已在依赖里,上游的
//! 静态资源 `ServeDir` 就用它)。它按 64KB 块流式读,并且把这些都做了:
//!
//!   · `Range: bytes=a-b` / `bytes=a-` / `bytes=-n` → `206` + `Content-Range`
//!   · 越界 / 解析失败 / 多段(`bytes=0-1,5-6`)→ `416` + `Content-Range: bytes */<size>`
//!   · `Accept-Ranges: bytes`、`Content-Length`、`Last-Modified`
//!   · `If-Modified-Since` / `If-Unmodified-Since` → `304` / `412`
//!   · `Content-Type` 按扩展名 `mime_guess`(pptx/docx/xlsx 都认得,所以浏览器能内联预览)
//!   · `HEAD` 只读 metadata,不开文件
//!
//! 自己写一遍这些就是自己写一遍它的 bug。**已知缺口**:不支持 `If-Range`,断点续传时
//! 若文件在两次请求之间被改写,服务端不会退回整份 —— 会话产物是一次写定的,可接受。
//!
//! ## 两种凭证
//!
//! 这条路由挂在**公共**路由组(不经 `require_token`),自己认凭证 —— 二者任一即可:
//!
//!   · `Authorization: Bearer <codeg token>` —— 平台服务端调用(取产物做缩略图、校验等)。
//!     判据与 `web::auth::require_token` 一致:与本机 token 全等。
//!   · `?t=<票>` —— 浏览器直连。票由平台签(`web/src/lib/codeg/file-ticket.ts`),
//!     **绑死单个文件 + 短时效**,codeg 用自己的 token 当 HMAC 密钥本地验,见
//!     [`super::file_ticket`]。
//!
//! 为什么要票:浏览器给不了 `Authorization` 头的场合恰好全是下载/预览要用的
//! (`<a download>`、`<img src>`、`<video src>`、`window.open`)。要么由平台代理整条流
//! (字节过 Vercel),要么把凭证放进 URL —— 放的必须是票,不能是那条全权 token。
//!
//! ⚠️ **票只认在这个 handler 里**。绝不要把验票逻辑接进 `require_token`:那样一张下载票
//! 就能调 `/api/myclaw/exec`,等于把整台实例交出去。
//!
//! ## 为什么不做路径牢笼
//!
//! codeg 的单 token 本来就等价于容器内全权限(`exec` 能 `cat` 任何文件、`terminal_spawn`
//! 能起 shell),对 Bearer 那条路加 jail 拦不住任何拿到 token 的人,只会让平台取日志 /
//! 取产物时多一层假门。票那条路的边界不在这里 —— 它由签发侧(平台校验 owner)+ 票里那条
//! 路径共同决定,一张票只能读它自己那一个文件。

use std::path::{Path, PathBuf};

use axum::body::Body;
use axum::extract::{Extension, Query, Request};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use tower_http::services::ServeFile;

use super::file_ticket;
use crate::app_error::AppCommandError;
use crate::commands::myclaw_upload::expand_tilde;
use crate::paths::simplify_verbatim_path;

/// 本机 token,由 router 以 Extension 挂在这条路由上。
///
/// 这条路由在公共组里,拿不到 `require_token` 那个闭包捕获的 token,所以显式传进来。
/// 它同时是 Bearer 的比对值和票的 HMAC 密钥。
#[derive(Clone)]
pub struct ServerToken(pub String);

#[derive(Deserialize)]
pub struct DownloadQuery {
    /// 绝对路径(`~` / `~/…` 按 `$HOME` 展开,与 `upload_file` 同一套规则)。
    pub path: String,
    /// `1` / `true` / `yes` → `Content-Disposition: attachment`(浏览器落盘)。
    /// 缺省 `inline`,让平台能把同一条 URL 直接用于预览。
    pub download: Option<String>,
    /// 覆盖交给客户端的文件名;只取末段,带目录的写法会被削掉。
    pub name: Option<String>,
    /// 下载票(浏览器直连时用)。没有 Bearer 就必须有它。
    pub t: Option<String>,
}

pub async fn download(
    Extension(ServerToken(secret)): Extension<ServerToken>,
    Query(q): Query<DownloadQuery>,
    req: Request,
) -> Result<Response, AppCommandError> {
    // 先认凭证,再碰文件系统 —— 未授权的请求连「这个路径存在吗」都不该问出来
    // (否则 404/400 的差别就是一条存在性探测信道)。
    if let Some(rejection) =
        reject_unauthorized(&secret, req.headers(), &q, file_ticket::now_secs())
    {
        return Ok(rejection);
    }

    let target = resolve_target(&q.path).await?;
    let name = display_name(&q.name, &target);
    let disposition = if is_truthy(q.download.as_deref()) {
        "attachment"
    } else {
        "inline"
    };

    // ServeFile 的 single-file 模式完全忽略请求 URI 的路径段(tower-http
    // `ServeVariant::SingleFile` 直接返回 base path),所以路由挂在哪都不影响。
    let mut svc = ServeFile::new(&target);
    let res = svc
        .try_call(req)
        .await
        .map_err(|e| AppCommandError::io_error("failed to read file").with_detail(e.to_string()))?;

    let mut res = res.map(Body::new);
    if let Some(v) = disposition_header(disposition, &name) {
        res.headers_mut().insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(res)
}

// ---------------------------------------------------------------------------
// 鉴权
// ---------------------------------------------------------------------------

/// Bearer 或票,二者任一。`None` = 放行;`Some(resp)` = 拿这个 401 回去。
///
/// 回现成的 `Response` 而不是 `AppCommandError`:后者没有映射到 401 的 code
/// (`AuthenticationFailed` 被上游刻意映到 422,见 handlers/error.rs 的测试)。
fn reject_unauthorized(
    secret: &str,
    headers: &HeaderMap,
    q: &DownloadQuery,
    now: i64,
) -> Option<Response> {
    if secret.is_empty() {
        // 与 auth.rs 同一条 fail-closed:空 token 时 `Bearer ` 会误中。
        return Some((StatusCode::UNAUTHORIZED, "Server token is not configured").into_response());
    }

    if bearer_matches(headers, secret) {
        return None;
    }

    // ⚠️ 不要写成 `… .filter(…)?` —— 在返回 Option<Response> 的函数里,`?` 遇到 None
    // 会 return None,也就是**放行**。没有凭证必须显式拒。
    let Some(ticket) = q.t.as_deref().map(str::trim).filter(|t| !t.is_empty()) else {
        return Some((StatusCode::UNAUTHORIZED, "Invalid or missing token").into_response());
    };
    // 注意:比对的是 query 里的**原始** path 串,展开 `~` / canonicalize 之前。
    match file_ticket::verify_ticket(secret, ticket, &q.path, now) {
        Ok(()) => None,
        Err(e) => {
            tracing::warn!(reason = e.as_str(), "[myclaw/file] download ticket refused");
            Some((StatusCode::UNAUTHORIZED, e.public_message()).into_response())
        }
    }
}

fn bearer_matches(headers: &HeaderMap, secret: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| t == secret)
}

// ---------------------------------------------------------------------------
// Path / name
// ---------------------------------------------------------------------------

async fn resolve_target(raw: &str) -> Result<PathBuf, AppCommandError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(AppCommandError::invalid_input("path must not be empty"));
    }
    let expanded = expand_tilde(raw);
    if !expanded.is_absolute() {
        return Err(AppCommandError::invalid_input(
            "path must be absolute (or start with ~)",
        ));
    }

    let meta = tokio::fs::metadata(&expanded).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppCommandError::not_found(format!("no such file: {}", expanded.display()))
        } else {
            AppCommandError::io_error(format!("cannot read {}", expanded.display()))
                .with_detail(e.to_string())
        }
    })?;
    if meta.is_dir() {
        // 目录压包是上游 `download_workspace_dir` 的活,这条只管单文件。
        return Err(AppCommandError::invalid_input(format!(
            "{} is a directory, not a file",
            expanded.display()
        )));
    }
    if !meta.is_file() {
        return Err(AppCommandError::invalid_input(format!(
            "{} is not a regular file",
            expanded.display()
        )));
    }

    // canonicalize 只为把 `..` / 符号链接落到实处(影响 mime 猜测与日志可读性);
    // 失败时原样用展开后的路径 —— 上一步的 metadata 已经证明它可读。
    Ok(tokio::fs::canonicalize(&expanded)
        .await
        .map(|p| simplify_verbatim_path(&p))
        .unwrap_or(expanded))
}

fn display_name(override_name: &Option<String>, target: &Path) -> String {
    let from_override = override_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        // 只取末段:`?name=../../etc/passwd` 落成 `passwd`,不让调用方用文件名穿目录。
        .and_then(|s| {
            Path::new(s)
                .file_name()
                .and_then(|n| n.to_str())
                .map(str::to_owned)
        });
    from_override.unwrap_or_else(|| {
        target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("download")
            .to_string()
    })
}

fn is_truthy(v: Option<&str>) -> bool {
    matches!(
        v.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// 两段式文件名:ASCII 兜底 + RFC 5987 的 `filename*`,中文名在所有浏览器上都正确落盘。
///
/// 上游 `workspace_files::attachment_header` 是同样的写法,但它私有且写死
/// `attachment`;这里要 `inline`(预览),按 fork 边界在本文件另写一份,不去动上游。
fn disposition_header(kind: &str, name: &str) -> Option<HeaderValue> {
    HeaderValue::from_str(&format!(
        "{kind}; filename=\"{}\"; filename*=UTF-8''{}",
        ascii_fallback(name),
        urlencoding::encode(name)
    ))
    .ok()
}

fn ascii_fallback(name: &str) -> String {
    let out: String = name
        .chars()
        .map(|c| {
            if c.is_control() || c == '"' || c == '\\' || !c.is_ascii() {
                '_'
            } else {
                c
            }
        })
        .collect();
    if out.trim_matches('_').is_empty() {
        "download".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Method, StatusCode};

    struct Tmp(PathBuf);

    impl Tmp {
        fn new(name: &str, bytes: &[u8]) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "codeg-dl-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join(name);
            std::fs::write(&path, bytes).unwrap();
            Self(path)
        }
        fn path(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.parent().unwrap());
        }
    }

    const SECRET: &str = "codeg-token-for-tests";

    /// 带 Bearer 的请求 —— 平台服务端那条路。
    fn req(range: Option<&str>) -> Request {
        let mut b = Request::builder()
            .method(Method::GET)
            .uri("/api/myclaw/file")
            .header(header::AUTHORIZATION, format!("Bearer {SECRET}"));
        if let Some(r) = range {
            b = b.header(header::RANGE, r);
        }
        b.body(Body::empty()).unwrap()
    }

    fn ext() -> Extension<ServerToken> {
        Extension(ServerToken(SECRET.to_string()))
    }

    async fn call(path: &str, flag: Option<&str>, range: Option<&str>) -> Response {
        download_with_name(path, flag, None, range).await
    }

    async fn download_with_name(
        path: &str,
        flag: Option<&str>,
        name: Option<&str>,
        range: Option<&str>,
    ) -> Response {
        super::download(
            ext(),
            Query(DownloadQuery {
                path: path.to_string(),
                download: flag.map(str::to_owned),
                name: name.map(str::to_owned),
                t: None,
            }),
            req(range),
        )
        .await
        .unwrap()
    }

    async fn body_bytes(res: Response) -> Vec<u8> {
        axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn full_get_streams_the_whole_file_with_length_and_range_support() {
        let f = Tmp::new("note.txt", b"hello download");
        let res = call(&f.path(), None, None).await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(res.headers()[header::CONTENT_LENGTH], "14");
        assert!(res.headers().contains_key(header::LAST_MODIFIED));
        assert_eq!(body_bytes(res).await, b"hello download");
    }

    #[tokio::test]
    async fn range_request_returns_206_with_content_range() {
        let f = Tmp::new("blob.bin", b"0123456789");
        let res = call(&f.path(), None, Some("bytes=2-5")).await;

        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(res.headers()[header::CONTENT_RANGE], "bytes 2-5/10");
        assert_eq!(res.headers()[header::CONTENT_LENGTH], "4");
        assert_eq!(body_bytes(res).await, b"2345");
    }

    #[tokio::test]
    async fn suffix_range_returns_the_tail() {
        let f = Tmp::new("blob.bin", b"0123456789");
        let res = call(&f.path(), None, Some("bytes=-3")).await;

        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(res.headers()[header::CONTENT_RANGE], "bytes 7-9/10");
        assert_eq!(body_bytes(res).await, b"789");
    }

    #[tokio::test]
    async fn open_ended_range_runs_to_eof() {
        let f = Tmp::new("blob.bin", b"0123456789");
        let res = call(&f.path(), None, Some("bytes=8-")).await;

        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(res.headers()[header::CONTENT_RANGE], "bytes 8-9/10");
        assert_eq!(body_bytes(res).await, b"89");
    }

    #[tokio::test]
    async fn unsatisfiable_range_is_416_and_reports_the_size() {
        let f = Tmp::new("blob.bin", b"0123456789");
        let res = call(&f.path(), None, Some("bytes=50-60")).await;

        assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(res.headers()[header::CONTENT_RANGE], "bytes */10");
    }

    #[tokio::test]
    async fn multipart_range_is_refused_rather_than_silently_truncated() {
        let f = Tmp::new("blob.bin", b"0123456789");
        let res = call(&f.path(), None, Some("bytes=0-1,5-6")).await;
        assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test]
    async fn content_type_comes_from_the_extension() {
        let f = Tmp::new("deck.pptx", b"PK\x03\x04stub");
        let res = call(&f.path(), None, None).await;
        assert_eq!(
            res.headers()[header::CONTENT_TYPE],
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        );
    }

    #[tokio::test]
    async fn disposition_defaults_to_inline_and_switches_on_download_flag() {
        let f = Tmp::new("note.txt", b"x");
        let inline = call(&f.path(), None, None).await;
        assert!(inline.headers()[header::CONTENT_DISPOSITION]
            .to_str()
            .unwrap()
            .starts_with("inline; filename=\"note.txt\""));

        let attached = call(&f.path(), Some("1"), None).await;
        assert!(attached.headers()[header::CONTENT_DISPOSITION]
            .to_str()
            .unwrap()
            .starts_with("attachment; filename=\"note.txt\""));
    }

    #[tokio::test]
    async fn cjk_name_travels_as_rfc5987_with_an_ascii_fallback() {
        let f = Tmp::new("deck.pptx", b"x");
        let res = download_with_name(&f.path(), Some("true"), Some("杭州西湖.pptx"), None).await;
        let v = res.headers()[header::CONTENT_DISPOSITION].to_str().unwrap();
        assert!(v.contains("filename=\"____.pptx\""), "{v}");
        assert!(v.contains("filename*=UTF-8''%E6%9D%AD%E5%B7%9E"), "{v}");
    }

    #[tokio::test]
    async fn head_reports_the_size_without_a_body() {
        let f = Tmp::new("note.txt", b"hello download");
        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/api/myclaw/file")
            .header(header::AUTHORIZATION, format!("Bearer {SECRET}"))
            .body(Body::empty())
            .unwrap();
        let res = super::download(
            ext(),
            Query(DownloadQuery {
                path: f.path(),
                download: None,
                name: None,
                t: None,
            }),
            req,
        )
        .await
        .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()[header::CONTENT_LENGTH], "14");
        assert!(body_bytes(res).await.is_empty());
    }

    // --- 参数校验 -----------------------------------------------------------

    async fn err(path: &str) -> AppCommandError {
        super::download(
            ext(),
            Query(DownloadQuery {
                path: path.to_string(),
                download: None,
                name: None,
                t: None,
            }),
            req(None),
        )
        .await
        .err()
        .expect("expected an error")
    }

    #[tokio::test]
    async fn missing_file_is_not_found() {
        let e = err("/tmp/codeg-download-does-not-exist-9a5123e3").await;
        assert!(matches!(e.code, crate::app_error::AppErrorCode::NotFound));
    }

    #[tokio::test]
    async fn directory_and_relative_paths_are_rejected() {
        for p in ["/tmp", "relative/note.txt", "   "] {
            let e = err(p).await;
            assert!(
                matches!(e.code, crate::app_error::AppErrorCode::InvalidInput),
                "{p}: {}",
                e.message
            );
        }
    }

    // --- 鉴权 ---------------------------------------------------------------
    //
    // 这条路由在公共组里(浏览器带不了 Authorization 头),所以 401 这几条是它唯一的
    // 防线 —— 一旦松掉就是任意文件读取。

    /// 无凭证的请求连「文件在不在」都不该问出来:必须是 401,不是 404/400。
    async fn anon(path: &str, ticket: Option<&str>) -> Response {
        super::download(
            ext(),
            Query(DownloadQuery {
                path: path.to_string(),
                download: None,
                name: None,
                t: ticket.map(str::to_owned),
            }),
            Request::builder()
                .method(Method::GET)
                .uri("/api/myclaw/file")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap_or_else(|e| e.into_response())
    }

    #[tokio::test]
    async fn without_any_credential_it_is_401_even_for_a_file_that_exists() {
        let f = Tmp::new("note.txt", b"secret bytes");
        let res = anon(&f.path(), None).await;
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert!(body_bytes(res).await != b"secret bytes");
    }

    /// 不存在的路径同样只回 401 —— 否则 401/404 的差别就是存在性探测信道。
    #[tokio::test]
    async fn an_anonymous_probe_cannot_tell_missing_from_present() {
        let present = Tmp::new("note.txt", b"x");
        let a = anon(&present.path(), None).await.status();
        let b = anon("/home/ubuntu/definitely-not-here-9a5123e3", None)
            .await
            .status();
        assert_eq!(a, StatusCode::UNAUTHORIZED);
        assert_eq!(b, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_wrong_bearer_is_401() {
        let f = Tmp::new("note.txt", b"x");
        let res = super::download(
            ext(),
            Query(DownloadQuery {
                path: f.path(),
                download: None,
                name: None,
                t: None,
            }),
            Request::builder()
                .method(Method::GET)
                .uri("/api/myclaw/file")
                .header(header::AUTHORIZATION, "Bearer not-the-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_valid_ticket_downloads_without_any_header() {
        let f = Tmp::new("deck.pptx", b"0123456789");
        let path = f.path();
        let ticket =
            super::file_ticket::sign_ticket(SECRET, &path, super::file_ticket::now_secs() + 900);
        let res = anon(&path, Some(&ticket)).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_bytes(res).await, b"0123456789");
    }

    /// 票绑死单个文件:换个路径就废 —— 这是「一张票 ≠ 全盘读取」的那道闸。
    #[tokio::test]
    async fn a_ticket_does_not_travel_to_another_file() {
        let a = Tmp::new("mine.txt", b"mine");
        let b = Tmp::new("yours.txt", b"yours");
        let ticket = super::file_ticket::sign_ticket(
            SECRET,
            &a.path(),
            super::file_ticket::now_secs() + 900,
        );
        let res = anon(&b.path(), Some(&ticket)).await;
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn an_expired_or_forged_ticket_is_401() {
        let f = Tmp::new("note.txt", b"x");
        let path = f.path();
        let expired =
            super::file_ticket::sign_ticket(SECRET, &path, super::file_ticket::now_secs() - 3600);
        assert_eq!(
            anon(&path, Some(&expired)).await.status(),
            StatusCode::UNAUTHORIZED
        );
        let other_key = super::file_ticket::sign_ticket(
            "another-instance",
            &path,
            super::file_ticket::now_secs() + 900,
        );
        assert_eq!(
            anon(&path, Some(&other_key)).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            anon(&path, Some("garbage")).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }

    /// Range 续传 = 同一条 URL 被请求多次。票必须每次都认。
    #[tokio::test]
    async fn a_ticket_survives_a_ranged_resume() {
        let f = Tmp::new("blob.bin", b"0123456789");
        let path = f.path();
        let ticket =
            super::file_ticket::sign_ticket(SECRET, &path, super::file_ticket::now_secs() + 900);
        for (range, want) in [("bytes=0-4", &b"01234"[..]), ("bytes=5-9", &b"56789"[..])] {
            let res = super::download(
                ext(),
                Query(DownloadQuery {
                    path: path.clone(),
                    download: Some("1".into()),
                    name: None,
                    t: Some(ticket.clone()),
                }),
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/myclaw/file")
                    .header(header::RANGE, range)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT, "{range}");
            assert_eq!(body_bytes(res).await, want, "{range}");
        }
    }

    #[tokio::test]
    async fn an_unconfigured_server_token_fails_closed() {
        let f = Tmp::new("note.txt", b"x");
        let res = super::download(
            Extension(ServerToken(String::new())),
            Query(DownloadQuery {
                path: f.path(),
                download: None,
                name: None,
                t: None,
            }),
            Request::builder()
                .method(Method::GET)
                .uri("/api/myclaw/file")
                .header(header::AUTHORIZATION, "Bearer ")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    // --- 与压缩层叠起来 -----------------------------------------------------
    //
    // 路由挂在顶层 CompressionLayer 之下。`.md` / `.csv` 这类产物落在 `text/*`
    // 允许名单里,一旦被 gzip 就会丢掉 `Content-Length`(tower-http 压缩时会删掉
    // 它),代理端拿不到长度、进度条也就没了。compression.rs 因此对带
    // `Content-Disposition` 的响应一律放行,这里连着真层验一遍。

    fn app() -> axum::Router {
        axum::Router::new()
            .route("/api/myclaw/file", axum::routing::get(super::download))
            .layer(ext())
            .layer(crate::web::compression::compression_layer())
    }

    async fn through_layer(uri: String, range: Option<&str>) -> Response {
        use tower::ServiceExt;
        let mut b = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {SECRET}"))
            .header(header::ACCEPT_ENCODING, "gzip, br");
        if let Some(r) = range {
            b = b.header(header::RANGE, r);
        }
        app().oneshot(b.body(Body::empty()).unwrap()).await.unwrap()
    }

    #[tokio::test]
    async fn text_artifact_keeps_its_length_through_the_compression_layer() {
        let body = "# 报告\n".repeat(200);
        let f = Tmp::new("report.md", body.as_bytes());
        let uri = format!("/api/myclaw/file?path={}", urlencoding::encode(&f.path()));

        let res = through_layer(uri, None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(
            !res.headers().contains_key(header::CONTENT_ENCODING),
            "a named file must not be gzipped: {:?}",
            res.headers()
        );
        assert_eq!(
            res.headers()[header::CONTENT_LENGTH],
            body.len().to_string()
        );
        assert_eq!(body_bytes(res).await, body.as_bytes());
    }

    #[tokio::test]
    async fn range_survives_the_compression_layer_byte_exact() {
        let f = Tmp::new("blob.bin", b"0123456789");
        let uri = format!("/api/myclaw/file?path={}", urlencoding::encode(&f.path()));

        let res = through_layer(uri, Some("bytes=2-5")).await;
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert!(!res.headers().contains_key(header::CONTENT_ENCODING));
        assert_eq!(res.headers()[header::CONTENT_RANGE], "bytes 2-5/10");
        assert_eq!(body_bytes(res).await, b"2345");
    }

    /// 端到端:按平台侧拼 URL 的方式(`URLSearchParams`)走一条真 URI,只带票、不带头。
    /// 路径故意带空格和中文 —— 百分号编解码后必须与签名里的 `p` 逐字符相等,否则 401。
    #[tokio::test]
    async fn a_ticketed_url_round_trips_through_real_query_parsing() {
        let f = Tmp::new("西湖 报告.pptx", b"deck-bytes");
        let path = f.path();
        let ticket =
            super::file_ticket::sign_ticket(SECRET, &path, super::file_ticket::now_secs() + 900);
        let uri = format!(
            "/api/myclaw/file?path={}&t={}&download=1",
            urlencoding::encode(&path),
            urlencoding::encode(&ticket)
        );

        use tower::ServiceExt;
        let res = axum::Router::new()
            .route("/api/myclaw/file", axum::routing::get(super::download))
            .layer(ext())
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let disp = res.headers()[header::CONTENT_DISPOSITION]
            .to_str()
            .unwrap()
            .to_string();
        assert!(disp.starts_with("attachment;"), "{disp}");
        assert!(
            disp.contains("filename*=UTF-8''%E8%A5%BF%E6%B9%96"),
            "{disp}"
        );
        assert_eq!(body_bytes(res).await, b"deck-bytes");
    }

    // --- 纯函数 -------------------------------------------------------------

    #[test]
    fn name_override_cannot_walk_out_of_its_segment() {
        let t = Path::new("/home/ubuntu/deck.pptx");
        assert_eq!(display_name(&None, t), "deck.pptx");
        assert_eq!(display_name(&Some("  ".into()), t), "deck.pptx");
        assert_eq!(display_name(&Some("../../etc/passwd".into()), t), "passwd");
        assert_eq!(display_name(&Some("我的.pptx".into()), t), "我的.pptx");
    }

    #[test]
    fn truthy_only_accepts_affirmative_spellings() {
        for v in ["1", "true", "TRUE", " yes ", "on"] {
            assert!(is_truthy(Some(v)), "{v}");
        }
        for v in ["0", "false", "", "no", "inline"] {
            assert!(!is_truthy(Some(v)), "{v}");
        }
        assert!(!is_truthy(None));
    }

    #[test]
    fn ascii_fallback_never_degrades_to_an_empty_filename() {
        assert_eq!(ascii_fallback("deck.pptx"), "deck.pptx");
        assert_eq!(ascii_fallback("a\"b\\c.txt"), "a_b_c.txt");
        assert_eq!(ascii_fallback("西湖"), "download");
    }
}
