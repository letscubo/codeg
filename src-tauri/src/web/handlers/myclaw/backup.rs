//! `POST /api/myclaw/backup` · `GET /api/myclaw/backup` · `POST /api/myclaw/backup/cancel`
//! —— fork(letscubo)专属:把整个家目录(默认 `$HOME`,即 `/home/ubuntu`)备份到平台给的存储。
//!
//! ## 为什么不用上游的 `commands/backup`
//!
//! 上游那套只打包 codeg 自己的数据(`~/.codeg` 下的白名单分区),产物让用户自己下载。
//! 平台要的是**整个家目录**(各 agent 的 home、工作区、装过的东西、codeg 数据),并且
//! 直接传到平台的存储。只借用它的数据库快照(`VACUUM INTO`,见 `snapshot_db_to`)。
//!
//! ## 流程(异步:POST 立即返回,GET 轮询,完成后签名通知平台)
//!
//! 1. **快照**:`VACUUM INTO` 把 codeg.db 拍成一致的副本(运行中直接拷 sqlite 可能不一致)。
//!    包里 codeg.db 的位置放这份副本,活的 `codeg.db` / `-wal` / `-shm` 不进包。
//! 2. **打包**:单独一条低优先级线程(nice 10)遍历家目录,流式写 tar.gz。
//! 3. **切片上传**:压缩流按 `partBytes` 切成一片片临时文件,写满一片就 PUT 到平台给的
//!    第 i 个预签名地址,传完即删。有界通道让磁盘上最多同时留两三片 —— 不用先占一整份
//!    磁盘,也不受对象存储单次 PUT 5GB 的上限。还原 = 按序拼接各片再 `tar xz`。
//! 4. **清单**:最后把清单(每片大小 / sha256、文件数、跳过的文件)PUT 到 `manifestUrl`。
//! 5. **通知**:带签名 `POST /api/codeg/backup`(尽力而为;平台也可以只轮询 GET)。
//!
//! ## 容错
//!
//! - 读不了的文件(权限、遍历时被删)、socket / fifo / 设备文件:跳过并计数,不让整次失败。
//! - 遍历时文件变短:按开始时的大小补零,保证 tar 结构不坏;变长:只取开始时的大小。
//! - 每片上传失败重试 3 次;仍失败整次记 failed。
//! - 同一时刻只跑一个备份(第二个 POST 回 409)。任务态在进程内存里,codeg 重启即丢 ——
//!   平台按「查不到 = 没跑完」处理并重投。
//!
//! ## 维护标记
//!
//! 跑备份期间 [`maintenance`] 返回 `Some("backup")`:`/api/myclaw/activity` 据此告诉平台
//! 「正在维护,别暂停」,codeg 自己的闲时自动升级也不在这时重启。
//!
//! 鉴权:与 `/api/myclaw/exec` 同,在受保护路由组内(单 token = 容器全权限)。

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use axum::{Extension, Json};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::app_error::AppCommandError;
use crate::app_state::AppState;

/// 每片默认 256 MiB。
const DEFAULT_PART_BYTES: u64 = 256 * 1024 * 1024;
const MIN_PART_BYTES: u64 = 1024 * 1024;
const MAX_PART_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// 默认压缩级别:3 —— 比默认的 6 快不少,体积只大一点(大头是视频 / 模型这类已压缩文件)。
const DEFAULT_LEVEL: u32 = 3;
const MAX_PARTS: usize = 1000;
const PART_UPLOAD_ATTEMPTS: u32 = 3;
const PART_UPLOAD_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const NOTIFY_PATH: &str = "/api/codeg/backup";
const NOTIFY_TIMEOUT: Duration = Duration::from_secs(15);
/// 清单里最多列几个被跳过的路径(只为排查,不是完整清单)。
const MAX_SKIPPED_SAMPLES: usize = 20;
pub const MANIFEST_FORMAT: &str = "codeg-home-backup/1";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartRequest {
    /// 平台侧这次备份的 id,原样带回(状态、清单、通知里都有)。
    pub backup_id: String,
    /// 预签名的 PUT 地址,第 i 片用第 i 个。片数超过地址数 = 失败(地址给少了)。
    pub part_urls: Vec<String>,
    /// 清单的预签名 PUT 地址。
    pub manifest_url: String,
    #[serde(default)]
    pub part_bytes: Option<u64>,
    /// 要备份的目录,默认 `$HOME`。必须是已存在的绝对路径。
    #[serde(default)]
    pub root: Option<String>,
    /// 相对 `root` 的路径,命中的目录 / 文件整个跳过。默认不排除。
    #[serde(default)]
    pub exclude: Vec<String>,
    /// gzip 级别 1–9,默认 3。
    #[serde(default)]
    pub level: Option<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PartInfo {
    pub index: usize,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    pub backup_id: String,
    /// `running` | `succeeded` | `failed` | `cancelled`
    pub state: &'static str,
    /// `snapshot` | `archiving` | `manifest` | `done`
    pub phase: &'static str,
    pub root: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    /// 已打进包的文件数(含目录、符号链接)。
    pub files: u64,
    /// 跳过的条目数(读不了的文件、socket 等)。
    pub skipped: u64,
    pub skipped_samples: Vec<String>,
    /// 打进包的原始字节数。
    pub bytes_in: u64,
    /// 已上传的压缩后字节数(各片之和)。
    pub bytes_out: u64,
    pub parts: Vec<PartInfo>,
    pub error: Option<String>,
    pub codeg_version: &'static str,
}

impl BackupStatus {
    fn new(backup_id: &str, root: &Path) -> Self {
        Self {
            backup_id: backup_id.to_string(),
            state: "running",
            phase: "snapshot",
            root: root.to_string_lossy().into_owned(),
            started_at: chrono::Utc::now().to_rfc3339(),
            finished_at: None,
            files: 0,
            skipped: 0,
            skipped_samples: Vec::new(),
            bytes_in: 0,
            bytes_out: 0,
            parts: Vec::new(),
            error: None,
            codeg_version: env!("CARGO_PKG_VERSION"),
        }
    }

    fn note_skipped(&mut self, path: &Path, why: &str) {
        self.skipped += 1;
        if self.skipped_samples.len() < MAX_SKIPPED_SAMPLES {
            self.skipped_samples
                .push(format!("{}: {why}", path.to_string_lossy()));
        }
    }
}

type SharedStatus = Arc<Mutex<BackupStatus>>;

struct Job {
    status: SharedStatus,
    cancel: CancellationToken,
}

/// 当前 / 上一次备份。进程内存,codeg 重启即丢。
static JOB: LazyLock<Mutex<Option<Job>>> = LazyLock::new(|| Mutex::new(None));

/// 正在备份时返回 `Some("backup")`(给 activity 与自动升级用)。
pub fn maintenance() -> Option<&'static str> {
    let job = JOB.lock().unwrap();
    match job.as_ref() {
        Some(j) if j.status.lock().unwrap().state == "running" => Some("backup"),
        _ => None,
    }
}

/// 校验后的请求。
#[derive(Debug)]
struct Plan {
    backup_id: String,
    part_urls: Vec<String>,
    manifest_url: String,
    part_bytes: u64,
    root: PathBuf,
    exclude: Vec<String>,
    level: u32,
}

fn is_http_url(u: &str) -> bool {
    reqwest::Url::parse(u)
        .map(|p| matches!(p.scheme(), "https" | "http") && p.host_str().is_some())
        .unwrap_or(false)
}

/// 相对路径、不含 `..` / 绝对前缀。
fn clean_relative(p: &str) -> Option<PathBuf> {
    let trimmed = p.trim().trim_start_matches("./");
    if trimmed.is_empty() {
        return None;
    }
    let path = Path::new(trimmed);
    if path
        .components()
        .all(|c| matches!(c, Component::Normal(_)))
    {
        Some(path.to_path_buf())
    } else {
        None
    }
}

fn validate(req: StartRequest, default_root: &Path) -> Result<Plan, AppCommandError> {
    let id = req.backup_id.trim();
    if id.is_empty()
        || id.len() > 128
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(AppCommandError::invalid_input(
            "backupId must be 1-128 chars of [A-Za-z0-9_-]",
        ));
    }
    if req.part_urls.is_empty() || req.part_urls.len() > MAX_PARTS {
        return Err(AppCommandError::invalid_input(format!(
            "partUrls must hold 1-{MAX_PARTS} upload URLs"
        )));
    }
    if !req.part_urls.iter().all(|u| is_http_url(u)) || !is_http_url(&req.manifest_url) {
        return Err(AppCommandError::invalid_input(
            "partUrls / manifestUrl must be http(s) URLs",
        ));
    }
    let part_bytes = req.part_bytes.unwrap_or(DEFAULT_PART_BYTES);
    if !(MIN_PART_BYTES..=MAX_PART_BYTES).contains(&part_bytes) {
        return Err(AppCommandError::invalid_input(format!(
            "partBytes must be between {MIN_PART_BYTES} and {MAX_PART_BYTES}"
        )));
    }
    let level = req.level.unwrap_or(DEFAULT_LEVEL);
    if !(1..=9).contains(&level) {
        return Err(AppCommandError::invalid_input("level must be 1-9"));
    }
    let root = match req.root.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(r) => PathBuf::from(r),
        None => default_root.to_path_buf(),
    };
    if !root.is_absolute() || !root.is_dir() {
        return Err(AppCommandError::invalid_input(
            "root must be an existing absolute directory",
        ));
    }
    let mut exclude = Vec::with_capacity(req.exclude.len());
    for e in &req.exclude {
        let Some(rel) = clean_relative(e) else {
            return Err(AppCommandError::invalid_input(format!(
                "exclude entries must be relative paths without '..': {e}"
            )));
        };
        exclude.push(rel.to_string_lossy().into_owned());
    }
    Ok(Plan {
        backup_id: id.to_string(),
        part_urls: req.part_urls,
        manifest_url: req.manifest_url,
        part_bytes,
        root,
        exclude,
        level,
    })
}

fn default_root() -> PathBuf {
    dirs::home_dir().unwrap_or_else(|| PathBuf::from("/home/ubuntu"))
}

pub async fn start(
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<StartRequest>,
) -> Result<Json<BackupStatus>, AppCommandError> {
    let plan = validate(req, &default_root())?;
    let status = Arc::new(Mutex::new(BackupStatus::new(&plan.backup_id, &plan.root)));
    let cancel = CancellationToken::new();
    {
        let mut job = JOB.lock().unwrap();
        if let Some(j) = job.as_ref() {
            let s = j.status.lock().unwrap();
            if s.state == "running" {
                return Err(AppCommandError::already_exists("a backup is already running")
                    .with_detail(s.backup_id.clone()));
            }
        }
        *job = Some(Job {
            status: status.clone(),
            cancel: cancel.clone(),
        });
    }
    let snapshot = status.lock().unwrap().clone();
    tokio::spawn(run(state, plan, status, cancel));
    Ok(Json(snapshot))
}

pub async fn get() -> Result<Json<BackupStatus>, AppCommandError> {
    let job = JOB.lock().unwrap();
    match job.as_ref() {
        Some(j) => Ok(Json(j.status.lock().unwrap().clone())),
        None => Err(AppCommandError::not_found(
            "no backup has run since codeg started",
        )),
    }
}

pub async fn cancel() -> Result<Json<BackupStatus>, AppCommandError> {
    let job = JOB.lock().unwrap();
    match job.as_ref() {
        Some(j) => {
            j.cancel.cancel();
            Ok(Json(j.status.lock().unwrap().clone()))
        }
        None => Err(AppCommandError::not_found("no backup to cancel")),
    }
}

async fn run(state: Arc<AppState>, plan: Plan, status: SharedStatus, cancel: CancellationToken) {
    let result = run_inner(&state, &plan, &status, &cancel).await;
    {
        let mut s = status.lock().unwrap();
        s.finished_at = Some(chrono::Utc::now().to_rfc3339());
        match &result {
            Ok(()) => {
                s.state = "succeeded";
                s.phase = "done";
            }
            Err(e) if cancel.is_cancelled() => {
                s.state = "cancelled";
                s.error = Some(e.clone());
            }
            Err(e) => {
                s.state = "failed";
                s.error = Some(e.clone());
            }
        }
    }
    let final_status = status.lock().unwrap().clone();
    tracing::info!(
        "[myclaw/backup] {} {} files={} skipped={} in={} out={} parts={}",
        final_status.backup_id,
        final_status.state,
        final_status.files,
        final_status.skipped,
        final_status.bytes_in,
        final_status.bytes_out,
        final_status.parts.len()
    );
    let body = serde_json::to_value(&final_status).unwrap_or(Value::Null);
    if let Err(e) =
        crate::commands::myclaw_platform::post_signed(&state.db, NOTIFY_PATH, body, NOTIFY_TIMEOUT)
            .await
    {
        tracing::warn!("[myclaw/backup] notify platform failed: {e}");
    }
}

async fn run_inner(
    state: &Arc<AppState>,
    plan: &Plan,
    status: &SharedStatus,
    cancel: &CancellationToken,
) -> Result<(), String> {
    // 临时目录放在 root 之外(默认 /tmp),同时也加进排除集兜底。
    let work = tempfile::Builder::new()
        .prefix("codeg-home-backup-")
        .tempdir()
        .map_err(|e| format!("create temp dir: {e}"))?;

    // ① 数据库一致快照
    let live_db = state.data_dir.join("codeg.db");
    let snap_path = work.path().join("codeg.db");
    let db_entry = match live_db.strip_prefix(&plan.root) {
        Ok(rel) if live_db.exists() => {
            crate::commands::backup::core::snapshot_db_to(&state.db.conn, &snap_path)
                .await
                .map_err(|e| format!("database snapshot failed: {}", e.message))?;
            Some(DbEntry {
                snapshot: snap_path.clone(),
                rel: rel.to_path_buf(),
            })
        }
        _ => None,
    };

    let mut excluded: HashSet<PathBuf> = plan.exclude.iter().map(|r| plan.root.join(r)).collect();
    excluded.insert(work.path().to_path_buf());
    let live_db_files: Vec<PathBuf> = ["codeg.db", "codeg.db-wal", "codeg.db-shm", "codeg.db-journal"]
        .iter()
        .map(|n| state.data_dir.join(n))
        .collect();
    if db_entry.is_some() {
        excluded.extend(live_db_files);
    }

    status.lock().unwrap().phase = "archiving";

    // ② 打包线程 → 有界通道 → ③ 逐片上传
    let (tx, mut rx) = mpsc::channel::<PartFile>(1);
    let (done_tx, done_rx) = oneshot::channel::<io::Result<()>>();
    let parts_dir = work.path().join("parts");
    std::fs::create_dir_all(&parts_dir).map_err(|e| format!("create parts dir: {e}"))?;
    let archive = ArchiveJob {
        root: plan.root.clone(),
        excluded,
        db_entry,
        parts_dir,
        part_bytes: plan.part_bytes,
        max_parts: plan.part_urls.len(),
        level: plan.level,
    };
    {
        let status = status.clone();
        let cancel = cancel.clone();
        std::thread::Builder::new()
            .name("myclaw-backup".into())
            .spawn(move || {
                lower_thread_priority();
                let sink = |part: PartFile| {
                    tx.blocking_send(part).map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "uploader stopped")
                    })
                };
                let r = archive.write(sink, &status, &cancel);
                let _ = done_tx.send(r);
            })
            .map_err(|e| format!("spawn archiver thread: {e}"))?;
    }

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(PART_UPLOAD_TIMEOUT)
        .build()
        .map_err(|e| e.without_url().to_string())?;

    let mut upload_err: Option<String> = None;
    while let Some(part) = rx.recv().await {
        if upload_err.is_none() {
            if cancel.is_cancelled() {
                upload_err = Some("cancelled".into());
            } else if let Err(e) = upload_part(&client, &plan.part_urls[part.index], &part).await {
                upload_err = Some(format!("upload part {}: {e}", part.index));
            } else {
                let mut s = status.lock().unwrap();
                s.bytes_out += part.bytes;
                s.parts.push(PartInfo {
                    index: part.index,
                    bytes: part.bytes,
                    sha256: part.sha256.clone(),
                });
            }
        }
        let _ = std::fs::remove_file(&part.path);
        if upload_err.is_some() {
            // 让打包线程停下来:取消后它在下一个条目处退出,通道关闭后 blocking_send 也会失败
            cancel.cancel();
            rx.close();
        }
    }
    let archive_result = done_rx
        .await
        .map_err(|_| "archiver thread exited unexpectedly".to_string())?;
    if let Some(e) = upload_err {
        return Err(e);
    }
    archive_result.map_err(|e| format!("archive: {e}"))?;

    // ④ 清单
    status.lock().unwrap().phase = "manifest";
    let manifest = manifest_json(&status.lock().unwrap(), plan);
    put_bytes(
        &client,
        &plan.manifest_url,
        serde_json::to_vec_pretty(&manifest).unwrap_or_default(),
        "application/json",
    )
    .await
    .map_err(|e| format!("upload manifest: {e}"))?;
    Ok(())
}

fn manifest_json(s: &BackupStatus, plan: &Plan) -> Value {
    json!({
        "format": MANIFEST_FORMAT,
        "backupId": s.backup_id,
        "codegVersion": s.codeg_version,
        "root": s.root,
        "startedAt": s.started_at,
        "completedAt": chrono::Utc::now().to_rfc3339(),
        "compression": "gzip",
        "level": plan.level,
        "partBytes": plan.part_bytes,
        "parts": s.parts,
        "files": s.files,
        "skipped": s.skipped,
        "skippedSamples": s.skipped_samples,
        "bytesIn": s.bytes_in,
        "bytesOut": s.bytes_out,
        "exclude": plan.exclude,
        "restore": "concatenate parts in index order, then `tar -xzf -` inside root",
    })
}

/// 本线程降到 nice 10(只影响这条专用线程,不碰 tokio 的线程池)。
fn lower_thread_priority() {
    #[cfg(target_os = "linux")]
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, 10);
    }
}

async fn upload_part(client: &reqwest::Client, url: &str, part: &PartFile) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 1..=PART_UPLOAD_ATTEMPTS {
        let file = tokio::fs::File::open(&part.path)
            .await
            .map_err(|e| format!("open part: {e}"))?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let res = client
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, part.bytes)
            .header(reqwest::header::CONTENT_TYPE, "application/gzip")
            .body(body)
            .send()
            .await;
        match res {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => {
                let code = r.status();
                let text = r.text().await.unwrap_or_default();
                last = format!("HTTP {code}: {}", text.chars().take(200).collect::<String>());
            }
            Err(e) => last = e.without_url().to_string(),
        }
        if attempt < PART_UPLOAD_ATTEMPTS {
            tokio::time::sleep(Duration::from_secs(2 * u64::from(attempt))).await;
        }
    }
    Err(last)
}

async fn put_bytes(
    client: &reqwest::Client,
    url: &str,
    bytes: Vec<u8>,
    content_type: &str,
) -> Result<(), String> {
    let res = client
        .put(url)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(bytes)
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if res.status().is_success() {
        Ok(())
    } else {
        let code = res.status();
        let text = res.text().await.unwrap_or_default();
        Err(format!("HTTP {code}: {}", text.chars().take(200).collect::<String>()))
    }
}

// ─── 打包(同步,跑在专用线程) ─────────────────────────────────────────────

/// 一片写好的临时文件。
#[derive(Debug)]
pub struct PartFile {
    pub index: usize,
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone)]
struct DbEntry {
    /// `VACUUM INTO` 出来的副本
    snapshot: PathBuf,
    /// 它在包里的位置(活库相对 root 的路径)
    rel: PathBuf,
}

struct ArchiveJob {
    root: PathBuf,
    excluded: HashSet<PathBuf>,
    db_entry: Option<DbEntry>,
    parts_dir: PathBuf,
    part_bytes: u64,
    max_parts: usize,
    level: u32,
}

impl ArchiveJob {
    fn is_excluded(&self, path: &Path) -> bool {
        self.excluded.iter().any(|e| path == e || path.starts_with(e))
    }

    fn write<F>(self, sink: F, status: &SharedStatus, cancel: &CancellationToken) -> io::Result<()>
    where
        F: FnMut(PartFile) -> io::Result<()>,
    {
        let parts = PartWriter::new(self.parts_dir.clone(), self.part_bytes, self.max_parts, sink);
        let gz = GzEncoder::new(parts, Compression::new(self.level));
        let mut tar = tar::Builder::new(gz);
        tar.follow_symlinks(false);
        tar.mode(tar::HeaderMode::Complete);

        let walker = walkdir::WalkDir::new(&self.root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| !self.is_excluded(e.path()));
        for entry in walker {
            if cancel.is_cancelled() {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
            }
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    let p = e.path().map(Path::to_path_buf).unwrap_or_default();
                    status.lock().unwrap().note_skipped(&p, &e.to_string());
                    continue;
                }
            };
            let abs = entry.path();
            let Ok(rel) = abs.strip_prefix(&self.root) else {
                continue;
            };
            if rel.as_os_str().is_empty() {
                continue;
            }
            let ft = entry.file_type();
            if ft.is_file() {
                // 打不开 / 读不到元数据:还没往包里写任何东西,跳过即可
                let opened = File::open(abs).and_then(|f| f.metadata().map(|m| (f, m)));
                let (file, meta) = match opened {
                    Ok(v) => v,
                    Err(e) => {
                        status.lock().unwrap().note_skipped(abs, &e.to_string());
                        continue;
                    }
                };
                // 从这里开始已经在写包:任何错误都会留下半截条目,只能整次失败
                let read_failed = append_regular(&mut tar, file, &meta, rel)?;
                let mut s = status.lock().unwrap();
                s.files += 1;
                s.bytes_in += meta.len();
                if read_failed {
                    s.note_skipped(abs, "read error mid-file; remainder zero-filled");
                }
            } else if ft.is_dir() || ft.is_symlink() {
                // 目录 / 链接没有正文:读元数据或链接目标失败发生在写之前,可以跳过;
                // 写入端出错(磁盘满、上传端停了)是致命的
                match tar.append_path_with_name(abs, rel) {
                    Ok(()) => status.lock().unwrap().files += 1,
                    Err(e) if is_sink_error(&e) => return Err(e),
                    Err(e) => status.lock().unwrap().note_skipped(abs, &e.to_string()),
                }
            } else {
                status.lock().unwrap().note_skipped(abs, "special file");
            }
        }

        if let Some(db) = &self.db_entry {
            let file = File::open(&db.snapshot)?;
            let meta = file.metadata()?;
            append_regular(&mut tar, file, &meta, &db.rel)?;
            let mut s = status.lock().unwrap();
            s.files += 1;
            s.bytes_in += meta.len();
        }

        let gz = tar.into_inner()?;
        let parts = gz.finish()?;
        parts.finish()
    }
}

/// 写入端(压缩流 → 分片)出的错带这个标记,用来和「读源文件出错」区分。
#[derive(Debug)]
struct SinkError(String);
impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SinkError {}

fn sink_error(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(SinkError(e.to_string()))
}

fn is_sink_error(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<SinkError>())
}

/// 追加一个已打开的普通文件,**按打开时的大小写满**。返回读源文件时是否出过错。
///
/// 文件在遍历中变短、或读到一半出 I/O 错误,剩下的部分补零;变长只取前面那段。这样
/// tar 的结构不会因为文件在变或读失败而损坏 —— 这里返回的错误只可能来自写入端。
fn append_regular<W: Write>(
    tar: &mut tar::Builder<W>,
    file: File,
    meta: &std::fs::Metadata,
    rel: &Path,
) -> io::Result<bool> {
    let size = meta.len();
    let mut header = tar::Header::new_gnu();
    header.set_metadata_in_mode(meta, tar::HeaderMode::Complete);
    header.set_size(size);
    let mut src = Lenient::new(file);
    tar.append_data(&mut header, rel, exact_len(&mut src, size))?;
    Ok(src.failed)
}

/// 正好 `size` 字节:源不够就补零,多了截掉。
fn exact_len<R: Read>(src: R, size: u64) -> impl Read {
    src.take(size).chain(io::repeat(0).take(size)).take(size)
}

/// 读出错时当作读完(返回 0),并记下出过错 —— 由 [`exact_len`] 补零。
struct Lenient<R> {
    inner: R,
    failed: bool,
}

impl<R> Lenient<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            failed: false,
        }
    }
}

impl<R: Read> Read for Lenient<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.failed {
            return Ok(0);
        }
        loop {
            match self.inner.read(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.failed = true;
                    return Ok(0);
                }
            }
        }
    }
}

/// 把压缩流切成固定大小的片:写满一片就关文件、算好 sha256、交给 `sink`。
struct PartWriter<F: FnMut(PartFile) -> io::Result<()>> {
    dir: PathBuf,
    part_bytes: u64,
    max_parts: usize,
    next_index: usize,
    current: Option<OpenPart>,
    sink: F,
}

struct OpenPart {
    index: usize,
    path: PathBuf,
    file: io::BufWriter<File>,
    written: u64,
    hasher: Sha256,
}

impl<F: FnMut(PartFile) -> io::Result<()>> PartWriter<F> {
    fn new(dir: PathBuf, part_bytes: u64, max_parts: usize, sink: F) -> Self {
        Self {
            dir,
            part_bytes,
            max_parts,
            next_index: 0,
            current: None,
            sink,
        }
    }

    fn open_next(&mut self) -> io::Result<()> {
        if self.next_index >= self.max_parts {
            return Err(sink_error(format!(
                "backup is larger than the {} upload slots provided ({} bytes each)",
                self.max_parts, self.part_bytes
            )));
        }
        let index = self.next_index;
        self.next_index += 1;
        let path = self.dir.join(format!("part-{index:05}"));
        let file = File::create(&path).map_err(sink_error)?;
        self.current = Some(OpenPart {
            index,
            path,
            file: io::BufWriter::with_capacity(1 << 20, file),
            written: 0,
            hasher: Sha256::new(),
        });
        Ok(())
    }

    fn close_current(&mut self) -> io::Result<()> {
        let Some(mut p) = self.current.take() else {
            return Ok(());
        };
        p.file.flush().map_err(sink_error)?;
        drop(p.file);
        let part = PartFile {
            index: p.index,
            path: p.path,
            bytes: p.written,
            sha256: hex_lower(&p.hasher.finalize()),
        };
        (self.sink)(part).map_err(sink_error)
    }

    fn finish(mut self) -> io::Result<()> {
        self.close_current()
    }
}

impl<F: FnMut(PartFile) -> io::Result<()>> Write for PartWriter<F> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.current.is_none() {
            self.open_next()?;
        }
        let p = self.current.as_mut().expect("part is open");
        let room = (self.part_bytes - p.written) as usize;
        let n = room.min(buf.len());
        p.file.write_all(&buf[..n]).map_err(sink_error)?;
        p.hasher.update(&buf[..n]);
        p.written += n as u64;
        if p.written >= self.part_bytes {
            self.close_current()?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.current.as_mut() {
            Some(p) => p.file.flush().map_err(sink_error),
            None => Ok(()),
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;

    /// 压不小的伪随机字节(xorshift),保证压缩后仍跨好几片。
    fn noise(len: usize) -> Vec<u8> {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    fn req(root: &Path) -> StartRequest {
        StartRequest {
            backup_id: "bk-1".into(),
            part_urls: vec!["https://r2.example/p0?sig=x".into()],
            manifest_url: "https://r2.example/m?sig=x".into(),
            part_bytes: None,
            root: Some(root.to_string_lossy().into_owned()),
            exclude: vec![],
            level: None,
        }
    }

    #[test]
    fn validate_defaults_and_rejects_bad_input() {
        let dir = tempfile::tempdir().unwrap();
        let plan = validate(req(dir.path()), Path::new("/nonexistent")).unwrap();
        assert_eq!(plan.part_bytes, DEFAULT_PART_BYTES);
        assert_eq!(plan.level, DEFAULT_LEVEL);
        assert_eq!(plan.root, dir.path());

        let mut r = req(dir.path());
        r.backup_id = "a b".into();
        assert!(validate(r, dir.path()).is_err());

        let mut r = req(dir.path());
        r.part_urls = vec!["ftp://x/y".into()];
        assert!(validate(r, dir.path()).is_err());

        let mut r = req(dir.path());
        r.exclude = vec!["../etc".into()];
        assert!(validate(r, dir.path()).is_err());

        let mut r = req(dir.path());
        r.exclude = vec!["/abs".into()];
        assert!(validate(r, dir.path()).is_err());

        let mut r = req(dir.path());
        r.part_bytes = Some(10);
        assert!(validate(r, dir.path()).is_err());

        let mut r = req(dir.path());
        r.root = Some("relative/dir".into());
        assert!(validate(r, dir.path()).is_err());

        let mut r = req(dir.path());
        r.exclude = vec!["./.npm-global".into(), ".cache/pip".into()];
        assert_eq!(
            validate(r, dir.path()).unwrap().exclude,
            vec![".npm-global".to_string(), ".cache/pip".to_string()]
        );
    }

    /// 造一棵树,切成很小的片打包,拼回去解压核对内容、排除项和数据库快照的替换。
    #[test]
    fn archive_round_trip_with_parts_exclusions_and_db_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        std::fs::create_dir_all(r.join(".openclaw/agents")).unwrap();
        std::fs::write(r.join(".openclaw/openclaw.json"), b"{\"a\":1}").unwrap();
        std::fs::create_dir_all(r.join("workspace")).unwrap();
        // 一个比片大、压不小的文件,保证跨片
        let big = noise(3_000_000);
        std::fs::write(r.join("workspace/big.bin"), &big).unwrap();
        std::fs::create_dir_all(r.join(".npm-global/lib")).unwrap();
        std::fs::write(r.join(".npm-global/lib/x.js"), b"skip me").unwrap();
        std::fs::create_dir_all(r.join(".codeg/data")).unwrap();
        std::fs::write(r.join(".codeg/data/codeg.db"), b"LIVE").unwrap();
        std::fs::write(r.join(".codeg/data/codeg.db-wal"), b"WAL").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("workspace/big.bin", r.join("link")).unwrap();

        let work = tempfile::tempdir().unwrap();
        let snap = work.path().join("codeg.db");
        std::fs::write(&snap, b"SNAPSHOT").unwrap();
        let parts_dir = work.path().join("parts");
        std::fs::create_dir_all(&parts_dir).unwrap();

        let mut excluded: HashSet<PathBuf> = HashSet::new();
        excluded.insert(r.join(".npm-global"));
        excluded.insert(r.join(".codeg/data/codeg.db"));
        excluded.insert(r.join(".codeg/data/codeg.db-wal"));
        let job = ArchiveJob {
            root: r.to_path_buf(),
            excluded,
            db_entry: Some(DbEntry {
                snapshot: snap,
                rel: PathBuf::from(".codeg/data/codeg.db"),
            }),
            parts_dir,
            part_bytes: MIN_PART_BYTES,
            max_parts: 50,
            level: 1,
        };
        let status = Arc::new(Mutex::new(BackupStatus::new("t", r)));
        let mut got: Vec<PartFile> = Vec::new();
        job.write(
            |p| {
                got.push(p);
                Ok(())
            },
            &status,
            &CancellationToken::new(),
        )
        .unwrap();

        // 不可压缩的大文件 → 至少两片;除最后一片外都是整片;sha256 对得上
        assert!(got.len() >= 2, "expected several parts, got {}", got.len());
        let mut joined = Vec::new();
        for (i, p) in got.iter().enumerate() {
            assert_eq!(p.index, i);
            let bytes = std::fs::read(&p.path).unwrap();
            assert_eq!(bytes.len() as u64, p.bytes);
            if i + 1 < got.len() {
                assert_eq!(p.bytes, MIN_PART_BYTES);
            }
            assert_eq!(hex_lower(&Sha256::digest(&bytes)), p.sha256);
            joined.extend(bytes);
        }

        let mut ar = tar::Archive::new(GzDecoder::new(&joined[..]));
        let mut seen = std::collections::HashMap::new();
        for e in ar.entries().unwrap() {
            let mut e = e.unwrap();
            let path = e.path().unwrap().to_string_lossy().into_owned();
            let mut buf = Vec::new();
            e.read_to_end(&mut buf).unwrap();
            seen.insert(path, buf);
        }
        assert_eq!(seen[".openclaw/openclaw.json"], b"{\"a\":1}");
        assert_eq!(seen["workspace/big.bin"], big);
        assert_eq!(seen[".codeg/data/codeg.db"], b"SNAPSHOT");
        assert!(!seen.keys().any(|k| k.starts_with(".npm-global")));
        assert!(!seen.contains_key(".codeg/data/codeg.db-wal"));
        #[cfg(unix)]
        assert!(seen.contains_key("link"));

        let s = status.lock().unwrap();
        assert_eq!(s.skipped, 0);
        assert!(s.bytes_in >= big.len() as u64);
    }

    #[test]
    fn too_many_parts_is_a_fatal_sink_error() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("noise.bin"), noise(3_000_000)).unwrap();
        let work = tempfile::tempdir().unwrap();
        let job = ArchiveJob {
            root: root.path().to_path_buf(),
            excluded: HashSet::new(),
            db_entry: None,
            parts_dir: work.path().to_path_buf(),
            part_bytes: MIN_PART_BYTES,
            max_parts: 1,
            level: 1,
        };
        let status = Arc::new(Mutex::new(BackupStatus::new("t", root.path())));
        let err = job
            .write(|_| Ok(()), &status, &CancellationToken::new())
            .unwrap_err();
        assert!(err.to_string().contains("upload slots"), "{err}");
    }

    #[test]
    fn cancellation_stops_the_walk() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a"), b"a").unwrap();
        let work = tempfile::tempdir().unwrap();
        let job = ArchiveJob {
            root: root.path().to_path_buf(),
            excluded: HashSet::new(),
            db_entry: None,
            parts_dir: work.path().to_path_buf(),
            part_bytes: MIN_PART_BYTES,
            max_parts: 10,
            level: 1,
        };
        let status = Arc::new(Mutex::new(BackupStatus::new("t", root.path())));
        let token = CancellationToken::new();
        token.cancel();
        let err = job.write(|_| Ok(()), &status, &token).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Interrupted);
    }

    #[test]
    fn exact_len_pads_short_sources_and_truncates_long_ones() {
        let mut out = Vec::new();
        exact_len(&b"abc"[..], 6).read_to_end(&mut out).unwrap();
        assert_eq!(out, b"abc\0\0\0");
        let mut out = Vec::new();
        exact_len(&b"abcdef"[..], 3).read_to_end(&mut out).unwrap();
        assert_eq!(out, b"abc");
    }

    /// 读到一半出错:不抛错、记下失败,剩余部分由 exact_len 补零。
    #[test]
    fn lenient_reader_turns_read_errors_into_zero_fill() {
        struct Flaky(u8);
        impl Read for Flaky {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0 == 0 {
                    return Err(io::Error::other("EIO"));
                }
                self.0 -= 1;
                buf[0] = b'x';
                Ok(1)
            }
        }
        let mut src = Lenient::new(Flaky(2));
        let mut out = Vec::new();
        exact_len(&mut src, 5).read_to_end(&mut out).unwrap();
        assert_eq!(out, b"xx\0\0\0");
        assert!(src.failed);
    }

    #[test]
    fn append_regular_writes_the_file_body() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        std::fs::write(&p, b"hello world").unwrap();
        let file = File::open(&p).unwrap();
        let meta = file.metadata().unwrap();
        let mut tar = tar::Builder::new(Vec::new());
        assert!(!append_regular(&mut tar, file, &meta, Path::new("f")).unwrap());
        let bytes = tar.into_inner().unwrap();
        let mut ar = tar::Archive::new(&bytes[..]);
        let mut e = ar.entries().unwrap().next().unwrap().unwrap();
        let mut buf = Vec::new();
        e.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
    }

    #[test]
    fn maintenance_is_none_without_a_running_job() {
        let mut job = JOB.lock().unwrap();
        let prev = job.take();
        drop(job);
        assert_eq!(maintenance(), None);
        let status = Arc::new(Mutex::new(BackupStatus::new("m", Path::new("/"))));
        *JOB.lock().unwrap() = Some(Job {
            status: status.clone(),
            cancel: CancellationToken::new(),
        });
        assert_eq!(maintenance(), Some("backup"));
        status.lock().unwrap().state = "succeeded";
        assert_eq!(maintenance(), None);
        *JOB.lock().unwrap() = prev;
    }
}
