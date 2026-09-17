//! fork(letscubo)专属: locate the official DeepSeek Harness launcher (`dsh`)
//! for the CLI transport and check it is new enough.
//!
//! `--json` and `--session-id` only exist from `@deepseek-ai/dsh@0.1.6-alpha.2`
//! on; an older launcher accepts `--profile headless` but then fails the run
//! with `error: unknown option '--json'`. The probe turns that into a clear
//! install error before any turn is attempted.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

pub const EXECUTABLE_ENV: &str = "DSH_EXECUTABLE";
pub const REQUIRED_PACKAGE: &str = "@deepseek-ai/dsh@0.1.6-alpha.2";
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);

type ProbeCache = HashMap<PathBuf, (Option<SystemTime>, Result<(), String>)>;
static PROBE_CACHE: std::sync::Mutex<Option<ProbeCache>> = std::sync::Mutex::new(None);

/// Resolve `dsh`: `DSH_EXECUTABLE` (session env, then codeg's env), else PATH
/// and the npm global prefixes. The result has passed the capability probe.
pub async fn resolve_dsh_executable(
    runtime_env: &BTreeMap<String, String>,
) -> Result<PathBuf, String> {
    let explicit = runtime_env
        .get(EXECUTABLE_ENV)
        .cloned()
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var(EXECUTABLE_ENV).ok().filter(|v| !v.is_empty()));
    let path = match explicit {
        Some(path) => {
            let path = PathBuf::from(path);
            if !is_executable_file(&path) {
                return Err(format!(
                    "{EXECUTABLE_ENV}={} is not an executable file",
                    path.display()
                ));
            }
            path
        }
        None => locate_dsh().await.ok_or_else(|| {
            format!(
                "dsh not found on PATH or in the npm global prefix; install {REQUIRED_PACKAGE} \
                 (npm install -g) or set {EXECUTABLE_ENV}"
            )
        })?,
    };
    probe_dsh_capabilities(&path).await?;
    Ok(path)
}

async fn locate_dsh() -> Option<PathBuf> {
    if let Some(path) = crate::commands::acp::resolve_npx_command("dsh").await {
        return Some(path);
    }
    let candidate = crate::process::user_npm_prefix()?.join("bin").join("dsh");
    is_executable_file(&candidate).then_some(candidate)
}

/// `dsh --profile headless --help` must mention `--json` and `--session-id`.
/// Cached per binary path + mtime, failures included.
pub async fn probe_dsh_capabilities(bin: &Path) -> Result<(), String> {
    let mtime = std::fs::metadata(bin).and_then(|m| m.modified()).ok();
    if let Some(cached) = cached_probe(bin, mtime) {
        return cached;
    }
    let result = run_probe(bin).await;
    if let Ok(mut guard) = PROBE_CACHE.lock() {
        guard
            .get_or_insert_with(HashMap::new)
            .insert(bin.to_path_buf(), (mtime, result.clone()));
    }
    result
}

fn cached_probe(bin: &Path, mtime: Option<SystemTime>) -> Option<Result<(), String>> {
    let guard = PROBE_CACHE.lock().ok()?;
    let (cached_mtime, result) = guard.as_ref()?.get(bin)?;
    (*cached_mtime == mtime).then(|| result.clone())
}

async fn run_probe(bin: &Path) -> Result<(), String> {
    let mut command = crate::process::tokio_command(bin);
    command
        .args(["--profile", "headless", "--help"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(PROBE_TIMEOUT, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Err(format!("could not run {}: {e}", bin.display())),
        Err(_) => {
            return Err(format!(
                "{} did not answer `--profile headless --help` within {}s",
                bin.display(),
                PROBE_TIMEOUT.as_secs()
            ))
        }
    };
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    check_help_text(bin, &text)
}

pub(crate) fn check_help_text(bin: &Path, help: &str) -> Result<(), String> {
    if help.contains("--json") && help.contains("--session-id") {
        Ok(())
    } else {
        Err(format!(
            "{} does not support `--json` / `--session-id`; upgrade to {REQUIRED_PACKAGE}",
            bin.display()
        ))
    }
}

#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn script(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("dsh");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[tokio::test]
    async fn probe_accepts_a_launcher_that_lists_both_flags_and_caches_failures_by_mtime() {
        let tmp = tempfile::tempdir().unwrap();
        let old = script(tmp.path(), "echo 'Options: --help'");
        assert!(probe_dsh_capabilities(&old).await.is_err());
        // Same path, same mtime → cached failure even if the file content changed underneath.
        std::fs::write(&old, "#!/bin/sh\necho '--json --session-id <id>'\n").unwrap();
        let mtime = std::fs::metadata(&old).unwrap().modified().unwrap();
        std::fs::File::open(&old)
            .unwrap()
            .set_modified(mtime - Duration::from_secs(5))
            .ok();
        let bumped = std::fs::metadata(&old).unwrap().modified().unwrap();
        // A changed mtime re-probes.
        std::fs::File::open(&old)
            .unwrap()
            .set_modified(bumped + Duration::from_secs(60))
            .unwrap();
        assert!(probe_dsh_capabilities(&old).await.is_ok());
    }

    #[test]
    fn help_text_check_is_exact() {
        assert!(check_help_text(Path::new("/x"), "  --json  --session-id <id>").is_ok());
        let err = check_help_text(Path::new("/x"), "--json only").unwrap_err();
        assert!(err.contains(REQUIRED_PACKAGE));
    }

    #[tokio::test]
    async fn explicit_executable_env_must_be_executable() {
        let mut env = BTreeMap::new();
        env.insert(EXECUTABLE_ENV.to_string(), "/nonexistent/dsh".to_string());
        assert!(resolve_dsh_executable(&env)
            .await
            .unwrap_err()
            .contains("not an executable file"));
    }
}
