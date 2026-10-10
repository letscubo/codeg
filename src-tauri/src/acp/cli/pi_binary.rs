//! fork(letscubo)专属: locate the official pi CLI for the CLI transport and
//! check it is new enough.
//!
//! The driver needs `--mode json` (the NDJSON session event stream) and
//! `--session-id` (continue an exact session, creating it when missing). Both
//! exist in @earendil-works/pi-coding-agent 0.84.2, but an older build accepts
//! `--print` and then ignores the rest, which would read back as a turn that
//! produced nothing. The probe turns that into a clear install error before any
//! turn is attempted.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

pub const EXECUTABLE_ENV: &str = "PI_EXECUTABLE";
pub const REQUIRED_PACKAGE: &str = "@earendil-works/pi-coding-agent";
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);

type ProbeCache = HashMap<PathBuf, (Option<SystemTime>, Result<(), String>)>;
static PROBE_CACHE: std::sync::Mutex<Option<ProbeCache>> = std::sync::Mutex::new(None);

/// Resolve `pi`: `PI_EXECUTABLE` (session env, then codeg's env), else PATH and
/// the npm global prefix. The result has passed the capability probe.
pub async fn resolve_pi_executable(
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
        None => locate_pi().await.ok_or_else(|| {
            format!(
                "pi not found on PATH or in the npm global prefix; install {REQUIRED_PACKAGE} \
                 (npm install -g) or set {EXECUTABLE_ENV}"
            )
        })?,
    };
    probe_pi_capabilities(&path).await?;
    Ok(path)
}

async fn locate_pi() -> Option<PathBuf> {
    if let Some(path) = crate::commands::acp::resolve_npx_command("pi").await {
        return Some(path);
    }
    let candidate = crate::process::user_npm_prefix()?.join("bin").join("pi");
    is_executable_file(&candidate).then_some(candidate)
}

/// `pi --help` must list `--mode` and `--session-id`.
/// Cached per binary path + mtime, failures included.
pub async fn probe_pi_capabilities(bin: &Path) -> Result<(), String> {
    let mtime = std::fs::metadata(bin)
        .and_then(|m| m.modified())
        .ok();
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
    command.arg("--help");
    let output = tokio::time::timeout(PROBE_TIMEOUT, command.output())
        .await
        .map_err(|_| format!("{} --help timed out", bin.display()))?
        .map_err(|e| format!("{} --help failed: {e}", bin.display()))?;
    // pi prints its help on stdout; take stderr too so a build that complains
    // there is not mistaken for one missing the flags.
    let help = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    check_help_text(bin, &help)
}

pub(crate) fn check_help_text(bin: &Path, help: &str) -> Result<(), String> {
    let missing: Vec<&str> = ["--mode", "--session-id"]
        .into_iter()
        .filter(|flag| !help.contains(*flag))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} is too old for the CLI transport: `--help` does not list {}. Install {REQUIRED_PACKAGE} \
         (npm install -g) or set {EXECUTABLE_ENV} to a newer build",
        bin.display(),
        missing.join(" and ")
    ))
}

#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    std::fs::metadata(path).map(|m| m.is_file()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_text_check_names_every_missing_flag() {
        let bin = Path::new("/tmp/pi");
        // 0.84.2 的真实 help 里两个都有
        assert!(check_help_text(bin, "  --mode <mode>   ...\n  --session-id <id>  ...").is_ok());
        let err = check_help_text(bin, "  --print, -p   Non-interactive mode").unwrap_err();
        assert!(err.contains("--mode"), "{err}");
        assert!(err.contains("--session-id"), "{err}");
        let err = check_help_text(bin, "  --mode <mode>  Output mode").unwrap_err();
        assert!(!err.contains("--mode and"), "只缺一个时不该两个都报: {err}");
        assert!(err.contains("--session-id"), "{err}");
    }

    #[tokio::test]
    async fn explicit_executable_env_must_be_executable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("not-executable");
        std::fs::write(&path, "#!/bin/sh\n").expect("write");
        let env = BTreeMap::from([(
            EXECUTABLE_ENV.to_string(),
            path.display().to_string(),
        )]);
        let err = resolve_pi_executable(&env).await.unwrap_err();
        assert!(err.contains("is not an executable file"), "{err}");
    }
}
