//! fork(letscubo)专属: locate the OpenAI Codex CLI (`codex`) for the CLI
//! transport. The driver runs `codex app-server`, so a build too old to have it
//! fails the handshake with a clear error — no separate capability probe.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const EXECUTABLE_ENV: &str = "CODEX_EXECUTABLE";
pub const REQUIRED_PACKAGE: &str = "@openai/codex@0.155.0";

/// Resolve `codex`: `CODEX_EXECUTABLE` (session env, then codeg's env), else
/// PATH and the npm global prefixes (where the registry install puts it).
pub async fn resolve_codex_executable(
    runtime_env: &BTreeMap<String, String>,
) -> Result<PathBuf, String> {
    let explicit = runtime_env
        .get(EXECUTABLE_ENV)
        .cloned()
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var(EXECUTABLE_ENV).ok().filter(|v| !v.is_empty()));
    match explicit {
        Some(path) => {
            let path = PathBuf::from(path);
            if is_executable_file(&path) {
                Ok(path)
            } else {
                Err(format!(
                    "{EXECUTABLE_ENV}={} is not an executable file",
                    path.display()
                ))
            }
        }
        None => locate_codex().await.ok_or_else(|| {
            format!(
                "codex not found on PATH or in the npm global prefix; install {REQUIRED_PACKAGE} \
                 (npm install -g) or set {EXECUTABLE_ENV}"
            )
        }),
    }
}

async fn locate_codex() -> Option<PathBuf> {
    if let Some(path) = crate::commands::acp::resolve_npx_command("codex").await {
        return Some(path);
    }
    let candidate = crate::process::user_npm_prefix()?.join("bin").join("codex");
    is_executable_file(&candidate).then_some(candidate)
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

    #[tokio::test]
    async fn explicit_executable_env_must_be_executable() {
        let mut env = BTreeMap::new();
        env.insert(EXECUTABLE_ENV.to_string(), "/nonexistent/codex".to_string());
        assert!(resolve_codex_executable(&env)
            .await
            .unwrap_err()
            .contains("not an executable file"));
    }

    #[tokio::test]
    async fn explicit_executable_env_wins() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("codex");
        std::fs::write(&bin, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut env = BTreeMap::new();
        env.insert(EXECUTABLE_ENV.to_string(), bin.display().to_string());
        assert_eq!(resolve_codex_executable(&env).await.unwrap(), bin);
    }
}
