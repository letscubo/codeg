//! fork(letscubo)专属: locate the Claude Code native binary for the CLI transport.
//!
//! Mirrors `claude-agent-acp`'s `claudeCliPath()` so a CLI turn runs the exact
//! binary an ACP session of the same install runs: an explicit
//! `CLAUDE_CODE_EXECUTABLE` wins; otherwise the platform package that
//! `@anthropic-ai/claude-agent-sdk` ships as an optional dependency, nested
//! under the adapter or hoisted beside it in an npm global prefix.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const EXECUTABLE_ENV: &str = "CLAUDE_CODE_EXECUTABLE";
const ADAPTER_PACKAGE: &str = "@agentclientprotocol/claude-agent-acp";
const SDK_SCOPE: &str = "@anthropic-ai";

/// Resolve the binary, preferring `CLAUDE_CODE_EXECUTABLE` from the session's
/// runtime env, then from codeg's own env, then the SDK platform package.
pub async fn resolve_claude_executable(
    runtime_env: &BTreeMap<String, String>,
) -> Result<PathBuf, String> {
    let explicit = runtime_env
        .get(EXECUTABLE_ENV)
        .cloned()
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var(EXECUTABLE_ENV).ok().filter(|v| !v.is_empty()));
    if let Some(path) = explicit {
        let path = PathBuf::from(path);
        return if is_executable_file(&path) {
            Ok(path)
        } else {
            Err(format!(
                "{EXECUTABLE_ENV}={} is not an executable file",
                path.display()
            ))
        };
    }

    let mut prefixes = Vec::new();
    if let Some(prefix) = crate::commands::acp::cached_npm_global_prefix().await {
        prefixes.push(prefix);
    }
    if let Some(prefix) = crate::process::user_npm_prefix() {
        if !prefixes.contains(&prefix) {
            prefixes.push(prefix);
        }
    }
    let candidates = candidate_paths(&prefixes, &platform_package_binaries());
    first_executable(&candidates).ok_or_else(|| {
        let looked: Vec<String> = candidates.iter().map(|p| p.display().to_string()).collect();
        format!(
            "Claude Code native binary not found; install {ADAPTER_PACKAGE} or set \
             {EXECUTABLE_ENV} (looked in: {})",
            looked.join(", ")
        )
    })
}

/// `(package directory, file name)` pairs for this platform, preferred first.
pub(crate) fn platform_package_binaries() -> Vec<(String, &'static str)> {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        other => other,
    };
    let file = if cfg!(windows) {
        "claude.exe"
    } else {
        "claude"
    };
    let packages = match std::env::consts::OS {
        "linux" => {
            // Both libc variants can be installed side by side and the wrong
            // one crashes at runtime rather than failing to spawn.
            let glibc = format!("claude-agent-sdk-linux-{arch}");
            let musl = format!("{glibc}-musl");
            if host_is_musl() {
                vec![musl, glibc]
            } else {
                vec![glibc, musl]
            }
        }
        "macos" => vec![format!("claude-agent-sdk-darwin-{arch}")],
        "windows" => vec![format!("claude-agent-sdk-win32-{arch}")],
        os => vec![format!("claude-agent-sdk-{os}-{arch}")],
    };
    packages.into_iter().map(|p| (p, file)).collect()
}

/// Nested-under-adapter candidates first, then hoisted ones, per prefix.
pub(crate) fn candidate_paths(
    prefixes: &[PathBuf],
    package_binaries: &[(String, &'static str)],
) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for prefix in prefixes {
        let node_modules = node_modules_dir(prefix);
        let nested = node_modules
            .join(ADAPTER_PACKAGE)
            .join("node_modules")
            .join(SDK_SCOPE);
        let hoisted = node_modules.join(SDK_SCOPE);
        for base in [nested, hoisted] {
            for (package, file) in package_binaries {
                out.push(base.join(package).join(file));
            }
        }
    }
    out
}

pub(crate) fn first_executable(candidates: &[PathBuf]) -> Option<PathBuf> {
    candidates.iter().find(|p| is_executable_file(p)).cloned()
}

fn node_modules_dir(prefix: &Path) -> PathBuf {
    if cfg!(windows) {
        prefix.join("node_modules")
    } else {
        prefix.join("lib").join("node_modules")
    }
}

fn host_is_musl() -> bool {
    if cfg!(target_env = "musl") {
        return true;
    }
    std::fs::read_dir("/lib")
        .map(|entries| {
            entries
                .flatten()
                .any(|e| e.file_name().to_string_lossy().starts_with("ld-musl-"))
        })
        .unwrap_or(false)
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

    fn touch(path: &Path, mode: u32) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    fn packages() -> Vec<(String, &'static str)> {
        vec![("claude-agent-sdk-linux-x64".to_string(), "claude")]
    }

    #[test]
    fn prefers_the_sdk_nested_under_the_adapter() {
        let tmp = tempfile::tempdir().unwrap();
        let nm = tmp.path().join("lib/node_modules");
        let nested = nm.join(
            "@agentclientprotocol/claude-agent-acp/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude",
        );
        let hoisted = nm.join("@anthropic-ai/claude-agent-sdk-linux-x64/claude");
        touch(&nested, 0o755);
        touch(&hoisted, 0o755);
        let found = first_executable(&candidate_paths(&[tmp.path().to_path_buf()], &packages()));
        assert_eq!(found, Some(nested));
    }

    #[test]
    fn falls_back_to_a_hoisted_sdk_and_later_prefixes() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let hoisted = second
            .path()
            .join("lib/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude");
        touch(&hoisted, 0o755);
        let prefixes = [first.path().to_path_buf(), second.path().to_path_buf()];
        assert_eq!(
            first_executable(&candidate_paths(&prefixes, &packages())),
            Some(hoisted)
        );
    }

    #[test]
    fn ignores_files_without_the_execute_bit() {
        let tmp = tempfile::tempdir().unwrap();
        touch(
            &tmp.path()
                .join("lib/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude"),
            0o644,
        );
        assert_eq!(
            first_executable(&candidate_paths(&[tmp.path().to_path_buf()], &packages())),
            None
        );
    }
}
