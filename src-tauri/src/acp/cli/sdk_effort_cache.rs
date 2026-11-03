//! Metadata cache keyed by the exact launch context and current config bytes.
//! No expiry window permits stale configuration: unreadable/unbounded sources
//! disable caching, and every lookup rechecks the configuration fingerprint.
use super::super::effort::CliEffortContext;
use super::{ModelRequest, ProbeResult};
use crate::models::agent::AgentType;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

fn entries() -> &'static Mutex<BTreeMap<String, ProbeResult>> {
    static CACHE: OnceLock<Mutex<BTreeMap<String, ProbeResult>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(super) fn get(context: &CliEffortContext, models: &[ModelRequest]) -> Option<ProbeResult> {
    let fingerprint = fingerprint(context)?;
    let cache = entries().lock().ok()?;
    let mut result = ProbeResult {
        version: String::new(),
        models: vec![],
    };
    for model in models {
        let hit = cache.get(&key(&fingerprint, model))?;
        if !result.version.is_empty() && result.version != hit.version {
            return None;
        }
        result.version = hit.version.clone();
        result.models.extend(hit.models.clone());
    }
    (!models.is_empty()).then_some(result)
}

pub(super) fn put(fingerprint: &str, result: &ProbeResult) {
    let Ok(mut cache) = entries().lock() else {
        return;
    };
    if cache.len() > 1024 {
        cache.clear();
    }
    for model in &result.models {
        let query = ModelRequest {
            model: model.model.clone(),
            provider: model.provider.clone(),
        };
        cache.insert(
            key(fingerprint, &query),
            ProbeResult {
                version: result.version.clone(),
                models: vec![model.clone()],
            },
        );
    }
}

fn key(fingerprint: &str, model: &ModelRequest) -> String {
    format!(
        "{fingerprint}:{}",
        serde_json::to_string(model).unwrap_or_default()
    )
}

fn config_files(dir: &Path, files: &mut Vec<PathBuf>) -> Option<()> {
    match std::fs::read_dir(dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Some(()),
        Err(_) => return None,
        Ok(entries) => {
            for entry in entries {
                let entry = entry.ok()?;
                let path = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if [
                    "sessions",
                    "attachments",
                    "logs",
                    "cache",
                    "node_modules",
                    ".git",
                ]
                .contains(&name.as_ref())
                {
                    continue;
                }
                if name.starts_with("codeg-") && name.ends_with(".patch.yml") {
                    continue;
                }
                if entry.file_type().ok()?.is_symlink() && path.is_dir() {
                    return None;
                }
                if path.is_dir() {
                    config_files(&path, files)?;
                } else if path
                    .extension()
                    .is_some_and(|v| ["json", "yaml", "yml", "mjs", "js"].iter().any(|s| v == *s))
                {
                    files.push(path);
                }
                if files.len() > 1024 {
                    return None;
                }
            }
        }
    }
    Some(())
}

pub(super) fn fingerprint(context: &CliEffortContext) -> Option<String> {
    let mut digest = Sha256::new();
    digest.update(format!("{:?}", context.agent_type));
    digest.update(context.executable.to_string_lossy().as_bytes());
    digest.update(context.working_dir.to_string_lossy().as_bytes());
    digest.update(serde_json::to_vec(&context.runtime_env).ok()?);
    let executable = std::fs::canonicalize(&context.executable).ok()?;
    let metadata = std::fs::metadata(&executable).ok()?;
    digest.update(format!(
        "{:?}:{}",
        metadata.modified().ok()?,
        metadata.len()
    ));
    let home = match context.agent_type {
        AgentType::DeepSeek => {
            crate::parsers::deepseek::resolve_dsh_home_dir_for_launch(&context.runtime_env)
        }
        AgentType::Pi => context
            .runtime_env
            .get("PI_CODING_AGENT_DIR")
            .cloned()
            .or_else(|| std::env::var("PI_CODING_AGENT_DIR").ok())
            .map(PathBuf::from)
            .unwrap_or_else(|| dirs::home_dir().unwrap_or_default().join(".pi/agent")),
        _ => return None,
    };
    let mut files = Vec::new();
    config_files(&home, &mut files)?;
    files.push(context.working_dir.join(".pi/settings.json"));
    // Installed dependency updates must also invalidate metadata. Their package
    // manifests accompany normal npm upgrades even when the CLI stub is stable.
    for ancestor in executable.ancestors().skip(1).take(8) {
        files.push(ancestor.join("package.json"));
        for package in ["@earendil-works/pi-ai", "@deepseek-ai/dsh-llm-pi-ai"] {
            files.push(
                ancestor
                    .join("node_modules")
                    .join(package)
                    .join("package.json"),
            );
        }
    }
    files.sort();
    files.dedup();
    for file in files {
        digest.update(file.to_string_lossy().as_bytes());
        match std::fs::read(&file) {
            Ok(bytes) if bytes.len() <= 2 * 1024 * 1024 => {
                digest.update([1]);
                digest.update(bytes);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => digest.update([0]),
            _ => return None,
        }
    }
    Some(format!("{:x}", digest.finalize()))
}
