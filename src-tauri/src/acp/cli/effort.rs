//! Claude Code's effort is a per-invocation CLI option, not connection env.
//! Official contract: https://code.claude.com/docs/en/model-config#adjust-effort-level
//! Environment effort outranks --effort. Refuse conflicting configuration;
//! never change shared settings or pin effort globally over skills/subagents.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::models::agent::AgentType;
use serde::{Deserialize, Deserializer, Serialize};

pub const LEVELS: &[&str] = &["low", "medium", "high", "xhigh", "max"];
pub const ALL_LEVELS: &[&str] = &["none", "minimal", "low", "medium", "high", "xhigh", "max"];
pub const EFFORT_ENV: &str = "CLAUDE_CODE_EFFORT_LEVEL";

/// Missing preserves old clients; Some(None) explicitly clears this turn's override.
pub fn deserialize_effort<'de, D>(d: D) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(d).map(Some)
}

#[derive(Debug, Clone)]
pub struct CliTurnOptions {
    pub model: Option<String>,
    pub provider: Option<String>,
    pub reasoning_effort: Option<Option<String>>,
}

pub fn model_route_for_turn(
    options: Option<&CliTurnOptions>,
    state: &crate::acp::session_state::SessionState,
    launch_provider: Option<&str>,
) -> (Option<String>, Option<String>) {
    let model = options
        .and_then(|o| o.model.clone())
        .or_else(|| state.cli_model.clone());
    let provider = options
        .and_then(|o| o.provider.clone())
        .or_else(|| {
            state
                .cli_effort_context
                .as_ref()
                .and_then(|c| c.provider.clone())
        })
        .or_else(|| launch_provider.map(str::to_owned));
    (model, provider)
}

#[derive(Clone)]
pub struct CliEffortContext {
    pub agent_type: AgentType,
    pub provider: Option<String>,
    pub executable: PathBuf,
    pub working_dir: PathBuf,
    pub runtime_env: BTreeMap<String, String>,
}

impl std::fmt::Debug for CliEffortContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CliEffortContext")
            .field("executable", &self.executable)
            .field("working_dir", &self.working_dir)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EffortCapability {
    pub supported: bool,
    pub transport: &'static str,
    pub per_turn: bool,
    pub clear: bool,
    pub levels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_levels: Option<BTreeMap<String, Vec<String>>>,
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl EffortCapability {
    pub fn unavailable(reason: String, version: Option<String>) -> Self {
        Self {
            supported: false,
            transport: "cli",
            per_turn: true,
            clear: true,
            levels: vec![],
            model_levels: None,
            version,
            reason: Some(reason),
        }
    }
}

/// No API call or updater: inspect the installed executable only.
pub async fn probe(executable: &Path) -> EffortCapability {
    let version_text = match read_probe(executable, "--version").await {
        Ok(text) => text,
        Err(reason) => return EffortCapability::unavailable(reason, None),
    };
    let version = version_text
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string();
    let parts: Vec<u32> = version.split('.').filter_map(|p| p.parse().ok()).collect();
    // The pinned 2.1.276 is the oldest build verified by this integration.
    if parts.len() != 3 || parts.as_slice() < [2, 1, 276].as_slice() {
        return EffortCapability::unavailable(
            "Claude Code 2.1.276 or newer is required for per-turn effort".into(),
            Some(version),
        );
    }
    match read_probe(executable, "--help").await {
        Ok(help) if help.contains("--effort") => EffortCapability {
            supported: true,
            transport: "cli",
            per_turn: true,
            clear: true,
            levels: LEVELS.iter().map(|level| level.to_string()).collect(),
            model_levels: None,
            version: Some(version),
            reason: None,
        },
        Ok(_) => EffortCapability::unavailable(
            "Installed Claude Code does not advertise --effort".into(),
            Some(version),
        ),
        Err(reason) => EffortCapability::unavailable(reason, Some(version)),
    }
}

async fn read_probe(executable: &Path, argument: &str) -> Result<String, String> {
    let mut command = crate::process::tokio_command(executable);
    command
        .arg(argument)
        .env("DISABLE_AUTOUPDATER", "1")
        .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(10), command.output())
        .await
        .map_err(|_| "Claude Code capability probe timed out".to_string())?
        .map_err(|e| format!("Claude Code capability probe failed: {e}"))?;
    if !output.status.success() {
        return Err("Claude Code capability probe failed".into());
    }
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

pub fn validate_level(requested: &Option<Option<String>>) -> Result<(), String> {
    if let Some(Some(level)) = requested {
        if !ALL_LEVELS.contains(&level.as_str()) {
            return Err(format!(
                "Unsupported reasoningEffort; available: {}",
                ALL_LEVELS.join(", ")
            ));
        }
    }
    Ok(())
}

impl CliEffortContext {
    pub fn turn_options(
        &self,
        model: Option<String>,
        provider: Option<String>,
        reasoning_effort: Option<Option<String>>,
    ) -> CliTurnOptions {
        CliTurnOptions {
            model,
            provider: provider.or_else(|| self.provider.clone()),
            reasoning_effort,
        }
    }

    pub async fn validate(
        &self,
        model: Option<&str>,
        requested: &Option<Option<String>>,
    ) -> Result<(), String> {
        self.resolve_turn(model, self.provider.as_deref(), requested)
            .await
            .map(|_| ())
    }

    pub async fn resolve_turn(
        &self,
        model: Option<&str>,
        provider: Option<&str>,
        requested: &Option<Option<String>>,
    ) -> Result<Option<String>, String> {
        if requested.is_none() {
            return Ok(None);
        }
        validate_level(requested)?;
        if matches!(self.agent_type, AgentType::Pi | AgentType::DeepSeek) {
            return super::sdk_effort::resolve_turn(self, model, provider, requested).await;
        }
        // Codex validates against native model/list inside the accepted turn's
        // app-server process. No shared configuration or stale capability cache.
        if self.agent_type == AgentType::Codex {
            return Ok(requested.clone().flatten());
        }
        if self.agent_type != AgentType::ClaudeCode {
            return Err("Per-turn effort is unavailable for this runtime".into());
        }
        if requested
            .as_ref()
            .and_then(|v| v.as_deref())
            .is_some_and(|v| !LEVELS.contains(&v))
        {
            return Err("Claude Code supports only low, medium, high, xhigh and max".into());
        }
        if !model.is_some_and(crate::web::llm_bridge::is_claude_model) {
            return Err(
                "Per-turn reasoning effort is supported only for native Claude models".into(),
            );
        }
        let capability = probe(&self.executable).await;
        if !capability.supported {
            return Err(capability
                .reason
                .unwrap_or_else(|| "Claude Code effort is unavailable".into()));
        }
        self.validate_configuration(requested)?;
        Ok(requested.as_ref().and_then(Clone::clone))
    }

    pub fn validate_configuration(&self, requested: &Option<Option<String>>) -> Result<(), String> {
        if requested.is_none() {
            return Ok(());
        }
        for key in [EFFORT_ENV, "CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING"] {
            if self
                .runtime_env
                .get(key)
                .filter(|v| !v.is_empty())
                .is_some()
                || std::env::var(key).ok().filter(|v| !v.is_empty()).is_some()
            {
                return Err(format!("Claude Code effort conflicts with {key}; remove the override before changing depth"));
            }
        }
        let max_thinking = self
            .runtime_env
            .get("MAX_THINKING_TOKENS")
            .cloned()
            .or_else(|| std::env::var("MAX_THINKING_TOKENS").ok());
        if max_thinking
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        {
            return Err("Claude Code effort conflicts with MAX_THINKING_TOKENS; remove the fixed budget override before changing depth".into());
        }
        for path in self.settings_paths()? {
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {
                    return Err(
                        "Could not read Claude Code settings to verify reasoning effort".into(),
                    )
                }
            };
            let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                "Could not parse Claude Code settings to verify reasoning effort".to_string()
            })?;
            if let Some(key) = conflicting_setting(&value, requested == &Some(None)) {
                return Err(format!("Claude Code effort conflicts with settings key {key}; update the setting before changing depth"));
            }
        }
        Ok(())
    }

    fn settings_paths(&self) -> Result<Vec<PathBuf>, String> {
        let config = self
            .runtime_env
            .get("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .or_else(|| std::env::var("CLAUDE_CONFIG_DIR").ok().map(PathBuf::from))
            .unwrap_or_else(|| crate::parsers::claude::resolve_claude_config_dir());
        let mut paths = vec![
            config.join("settings.json"),
            config.join("remote-settings.json"),
            PathBuf::from("/etc/claude-code/managed-settings.json"),
            PathBuf::from("/Library/Application Support/ClaudeCode/managed-settings.json"),
        ];
        for dir in self.working_dir.ancestors() {
            paths.push(dir.join(".claude/settings.json"));
            paths.push(dir.join(".claude/settings.local.json"));
        }
        // Official managed settings fragments are merged with the main policy.
        // Fail closed on unreadable policy, rather than missing an effort cap.
        for directory in [
            "/etc/claude-code/managed-settings.d",
            "/Library/Application Support/ClaudeCode/managed-settings.d",
        ] {
            let entries =
                match std::fs::read_dir(directory) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => return Err(
                        "Could not read Claude Code managed settings to verify reasoning effort"
                            .into(),
                    ),
                };
            for entry in entries {
                let path = entry
                    .map_err(|_| "Could not inspect Claude Code managed settings".to_string())?
                    .path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    paths.push(path);
                }
            }
        }
        Ok(paths)
    }
}

fn conflicting_setting(value: &serde_json::Value, clearing: bool) -> Option<&'static str> {
    // A dynamic policy can inject a cap we cannot safely inspect without running it.
    if value
        .get("policyHelper")
        .is_some_and(|value| !value.is_null())
    {
        return Some("policyHelper");
    }
    if let Some(env) = value.get("env") {
        for key in [EFFORT_ENV, "CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING"] {
            if env
                .get(key)
                .is_some_and(|v| !v.is_null() && v.as_str() != Some(""))
            {
                return Some(key);
            }
        }
        if env
            .get("MAX_THINKING_TOKENS")
            .is_some_and(|v| !v.is_null() && v.as_str() != Some(""))
        {
            return Some("MAX_THINKING_TOKENS");
        }
    }
    if value.get("alwaysThinkingEnabled") == Some(&serde_json::Value::Bool(false)) {
        return Some("alwaysThinkingEnabled");
    }
    if value.get("maxEffortLevel").is_some_and(|v| !v.is_null()) {
        return Some("maxEffortLevel");
    }
    if clearing && value.get("effortLevel").is_some_and(|v| !v.is_null()) {
        return Some("effortLevel");
    }
    if let Some(models) = value.get("modelSettings").and_then(|v| v.as_object()) {
        for model in models.values() {
            if model.get("maxEffortLevel").is_some_and(|v| !v.is_null()) {
                return Some("modelSettings.maxEffortLevel");
            }
            if clearing && model.get("effortLevel").is_some_and(|v| !v.is_null()) {
                return Some("modelSettings.effortLevel");
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(unix)]
    #[tokio::test]
    async fn capability_probe_reads_installed_binary_and_rejects_unverified_versions() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let executable = tmp.path().join("claude");
        for (version, expected) in [("2.1.276", true), ("2.1.275", false), ("unknown", false)] {
            std::fs::write(&executable, format!("#!/bin/sh\ncase \"$1\" in --version) echo '{version} (Claude Code)';; --help) echo '--effort <level>';; esac\n")).unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
            let capability = probe(&executable).await;
            assert_eq!(capability.supported, expected);
            assert_eq!(capability.version.as_deref(), Some(version));
        }
    }

    #[test]
    fn explicit_effort_overrides_settings_but_env_and_caps_are_rejected() {
        assert_eq!(
            conflicting_setting(&json!({"effortLevel":"low"}), false),
            None
        );
        assert_eq!(
            conflicting_setting(&json!({"effortLevel":"low"}), true),
            Some("effortLevel")
        );
        assert_eq!(
            conflicting_setting(&json!({"env":{EFFORT_ENV:"high"}}), false),
            Some(EFFORT_ENV)
        );
        assert_eq!(
            conflicting_setting(&json!({"maxEffortLevel":"medium"}), false),
            Some("maxEffortLevel")
        );
        for budget in ["0", "000", "0e0", "1024"] {
            assert_eq!(
                conflicting_setting(&json!({"env":{"MAX_THINKING_TOKENS":budget}}), false),
                Some("MAX_THINKING_TOKENS")
            );
        }
        assert!(validate_level(&Some(Some("ultracode".into()))).is_err());
        for level in LEVELS {
            assert!(validate_level(&Some(Some(level.to_string()))).is_ok());
        }
    }
}
