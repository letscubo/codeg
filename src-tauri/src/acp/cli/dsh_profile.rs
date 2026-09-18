//! fork(letscubo)专属: what codeg puts beside a DeepSeek Harness home for the
//! CLI transport.
//!
//! Two files, both under `$DSH_HOME` (resolved from the launch env, one home
//! per MyClaw agent identity):
//!
//! * `plugins/codeg-tool-search.mjs` — the tool-search plugin shipped inside
//!   codeg, materialized once per launch (rewritten only when the bytes differ).
//! * `codeg-<connection_id>.patch.yml` — a per-connection `--patch` overlay:
//!   the default model, the `codeg-mcp` companion as a stdio MCP server, and
//!   the plugin entry. Rewritten before every turn (the model can change per
//!   turn) and removed when the driver stops.
//!
//! MyClaw owns `$DSH_HOME/cordis.patch.yml` (the app MCP servers). codeg never
//! reads or writes that file, so neither side has to parse the other's YAML.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::acp::connection::{companion_launch_spec, CompanionLaunchSpec, DelegationInjection};
use crate::acp::host_tools_policy::HostToolsPolicy;

pub const TOOL_SEARCH_ENV: &str = "DSH_TOOL_SEARCH";
const PLUGIN_SOURCE: &str = include_str!("../../../resources/dsh/codeg-tool-search.mjs");
const PLUGIN_FILE_NAME: &str = "codeg-tool-search.mjs";

/// Everything a driver needs to write its per-turn patch.
#[derive(Debug, Clone)]
pub(crate) struct DshLaunchProfile {
    pub dsh_home: PathBuf,
    pub plugin_path: PathBuf,
    pub companion: Option<CompanionLaunchSpec>,
    /// `off` / `on` / `auto`, from `DSH_TOOL_SEARCH` in the launch env.
    pub tool_search_mode: String,
}

impl DshLaunchProfile {
    pub fn patch_path(&self, connection_id: &str) -> PathBuf {
        self.dsh_home
            .join(format!("codeg-{connection_id}.patch.yml"))
    }
}

/// Resolve the home, materialize the plugin, and register the companion MCP
/// token for this connection. Nothing turn-specific happens here.
pub(crate) async fn prepare_launch_profile(
    connection_id: &str,
    working_dir: &Path,
    runtime_env: &BTreeMap<String, String>,
    delegation: Option<&DelegationInjection>,
    tasks_enabled: bool,
) -> Result<DshLaunchProfile, String> {
    let dsh_home = crate::parsers::deepseek::resolve_dsh_home_dir_for_launch(runtime_env);
    let plugin_path = materialize_plugin(&dsh_home)?;
    let companion = match delegation {
        Some(injection) => {
            companion_launch_spec(
                injection,
                connection_id,
                working_dir,
                tasks_enabled,
                HostToolsPolicy::from_env(runtime_env),
                crate::acp::connection::locate_codeg_mcp_binary,
            )
            .await
        }
        None => None,
    };
    Ok(DshLaunchProfile {
        dsh_home,
        plugin_path,
        companion,
        tool_search_mode: tool_search_mode(runtime_env),
    })
}

/// Write `$DSH_HOME/codeg-<connection_id>.patch.yml` for the next turn.
pub(crate) fn write_turn_patch(
    profile: &DshLaunchProfile,
    connection_id: &str,
    provider: Option<&str>,
    model: Option<&str>,
) -> Result<PathBuf, String> {
    let path = profile.patch_path(connection_id);
    let body = render_patch(profile, provider, model);
    crate::commands::mcp::write_private_text_file(&path, &body)
        .map_err(|e| format!("could not write {}: {e}", path.display()))?;
    Ok(path)
}

pub(crate) fn remove_turn_patch(profile: &DshLaunchProfile, connection_id: &str) {
    let path = profile.patch_path(connection_id);
    if let Err(e) = std::fs::remove_file(&path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!("[CLI][dsh] could not remove {}: {e}", path.display());
        }
    }
}

fn tool_search_mode(runtime_env: &BTreeMap<String, String>) -> String {
    match runtime_env
        .get(TOOL_SEARCH_ENV)
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("off") | Some("0") | Some("false") => "off",
        Some("on") | Some("1") | Some("true") => "on",
        _ => "auto",
    }
    .to_string()
}

/// The patch as YAML: a top-level list of loader patch entries.
pub(crate) fn render_patch(
    profile: &DshLaunchProfile,
    provider: Option<&str>,
    model: Option<&str>,
) -> String {
    let mut entries: Vec<Value> = Vec::new();
    if let (Some(provider), Some(model)) = (
        provider.filter(|p| !p.is_empty()),
        model.filter(|m| !m.is_empty()),
    ) {
        entries.push(json!({
            "id": "agent-default-model",
            "config": { "provider": provider, "model": model },
        }));
    }
    let mut inserts: Vec<Value> = Vec::new();
    if let Some(companion) = &profile.companion {
        inserts.push(json!({
            "id": "codeg-mcp",
            "name": "@deepseek-ai/dsh-mcp-client",
            "config": {
                "serverName": "codeg-mcp",
                "transport": "stdio",
                "command": companion.command.display().to_string(),
                "args": companion.args,
                "env": {},
            },
        }));
    }
    if profile.tool_search_mode != "off" {
        inserts.push(json!({
            "id": "codeg-tool-search",
            "name": profile.plugin_path.display().to_string(),
            "config": { "mode": profile.tool_search_mode, "exemptServers": ["codeg-mcp"] },
        }));
    }
    if !inserts.is_empty() {
        entries.push(json!({ "insert": inserts }));
    }
    let yaml = serde_yaml::to_string(&Value::Array(entries)).unwrap_or_else(|_| "[]\n".to_string());
    format!("# Written by codeg for one CLI-transport connection; do not edit.\n{yaml}")
}

/// Write the bundled plugin under `$DSH_HOME/plugins/`, only when it differs.
fn materialize_plugin(dsh_home: &Path) -> Result<PathBuf, String> {
    let dir = dsh_home.join("plugins");
    let path = dir.join(PLUGIN_FILE_NAME);
    let current = std::fs::read_to_string(&path).ok();
    if current.as_deref() != Some(PLUGIN_SOURCE) {
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("could not create {}: {e}", dir.display()))?;
        std::fs::write(&path, PLUGIN_SOURCE)
            .map_err(|e| format!("could not write {}: {e}", path.display()))?;
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(companion: bool, mode: &str) -> DshLaunchProfile {
        DshLaunchProfile {
            dsh_home: PathBuf::from("/h"),
            plugin_path: PathBuf::from("/h/plugins/codeg-tool-search.mjs"),
            companion: companion.then(|| CompanionLaunchSpec {
                command: PathBuf::from("/opt/codeg/codeg-mcp"),
                args: vec!["--token".into(), "t".into()],
                token: "t".into(),
                feedback_available: true,
                delegation_enabled: false,
            }),
            tool_search_mode: mode.to_string(),
        }
    }

    #[test]
    fn patch_lists_model_companion_and_plugin() {
        let yaml = render_patch(&profile(true, "auto"), Some("myclaw"), Some("kimi-k3"));
        let parsed: Vec<serde_yaml::Value> =
            serde_yaml::from_str(yaml.split_once('\n').unwrap().1).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["id"], "agent-default-model");
        assert_eq!(parsed[0]["config"]["model"], "kimi-k3");
        let inserts = parsed[1]["insert"].as_sequence().unwrap();
        assert_eq!(inserts[0]["config"]["serverName"], "codeg-mcp");
        assert_eq!(inserts[0]["config"]["transport"], "stdio");
        assert_eq!(inserts[0]["config"]["args"][1], "t");
        assert_eq!(inserts[1]["name"], "/h/plugins/codeg-tool-search.mjs");
        assert_eq!(inserts[1]["config"]["mode"], "auto");
    }

    #[test]
    fn off_mode_omits_the_plugin_and_no_model_omits_the_model_entry() {
        let yaml = render_patch(&profile(false, "off"), None, Some("m"));
        assert!(!yaml.contains("codeg-tool-search"));
        assert!(!yaml.contains("agent-default-model"));
        assert!(!yaml.contains("codeg-mcp"));
    }

    #[test]
    fn tool_search_mode_reads_the_env() {
        let mut env = BTreeMap::new();
        assert_eq!(tool_search_mode(&env), "auto");
        env.insert(TOOL_SEARCH_ENV.into(), "OFF".into());
        assert_eq!(tool_search_mode(&env), "off");
        env.insert(TOOL_SEARCH_ENV.into(), "on".into());
        assert_eq!(tool_search_mode(&env), "on");
    }

    #[test]
    fn plugin_is_written_once_and_left_alone_when_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let path = materialize_plugin(tmp.path()).unwrap();
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .contains("codeg-tool-search"));
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        materialize_plugin(tmp.path()).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);
    }
}
