//! fork(letscubo)专属: what codeg puts beside a DeepSeek Harness home for the
//! CLI transport.
//!
//! Two files, both under `$DSH_HOME` (resolved from the launch env, one home
//! per MyClaw agent identity):
//!
//! * `plugins/codeg-tool-search.mjs` — the tool-search plugin shipped inside
//!   codeg, materialized once per launch (rewritten only when the bytes differ).
//! * `plugins/codeg-headless-runner.mjs` — codeg's replacement for the stock
//!   headless runner: identical output, but it waits for BACKGROUND subagents
//!   (and the parent's follow-up turns they trigger) before exiting, where the
//!   stock runner exits and kills them. See the file's own header.
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
const RUNNER_SOURCE: &str = include_str!("../../../resources/dsh/codeg-headless-runner.mjs");
const RUNNER_FILE_NAME: &str = "codeg-headless-runner.mjs";
/// `stock` keeps dsh's own headless runner (no background-subagent wait) — an
/// escape hatch should a dsh bump break the replacement's internals.
pub const RUNNER_ENV: &str = "CODEG_DSH_RUNNER";
/// Shared with the claude driver: how long background work may run past the
/// model's own turn before it is stopped.
pub const BACKGROUND_WAIT_ENV: &str = "CODEG_CLI_BACKGROUND_WAIT_SECS";
const DEFAULT_BACKGROUND_WAIT_SECS: u64 = 20 * 60;

/// Everything a driver needs to write its per-turn patch.
#[derive(Debug, Clone)]
pub(crate) struct DshLaunchProfile {
    pub dsh_home: PathBuf,
    pub plugin_path: PathBuf,
    pub companion: Option<CompanionLaunchSpec>,
    /// `off` / `on` / `auto`, from `DSH_TOOL_SEARCH` in the launch env.
    pub tool_search_mode: String,
    /// codeg's runner, or `None` when `CODEG_DSH_RUNNER=stock`.
    pub runner_path: Option<PathBuf>,
    pub background_wait_secs: u64,
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
    let runner_path = if runner_is_stock(runtime_env) {
        None
    } else {
        Some(materialize(&dsh_home, RUNNER_FILE_NAME, RUNNER_SOURCE)?)
    };
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
        runner_path,
        background_wait_secs: background_wait_secs(runtime_env),
    })
}

fn runner_is_stock(runtime_env: &BTreeMap<String, String>) -> bool {
    runtime_env
        .get(RUNNER_ENV)
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("stock"))
}

fn background_wait_secs(runtime_env: &BTreeMap<String, String>) -> u64 {
    runtime_env
        .get(BACKGROUND_WAIT_ENV)
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_BACKGROUND_WAIT_SECS)
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
            "id": crate::acp::delegation::companion::COMPANION_SERVER_NAME,
            "name": "@deepseek-ai/dsh-mcp-client",
            "config": {
                "serverName": crate::acp::delegation::companion::COMPANION_SERVER_NAME,
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
            "config": { "mode": profile.tool_search_mode, "exemptServers": [crate::acp::delegation::companion::COMPANION_SERVER_NAME] },
        }));
    }
    if !inserts.is_empty() {
        entries.push(json!({ "insert": inserts }));
    }
    let yaml = serde_yaml::to_string(&Value::Array(entries)).unwrap_or_else(|_| "[]\n".to_string());
    let runner = profile
        .runner_path
        .as_deref()
        .map(|path| runner_entries(path, profile.background_wait_secs))
        .unwrap_or_default();
    format!("# Written by codeg for one CLI-transport connection; do not edit.\n{yaml}{runner}")
}

/// Swap the stock `headless-runner` row for codeg's. Hand-written because the
/// task / session / json wiring are `!!js` expressions over the
/// `headlessStartup` provider (the stock row's own form), which serde_yaml
/// cannot emit. Changing the stock row's `name` does not take (verified on A3);
/// it has to be disabled and a new row inserted.
fn runner_entries(path: &Path, background_wait_secs: u64) -> String {
    // A JSON string is a valid YAML double-quoted scalar.
    let name = serde_json::to_string(&path.display().to_string()).unwrap_or_default();
    format!(
        "- id: headless-runner\n  disabled: true\n\
         - insert:\n\
         \x20   - id: codeg-headless-runner\n\
         \x20     name: {name}\n\
         \x20     inject: [headlessStartup]\n\
         \x20     config:\n\
         \x20       task: !!js ctx.headlessStartup.task\n\
         \x20       sessionId: !!js ctx.headlessStartup.sessionId\n\
         \x20       json: !!js ctx.headlessStartup.json\n\
         \x20       backgroundWaitSecs: {background_wait_secs}\n"
    )
}

/// Write the bundled plugin under `$DSH_HOME/plugins/`, only when it differs.
fn materialize_plugin(dsh_home: &Path) -> Result<PathBuf, String> {
    materialize(dsh_home, PLUGIN_FILE_NAME, PLUGIN_SOURCE)
}

/// Write one bundled file under `$DSH_HOME/plugins/`, only when it differs.
fn materialize(dsh_home: &Path, file_name: &str, source: &str) -> Result<PathBuf, String> {
    let dir = dsh_home.join("plugins");
    let path = dir.join(file_name);
    let current = std::fs::read_to_string(&path).ok();
    if current.as_deref() != Some(source) {
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("could not create {}: {e}", dir.display()))?;
        std::fs::write(&path, source)
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
            runner_path: None,
            background_wait_secs: 1200,
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
        assert_eq!(inserts[0]["id"], "myclaw");
        assert_eq!(inserts[0]["config"]["serverName"], "myclaw");
        assert_eq!(inserts[0]["config"]["transport"], "stdio");
        assert_eq!(inserts[0]["config"]["args"][1], "t");
        assert_eq!(inserts[1]["name"], "/h/plugins/codeg-tool-search.mjs");
        assert_eq!(inserts[1]["config"]["mode"], "auto");
        // 豁免名单必须与 serverName 同名,否则委派工具会被 tool-search 当普通 MCP 工具藏起来
        assert_eq!(inserts[1]["config"]["exemptServers"][0], inserts[0]["config"]["serverName"]);
    }

    #[test]
    fn off_mode_omits_the_plugin_and_no_model_omits_the_model_entry() {
        let yaml = render_patch(&profile(false, "off"), None, Some("m"));
        assert!(!yaml.contains("codeg-tool-search"));
        assert!(!yaml.contains("agent-default-model"));
        assert!(!yaml.contains("codeg-mcp"));
    }

    #[test]
    fn the_runner_swap_disables_the_stock_row_and_wires_headless_startup() {
        let mut p = profile(true, "auto");
        p.runner_path = Some(PathBuf::from("/h/plugins/codeg-headless-runner.mjs"));
        p.background_wait_secs = 900;
        let yaml = render_patch(&p, Some("myclaw"), Some("kimi-k3"));
        let body = yaml.split_once('\n').unwrap().1;
        // Parses as YAML once the `!!js` tag (a dsh loader type) is neutralized.
        let parsed: Vec<serde_yaml::Value> =
            serde_yaml::from_str(&body.replace("!!js ", "")).unwrap();
        let disabled = parsed
            .iter()
            .find(|e| e["id"] == "headless-runner")
            .expect("stock runner row");
        assert_eq!(disabled["disabled"], true);
        let runner = parsed
            .iter()
            .filter_map(|e| e["insert"].as_sequence())
            .flatten()
            .find(|e| e["id"] == "codeg-headless-runner")
            .expect("codeg runner row");
        assert_eq!(runner["name"], "/h/plugins/codeg-headless-runner.mjs");
        assert_eq!(runner["inject"][0], "headlessStartup");
        assert_eq!(runner["config"]["task"], "ctx.headlessStartup.task");
        assert_eq!(
            runner["config"]["sessionId"],
            "ctx.headlessStartup.sessionId"
        );
        assert_eq!(runner["config"]["json"], "ctx.headlessStartup.json");
        assert_eq!(runner["config"]["backgroundWaitSecs"], 900);
        assert!(body.contains("task: !!js ctx.headlessStartup.task"));
    }

    #[test]
    fn stock_runner_and_wait_budget_read_the_env() {
        let mut env = BTreeMap::new();
        assert!(!runner_is_stock(&env));
        assert_eq!(background_wait_secs(&env), 1200);
        env.insert(RUNNER_ENV.into(), " Stock ".into());
        env.insert(BACKGROUND_WAIT_ENV.into(), "60".into());
        assert!(runner_is_stock(&env));
        assert_eq!(background_wait_secs(&env), 60);
        let yaml = render_patch(&profile(false, "off"), None, None);
        assert!(!yaml.contains("headless-runner"), "no runner path, no swap");
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
