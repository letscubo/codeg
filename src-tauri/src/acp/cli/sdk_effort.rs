//! Exact installed Pi/DSH model metadata; probes never submit a model request.
use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::effort::{CliEffortContext, EffortCapability, ALL_LEVELS};
use crate::models::agent::AgentType;

const PI_PROBE: &str = include_str!("../../../resources/pi/effort-probe.mjs");
const DSH_RUNNER: &str = include_str!("../../../resources/dsh/codeg-headless-runner.mjs");
const WIRE_SOURCE: &str = include_str!("../../../resources/codeg-effort-wire.mjs");

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRequest {
    pub model: String,
    #[serde(default)]
    pub provider: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapability {
    pub model: String,
    pub provider: Option<String>,
    pub known: bool,
    pub levels: Vec<String>,
    pub default_level: Option<String>,
    pub clear_error: Option<String>,
}

#[derive(Clone, Deserialize)]
struct ProbeResult {
    version: String,
    models: Vec<ModelCapability>,
}

#[path = "sdk_effort_cache.rs"]
mod cache;

/// Isolated fixtures can exercise the production cache/resolve/driver path
/// without inspecting an installed SDK or launching a metadata probe.
#[cfg(test)]
pub(crate) fn seed_test_capabilities(
    context: &CliEffortContext,
    version: &str,
    models: Vec<ModelCapability>,
) {
    let fingerprint = cache::fingerprint(context).expect("isolated metadata cache fingerprint");
    cache::put(
        &fingerprint,
        &ProbeResult {
            version: version.to_owned(),
            models,
        },
    );
}

pub fn validate_models(models: &[ModelRequest]) -> Result<(), String> {
    if models.len() > 256 {
        return Err("At most 256 model capabilities may be requested".into());
    }
    let mut seen = BTreeMap::new();
    for request in models {
        if request.model.trim().is_empty()
            || request.model.len() > 256
            || request
                .provider
                .as_ref()
                .is_some_and(|p| p.trim().is_empty() || p.len() > 256)
        {
            return Err("An exact model and provider route are required".into());
        }
        if let Some(previous) = seen.insert(&request.model, &request.provider) {
            if previous != &request.provider {
                return Err(
                    "One model cannot use multiple provider routes in a capability request".into(),
                );
            }
        }
    }
    Ok(())
}

pub async fn capability(context: &CliEffortContext, models: &[ModelRequest]) -> EffortCapability {
    match inspect(context, models).await {
        Ok(result) => {
            let mut model_levels: BTreeMap<String, Vec<String>> =
                models.iter().map(|m| (m.model.clone(), vec![])).collect();
            let mut union = BTreeSet::new();
            for model in result.models {
                if !model.known {
                    continue;
                }
                let levels: Vec<String> = model
                    .levels
                    .into_iter()
                    .filter(|v| ALL_LEVELS.contains(&v.as_str()))
                    .collect();
                union.extend(levels.iter().cloned());
                model_levels.insert(model.model, levels);
            }
            EffortCapability {
                supported: true,
                transport: "cli",
                per_turn: true,
                clear: true,
                levels: ALL_LEVELS
                    .iter()
                    .filter(|v| union.contains(**v))
                    .map(|v| v.to_string())
                    .collect(),
                model_levels: Some(model_levels),
                version: Some(result.version),
                reason: None,
            }
        }
        Err(reason) => {
            let mut unavailable = EffortCapability::unavailable(reason, None);
            unavailable.model_levels =
                Some(models.iter().map(|m| (m.model.clone(), vec![])).collect());
            unavailable
        }
    }
}

pub async fn resolve_turn(
    context: &CliEffortContext,
    model: Option<&str>,
    provider: Option<&str>,
    requested: &Option<Option<String>>,
) -> Result<Option<String>, String> {
    let request = ModelRequest {
        model: model
            .filter(|m| !m.trim().is_empty())
            .ok_or("An exact model is required for reasoning effort")?
            .into(),
        provider: provider.map(str::to_owned),
    };
    let result = inspect(context, std::slice::from_ref(&request)).await?;
    let info = result
        .models
        .into_iter()
        .find(|m| m.model == request.model && m.provider == request.provider && m.known)
        .ok_or("The selected provider/model is not present in the installed runtime")?;
    resolve_model_level(context.agent_type, &info, requested)
}

fn resolve_model_level(
    agent_type: AgentType,
    info: &ModelCapability,
    requested: &Option<Option<String>>,
) -> Result<Option<String>, String> {
    if !info.known {
        return Err("The selected provider/model is not present in the installed runtime".into());
    }
    match requested {
        Some(Some(level)) if info.levels.contains(level) => Ok(Some(if level == "none" {
            "off".into()
        } else {
            level.clone()
        })),
        Some(Some(_)) => Err(
            "The installed runtime does not support this effort for the exact provider/model"
                .into(),
        ),
        Some(None) => {
            if let Some(reason) = &info.clear_error {
                return Err(reason.clone());
            }
            if agent_type == AgentType::Pi {
                info.default_level.clone().map(Some).ok_or_else(|| {
                    "Could not verify the installed Pi model's default thinking level".into()
                })
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

async fn inspect(
    context: &CliEffortContext,
    models: &[ModelRequest],
) -> Result<ProbeResult, String> {
    validate_models(models)?;
    if let Some(result) = cache::get(context, models) {
        return Ok(result);
    }
    let revision = cache::fingerprint(context);
    let output = match context.agent_type {
        AgentType::Pi => {
            let temp = tempfile::tempdir().map_err(|_| "Could not prepare Pi metadata probe")?;
            let probe_dir = temp.path().join("pi");
            std::fs::create_dir(&probe_dir).map_err(|_| "Could not prepare Pi metadata probe")?;
            let probe = probe_dir.join("effort-probe.mjs");
            std::fs::write(&probe, PI_PROBE).map_err(|_| "Could not prepare Pi metadata probe")?;
            std::fs::write(temp.path().join("codeg-effort-wire.mjs"), WIRE_SOURCE)
                .map_err(|_| "Could not prepare Pi wire validation")?;
            let node = crate::commands::acp::resolve_npx_command("node")
                .await
                .ok_or("Node.js is required to inspect installed Pi metadata")?;
            let mut command = crate::process::tokio_command(node);
            command
                .arg(&probe)
                .arg(&context.executable)
                .arg(
                    serde_json::to_string(models)
                        .map_err(|_| "Could not serialize model capability request")?,
                )
                .env("CODEG_PI_EFFORT_PROBE", "1")
                .env("PI_OFFLINE", "1");
            run_probe(command, context).await?
        }
        AgentType::DeepSeek => {
            if super::dsh_profile::runner_is_stock(&context.runtime_env) {
                return Err("Per-turn effort requires the Codeg DeepSeek Harness runner; stock runner is unsupported".into());
            }
            let temp =
                tempfile::tempdir().map_err(|_| "Could not prepare model capability probe")?;
            let runner_dir = temp.path().join("dsh");
            std::fs::create_dir(&runner_dir)
                .map_err(|_| "Could not prepare model capability runner")?;
            let runner = runner_dir.join("effort-runner.mjs");
            std::fs::write(&runner, DSH_RUNNER)
                .map_err(|_| "Could not prepare model capability runner")?;
            std::fs::write(temp.path().join("codeg-effort-wire.mjs"), WIRE_SOURCE)
                .map_err(|_| "Could not prepare model wire validation")?;
            let patch = temp.path().join("probe.patch.yml");
            let config = serde_json::json!([
                {"id":"headless-runner","disabled":true},
                {"insert":[{"id":"codeg-effort-capabilities","name":runner,"config":{"capabilityModels":models,"json":true}}]}
            ]);
            std::fs::write(
                &patch,
                serde_json::to_vec(&config).map_err(|_| "Could not serialize capability patch")?,
            )
            .map_err(|_| "Could not prepare capability patch")?;
            let mut command = crate::process::tokio_command(&context.executable);
            command
                .args(["--profile", "headless", "--patch"])
                .arg(&patch)
                .arg("--json");
            run_probe(command, context).await?
        }
        _ => return Err("Runtime model metadata is unavailable".into()),
    };
    // Only our explicit marker is accepted; other startup output is never a capability.
    let value = output
        .lines()
        .find_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            (value.get("type")?.as_str()? == "codeg_effort_capabilities").then_some(value)
        })
        .ok_or("The installed runtime did not return verified model metadata")?;
    let result: ProbeResult =
        serde_json::from_value(value).map_err(|_| "Invalid runtime model metadata response")?;
    if result.version.trim().is_empty() {
        return Err("Installed runtime version could not be verified".into());
    }
    if let Some(revision) = revision {
        if cache::fingerprint(context).as_deref() != Some(revision.as_str()) {
            return Err("Runtime model configuration changed during capability verification; retry the turn".into());
        }
        cache::put(&revision, &result);
    }
    Ok(result)
}

async fn run_probe(
    mut command: tokio::process::Command,
    context: &CliEffortContext,
) -> Result<String, String> {
    command
        .current_dir(&context.working_dir)
        .envs(&context.runtime_env)
        .env("CODEG_PI_EFFORT_PROBE", "1")
        .env("PI_OFFLINE", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .map_err(|_| "Runtime model capability probe timed out")?
        .map_err(|_| "Runtime model capability probe could not start")?;
    if !output.status.success() {
        return Err("Runtime model capability probe failed; no effort was applied".into());
    }
    if output.stdout.len() > 2 * 1024 * 1024 {
        return Err("Runtime model capability response exceeded its limit".into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
#[path = "sdk_effort_tests.rs"]
mod tests;
