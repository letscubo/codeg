//! Task-only adapter. Each scheduler run replays its saved depth; chat preferences are untouched.
use std::collections::BTreeMap;
use std::time::Duration;

use crate::acp::cli::effort::CliTurnOptions;
use crate::acp::manager::ConnectionManager;
use crate::acp::types::ConnectionStatus;
use crate::models::{AgentType, AutomationAction, AutomationConfig};

pub(crate) struct Plan {
    pub runtime_env: BTreeMap<String, String>,
    pub cli_options: Option<CliTurnOptions>,
    pub openclaw_level: Option<Option<String>>,
}

pub(crate) fn validate_config(agent: &str, config: &AutomationConfig) -> Result<(), String> {
    if config.reasoning_effort.is_none() {
        return Ok(());
    }
    if config.action == AutomationAction::EnqueueTask {
        return Err("Thinking depth requires a launch-session automation".into());
    }
    let agent: AgentType = serde_json::from_value(serde_json::Value::String(agent.into()))
        .map_err(|_| "Invalid automation agent type".to_string())?;
    plan(agent, config).map(|_| ())
}

pub(crate) fn plan(agent: AgentType, config: &AutomationConfig) -> Result<Plan, String> {
    let mut result = Plan {
        runtime_env: BTreeMap::new(),
        cli_options: None,
        openclaw_level: None,
    };
    let Some(level) = &config.reasoning_effort else {
        return Ok(result);
    };
    if let Some(level) = level {
        if ![
            "off", "none", "minimal", "low", "medium", "high", "xhigh", "max",
        ]
        .contains(&level.as_str())
        {
            return Err("Invalid automation reasoning effort".into());
        }
    }
    match agent {
        AgentType::OpenClaw => {
            result.openclaw_level =
                Some(
                    level
                        .as_ref()
                        .map(|s| if s == "none" { "off".into() } else { s.clone() }),
                );
        }
        AgentType::Hermes => {
            // Reuse main's immutable launch snapshot and mandatory ACP handshake.
            result.runtime_env.insert(
                super::hermes_reasoning::SNAPSHOT_ENV.into(),
                level
                    .as_deref()
                    .map(|s| if s == "off" { "none" } else { s })
                    .unwrap_or("_inherit")
                    .into(),
            );
            super::hermes_reasoning::validate_snapshot(agent, &result.runtime_env)
                .map_err(|e| e.to_string())?;
        }
        AgentType::ClaudeCode | AgentType::Pi | AgentType::DeepSeek | AgentType::Codex => {
            result.cli_options = Some(CliTurnOptions {
                model: None,
                provider: None,
                reasoning_effort: Some(level.as_ref().map(|s| {
                    if s == "off" {
                        "none".into()
                    } else {
                        s.clone()
                    }
                })),
            });
        }
        // A catalog model's depth is not proof that another runtime transports it.
        _ if level.is_none() => {}
        _ => return Err("Automation thinking depth is unavailable for this runtime".into()),
    }
    Ok(result)
}

fn patch_args(key: &str, level: &Option<String>) -> Vec<String> {
    vec![
        "gateway".into(),
        "call".into(),
        "sessions.patch".into(),
        "--params".into(),
        serde_json::json!({"key":key,"thinkingLevel":level}).to_string(),
        "--json".into(),
    ]
}

fn patch_succeeded(success: bool, stdout: &[u8]) -> bool {
    success
        && serde_json::from_slice::<serde_json::Value>(stdout)
            .ok()
            .and_then(|v| v.get("ok").and_then(serde_json::Value::as_bool))
            == Some(true)
}

/// The Gateway owns OpenClaw depth. Wait for ACP reset/load before patching the exact task key.
/// No shell interpolation; failure stops this run before its prompt can execute.
pub(crate) async fn prepare_openclaw(
    manager: &ConnectionManager,
    connection_id: &str,
    env: &BTreeMap<String, String>,
    level: Option<&Option<String>>,
) -> Result<(), String> {
    let Some(level) = level else { return Ok(()) };
    let key = env
        .get("OPENCLAW_SESSION_KEY")
        .filter(|s| !s.is_empty())
        .ok_or("OpenClaw automation session key is missing")?;
    let state = manager
        .get_state(connection_id)
        .await
        .ok_or("Automation connection is missing")?;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            {
                let s = state.read().await;
                if s.status == ConnectionStatus::Connected && s.external_id.is_some() {
                    return Ok(());
                }
                if matches!(
                    s.status,
                    ConnectionStatus::Disconnected | ConnectionStatus::Error
                ) {
                    return Err("OpenClaw automation connection failed");
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| "OpenClaw automation connection timed out")??;
    let executable =
        crate::commands::acp::resolve_system_agent_binary_for(AgentType::OpenClaw, "openclaw")
            .ok_or("OpenClaw executable is unavailable")?;
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        crate::process::tokio_command(executable)
            .args(patch_args(key, level))
            .envs(env)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "OpenClaw automation depth timed out")?
    .map_err(|_| "Could not set OpenClaw automation depth")?;
    if !patch_succeeded(output.status.success(), &output.stdout) {
        return Err("OpenClaw did not confirm the automation thinking depth".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn myclaw_prompt_automation_depth_preserves_legacy_clear_and_exact_snapshots() {
        let legacy: AutomationConfig = serde_json::from_value(json!({})).unwrap();
        assert!(legacy.reasoning_effort.is_none());
        assert!(serde_json::to_value(&legacy)
            .unwrap()
            .get("reasoning_effort")
            .is_none());
        assert!(plan(AgentType::Hermes, &legacy)
            .unwrap()
            .cli_options
            .is_none());
        for depth in [Some("low"), Some("high"), None] {
            let config: AutomationConfig =
                serde_json::from_value(json!({"reasoning_effort":depth})).unwrap();
            assert_eq!(
                serde_json::to_value(&config).unwrap()["reasoning_effort"],
                json!(depth)
            );
            assert_eq!(
                plan(AgentType::Hermes, &config).unwrap().runtime_env
                    [super::super::hermes_reasoning::SNAPSHOT_ENV],
                depth.unwrap_or("_inherit")
            );
            for agent in [
                AgentType::ClaudeCode,
                AgentType::Pi,
                AgentType::DeepSeek,
                AgentType::Codex,
            ] {
                assert_eq!(
                    plan(agent, &config)
                        .unwrap()
                        .cli_options
                        .unwrap()
                        .reasoning_effort,
                    Some(depth.map(str::to_owned))
                );
            }
            assert_eq!(
                plan(AgentType::OpenClaw, &config).unwrap().openclaw_level,
                Some(depth.map(str::to_owned))
            );
        }
    }

    #[test]
    fn myclaw_prompt_automation_depth_rejects_unsupported_runtime_and_bad_values() {
        let bad: AutomationConfig =
            serde_json::from_value(json!({"reasoning_effort":"unknown"})).unwrap();
        assert!(plan(AgentType::Hermes, &bad).is_err());
        let explicit: AutomationConfig =
            serde_json::from_value(json!({"reasoning_effort":"high"})).unwrap();
        assert!(plan(AgentType::Gemini, &explicit).is_err());
        assert!(serde_json::from_value::<AutomationConfig>(json!({"reasoning_effort":3})).is_err());
        let enqueue = AutomationConfig {
            action: AutomationAction::EnqueueTask,
            ..explicit
        };
        assert!(validate_config("hermes", &enqueue).is_err());
    }

    #[test]
    fn myclaw_prompt_automation_openclaw_patch_is_exact_scoped_and_requires_ack() {
        let key = "agent:identity:codeg-task-exact";
        for depth in [Some("high".into()), None] {
            let args = patch_args(key, &depth);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&args[4]).unwrap(),
                json!({"key":key,"thinkingLevel":depth})
            );
        }
        assert!(patch_succeeded(true, br#"{"ok":true}"#));
        for output in [br#"{"ok":false}"#.as_slice(), b"{}", b"not json"] {
            assert!(!patch_succeeded(true, output));
        }
        assert!(!patch_succeeded(false, br#"{"ok":true}"#));
    }

    #[test]
    fn myclaw_prompt_automation_hermes_snapshot_is_scoped_and_recomputed_per_run() {
        let key = super::super::hermes_reasoning::SNAPSHOT_ENV;
        let mut config: AutomationConfig = serde_json::from_value(json!({
            "reasoning_effort":"low", "reuse_session":true,
            "runtime_env":{"HERMES_HOME":"/exact-identity","CODEG_HERMES_REASONING_EFFORT":"high"}
        }))
        .unwrap();
        let first = plan(AgentType::Hermes, &config).unwrap();
        let mut env = config.runtime_env.clone();
        env.extend(first.runtime_env.clone());
        assert_eq!(env[key], "low");
        assert_eq!(env["HERMES_HOME"], "/exact-identity");
        assert!(super::super::hermes_reasoning::snapshot_required(
            AgentType::Hermes,
            &env
        ));
        config.reasoning_effort = Some(Some("high".into()));
        assert_eq!(
            plan(AgentType::Hermes, &config).unwrap().runtime_env[key],
            "high"
        );
        assert_eq!(first.runtime_env[key], "low");
        config.reasoning_effort = Some(None);
        assert_eq!(
            plan(AgentType::Hermes, &config).unwrap().runtime_env[key],
            "_inherit"
        );
        config.reasoning_effort = Some(Some("off".into()));
        assert_eq!(
            plan(AgentType::Hermes, &config).unwrap().runtime_env[key],
            "none"
        );
        config.reasoning_effort = None;
        assert!(plan(AgentType::Hermes, &config)
            .unwrap()
            .runtime_env
            .is_empty());
        config.reasoning_effort = Some(Some("high".into()));
        for runtime in [
            AgentType::OpenClaw,
            AgentType::ClaudeCode,
            AgentType::Pi,
            AgentType::DeepSeek,
            AgentType::Codex,
        ] {
            assert!(plan(runtime, &config).unwrap().runtime_env.is_empty());
        }
    }
}
