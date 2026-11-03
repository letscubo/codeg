//! OpenClaw ACP child hook and separately loadable Gateway plugin artifacts.
//! Materialization never edits OpenClaw config or starts/restarts a Gateway.
use std::path::Path;

use crate::models::agent::AgentType;

const RESOURCES: &[(&str, &str)] = &[
    (
        "package.json",
        include_str!("../../resources/openclaw/package.json"),
    ),
    (
        "compat.mjs",
        include_str!("../../resources/openclaw/compat.mjs"),
    ),
    (
        "native-gateway.mjs",
        include_str!("../../resources/openclaw/native-gateway.mjs"),
    ),
    (
        "plugin.mjs",
        include_str!("../../resources/openclaw/plugin.mjs"),
    ),
    (
        "openclaw.plugin.json",
        include_str!("../../resources/openclaw/openclaw.plugin.json"),
    ),
    (
        "acp-bridge.mjs",
        include_str!("../../resources/openclaw/acp-bridge.mjs"),
    ),
    (
        "acp-bootstrap.mjs",
        include_str!("../../resources/openclaw/acp-bootstrap.mjs"),
    ),
    (
        "backend-lifecycle.mjs",
        include_str!("../../resources/openclaw/backend-lifecycle.mjs"),
    ),
];

fn plan(
    env: &[(String, String)],
    dir: &Path,
    inherited: Option<&str>,
) -> Option<Vec<(String, String)>> {
    let value = |name: &str| {
        env.iter()
            .rev()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    if value("CODEG_OPENCLAW_NATIVE_BOOTSTRAP")
        .is_some_and(|s| matches!(s, "0" | "false" | "off" | "stock"))
    {
        return None;
    }
    let file = dir.join("acp-bootstrap.mjs").to_str()?.to_string();
    let import = format!("--import={}", serde_json::to_string(&file).ok()?);
    let original = value("NODE_OPTIONS").or(inherited).unwrap_or_default();
    let mut result = env.to_vec();
    result.retain(|(key, _)| key != "NODE_OPTIONS");
    result.push((
        "NODE_OPTIONS".into(),
        format!("{original} {import}").trim().to_string(),
    ));
    Some(result)
}

pub(super) fn prepare_launch_env(agent: AgentType, env: &mut Vec<(String, String)>) -> bool {
    if agent != AgentType::OpenClaw {
        return false;
    }
    let root = crate::paths::codeg_home_dir().join("owned-bootstrap");
    match super::owned_bootstrap::materialize_owned_resources(&root, "openclaw-v1", RESOURCES) {
        Ok(dir) => {
            let inherited = std::env::var("NODE_OPTIONS").ok();
            if let Some(planned) = plan(env, &dir, inherited.as_deref()) {
                *env = planned;
                true
            } else {
                false
            }
        }
        Err(error) => {
            tracing::warn!(
                "[ACP][OpenClaw] owned bootstrap unavailable: {error}; using ordinary ACP"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn myclaw_prompt_openclaw_bootstrap_preserves_env_and_quotes_import() {
        let planned = plan(
            &[("NODE_OPTIONS".into(), "--no-warnings".into())],
            Path::new("/tmp/a b"),
            Some("--trace-warnings"),
        )
        .unwrap();
        assert_eq!(
            planned.last().unwrap().1,
            "--no-warnings --import=\"/tmp/a b/acp-bootstrap.mjs\""
        );
        let empty = plan(
            &[("NODE_OPTIONS".into(), String::new())],
            Path::new("/tmp/a"),
            Some("--trace-warnings"),
        )
        .unwrap();
        assert!(!empty.last().unwrap().1.contains("trace-warnings"));
        assert!(plan(
            &[("CODEG_OPENCLAW_NATIVE_BOOTSTRAP".into(), "off".into())],
            Path::new("/tmp/a"),
            None
        )
        .is_none());
    }
}
