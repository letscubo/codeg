//! Database-free: serde, argv, model metadata and YAML patch construction only.
use super::super::dsh_profile::{render_patch_with_options, DshLaunchProfile};
use super::super::effort::{CliTurnOptions, ALL_LEVELS};
use super::*;
use serde_json::json;

fn model(levels: &[&str], default: Option<&str>) -> ModelCapability {
    serde_json::from_value(json!({
        "model":"exact-model", "provider":"myclaw", "known":true,
        "levels":levels, "defaultLevel":default,
    }))
    .unwrap()
}

#[test]
fn myclaw_prompt_native_levels_reject_missing_efforts_and_preserve_clear() {
    let info = model(&["none", "low", "high", "max"], Some("high"));
    for runtime in [AgentType::Pi, AgentType::DeepSeek] {
        assert_eq!(
            resolve_model_level(runtime, &info, &Some(Some("none".into()))).unwrap(),
            Some("off".into())
        );
        assert_eq!(
            resolve_model_level(runtime, &info, &Some(Some("max".into()))).unwrap(),
            Some("max".into())
        );
        assert!(resolve_model_level(runtime, &info, &Some(Some("medium".into()))).is_err());
        assert_eq!(resolve_model_level(runtime, &info, &None).unwrap(), None);
    }
    assert_eq!(
        resolve_model_level(AgentType::Pi, &info, &Some(None)).unwrap(),
        Some("high".into())
    );
    assert_eq!(
        resolve_model_level(AgentType::DeepSeek, &info, &Some(None)).unwrap(),
        None
    );
    let plain = model(&[], Some("off"));
    assert_eq!(
        resolve_model_level(AgentType::Pi, &plain, &Some(None)).unwrap(),
        Some("off".into())
    );
    assert_eq!(
        resolve_model_level(AgentType::DeepSeek, &plain, &Some(None)).unwrap(),
        None
    );
    assert!(resolve_model_level(AgentType::Pi, &plain, &Some(Some("high".into()))).is_err());
    let mut unverified = model(&[], None);
    unverified.known = false;
    assert!(resolve_model_level(AgentType::DeepSeek, &unverified, &Some(None)).is_err());
    let mut uncertain_default = model(&["low", "high"], Some("high"));
    uncertain_default.clear_error = Some("unverified project trust".into());
    assert!(resolve_model_level(AgentType::Pi, &uncertain_default, &Some(None)).is_err());
}

#[test]
fn myclaw_prompt_native_capability_requests_preserve_exact_route_identity() {
    let requests = vec![ModelRequest {
        model: "same-model".into(),
        provider: Some("myclaw".into()),
    }];
    assert!(validate_models(&requests).is_ok());
    let mut ambiguous = requests.clone();
    ambiguous.push(ModelRequest {
        model: "same-model".into(),
        provider: Some("other".into()),
    });
    assert!(validate_models(&ambiguous).is_err());
    assert!(validate_models(&[ModelRequest {
        model: " ".into(),
        provider: None
    }])
    .is_err());
    assert!(ALL_LEVELS.contains(&"minimal"));
    assert!(ALL_LEVELS.contains(&"none"));
    for value in ["stock", " Stock ", "STOCK"] {
        assert!(super::super::dsh_profile::runner_is_stock(&BTreeMap::from(
            [("CODEG_DSH_RUNNER".into(), value.into())]
        )));
    }
}

#[test]
fn myclaw_prompt_pi_args_pin_effort_and_route_on_each_resumed_turn() {
    for thinking in [Some("low"), Some("max"), Some("off"), Some("high"), None] {
        let args = super::super::pi_driver::build_args_with_thinking(
            Some("exact-session"),
            Some("myclaw-anthropic"),
            Some("exact-model"),
            None,
            thinking,
        );
        assert!(args
            .windows(2)
            .any(|v| v == ["--session-id", "exact-session"]));
        assert!(args
            .windows(2)
            .any(|v| v == ["--provider", "myclaw-anthropic"]));
        assert!(args.windows(2).any(|v| v == ["--model", "exact-model"]));
        match thinking {
            Some(level) => assert!(args.windows(2).any(|v| v == ["--thinking", level])),
            None => assert!(!args.iter().any(|v| v == "--thinking")),
        }
    }
}

#[test]
fn myclaw_prompt_dsh_patch_uses_runner_selection_without_mutating_default_schema() {
    let profile = DshLaunchProfile {
        dsh_home: "/not-created".into(),
        plugin_path: "/not-created/tool.mjs".into(),
        companion: None,
        tool_search_mode: "off".into(),
        runner_path: Some("/not-created/runner.mjs".into()),
        background_wait_secs: 30,
    };
    for effort in [Some("high"), Some("off"), None] {
        let options = CliTurnOptions {
            model: Some("exact-model".into()),
            provider: Some("myclaw".into()),
            reasoning_effort: Some(effort.map(str::to_owned)),
        };
        let yaml = render_patch_with_options(
            &profile,
            Some("myclaw"),
            Some("exact-model"),
            Some(&options),
        );
        let entries: serde_json::Value = serde_yaml::from_str(&yaml.replace("!!js ", "")).unwrap();
        let entries = entries.as_array().unwrap();
        let default = entries
            .iter()
            .find(|v| v["id"] == "agent-default-model")
            .unwrap();
        assert!(default["config"].get("reasoningEffort").is_none());
        let runner = entries
            .iter()
            .filter_map(|v| v["insert"].as_array())
            .flatten()
            .find(|v| v["id"] == "codeg-headless-runner")
            .unwrap();
        assert_eq!(runner["config"]["provider"], "myclaw");
        assert!(runner.get("provider").is_none());
        assert_eq!(runner["config"]["model"], "exact-model");
        assert_eq!(runner["config"]["reasoningEffort"], json!(effort));
    }
    let legacy = render_patch_with_options(&profile, Some("myclaw"), Some("exact-model"), None);
    assert!(!legacy.contains("reasoningEffort"));
}

#[test]
fn myclaw_prompt_native_probe_cache_invalidates_on_configuration_or_env_change() {
    let dir = tempfile::tempdir().unwrap();
    let executable = dir.path().join("not-executed");
    std::fs::write(&executable, "not an executable").unwrap();
    let home = dir.path().join("agent");
    std::fs::create_dir(&home).unwrap();
    let config = home.join("models.json");
    std::fs::write(&config, r#"{"models":{"effort":"low"}}"#).unwrap();
    let context = CliEffortContext {
        agent_type: AgentType::Pi,
        provider: Some("myclaw".into()),
        executable,
        working_dir: dir.path().to_path_buf(),
        runtime_env: BTreeMap::from([("PI_CODING_AGENT_DIR".into(), home.display().to_string())]),
    };
    let query = ModelRequest {
        model: "exact-model".into(),
        provider: Some("myclaw".into()),
    };
    let result = ProbeResult {
        version: "0.84.2".into(),
        models: vec![model(&["low"], Some("low"))],
    };
    let before = cache::fingerprint(&context).unwrap();
    cache::put(&before, &result);
    assert!(cache::get(&context, std::slice::from_ref(&query)).is_some());
    std::fs::write(&config, r#"{"models":{"effort":"max"}}"#).unwrap();
    assert!(cache::get(&context, std::slice::from_ref(&query)).is_none());
    let after = cache::fingerprint(&context).unwrap();
    assert_ne!(before, after);
    cache::put(&after, &result);
    let mut changed_env = context.clone();
    changed_env
        .runtime_env
        .insert("NEW_PROVIDER_CONFIG".into(), "changed".into());
    assert!(cache::get(&changed_env, std::slice::from_ref(&query)).is_none());
    std::fs::write(home.join("settings.json"), "{}").unwrap();
    assert!(cache::get(&context, std::slice::from_ref(&query)).is_none());
}
