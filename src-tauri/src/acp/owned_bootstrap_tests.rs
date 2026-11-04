//! Pure launch plans and temporary resource files only. No ACP runtime imports,
//! process launch, ConnectionManager, database or provider/model calls.
use super::*;

#[test]
fn myclaw_prompt_hermes_owned_bootstrap_snapshot_enables_both_bridges() {
    let env = vec![(
        crate::acp::hermes_reasoning::SNAPSHOT_ENV.into(),
        "low".into(),
    )];
    let original = env.clone();
    let result = plan_launch_env(AgentType::Hermes, &env, Path::new("owned"), None, None).unwrap();
    assert_eq!(env_value(&result, HERMES_MARKER), Some("1"));
    assert_eq!(env_value(&result, REASONING_MARKER), Some("1"));
    assert_eq!(
        env_value(&result, crate::acp::hermes_reasoning::SNAPSHOT_ENV),
        Some("low")
    );
    assert_eq!(env, original);
    assert!(plan_launch_env(AgentType::ClaudeCode, &env, Path::new("owned"), None, None).is_none());
}

#[test]
fn myclaw_prompt_hermes_owned_bootstrap_native_opt_out_keeps_snapshot_enabled() {
    for opt_out in ["0", "false", "OFF", "stock"] {
        let env = vec![
            (HERMES_MARKER.into(), opt_out.into()),
            (
                crate::acp::hermes_reasoning::SNAPSHOT_ENV.into(),
                "high".into(),
            ),
        ];
        let result =
            plan_launch_env(AgentType::Hermes, &env, Path::new("owned"), None, None).unwrap();
        assert_eq!(env_value(&result, HERMES_MARKER), Some(opt_out));
        assert_eq!(env_value(&result, REASONING_MARKER), Some("1"));
        assert_eq!(
            env_value(&result, crate::acp::hermes_reasoning::SNAPSHOT_ENV),
            Some("high")
        );
    }
}

#[test]
fn myclaw_prompt_owned_bootstrap_preserves_child_env_and_inherited_python_path() {
    let env = vec![
        ("HERMES_HOME".into(), "/identity-a".into()),
        ("TOKEN".into(), "fixture-value".into()),
    ];
    let inherited = std::env::join_paths([Path::new("alpha"), Path::new("beta")]).unwrap();
    let result = plan_launch_env(
        AgentType::Hermes,
        &env,
        Path::new("owned"),
        Some(&inherited),
        None,
    )
    .unwrap();
    assert_eq!(env_value(&result, "HERMES_HOME"), Some("/identity-a"));
    assert_eq!(env_value(&result, "TOKEN"), Some("fixture-value"));
    assert_eq!(env_value(&result, HERMES_MARKER), Some("1"));
    assert_eq!(env_value(&result, HERMES_OWNED_DIR), Some("owned"));
    let paths: Vec<_> = std::env::split_paths(env_value(&result, PYTHON_PATH).unwrap()).collect();
    assert_eq!(
        paths,
        vec![
            PathBuf::from("owned"),
            PathBuf::from("alpha"),
            PathBuf::from("beta")
        ]
    );
    assert_eq!(env.len(), 2, "the source environment is immutable");
}

#[test]
fn myclaw_prompt_owned_bootstrap_explicit_empty_suppresses_inherited_python_path() {
    let env = vec![(PYTHON_PATH.into(), String::new())];
    let result = plan_launch_env(
        AgentType::Hermes,
        &env,
        Path::new("owned"),
        Some(OsStr::new("ambient")),
        None,
    )
    .unwrap();
    assert_eq!(env_value(&result, PYTHON_PATH), Some("owned"));
    let env = vec![(PYTHON_PATH.into(), "explicit".into())];
    let result = plan_launch_env(
        AgentType::Hermes,
        &env,
        Path::new("owned"),
        Some(OsStr::new("ambient")),
        None,
    )
    .unwrap();
    assert_eq!(
        std::env::split_paths(env_value(&result, PYTHON_PATH).unwrap()).collect::<Vec<_>>(),
        vec![PathBuf::from("owned"), PathBuf::from("explicit")]
    );
}

#[test]
fn myclaw_prompt_owned_bootstrap_unrelated_agent_and_opt_out_are_unchanged() {
    let env = vec![(PYTHON_PATH.into(), "preserved".into())];
    for kind in [
        AgentType::OpenClaw,
        AgentType::DeepSeek,
        AgentType::ClaudeCode,
    ] {
        assert!(plan_launch_env(kind, &env, Path::new("owned"), None, None).is_none());
    }
    for value in ["0", "false", "OFF", "stock"] {
        let opt_out = vec![(HERMES_MARKER.into(), value.into())];
        assert!(
            plan_launch_env(AgentType::Hermes, &opt_out, Path::new("owned"), None, None).is_none()
        );
    }
}

#[test]
fn myclaw_prompt_owned_bootstrap_node_options_preserve_explicit_and_removed_values() {
    let import = |dir: &str| {
        format!(
            "--import={}",
            serde_json::to_string(Path::new(dir).join("node-bootstrap.mjs").to_str().unwrap())
                .unwrap()
        )
    };
    let env = vec![(NODE_OPTIONS.into(), "--no-warnings".into())];
    let plan = plan_launch_env(
        AgentType::Hermes,
        &env,
        Path::new("owned path"),
        None,
        Some(OsStr::new("--trace-warnings")),
    )
    .unwrap();
    assert_eq!(
        env_value(&plan, NODE_OPTIONS),
        Some(format!("--no-warnings {}", import("owned path")).as_str())
    );
    let empty = vec![(NODE_OPTIONS.into(), String::new())];
    let plan = plan_launch_env(
        AgentType::Hermes,
        &empty,
        Path::new("owned"),
        None,
        Some(OsStr::new("--trace-warnings")),
    )
    .unwrap();
    assert_eq!(
        env_value(&plan, NODE_OPTIONS),
        Some(import("owned").as_str())
    );
    let plan = plan_launch_env(
        AgentType::Hermes,
        &[],
        Path::new("owned"),
        None,
        Some(OsStr::new("--trace-warnings")),
    )
    .unwrap();
    assert_eq!(
        env_value(&plan, NODE_OPTIONS),
        Some(format!("--trace-warnings {}", import("owned")).as_str())
    );
}

#[test]
fn myclaw_prompt_owned_bootstrap_resources_are_content_addressed_and_private() {
    let tmp = tempfile::tempdir().unwrap();
    let first = [
        Resource {
            name: "sitecustomize.py",
            source: "first",
        },
        Resource {
            name: "codeg_native_steering.py",
            source: "module",
        },
    ];
    let second = [
        Resource {
            name: "sitecustomize.py",
            source: "second",
        },
        Resource {
            name: "codeg_native_steering.py",
            source: "module",
        },
    ];
    let a = materialize(tmp.path(), &first).unwrap();
    assert_eq!(materialize(tmp.path(), &first).unwrap(), a);
    let b = materialize(tmp.path(), &second).unwrap();
    assert_ne!(a, b);
    assert_eq!(
        std::fs::read_to_string(a.join("sitecustomize.py")).unwrap(),
        "first"
    );
    assert_eq!(
        std::fs::read_to_string(b.join("sitecustomize.py")).unwrap(),
        "second"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&a).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(a.join("sitecustomize.py"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn myclaw_prompt_owned_bootstrap_parallel_materialization_publishes_complete_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let root = root.clone();
            std::thread::spawn(move || {
                materialize(
                    &root,
                    &[Resource {
                        name: "sitecustomize.py",
                        source: "complete-source",
                    }],
                )
                .unwrap()
            })
        })
        .collect();
    let paths: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert!(paths.iter().all(|path| path == &paths[0]));
    assert_eq!(
        std::fs::read_to_string(paths[0].join("sitecustomize.py")).unwrap(),
        "complete-source"
    );
}

#[test]
fn myclaw_prompt_owned_bootstrap_namespaces_cannot_escape_owned_root() {
    let tmp = tempfile::tempdir().unwrap();
    let resources = [("plugin.mjs", "owned-source")];
    let first = materialize_owned_resources(tmp.path(), "openclaw-v1", &resources).unwrap();
    let second = materialize_owned_resources(tmp.path(), "hermes-v1", &resources).unwrap();
    assert_ne!(first, second);
    for namespace in ["../escape", "/absolute", "", "space name"] {
        assert!(materialize_owned_resources(tmp.path(), namespace, &resources).is_err());
    }
    for file in [
        "..",
        "../escape.py",
        "a/b.py",
        "a\\b.py",
        "C:escape.py",
        "a\0.py",
    ] {
        assert!(materialize_owned_resources(tmp.path(), "valid", &[(file, "source")]).is_err());
    }
}
