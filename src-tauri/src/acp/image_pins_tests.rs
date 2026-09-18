//! fork(letscubo)专属: `docker/Dockerfile.all` (myclaw-all 镜像) 与 registry 的 pin 一致。
//!
//! myclaw-deploy 打包时拉所选 codeg tag 下的这份 Dockerfile 原样构建,不再覆盖版本;
//! 镜像里预装的 agent 版本若与本 codeg 的 pin 不同,首次使用就会被当成要升级/重装。

use super::registry::{get_agent_meta, AgentDistribution};
use crate::models::agent::AgentType;

const DOCKERFILE_ALL: &str = include_str!("../../../docker/Dockerfile.all");

/// Dockerfile.all 里的 ARG ↔ registry 里 pin 了版本的 agent。
const PINNED: &[(AgentType, &str)] = &[
    (AgentType::OpenClaw, "V_OPENCLAW"),
    (AgentType::ClaudeCode, "V_CLAUDE_CODE"),
    (AgentType::Pi, "V_PI"),
    (AgentType::DeepSeek, "V_DSH"),
    (AgentType::Hermes, "V_HERMES_BRIDGE"),
];

fn arg_default(name: &str) -> Option<&'static str> {
    let prefix = format!("ARG {name}=");
    DOCKERFILE_ALL
        .lines()
        .find_map(|line| line.trim().strip_prefix(prefix.as_str()))
        .map(str::trim)
}

#[test]
fn dockerfile_all_matches_registry_pins() {
    for (agent_type, arg) in PINNED {
        let pinned = match get_agent_meta(*agent_type).distribution {
            AgentDistribution::Npx { version, .. } => version,
            other => panic!("expected npx distribution for {agent_type:?}, got {other:?}"),
        };
        assert_eq!(
            arg_default(arg),
            Some(pinned),
            "docker/Dockerfile.all 的 {arg} 与 registry 里 {agent_type:?} 的 pin 不一致"
        );
    }
}

#[test]
fn codeg_itself_comes_from_the_build_args() {
    // tag 里的文件不知道自己那次 release 的 sha,两者都必须由打包时传入
    assert_eq!(arg_default("CODEG_VERSION"), None);
    assert_eq!(arg_default("CODEG_SHA256"), None);
    assert!(DOCKERFILE_ALL
        .lines()
        .any(|l| l.trim() == "ARG CODEG_VERSION"));
    assert!(DOCKERFILE_ALL
        .lines()
        .any(|l| l.trim() == "ARG CODEG_SHA256"));
}
