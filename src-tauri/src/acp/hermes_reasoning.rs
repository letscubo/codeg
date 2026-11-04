//! Snapshot admission and handshake for the shared owned Hermes bootstrap.
//! The original Hermes launcher remains responsible for Python and its environment.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::acp::error::AcpError;
use crate::models::agent::AgentType;

pub const SNAPSHOT_ENV: &str = "CODEG_HERMES_REASONING_EFFORT";
const HANDSHAKE: &str = "codegHermesReasoningSnapshotV1";

pub(crate) fn snapshot_required(
    agent_type: AgentType,
    runtime_env: &BTreeMap<String, String>,
) -> bool {
    agent_type == AgentType::Hermes && runtime_env.contains_key(SNAPSHOT_ENV)
}

pub(crate) fn validate_snapshot(
    agent_type: AgentType,
    runtime_env: &BTreeMap<String, String>,
) -> Result<(), AcpError> {
    if !snapshot_required(agent_type, runtime_env) {
        return Ok(());
    }
    let valid = runtime_env.get(SNAPSHOT_ENV).is_some_and(|effort| {
        [
            "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra", "_inherit",
        ]
        .contains(&effort.as_str())
    });
    if valid {
        Ok(())
    } else {
        Err(AcpError::protocol("Unsupported Hermes reasoning snapshot"))
    }
}

pub(crate) fn init_confirms_snapshot(meta: Option<&Map<String, Value>>) -> bool {
    meta.and_then(|m| m.get(HANDSHAKE)).and_then(Value::as_bool) == Some(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn myclaw_prompt_hermes_snapshot_gate_and_handshake_are_strict_and_hermes_scoped() {
        let env = BTreeMap::from([(SNAPSHOT_ENV.into(), "low".into())]);
        assert!(snapshot_required(AgentType::Hermes, &env));
        assert!(!snapshot_required(AgentType::OpenClaw, &env));
        assert!(!snapshot_required(AgentType::Hermes, &BTreeMap::new()));
        assert!(init_confirms_snapshot(Some(
            serde_json::json!({"codegHermesReasoningSnapshotV1":true})
                .as_object()
                .unwrap()
        )));
        assert!(!init_confirms_snapshot(Some(
            serde_json::json!({"codegHermesReasoningSnapshotV1":"true"})
                .as_object()
                .unwrap()
        )));
        assert!(!init_confirms_snapshot(None));
    }

    #[test]
    fn myclaw_prompt_hermes_invalid_snapshot_is_rejected_before_bootstrap() {
        for value in [
            "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra", "_inherit",
        ] {
            let env = BTreeMap::from([(SNAPSHOT_ENV.into(), value.into())]);
            assert!(validate_snapshot(AgentType::Hermes, &env).is_ok());
        }
        for value in ["", " low ", "off", "unknown"] {
            let env = BTreeMap::from([(SNAPSHOT_ENV.into(), value.into())]);
            assert!(validate_snapshot(AgentType::Hermes, &env).is_err());
            assert!(validate_snapshot(AgentType::OpenClaw, &env).is_ok());
        }
        assert!(validate_snapshot(AgentType::Hermes, &BTreeMap::new()).is_ok());
    }
}
