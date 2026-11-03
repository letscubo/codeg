//! Codeg-owned ACP bootstrap resources. Installing this hook is not a native
//! capability proof: the actual loaded runtime must still verify its source
//! and advertise exact active-turn readiness over the owning ACP connection.
use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::models::agent::AgentType;

const HERMES_MARKER: &str = "CODEG_HERMES_NATIVE_BOOTSTRAP";
const PYTHON_PATH: &str = "PYTHONPATH";
const NODE_OPTIONS: &str = "NODE_OPTIONS";
const HERMES_OWNED_DIR: &str = "CODEG_HERMES_OWNED_BOOTSTRAP_DIR";
const HERMES_RESOURCES: &[Resource<'_>] = &[
    Resource {
        name: "node-bootstrap.mjs",
        source: include_str!("../../resources/hermes/node-bootstrap.mjs"),
    },
    Resource {
        name: "sitecustomize.py",
        source: include_str!("../../resources/hermes/sitecustomize.py"),
    },
    Resource {
        name: "codeg_native_steering.py",
        source: include_str!("../../resources/hermes/codeg_native_steering.py"),
    },
];

struct Resource<'a> {
    name: &'a str,
    source: &'a str,
}

fn env_value<'a>(env: &'a [(String, String)], key: &str) -> Option<&'a str> {
    env.iter()
        .rev()
        .find(|(name, _)| {
            if cfg!(windows) {
                name.eq_ignore_ascii_case(key)
            } else {
                name == key
            }
        })
        .map(|(_, value)| value.as_str())
}

fn disabled(env: &[(String, String)]) -> bool {
    env_value(env, HERMES_MARKER).is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "stock"
        )
    })
}

/// Plan only child-environment additions. A present empty value is the ACP
/// launcher's explicit env_remove convention, so it must suppress inheritance.
fn plan_launch_env(
    agent_type: AgentType,
    env: &[(String, String)],
    script_dir: &Path,
    inherited_python_path: Option<&OsStr>,
    inherited_node_options: Option<&OsStr>,
) -> Option<Vec<(String, String)>> {
    if agent_type != AgentType::Hermes || disabled(env) {
        return None;
    }
    let original = match env_value(env, PYTHON_PATH) {
        Some("") => None,
        Some(value) => Some(OsStr::new(value)),
        None => inherited_python_path,
    };
    let mut paths = vec![script_dir.to_path_buf()];
    if let Some(original) = original {
        paths.extend(std::env::split_paths(original));
    }
    let python_path = std::env::join_paths(paths).ok()?.into_string().ok()?;
    let node_options = match env_value(env, NODE_OPTIONS) {
        Some(value) => value,
        None => inherited_node_options
            .unwrap_or_else(|| OsStr::new(""))
            .to_str()?,
    };
    let node_import =
        serde_json::to_string(script_dir.join("node-bootstrap.mjs").to_str()?).ok()?;
    let mut planned = env.to_vec();
    planned.retain(|(key, _)| {
        if cfg!(windows) {
            ![PYTHON_PATH, HERMES_MARKER, NODE_OPTIONS, HERMES_OWNED_DIR]
                .iter()
                .any(|name| key.eq_ignore_ascii_case(name))
        } else {
            ![PYTHON_PATH, HERMES_MARKER, NODE_OPTIONS, HERMES_OWNED_DIR].contains(&key.as_str())
        }
    });
    planned.push((PYTHON_PATH.into(), python_path));
    planned.push((
        NODE_OPTIONS.into(),
        format!("{node_options} --import={node_import}")
            .trim()
            .into(),
    ));
    planned.push((HERMES_OWNED_DIR.into(), script_dir.to_str()?.into()));
    planned.push((HERMES_MARKER.into(), "1".into()));
    Some(planned)
}

fn source_revision(resources: &[Resource<'_>]) -> String {
    let mut hash = Sha256::new();
    for resource in resources {
        hash.update((resource.name.len() as u64).to_le_bytes());
        hash.update(resource.name.as_bytes());
        hash.update((resource.source.len() as u64).to_le_bytes());
        hash.update(resource.source.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn create_private_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(path)?;
    Ok(())
}

/// Content-addressed paths keep a different Codeg build from overwriting a
/// still-running ACP process's module. Atomic publication prevents a parallel
/// launch from importing a partially written Python file.
fn materialize(root: &Path, resources: &[Resource<'_>]) -> std::io::Result<PathBuf> {
    materialize_named(root, "hermes-v1", resources)
}

/// Shared by explicit Codeg-owned bootstraps. Names describe bundled resources,
/// never user paths; single-component validation keeps writes under this root.
pub(super) fn materialize_owned_resources(
    root: &Path,
    namespace: &str,
    resources: &[(&str, &str)],
) -> std::io::Result<PathBuf> {
    let resources: Vec<_> = resources
        .iter()
        .map(|(name, source)| Resource { name, source })
        .collect();
    materialize_named(root, namespace, &resources)
}

fn materialize_named(
    root: &Path,
    namespace: &str,
    resources: &[Resource<'_>],
) -> std::io::Result<PathBuf> {
    let valid_namespace = !namespace.is_empty()
        && namespace.len() <= 64
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-');
    let valid_names = resources.iter().all(|resource| {
        !resource.name.is_empty()
            && resource.name != "."
            && resource.name != ".."
            && resource
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    });
    if !valid_namespace || !valid_names {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid owned resource name",
        ));
    }
    let dir = root.join(format!("{namespace}-{}", source_revision(resources)));
    create_private_dir(&dir)?;
    for resource in resources {
        let path = dir.join(resource.name);
        if std::fs::read(&path).ok().as_deref() == Some(resource.source.as_bytes()) {
            continue;
        }
        let mut pending = tempfile::NamedTempFile::new_in(&dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            pending
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        pending.write_all(resource.source.as_bytes())?;
        pending.flush()?;
        pending.persist(&path).map_err(|error| error.error)?;
    }
    dir.canonicalize()
}

/// Called after the ordinary environment merge; launcher, argv, cwd, home,
/// credentials and process ownership remain those of the existing ACP launch.
/// Failure leaves the ordinary launcher untouched and native support disabled.
pub(super) fn prepare_launch_env(agent_type: AgentType, env: &mut Vec<(String, String)>) -> bool {
    if agent_type != AgentType::Hermes || disabled(env) {
        return false;
    }
    let inherited: Option<OsString> = std::env::var_os(PYTHON_PATH);
    let inherited_node = std::env::var_os(NODE_OPTIONS);
    let prepared = materialize(
        &crate::paths::codeg_home_dir().join("owned-bootstrap"),
        HERMES_RESOURCES,
    );
    match prepared {
        Ok(dir) => match plan_launch_env(
            agent_type,
            env,
            &dir,
            inherited.as_deref(),
            inherited_node.as_deref(),
        ) {
            Some(planned) => {
                *env = planned;
                true
            }
            None => {
                tracing::warn!(
                    "[ACP][Hermes] owned bootstrap environment unavailable; using ordinary ACP"
                );
                false
            }
        },
        Err(error) => {
            tracing::warn!(
                "[ACP][Hermes] owned bootstrap unavailable: {error}; using ordinary ACP"
            );
            false
        }
    }
}

#[cfg(test)]
#[path = "owned_bootstrap_tests.rs"]
mod tests;
