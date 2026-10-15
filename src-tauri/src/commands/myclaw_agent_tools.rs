//! Merge identity-scoped platform tools into the existing MCP transports.
//!
//! Web remains the authorization boundary. HTTP's explicit platformSessionId is
//! a selector, not proof of the native session that originated the request.

use serde_json::Value;

use crate::acp::delegation::companion::is_embedded_tool_name;
use crate::commands::myclaw_upload::ArtifactUploadAccess;

fn is_legacy(identity: &Value) -> bool {
    matches!(
        identity.get("agentKey").and_then(Value::as_str),
        Some("legacy-openclaw" | "legacy-hermes" | "legacy-claude_code")
    )
}

/// Only add the documented selector. In particular, model arguments cannot
/// replace transport-authenticated agentKey or the stdio conversationId.
pub fn identity_with_selector(mut identity: Value, arguments: &Value) -> Value {
    if let (Some(object), Some(selector)) = (
        identity.as_object_mut(),
        arguments.get("platformSessionId").and_then(Value::as_str),
    ) {
        if !selector.trim().is_empty() {
            object.insert("platformSessionId".into(), Value::String(selector.into()));
        }
    }
    identity
}

fn contains_name(tools: &[Value], name: &str) -> bool {
    tools
        .iter()
        .any(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
}

pub fn append_tools(tools: &mut Vec<Value>, extra: Vec<Value>) {
    for tool in extra {
        let Some(name) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        if !is_embedded_tool_name(name) && !contains_name(tools, name) {
            tools.push(tool);
        }
    }
}

/// No scoped catalog cache: grants and the active actor may change between
/// calls. Legacy principals never request the all-instance management catalog.
pub async fn catalog(
    access: &dyn ArtifactUploadAccess,
    identity: Value,
) -> Result<(Vec<Value>, Option<String>), String> {
    let legacy = is_legacy(&identity);
    let management = async {
        if legacy {
            return Ok((Vec::new(), None));
        }
        let tools = access.platform_catalog().await?;
        Ok::<_, String>((tools, access.platform_overview().await))
    };
    let (management, scoped) = tokio::join!(management, access.agent_tool_catalog(identity));
    let (mut tools, mut overview) = match (management, &scoped) {
        (Ok(result), _) => result,
        (Err(error), Err(_)) => return Err(error),
        (Err(_), Ok(_)) => (Vec::new(), None),
    };
    match scoped {
        Ok((extra, instructions)) => {
            append_tools(&mut tools, extra);
            if let Some(instructions) = instructions {
                overview = Some(match overview {
                    Some(existing) => format!("{existing}\n\n{instructions}"),
                    None => instructions,
                });
            }
        }
        Err(error) if legacy => return Err(error),
        Err(_) => {}
    }
    Ok((tools, overview))
}

pub async fn call(
    access: &dyn ArtifactUploadAccess,
    name: &str,
    arguments: Value,
    identity: Value,
) -> Result<Value, String> {
    let identity = identity_with_selector(identity, &arguments);
    // Existing 3D names always reach the scoped authorizer, even when a fresh
    // directory request fails. Never fall back to the management call endpoint.
    if is_legacy(&identity) || name.starts_with("creative_3d_") {
        return access.agent_tool_call(name, arguments, identity).await;
    }
    // Preserve the existing management call path and its cached directory.
    if let Ok(tools) = access.platform_catalog().await {
        if contains_name(&tools, name) {
            return access.platform_call(name, arguments, identity).await;
        }
    }
    // Future scoped tool names can be published by Web without another Codeg
    // release. Unadvertised names retain the existing platform error behavior.
    if let Ok((tools, _)) = access.agent_tool_catalog(identity.clone()).await {
        if contains_name(&tools, name) {
            return access.agent_tool_call(name, arguments, identity).await;
        }
    }
    access.platform_call(name, arguments, identity).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Stub {
        seen: Mutex<Vec<(String, Value)>>,
        management_unavailable: bool,
        scoped_unavailable: bool,
    }

    #[async_trait::async_trait]
    impl ArtifactUploadAccess for Stub {
        async fn upload(&self, _: &str) -> Result<String, String> {
            panic!("not an upload")
        }

        async fn platform_catalog(&self) -> Result<Vec<Value>, String> {
            self.seen
                .lock()
                .unwrap()
                .push(("management_catalog".into(), Value::Null));
            if self.management_unavailable {
                return Err("management unavailable".into());
            }
            Ok(vec![json!({"name": "list_agents"})])
        }

        async fn platform_call(
            &self,
            name: &str,
            _: Value,
            identity: Value,
        ) -> Result<Value, String> {
            self.seen
                .lock()
                .unwrap()
                .push((format!("management:{name}"), identity));
            Ok(json!({"content": []}))
        }

        async fn agent_tool_catalog(
            &self,
            identity: Value,
        ) -> Result<(Vec<Value>, Option<String>), String> {
            self.seen
                .lock()
                .unwrap()
                .push(("scoped_catalog".into(), identity));
            if self.scoped_unavailable {
                return Err("scoped unavailable".into());
            }
            Ok((
                vec![
                    json!({"name": "creative_3d_list_assets"}),
                    json!({"name": "future_agent_tool"}),
                    json!({"name": "upload_file"}),
                    json!({"name": "list_agents"}),
                    json!({"name": "future_agent_tool"}),
                ],
                Some("Agent tools".into()),
            ))
        }

        async fn agent_tool_call(
            &self,
            name: &str,
            _: Value,
            identity: Value,
        ) -> Result<Value, String> {
            self.seen
                .lock()
                .unwrap()
                .push((format!("scoped:{name}"), identity));
            Ok(json!({"content": []}))
        }
    }

    #[test]
    fn selector_cannot_replace_transport_identity() {
        let identity = identity_with_selector(
            json!({"agentKey": "va-actual", "conversationId": 42}),
            &json!({
                "platformSessionId": "exact-session", "agentKey": "va-fake", "conversationId": 99,
                "userId": "fake", "runtime": "fake"
            }),
        );
        assert_eq!(
            identity,
            json!({"agentKey": "va-actual", "conversationId": 42, "platformSessionId": "exact-session"})
        );
        for args in [
            json!({}),
            json!({"platformSessionId": null}),
            json!({"platformSessionId": " "}),
        ] {
            assert_eq!(
                identity_with_selector(json!({"conversationId": 42}), &args),
                json!({"conversationId": 42})
            );
        }
    }

    #[tokio::test]
    async fn scoped_directory_is_fresh_per_identity_and_filters_reserved_names() {
        let stub = Stub::default();
        for agent in ["va-one", "va-two", "va-one"] {
            let (tools, overview) = catalog(&stub, json!({"agentKey": agent})).await.unwrap();
            let names: Vec<_> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
            assert_eq!(
                names,
                [
                    "list_agents",
                    "creative_3d_list_assets",
                    "future_agent_tool"
                ]
            );
            assert_eq!(overview.as_deref(), Some("Agent tools"));
        }
        let seen = stub.seen.lock().unwrap();
        let identities: Vec<_> = seen
            .iter()
            .filter(|(name, _)| name == "scoped_catalog")
            .map(|(_, identity)| identity["agentKey"].as_str().unwrap())
            .collect();
        assert_eq!(identities, ["va-one", "va-two", "va-one"]);
    }

    #[tokio::test]
    async fn legacy_only_uses_agent_directory_and_call() {
        let stub = Stub {
            management_unavailable: true,
            ..Stub::default()
        };
        let identity = json!({"agentKey": "legacy-openclaw"});
        let (tools, _) = catalog(&stub, identity.clone()).await.unwrap();
        assert!(contains_name(&tools, "creative_3d_list_assets"));
        call(
            &stub,
            "creative_3d_list_assets",
            json!({"platformSessionId": "session-a"}),
            identity,
        )
        .await
        .unwrap();
        let seen = stub.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, "scoped_catalog");
        assert_eq!(
            seen[1],
            (
                "scoped:creative_3d_list_assets".into(),
                json!({"agentKey": "legacy-openclaw", "platformSessionId": "session-a"})
            )
        );
    }

    #[tokio::test]
    async fn management_calls_keep_the_existing_path_without_scoped_catalog() {
        let stub = Stub {
            scoped_unavailable: true,
            ..Stub::default()
        };
        call(
            &stub,
            "list_agents",
            json!({}),
            json!({"agentKey": "va-actual"}),
        )
        .await
        .unwrap();
        let seen = stub.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[1].0, "management:list_agents");
    }

    #[tokio::test]
    async fn creative_call_never_falls_back_to_management_on_catalog_failure() {
        let stub = Stub {
            scoped_unavailable: true,
            ..Stub::default()
        };
        call(
            &stub,
            "creative_3d_create_model",
            json!({"platformSessionId": "exact"}),
            json!({"conversationId": 42}),
        )
        .await
        .unwrap();
        let seen = stub.seen.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[(
                "scoped:creative_3d_create_model".into(),
                json!({"conversationId": 42, "platformSessionId": "exact"})
            )]
        );
    }

    #[tokio::test]
    async fn future_advertised_tool_uses_the_scoped_path() {
        let stub = Stub::default();
        call(
            &stub,
            "future_agent_tool",
            json!({}),
            json!({"conversationId": 42}),
        )
        .await
        .unwrap();
        let seen = stub.seen.lock().unwrap();
        assert_eq!(seen.last().unwrap().0, "scoped:future_agent_tool");
    }

    #[tokio::test]
    async fn scoped_directory_survives_management_unavailable() {
        let stub = Stub {
            management_unavailable: true,
            ..Stub::default()
        };
        let (tools, _) = catalog(&stub, json!({"conversationId": 42})).await.unwrap();
        assert!(contains_name(&tools, "creative_3d_list_assets"));
    }
}
