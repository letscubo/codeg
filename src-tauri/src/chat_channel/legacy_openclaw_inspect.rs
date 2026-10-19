//! Read-only, conservative discovery of the existing native Telegram DM routes.
//! Official contracts: docs.openclaw.ai/cli/config, /cli/gateway/query,
//! /channels/telegram/access-control; openclaw/openclaw gateway sessions.list
//! exposes SessionOrigin (provider/chatType/from/to/accountId/threadId).
//! Never derive a session key, write a config, start a gateway, or install a CLI.
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, path::Path, time::Duration};
use tokio::io::AsyncReadExt;

fn unavailable(reason: &str) -> Value {
    json!({"kind":"openclaw","ready":false,"reasonCodes":[reason]})
}
async fn cli(binary: &Path, args: &[&str]) -> Result<Value, &'static str> {
    let mut child = tokio::process::Command::new(binary)
        .args(args)
        .env("OPENCLAW_CONFIG_READONLY", "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| "openclaw_cli_unavailable")?;
    let mut stdout = child.stdout.take().ok_or("openclaw_cli_unavailable")?;
    let read = async {
        let mut bytes = Vec::new();
        (&mut stdout)
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| "openclaw_inspection_read_failed")?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err("openclaw_inspection_too_large");
        }
        let status = child
            .wait()
            .await
            .map_err(|_| "openclaw_inspection_read_failed")?;
        if !status.success() {
            return Err("openclaw_inspection_unsupported");
        }
        serde_json::from_slice(&bytes).map_err(|_| "openclaw_inspection_invalid_json")
    };
    tokio::time::timeout(Duration::from_secs(25), read)
        .await
        .map_err(|_| "openclaw_inspection_timeout")?
}
fn numeric_id(value: &Value) -> Option<String> {
    let value = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|v| v.to_string()))?;
    let value = value
        .strip_prefix("telegram:")
        .or_else(|| value.strip_prefix("tg:"))
        .unwrap_or(&value);
    (!value.is_empty()
        && value.bytes().all(|b| b.is_ascii_digit())
        && value.parse::<u64>().ok()? > 0)
        .then(|| value.to_string())
}
fn nonempty(value: &Value) -> bool {
    !value.is_null()
        && !value.as_object().is_some_and(|v| v.is_empty())
        && !value.as_array().is_some_and(|v| v.is_empty())
}
fn contains_include(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("$include") || object.values().any(contains_include)
        }
        Value::Array(array) => array.iter().any(contains_include),
        _ => false,
    }
}
fn inspect_snapshot(
    config: &Value,
    status: &Value,
    listing: &Value,
) -> Result<Value, &'static str> {
    if contains_include(config) {
        return Err("openclaw_config_include_unsupported");
    }
    let tg = &config["channels"]["telegram"];
    if !tg.is_object() {
        return Err("openclaw_telegram_not_configured");
    }
    if nonempty(&tg["accounts"])
        || nonempty(&config["bindings"])
        || nonempty(&tg["direct"])
        || nonempty(&tg["dms"])
        || nonempty(&tg["groups"])
    {
        return Err("openclaw_complex_routing_unsupported");
    }
    // Both saved configuration and the live Gateway must confirm ownership was
    // relinquished. An absent channel or unavailable Gateway is not evidence.
    let accounts = status["channelAccounts"]["telegram"]
        .as_array()
        .ok_or("openclaw_native_state_unconfirmed")?;
    if tg["enabled"] != false
        || accounts.len() != 1
        || accounts[0]["accountId"] != "default"
        || accounts[0]["enabled"] != false
        || accounts[0]["running"] != false
        || status["partial"] == true
    {
        return Err("openclaw_native_delivery_active_or_unknown");
    }
    let token = tg["botToken"]
        .as_str()
        .filter(|s| {
            !s.is_empty()
                && !s.contains("${")
                && !s.contains("REDACTED")
                && s.split_once(':').is_some_and(|(id, secret)| {
                    id.bytes().all(|b| b.is_ascii_digit()) && !secret.is_empty()
                })
        })
        .ok_or("openclaw_bot_identity_unconfirmed")?;
    if nonempty(&tg["tokenFile"]) || nonempty(&tg["apiRoot"]) {
        return Err("openclaw_token_or_transport_unsupported");
    }
    let allow = tg["allowFrom"]
        .as_array()
        .ok_or("openclaw_dm_policy_unsupported")?;
    let allow_all = tg["dmPolicy"] == "open" && allow.iter().any(|v| v == "*");
    let allowed: Option<Vec<String>> = if allow_all {
        Some(Vec::new())
    } else {
        allow.iter().map(numeric_id).collect()
    };
    let allowed = allowed.ok_or("openclaw_dm_policy_unsupported")?;
    if !allow_all && (tg["dmPolicy"] != "allowlist" || allowed.is_empty()) {
        return Err("openclaw_dm_policy_unsupported");
    }
    if listing["hasMore"] == true {
        return Err("openclaw_session_inventory_incomplete");
    }
    let rows = listing["sessions"]
        .as_array()
        .ok_or("openclaw_session_inventory_unavailable")?;
    if rows.len() >= 200 {
        return Err("openclaw_session_inventory_incomplete");
    }
    let mut sessions = Vec::new();
    let mut targets = HashSet::new();
    for row in rows {
        let origin = &row["origin"];
        if origin["provider"] != "telegram" {
            continue;
        }
        if origin["chatType"] != "direct" || !origin["threadId"].is_null() {
            return Err("openclaw_telegram_groups_or_topics_unsupported");
        }
        if origin["accountId"].as_str().is_some_and(|v| v != "default") {
            return Err("openclaw_multiple_accounts_unsupported");
        }
        let chat = numeric_id(&origin["from"]).ok_or("openclaw_session_origin_incomplete")?;
        // A direct source and its outbound route must identify the same peer.
        let to = numeric_id(&origin["to"]).ok_or("openclaw_session_origin_incomplete")?;
        if chat != to {
            return Err("openclaw_session_origin_ambiguous");
        }
        if !allow_all && !allowed.contains(&chat) {
            continue;
        }
        let key = row["key"]
            .as_str()
            .filter(|s| s.starts_with("agent:") && s.len() <= 512)
            .ok_or("openclaw_session_identity_missing")?;
        let id = row["sessionId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("openclaw_session_identity_missing")?;
        if !targets.insert(chat.clone()) {
            return Err("openclaw_session_origin_ambiguous");
        }
        sessions.push(json!({"sessionKey":key,"nativeSessionId":id,"chatId":chat,"threadId":null,"chatType":"private","senderId":chat}));
    }
    if sessions.is_empty() {
        return Err("openclaw_existing_telegram_sessions_missing");
    }
    Ok(
        json!({"kind":"openclaw","ready":true,"nativeDeliveryDisabled":true,"botTokenSha256":format!("{:x}",Sha256::digest(token.as_bytes())),"policy":{"allowAllUsers":allow_all,"allowedUserIds":allowed,"requireMention":false},"sessions":sessions,"reasonCodes":[]}),
    )
}
/// Fail closed on unsupported versions or ambiguous policies; only returns a
/// token fingerprint and existing session references, never credentials/history.
pub async fn inspect() -> Result<Value, String> {
    let result = async {
        let binary = std::env::var("CODEG_LEGACY_OPENCLAW_CLI")
            .map(std::path::PathBuf::from)
            .map_err(|_| "openclaw_native_cli_not_configured")?;
        if !binary.is_absolute() || !binary.is_file() {
            return Err("openclaw_native_cli_unavailable");
        }
        let location = cli(&binary, &["config", "file", "--json"]).await?;
        let path = location["path"]
            .as_str()
            .filter(|p| Path::new(p).is_absolute())
            .ok_or("openclaw_config_path_unavailable")?;
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|_| "openclaw_config_unreadable")?;
        if !metadata.is_file() || metadata.len() > 1024 * 1024 {
            return Err("openclaw_config_unsupported");
        }
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| "openclaw_config_unreadable")?;
        let config: Value =
            serde_json::from_slice(&bytes).map_err(|_| "openclaw_json5_config_unsupported")?;
        let status = cli(
            &binary,
            &[
                "gateway",
                "call",
                "channels.status",
                "--params",
                "{\"probe\":false}",
                "--json",
            ],
        )
        .await?;
        let listing = cli(
            &binary,
            &[
                "gateway",
                "call",
                "sessions.list",
                "--params",
                "{\"limit\":200,\"includeDerivedTitles\":false,\"includeLastMessage\":false}",
                "--json",
            ],
        )
        .await?;
        let mut result = inspect_snapshot(&config, &status, &listing)?;
        result["runtimeEnv"] = json!({"OPENCLAW_ACP_COMMAND":binary.to_string_lossy()});
        Ok(result)
    }
    .await;
    Ok(result.unwrap_or_else(unavailable))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn data() -> (Value, Value, Value) {
        (
            json!({"channels":{"telegram":{"enabled":false,"botToken":"123:secret","dmPolicy":"allowlist","allowFrom":["tg:456"]}}}),
            json!({"channelAccounts":{"telegram":[{"accountId":"default","enabled":false,"running":false}]}}),
            json!({"sessions":[{"key":"agent:original:telegram:direct:456","sessionId":"existing-id","origin":{"provider":"telegram","chatType":"direct","from":"telegram:456","to":"telegram:456","accountId":"default"}}]}),
        )
    }
    #[test]
    fn preserves_exact_route_without_returning_token() {
        let (c, s, l) = data();
        let result = inspect_snapshot(&c, &s, &l).unwrap();
        assert_eq!(result["sessions"][0]["sessionKey"], l["sessions"][0]["key"]);
        assert!(!result.to_string().contains("secret"));
        assert_eq!(result["policy"]["allowedUserIds"], json!(["456"]));
    }
    #[test]
    fn requires_live_disabled_adapter_and_unambiguous_origin() {
        let (c, mut s, mut l) = data();
        s["channelAccounts"]["telegram"][0]["running"] = true.into();
        assert!(inspect_snapshot(&c, &s, &l).is_err());
        s["channelAccounts"]["telegram"][0]["running"] = false.into();
        l["sessions"][0]["origin"]["to"] = "telegram:999".into();
        assert!(inspect_snapshot(&c, &s, &l).is_err());
        l["sessions"][0]["origin"]["to"] = "telegram:456".into();
        l["sessions"][0]["origin"]["threadId"] = 7.into();
        assert!(inspect_snapshot(&c, &s, &l).is_err());
    }
    #[test]
    fn refuses_pairing_complex_configs_and_incomplete_inventory() {
        let (mut c, s, mut l) = data();
        c["channels"]["telegram"]["dmPolicy"] = "pairing".into();
        assert!(inspect_snapshot(&c, &s, &l).is_err());
        c["channels"]["telegram"]["dmPolicy"] = "allowlist".into();
        l["hasMore"] = true.into();
        assert!(inspect_snapshot(&c, &s, &l).is_err());
        l["hasMore"] = false.into();
        c["channels"]["telegram"]["groups"] = json!({"*":{}});
        assert!(inspect_snapshot(&c, &s, &l).is_err());
    }
}
