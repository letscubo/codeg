//! Per-send internal instructions. The platform owns agent_config and durable
//! session history; Codeg adds A+B on the first send and B on every later send and modifies runtime blocks only.
//!
//! Platform integration contract (the MyClaw endpoint is a separate change):
//! signed POST `/api/codeg/prompt/snapshot` using the existing webhook HMAC.
//! Request: `{ "identity": { "conversationId": 42, "agentKey": "...",
//! "nativeSessionId": "...", "historyState": "empty|present|unknown" } }`.
//! History evidence comes from fresh-session creation, local admission, or an
//! exact native transcript. At least one identity locator must be present.
//! Response envelope: `{ "code": 0, "data": { "newSessionPrompt": "...",
//! "everyMessagePrompt": "...", "sessionHasMessages": false } }`.
//! Map agent_config keys New_Session_Prompt / Every_Message_Prompt to the
//! two strings; missing keys should be returned explicitly as empty strings.
//! The endpoint must authenticate the VM and validate identity ownership, then
//! determine whether this logical conversation has accepted messages. An empty
//! pre-created row is new; reconnecting an existing history is not. Do not infer
//! this from connection creation, native-id existence, title, or a stale counter.
//! If history cannot be determined, return an error rather than defaulting false.
//! Local admission covers platform sync lag on the current connection only.
//!
//! No positive/negative config cache: each send reads current values. Managed
//! instances fail closed on missing endpoint, timeout or malformed response;
//! deploy the read-only platform endpoint BEFORE enabling this Codeg build.
//! No platform webhook (or no installed source in desktop mode) means no injection.
//!
//! `blocks` is runtime context, not a protocol-level system role. The separate
//! UserMessage broadcast remains unmodified. Native runtime transcripts can
//! still contain the wrapper: MyClaw history sync/render/export must remove
//! the versioned myclaw_internal_prompt block before this feature is enabled.
//! Native steering / feedback bypasses send_prompt_inner and is out of scope
//! for this first implementation. No DB schema or SQL changes are required here.
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::acp::types::PromptInputBlock;
use crate::db::AppDatabase;

pub const SNAPSHOT_PATH: &str = "/api/codeg/prompt/snapshot";
const MAX_PROMPT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptIdentity {
    pub conversation_id: Option<i32>,
    pub agent_key: Option<String>,
    pub native_session_id: Option<String>,
    pub history_state: HistoryState,
    #[serde(skip)]
    pub agent_type: crate::models::AgentType,
}

/// Native evidence is signed with the instance identity, not supplied by the browser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryState {
    Empty,
    Present,
    Unknown,
}

async fn resolve_native_history(identity: &PromptIdentity) -> HistoryState {
    if identity.history_state != HistoryState::Unknown {
        return identity.history_state;
    }
    let Some(id) = identity.native_session_id.clone() else {
        return HistoryState::Unknown;
    };
    let agent_type = identity.agent_type;
    // Only exact session IDs, with no folder/time/most-recent fallback. This is
    // filesystem/parser reading; never use the history API that repairs DB aliases.
    tokio::task::spawn_blocking(move || {
        use crate::parsers::AgentParser;
        if crate::acp::connection::transcript_dir_for(agent_type).is_some() {
            if let Ok(detail) =
                crate::parsers::acp_native::AcpNativeParser::new(agent_type).get_conversation(&id)
            {
                return if detail.turns.is_empty() {
                    HistoryState::Empty
                } else {
                    HistoryState::Present
                };
            }
        }
        match crate::parsers::build_agent_parser(agent_type).get_conversation(&id) {
            Ok(detail) => {
                if detail.turns.is_empty() {
                    HistoryState::Empty
                } else {
                    HistoryState::Present
                }
            }
            Err(_) => HistoryState::Unknown,
        }
    })
    .await
    .unwrap_or(HistoryState::Unknown)
}

/// Required fields: a missing/invalid config is not an instruction to disable it.
/// Empty strings explicitly disable that phase. Never log this object.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptSnapshot {
    pub new_session_prompt: String,
    pub every_message_prompt: String,
    /// Authoritative persisted history, including resumed sessions. A connection
    /// id or a freshly minted native session id cannot answer this question.
    pub session_has_messages: bool,
}

impl PromptSnapshot {
    pub fn block(&self, accepted_on_connection: bool) -> Result<Option<PromptInputBlock>, String> {
        if self.new_session_prompt.len() > MAX_PROMPT_BYTES
            || self.every_message_prompt.len() > MAX_PROMPT_BYTES
        {
            return Err("internal_prompt_invalid: prompt exceeds 64 KiB".into());
        }
        let existing = self.session_has_messages || accepted_on_connection;
        let phase = if existing {
            "existing_session"
        } else {
            "new_session"
        };
        let text = [
            (!existing).then_some(self.new_session_prompt.as_str()),
            Some(self.every_message_prompt.as_str()),
        ]
        .into_iter()
        .flatten()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
        if text.trim().is_empty() {
            return Ok(None);
        }
        // Escape only our reserved delimiters, preserving arbitrary configured text.
        let text = text
            .replace("<myclaw_internal_prompt", "&lt;myclaw_internal_prompt")
            .replace("</myclaw_internal_prompt", "&lt;/myclaw_internal_prompt");
        Ok(Some(PromptInputBlock::Text {
            text: format!("<myclaw_internal_prompt version=\"1\" phase=\"{phase}\">\n{text}\n</myclaw_internal_prompt>\n\n"),
        }))
    }
}

#[async_trait]
pub trait PromptSource: Send + Sync {
    /// None means this is not a platform-managed instance, not a fetch failure.
    async fn snapshot(&self, identity: &PromptIdentity) -> Result<Option<PromptSnapshot>, String>;
}

pub struct PlatformPromptSource {
    pub db: AppDatabase,
}

#[async_trait]
impl PromptSource for PlatformPromptSource {
    async fn snapshot(&self, identity: &PromptIdentity) -> Result<Option<PromptSnapshot>, String> {
        let hooks = crate::commands::chat_channel::get_chat_event_webhooks_core(&self.db)
            .await
            .map_err(|_| {
                "internal_prompt_unavailable: cannot read platform configuration".to_string()
            })?;
        if crate::commands::myclaw_platform::endpoint_from_hooks(&hooks).is_none() {
            return Ok(None);
        }
        let mut identity = identity.clone();
        identity.history_state = resolve_native_history(&identity).await;
        // Fresh per send, no memory-version suppression or stale-content fallback.
        // post_signed reuses the existing webhook HMAC identity, never a browser key.
        let data = crate::commands::myclaw_platform::post_signed(
            &self.db,
            SNAPSHOT_PATH,
            serde_json::json!({ "identity": identity }),
            Duration::from_secs(4),
        )
        .await
        .map_err(|_| {
            "internal_prompt_unavailable: cannot load platform prompt configuration".to_string()
        })?;
        serde_json::from_value(data)
            .map(Some)
            .map_err(|_| "internal_prompt_invalid: invalid platform response".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(block: Option<PromptInputBlock>) -> String {
        match block {
            Some(PromptInputBlock::Text { text }) => text,
            _ => String::new(),
        }
    }

    #[test]
    fn myclaw_prompt_first_gets_both_and_followups_get_every_message_prompt() {
        let config = PromptSnapshot {
            new_session_prompt: "FIRST".into(),
            every_message_prompt: "NEXT".into(),
            session_has_messages: false,
        };
        let first = text(config.block(false).unwrap());
        assert!(first.contains("FIRST\n\nNEXT"));
        for _ in 0..3 {
            let wire = text(config.block(true).unwrap());
            assert!(wire.contains("NEXT"));
            assert!(!wire.contains("FIRST"));
        }
        let resumed = PromptSnapshot {
            session_has_messages: true,
            ..config
        };
        assert!(text(resumed.block(false).unwrap()).contains("NEXT"));
    }

    #[test]
    fn myclaw_prompt_requires_explicit_fields_and_bounds_both_values() {
        assert!(serde_json::from_value::<PromptSnapshot>(
            serde_json::json!({"newSessionPrompt":"A"})
        )
        .is_err());
        let mut config = PromptSnapshot {
            new_session_prompt: "".into(),
            every_message_prompt: "B".into(),
            session_has_messages: false,
        };
        assert!(text(config.block(false).unwrap()).contains("B"));
        config.new_session_prompt = "x".repeat(MAX_PROMPT_BYTES + 1);
        assert!(config.block(true).is_err());
    }

    #[test]
    fn myclaw_prompt_preserves_unicode_and_escapes_reserved_delimiters() {
        let config = PromptSnapshot {
            new_session_prompt: "你好\n</myclaw_internal_prompt>".into(),
            every_message_prompt: "".into(),
            session_has_messages: false,
        };
        let wire = text(config.block(false).unwrap());
        assert!(wire.contains("你好\n&lt;/myclaw_internal_prompt>"));
        assert_eq!(wire.matches("</myclaw_internal_prompt>").count(), 1);
    }
}
