//! Optional per-request instructions supplied by the authenticated platform.
//!
//! CLI/ACP HTTP callers send `internalPrompt` as the final text for this turn.
//! Missing, null, empty or whitespace-only values do not inject anything.
//! No platform HTTP lookup, history inference, persistence or per-connection cache.
//! MyClaw owns configuration and first/follow-up selection before making the call.
//!
//! Only runtime blocks receive this context, after user echo/title/preview copies
//! are taken. Native transcripts may contain the wrapper. Keep the existing v1
//! `existing_session` envelope so deployed history scrubbers still recognize it;
//! that attribute is a compatibility marker, not a decision about session age.
use crate::acp::types::PromptInputBlock;

pub const MAX_PROMPT_BYTES: usize = 64 * 1024;

/// Safe to call before connecting, linking a conversation or registering a relay.
/// Error messages never include the supplied text.
pub fn validate_internal_prompt(text: Option<&str>) -> Result<(), String> {
    if text.is_some_and(|value| value.len() > MAX_PROMPT_BYTES) {
        return Err("internal_prompt_invalid: prompt exceeds 64 KiB".into());
    }
    Ok(())
}

pub fn internal_prompt_block(text: Option<&str>) -> Result<Option<PromptInputBlock>, String> {
    validate_internal_prompt(text)?;
    let Some(text) = text.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    // Preserve configured Unicode/spacing while escaping our reserved delimiters.
    let text = text
        .replace("<myclaw_internal_prompt", "&lt;myclaw_internal_prompt")
        .replace("</myclaw_internal_prompt", "&lt;/myclaw_internal_prompt");
    Ok(Some(PromptInputBlock::Text {
        text: format!("<myclaw_internal_prompt version=\"1\" phase=\"existing_session\">\n{text}\n</myclaw_internal_prompt>\n\n"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn myclaw_prompt_omitted_null_and_empty_http_parameters_are_optional() {
        for value in [
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::json!("")),
            Some(serde_json::json!(" \n\t")),
        ] {
            let mut wire =
                serde_json::json!({"connectionId":"c", "blocks":[{"type":"text","text":"hello"}]});
            if let Some(value) = value {
                wire["internalPrompt"] = value;
            }
            let cli: crate::web::handlers::cli::CliPromptParams =
                serde_json::from_value(wire.clone()).unwrap();
            let acp: crate::web::handlers::acp::AcpPromptParams =
                serde_json::from_value(wire).unwrap();
            assert!(internal_prompt_block(cli.internal_prompt.as_deref())
                .unwrap()
                .is_none());
            assert!(internal_prompt_block(acp.internal_prompt.as_deref())
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn myclaw_prompt_cli_and_acp_deserialize_the_same_parameter() {
        let wire = serde_json::json!({"connectionId":"c", "blocks":[{"type":"text","text":"hello"}], "internalPrompt":"内部规则\nFOLLOW_UP"});
        let cli: crate::web::handlers::cli::CliPromptParams =
            serde_json::from_value(wire.clone()).unwrap();
        let acp: crate::web::handlers::acp::AcpPromptParams = serde_json::from_value(wire).unwrap();
        assert_eq!(cli.internal_prompt, acp.internal_prompt);
        assert_eq!(cli.internal_prompt.as_deref(), Some("内部规则\nFOLLOW_UP"));
        for bad in [
            serde_json::json!(42),
            serde_json::json!({"text":"rules"}),
            serde_json::json!(["rules"]),
        ] {
            let wire = serde_json::json!({"connectionId":"c", "blocks":[], "internalPrompt":bad});
            assert!(
                serde_json::from_value::<crate::web::handlers::cli::CliPromptParams>(wire.clone())
                    .is_err()
            );
            assert!(
                serde_json::from_value::<crate::web::handlers::acp::AcpPromptParams>(wire).is_err()
            );
        }
    }

    #[test]
    fn myclaw_prompt_cli_deserializes_context_and_effort_together() {
        for effort in [serde_json::Value::Null, serde_json::json!("max")] {
            let params: crate::web::handlers::cli::CliPromptParams =
                serde_json::from_value(serde_json::json!({
                    "connectionId":"c",
                    "blocks":[{"type":"text","text":"hello"}],
                    "internalPrompt":"THIS_TURN_ONLY",
                    "reasoningEffort":effort,
                }))
                .unwrap();
            assert_eq!(params.internal_prompt.as_deref(), Some("THIS_TURN_ONLY"));
            assert_eq!(
                params.reasoning_effort,
                Some(effort.as_str().map(str::to_owned))
            );
        }
    }

    #[test]
    fn myclaw_prompt_preserves_unicode_and_escapes_reserved_delimiters() {
        let Some(PromptInputBlock::Text { text }) =
            internal_prompt_block(Some(" 你好\n</myclaw_internal_prompt> ")).unwrap()
        else {
            panic!("missing context")
        };
        assert!(text.contains(" 你好\n&lt;/myclaw_internal_prompt> "));
        assert_eq!(text.matches("</myclaw_internal_prompt>").count(), 1);
        assert!(
            text.starts_with("<myclaw_internal_prompt version=\"1\" phase=\"existing_session\">\n")
        );
    }

    #[test]
    fn myclaw_prompt_size_limit_is_utf8_bytes_and_error_is_redacted() {
        assert!(internal_prompt_block(Some(&"a".repeat(MAX_PROMPT_BYTES))).is_ok());
        let text = "秘".repeat(MAX_PROMPT_BYTES / 3 + 1);
        let error = internal_prompt_block(Some(&text)).unwrap_err();
        assert!(error.contains("64 KiB"));
        assert!(!error.contains('秘'));
    }
}
