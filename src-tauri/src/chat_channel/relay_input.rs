//! Display-only transforms. Original ACP events/transcripts remain untouched.
use super::turn_relay::RelayInput;
use crate::acp::types::{AcpEvent, EventEnvelope};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Presentation {
    pub text_prefix: String,
    pub strip_agent_mentions: bool,
    pub suppress_no_action: bool,
    pub suppress_slack_no_reply: bool,
}
#[derive(Default)]
struct Filter {
    pending: String,
    started: bool,
    opts: Presentation,
}
fn normalized_sentinel(text: &str) -> &str {
    text.trim()
        .trim_end_matches('.')
        .trim()
        .trim_matches('`')
        .trim()
}
impl Presentation {
    fn suppresses(&self, text: &str) -> bool {
        let value = normalized_sentinel(text);
        (self.suppress_no_action && value == "NO_ACTION")
            || (self.suppress_slack_no_reply && value == "[[MYCLAW_NO_REPLY]]")
    }
    fn may_suppress(&self, text: &str) -> bool {
        let value = normalized_sentinel(text);
        (self.suppress_no_action && "NO_ACTION".starts_with(value))
            || (self.suppress_slack_no_reply && "[[MYCLAW_NO_REPLY]]".starts_with(value))
    }
}
impl Filter {
    fn push(&mut self, text: &str, finished: bool) -> Option<String> {
        self.pending.push_str(text);
        if !self.started && !finished && self.opts.may_suppress(&self.pending) {
            return None;
        }
        if finished && !self.started && self.opts.suppresses(&self.pending) {
            self.pending.clear();
            return None;
        }
        let mut ready = std::mem::take(&mut self.pending);
        if self.opts.strip_agent_mentions {
            if !finished {
                if let Some(start) = ready.rfind('[') {
                    if !ready[start..].contains(')') && !ready[start..].contains('\n') {
                        self.pending = ready.split_off(start);
                    }
                }
            }
            static MENTIONS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
            let re = MENTIONS.get_or_init(|| {
                regex::Regex::new(r"\[@([^\]]+)\]\(mention://agent/[^)]+\)").unwrap()
            });
            ready = re.replace_all(&ready, "@$1").into_owned();
        }
        if ready.is_empty() {
            return None;
        }
        if !self.started {
            ready = format!("{}{ready}", self.opts.text_prefix);
            self.started = true;
        }
        Some(ready)
    }
}
pub(crate) fn wrap(
    mut rx: mpsc::UnboundedReceiver<RelayInput>,
    opts: Presentation,
) -> mpsc::UnboundedReceiver<RelayInput> {
    let (tx, out) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut filter = Filter {
            opts,
            ..Default::default()
        };
        while let Some(input) = rx.recv().await {
            match input {
                RelayInput::Event(env) => {
                    if let AcpEvent::ContentDelta {
                        text,
                        parent_tool_use_id: None,
                    } = &env.payload
                    {
                        if let Some(text) = filter.push(text, false) {
                            let event = EventEnvelope {
                                payload: AcpEvent::ContentDelta {
                                    text,
                                    parent_tool_use_id: None,
                                },
                                ..(*env).clone()
                            };
                            if tx.send(RelayInput::Event(Arc::new(event))).is_err() {
                                break;
                            }
                        }
                        continue;
                    }
                    let terminal = matches!(
                        &env.payload,
                        AcpEvent::TurnComplete { .. } | AcpEvent::Error { terminal: true, .. }
                    );
                    if terminal {
                        let silent = !filter.started && filter.opts.suppresses(&filter.pending);
                        if let Some(text) = filter.push("", true) {
                            let _ = tx.send(RelayInput::Event(Arc::new(EventEnvelope {
                                payload: AcpEvent::ContentDelta {
                                    text,
                                    parent_tool_use_id: None,
                                },
                                ..(*env).clone()
                            })));
                        }
                        if silent
                            && matches!(&env.payload,AcpEvent::TurnComplete{stop_reason,..} if stop_reason=="end_turn")
                        {
                            let _ = tx.send(RelayInput::SilentComplete);
                            continue;
                        }
                    }
                    if tx.send(RelayInput::Event(env)).is_err() {
                        break;
                    }
                }
                other => {
                    if tx.send(other).is_err() {
                        break;
                    }
                }
            }
        }
    });
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_mentions_never_leak() {
        let mut f = Filter {
            opts: Presentation {
                text_prefix: "Alice:\n".into(),
                strip_agent_mentions: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            f.push("Hello [@Bob](mention://age", false).as_deref(),
            Some("Alice:\nHello ")
        );
        assert_eq!(f.push("nt/uuid) done", false).as_deref(), Some("@Bob done"));
    }
    #[test]
    fn no_action_is_buffered_without_prefix() {
        let mut f = Filter {
            opts: Presentation {
                suppress_no_action: true,
                text_prefix: "A:\n".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(f.push("`NO_", false).is_none());
        assert!(f.push("ACTION`.", false).is_none());
        assert!(f.push("", true).is_none());
    }
    #[test]
    fn ordinary_no_prefix_can_stream() {
        let mut f = Filter::default();
        assert_eq!(f.push("Hello", false).as_deref(), Some("Hello"));
    }
    #[test]
    fn slack_sentinel_is_opt_in_and_fragment_safe() {
        let mut filter = Filter {
            opts: Presentation {
                suppress_slack_no_reply: true,
                ..Default::default()
            },
            ..Default::default()
        };
        for part in ["[[MY", "CLAW_", "NO_REPLY", "]]\n"] {
            assert!(filter.push(part, false).is_none());
        }
        assert!(filter.push("", true).is_none());
        let mut ordinary = Filter::default();
        assert_eq!(
            ordinary.push("[[MYCLAW_NO_REPLY]]", false).as_deref(),
            Some("[[MYCLAW_NO_REPLY]]")
        );
        let mut prose = Filter {
            opts: Presentation {
                suppress_slack_no_reply: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(prose.push("[[MYCLAW_NO_REPLY]]", false).is_none());
        assert_eq!(
            prose.push(" is a marker", false).as_deref(),
            Some("[[MYCLAW_NO_REPLY]] is a marker")
        );
    }
}
