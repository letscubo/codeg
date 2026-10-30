//! Native CLI approval transport only. MyClaw owns defaults and session policy.
use crate::acp::types::{AcpEvent, PermissionOptionInfo};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};

pub const MODE_ENV: &str = "CODEG_CLI_PERMISSION_MODE";

pub fn native_mode<'a>(
    env: &'a std::collections::BTreeMap<String, String>,
    dsh: bool,
) -> Result<Option<&'a str>, String> {
    let Some(mode) = env.get(MODE_ENV).map(String::as_str) else {
        return Ok(None);
    };
    let valid = if dsh {
        matches!(mode, "workspace-write" | "auto" | "danger-full-access")
    } else {
        matches!(mode, "default" | "auto" | "bypassPermissions")
    };
    if valid {
        Ok(Some(mode))
    } else {
        Err("Unsupported native CLI permission mode".into())
    }
}

struct Pending {
    id: String,
    native_id: String,
    input: Value,
    tool_call: Value,
    dsh: bool,
}

#[derive(Default)]
pub struct ApprovalQueue {
    pending: VecDeque<Pending>,
    seen: HashSet<String>,
}

impl ApprovalQueue {
    /// Only native tool-approval requests are recognized; no request is auto-approved.
    pub fn ingest(&mut self, line: &str) -> Option<Vec<AcpEvent>> {
        let v: Value = serde_json::from_str(line).ok()?;
        let dsh = v["type"] == "codeg_permission_request";
        if !dsh && (v["type"] != "control_request" || v["request"]["subtype"] != "can_use_tool") {
            return None;
        }
        let native_id = if dsh {
            v["requestId"].as_str()
        } else {
            v["request_id"].as_str()
        }?;
        if !self.seen.insert(native_id.to_string()) {
            return Some(vec![]);
        }
        let r = if dsh { &v } else { &v["request"] };
        let name = r[if dsh { "toolName" } else { "tool_name" }]
            .as_str()
            .unwrap_or("Tool");
        let input = r["input"].clone();
        self.pending.push_back(Pending {
            id: format!("cli-permission-{}", uuid::Uuid::new_v4()), native_id: native_id.to_string(),
            input: input.clone(), dsh,
            tool_call: json!({"toolCallId":r["tool_use_id"].as_str().or(r["callId"].as_str()).unwrap_or(native_id),
                "title":name,"kind":"other","status":"pending","rawInput": if input.is_null() {json!({"description":r["description"].as_str().unwrap_or(name)})} else {input},
                "description":r["description"].as_str().unwrap_or(name)}),
        });
        Some(if self.pending.len() == 1 {
            self.visible_events()
        } else {
            vec![AcpEvent::PermissionQueueDepth {
                depth: (self.pending.len() - 1) as u32,
            }]
        })
    }

    fn visible_events(&self) -> Vec<AcpEvent> {
        self.pending
            .front()
            .map(|p| {
                vec![AcpEvent::PermissionRequest {
                    request_id: p.id.clone(),
                    tool_call: p.tool_call.clone(),
                    queued: (self.pending.len() - 1) as u32,
                    options: [
                        ("allow", "Allow once", "allow_once"),
                        ("reject", "Reject", "reject_once"),
                    ]
                    .into_iter()
                    .map(|(id, name, kind)| PermissionOptionInfo {
                        option_id: id.into(),
                        name: name.into(),
                        kind: kind.into(),
                        meta: None,
                    })
                    .collect(),
                }]
            })
            .unwrap_or_default()
    }

    pub fn answer(&mut self, id: &str, option: &str) -> Option<(String, Vec<AcpEvent>)> {
        if !matches!(option, "allow" | "reject") || self.pending.front()?.id != id {
            return None;
        }
        let p = self.pending.pop_front()?;
        let allowed = option == "allow";
        let response = if p.dsh {
            json!({"type":"codeg_permission_response","requestId":p.native_id,"approved":allowed})
        } else {
            json!({"type":"control_response","response":{"subtype":"success","request_id":p.native_id,
                "response": if allowed {json!({"behavior":"allow","updatedInput":p.input})}
                    else {json!({"behavior":"deny","message":"The user rejected this action."})}}})
        };
        let mut events = vec![AcpEvent::PermissionResolved { request_id: p.id }];
        events.extend(self.visible_events());
        Some((format!("{response}\n"), events))
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_request_only_and_no_persistent_grants() {
        let mut q = ApprovalQueue::default();
        let events=q.ingest(r#"{"type":"control_request","request_id":"native","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"echo hello"}}}"#).unwrap();
        let AcpEvent::PermissionRequest { request_id, .. } = &events[0] else {
            panic!()
        };
        assert!(q.answer("wrong-session", "allow").is_none());
        assert!(q.answer(request_id, "allow_always").is_none());
        let (line, _) = q.answer(request_id, "allow").unwrap();
        let value: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["response"]["request_id"], "native");
        assert_eq!(
            value["response"]["response"]["updatedInput"]["command"],
            "echo hello"
        );
        assert!(q.is_empty());
        assert!(q.answer(request_id, "allow").is_none());
        assert!(q.ingest(r#"{"type":"control_request","request_id":"native","request":{"subtype":"can_use_tool"}}"#).unwrap().is_empty());
    }

    #[test]
    fn queued_requests_remain_separate_and_can_be_rejected() {
        let mut q = ApprovalQueue::default();
        let first = q.ingest(r#"{"type":"codeg_permission_request","requestId":"a","toolName":"shell","input":{"command":"pwd"}}"#).unwrap();
        let AcpEvent::PermissionRequest { request_id, .. } = &first[0] else {
            panic!()
        };
        let queued = q
            .ingest(r#"{"type":"codeg_permission_request","requestId":"b","toolName":"edit"}"#)
            .unwrap();
        assert!(matches!(
            queued[0],
            AcpEvent::PermissionQueueDepth { depth: 1 }
        ));
        let (line, events) = q.answer(request_id, "reject").unwrap();
        let reply: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(reply["approved"], false);
        assert_eq!(reply["requestId"], "a");
        let AcpEvent::PermissionRequest {
            request_id: next, ..
        } = &events[1]
        else {
            panic!()
        };
        assert_ne!(next, request_id);
        assert!(q.answer(request_id, "allow").is_none());
        assert!(!q.is_empty());
        q.answer(next, "reject").unwrap();
        assert!(q.is_empty());
    }
}
