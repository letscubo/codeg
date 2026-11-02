//! Shared event projection excludes raw payloads, reasoning and nested text.
//! Channels choose which tool metadata is suitable for their audience.
use crate::acp::types::{AcpEvent, ConnectionStatus};

pub(super) enum PublicEvent<'a> {
    Text(&'a str),
    ToolStarted {
        id: &'a str,
        name: Option<&'a str>,
        title: &'a str,
        description: Option<&'a str>,
    },
    ToolUpdated {
        id: &'a str,
        name: Option<&'a str>,
        title: Option<&'a str>,
        description: Option<&'a str>,
        state: Option<&'a str>,
    },
    Finished(&'a str),
    Failed,
    AwaitingUser(&'static str, &'a str),
    Resumed(&'static str, &'a str),
    Ignore,
}

pub(super) fn classify(event: &AcpEvent) -> PublicEvent<'_> {
    match event {
        AcpEvent::ContentDelta {
            text,
            parent_tool_use_id: None,
        } => PublicEvent::Text(text),
        AcpEvent::ToolCall {
            tool_call_id,
            tool_name,
            title,
            description,
            ..
        } => PublicEvent::ToolStarted {
            id: tool_call_id,
            name: tool_name.as_deref(),
            title,
            description: description.as_deref(),
        },
        AcpEvent::ToolCallUpdate {
            tool_call_id,
            tool_name,
            title,
            description,
            status,
            ..
        } => PublicEvent::ToolUpdated {
            id: tool_call_id,
            name: tool_name.as_deref(),
            title: title.as_deref(),
            description: description.as_deref(),
            state: status.as_deref(),
        },
        AcpEvent::TurnComplete { stop_reason, .. } => PublicEvent::Finished(stop_reason),
        AcpEvent::Error { terminal: true, .. }
        | AcpEvent::StatusChanged {
            status: ConnectionStatus::Disconnected | ConnectionStatus::Error,
        } => PublicEvent::Failed,
        AcpEvent::PermissionRequest { request_id, .. } => {
            PublicEvent::AwaitingUser("permission", request_id)
        }
        AcpEvent::QuestionRequest { question_id, .. } => {
            PublicEvent::AwaitingUser("question", question_id)
        }
        AcpEvent::PlanApprovalRequest { approval_id, .. } => {
            PublicEvent::AwaitingUser("plan", approval_id)
        }
        AcpEvent::PermissionResolved { request_id } => {
            PublicEvent::Resumed("permission", request_id)
        }
        AcpEvent::QuestionResolved { question_id } => PublicEvent::Resumed("question", question_id),
        AcpEvent::PlanApprovalResolved { approval_id } => PublicEvent::Resumed("plan", approval_id),
        _ => PublicEvent::Ignore,
    }
}
