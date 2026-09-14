//! fork(letscubo)专属: title / kind / locations for a Claude Code `tool_use`,
//! ported from `claude-agent-acp`'s `toolInfoFromToolUse` (dist/tools.js,
//! 0.75.1) so CLI-transport tool cards read the same as ACP ones.

use std::path::Path;

use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct ToolInfo {
    pub title: String,
    pub kind: &'static str,
    pub locations: Option<Value>,
}

pub fn tool_info(name: &str, input: &Value, cwd: Option<&Path>) -> ToolInfo {
    let s = |key: &str| {
        input
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
    };
    match name {
        "Agent" | "Task" => info(s("description").unwrap_or("Task"), "think", None),
        "Bash" => info(s("command").unwrap_or("Terminal"), "execute", None),
        "Read" => {
            let path = s("file_path");
            let display = path.map_or_else(|| "File".to_string(), |p| display_path(p, cwd));
            let offset = input.get("offset").and_then(Value::as_u64);
            let range = match (input.get("limit").and_then(Value::as_u64), offset) {
                (Some(limit), _) if limit > 0 => {
                    let start = offset.unwrap_or(1);
                    format!(" ({start} - {})", start + limit - 1)
                }
                (_, Some(offset)) if offset > 0 => format!(" (from line {offset})"),
                _ => String::new(),
            };
            let locations = path.map(|p| json!([{ "path": p, "line": offset.unwrap_or(1) }]));
            info(&format!("Read {display}{range}"), "read", locations)
        }
        "Write" => {
            let path = s("file_path");
            let title = path.map_or_else(
                || "Preparing file…".to_string(),
                |p| format!("Write {}", display_path(p, cwd)),
            );
            info(&title, "edit", path.map(|p| json!([{ "path": p }])))
        }
        "Edit" => {
            let path = s("file_path");
            let title = path.map_or_else(
                || "Edit".to_string(),
                |p| format!("Edit {}", display_path(p, cwd)),
            );
            info(&title, "edit", path.map(|p| json!([{ "path": p }])))
        }
        "Glob" => {
            let mut label = "Find".to_string();
            if let Some(path) = s("path") {
                label.push_str(&format!(" `{path}`"));
            }
            if let Some(pattern) = s("pattern") {
                label.push_str(&format!(" `{pattern}`"));
            }
            info(&label, "search", s("path").map(|p| json!([{ "path": p }])))
        }
        "Grep" => info(&grep_label(input), "search", None),
        "WebFetch" => info(
            &s("url").map_or_else(|| "Fetch".to_string(), |u| format!("Fetch {u}")),
            "fetch",
            None,
        ),
        "WebSearch" => info(
            &s("query").map_or_else(|| "Web search".to_string(), |q| format!("Search \"{q}\"")),
            "fetch",
            None,
        ),
        "TodoWrite" => {
            let title = match input.get("todos").and_then(Value::as_array) {
                Some(todos) => format!(
                    "Update TODOs: {}",
                    todos
                        .iter()
                        .map(|t| t.get("content").and_then(Value::as_str).unwrap_or(""))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                None => "Update TODOs".to_string(),
            };
            info(&title, "think", None)
        }
        "ReportFindings" => {
            let count = input
                .get("findings")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let title = match count {
                0 => "Report findings: none found".to_string(),
                1 => "Report 1 finding".to_string(),
                n => format!("Report {n} findings"),
            };
            info(&title, "think", None)
        }
        "TaskCreate" => info(
            &s("subject").map_or_else(
                || "Create task".to_string(),
                |v| format!("Create task: {v}"),
            ),
            "think",
            None,
        ),
        "TaskUpdate" => info(
            &s("subject").map_or_else(
                || "Update task".to_string(),
                |v| format!("Update task: {v}"),
            ),
            "think",
            None,
        ),
        "TaskList" => info("List tasks", "think", None),
        "TaskGet" => info("Get task", "think", None),
        "ExitPlanMode" => info("Approve Plan", "switch_mode", None),
        "" => info("Unknown Tool", "other", None),
        other => info(other, "other", None),
    }
}

fn info(title: &str, kind: &'static str, locations: Option<Value>) -> ToolInfo {
    ToolInfo {
        title: title.to_string(),
        kind,
        locations,
    }
}

fn display_path(path: &str, cwd: Option<&Path>) -> String {
    cwd.and_then(|cwd| Path::new(path).strip_prefix(cwd).ok())
        .map_or_else(|| path.to_string(), |rel| rel.display().to_string())
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Number(n)) => n.as_f64() != Some(0.0),
        Some(Value::Array(_) | Value::Object(_)) => true,
        Some(Value::Null) | None => false,
    }
}

fn scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn grep_label(input: &Value) -> String {
    let get = |key: &str| input.get(key).filter(|v| !v.is_null());
    let mut label = "grep".to_string();
    if truthy(input.get("-i")) {
        label.push_str(" -i");
    }
    if truthy(input.get("-n")) {
        label.push_str(" -n");
    }
    for flag in ["-A", "-B", "-C"] {
        if let Some(v) = get(flag) {
            label.push_str(&format!(" {flag} {}", scalar(v)));
        }
    }
    match input.get("output_mode").and_then(Value::as_str) {
        Some("files_with_matches") => label.push_str(" -l"),
        Some("count") => label.push_str(" -c"),
        _ => {}
    }
    if let Some(v) = get("head_limit") {
        label.push_str(&format!(" | head -{}", scalar(v)));
    }
    if truthy(input.get("glob")) {
        label.push_str(&format!(" --include=\"{}\"", scalar(&input["glob"])));
    }
    if truthy(input.get("type")) {
        label.push_str(&format!(" --type={}", scalar(&input["type"])));
    }
    if truthy(input.get("multiline")) {
        label.push_str(" -P");
    }
    if truthy(input.get("pattern")) {
        label.push_str(&format!(" \"{}\"", scalar(&input["pattern"])));
    }
    if truthy(input.get("path")) {
        label.push_str(&format!(" {}", scalar(&input["path"])));
    }
    label
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bash_uses_the_command_as_title() {
        let i = tool_info("Bash", &json!({ "command": "ls /etc | head -3" }), None);
        assert_eq!((i.title.as_str(), i.kind), ("ls /etc | head -3", "execute"));
        assert_eq!(tool_info("Bash", &json!({}), None).title, "Terminal");
    }

    #[test]
    fn read_shows_a_cwd_relative_path_and_line_range() {
        let i = tool_info(
            "Read",
            &json!({ "file_path": "/w/src/a.rs", "offset": 10, "limit": 5 }),
            Some(Path::new("/w")),
        );
        assert_eq!(i.title, "Read src/a.rs (10 - 14)");
        assert_eq!(i.kind, "read");
        assert_eq!(
            i.locations,
            Some(json!([{ "path": "/w/src/a.rs", "line": 10 }]))
        );
        assert_eq!(tool_info("Read", &json!({}), None).title, "Read File");
    }

    #[test]
    fn grep_label_matches_the_adapter() {
        let i = tool_info(
            "Grep",
            &json!({ "pattern": "fn main", "-i": true, "output_mode": "files_with_matches", "path": "src" }),
            None,
        );
        assert_eq!(i.title, "grep -i -l \"fn main\" src");
        assert_eq!(i.kind, "search");
    }

    #[test]
    fn unknown_tools_fall_back_to_their_name() {
        let i = tool_info("mcp__x__y", &json!({}), None);
        assert_eq!((i.title.as_str(), i.kind), ("mcp__x__y", "other"));
    }
}
