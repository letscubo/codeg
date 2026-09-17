//! fork(letscubo)专属: title / kind / locations for a DeepSeek Harness tool
//! call, so CLI-transport tool cards read like the ACP ones. Built-in dsh tool
//! names are lowercase (`bash`, `read`, `edit`, `glob`, `grep`, …); MCP tools
//! arrive as `mcp__<server>__<tool>`.

use std::path::Path;

use serde_json::{json, Value};

use super::tool_info::ToolInfo;

pub fn dsh_tool_info(name: &str, input: &Value, cwd: Option<&Path>) -> ToolInfo {
    let s = |key: &str| {
        input
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
    };
    let path_key = s("path")
        .or_else(|| s("file_path"))
        .or_else(|| s("filePath"));
    match name {
        "bash" | "pwsh" | "bash_persistent" | "pwsh_persistent" => {
            info(s("command").unwrap_or("Terminal"), "execute", None)
        }
        "read" => {
            let display = path_key.map_or_else(|| "File".to_string(), |p| display_path(p, cwd));
            let locations = path_key.map(|p| json!([{ "path": p }]));
            info(&format!("Read {display}"), "read", locations)
        }
        "edit" | "write" | "str_replace_editor" | "create" => {
            let display = path_key.map_or_else(|| "File".to_string(), |p| display_path(p, cwd));
            let locations = path_key.map(|p| json!([{ "path": p }]));
            info(&format!("Edit {display}"), "edit", locations)
        }
        "glob" | "grep" | "fs_search" => {
            let pattern = s("pattern").or_else(|| s("query")).unwrap_or("Search");
            info(pattern, "search", None)
        }
        "web_fetch" | "fetch" => info(s("url").unwrap_or("Fetch"), "fetch", None),
        "web_search" => info(s("query").unwrap_or("Web search"), "fetch", None),
        "todo" | "todo_write" => info("Update todos", "think", None),
        "subagent" | "delegate" => info(s("description").unwrap_or("Task"), "think", None),
        "search_tools" => info(
            &format!("Search tools: {}", s("query").unwrap_or("")),
            "think",
            None,
        ),
        other => {
            if let Some(rest) = other.strip_prefix("mcp__") {
                if let Some((server, tool)) = rest.split_once("__") {
                    return info(&format!("{server}: {tool}"), "other", None);
                }
            }
            info(other, "other", None)
        }
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
    match cwd.and_then(|c| Path::new(path).strip_prefix(c).ok()) {
        Some(rel) => rel.display().to_string(),
        None => path.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_tools_get_kind_and_title() {
        let cwd = Path::new("/ws");
        let bash = dsh_tool_info("bash", &json!({"command":"ls -la"}), Some(cwd));
        assert_eq!((bash.title.as_str(), bash.kind), ("ls -la", "execute"));
        let read = dsh_tool_info("read", &json!({"path":"/ws/src/a.rs"}), Some(cwd));
        assert_eq!((read.title.as_str(), read.kind), ("Read src/a.rs", "read"));
        assert!(read.locations.is_some());
    }

    #[test]
    fn mcp_tools_show_server_and_tool() {
        let t = dsh_tool_info("mcp__app-notion__notion-fetch", &json!({}), None);
        assert_eq!(
            (t.title.as_str(), t.kind),
            ("app-notion: notion-fetch", "other")
        );
    }
}
