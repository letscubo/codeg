//! 工具调用的一句话说明(description)—— 界面上替代「整条命令」显示。
//!
//! ## 为什么在 codeg 里做
//! 前端(codeg 自己的 UI、MyClaw 的对话页)都只负责显示。说明在这里生成,实时事件与落库历史
//! 用**同一个函数**,刷新前后看到的一样(用户 2026-09-19 定)。
//!
//! ## 取值顺序
//! 1. 工具参数里自带的 `description` —— DeepSeek dsh、Claude Code、opencode 的 bash 工具都要求
//!    模型填一句「主动语态、5–10 个词」的说明,直接用它,这是最贴合意图的;openclaw 的 exec
//!    把同一句话放在 `title` 里(见 [`command_title`]);
//! 1'. openclaw 的 `process`(管后台进程)没有说明也没有命令,按 `action` 翻成短句(见
//!    [`describe_openclaw_process`]);
//! 2. 没有就解析命令:`parse_command`(vendor/codex-shell-command,搬自 codex)归类成 读文件 / 列目录 / 搜索,
//!    生成 `Read foo.txt`、`Search "todo" in src`、`List src` 这类短句;
//! 3. 归不了类 → 返回 None,调用方照旧显示命令原文(与 codex 的兜底一致,不猜)。

use codex_shell_command::ParsedCommand;
use codex_shell_command::parse_command;

/// 说明最长多少字符 —— 超了截断,界面一行放得下。
const MAX_LEN: usize = 72;

fn clip(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= MAX_LEN {
        return trimmed.to_string();
    }
    let cut: String = trimmed.chars().take(MAX_LEN - 1).collect();
    format!("{cut}…")
}

/// 工具参数里模型自己写的说明(`description`,少数实现叫 `summary`)。
fn model_written(raw_input: Option<&serde_json::Value>) -> Option<String> {
    let obj = raw_input?.as_object()?;
    for key in ["description", "summary"] {
        if let Some(text) = obj.get(key).and_then(|v| v.as_str()) {
            if !text.trim().is_empty() {
                return Some(clip(text));
            }
        }
    }
    None
}

/// openclaw exec 的说明:它把模型写的那句话放在 `title` 里,而不是 `description`。
///
/// 只在参数里**同时有 `command`** 时才认 —— `title` 是个常见参数名(建页面、建 issue 的
/// MCP 工具都有),历史记录里 kind 一律是 None,不加这道闸会把「页面标题」当成调用说明。
fn command_title(raw_input: Option<&serde_json::Value>) -> Option<String> {
    let obj = raw_input?.as_object()?;
    obj.get("command")?.as_str()?;
    let text = obj.get("title")?.as_str()?;
    (!text.trim().is_empty()).then(|| clip(text))
}

/// openclaw 的 `process` 工具(后台进程的 list / poll / log / kill …)。
///
/// 它的 ACP 标题是把工具名和参数平铺成一行(`process: action: log, sessionId: gentle-zephyr`),
/// 参数里没有说明也没有命令,不译就只能原样显示。工具名取标题冒号前那段:实时是整行标题,
/// 历史里可能只剩 `process`,两种都认。action 不认识就返回 None,照旧显示原文,不猜。
fn describe_openclaw_process(title: &str, raw_input: Option<&serde_json::Value>) -> Option<String> {
    if title.split(':').next()?.trim() != "process" {
        return None;
    }
    let obj = raw_input?.as_object()?;
    let action = obj.get("action")?.as_str()?.trim();
    let session = obj
        .get("sessionId")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let on = |verb: &str, fallback: &str| match session {
        Some(id) => format!("{verb} {id}"),
        None => fallback.to_string(),
    };
    let text = match action {
        "list" => "List background processes".to_string(),
        "poll" => on("Check", "Check a background process"),
        "log" => on("Read output of", "Read process output"),
        "write" | "submit" | "paste" | "send-keys" => on("Send input to", "Send input to a process"),
        "kill" => on("Stop", "Stop a background process"),
        "clear" => on("Clear output of", "Clear process output"),
        "remove" => on("Remove", "Remove a background process"),
        _ => return None,
    };
    Some(clip(&text))
}

/// 参数里的 `cwd` / `workdir`(不同实现叫法不同)。
fn cwd_of(raw_input: Option<&serde_json::Value>) -> Option<String> {
    let obj = raw_input?.as_object()?;
    ["cwd", "workdir", "working_dir"]
        .iter()
        .find_map(|k| obj.get(*k).and_then(|v| v.as_str()))
        .map(str::to_string)
}

/// 命令字符串 → 一句话。取参数里的 `command`;没有就用 title(多数实现把命令放这里)。
fn command_text(raw_input: Option<&serde_json::Value>, title: &str) -> String {
    raw_input
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("command"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| title.to_string())
}

/// 顶层(引号外)按 `&&` / `;` 切分 —— 只用于剥前缀,不解析命令本身。
fn split_top_level(command: &str) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    cur.push(c);
                }
                ';' => {
                    parts.push(cur.trim().to_string());
                    cur.clear();
                }
                '&' if chars.peek() == Some(&'&') => {
                    chars.next();
                    parts.push(cur.trim().to_string());
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts.into_iter().filter(|p| !p.is_empty()).collect()
}

/// 这一段是不是「噪声前缀」:切目录或设变量,都不是这次真正做的事。
///
/// `cd` 只在目标看着是工作目录时才算噪声:会话目录(`…/.myclaw/sessions/<id>/…`)或与本次调用
/// 的 `cwd` 相同。`cd /etc && ls` 里的 `cd /etc` 是命令的一部分,保留(crush 同一条判据)。
fn is_noise_prefix(segment: &str, cwd: Option<&str>) -> bool {
    let seg = segment.trim();
    // VAR=value / export VAR=value
    let assignment = seg.strip_prefix("export ").unwrap_or(seg);
    if let Some((name, _)) = assignment.split_once('=') {
        let looks_like_var = !name.is_empty()
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && name.chars().next().is_some_and(|c| !c.is_ascii_digit());
        if looks_like_var {
            return true;
        }
    }
    let Some(rest) = seg.strip_prefix("cd ") else {
        return false;
    };
    let target = rest.trim().trim_matches(['"', '\'']).trim_end_matches('/');
    if target.is_empty() {
        return false;
    }
    if let Some(cwd) = cwd {
        if target == cwd.trim_end_matches('/') {
            return true;
        }
    }
    target.contains("/.myclaw/sessions/")
}

/// 去掉开头的噪声段(切目录、设变量)。剥完为空则原样返回 —— 整条就是 `cd xxx` 时还得显示它。
pub fn strip_noise_prefix(command: &str, cwd: Option<&str>) -> String {
    let parts = split_top_level(command);
    let kept: Vec<String> = parts
        .iter()
        .skip_while(|p| is_noise_prefix(p, cwd))
        .cloned()
        .collect();
    if kept.is_empty() {
        return command.trim().to_string();
    }
    kept.join(" && ")
}

/// 一条命令 → 说明。归不了类返回 None(调用方显示原文)。
pub fn describe_command(command: &str) -> Option<String> {
    describe_command_in(command, None)
}

/// 同上,但知道这次调用的工作目录 —— 用于判断开头的 `cd` 是不是噪声。
pub fn describe_command_in(command: &str, cwd: Option<&str>) -> Option<String> {
    let cleaned = strip_noise_prefix(command, cwd);
    let tokens = shlex::split(&cleaned)?;
    if tokens.is_empty() {
        return None;
    }
    let parsed = parse_command(&tokens);
    let mut parts: Vec<String> = Vec::new();
    for item in &parsed {
        match item {
            ParsedCommand::Read { name, .. } => parts.push(format!("Read {name}")),
            ParsedCommand::ListFiles { path, .. } => {
                parts.push(match path {
                    Some(p) => format!("List {p}"),
                    None => "List files".to_string(),
                });
            }
            ParsedCommand::Search { query, path, .. } => {
                parts.push(match (query, path) {
                    (Some(q), Some(p)) => format!("Search \"{q}\" in {p}"),
                    (Some(q), None) => format!("Search \"{q}\""),
                    (None, Some(p)) => format!("Search in {p}"),
                    (None, None) => "Search".to_string(),
                });
            }
            // 归不了类:整条按原文显示更诚实 —— 与 codex 同一条兜底规则
            ParsedCommand::Unknown { .. } => return None,
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some(clip(&parts.join(" · ")))
}

/// 一次工具调用的说明。`title` 是 ACP 给的标题(命令类里多半就是命令本身)。
pub fn describe_tool_call(
    title: &str,
    kind: Option<&str>,
    raw_input: Option<&serde_json::Value>,
) -> Option<String> {
    if let Some(text) = model_written(raw_input).or_else(|| command_title(raw_input)) {
        return Some(text);
    }
    if let Some(text) = describe_openclaw_process(title, raw_input) {
        return Some(text);
    }
    // 只对命令类做解析;读写文件、MCP 调用等由各自的 title 表达,已经够清楚
    let is_exec = matches!(kind, Some("execute") | Some("exec") | None);
    if !is_exec {
        return None;
    }
    let command = command_text(raw_input, title);
    let cwd = cwd_of(raw_input);
    if let Some(text) = describe_command_in(&command, cwd.as_deref()) {
        return Some(text);
    }
    /*
     * 归不了类(自定义程序,如 officecli / myclaw)→ 不猜它在做什么,但把切目录、设变量这些
     * 噪声前缀去掉再交给界面:三条只有参数不同的命令,带着同样的 `cd 会话目录` 前缀在一行里
     * 长得一模一样(用户 2026-09-19 反馈)。没得可去就返回 None,调用方照旧显示 title。
     */
    let cleaned = strip_noise_prefix(&command, cwd.as_deref());
    if cleaned != command.trim() {
        return Some(clip(&cleaned));
    }
    None
}

#[cfg(test)]
mod tests;
