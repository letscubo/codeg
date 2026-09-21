use super::*;
use serde_json::json;

#[test]
fn model_written_description_wins() {
    let input = json!({ "command": "cd /tmp && officecli save a.pptx", "description": "Create and open the PPTX file" });
    assert_eq!(
        describe_tool_call("cd /tmp && officecli save a.pptx", Some("execute"), Some(&input)),
        Some("Create and open the PPTX file".to_string())
    );
}

#[test]
fn falls_back_to_parsing_the_command() {
    // cd 当路径上下文:结果是读 src/lib.rs,而不是「cd … && cat …」
    let input = json!({ "command": "cd src && cat lib.rs" });
    assert_eq!(
        describe_tool_call("cd src && cat lib.rs", Some("execute"), Some(&input)),
        Some("Read lib.rs".to_string())
    );
    assert_eq!(describe_command("rg -n \"todo\" src"), Some("Search \"todo\" in src".to_string()));
    assert_eq!(describe_command("ls -la src"), Some("List src".to_string()));
}

#[test]
fn unknown_command_has_no_description() {
    // 归不了类 → None,调用方显示命令原文(与 codex 同一条兜底)
    assert_eq!(
        describe_command("cd ~/out && officecli save a.pptx && myclaw deliver ./a.pptx"),
        None
    );
    assert_eq!(describe_command(""), None);
}

#[test]
fn non_exec_tools_are_left_alone() {
    // 读写文件 / MCP 调用的 title 本身就清楚,不去猜
    assert_eq!(describe_tool_call("Edit note.txt", Some("edit"), None), None);
    assert_eq!(
        describe_tool_call("mcp__app-youtube__youtube_search", Some("other"), None),
        None
    );
}

#[test]
fn description_is_clipped() {
    let long = "x".repeat(200);
    let input = json!({ "description": long });
    let out = describe_tool_call("cmd", Some("execute"), Some(&input)).unwrap();
    assert!(out.chars().count() <= MAX_LEN, "{}", out.chars().count());
    assert!(out.ends_with('…'));
}

#[test]
fn strips_cd_and_variable_assignments() {
    // 会话目录 + 变量赋值都是噪声;剩下的才是这次做的事
    let cmd = "cd ~/.myclaw/sessions/1947d8f1-87a8/output && FILE=\"a.pptx\" && SLIDE=1 && officecli set \"$FILE\" --prop fill=FFFFFF";
    assert_eq!(
        strip_noise_prefix(cmd, None),
        "officecli set \"$FILE\" --prop fill=FFFFFF"
    );
    // 与 cwd 相同的 cd 也算噪声
    assert_eq!(strip_noise_prefix("cd /work/out && ls", Some("/work/out")), "ls");
}

#[test]
fn keeps_meaningful_cd_and_empty_result() {
    // 不是工作目录 → cd 是命令的一部分,保留
    assert_eq!(strip_noise_prefix("cd /etc && ls -1", None), "cd /etc && ls -1");
    // 整条就是 cd → 保留原文,不能剥成空
    let only = "cd ~/.myclaw/sessions/1947d8f1-87a8/output";
    assert_eq!(strip_noise_prefix(only, None), only);
    // 引号里的 && 不切分
    assert_eq!(strip_noise_prefix("echo \"a && b\"", None), "echo \"a && b\"");
}

#[test]
fn unknown_command_falls_back_to_cleaned_text() {
    let input = json!({
        "command": "cd ~/.myclaw/sessions/1947d8f1-87a8/output && officecli save a.pptx",
        "yieldMs": 10000
    });
    assert_eq!(
        describe_tool_call("cd ~/.myclaw/sessions/1947d8f1-87a8/output && officecli save a.pptx", Some("execute"), Some(&input)),
        Some("officecli save a.pptx".to_string())
    );
}

// ── openclaw ─────────────────────────────────────────────────────────────
// 输入取自 A5 实录(acp-transcripts/openclaw-acp):ACP 标题是「工具名: 参数: 值, …」平铺。

#[test]
fn openclaw_exec_uses_the_title_the_model_wrote() {
    let input = json!({ "command": "sleep 20 && echo 1", "title": "等待 20 秒后打印 1", "background": true });
    let acp_title = "exec: command: sleep 20 && echo 1, title: 等待 20 秒后打印 1, background: true";
    assert_eq!(
        describe_tool_call(acp_title, Some("execute"), Some(&input)),
        Some("等待 20 秒后打印 1".to_string())
    );
    // 历史记录路径:kind 为 None,结果一致
    assert_eq!(
        describe_tool_call("exec", None, Some(&input)),
        Some("等待 20 秒后打印 1".to_string())
    );
}

#[test]
fn a_title_without_a_command_is_not_a_description() {
    // 建页面 / 建 issue 这类工具的 `title` 是内容,不是调用说明
    let input = json!({ "title": "Q3 roadmap", "parent": "abc" });
    assert_eq!(describe_tool_call("mcp__app-notion__create_page", None, Some(&input)), None);
}

#[test]
fn openclaw_process_actions_are_translated() {
    let list = json!({ "action": "list" });
    assert_eq!(
        describe_tool_call("process: action: list", None, Some(&list)),
        Some("List background processes".to_string())
    );
    let log = json!({ "action": "log", "sessionId": "gentle-zephyr" });
    assert_eq!(
        describe_tool_call("process: action: log, sessionId: gentle-zephyr", None, Some(&log)),
        Some("Read output of gentle-zephyr".to_string())
    );
    // 历史里工具名可能只剩 `process`
    assert_eq!(
        describe_tool_call("process", None, Some(&log)),
        Some("Read output of gentle-zephyr".to_string())
    );
    let kill = json!({ "action": "kill", "sessionId": "gentle-zephyr" });
    assert_eq!(
        describe_tool_call("process", None, Some(&kill)),
        Some("Stop gentle-zephyr".to_string())
    );
    // 缺 sessionId 也有一句能读的
    assert_eq!(
        describe_tool_call("process", None, Some(&json!({ "action": "poll" }))),
        Some("Check a background process".to_string())
    );
}

#[test]
fn openclaw_process_does_not_guess() {
    // 不认识的 action → 不猜,调用方照旧显示原文
    assert_eq!(
        describe_tool_call("process: action: rewind", None, Some(&json!({ "action": "rewind" }))),
        None
    );
    // 别的工具碰巧有 `action` 参数,不当成 process
    assert_eq!(
        describe_tool_call("browser: action: list", None, Some(&json!({ "action": "list" }))),
        None
    );
}
