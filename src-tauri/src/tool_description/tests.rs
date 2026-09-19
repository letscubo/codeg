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
