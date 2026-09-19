//! PowerShell 分支的空实现 —— codeg 只在 Linux/Ubuntu 上打包部署(用户 2026-09-19 定)。
//!
//! 上游(codex `shell-command/src/powershell.rs`)要 `command_safety` 整套 + tree-sitter-powershell,
//! 为一条用不到的分支引进来不值当。返回 None 时 `parse_command` 会走原本的「归不了类 → 显示原文」兜底。

/// 上游同名函数:从 `pwsh -c "..."` 里取出脚本。本实现恒为 None。
pub fn extract_powershell_command(_command: &[String]) -> Option<(&str, &str)> {
    None
}
