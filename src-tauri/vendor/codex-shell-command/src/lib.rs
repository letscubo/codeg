//! codex 的 shell 命令解析(裁剪版):只保留把命令归类成 读文件 / 列目录 / 搜索 的部分。
//!
//! 来源:openai/codex `codex-rs/shell-command/src/{parse_command,bash,shell_detect}.rs`(Apache-2.0)。
//! 裁掉:命令安全判定(command_safety)、shell 快照、PowerShell 解析(codeg 只在 Linux 上部署,
//! powershell.rs 换成空实现)。同步上游时整体覆盖这几个文件即可。

mod bash;
pub mod parse_command;
mod powershell;
mod shell_detect;

pub use parse_command::ParsedCommand;
pub use parse_command::parse_command;
