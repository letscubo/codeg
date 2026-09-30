//! fork(letscubo)专属: 把伴生的交付义务送到 Hermes 的系统提示词里。
//!
//! ## 为什么需要
//!
//! 伴生(`codeg-mcp`)的交付义务写在 MCP `InitializeResult.instructions` 里
//! (`COMPANION_INSTRUCTIONS`),多数 runtime 会把它并进系统提示。Hermes 不会:
//! hermes-agent(2026.9.14,npm 桥接包内的官方源码)`tools/mcp_*.py` 里没有任何读取
//! `instructions` 的地方。
//!
//! 同时 Hermes 的 tool-search 把**所有** MCP 工具(`mcp-*` 工具集)都延后到
//! `tool_search` 背后,模型的工具表里看不到 `upload_file`,只在 `tool_search` 的描述
//! 里留一行摘要。2026-09-30 在 C8 上实测:伴生已注入、工具已登记
//! (`mcp__myclaw__upload_file`),模型却回复"没有上传能力"、从头到尾没搜过工具。
//!
//! ## 怎么送
//!
//! Hermes 原生读取进程 cwd 里的 `.hermes.md`,作为 "Project Context" 并进系统提示
//! (`agent/prompt_builder.py`)。codeg 本来就把 Hermes 进程的 cwd 钉在会话目录
//! (`build_agent` → `with_current_dir`),所以在拉起进程**之前**往那里写一份即可。
//!
//! 不走的路(同一次排查里核对过):
//! - `HERMES_EPHEMERAL_SYSTEM_PROMPT` / `agent.system_prompt`:只有 gateway 与 CLI 读,
//!   ACP 建 agent 时不传 `ephemeral_system_prompt`。
//! - 关掉 Hermes 的 tool-search:上传工具会直接可见,但授权了应用的 agent 会把上游全部
//!   MCP 工具定义塞进每一轮(openclaw 那边实测 Notion + Higgsfield 从 1.8 万涨到 12 万
//!   tokens)。
//! - Hermes 插件的 system prompt section:用户插件默认不启用,要在**共享**
//!   `~/.hermes/config.yaml` 里登记 `plugins.enabled`,而那份配置平台会整体改写。
//!
//! ## 不能盖掉用户的东西
//!
//! Hermes 在 cwd 里**只加载一种**项目上下文,先到先得:`.hermes.md`/`HERMES.md`(向上走到
//! git 根)→ `AGENTS.md` 链 → `CLAUDE.md` → `.cursorrules`。所以只在「放进去不会遮住任何
//! 东西」时才写:cwd 里没有任何这类文件、且 cwd 不在 git 工作树里。我们自己写过的那份
//! (带 frontmatter 标记)每次覆盖更新。
//!
//! 会话目录里的点文件不会出现在平台的会话产物列表里(那边的 `find` 跳过 `.*`)。

use std::path::{Path, PathBuf};

use crate::acp::delegation::companion::COMPANION_INSTRUCTIONS;

/// 写入的文件名。优先级最高的那个名字 —— 其余候选存在时我们本来就不写。
pub const CONTEXT_FILE: &str = ".hermes.md";

/// frontmatter 里的标记行。Hermes 加载时会剥掉 frontmatter(`_strip_yaml_frontmatter`),
/// 所以标记不进提示词,只用来认出"这是 codeg 写的、可以覆盖"。
const MARKER: &str = "generator: codeg-companion";

/// cwd 里只要有其中任何一个,Hermes 就会用它当项目上下文 —— 我们写 `.hermes.md` 会把它
/// 遮住,所以一律不写。`.hermes.md` 本身单独判断(自己写的可以覆盖)。
const PROJECT_CONTEXT_FILES: &[&str] = &[
    "HERMES.md",
    "AGENTS.override.md",
    "AGENTS.md",
    "agents.md",
    "CLAUDE.md",
    "claude.md",
    ".cursorrules",
];

/// 这一次做了什么 —— 给测试和日志用。
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Written,
    SkippedNoDir,
    SkippedInGitRepo,
    SkippedUserContext(String),
    Failed(String),
}

/// 渲染文件内容。伴生说明原样引用(单一事实源在 `COMPANION_INSTRUCTIONS`),后面补
/// Hermes 专有的一段:它的 MCP 工具是按需加载的。
pub fn render() -> String {
    format!(
        "---\n{MARKER}\nnote: written by codeg for this session; not user content\n---\n\
## Platform channel (myclaw)\n\n\
An MCP server named `myclaw` is attached to this session. It describes itself as follows:\n\n\
{COMPANION_INSTRUCTIONS}\n\n\
In this runtime MCP tools are loaded on demand, so `upload_file` is not in your direct tool \
list. Find it with `tool_search` (query: \"upload_file\") and invoke it with `tool_call`; its full \
name is `mcp__myclaw__upload_file`. Do this for every file you deliver, and never conclude that \
you cannot upload without searching first.\n"
    )
}

/// 从 `dir` 往上找 `.git`(目录或 worktree 的文件)。在 git 工作树里写 `.hermes.md`
/// 会遮住仓库根的 `AGENTS.md` 链(Hermes 从 cwd 往上找 `.hermes.md` 直到 git 根)。
fn inside_git_repo(dir: &Path) -> bool {
    dir.ancestors().any(|d| d.join(".git").exists())
}

fn is_ours(path: &Path) -> bool {
    std::fs::read_to_string(path)
        .map(|s| s.starts_with("---\n") && s.lines().take(6).any(|l| l.trim() == MARKER))
        .unwrap_or(false)
}

/// 需要写(或刷新)时写一份;任何不安全的情况都跳过。失败不影响拉起 —— 最坏情况只是
/// 这一轮模型不知道要上传,与写入前相同。
pub fn ensure_companion_context(dir: &Path) -> Outcome {
    if !dir.is_dir() {
        return Outcome::SkippedNoDir;
    }
    if inside_git_repo(dir) {
        return Outcome::SkippedInGitRepo;
    }
    if let Some(name) = PROJECT_CONTEXT_FILES
        .iter()
        .find(|n| dir.join(n).exists())
        .map(|n| n.to_string())
        .or_else(|| {
            dir.join(".cursor/rules")
                .is_dir()
                .then(|| ".cursor/rules".to_string())
        })
    {
        return Outcome::SkippedUserContext(name);
    }
    let target: PathBuf = dir.join(CONTEXT_FILE);
    if target.exists() && !is_ours(&target) {
        return Outcome::SkippedUserContext(CONTEXT_FILE.to_string());
    }
    // 先写临时文件再改名:Hermes 与本进程可能同时看这个目录,不能让它读到半截。
    let tmp = dir.join(format!("{CONTEXT_FILE}.codeg-tmp"));
    let result = std::fs::write(&tmp, render()).and_then(|_| std::fs::rename(&tmp, &target));
    match result {
        Ok(()) => Outcome::Written,
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Outcome::Failed(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn writes_into_an_empty_session_dir() {
        let d = dir();
        assert_eq!(ensure_companion_context(d.path()), Outcome::Written);
        let body = std::fs::read_to_string(d.path().join(CONTEXT_FILE)).unwrap();
        assert!(body.contains(COMPANION_INSTRUCTIONS), "伴生说明要原样带上");
        assert!(body.contains("mcp__myclaw__upload_file"));
        assert!(body.contains("tool_search"));
        // 标记只在 frontmatter 里 —— Hermes 剥掉 frontmatter 后正文里不该再出现
        let after_frontmatter = body.splitn(3, "---\n").nth(2).unwrap();
        assert!(!after_frontmatter.contains(MARKER));
        assert!(
            !d.path().join(".hermes.md.codeg-tmp").exists(),
            "临时文件要改名掉"
        );
    }

    #[test]
    fn refreshes_its_own_stale_file() {
        let d = dir();
        let target = d.path().join(CONTEXT_FILE);
        std::fs::write(&target, format!("---\n{MARKER}\n---\nold text\n")).unwrap();
        assert_eq!(ensure_companion_context(d.path()), Outcome::Written);
        assert!(std::fs::read_to_string(&target)
            .unwrap()
            .contains(COMPANION_INSTRUCTIONS));
    }

    /// 用户自己的 `.hermes.md` 绝不能覆盖。
    #[test]
    fn never_overwrites_a_user_hermes_md() {
        let d = dir();
        let target = d.path().join(CONTEXT_FILE);
        std::fs::write(&target, "my own project notes\n").unwrap();
        assert_eq!(
            ensure_companion_context(d.path()),
            Outcome::SkippedUserContext(CONTEXT_FILE.into())
        );
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "my own project notes\n"
        );
    }

    /// 已有别的项目上下文文件时不写 —— `.hermes.md` 的优先级最高,会把它遮住。
    #[test]
    fn skips_when_another_project_context_file_exists() {
        for name in PROJECT_CONTEXT_FILES {
            let d = dir();
            std::fs::write(d.path().join(name), "x").unwrap();
            // 只断言"因已有项目上下文而跳过";具体报哪个名字随文件系统大小写敏感性而变
            // (macOS 默认不敏感:写 agents.md 时 AGENTS.md 也算存在)。
            assert!(
                matches!(
                    ensure_companion_context(d.path()),
                    Outcome::SkippedUserContext(_)
                ),
                "{name}"
            );
            assert!(!d.path().join(CONTEXT_FILE).exists(), "{name}");
        }
        let d = dir();
        std::fs::create_dir_all(d.path().join(".cursor/rules")).unwrap();
        assert_eq!(
            ensure_companion_context(d.path()),
            Outcome::SkippedUserContext(".cursor/rules".into())
        );
    }

    /// 在 git 工作树里(哪怕是子目录)不写:会遮住仓库根的 AGENTS.md 链。
    #[test]
    fn skips_inside_a_git_work_tree() {
        let d = dir();
        std::fs::create_dir_all(d.path().join(".git")).unwrap();
        let sub = d.path().join("pkg/app");
        std::fs::create_dir_all(&sub).unwrap();
        assert_eq!(ensure_companion_context(&sub), Outcome::SkippedInGitRepo);
        assert!(!sub.join(CONTEXT_FILE).exists());
    }

    #[test]
    fn missing_dir_is_skipped_not_created() {
        let d = dir();
        let gone = d.path().join("not-there");
        assert_eq!(ensure_companion_context(&gone), Outcome::SkippedNoDir);
        assert!(!gone.exists());
    }
}
