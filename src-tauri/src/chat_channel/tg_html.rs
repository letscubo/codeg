//! fork(letscubo)专属: agent 回复(Markdown)→ Telegram HTML(`parse_mode: "HTML"`)。
//!
//! Telegram 只认一小撮 HTML 标签(b / i / s / u / code / pre / a / blockquote),其余文本
//! 必须转义 `& < >`。这里覆盖 agent 回复里常见的写法:
//!
//! | Markdown                | Telegram                                   |
//! |-------------------------|--------------------------------------------|
//! | ```` ```lang ````代码块   | `<pre><code class="language-lang">`        |
//! | `` `code` ``            | `<code>`                                   |
//! | `**粗**` / `__粗__`       | `<b>`                                      |
//! | `*斜*`                   | `<i>`(`_斜_` 不转,免得误伤 snake_case)       |
//! | `~~删~~`                 | `<s>`                                      |
//! | `[文字](https://…)`      | `<a href>`                                 |
//! | `# 标题`                  | `<b>标题</b>`(Telegram 没有标题)             |
//! | `> 引用`                  | `<blockquote>`                             |
//! | `- 列表` / `* 列表`        | `• 列表`                                    |
//! | `|表格|`                  | `<pre>` 原样等宽(Telegram 没有表格)          |
//! | `---`                    | 一条横线字符                                 |
//!
//! 正式消息和流式草稿都用它。只给**成对闭合**的标记加格式,写到一半的 `**` / `` ` `` 原样留着,
//! 没闭合的代码块自动补上 —— 所以半截的草稿也产出合法 HTML。发送方在 Telegram 报解析错误时
//! 退回纯文本重发,所以这里转换不完美也不会丢消息。

use regex::Regex;
use std::sync::OnceLock;

/// 一段 Markdown 转成 Telegram HTML。
///
/// `open_fence`:上一段结束时还没闭合的代码块语言(长回复按字数切成多条时,代码块可能被
/// 切断)。进来时非空 → 这一段从代码块内部开始;出去时写回本段结束时的状态。每条消息里
/// 的代码块都会自己闭合,切断处两边各自成块。
pub fn render_chunk(md: &str, open_fence: &mut Option<String>) -> String {
    let mut out: Vec<String> = Vec::new();
    let lines: Vec<&str> = md.split('\n').collect();
    let mut i = 0;

    // 从上一段带过来的未闭合代码块
    if let Some(lang) = open_fence.take() {
        let (block, next, closed) = collect_fence(&lines, 0);
        out.push(pre_block(&lang, &block));
        if !closed {
            *open_fence = Some(lang);
        }
        i = next;
    }

    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim_start();

        if let Some(lang) = trimmed.strip_prefix("```") {
            let lang = lang.trim().to_string();
            let (block, next, closed) = collect_fence(&lines, i + 1);
            out.push(pre_block(&lang, &block));
            if !closed {
                *open_fence = Some(lang);
            }
            i = next;
            continue;
        }

        if trimmed.starts_with('|') {
            let mut rows = Vec::new();
            while i < lines.len() && lines[i].trim_start().starts_with('|') {
                rows.push(lines[i].trim_end());
                i += 1;
            }
            out.push(format!("<pre>{}</pre>", escape(&rows.join("\n"))));
            continue;
        }

        if trimmed.starts_with('>') {
            let mut quoted = Vec::new();
            while i < lines.len() && lines[i].trim_start().starts_with('>') {
                let q = lines[i].trim_start()[1..].strip_prefix(' ').unwrap_or(&lines[i].trim_start()[1..]);
                quoted.push(inline(q));
                i += 1;
            }
            out.push(format!("<blockquote>{}</blockquote>", quoted.join("\n")));
            continue;
        }

        out.push(block_line(line));
        i += 1;
    }
    out.join("\n")
}

/// 从 `start` 收代码块内容直到收尾的 ```;返回(内容, 下一行下标, 是否闭合)。
fn collect_fence(lines: &[&str], start: usize) -> (String, usize, bool) {
    let mut body = Vec::new();
    let mut j = start;
    while j < lines.len() {
        if lines[j].trim_start().starts_with("```") {
            return (body.join("\n"), j + 1, true);
        }
        body.push(lines[j]);
        j += 1;
    }
    (body.join("\n"), j, false)
}

fn pre_block(lang: &str, body: &str) -> String {
    let lang: String = lang
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '+'))
        .collect();
    if lang.is_empty() {
        format!("<pre>{}</pre>", escape(body))
    } else {
        format!(
            "<pre><code class=\"language-{lang}\">{}</code></pre>",
            escape(body)
        )
    }
}

fn block_line(line: &str) -> String {
    let trimmed = line.trim_start();
    let indent = &line[..line.len() - trimmed.len()];

    if let Some(caps) = re(&HEADING, r"^#{1,6}\s+(.*)$").captures(trimmed) {
        return format!("<b>{}</b>", inline(caps[1].trim_end_matches('#').trim()));
    }
    if re(&RULE, r"^(-{3,}|\*{3,}|_{3,})\s*$").is_match(trimmed) {
        return "──────────".to_string();
    }
    if let Some(caps) = re(&BULLET, r"^[-*+]\s+(.*)$").captures(trimmed) {
        return format!("{indent}• {}", inline(&caps[1]));
    }
    inline(line)
}

/// 行内:先按反引号切出 `code`,其余部分转义后再套粗体 / 删除线 / 斜体 / 链接。
fn inline(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find('`') {
        let after = &rest[start + 1..];
        match after.find('`') {
            Some(end) if end > 0 => {
                out.push_str(&inline_plain(&rest[..start]));
                out.push_str(&format!("<code>{}</code>", escape(&after[..end])));
                rest = &after[end + 1..];
            }
            _ => break,
        }
    }
    out.push_str(&inline_plain(rest));
    out
}

fn inline_plain(text: &str) -> String {
    let s = escape(text);
    let s = re(&LINK, r"\[([^\]\n]+)\]\((https?://[^\s)]+)\)")
        .replace_all(&s, r#"<a href="$2">$1</a>"#);
    let s = re(&BOLD_STAR, r"\*\*([^*\n]+?)\*\*").replace_all(&s, "<b>$1</b>");
    let s = re(&BOLD_UNDER, r"__([^_\n]+?)__").replace_all(&s, "<b>$1</b>");
    let s = re(&STRIKE, r"~~([^~\n]+?)~~").replace_all(&s, "<s>$1</s>");
    let s = re(&ITALIC, r"(^|[\s(（:：])\*([^*\s](?:[^*\n]*[^*\s])?)\*").replace_all(&s, "$1<i>$2</i>");
    s.into_owned()
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

static HEADING: OnceLock<Regex> = OnceLock::new();
static RULE: OnceLock<Regex> = OnceLock::new();
static BULLET: OnceLock<Regex> = OnceLock::new();
static LINK: OnceLock<Regex> = OnceLock::new();
static BOLD_STAR: OnceLock<Regex> = OnceLock::new();
static BOLD_UNDER: OnceLock<Regex> = OnceLock::new();
static STRIKE: OnceLock<Regex> = OnceLock::new();
static ITALIC: OnceLock<Regex> = OnceLock::new();

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).expect("static regex"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(md: &str) -> String {
        render_chunk(md, &mut None)
    }

    #[test]
    fn inline_styles_and_escaping() {
        assert_eq!(r("**运行环境**:a < b & c"), "<b>运行环境</b>:a &lt; b &amp; c");
        assert_eq!(r("用 `rm -rf <dir>` 删"), "用 <code>rm -rf &lt;dir&gt;</code> 删");
        assert_eq!(r("~~旧~~ 新"), "<s>旧</s> 新");
        assert_eq!(r("这是 *重点* 内容"), "这是 <i>重点</i> 内容");
        assert_eq!(r("见 [文档](https://a.com/x?y=1&z=2)"), r#"见 <a href="https://a.com/x?y=1&amp;z=2">文档</a>"#);
    }

    #[test]
    fn snake_case_and_math_are_left_alone() {
        assert_eq!(r("my_var_name 和 2 * 3 * 4"), "my_var_name 和 2 * 3 * 4");
    }

    #[test]
    fn blocks() {
        assert_eq!(r("# 服务器状态总结"), "<b>服务器状态总结</b>");
        assert_eq!(r("- 根分区 `/` 充裕\n  - 子项"), "• 根分区 <code>/</code> 充裕\n  • 子项");
        assert_eq!(r("> 引用一\n> 引用二"), "<blockquote>引用一\n引用二</blockquote>");
        assert_eq!(r("---"), "──────────");
        assert_eq!(r("|a|b|\n|-|-|\n|1|2|"), "<pre>|a|b|\n|-|-|\n|1|2|</pre>");
    }

    #[test]
    fn half_written_markdown_stays_valid_for_drafts() {
        // 草稿里写到一半:没闭合的标记原样留着,不产出半截标签
        assert_eq!(r("## 服务器 **内存状态"), "<b>服务器 **内存状态</b>");
        assert_eq!(r("看 `df -h"), "看 `df -h");
        assert_eq!(r("```bash\ndf -h"), "<pre><code class=\"language-bash\">df -h</code></pre>");
    }

    #[test]
    fn fenced_code_keeps_content_verbatim() {
        let md = "看:\n```bash\necho \"**x**\" <in\n```\n完";
        assert_eq!(
            r(md),
            "看:\n<pre><code class=\"language-bash\">echo \"**x**\" &lt;in</code></pre>\n完"
        );
    }

    #[test]
    fn fence_split_across_chunks_stays_code_on_both_sides() {
        let mut open = None;
        let a = render_chunk("前\n```rust\nfn a() {}", &mut open);
        assert_eq!(a, "前\n<pre><code class=\"language-rust\">fn a() {}</code></pre>");
        assert_eq!(open.as_deref(), Some("rust"));
        let b = render_chunk("fn b() {}\n```\n后", &mut open);
        assert_eq!(b, "<pre><code class=\"language-rust\">fn b() {}</code></pre>\n后");
        assert_eq!(open, None);
    }
}
