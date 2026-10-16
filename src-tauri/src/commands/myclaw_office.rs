//! fork(letscubo)专属: 内置 Office 工具 —— documents / pdf / presentations / spreadsheets 四组,
//! 挂在伴生服务 `myclaw` 上,每个会话自动就有(不是应用、不要平台投影)。
//!
//! ## 分工
//!
//! 模型照旧在 shell 里用 `officecli` 建 / 改文件;这里只给它三样 shell 里拿不顺手的东西:
//!
//!   · `*_guide`  —— 该格式的完整做法(`officecli load_skill <scene>`)+ 平台的交付规矩;
//!   · `*_render` —— 截图,以 MCP `image` 块回给模型,让它**看见**做出来的东西;
//!   · `*_check`  —— schema 校验 + 问题扫描 + 大纲,一次给全。
//!
//! PDF 没有 officecli 的导出插件(容器实测 `view pdf` 报 No exporter plugin),所以:
//! 导出 = `officecli view html` → chromium(playwright,镜像自带)打印;读 / 截图 = poppler
//! (`pdftotext` / `pdftoppm`,镜像里要装 `poppler-utils`,老镜像没有就明确报错)。
//!
//! ## 唯一一份实现
//!
//! stdio 伴生(经 UDS 交给父进程 listener)与 HTTP 端点(openclaw / pi)都调 [`call`]。
//! 返回值是完整的 MCP `CallToolResult`,两边原样转出 —— 文案只在这里改。
//!
//! ## 路径
//!
//! 相对路径按会话目录解析;HTTP 那条没有会话目录,只认绝对路径或 `~`。解析后必须落在
//! 家目录、`/tmp` 或会话目录之下 —— agent 本来就有 shell,这不是安全边界,只是不让这几个
//! 工具成为读任意文件的捷径。截图写到 `<会话目录 | 文件所在目录>/.codeg-media/office/`。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;

use base64::Engine as _;
use serde_json::{json, Value};

use crate::commands::acp::resolve_command_on_path;
use crate::commands::office_tools::resolve_officecli;
use crate::process::tokio_command;

/// 四组工具的 MCP 定义(13 个)。伴生与 HTTP 端点的 `tools/list` 都从这里取。
pub const OFFICE_TOOL_SCHEMA_JSON: &str = include_str!("myclaw_office_tools.json");

/// 单次最多回几张图 —— 多了模型也看不过来,更多页让它分批或看接触表。
const MAX_IMAGES: usize = 8;
/// 单张图原始字节上限;超过的不内联,只给路径。
const MAX_IMAGE_BYTES: usize = 3 * 1024 * 1024;
/// 一次结果里图片原始字节总和上限。base64 后约 8 MiB,在 UDS 16 MiB 帧上限之内。
const MAX_TOTAL_IMAGE_BYTES: usize = 6 * 1024 * 1024;
/// 文本输出(指南、PDF 正文、检查结果)的字符上限。
const MAX_TEXT_CHARS: usize = 120_000;

const RENDER_TIMEOUT: Duration = Duration::from_secs(120);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfficeGroup {
    Documents,
    Pdf,
    Presentations,
    Spreadsheets,
}

impl OfficeGroup {
    pub const ALL: [OfficeGroup; 4] = [
        OfficeGroup::Documents,
        OfficeGroup::Pdf,
        OfficeGroup::Presentations,
        OfficeGroup::Spreadsheets,
    ];

    /// `--features` 里的组名,也是工具名前缀。
    pub fn feature(self) -> &'static str {
        match self {
            OfficeGroup::Documents => "documents",
            OfficeGroup::Pdf => "pdf",
            OfficeGroup::Presentations => "presentations",
            OfficeGroup::Spreadsheets => "spreadsheets",
        }
    }

    /// 工具属于哪一组 —— 只认 schema 里真有的名字,前缀对上但名字不存在的不算。
    pub fn of_tool(name: &str) -> Option<OfficeGroup> {
        if !tool_names().contains(&name) {
            return None;
        }
        OfficeGroup::ALL
            .into_iter()
            .find(|g| name.strip_prefix(g.feature()).is_some_and(|rest| rest.starts_with('_')))
    }

    /// 需要 officecli 的组。pdf 组各工具自己报缺什么。
    pub fn needs_officecli(self) -> bool {
        !matches!(self, OfficeGroup::Pdf)
    }

    /// 可接受的扩展名(含宏版本)。
    fn extensions(self) -> &'static [&'static str] {
        match self {
            OfficeGroup::Documents => &["docx", "docm"],
            OfficeGroup::Presentations => &["pptx", "pptm"],
            OfficeGroup::Spreadsheets => &["xlsx", "xlsm"],
            OfficeGroup::Pdf => &["pdf"],
        }
    }

    /// `*_guide` 的 scene 白名单,第一个是缺省值。与 `officecli load_skill` 的名字一致。
    fn scenes(self) -> &'static [&'static str] {
        match self {
            OfficeGroup::Documents => &["word", "academic-paper", "word-form"],
            OfficeGroup::Presentations => &["pptx", "pitch-deck", "morph-ppt", "morph-ppt-3d"],
            OfficeGroup::Spreadsheets => &["excel", "financial-model", "data-dashboard"],
            OfficeGroup::Pdf => &[],
        }
    }
}

fn schema_tools() -> &'static [Value] {
    static TOOLS: OnceLock<Vec<Value>> = OnceLock::new();
    TOOLS.get_or_init(|| {
        serde_json::from_str::<Value>(OFFICE_TOOL_SCHEMA_JSON)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
    })
}

fn tool_names() -> &'static [&'static str] {
    static NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();
    NAMES.get_or_init(|| {
        schema_tools()
            .iter()
            .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
            .collect()
    })
}

/// 内置 Office 工具的全部名字(不论组开没开)—— 平台工具不能与之同名。
pub fn is_office_tool_name(name: &str) -> bool {
    tool_names().contains(&name)
}

/// 按开启的组过滤出的工具定义,顺序同 schema 文件。
pub fn tools_for(enabled: impl Fn(OfficeGroup) -> bool) -> Vec<Value> {
    schema_tools()
        .iter()
        .filter(|t| {
            t.get("name")
                .and_then(|n| n.as_str())
                .and_then(OfficeGroup::of_tool)
                .is_some_and(&enabled)
        })
        .cloned()
        .collect()
}

/// 本机是否装了 officecli —— 决定 documents / presentations / spreadsheets 三组开不开。
pub fn officecli_available() -> bool {
    resolve_officecli().is_some()
}

/// 本机该开哪几组:documents / presentations / spreadsheets 要 officecli;pdf 常开,缺依赖由工具自报。
/// stdio 注入、HTTP 端点与 Hermes 的 `.hermes.md` 同一判据。
pub fn host_office_features() -> crate::acp::delegation::companion::OfficeFeatures {
    let officecli = officecli_available();
    crate::acp::delegation::companion::OfficeFeatures {
        documents: officecli,
        pdf: true,
        presentations: officecli,
        spreadsheets: officecli,
    }
}

/// 外部程序的位置。测试里换成假脚本。
#[derive(Debug, Clone, Default)]
pub(crate) struct Binaries {
    pub officecli: Option<PathBuf>,
    pub pdftotext: Option<PathBuf>,
    pub pdftoppm: Option<PathBuf>,
    pub python: Option<PathBuf>,
}

impl Binaries {
    fn detect() -> Self {
        Self {
            officecli: resolve_officecli(),
            pdftotext: resolve_command_on_path("pdftotext"),
            pdftoppm: resolve_command_on_path("pdftoppm"),
            python: resolve_command_on_path("python3"),
        }
    }
}

/// 执行一个内置 Office 工具,返回完整的 MCP `CallToolResult`。失败也是结果(`isError: true`),
/// 不是协议错误 —— 模型要读到原因自己改。
pub async fn call(name: &str, arguments: &Value, working_dir: Option<&Path>) -> Value {
    call_with(name, arguments, working_dir, &Binaries::detect()).await
}

pub(crate) async fn call_with(
    name: &str,
    arguments: &Value,
    working_dir: Option<&Path>,
    bins: &Binaries,
) -> Value {
    let Some(group) = OfficeGroup::of_tool(name) else {
        return error_result(format!("unknown tool: {name}"));
    };
    let action = &name[group.feature().len() + 1..];
    let outcome = match (group, action) {
        (OfficeGroup::Pdf, "guide") => Ok(text_result(format!("{PDF_GUIDE}\n\n---\n\n{PLATFORM_RULES}"))),
        (OfficeGroup::Pdf, "export") => pdf_export(arguments, working_dir, bins).await,
        (OfficeGroup::Pdf, "read") => pdf_read(arguments, working_dir, bins).await,
        (OfficeGroup::Pdf, "render") => pdf_render(arguments, working_dir, bins).await,
        (_, "guide") => office_guide(group, arguments, bins).await,
        (_, "render") => office_render(group, arguments, working_dir, bins).await,
        (_, "check") => office_check(group, arguments, working_dir, bins).await,
        _ => Err(format!("unknown tool: {name}")),
    };
    outcome.unwrap_or_else(error_result)
}

// ─── guide ──────────────────────────────────────────────────────────────

async fn office_guide(group: OfficeGroup, args: &Value, bins: &Binaries) -> Result<Value, String> {
    let scenes = group.scenes();
    let scene = match args.get("scene").and_then(|v| v.as_str()).map(str::trim) {
        None | Some("") => scenes[0],
        Some(s) => *scenes
            .iter()
            .find(|known| **known == s)
            .ok_or_else(|| format!("unknown scene `{s}`; use one of: {}", scenes.join(", ")))?,
    };
    let guide = load_skill(scene, bins).await?;
    Ok(text_result(format!("{guide}\n\n---\n\n{PLATFORM_RULES}")))
}

// ─── `@` 点名时预载指南 ──────────────────────────────────────────────────

/// 用户在输入框 `@` 了内置能力时,平台在这一轮附的传输块(web: `lib/apps/builtin-mention.ts`)。
/// 形如 `<myclaw_builtin_mention scenes="pptx">…</myclaw_builtin_mention>`。
pub const BUILTIN_MENTION_TAG: &str = "myclaw_builtin_mention";
/// codeg 展开进去的指南块。与上面那块一样只在传输层存在,平台同步历史时剥掉。
pub const OFFICE_GUIDE_TAG: &str = "myclaw_office_guide";
/// 一轮最多预载几份指南(每份 35–44KB)。
const MAX_PRELOADED_SCENES: usize = 2;

/// 文本里点名块的 `scenes` 属性 → 白名单内、去重保序的 scene,最多 [`MAX_PRELOADED_SCENES`] 个。
pub fn mentioned_scenes(text: &str) -> Vec<&'static str> {
    let open = format!("<{BUILTIN_MENTION_TAG}");
    let mut out: Vec<&'static str> = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find(&open) {
        let tag = &rest[at..];
        let end = tag.find('>').unwrap_or(tag.len());
        let head = &tag[..end];
        if let Some(value) = head
            .split_once("scenes=\"")
            .and_then(|(_, v)| v.split_once('"'))
            .map(|(v, _)| v)
        {
            for scene in value.split(',').map(str::trim) {
                let known = OfficeGroup::ALL
                    .into_iter()
                    .flat_map(|g| g.scenes().iter().copied())
                    .find(|s| *s == scene);
                if let Some(known) = known {
                    if !out.contains(&known) && out.len() < MAX_PRELOADED_SCENES {
                        out.push(known);
                    }
                }
            }
        }
        rest = &tag[end..];
    }
    out
}

/// 点名了 Office 能力的那一轮:直接把对应指南塞进本轮(不等模型自己去调 `*_guide`),
/// 接在 blocks 末尾。取不到(没装 officecli 等)就跳过,只记日志 —— 绝不卡对话。
pub async fn preload_mentioned_guides(blocks: &mut Vec<crate::acp::types::PromptInputBlock>) {
    use crate::acp::types::PromptInputBlock;
    let mut scenes: Vec<&'static str> = Vec::new();
    for block in blocks.iter() {
        if let PromptInputBlock::Text { text } = block {
            for scene in mentioned_scenes(text) {
                if !scenes.contains(&scene) && scenes.len() < MAX_PRELOADED_SCENES {
                    scenes.push(scene);
                }
            }
        }
    }
    if scenes.is_empty() {
        return;
    }
    let bins = Binaries::detect();
    for scene in scenes {
        match load_skill(scene, &bins).await {
            Ok(guide) => {
                blocks.push(PromptInputBlock::Text {
                    text: format!(
                        "\n\n<{OFFICE_GUIDE_TAG} scene=\"{scene}\">\nThe user picked this capability for this \
request, so its guide is loaded for you below. Follow it; do not call the guide tool again for it.\n\n\
{guide}\n\n---\n\n{PLATFORM_RULES}\n</{OFFICE_GUIDE_TAG}>"
                    ),
                });
                tracing::info!("[MyclawOffice] preloaded the {scene} guide for a mentioned capability");
            }
            Err(e) => tracing::warn!("[MyclawOffice] could not preload the {scene} guide: {e}"),
        }
    }
}

async fn load_skill(scene: &str, bins: &Binaries) -> Result<String, String> {
    let officecli = require(&bins.officecli, OFFICECLI_MISSING)?;
    let mut cmd = tokio_command(officecli);
    cmd.arg("load_skill").arg(scene);
    let out = run(cmd, COMMAND_TIMEOUT).await?;
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if !out.status.success() || text.is_empty() {
        return Err(format!(
            "officecli load_skill {scene} failed: {}",
            stderr_tail(&out)
        ));
    }
    Ok(truncate_chars(&text, MAX_TEXT_CHARS))
}

// ─── render ─────────────────────────────────────────────────────────────

async fn office_render(
    group: OfficeGroup,
    args: &Value,
    working_dir: Option<&Path>,
    bins: &Binaries,
) -> Result<Value, String> {
    let officecli = require(&bins.officecli, OFFICECLI_MISSING)?.clone();
    let file = resolve_input(args, working_dir, group)?;
    flush(&officecli, &file).await;
    let media = media_dir(working_dir, &file)?;
    let stem = file_stem(&file);
    let pages = match args.get("pages") {
        None | Some(Value::Null) => None,
        Some(v) => Some(parse_pages(v)?),
    };
    let range = args
        .get("range")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    // (输出文件, 额外参数)
    let mut shots: Vec<(PathBuf, Vec<String>)> = Vec::new();
    if let Some(range) = range {
        shots.push((
            media.join(format!("{stem}-range.png")),
            vec!["--range".into(), range.to_string()],
        ));
    } else if let Some(pages) = &pages {
        for p in pages {
            shots.push((
                media.join(format!("{stem}-p{p}.png")),
                vec!["--page".into(), p.to_string()],
            ));
        }
    } else if group == OfficeGroup::Spreadsheets {
        shots.push((media.join(format!("{stem}.png")), Vec::new()));
    } else {
        let grid = match args.get("grid").and_then(|v| v.as_u64()) {
            Some(n) if (1..=8).contains(&n) => n.to_string(),
            _ => "auto".to_string(),
        };
        shots.push((
            media.join(format!("{stem}-overview.png")),
            vec!["--grid".into(), grid],
        ));
    }

    let mut total_pages: Option<u32> = None;
    let mut images = Vec::new();
    for (out_path, extra) in &shots {
        let _ = std::fs::remove_file(out_path);
        let mut cmd = tokio_command(&officecli);
        cmd.arg("view")
            .arg(&file)
            .arg("screenshot")
            .args(extra)
            .arg("-o")
            .arg(out_path)
            .arg("--json");
        let out = run(cmd, RENDER_TIMEOUT).await?;
        let envelope: Value = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
        if total_pages.is_none() {
            total_pages = pages_total(&envelope);
        }
        let ok = envelope.get("success").and_then(|v| v.as_bool()) == Some(true);
        if !ok || !out_path.is_file() {
            let reason = envelope_error(&envelope)
                .or_else(|| envelope_warnings(&envelope))
                .unwrap_or_else(|| stderr_tail(&out));
            // 容器实测:区域截图(clip)要 Chrome 系浏览器,officecli 在镜像里找不到;整表 / 按页可用
            let hint = if range.is_some() {
                " Rendering a cell range is not available here; call it again without `range` (use `pages` for a sheet)."
            } else {
                ""
            };
            return Err(format!("officecli could not render {}: {reason}{hint}", file.display()));
        }
        images.push(out_path.clone());
    }

    let what = match (range, &pages) {
        (Some(r), _) => format!("range {r}"),
        (None, Some(p)) => format!("{} {}", unit(group, p.len()), join_u32(p)),
        (None, None) if group == OfficeGroup::Spreadsheets => "the first sheet".to_string(),
        (None, None) => "a contact sheet of every page".to_string(),
    };
    let total = total_pages
        .map(|n| format!(" ({n} {} in total)", unit(group, n as usize)))
        .unwrap_or_default();
    let summary = format!(
        "Rendered {what} of {}{total}.\n\
         Look at every image: overflowing or clipped text, overlapping or off-page boxes, empty \
         areas, wrong fonts, low contrast. Fix what you find and render again before delivering. \
         Pass `pages` to look at individual {} at full size.",
        file.display(),
        unit(group, 2),
    );
    Ok(image_result(summary, &images))
}

fn unit(group: OfficeGroup, n: usize) -> &'static str {
    match (group, n == 1) {
        (OfficeGroup::Presentations, true) => "slide",
        (OfficeGroup::Presentations, false) => "slides",
        (OfficeGroup::Spreadsheets, true) => "sheet",
        (OfficeGroup::Spreadsheets, false) => "sheets",
        (_, true) => "page",
        (_, false) => "pages",
    }
}

/// officecli 的 `--json` 封包把总页数放在 warning 里:`[pages] total=3`。
fn pages_total(envelope: &Value) -> Option<u32> {
    envelope
        .get("warnings")?
        .as_array()?
        .iter()
        .filter_map(|w| w.get("message").and_then(|m| m.as_str()))
        .find_map(|m| m.strip_prefix("[pages] total="))
        .and_then(|n| n.trim().parse().ok())
}

/// 渲染失败时原因常常只在 warnings 里(如找不到浏览器)。
fn envelope_warnings(envelope: &Value) -> Option<String> {
    let messages: Vec<&str> = envelope
        .get("warnings")?
        .as_array()?
        .iter()
        .filter_map(|w| w.get("message").and_then(|m| m.as_str()))
        .filter(|m| !m.starts_with("[pages]"))
        .collect();
    (!messages.is_empty()).then(|| messages.join("; "))
}

/// officecli 的失败封包形如 `{ "success": false, "error": { "error": "File not found: …", "code": … } }`,
/// 也见过 `error` 直接是字符串的。
fn envelope_error(envelope: &Value) -> Option<String> {
    let err = envelope.get("error")?;
    err.as_str()
        .or_else(|| err.get("error").and_then(|m| m.as_str()))
        .or_else(|| err.get("message").and_then(|m| m.as_str()))
        .map(str::to_string)
        .or_else(|| Some(err.to_string()))
}

// ─── check ──────────────────────────────────────────────────────────────

async fn office_check(
    group: OfficeGroup,
    args: &Value,
    working_dir: Option<&Path>,
    bins: &Binaries,
) -> Result<Value, String> {
    let officecli = require(&bins.officecli, OFFICECLI_MISSING)?.clone();
    let file = resolve_input(args, working_dir, group)?;
    flush(&officecli, &file).await;

    let validate = officecli_json(&officecli, &file, &["validate"]).await;
    let issues = officecli_json(&officecli, &file, &["view", "issues"]).await;
    let outline = officecli_json(&officecli, &file, &["view", "outline"]).await;

    let issue_count = issues
        .get("data")
        .and_then(|d| d.get("count"))
        .and_then(|c| c.as_u64());
    let valid = validate.get("success").and_then(|v| v.as_bool()) == Some(true);
    let verdict = match (valid, issue_count) {
        (true, Some(0)) => "Clean: schema validation passed and the issue scan found nothing. Still render it and look before delivering.".to_string(),
        (true, Some(n)) => format!("{n} issue(s) found. Fix each one, then run this check again."),
        (false, _) => "Schema validation FAILED. Fix the errors below, then run this check again.".to_string(),
        (true, None) => "Schema validation passed; the issue scan returned no count — read its output below.".to_string(),
    };
    let report = json!({
        "file": file.display().to_string(),
        "validate": data_or_error(&validate),
        "issues": data_or_error(&issues),
        "outline": data_or_error(&outline),
    });
    let body = serde_json::to_string_pretty(&report).unwrap_or_default();
    Ok(text_result(format!("{verdict}\n\n{}", truncate_chars(&body, MAX_TEXT_CHARS))))
}

/// 跑一条 `officecli <args> <file> --json`,返回它的封包;跑不起来时造一个失败封包。
async fn officecli_json(officecli: &Path, file: &Path, verb: &[&str]) -> Value {
    let mut cmd = tokio_command(officecli);
    cmd.arg(verb[0]).arg(file).args(&verb[1..]).arg("--json");
    match run(cmd, COMMAND_TIMEOUT).await {
        Ok(out) => serde_json::from_slice(&out.stdout).unwrap_or_else(|_| {
            json!({ "success": false, "error": stderr_tail(&out) })
        }),
        Err(e) => json!({ "success": false, "error": e }),
    }
}

fn data_or_error(envelope: &Value) -> Value {
    if envelope.get("success").and_then(|v| v.as_bool()) == Some(true) {
        envelope.get("data").cloned().unwrap_or(Value::Null)
    } else {
        json!({ "error": envelope_error(envelope).unwrap_or_else(|| "failed".to_string()) })
    }
}

// ─── pdf ────────────────────────────────────────────────────────────────

async fn pdf_export(args: &Value, working_dir: Option<&Path>, bins: &Binaries) -> Result<Value, String> {
    let source = resolve_path_arg(args, "path", working_dir, true)?;
    let ext = extension(&source);
    let is_html = matches!(ext.as_str(), "html" | "htm");
    let office_group = [
        OfficeGroup::Documents,
        OfficeGroup::Presentations,
        OfficeGroup::Spreadsheets,
    ]
    .into_iter()
    .find(|g| g.extensions().contains(&ext.as_str()));
    if !is_html && office_group.is_none() {
        return Err(format!(
            "pdf_export takes a .docx, .pptx, .xlsx or .html file, not `{}`",
            source.display()
        ));
    }
    let out = match args.get("out").and_then(|v| v.as_str()).map(str::trim) {
        Some(o) if !o.is_empty() => resolve_output(o, working_dir)?,
        _ => source.with_extension("pdf"),
    };
    if extension(&out) != "pdf" {
        return Err(format!("`out` must end in .pdf, got `{}`", out.display()));
    }
    let python = require(&bins.python, "python3 is not available on this instance")?.clone();

    let html = if is_html {
        source.clone()
    } else {
        let officecli = require(&bins.officecli, OFFICECLI_MISSING)?.clone();
        flush(&officecli, &source).await;
        let html = media_dir(working_dir, &source)?.join(format!("{}-export.html", file_stem(&source)));
        let mut cmd = tokio_command(&officecli);
        cmd.arg("view").arg(&source).arg("html").arg("-o").arg(&html);
        let res = run(cmd, RENDER_TIMEOUT).await?;
        if !res.status.success() || !html.is_file() {
            return Err(format!("officecli could not convert {} to HTML: {}", source.display(), stderr_tail(&res)));
        }
        html
    };

    let _ = std::fs::remove_file(&out);
    let mut cmd = tokio_command(&python);
    cmd.arg("-c").arg(PRINT_PDF_SCRIPT).arg(&html).arg(&out);
    if std::env::var_os("PLAYWRIGHT_BROWSERS_PATH").is_none() && Path::new(DEFAULT_BROWSERS_PATH).is_dir() {
        cmd.env("PLAYWRIGHT_BROWSERS_PATH", DEFAULT_BROWSERS_PATH);
    }
    let res = run(cmd, RENDER_TIMEOUT).await?;
    let size = std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0);
    if !res.status.success() || size == 0 {
        return Err(format!("printing {} to PDF failed: {}", html.display(), stderr_tail(&res)));
    }
    Ok(text_result(format!(
        "Exported {} ({size} bytes). Look at it with pdf_render before delivering it.",
        out.display()
    )))
}

async fn pdf_read(args: &Value, working_dir: Option<&Path>, bins: &Binaries) -> Result<Value, String> {
    let file = resolve_input(args, working_dir, OfficeGroup::Pdf)?;
    let pdftotext = require(&bins.pdftotext, POPPLER_MISSING)?.clone();
    let span = parse_span(args.get("pages"))?;
    let mut cmd = tokio_command(&pdftotext);
    cmd.arg("-layout");
    if let Some((first, last)) = span {
        cmd.arg("-f").arg(first.to_string()).arg("-l").arg(last.to_string());
    }
    cmd.arg(&file).arg("-");
    let out = run(cmd, COMMAND_TIMEOUT).await?;
    if !out.status.success() {
        return Err(format!("pdftotext failed on {}: {}", file.display(), stderr_tail(&out)));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let text = text.trim();
    if text.is_empty() {
        return Ok(text_result(format!(
            "{} has no extractable text (it may be scanned images). Use pdf_render to look at it.",
            file.display()
        )));
    }
    Ok(text_result(truncate_chars(text, MAX_TEXT_CHARS)))
}

async fn pdf_render(args: &Value, working_dir: Option<&Path>, bins: &Binaries) -> Result<Value, String> {
    let file = resolve_input(args, working_dir, OfficeGroup::Pdf)?;
    let pdftoppm = require(&bins.pdftoppm, POPPLER_MISSING)?.clone();
    let (first, last) = parse_span(args.get("pages"))?.unwrap_or((1, 4));
    let last = last.min(first + MAX_IMAGES as u32 - 1);
    let media = media_dir(working_dir, &file)?;
    let prefix = format!("{}-pdf", file_stem(&file));
    clear_prefixed(&media, &prefix);
    let mut cmd = tokio_command(&pdftoppm);
    cmd.arg("-png")
        .arg("-r")
        .arg("110")
        .arg("-f")
        .arg(first.to_string())
        .arg("-l")
        .arg(last.to_string())
        .arg(&file)
        .arg(media.join(&prefix));
    let out = run(cmd, RENDER_TIMEOUT).await?;
    if !out.status.success() {
        return Err(format!("pdftoppm failed on {}: {}", file.display(), stderr_tail(&out)));
    }
    let mut images: Vec<PathBuf> = std::fs::read_dir(&media)
        .map_err(|e| e.to_string())?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&format!("{prefix}-")) && n.ends_with(".png"))
        })
        .collect();
    images.sort();
    if images.is_empty() {
        return Err(format!("{} has no page {first}", file.display()));
    }
    let summary = format!(
        "Rendered pages {first}-{} of {}. Look at every image before delivering.",
        first + images.len() as u32 - 1,
        file.display()
    );
    Ok(image_result(summary, &images))
}

/// pdftoppm 按页数补零命名,事先不知道文件名 —— 每次先清掉同前缀的旧图。
fn clear_prefixed(dir: &Path, prefix: &str) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with(&format!("{prefix}-")))
        {
            let _ = std::fs::remove_file(path);
        }
    }
}

// ─── 路径 ───────────────────────────────────────────────────────────────

/// 读 `path` 参数并要求扩展名属于该组。
fn resolve_input(args: &Value, working_dir: Option<&Path>, group: OfficeGroup) -> Result<PathBuf, String> {
    let file = resolve_path_arg(args, "path", working_dir, true)?;
    let ext = extension(&file);
    if !group.extensions().contains(&ext.as_str()) {
        return Err(format!(
            "{}_* tools take {} files, not `{}`",
            group.feature(),
            group
                .extensions()
                .iter()
                .map(|e| format!(".{e}"))
                .collect::<Vec<_>>()
                .join(" / "),
            file.display()
        ));
    }
    Ok(file)
}

fn resolve_path_arg(
    args: &Value,
    key: &str,
    working_dir: Option<&Path>,
    must_exist: bool,
) -> Result<PathBuf, String> {
    let raw = args
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("`{key}` is required (the file's path)"))?;
    let path = absolutize(raw, working_dir)?;
    if !must_exist {
        return Ok(path);
    }
    let canonical = std::fs::canonicalize(&path)
        .map_err(|_| format!("no such file: {}", path.display()))?;
    if !canonical.is_file() {
        return Err(format!("not a file: {}", path.display()));
    }
    ensure_allowed(&canonical, working_dir)?;
    Ok(canonical)
}

/// 输出路径:可以还不存在,但所在目录必须存在且在允许范围内。
fn resolve_output(raw: &str, working_dir: Option<&Path>) -> Result<PathBuf, String> {
    let path = absolutize(raw, working_dir)?;
    let parent = path
        .parent()
        .ok_or_else(|| format!("invalid output path: {raw}"))?;
    let parent = std::fs::canonicalize(parent)
        .map_err(|_| format!("the folder for {} does not exist", path.display()))?;
    ensure_allowed(&parent, working_dir)?;
    let name = path
        .file_name()
        .ok_or_else(|| format!("invalid output path: {raw}"))?;
    Ok(parent.join(name))
}

fn absolutize(raw: &str, working_dir: Option<&Path>) -> Result<PathBuf, String> {
    let expanded = if raw == "~" {
        dirs::home_dir().ok_or("cannot resolve ~")?
    } else if let Some(rest) = raw.strip_prefix("~/") {
        dirs::home_dir().ok_or("cannot resolve ~")?.join(rest)
    } else {
        PathBuf::from(raw)
    };
    if expanded.is_absolute() {
        return Ok(expanded);
    }
    match working_dir {
        Some(dir) => Ok(dir.join(expanded)),
        None => Err(format!(
            "`{raw}` is relative; pass an absolute path (or one starting with ~/)"
        )),
    }
}

fn ensure_allowed(canonical: &Path, working_dir: Option<&Path>) -> Result<(), String> {
    let mut roots: Vec<PathBuf> = Vec::new();
    if let Some(home) = dirs::home_dir() {
        roots.push(home);
    }
    roots.push(PathBuf::from("/tmp"));
    roots.push(std::env::temp_dir());
    if let Some(dir) = working_dir {
        roots.push(dir.to_path_buf());
    }
    let allowed = roots.iter().any(|root| {
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.clone());
        canonical.starts_with(root)
    });
    if allowed {
        Ok(())
    } else {
        Err(format!(
            "{} is outside the home and session folders",
            canonical.display()
        ))
    }
}

/// 截图目录:有会话目录用会话目录,否则用文件所在目录。
fn media_dir(working_dir: Option<&Path>, file: &Path) -> Result<PathBuf, String> {
    let base = working_dir
        .map(Path::to_path_buf)
        .or_else(|| file.parent().map(Path::to_path_buf))
        .ok_or("cannot pick a folder for the images")?;
    let dir = base.join(".codeg-media").join("office");
    std::fs::create_dir_all(&dir).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    Ok(dir)
}

fn extension(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn file_stem(path: &Path) -> String {
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
    stem.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

// ─── 页码 ───────────────────────────────────────────────────────────────

/// `"1"`、`"2-4"`、`"1,3,5"`、`3`、`[1,2]` → 去重保序的页码,最多 [`MAX_IMAGES`] 个。
fn parse_pages(value: &Value) -> Result<Vec<u32>, String> {
    let bad = || format!("`pages` must look like \"1\", \"2-4\" or \"1,3,5\" (got {value})");
    let mut out: Vec<u32> = Vec::new();
    fn push(n: u32, out: &mut Vec<u32>) {
        if n >= 1 && !out.contains(&n) {
            out.push(n);
        }
    }
    match value {
        Value::Number(n) => push(n.as_u64().ok_or_else(bad)? as u32, &mut out),
        Value::Array(items) => {
            for item in items {
                push(item.as_u64().ok_or_else(bad)? as u32, &mut out);
            }
        }
        Value::String(s) => {
            for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                if let Some((a, b)) = part.split_once('-') {
                    let a: u32 = a.trim().parse().map_err(|_| bad())?;
                    let b: u32 = b.trim().parse().map_err(|_| bad())?;
                    if a == 0 || b < a {
                        return Err(bad());
                    }
                    for n in a..=b.min(a + MAX_IMAGES as u32) {
                        push(n, &mut out);
                    }
                } else {
                    push(part.parse().map_err(|_| bad())?, &mut out);
                }
            }
        }
        _ => return Err(bad()),
    }
    if out.is_empty() {
        return Err(bad());
    }
    out.truncate(MAX_IMAGES);
    Ok(out)
}

/// PDF 的连续页段:`"3"` → (3,3),`"2-5"` → (2,5)。缺省 None。
fn parse_span(value: Option<&Value>) -> Result<Option<(u32, u32)>, String> {
    let value = match value {
        None | Some(Value::Null) => return Ok(None),
        Some(v) => v,
    };
    let bad = || format!("`pages` must be one page or a span like \"2-5\" (got {value})");
    let s = match value {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.trim().to_string(),
        _ => return Err(bad()),
    };
    let (a, b) = match s.split_once('-') {
        Some((a, b)) => (a.trim().parse::<u32>(), b.trim().parse::<u32>()),
        None => (s.parse::<u32>(), s.parse::<u32>()),
    };
    match (a, b) {
        (Ok(a), Ok(b)) if a >= 1 && b >= a => Ok(Some((a, b))),
        _ => Err(bad()),
    }
}

fn join_u32(items: &[u32]) -> String {
    items.iter().map(u32::to_string).collect::<Vec<_>>().join(", ")
}

// ─── 进程 ───────────────────────────────────────────────────────────────

fn require<'a>(bin: &'a Option<PathBuf>, missing: &str) -> Result<&'a PathBuf, String> {
    bin.as_ref().ok_or_else(|| missing.to_string())
}

async fn run(mut cmd: tokio::process::Command, timeout: Duration) -> Result<std::process::Output, String> {
    cmd.stdin(Stdio::null()).kill_on_drop(true);
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(out)) => Ok(out),
        Ok(Err(e)) => Err(format!("could not start the command: {e}")),
        Err(_) => Err(format!("timed out after {}s", timeout.as_secs())),
    }
}

/// 把 officecli 常驻进程里的改动落盘。没有常驻进程时是空操作,失败也不拦 —— 渲染自己会报。
async fn flush(officecli: &Path, file: &Path) {
    let mut cmd = tokio_command(officecli);
    cmd.arg("save").arg(file);
    let _ = run(cmd, COMMAND_TIMEOUT).await;
}

fn stderr_tail(out: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&out.stderr);
    let text = if stderr.trim().is_empty() {
        String::from_utf8_lossy(&out.stdout).to_string()
    } else {
        stderr.to_string()
    };
    let text = text.trim();
    if text.is_empty() {
        return format!("exit status {}", out.status);
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= 600 {
        text.to_string()
    } else {
        format!("…{}", chars[chars.len() - 600..].iter().collect::<String>())
    }
}

// ─── 结果 ───────────────────────────────────────────────────────────────

fn text_result(text: String) -> Value {
    json!({ "content": [{ "type": "text", "text": text }] })
}

fn error_result(message: String) -> Value {
    json!({ "content": [{ "type": "text", "text": message }], "isError": true })
}

/// 一段说明 + 每张图一个 `image` 块;超出上限的图不内联,只列路径。
fn image_result(summary: String, images: &[PathBuf]) -> Value {
    let mut content = Vec::new();
    let mut listed = Vec::new();
    let mut skipped = Vec::new();
    let mut total = 0usize;
    for path in images.iter().take(MAX_IMAGES) {
        match std::fs::read(path) {
            Ok(bytes) if bytes.len() <= MAX_IMAGE_BYTES && total + bytes.len() <= MAX_TOTAL_IMAGE_BYTES => {
                total += bytes.len();
                listed.push(path.display().to_string());
                content.push(json!({
                    "type": "image",
                    "data": base64::engine::general_purpose::STANDARD.encode(&bytes),
                    "mimeType": "image/png",
                }));
            }
            _ => skipped.push(path.display().to_string()),
        }
    }
    let mut text = format!("{summary}\nImages: {}", listed.join(", "));
    if !skipped.is_empty() {
        text.push_str(&format!(
            "\nToo large to attach, open them from disk instead: {}",
            skipped.join(", ")
        ));
    }
    content.insert(0, json!({ "type": "text", "text": text }));
    json!({ "content": content })
}

fn truncate_chars(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((cut, _)) => format!("{}\n\n[truncated — {} more characters]", &text[..cut], text[cut..].chars().count()),
        None => text.to_string(),
    }
}

// ─── 文案 ───────────────────────────────────────────────────────────────

const OFFICECLI_MISSING: &str =
    "officecli is not installed on this instance, so this tool cannot run. Say so in one sentence and stop.";
const POPPLER_MISSING: &str = "This instance's image has no PDF reader (poppler-utils), so this tool cannot run here. \
Say so in one sentence; the instance needs a newer image.";

const DEFAULT_BROWSERS_PATH: &str = "/opt/pw-browsers";

/// 用 playwright 的 chromium 把一个 HTML 文件打印成 PDF。参数:HTML 路径、PDF 路径。
const PRINT_PDF_SCRIPT: &str = r#"
import pathlib, sys
from playwright.sync_api import sync_playwright
src, out = sys.argv[1], sys.argv[2]
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto(pathlib.Path(src).resolve().as_uri(), wait_until="networkidle")
    page.pdf(path=out, print_background=True, prefer_css_page_size=True)
    browser.close()
"#;

/// 每份指南末尾都接这一段 —— officecli 自己不知道的、只属于这个平台的规矩。
pub const PLATFORM_RULES: &str = "\
# Platform rules (MyClaw)

officecli is already installed — never install, upgrade or reinstall it, and never fall back to \
python-pptx, python-docx, openpyxl, LibreOffice or hand-written OOXML. Start every shell block that \
calls it with `export PATH=\"$HOME/.local/bin:$PATH\"`. If `officecli --version` fails, say so in \
one sentence and stop.

1. Work in this conversation's session directory (the path in the [Working directory: …] line) and \
use relative paths there. Every tool call runs in a fresh shell, so `cd` at the top of each block.
2. Pass `--locale` when you create a file (`officecli create deck.pptx --locale zh-CN`), matching \
the language of the content — otherwise CJK / Arabic / Hebrew text gets the wrong default fonts.
3. Never guess a command, flag or property: `officecli help <format> <element>` prints the real \
schema. Geometry always carries a unit (`--prop x=2cm`); a bare number is EMU and silently collapses \
the box. Quote paths (`'/slide[1]'`) and single-quote text containing `$`.
4. Prove it looks right before you deliver: run this server's *_check tool and fix everything it \
reports, then its *_render tool and LOOK at every image; repeat until a pass finds nothing new. \
\"It generated without an error\" is not evidence.
5. Deliver with this server's upload_file tool and put the returned link in your reply — the user \
cannot open files on this machine. Run `officecli save <file>` first (officecli keeps documents in \
memory). Upload every final file; a path in your reply delivers nothing.
6. Finish in one turn: post the outline, keep going without waiting for approval, and end only after \
the link is in your reply. If a check keeps failing after 3 fix rounds, report what failed, what you \
tried and the likely cause.";

const PDF_GUIDE: &str = "\
# PDF

**Making a PDF.** Do not draw PDFs by hand. Write the content as a Word document (follow \
documents_guide) — or as a self-contained HTML page when it is mostly layout — then call pdf_export \
on it. A deck or workbook can be exported the same way. Then call pdf_render and look at the pages: \
the export follows the source's HTML rendering, so fix layout in the source and export again.

**Reading a PDF the user gave you.** pdf_read returns the text with its layout kept (pass `pages` \
for long files). If it comes back empty the PDF is scanned images — use pdf_render and read the \
images instead.

**Checking a PDF.** pdf_render shows up to 8 pages per call (default: the first 4); pass `pages` \
(e.g. \"5-8\") for the rest.";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_schema_tool_belongs_to_a_group() {
        let names = tool_names();
        assert_eq!(names.len(), 13);
        for name in names {
            assert!(OfficeGroup::of_tool(name).is_some(), "{name} has no group");
        }
        assert_eq!(OfficeGroup::of_tool("presentations_render"), Some(OfficeGroup::Presentations));
        assert_eq!(OfficeGroup::of_tool("pdf_read"), Some(OfficeGroup::Pdf));
        assert_eq!(OfficeGroup::of_tool("presentations_upload"), None);
        assert_eq!(OfficeGroup::of_tool("upload_file"), None);
    }

    #[test]
    fn tools_for_filters_by_group() {
        let only_pdf = tools_for(|g| g == OfficeGroup::Pdf);
        let names: Vec<&str> = only_pdf
            .iter()
            .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
            .collect();
        assert_eq!(names, vec!["pdf_guide", "pdf_export", "pdf_read", "pdf_render"]);
        assert!(tools_for(|_| false).is_empty());
        assert_eq!(tools_for(|_| true).len(), 13);
    }

    #[test]
    fn pages_parse_ranges_lists_and_caps() {
        assert_eq!(parse_pages(&json!("1")).unwrap(), vec![1]);
        assert_eq!(parse_pages(&json!("2-4")).unwrap(), vec![2, 3, 4]);
        assert_eq!(parse_pages(&json!("1, 3,3,5")).unwrap(), vec![1, 3, 5]);
        assert_eq!(parse_pages(&json!(3)).unwrap(), vec![3]);
        assert_eq!(parse_pages(&json!([2, 1])).unwrap(), vec![2, 1]);
        assert_eq!(parse_pages(&json!("1-100")).unwrap().len(), MAX_IMAGES);
        assert!(parse_pages(&json!("0")).is_err());
        assert!(parse_pages(&json!("4-2")).is_err());
        assert!(parse_pages(&json!("x")).is_err());
        assert!(parse_pages(&json!(true)).is_err());
    }

    #[test]
    fn span_parses_single_and_range() {
        assert_eq!(parse_span(None).unwrap(), None);
        assert_eq!(parse_span(Some(&json!("3"))).unwrap(), Some((3, 3)));
        assert_eq!(parse_span(Some(&json!("2-5"))).unwrap(), Some((2, 5)));
        assert_eq!(parse_span(Some(&json!(7))).unwrap(), Some((7, 7)));
        assert!(parse_span(Some(&json!("5-2"))).is_err());
        assert!(parse_span(Some(&json!("0"))).is_err());
    }

    #[test]
    fn mentioned_scenes_reads_only_whitelisted_values() {
        let text = "做个PPT\n\n<myclaw_builtin_mention scenes=\"pptx, excel,rm -rf,pptx\">x</myclaw_builtin_mention>";
        assert_eq!(mentioned_scenes(text), vec!["pptx", "excel"]);
        assert!(mentioned_scenes("no block here").is_empty());
        assert!(mentioned_scenes("<myclaw_builtin_mention>x</myclaw_builtin_mention>").is_empty());
        let many = "<myclaw_builtin_mention scenes=\"pptx,word,excel\">";
        assert_eq!(mentioned_scenes(many).len(), MAX_PRELOADED_SCENES);
    }

    #[tokio::test]
    async fn preload_skips_turns_without_a_mention() {
        use crate::acp::types::PromptInputBlock;
        let mut blocks = vec![PromptInputBlock::Text { text: "hello".into() }];
        preload_mentioned_guides(&mut blocks).await;
        assert_eq!(blocks.len(), 1);
    }

    #[test]
    fn total_pages_comes_from_the_warning() {
        let envelope = json!({
            "success": true,
            "warnings": [{ "message": "[pages] total=3", "code": "warning" }]
        });
        assert_eq!(pages_total(&envelope), Some(3));
        assert_eq!(pages_total(&json!({ "success": true })), None);
    }

    #[test]
    fn relative_paths_need_a_session_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            absolutize("deck.pptx", Some(dir.path())).unwrap(),
            dir.path().join("deck.pptx")
        );
        assert!(absolutize("deck.pptx", None).is_err());
        assert!(absolutize("/abs/deck.pptx", None).is_ok());
    }

    #[tokio::test]
    async fn wrong_extension_and_missing_files_are_tool_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("notes.txt"), "x").unwrap();
        let bins = Binaries {
            officecli: Some(PathBuf::from("/bin/true")),
            ..Default::default()
        };
        let res = call_with(
            "presentations_check",
            &json!({ "path": "notes.txt" }),
            Some(dir.path()),
            &bins,
        )
        .await;
        assert_eq!(res["isError"], true);
        assert!(res["content"][0]["text"].as_str().unwrap().contains(".pptx"));

        let res = call_with(
            "documents_render",
            &json!({ "path": "missing.docx" }),
            Some(dir.path()),
            &bins,
        )
        .await;
        assert_eq!(res["isError"], true);
        assert!(res["content"][0]["text"].as_str().unwrap().contains("no such file"));
    }

    #[tokio::test]
    async fn missing_dependencies_are_reported_not_hidden() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.pdf"), "%PDF-1.4").unwrap();
        let none = Binaries::default();
        let res = call_with("pdf_read", &json!({ "path": "a.pdf" }), Some(dir.path()), &none).await;
        assert_eq!(res["isError"], true);
        assert!(res["content"][0]["text"].as_str().unwrap().contains("poppler"));
        let res = call_with("presentations_guide", &json!({}), None, &none).await;
        assert_eq!(res["isError"], true);
        assert!(res["content"][0]["text"].as_str().unwrap().contains("officecli is not installed"));
    }

    #[tokio::test]
    async fn guide_rejects_unknown_scenes_and_appends_platform_rules() {
        let none = Binaries::default();
        let res = call_with("documents_guide", &json!({ "scene": "pptx" }), None, &none).await;
        assert_eq!(res["isError"], true);
        assert!(res["content"][0]["text"].as_str().unwrap().contains("word, academic-paper, word-form"));

        let res = call_with("pdf_guide", &json!({}), None, &none).await;
        let text = res["content"][0]["text"].as_str().unwrap();
        assert!(res.get("isError").is_none());
        assert!(text.contains("pdf_export") && text.contains("Platform rules"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn render_inlines_the_screenshot_officecli_writes() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("deck.pptx"), "pk").unwrap();
        // 假 officecli:`view … -o <out>` 时写一张"图",并按真 officecli 的格式回 JSON 封包。
        let fake = dir.path().join("officecli");
        std::fs::write(
            &fake,
            "#!/bin/sh\n\
             if [ \"$1\" = view ]; then\n\
               while [ $# -gt 0 ]; do if [ \"$1\" = -o ]; then shift; printf PNG > \"$1\"; fi; shift; done\n\
               echo '{\"success\":true,\"warnings\":[{\"message\":\"[pages] total=5\"}]}'\n\
             fi\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let bins = Binaries {
            officecli: Some(fake),
            ..Default::default()
        };
        let res = call_with(
            "presentations_render",
            &json!({ "path": "deck.pptx", "pages": "1-2" }),
            Some(dir.path()),
            &bins,
        )
        .await;
        assert!(res.get("isError").is_none(), "{res}");
        let content = res["content"].as_array().unwrap();
        assert_eq!(content.len(), 3);
        let text = content[0]["text"].as_str().unwrap();
        assert!(text.contains("slides 1, 2") && text.contains("5 slides in total"), "{text}");
        assert_eq!(content[1]["type"], "image");
        assert_eq!(content[1]["data"], base64::engine::general_purpose::STANDARD.encode("PNG"));
        assert!(dir.path().join(".codeg-media/office/deck-p2.png").is_file());
    }
}
