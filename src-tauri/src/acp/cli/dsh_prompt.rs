//! fork(letscubo)专属: argv and prompt text for one `dsh --profile headless`
//! turn. The headless runner takes the task as a positional (or stdin) and
//! only accepts text, so images are written to the workspace and referenced by
//! path. The `UserMessage` echo and the transcript keep the original blocks.

use std::path::{Path, PathBuf};

use base64::Engine as _;
use sha2::{Digest as _, Sha256};

use super::stream_json::CliTurnError;
use crate::acp::types::PromptInputBlock;

/// Upper bound for the flattened prompt; a larger argv fails at spawn with an
/// opaque `E2BIG`, so refuse it here with a code the caller can act on.
const MAX_PROMPT_BYTES: usize = 512 * 1024;
/// Where image blocks land, relative to the turn's working directory.
const ATTACHMENT_DIR: &str = ".codeg/attachments";

/// `--profile headless --patch <patch> --json [--session-id <id>] -- <prompt>`.
/// Launcher flags first (`--profile`, `--patch`), app flags after; `--` keeps a
/// prompt that starts with `-` from being read as an option.
pub(crate) fn build_args(patch_path: &Path, session_id: Option<&str>, prompt: &str) -> Vec<String> {
    let mut args = vec![
        "--profile".to_string(),
        "headless".to_string(),
        "--patch".to_string(),
        patch_path.display().to_string(),
        "--json".to_string(),
    ];
    if let Some(id) = session_id.filter(|s| !s.is_empty()) {
        args.push("--session-id".to_string());
        args.push(id.to_string());
    }
    args.push("--".to_string());
    args.push(prompt.to_string());
    args
}

/// Flatten prompt blocks to one text; image bytes go to disk and become a
/// `[附件: <path>]` line. Empty or oversized prompts are refused.
pub(crate) fn flatten_prompt(
    blocks: &[PromptInputBlock],
    working_dir: &Path,
) -> Result<String, CliTurnError> {
    let mut parts: Vec<String> = Vec::new();
    for block in blocks {
        match block {
            PromptInputBlock::Text { text } => {
                if !text.trim().is_empty() {
                    parts.push(text.clone());
                }
            }
            PromptInputBlock::Image {
                data,
                mime_type,
                uri,
            } => {
                let path = attachment_path(working_dir, data, mime_type, uri.as_deref())?;
                parts.push(format!("[附件: {}]", path.display()));
            }
            PromptInputBlock::Resource {
                uri,
                mime_type,
                text,
                blob,
            } => match (text, blob, mime_type) {
                (Some(text), _, _) => parts.push(format!("[{uri}]\n{text}")),
                (None, Some(blob), Some(mime)) if mime.starts_with("image/") => {
                    let path = attachment_path(working_dir, blob, mime, Some(uri))?;
                    parts.push(format!("[附件: {}]", path.display()));
                }
                _ => parts.push(format!("[{uri}]")),
            },
            PromptInputBlock::ResourceLink { uri, name, .. } => {
                parts.push(format!("[{name}]({uri})"));
            }
        }
    }
    let prompt = parts.join("\n\n");
    if prompt.trim().is_empty() {
        return Err(CliTurnError {
            code: "invalid_params",
            message: "the prompt has no text content".to_string(),
            details: None,
        });
    }
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(CliTurnError {
            code: "prompt_too_large",
            message: format!(
                "the prompt is {} bytes; the DeepSeek Harness CLI takes at most {MAX_PROMPT_BYTES}",
                prompt.len()
            ),
            details: None,
        });
    }
    Ok(prompt)
}

/// An existing `file://` upload is referenced in place; anything else is
/// decoded and written content-addressed under the workspace.
pub(crate) fn attachment_path(
    working_dir: &Path,
    data: &str,
    mime_type: &str,
    uri: Option<&str>,
) -> Result<PathBuf, CliTurnError> {
    if let Some(path) = uri
        .and_then(|u| u.strip_prefix("file://"))
        .map(PathBuf::from)
        .filter(|p| p.is_absolute() && p.is_file())
    {
        return Ok(path);
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.trim())
        .map_err(|e| CliTurnError {
            code: "invalid_params",
            message: format!("image attachment is not valid base64: {e}"),
            details: None,
        })?;
    let digest = Sha256::digest(&bytes);
    let stem: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    let dir = working_dir.join(ATTACHMENT_DIR);
    std::fs::create_dir_all(&dir).map_err(|e| io_error(&dir, e))?;
    let path = dir.join(format!("{stem}.{}", extension_for(mime_type)));
    if !path.is_file() {
        std::fs::write(&path, &bytes).map_err(|e| io_error(&path, e))?;
    }
    Ok(path)
}

fn extension_for(mime_type: &str) -> &'static str {
    match mime_type.to_ascii_lowercase().as_str() {
        "image/png" => "png",
        "image/jpeg" | "image/jpg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/svg+xml" => "svg",
        _ => "bin",
    }
}

fn io_error(path: &Path, e: std::io::Error) -> CliTurnError {
    CliTurnError {
        code: "attachment_write_failed",
        message: format!("could not write the attachment {}: {e}", path.display()),
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_put_launcher_flags_before_app_flags_and_separate_the_prompt() {
        let args = build_args(Path::new("/h/codeg-x.patch.yml"), None, "-hello");
        assert_eq!(
            args,
            vec![
                "--profile",
                "headless",
                "--patch",
                "/h/codeg-x.patch.yml",
                "--json",
                "--",
                "-hello"
            ]
        );
        let args = build_args(Path::new("/h/p.yml"), Some("session-1"), "hi");
        assert_eq!(&args[5..], &["--session-id", "session-1", "--", "hi"]);
    }

    #[test]
    fn flatten_joins_text_and_writes_images_to_the_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let png = base64::engine::general_purpose::STANDARD.encode(b"\x89PNG fake");
        let blocks = vec![
            PromptInputBlock::Text {
                text: "look".into(),
            },
            PromptInputBlock::Image {
                data: png,
                mime_type: "image/png".into(),
                uri: None,
            },
            PromptInputBlock::ResourceLink {
                uri: "https://x".into(),
                name: "x".into(),
                mime_type: None,
                description: None,
            },
        ];
        let prompt = flatten_prompt(&blocks, tmp.path()).unwrap();
        let lines: Vec<&str> = prompt.split("\n\n").collect();
        assert_eq!(lines[0], "look");
        assert!(lines[1].starts_with("[附件: "));
        assert!(lines[1].ends_with(".png]"));
        assert_eq!(lines[2], "[x](https://x)");
        let path = lines[1].trim_start_matches("[附件: ").trim_end_matches(']');
        assert_eq!(std::fs::read(path).unwrap(), b"\x89PNG fake");
        // idempotent: same bytes, same file
        assert_eq!(flatten_prompt(&blocks, tmp.path()).unwrap(), prompt);
    }

    #[test]
    fn existing_file_uploads_are_referenced_in_place() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("up.png");
        std::fs::write(&file, b"x").unwrap();
        let blocks = vec![PromptInputBlock::Image {
            data: "ignored".into(),
            mime_type: "image/png".into(),
            uri: Some(format!("file://{}", file.display())),
        }];
        let prompt = flatten_prompt(&blocks, tmp.path()).unwrap();
        assert_eq!(prompt, format!("[附件: {}]", file.display()));
    }

    #[test]
    fn empty_and_oversized_prompts_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let err = flatten_prompt(&[PromptInputBlock::Text { text: "  ".into() }], tmp.path())
            .unwrap_err();
        assert_eq!(err.code, "invalid_params");
        let big = "x".repeat(MAX_PROMPT_BYTES + 1);
        let err = flatten_prompt(&[PromptInputBlock::Text { text: big }], tmp.path()).unwrap_err();
        assert_eq!(err.code, "prompt_too_large");
    }
}
