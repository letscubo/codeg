//! Compact receipts keep one body and reference UTF-16 ranges, matching JS slice.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub enum ResultFormat {
    #[default]
    #[serde(rename = "legacy")]
    Legacy,
    #[serde(rename = "compact-v1")]
    CompactV1,
}

const INVALID: &str = "invalid relay text ranges";

/// Compact legacy rows without changing segment identity or display text.
/// Telegram may omit whitespace-only chunks and separate its terminal note
/// with newlines that are absent from the note's own message.
pub(crate) fn compact(mut value: Value) -> Result<Value, String> {
    if value["result_format"] == "compact-v1" {
        ranges(&value)?;
        return Ok(value);
    }
    let Some(segments) = value["segments"].as_array() else {
        return Ok(value); // Older non-receipt metadata remains readable.
    };
    let full = value["final_text"].as_str().ok_or(INVALID)?;
    let mut byte_cursor = 0;
    let mut unit_cursor = 0;
    let mut offsets = Vec::with_capacity(segments.len());
    for segment in segments {
        let text = segment["text"].as_str().ok_or(INVALID)?;
        let remaining = &full[byte_cursor..];
        let skipped = remaining.find(text).ok_or(INVALID)?;
        let gap = &remaining[..skipped];
        if !gap.trim().is_empty() {
            return Err(INVALID.into());
        }
        let start = unit_cursor + gap.encode_utf16().count();
        let end = start + text.encode_utf16().count();
        offsets.push((start, end));
        byte_cursor += skipped + text.len();
        unit_cursor = end;
    }
    if !full[byte_cursor..].trim().is_empty() {
        return Err(INVALID.into());
    }
    for (segment, (start, end)) in value["segments"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(offsets)
    {
        let segment = segment.as_object_mut().ok_or(INVALID)?;
        segment.remove("text");
        segment.insert("text_start".into(), start.into());
        segment.insert("text_end".into(), end.into());
    }
    value["result_format"] = "compact-v1".into();
    Ok(value)
}

/// Return byte offsets while validating order, whitespace gaps and Unicode
/// boundaries in one pass. A range must never split a surrogate pair.
fn ranges(value: &Value) -> Result<Vec<(usize, usize)>, String> {
    let full = value["final_text"].as_str().ok_or(INVALID)?;
    let segments = value["segments"].as_array().ok_or(INVALID)?;
    let mut byte_cursor = 0;
    let mut unit_cursor = 0;
    let mut offsets = Vec::with_capacity(segments.len());
    for segment in segments {
        if segment.get("text").is_some() {
            return Err(INVALID.into());
        }
        let start = segment["text_start"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or(INVALID)?;
        let end = segment["text_end"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or(INVALID)?;
        if start < unit_cursor || end < start {
            return Err(INVALID.into());
        }
        let start_byte = byte_cursor + byte_offset(&full[byte_cursor..], start - unit_cursor)?;
        if !full[byte_cursor..start_byte].trim().is_empty() {
            return Err(INVALID.into());
        }
        let end_byte = start_byte + byte_offset(&full[start_byte..], end - start)?;
        offsets.push((start_byte, end_byte));
        byte_cursor = end_byte;
        unit_cursor = end;
    }
    if !full[byte_cursor..].trim().is_empty() {
        return Err(INVALID.into());
    }
    Ok(offsets)
}

fn byte_offset(text: &str, units: usize) -> Result<usize, String> {
    let mut remaining = units;
    for (offset, ch) in text.char_indices() {
        if remaining == 0 {
            return Ok(offset);
        }
        remaining = remaining.checked_sub(ch.len_utf16()).ok_or(INVALID)?;
    }
    if remaining == 0 {
        Ok(text.len())
    } else {
        Err(INVALID.into())
    }
}

pub(crate) fn for_wire(mut value: Value, format: ResultFormat) -> Result<Value, String> {
    match format {
        ResultFormat::CompactV1 => compact(value),
        ResultFormat::Legacy if value["result_format"] == "compact-v1" => {
            let offsets = ranges(&value)?;
            let full = value["final_text"].as_str().unwrap();
            let texts: Vec<_> = offsets
                .into_iter()
                .map(|(start, end)| full[start..end].to_owned())
                .collect();
            for (segment, text) in value["segments"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .zip(texts)
            {
                let segment = segment.as_object_mut().ok_or(INVALID)?;
                segment.remove("text_start");
                segment.remove("text_end");
                segment.insert("text".into(), text.into());
            }
            value.as_object_mut().unwrap().remove("result_format");
            Ok(value)
        }
        ResultFormat::Legacy => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn telegram_whitespace_gaps_and_terminal_note_round_trip_without_duplicate_body() {
        let original = json!({"final_text":"   🦀回答\n\n⏹ Stopped.","segments":[
            {"index":0,"text":"🦀回答","message_id":7,"delivered":true},
            {"index":1,"text":"⏹ Stopped.","delivered":false}
        ]});
        let compact = compact(original.clone()).unwrap();
        assert_eq!(compact["result_format"], "compact-v1");
        assert_eq!(compact["segments"][0]["text_start"], 3);
        assert_eq!(compact["segments"][0]["text_end"], 7);
        assert_eq!(compact["segments"][1]["text_start"], 9);
        assert!(compact["segments"][0].get("text").is_none());
        assert_eq!(for_wire(compact, ResultFormat::Legacy).unwrap(), original);
    }

    #[test]
    fn empty_tool_segments_and_repeated_text_preserve_identity() {
        let original = json!({"final_text":"🦀🦀", "segments":[
            {"index":0,"text":"","message_ts":"1.0"},
            {"index":1,"text":"🦀","message_ts":"2.0"},
            {"index":2,"text":"🦀","message_ts":"3.0"}
        ]});
        let compact = compact(original.clone()).unwrap();
        assert_eq!(compact["segments"][0]["text_end"], 0);
        assert_eq!(compact["segments"][2]["text_start"], 2);
        assert_eq!(compact["segments"][2]["text_end"], 4);
        assert_eq!(for_wire(compact, ResultFormat::Legacy).unwrap(), original);
    }

    #[test]
    fn rejects_missing_content_overlaps_and_split_surrogates() {
        for value in [
            json!({"final_text":"lost reply","segments":[{"text":"reply"}]}),
            json!({"result_format":"compact-v1","final_text":"🦀", "segments":[{"text_start":0,"text_end":1}]}),
            json!({"result_format":"compact-v1","final_text":"abc", "segments":[{"text_start":0,"text_end":2},{"text_start":1,"text_end":3}]}),
            json!({"result_format":"compact-v1","final_text":"abc", "segments":[{"text_start":0,"text_end":2}]}),
        ] {
            assert!(compact(value).is_err());
        }
    }

    #[test]
    fn legacy_wire_is_unchanged_and_empty_running_marker_compacts() {
        let old = json!({"final_text":"reply","segments":[{"text":"reply"}]});
        assert_eq!(for_wire(old.clone(), ResultFormat::Legacy).unwrap(), old);
        let running =
            compact(json!({"delivery_status":"running","final_text":"","segments":[]})).unwrap();
        assert_eq!(running["result_format"], "compact-v1");
    }
}
