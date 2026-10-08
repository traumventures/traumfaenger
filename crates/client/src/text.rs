//! Text preparation shared by the recorders: everything a frame or a context
//! field goes through before it leaves the process. Mirrors the TypeScript
//! client so both record the same way.

use std::{collections::BTreeMap, sync::LazyLock};

use regex::Regex;
use traumfaenger_api::limits;

/// Clips `text` to at most `max` bytes on a character boundary. Returns
/// whether anything was cut.
pub fn clip(text: &mut String, max: usize) -> bool {
    if text.len() <= max {
        return false;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    true
}

/// Inline base64 data of at least 1 KiB, as in `data:image/png;base64,...`.
static DATA_URL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"data:([\w.+-]+/[\w.+-]+);base64,([A-Za-z0-9+/]{1024,}={0,2})").unwrap());
/// A JSON string that is nothing but base64 of at least 1 KiB, as realtime audio chunks are.
static BASE64_STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""([A-Za-z0-9+/]{1024,}={0,2})""#).unwrap());

fn decoded_bytes(base64_len: usize) -> usize {
    base64_len * 3 / 4
}

/// Prepares frame data: inline base64 payloads (images, files, audio) become
/// a short placeholder. Everything else stays byte for byte.
pub fn prepare_data(text: &str) -> String {
    let mut prepared = text.to_owned();
    if prepared.contains(";base64,") {
        prepared = DATA_URL
            .replace_all(&prepared, |caps: &regex::Captures<'_>| {
                format!("[traumfaenger: omitted {} data, {} bytes]", &caps[1], decoded_bytes(caps[2].len()))
            })
            .into_owned();
    }
    if prepared.len() >= 1_026 {
        prepared = BASE64_STRING
            .replace_all(&prepared, |caps: &regex::Captures<'_>| {
                format!("\"[traumfaenger: omitted base64, {} bytes]\"", decoded_bytes(caps[1].len()))
            })
            .into_owned();
    }
    prepared
}

/// Placeholder for binary data that is not recorded.
pub fn binary_placeholder(kind: &str, bytes: Option<usize>) -> String {
    match bytes {
        Some(bytes) => format!("[traumfaenger: unrecorded {kind}, {bytes} bytes]"),
        None => format!("[traumfaenger: unrecorded {kind}]"),
    }
}

/// Decodes a body as text, or a placeholder when it is not text.
pub fn body_text(bytes: &[u8], content_type: Option<&str>) -> String {
    let content_type = content_type.unwrap_or("").to_ascii_lowercase();
    let textual = content_type.is_empty()
        || content_type.starts_with("text/")
        || content_type.contains("json")
        || content_type.contains("xml");
    if textual || (content_type.is_empty() && std::str::from_utf8(bytes).is_ok()) {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        binary_placeholder(&content_type, Some(bytes.len()))
    }
}

/// A single line of at most `MAX_NAME_LEN` bytes. `None` when empty; the flag
/// says whether anything had to change.
pub fn clean_line(value: &str) -> (Option<String>, bool) {
    let mut line: String = value.chars().map(|c| if c.is_control() { ' ' } else { c }).collect();
    line = line.trim().to_owned();
    if line.is_empty() {
        return (None, !value.is_empty());
    }
    let clipped = clip(&mut line, limits::MAX_NAME_LEN);
    let line = line.trim_end().to_owned();
    let changed = clipped || line != value;
    (Some(line), changed)
}

fn is_metadata_key(key: &str) -> bool {
    (1..=limits::MAX_METADATA_KEY_LEN).contains(&key.len())
        && key.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

/// Drops invalid keys and clips values to the limits. The flag says whether
/// anything had to change.
pub fn clean_metadata(metadata: &BTreeMap<String, String>) -> (BTreeMap<String, String>, bool) {
    let mut out = BTreeMap::new();
    let mut changed = false;
    for (key, value) in metadata {
        if !is_metadata_key(key) || out.len() >= limits::MAX_METADATA_ENTRIES {
            changed = true;
            continue;
        }
        let mut value = value.replace('\0', "");
        changed |= value.len() != metadata[key].len();
        changed |= clip(&mut value, limits::MAX_METADATA_VALUE_LEN);
        out.insert(key.clone(), value);
    }
    (out, changed)
}

/// Absolute http(s)/ws(s) URL without query string, fragment and
/// credentials, which can carry keys.
pub fn clean_url(raw: &str) -> Option<String> {
    let mut url = url::Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "https" | "http" | "wss" | "ws") {
        return None;
    }
    url.set_query(None);
    url.set_fragment(None);
    let _ = url.set_username("");
    let _ = url.set_password(None);
    let mut text = url.to_string();
    clip(&mut text, limits::MAX_URL_LEN);
    Some(text)
}

/// An error message within the limits.
pub fn describe(error: &str) -> String {
    let mut text = error.trim().to_owned();
    if text.is_empty() {
        text = "unknown error".into();
    }
    clip(&mut text, limits::MAX_ERROR_LEN);
    text
}

/// Splits a stream of SSE text into events as they complete. Only new text
/// is scanned, so one large event that arrives in many chunks costs linear time.
#[derive(Default)]
pub struct SseSplitter {
    buffer: String,
    held_cr: bool,
    undecoded: Vec<u8>,
}

impl SseSplitter {
    /// Feeds raw bytes (UTF-8 split across chunks is reassembled) and returns
    /// the events they complete.
    pub fn push_bytes(&mut self, chunk: &[u8]) -> Vec<String> {
        self.undecoded.extend_from_slice(chunk);
        let valid = match std::str::from_utf8(&self.undecoded) {
            Ok(_) => self.undecoded.len(),
            Err(error) if error.error_len().is_none() => error.valid_up_to(),
            Err(_) => self.undecoded.len(),
        };
        let rest = self.undecoded.split_off(valid);
        let text = String::from_utf8_lossy(&self.undecoded).into_owned();
        self.undecoded = rest;
        self.push(&text)
    }

    /// Feeds text and returns the events it completes.
    pub fn push(&mut self, chunk: &str) -> Vec<String> {
        let mut text = String::with_capacity(chunk.len() + 1);
        if self.held_cr {
            text.push('\r');
        }
        text.push_str(chunk);
        self.held_cr = text.ends_with('\r');
        if self.held_cr {
            text.pop();
        }
        // A boundary can start in the last character of the old buffer.
        let mut from = self.buffer.len().saturating_sub(1);
        while !self.buffer.is_char_boundary(from) {
            from -= 1;
        }
        self.buffer.push_str(&text.replace("\r\n", "\n").replace('\r', "\n"));
        let mut events = Vec::new();
        let mut start = 0;
        while let Some(offset) = self.buffer[from..].find("\n\n") {
            let boundary = from + offset;
            emit(&self.buffer[start..boundary], &mut events);
            start = boundary + 2;
            from = start;
        }
        if start > 0 {
            self.buffer.drain(..start);
        }
        events
    }

    /// Returns the incomplete last event, if any.
    pub fn end(&mut self) -> Vec<String> {
        if !self.undecoded.is_empty() {
            let rest = String::from_utf8_lossy(&std::mem::take(&mut self.undecoded)).into_owned();
            self.buffer.push_str(&rest);
        }
        let mut events = Vec::new();
        emit(&std::mem::take(&mut self.buffer), &mut events);
        self.held_cr = false;
        events
    }
}

fn emit(event: &str, events: &mut Vec<String>) {
    let trimmed = event.trim_matches('\n');
    if !trimmed.trim().is_empty() {
        events.push(trimmed.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_events_across_chunks_and_utf8_boundaries() {
        let mut splitter = SseSplitter::default();
        assert_eq!(splitter.push("data: 1\r"), Vec::<String>::new());
        assert_eq!(splitter.push("\n\r"), Vec::<String>::new());
        assert_eq!(splitter.push("\ndata: 2"), ["data: 1"]);
        assert_eq!(splitter.end(), ["data: 2"]);

        // A chunk that ends in a multi-byte character.
        let mut splitter = SseSplitter::default();
        assert!(splitter.push_bytes("data: {\"c\":\"Grüß".as_bytes()).is_empty());
        assert_eq!(splitter.push_bytes(b"\"}\n\n"), ["data: {\"c\":\"Grüß\"}"]);
        assert!(splitter.push("data: ä").is_empty());
        assert_eq!(splitter.push("\n\n"), ["data: ä"]);

        let mut splitter = SseSplitter::default();
        let bytes = "data: ä\n\n".as_bytes();
        assert!(splitter.push_bytes(&bytes[..7]).is_empty());
        assert_eq!(splitter.push_bytes(&bytes[7..]), ["data: ä"]);
    }

    #[test]
    fn replaces_inline_base64_and_keeps_the_rest() {
        let image = format!("{{\"url\":\"data:image/png;base64,{}\"}}", "A".repeat(4000));
        assert_eq!(prepare_data(&image), "{\"url\":\"[traumfaenger: omitted image/png data, 3000 bytes]\"}");
        let audio = format!("{{\"audio\":\"{}\"}}", "B".repeat(1024));
        assert_eq!(prepare_data(&audio), "{\"audio\":\"[traumfaenger: omitted base64, 768 bytes]\"}");
        let short = format!("{{\"id\":\"{}\"}}", "C".repeat(1023));
        assert_eq!(prepare_data(&short), short);
    }

    #[test]
    fn cleans_context() {
        assert_eq!(clean_line("a\nb"), (Some("a b".into()), true));
        assert_eq!(clean_line("ok"), (Some("ok".into()), false));
        assert_eq!(clean_line(""), (None, false));
        let (metadata, changed) = clean_metadata(&BTreeMap::from([
            ("ok".into(), "v".into()),
            ("bad key".into(), "x".into()),
            ("long".into(), "ä".repeat(400)),
        ]));
        assert!(changed && !metadata.contains_key("bad key"));
        assert!(metadata["long"].len() <= limits::MAX_METADATA_VALUE_LEN);
        assert_eq!(clean_url("https://u:p@x.example/v1?key=s#f").as_deref(), Some("https://x.example/v1"));
        assert_eq!(clean_url("ftp://x"), None);
        let mut text = "äää".to_owned();
        assert!(clip(&mut text, 5) && text == "ää");
    }
}
