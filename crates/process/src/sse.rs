//! Server-sent events as recorded: one frame per event, stamped on arrival.

use traumfaenger_api::Frame;

pub struct Event<'a> {
    pub t_ms: u64,
    /// The `event:` field, if any.
    pub name: Option<&'a str>,
    /// The `data:` lines joined with newlines.
    pub data: String,
}

/// Every event with data, in order. Comment-only frames (keep-alives such as
/// `: OPENROUTER PROCESSING`) yield nothing. A frame holding several events
/// (blank-line separated) yields each.
pub fn events<'a>(frames: &[&'a Frame]) -> Vec<Event<'a>> {
    let mut out = Vec::new();
    for frame in frames {
        for block in frame.data.split("\n\n") {
            let mut name = None;
            let mut data: Option<String> = None;
            for line in block.lines() {
                let line = line.strip_suffix('\r').unwrap_or(line);
                if line.starts_with(':') || line.is_empty() {
                    continue;
                }
                let (field, value) = line.split_once(':').unwrap_or((line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => name = Some(value),
                    "data" => match &mut data {
                        Some(existing) => {
                            existing.push('\n');
                            existing.push_str(value);
                        }
                        None => data = Some(value.to_owned()),
                    },
                    _ => {}
                }
            }
            if let Some(data) = data {
                out.push(Event { t_ms: frame.t_ms, name, data });
            }
        }
    }
    out
}
