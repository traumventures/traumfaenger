//! Anthropic messages API (direct, or through Langdock's Anthropic endpoint).

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use traumfaenger_api::{Frame, Transport};

use crate::{
    Content, Outcome, Parsed, PartKind, Side, Usage, error_outcome, finish_reason,
    json::{body, number, string, sum, text_or_json},
    looks_like_error,
    openai::Message,
    sse,
};

pub fn parse(transport: Transport, request: Option<&str>, responses: &[&Frame]) -> Result<Parsed, String> {
    let mut parsed = Parsed::default();
    let request = body(request.ok_or("no_request_body")?).ok_or("request_not_json")?;
    parsed.model_requested = string(&request, "model");
    parsed.stream = request.get("stream").and_then(Value::as_bool).unwrap_or(false);
    if let Some(tools) = request.get("tools").filter(|tools| !tools.is_null()) {
        parsed.tools = Some(Content::json(tools));
    }

    let mut names = HashMap::new();
    let mut offset = 0;
    if let Some(system) = request.get("system").filter(|system| !system.is_null()) {
        let mut builder = Message::new(Side::Request, 0, "system");
        blocks(system, &mut builder, &mut names);
        parsed.parts.extend(builder.parts);
        offset = 1;
    }
    let messages = request.get("messages").and_then(Value::as_array).ok_or("no_messages")?;
    for (index, message) in messages.iter().enumerate() {
        let role = if message.get("role").and_then(Value::as_str) == Some("assistant") { "assistant" } else { "user" };
        let mut builder = Message::new(Side::Request, (index + offset) as u32, role);
        if let Some(content) = message.get("content") {
            blocks(content, &mut builder, &mut names);
        }
        parsed.parts.extend(builder.parts);
    }

    if transport == Transport::Sse {
        read_stream(responses, &mut parsed);
        return Ok(parsed);
    }
    let Some(frame) = responses.first() else {
        parsed.notes.push("no_response");
        return Ok(parsed);
    };
    let Some(response) = body(&frame.data) else {
        parsed.notes.push("response_not_json");
        parsed.error = Some(frame.data.trim().to_owned());
        return Ok(parsed);
    };
    if read_error(&response, &mut parsed) {
        return Ok(parsed);
    }
    parsed.finished = true;
    parsed.model = string(&response, "model");
    if let Some(reason) = response.get("stop_reason").and_then(Value::as_str) {
        parsed.finish_reason = Some(finish_reason(reason));
    }
    if let Some(usage) = response.get("usage") {
        read_usage(usage, &mut parsed.usage);
    }
    let mut builder = Message::new(Side::Response, 0, "assistant");
    if let Some(content) = response.get("content") {
        blocks(content, &mut builder, &mut names);
    }
    parsed.parts.extend(builder.parts);
    Ok(parsed)
}

fn read_error(response: &Value, parsed: &mut Parsed) -> bool {
    if response.get("type").and_then(Value::as_str) != Some("error") {
        return false;
    }
    let error = response.get("error").cloned().unwrap_or(Value::Null);
    let kind = error.get("type").and_then(Value::as_str);
    parsed.error_kind = Some(match kind {
        Some("overloaded_error" | "api_error") => Outcome::ProviderError,
        other => error_outcome(None, other),
    });
    parsed.error = Some(string(&error, "message").unwrap_or_else(|| text_or_json(&error)));
    parsed.finish_reason = Some("error");
    true
}

/// Merges usage; streamed `message_delta` events repeat or extend `message_start`.
fn read_usage(usage: &Value, into: &mut Usage) {
    let uncached = number(usage, "input_tokens");
    let cached = number(usage, "cache_read_input_tokens");
    let written = number(usage, "cache_creation_input_tokens");
    if uncached.is_some() || cached.is_some() || written.is_some() {
        into.input = sum(&[uncached, cached, written]);
        into.cached = cached.or(into.cached);
        into.cache_write = written.or(into.cache_write);
    }
    if let Some(output) = number(usage, "output_tokens") {
        into.output = Some(output);
    }
}

fn read_stream(responses: &[&Frame], parsed: &mut Parsed) {
    // Blocks by index: kind, text or JSON, tool id and name.
    let mut open: BTreeMap<u64, (PartKind, String, Option<String>, Option<String>)> = BTreeMap::new();
    for event in sse::events(responses) {
        let Ok(data) = serde_json::from_str::<Value>(&event.data) else {
            parsed.notes.push("stream_event_not_json");
            continue;
        };
        let kind = event.name.or_else(|| data.get("type").and_then(Value::as_str)).unwrap_or("");
        match kind {
            "message_start" => {
                if let Some(message) = data.get("message") {
                    parsed.model = string(message, "model");
                    if let Some(usage) = message.get("usage") {
                        read_usage(usage, &mut parsed.usage);
                    }
                }
            }
            "content_block_start" => {
                let index = number(&data, "index").unwrap_or(open.len() as u64);
                let block = data.get("content_block").cloned().unwrap_or(Value::Null);
                let entry = match block.get("type").and_then(Value::as_str) {
                    Some("text") => (PartKind::Text, string(&block, "text").unwrap_or_default(), None, None),
                    Some("thinking") => {
                        (PartKind::Reasoning, string(&block, "thinking").unwrap_or_default(), None, None)
                    }
                    Some("tool_use" | "server_tool_use") => {
                        if parsed.ttft_ms.is_none() {
                            parsed.ttft_ms = Some(event.t_ms);
                        }
                        (PartKind::ToolCall, String::new(), string(&block, "id"), string(&block, "name"))
                    }
                    _ => (PartKind::Other, block.to_string(), None, None),
                };
                open.insert(index, entry);
            }
            "content_block_delta" => {
                let index = number(&data, "index").unwrap_or(0);
                let delta = data.get("delta").cloned().unwrap_or(Value::Null);
                let piece = string(&delta, "text")
                    .or_else(|| string(&delta, "thinking"))
                    .or_else(|| string(&delta, "partial_json"))
                    .unwrap_or_default();
                if !piece.is_empty() && parsed.ttft_ms.is_none() {
                    parsed.ttft_ms = Some(event.t_ms);
                }
                if let Some(entry) = open.get_mut(&index) {
                    entry.1.push_str(&piece);
                }
            }
            "message_delta" => {
                if let Some(reason) =
                    data.get("delta").and_then(|delta| delta.get("stop_reason")).and_then(Value::as_str)
                {
                    parsed.finish_reason = Some(finish_reason(reason));
                }
                if let Some(usage) = data.get("usage") {
                    read_usage(usage, &mut parsed.usage);
                }
            }
            "message_stop" => parsed.finished = true,
            "error" => {
                read_error(&data, parsed);
            }
            _ => {}
        }
    }
    let mut builder = Message::new(Side::Response, 0, "assistant");
    for (_, (kind, text, id, name)) in open {
        let text = if kind == PartKind::ToolCall && text.is_empty() { "{}".to_owned() } else { text };
        builder.push(kind, text, id, name);
    }
    parsed.parts.extend(builder.parts);
}

/// A string or an array of content blocks.
fn blocks(content: &Value, builder: &mut Message, names: &mut HashMap<String, String>) {
    let items = match content {
        Value::String(text) => return builder.push(PartKind::Text, text.clone(), None, None),
        Value::Array(items) => items,
        other => return builder.push(PartKind::Other, other.to_string(), None, None),
    };
    for block in items {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => builder.push(PartKind::Text, string(block, "text").unwrap_or_default(), None, None),
            Some("thinking") => {
                builder.push(PartKind::Reasoning, string(block, "thinking").unwrap_or_default(), None, None);
            }
            Some("image") => {
                builder.push(PartKind::Image, block.get("source").map(text_or_json).unwrap_or_default(), None, None)
            }
            Some("document") => builder.push(PartKind::File, block.to_string(), None, None),
            Some("tool_use" | "server_tool_use") => {
                let id = string(block, "id");
                let name = string(block, "name");
                if let (Some(id), Some(name)) = (&id, &name) {
                    names.insert(id.clone(), name.clone());
                }
                let input = block.get("input").map(Value::to_string).unwrap_or_else(|| "{}".to_owned());
                builder.push(PartKind::ToolCall, input, id, name);
            }
            Some("tool_result") => {
                let id = string(block, "tool_use_id");
                let name = id.as_ref().and_then(|id| names.get(id).cloned());
                let text = match block.get("content") {
                    Some(Value::Array(items)) => {
                        items.iter().map(|item| string(item, "text").unwrap_or_else(|| item.to_string())).collect()
                    }
                    Some(value) => text_or_json(value),
                    None => String::new(),
                };
                let flagged = block.get("is_error").and_then(Value::as_bool);
                builder.push(PartKind::ToolResult, text, id, name);
                if let Some(part) = builder.parts.last_mut() {
                    part.role = "tool";
                    part.is_error = flagged.unwrap_or_else(|| looks_like_error(&part.content.text));
                }
            }
            _ => builder.push(PartKind::Other, block.to_string(), None, None),
        }
    }
}
