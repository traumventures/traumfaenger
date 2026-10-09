//! OpenAI-style APIs: chat completions (OpenRouter, OpenAI, compatible
//! gateways), audio transcriptions and embeddings.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use traumfaenger_api::{Frame, Transport};

use crate::{
    Api, Content, Parsed, Part, PartKind, Side, Usage, error_outcome, finish_reason,
    json::{body, number, path_number, string, text_or_json},
    looks_like_error, sse,
};

pub fn parse(api: Api, transport: Transport, request: Option<&str>, responses: &[&Frame]) -> Result<Parsed, String> {
    let mut parsed = Parsed::default();
    let request = request.ok_or("no_request_body")?;
    let Some(request) = body(request) else {
        // Transcriptions may be multipart; the response can still be read.
        if api != Api::Transcription {
            return Err("request_not_json".into());
        }
        parsed.notes.push("request_not_json");
        read_response(api, transport, responses, &mut parsed)?;
        return Ok(parsed);
    };
    parsed.model_requested = string(&request, "model");
    parsed.stream = request.get("stream").and_then(Value::as_bool).unwrap_or(false);
    match api {
        Api::OpenAiChat => {
            if let Some(tools) = request.get("tools").filter(|tools| !tools.is_null()) {
                parsed.tools = Some(Content::json(tools));
            }
            let messages = request.get("messages").and_then(Value::as_array).ok_or("no_messages")?;
            request_messages(messages, &mut parsed);
        }
        Api::Transcription => {
            let audio = request.get("input_audio").or_else(|| request.get("file")).cloned().unwrap_or(Value::Null);
            parsed.parts.push(part(Side::Request, 0, 0, "user", PartKind::Audio, Content::json(&audio)));
        }
        Api::Embeddings => {
            let inputs = match request.get("input") {
                Some(Value::Array(items)) => items.iter().map(text_or_json).collect(),
                Some(other) => vec![text_or_json(other)],
                None => Vec::new(),
            };
            for (index, input) in inputs.into_iter().enumerate() {
                parsed.parts.push(part(Side::Request, 0, index as u32, "user", PartKind::Text, Content::new(input)));
            }
        }
        _ => unreachable!("not an OpenAI-style API"),
    }
    read_response(api, transport, responses, &mut parsed)?;
    Ok(parsed)
}

fn read_response(api: Api, transport: Transport, responses: &[&Frame], parsed: &mut Parsed) -> Result<(), String> {
    if transport == Transport::Sse {
        return read_stream(responses, parsed);
    }
    let Some(frame) = responses.first() else {
        parsed.notes.push("no_response");
        return Ok(());
    };
    let Some(response) = body(&frame.data) else {
        if frame.data.trim().is_empty() {
            parsed.notes.push("empty_response");
        } else {
            parsed.notes.push("response_not_json");
            parsed.error = Some(frame.data.trim().to_owned());
        }
        return Ok(());
    };
    if read_error(&response, parsed) {
        return Ok(());
    }
    parsed.finished = true;
    parsed.model = string(&response, "model");
    parsed.provider = string(&response, "provider");
    if let Some(usage) = response.get("usage") {
        read_usage(usage, parsed);
    }
    match api {
        Api::OpenAiChat => {
            let choices = response.get("choices").and_then(Value::as_array).ok_or("no_choices")?;
            for (position, choice) in choices.iter().enumerate() {
                let message_index = number(choice, "index").unwrap_or(position as u64) as u32;
                if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                    parsed.finish_reason = Some(finish_reason(reason));
                }
                choice_error(choice, parsed);
                if let Some(message) = choice.get("message") {
                    let mut builder = Message::new(Side::Response, message_index, "assistant");
                    if let Some(reasoning) = message.get("reasoning").and_then(Value::as_str) {
                        builder.push(PartKind::Reasoning, reasoning.to_owned(), None, None);
                    }
                    content_parts(message.get("content"), &mut builder);
                    for call in message.get("tool_calls").and_then(Value::as_array).into_iter().flatten() {
                        builder.tool_call(call);
                    }
                    parsed.parts.extend(builder.parts);
                }
            }
        }
        Api::Transcription => {
            if let Some(text) = string(&response, "text") {
                parsed.parts.push(part(Side::Response, 0, 0, "assistant", PartKind::Text, Content::new(text)));
            }
        }
        // Vectors are not worth keeping.
        _ => {}
    }
    Ok(())
}

/// `{"error": {...}}` bodies. Returns whether the body was an error.
fn read_error(response: &Value, parsed: &mut Parsed) -> bool {
    let Some(error) = response.get("error").filter(|error| !error.is_null()) else { return false };
    let code = error.get("code").and_then(|code| code.as_i64().or_else(|| code.as_str()?.parse().ok()));
    let kind = error.get("type").and_then(Value::as_str);
    parsed.error_kind = Some(error_outcome(code, kind));
    let message = string(error, "message").unwrap_or_else(|| text_or_json(error));
    // OpenRouter puts the upstream message into metadata.raw.
    let raw = error.get("metadata").and_then(|metadata| metadata.get("raw")).map(text_or_json);
    parsed.error = Some(match raw {
        Some(raw) if !raw.is_empty() => format!("{message}: {raw}"),
        _ => message,
    });
    if let Some(provider) = error.get("metadata").and_then(|metadata| string(metadata, "provider_name")) {
        parsed.provider = Some(provider);
    }
    parsed.finish_reason = Some("error");
    true
}

/// OpenRouter reports some upstream failures inside a 200, on the choice:
/// `finish_reason: "error"` with `choices[].error`.
fn choice_error(choice: &Value, parsed: &mut Parsed) {
    let Some(error) = choice.get("error").filter(|error| !error.is_null()) else { return };
    let code = error.get("code").and_then(|code| code.as_i64().or_else(|| code.as_str()?.parse().ok()));
    parsed.error_kind = Some(error_outcome(code, error.get("type").and_then(Value::as_str)));
    parsed.error = Some(string(error, "message").unwrap_or_else(|| text_or_json(error)));
    parsed.finish_reason = Some("error");
}

fn read_usage(usage: &Value, parsed: &mut Parsed) {
    let input = number(usage, "prompt_tokens").or_else(|| number(usage, "input_tokens"));
    parsed.usage = Usage {
        input,
        cached: path_number(usage, &["prompt_tokens_details", "cached_tokens"])
            .or_else(|| path_number(usage, &["input_tokens_details", "cached_tokens"])),
        cache_write: path_number(usage, &["prompt_tokens_details", "cache_write_tokens"]),
        output: number(usage, "completion_tokens").or_else(|| number(usage, "output_tokens")),
        reasoning: path_number(usage, &["completion_tokens_details", "reasoning_tokens"])
            .or_else(|| path_number(usage, &["output_tokens_details", "reasoning_tokens"])),
    };
    if let Some(cost) = usage.get("cost").and_then(Value::as_f64) {
        // With a provider key of our own (BYOK) OpenRouter charges only its
        // fee; the inference itself is billed upstream.
        let upstream = if usage.get("is_byok").and_then(Value::as_bool) == Some(true) {
            usage.get("cost_details").and_then(|details| details.get("upstream_inference_cost")).and_then(Value::as_f64)
        } else {
            None
        };
        parsed.cost_usd = Some(cost + upstream.unwrap_or(0.0));
    }
}

fn read_stream(responses: &[&Frame], parsed: &mut Parsed) -> Result<(), String> {
    #[derive(Default)]
    struct Choice {
        reasoning: String,
        content: String,
        tools: BTreeMap<u64, (Option<String>, String, String)>,
    }
    let mut choices: BTreeMap<u64, Choice> = BTreeMap::new();
    for event in sse::events(responses) {
        if event.data.trim() == "[DONE]" {
            parsed.finished = true;
            continue;
        }
        let Ok(chunk) = serde_json::from_str::<Value>(&event.data) else {
            parsed.notes.push("stream_event_not_json");
            continue;
        };
        if read_error(&chunk, parsed) {
            continue;
        }
        if parsed.model.is_none() {
            parsed.model = string(&chunk, "model");
        }
        if parsed.provider.is_none() {
            parsed.provider = string(&chunk, "provider");
        }
        if let Some(usage) = chunk.get("usage").filter(|usage| !usage.is_null()) {
            read_usage(usage, parsed);
        }
        for (position, choice) in chunk.get("choices").and_then(Value::as_array).into_iter().flatten().enumerate() {
            let index = number(choice, "index").unwrap_or(position as u64);
            let state = choices.entry(index).or_default();
            if let Some(delta) = choice.get("delta") {
                let mut produced = false;
                if let Some(text) = delta.get("reasoning").and_then(Value::as_str).filter(|text| !text.is_empty()) {
                    state.reasoning.push_str(text);
                    produced = true;
                }
                if let Some(text) = delta.get("content").and_then(Value::as_str).filter(|text| !text.is_empty()) {
                    state.content.push_str(text);
                    produced = true;
                }
                for call in delta.get("tool_calls").and_then(Value::as_array).into_iter().flatten() {
                    let slot = number(call, "index").unwrap_or(state.tools.len() as u64);
                    let entry = state.tools.entry(slot).or_default();
                    if let Some(id) = string(call, "id") {
                        entry.0 = Some(id);
                    }
                    if let Some(function) = call.get("function") {
                        entry.1.push_str(function.get("name").and_then(Value::as_str).unwrap_or(""));
                        entry.2.push_str(function.get("arguments").and_then(Value::as_str).unwrap_or(""));
                    }
                    produced = true;
                }
                if produced && parsed.ttft_ms.is_none() {
                    parsed.ttft_ms = Some(event.t_ms);
                }
            }
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                parsed.finish_reason = Some(finish_reason(reason));
                parsed.finished = true;
            }
            choice_error(choice, parsed);
        }
    }
    for (index, choice) in choices {
        let mut builder = Message::new(Side::Response, index as u32, "assistant");
        if !choice.reasoning.is_empty() {
            builder.push(PartKind::Reasoning, choice.reasoning, None, None);
        }
        if !choice.content.is_empty() {
            builder.push(PartKind::Text, choice.content, None, None);
        }
        for (_, (id, name, arguments)) in choice.tools {
            builder.push(PartKind::ToolCall, arguments, id, Some(name));
        }
        parsed.parts.extend(builder.parts);
    }
    Ok(())
}

/// Request messages. Tool results without a name get the name of the call they answer.
fn request_messages(messages: &[Value], parsed: &mut Parsed) {
    let mut names = HashMap::new();
    for (index, message) in messages.iter().enumerate() {
        let role = match message.get("role").and_then(Value::as_str).unwrap_or("") {
            "system" | "developer" => "system",
            "assistant" => "assistant",
            "tool" | "function" => "tool",
            _ => "user",
        };
        let mut builder = Message::new(Side::Request, index as u32, role);
        if role == "tool" {
            let id = string(message, "tool_call_id");
            let name = string(message, "name").or_else(|| id.as_ref().and_then(|id| names.get(id).cloned()));
            let text = match message.get("content") {
                Some(Value::Array(items)) => {
                    items.iter().map(|item| string(item, "text").unwrap_or_else(|| item.to_string())).collect()
                }
                Some(value) => text_or_json(value),
                None => String::new(),
            };
            builder.push(PartKind::ToolResult, text, id, name);
        } else {
            if role == "assistant"
                && let Some(reasoning) = message.get("reasoning").and_then(Value::as_str)
            {
                builder.push(PartKind::Reasoning, reasoning.to_owned(), None, None);
            }
            content_parts(message.get("content"), &mut builder);
            for call in message.get("tool_calls").and_then(Value::as_array).into_iter().flatten() {
                if let (Some(id), Some(name)) =
                    (string(call, "id"), call.get("function").and_then(|function| string(function, "name")))
                {
                    names.insert(id, name);
                }
                builder.tool_call(call);
            }
        }
        parsed.parts.extend(builder.parts);
    }
}

fn content_parts(content: Option<&Value>, builder: &mut Message) {
    match content {
        Some(Value::String(text)) => builder.push(PartKind::Text, text.clone(), None, None),
        Some(Value::Array(items)) => {
            for item in items {
                match item.get("type").and_then(Value::as_str) {
                    Some("text" | "input_text" | "output_text") => {
                        builder.push(PartKind::Text, string(item, "text").unwrap_or_default(), None, None);
                    }
                    Some("image_url" | "input_image") => {
                        let url = item.get("image_url").map(|image| image.get("url").unwrap_or(image));
                        builder.push(PartKind::Image, url.map(text_or_json).unwrap_or_default(), None, None);
                    }
                    Some("input_audio") => builder.push(PartKind::Audio, item.to_string(), None, None),
                    Some("file" | "input_file") => builder.push(PartKind::File, item.to_string(), None, None),
                    _ => builder.push(PartKind::Other, item.to_string(), None, None),
                }
            }
        }
        Some(Value::Null) | None => {}
        Some(other) => builder.push(PartKind::Other, other.to_string(), None, None),
    }
}

pub(crate) fn part(side: Side, message: u32, index: u32, role: &'static str, kind: PartKind, content: Content) -> Part {
    Part { side, message, index, role, kind, content, tool_call_id: None, tool_name: None, is_error: false }
}

/// Collects the parts of one message, numbering them and skipping empty text.
pub(crate) struct Message {
    side: Side,
    message: u32,
    role: &'static str,
    pub parts: Vec<Part>,
}

impl Message {
    pub fn new(side: Side, message: u32, role: &'static str) -> Self {
        Self { side, message, role, parts: Vec::new() }
    }

    pub fn push(&mut self, kind: PartKind, text: String, tool_call_id: Option<String>, tool_name: Option<String>) {
        if text.is_empty() && matches!(kind, PartKind::Text | PartKind::Reasoning) {
            return;
        }
        let is_error = kind == PartKind::ToolResult && looks_like_error(&text);
        self.parts.push(Part {
            side: self.side,
            message: self.message,
            index: self.parts.len() as u32,
            role: self.role,
            kind,
            content: Content::new(text),
            tool_call_id,
            tool_name,
            is_error,
        });
    }

    /// An OpenAI `tool_calls` entry.
    pub fn tool_call(&mut self, call: &Value) {
        let function = call.get("function");
        let name = function.and_then(|function| string(function, "name"));
        let arguments = function.and_then(|function| function.get("arguments")).map(text_or_json).unwrap_or_default();
        self.push(PartKind::ToolCall, arguments, string(call, "id"), name);
    }
}
