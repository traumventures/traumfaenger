//! Gemini `generateContent` / `streamGenerateContent` (direct, or through
//! Langdock's Google endpoint).

use serde_json::Value;
use traumfaenger_api::{Frame, Transport};

use crate::{
    Content, Parsed, PartKind, Side, error_outcome, finish_reason,
    json::{body, number, string, sum},
    looks_like_error,
    openai::Message,
    sse,
};

pub fn parse(transport: Transport, path: &str, request: Option<&str>, responses: &[&Frame]) -> Result<Parsed, String> {
    let mut parsed = Parsed::default();
    // .../models/<model>:<action>
    let target = path.rsplit('/').next().unwrap_or("");
    let (model, action) = target.split_once(':').unwrap_or((target, ""));
    parsed.model_requested = Some(model.to_owned()).filter(|model| !model.is_empty());
    parsed.stream = action == "streamGenerateContent";

    let request = body(request.ok_or("no_request_body")?).ok_or("request_not_json")?;
    if let Some(tools) = request.get("tools").filter(|tools| !tools.is_null()) {
        parsed.tools = Some(Content::json(tools));
    }
    let mut offset = 0;
    if let Some(system) = request.get("systemInstruction").or_else(|| request.get("system_instruction")) {
        let mut builder = Message::new(Side::Request, 0, "system");
        parts(system, &mut builder);
        parsed.parts.extend(builder.parts);
        offset = 1;
    }
    let contents = request.get("contents").and_then(Value::as_array).ok_or("no_contents")?;
    for (index, content) in contents.iter().enumerate() {
        let role = if content.get("role").and_then(Value::as_str) == Some("model") { "assistant" } else { "user" };
        let mut builder = Message::new(Side::Request, (index + offset) as u32, role);
        parts(content, &mut builder);
        parsed.parts.extend(builder.parts);
    }

    // Response chunks: SSE events, a JSON array (streaming without `alt=sse`) or one object.
    let mut chunks: Vec<(u64, Value)> = Vec::new();
    if transport == Transport::Sse {
        for event in sse::events(responses) {
            match serde_json::from_str(&event.data) {
                Ok(value) => chunks.push((event.t_ms, value)),
                Err(_) => parsed.notes.push("stream_event_not_json"),
            }
        }
    } else if let Some(frame) = responses.first() {
        match body(&frame.data) {
            Some(Value::Array(items)) => chunks.extend(items.into_iter().map(|item| (frame.t_ms, item))),
            Some(value) => chunks.push((frame.t_ms, value)),
            None => {
                parsed.notes.push("response_not_json");
                parsed.error = Some(frame.data.trim().to_owned());
            }
        }
    } else {
        parsed.notes.push("no_response");
    }

    let mut builder = Message::new(Side::Response, 0, "assistant");
    let mut text = String::new();
    let mut thought = String::new();
    let mut called = false;
    for (t_ms, chunk) in chunks {
        if let Some(error) = chunk.get("error") {
            parsed.error_kind = Some(error_outcome(
                error.get("code").and_then(Value::as_i64),
                error.get("status").and_then(Value::as_str),
            ));
            parsed.error = string(error, "message");
            parsed.finish_reason = Some("error");
            continue;
        }
        if let Some(version) = string(&chunk, "modelVersion") {
            parsed.model = Some(version);
        }
        if let Some(usage) = chunk.get("usageMetadata") {
            let thoughts = number(usage, "thoughtsTokenCount");
            parsed.usage.input = number(usage, "promptTokenCount").or(parsed.usage.input);
            parsed.usage.cached = number(usage, "cachedContentTokenCount").or(parsed.usage.cached);
            parsed.usage.output = sum(&[number(usage, "candidatesTokenCount"), thoughts]).or(parsed.usage.output);
            parsed.usage.reasoning = thoughts.or(parsed.usage.reasoning);
        }
        let Some(candidate) =
            chunk.get("candidates").and_then(Value::as_array).and_then(|candidates| candidates.first())
        else {
            continue;
        };
        for part in candidate
            .get("content")
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let produced = if let Some(call) = part.get("functionCall") {
                // Text so far stays before the call.
                flush(&mut builder, &mut thought, &mut text);
                called = true;
                builder.push(
                    PartKind::ToolCall,
                    call.get("args").map(Value::to_string).unwrap_or_else(|| "{}".to_owned()),
                    string(call, "id"),
                    string(call, "name"),
                );
                true
            } else if let Some(piece) = part.get("text").and_then(Value::as_str) {
                if part.get("thought").and_then(Value::as_bool) == Some(true) {
                    thought.push_str(piece);
                } else {
                    text.push_str(piece);
                }
                !piece.is_empty()
            } else {
                false
            };
            if produced && parsed.ttft_ms.is_none() && transport == Transport::Sse {
                parsed.ttft_ms = Some(t_ms);
            }
        }
        if let Some(reason) = candidate.get("finishReason").and_then(Value::as_str) {
            parsed.finished = true;
            parsed.finish_reason = Some(finish_reason(reason));
        }
    }
    flush(&mut builder, &mut thought, &mut text);
    if called && parsed.finish_reason == Some("stop") {
        parsed.finish_reason = Some("tool_calls");
    }
    parsed.parts.extend(builder.parts);
    Ok(parsed)
}

fn flush(builder: &mut Message, thought: &mut String, text: &mut String) {
    builder.push(PartKind::Reasoning, std::mem::take(thought), None, None);
    builder.push(PartKind::Text, std::mem::take(text), None, None);
}

/// The `parts` of a content or system instruction.
fn parts(content: &Value, builder: &mut Message) {
    for part in content.get("parts").and_then(Value::as_array).into_iter().flatten() {
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            let kind = if part.get("thought").and_then(Value::as_bool) == Some(true) {
                PartKind::Reasoning
            } else {
                PartKind::Text
            };
            builder.push(kind, text.to_owned(), None, None);
        } else if let Some(call) = part.get("functionCall") {
            builder.push(
                PartKind::ToolCall,
                call.get("args").map(Value::to_string).unwrap_or_else(|| "{}".to_owned()),
                string(call, "id"),
                string(call, "name"),
            );
        } else if let Some(result) = part.get("functionResponse") {
            let text = result.get("response").map(Value::to_string).unwrap_or_default();
            builder.push(PartKind::ToolResult, text, string(result, "id"), string(result, "name"));
            if let Some(last) = builder.parts.last_mut() {
                last.role = "tool";
                last.is_error = looks_like_error(&last.content.text);
            }
        } else if let Some(data) = part.get("inlineData").or_else(|| part.get("inline_data")) {
            let mime = data.get("mimeType").or_else(|| data.get("mime_type")).and_then(Value::as_str).unwrap_or("");
            let kind = if mime.starts_with("image/") {
                PartKind::Image
            } else if mime.starts_with("audio/") {
                PartKind::Audio
            } else {
                PartKind::File
            };
            builder.push(kind, data.to_string(), None, None);
        } else if part.get("fileData").is_some() {
            builder.push(PartKind::File, part.to_string(), None, None);
        } else {
            builder.push(PartKind::Other, part.to_string(), None, None);
        }
    }
}
