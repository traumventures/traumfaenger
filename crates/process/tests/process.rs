//! Hand-made exchanges for every supported API and failure shape. They mirror
//! real recordings but contain no real data.

use serde_json::{Value, json};
use traumfaenger_api::StoredExchange;
use traumfaenger_process::{Api, CostSource, Outcome, PartKind, Side, process};

fn exchange(
    transport: &str,
    url: &str,
    status: Option<u16>,
    request: Value,
    responses: &[(u64, &str)],
    end: Value,
) -> StoredExchange {
    let mut frames = vec![json!({ "t_ms": 0, "dir": "out", "data": request.to_string() })];
    frames.extend(responses.iter().map(|(t_ms, data)| json!({ "t_ms": t_ms, "dir": "in", "data": data })));
    serde_json::from_value(json!({
        "id": "e1", "service": "svc", "operation": "op", "session_id": null, "metadata": {},
        "transport": transport, "url": url, "started_at": "2026-10-09T10:00:00Z", "status": status,
        "segments": 1, "complete": true, "truncated": false, "frames": frames, "end": end,
    }))
    .expect("a valid stored exchange")
}

fn done(at_ms: u64) -> Value {
    json!({ "at_ms": at_ms, "reason": "complete" })
}

const OPENROUTER: &str = "https://openrouter.ai/api/v1/chat/completions";

#[test]
fn openrouter_http_with_tool_round_trip() {
    let request = json!({
        "model": "openai/gpt-x", "tools": [{ "type": "function", "function": { "name": "lookup" } }],
        "messages": [
            { "role": "system", "content": "be brief" },
            { "role": "user", "content": "hi" },
            { "role": "assistant", "content": null, "tool_calls": [{ "id": "c1", "type": "function", "function": { "name": "lookup", "arguments": "{\"q\":1}" } }] },
            { "role": "tool", "tool_call_id": "c1", "content": "{\"error\":\"not found\"}" },
        ],
    });
    let response = r#"

{"model":"openai/gpt-x","provider":"OpenAI","choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":"","tool_calls":[{"id":"c2","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}],"usage":{"prompt_tokens":100,"completion_tokens":20,"cost":0.0012,"cost_details":{"upstream_inference_prompt_cost":0.0009,"upstream_inference_completions_cost":0.0003},"prompt_tokens_details":{"cached_tokens":80,"cache_write_tokens":0},"completion_tokens_details":{"reasoning_tokens":5}}}"#;
    let call = process(&exchange("http", OPENROUTER, Some(200), request, &[(900, response)], done(900)));

    assert_eq!(call.api, Api::OpenAiChat);
    assert_eq!(call.parse.status(), "parsed");
    assert_eq!(call.gateway, "openrouter");
    assert_eq!(call.provider.as_deref(), Some("OpenAI"));
    assert_eq!(call.outcome, Outcome::Ok);
    assert_eq!(call.finish_reason, Some("tool_calls"));
    assert_eq!(
        (call.usage.input, call.usage.cached, call.usage.output, call.usage.reasoning),
        (Some(100), Some(80), Some(20), Some(5))
    );
    assert_eq!((call.cost_usd, call.cost_source), (Some(0.0012), CostSource::Reported));
    assert!((call.input_cost_usd.unwrap() - 0.0009).abs() < 1e-12);
    assert!((call.output_cost_usd.unwrap() - 0.0003).abs() < 1e-12);
    assert!(call.tools.is_some());
    let result = call.parts.iter().find(|part| part.kind == PartKind::ToolResult).unwrap();
    assert_eq!((result.tool_name.as_deref(), result.is_error, result.role), (Some("lookup"), true, "tool"));
    let emitted: Vec<_> = call.tool_calls().map(|part| part.tool_call_id.clone().unwrap()).collect();
    assert_eq!(emitted, ["c2"]);
    assert!(!call.starts_turn(), "ends with a tool result: continues a turn");
    // The empty assistant text is not stored.
    assert!(!call.parts.iter().any(|part| part.side == Side::Response && part.kind == PartKind::Text));
}

#[test]
fn openrouter_stream_skips_keepalives_for_ttft() {
    let request = json!({ "model": "m", "stream": true, "messages": [{ "role": "user", "content": "hi" }] });
    let frames = [
        (100, ": OPENROUTER PROCESSING"),
        (
            300,
            r#"data: {"model":"m-1","provider":"P","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}"#,
        ),
        (450, r#"data: {"choices":[{"index":0,"delta":{"content":"Hel"}}]}"#),
        (
            500,
            r#"data: {"choices":[{"index":0,"delta":{"content":"lo","tool_calls":[{"index":0,"id":"t1","function":{"name":"f","arguments":"{\"a\""}}]}}]}"#,
        ),
        (520, r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":":1}"}}]}}]}"#),
        (
            600,
            r#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":4,"cost":0.5}}"#,
        ),
        (601, "data: [DONE]"),
    ];
    let call = process(&exchange("sse", OPENROUTER, Some(200), request, &frames, done(601)));
    assert_eq!(call.ttft_ms, Some(450));
    assert_eq!(call.model.as_deref(), Some("m-1"));
    assert_eq!(call.outcome, Outcome::Ok);
    let text: Vec<_> = call
        .parts
        .iter()
        .filter(|part| part.side == Side::Response)
        .map(|part| (part.kind, part.content.text.as_str()))
        .collect();
    assert_eq!(text, [(PartKind::Text, "Hello"), (PartKind::ToolCall, "{\"a\":1}")]);
}

#[test]
fn stream_without_finish_is_incomplete_and_cancel_is_cancelled() {
    let request = json!({ "model": "m", "stream": true, "messages": [{ "role": "user", "content": "hi" }] });
    let frames = [(10, r#"data: {"choices":[{"index":0,"delta":{"content":"par"}}]}"#)];
    let cut = process(&exchange("sse", OPENROUTER, Some(200), request.clone(), &frames, done(20)));
    assert_eq!(cut.outcome, Outcome::Incomplete);
    let cancelled = process(&exchange(
        "sse",
        OPENROUTER,
        Some(200),
        request,
        &frames,
        json!({ "at_ms": 20, "reason": "cancelled" }),
    ));
    assert_eq!(cancelled.outcome, Outcome::Cancelled);
}

#[test]
fn errors_by_status_and_in_body() {
    let request = json!({ "model": "q", "messages": [{ "role": "user", "content": "hi" }] });
    let limited = r#"{"error":{"message":"Provider returned error","code":429,"metadata":{"raw":"q is rate-limited","provider_name":"Alibaba"}}}"#;
    let call = process(&exchange("http", OPENROUTER, Some(429), request.clone(), &[(5, limited)], done(5)));
    assert_eq!(call.outcome, Outcome::RateLimited);
    assert_eq!(call.provider.as_deref(), Some("Alibaba"));
    assert_eq!(call.error.as_deref(), Some("Provider returned error: q is rate-limited"));
    assert_eq!(call.cost_source, CostSource::Unknown);

    let flagged = r#"{"model":"m","choices":[{"index":0,"finish_reason":"error","error":{"code":400,"message":"flagged"},"message":{"role":"assistant","content":""}}]}"#;
    let call = process(&exchange("http", OPENROUTER, Some(200), request.clone(), &[(5, flagged)], done(5)));
    assert_eq!(
        (call.outcome, call.finish_reason, call.error.as_deref()),
        (Outcome::ClientError, Some("error"), Some("flagged"))
    );

    let network = process(&exchange(
        "http",
        OPENROUTER,
        None,
        request.clone(),
        &[],
        json!({ "at_ms": 30000, "reason": "error", "error": "timeout" }),
    ));
    assert_eq!((network.outcome, network.error.as_deref()), (Outcome::NetworkError, Some("timeout")));

    let overloaded = process(&exchange("http", OPENROUTER, Some(503), request, &[(5, "upstream down")], done(5)));
    assert_eq!(overloaded.outcome, Outcome::ProviderError);
    assert_eq!(overloaded.parse.status(), "partial");
}

#[test]
fn anthropic_stream_through_langdock_is_priced() {
    let request = json!({
        "model": "claude-sonnet-4-6-default", "stream": true,
        "system": [{ "type": "text", "text": "sys", "cache_control": { "type": "ephemeral" } }],
        "messages": [
            { "role": "user", "content": "hi" },
            { "role": "assistant", "content": [{ "type": "tool_use", "id": "tu1", "name": "lookup", "input": { "q": 1 } }] },
            { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "tu1", "content": "ok", "is_error": false }] },
        ],
    });
    let frames = [
        (
            50,
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":1000,\"cache_creation_input_tokens\":100,\"output_tokens\":1}}}",
        ),
        (60, "event: ping\ndata: {\"type\":\"ping\"}"),
        (
            70,
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}",
        ),
        (
            80,
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}",
        ),
        (
            90,
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu2\",\"name\":\"lookup\",\"input\":{}}}",
        ),
        (
            95,
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"q\\\":2}\"}}",
        ),
        (
            // Repeats `input_tokens` without the cache counts.
            99,
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"input_tokens\":10,\"output_tokens\":30}}",
        ),
        (100, "event: message_stop\ndata: {\"type\":\"message_stop\"}"),
    ];
    let call = process(&exchange(
        "sse",
        "https://api.langdock.com/anthropic/eu/messages",
        Some(200),
        request,
        &frames,
        done(100),
    ));
    assert_eq!(
        (call.api, call.gateway.as_str(), call.provider.as_deref()),
        (Api::AnthropicMessages, "langdock", Some("anthropic"))
    );
    assert_eq!(call.outcome, Outcome::Ok);
    assert_eq!(call.finish_reason, Some("tool_calls"));
    assert_eq!(
        (call.usage.input, call.usage.cached, call.usage.cache_write, call.usage.output),
        (Some(1110), Some(1000), Some(100), Some(30))
    );
    assert_eq!(call.ttft_ms, Some(80));
    assert_eq!(call.cost_source, CostSource::Priced);
    // 10 uncached x 3 + 1000 cached x 0.3 + 100 written x 3.75 + 30 out x 15, per million.
    let expected = (10.0 * 3.0 + 1000.0 * 0.3 + 100.0 * 3.75 + 30.0 * 15.0) / 1e6;
    assert!((call.cost_usd.unwrap() - expected).abs() < 1e-12);
    assert!((call.output_cost_usd.unwrap() - 30.0 * 15.0 / 1e6).abs() < 1e-12);
    let result = call.parts.iter().find(|part| part.kind == PartKind::ToolResult).unwrap();
    assert_eq!((result.tool_name.as_deref(), result.role, result.is_error), (Some("lookup"), "tool", false));
    assert!(!call.starts_turn(), "a tool result is not a new user turn");
    let emitted: Vec<_> =
        call.tool_calls().map(|part| (part.tool_call_id.clone().unwrap(), part.content.text.clone())).collect();
    assert_eq!(emitted, [("tu2".to_owned(), "{\"q\":2}".to_owned())]);
}

#[test]
fn anthropic_error_body() {
    let request = json!({ "model": "claude-x", "messages": [{ "role": "user", "content": "hi" }] });
    let body = r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#;
    let call =
        process(&exchange("http", "https://api.anthropic.com/v1/messages", Some(529), request, &[(5, body)], done(5)));
    assert_eq!((call.outcome, call.error.as_deref()), (Outcome::ProviderError, Some("Overloaded")));
}

#[test]
fn gemini_stream_through_langdock() {
    let request = json!({
        "systemInstruction": { "parts": [{ "text": "sys" }] },
        "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }],
    });
    let frames = [
        (
            40,
            r#"data: {"candidates":[{"content":{"role":"model","parts":[{"text":"thinking","thought":true}]}}],"modelVersion":"gemini-3.5-flash"}"#,
        ),
        (70, r#"data: {"candidates":[{"content":{"role":"model","parts":[{"text":"Hal"}]}}]}"#),
        (
            90,
            r#"data: {"candidates":[{"content":{"role":"model","parts":[{"text":"lo"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":40,"candidatesTokenCount":5,"thoughtsTokenCount":7}}"#,
        ),
    ];
    let url = "https://api.langdock.com/google/eu/v1beta/models/gemini-3.5-flash:streamGenerateContent";
    let call = process(&exchange("sse", url, Some(200), request, &frames, done(90)));
    assert_eq!(
        (call.api, call.provider.as_deref(), call.model.as_deref()),
        (Api::GeminiGenerate, Some("google"), Some("gemini-3.5-flash"))
    );
    assert_eq!(
        (call.usage.input, call.usage.cached, call.usage.output, call.usage.reasoning),
        (Some(100), Some(40), Some(12), Some(7))
    );
    assert_eq!((call.ttft_ms, call.finish_reason, call.outcome), (Some(40), Some("stop"), Outcome::Ok));
    assert_eq!(call.cost_source, CostSource::Priced);
    let response: Vec<_> = call
        .parts
        .iter()
        .filter(|part| part.side == Side::Response)
        .map(|part| (part.kind, part.content.text.as_str()))
        .collect();
    assert_eq!(response, [(PartKind::Reasoning, "thinking"), (PartKind::Text, "Hallo")]);
    assert_eq!(call.parts.iter().filter(|part| part.role == "system").count(), 1);
}

#[test]
fn gemini_function_call_sets_tool_calls() {
    let request = json!({ "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }] });
    let body = r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"lookup","args":{"q":1}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2}}"#;
    let url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-x:generateContent";
    let call = process(&exchange("http", url, Some(200), request, &[(10, body)], done(10)));
    assert_eq!((call.gateway.as_str(), call.finish_reason), ("google", Some("tool_calls")));
    assert_eq!(call.tool_calls().count(), 1);
}

#[test]
fn transcription_and_embeddings() {
    let request = json!({ "model": "openai/whisper", "input_audio": { "data": "[traumfaenger: omitted base64, 9000 bytes]", "format": "webm" } });
    let url = "https://openrouter.ai/api/v1/audio/transcriptions";
    let call = process(&exchange(
        "http",
        url,
        Some(200),
        request,
        &[(2000, r#"{"text":"Hallo","usage":{"cost":0.01}}"#)],
        done(2000),
    ));
    assert_eq!((call.api, call.outcome, call.cost_usd), (Api::Transcription, Outcome::Ok, Some(0.01)));
    assert!(call.parts.iter().any(|part| part.kind == PartKind::Audio));
    assert!(call.parts.iter().any(|part| part.side == Side::Response && part.content.text == "Hallo"));

    let request = json!({ "model": "e", "input": ["a", "b"] });
    let url = "https://openrouter.ai/api/v1/embeddings";
    let call = process(&exchange(
        "http",
        url,
        Some(200),
        request,
        &[(20, r#"{"model":"e","data":[{"embedding":[0.1]}],"usage":{"prompt_tokens":2}}"#)],
        done(20),
    ));
    assert_eq!((call.api, call.usage.input), (Api::Embeddings, Some(2)));
    assert_eq!(call.parts.len(), 2);
}

#[test]
fn unknown_api_is_kept() {
    let call =
        process(&exchange("http", "https://example.com/v9/do", Some(200), json!({ "x": 1 }), &[(5, "{}")], done(5)));
    assert_eq!((call.api, call.parse.status(), call.parse.reason()), (Api::Unknown, "unparsed", Some("unknown_api")));
    assert_eq!((call.gateway.as_str(), call.outcome), ("example.com", Outcome::Ok));
}

#[test]
fn identical_texts_share_a_hash() {
    let request = json!({ "model": "m", "messages": [{ "role": "system", "content": "same" }, { "role": "user", "content": "same" }] });
    let call = process(&exchange("http", OPENROUTER, Some(200), request, &[], done(1)));
    assert_eq!(call.parts[0].content.hash, call.parts[1].content.hash);
    assert!(call.starts_turn());
}
