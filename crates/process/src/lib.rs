//! The read side's parser: one recorded exchange in, one normalized call out.
//!
//! [`process`] never fails. Every exchange becomes a [`Call`]; when the format
//! is unknown or a body does not parse, the call says so in `parse` and keeps
//! what could be read (status, timing, outcome). Nothing is ever dropped.
//!
//! Text is split into [`Content`]s: message texts, system prompts, tool
//! definitions, tool arguments and results. Each is identified by the hash of
//! its text, so storage keeps every distinct text once however many calls
//! repeat it.
//!
//! Supported provider APIs: OpenAI-style chat completions (OpenRouter, OpenAI,
//! Langdock's OpenAI endpoint), Anthropic messages, Gemini `generateContent`,
//! OpenAI-style audio transcriptions and embeddings, each as plain HTTP or SSE.

mod anthropic;
mod gemini;
mod json;
mod openai;
pub mod pricing;
mod sse;

use traumfaenger_api::{Direction, EndReason, StoredExchange, Transport};

/// Bumped whenever processing changes its output, so stored results are rebuilt.
pub const PARSER_VERSION: u32 = 1;

/// Identifies a [`Content`]: the first 16 bytes of the BLAKE3 hash of its text.
pub type ContentHash = [u8; 16];

/// A distinct piece of text, stored once.
#[derive(Clone, Debug, PartialEq)]
pub struct Content {
    pub hash: ContentHash,
    pub text: String,
}

impl Content {
    pub fn new(text: String) -> Self {
        let digest = blake3::hash(text.as_bytes());
        let mut hash = [0u8; 16];
        hash.copy_from_slice(&digest.as_bytes()[..16]);
        Self { hash, text }
    }

    /// Canonical JSON (object keys sorted) of a value.
    pub fn json(value: &serde_json::Value) -> Self {
        Self::new(value.to_string())
    }
}

/// Which provider API the exchange spoke.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Api {
    OpenAiChat,
    AnthropicMessages,
    GeminiGenerate,
    Transcription,
    Embeddings,
    Unknown,
}

impl Api {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiChat => "openai_chat",
            Self::AnthropicMessages => "anthropic_messages",
            Self::GeminiGenerate => "gemini_generate",
            Self::Transcription => "transcription",
            Self::Embeddings => "embeddings",
            Self::Unknown => "unknown",
        }
    }
}

/// How the call ended, from the caller's point of view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Ok,
    /// HTTP 429, or a provider error that says so.
    RateLimited,
    /// Any other 4xx.
    ClientError,
    /// 5xx, or an error the provider reported inside a 200 (for example mid-stream).
    ProviderError,
    /// No response: network or protocol error before a status.
    NetworkError,
    Cancelled,
    /// The stream or socket ended without the provider finishing.
    Incomplete,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::RateLimited => "rate_limited",
            Self::ClientError => "client_error",
            Self::ProviderError => "provider_error",
            Self::NetworkError => "network_error",
            Self::Cancelled => "cancelled",
            Self::Incomplete => "incomplete",
        }
    }

    pub fn is_failure(self) -> bool {
        self != Self::Ok
    }
}

/// Whether the bodies could be read.
#[derive(Clone, Debug, PartialEq)]
pub enum Parse {
    Parsed,
    /// Read, but something was skipped; the reason is a short stable category.
    Partial(String),
    Unparsed(String),
}

impl Parse {
    pub fn status(&self) -> &'static str {
        match self {
            Self::Parsed => "parsed",
            Self::Partial(_) => "partial",
            Self::Unparsed(_) => "unparsed",
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Parsed => None,
            Self::Partial(reason) | Self::Unparsed(reason) => Some(reason),
        }
    }
}

/// Token usage as reported, normalized across APIs.
///
/// `input` counts every prompt token including cached reads and cache writes;
/// `output` counts every generated token including reasoning.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Usage {
    pub input: Option<u64>,
    pub cached: Option<u64>,
    pub cache_write: Option<u64>,
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CostSource {
    /// The provider reported the charged amount (OpenRouter `usage.cost`).
    Reported,
    /// Computed from usage and the price table in [`pricing`].
    Priced,
    Unknown,
}

impl CostSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reported => "reported",
            Self::Priced => "priced",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    Request,
    Response,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartKind {
    Text,
    Reasoning,
    ToolCall,
    ToolResult,
    Image,
    Audio,
    File,
    Other,
}

impl PartKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Reasoning => "reasoning",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::File => "file",
            Self::Other => "other",
        }
    }
}

/// One part of one message, in conversation order.
#[derive(Clone, Debug, PartialEq)]
pub struct Part {
    pub side: Side,
    /// Position of the message within its side.
    pub message: u32,
    /// Position of the part within the message.
    pub index: u32,
    /// `system`, `user`, `assistant` or `tool`, normalized across APIs.
    pub role: &'static str,
    pub kind: PartKind,
    pub content: Content,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
    /// Tool results only: the result reports an error.
    pub is_error: bool,
}

/// One processed exchange.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    pub api: Api,
    /// The service the request went to: `openrouter`, `langdock`, `anthropic`, … or the host.
    pub gateway: String,
    /// Who served it: OpenRouter's upstream provider, else the API's vendor.
    pub provider: Option<String>,
    pub model_requested: Option<String>,
    /// The model that answered, else the requested one.
    pub model: Option<String>,
    pub stream: bool,
    pub outcome: Outcome,
    /// Short provider or network error message.
    pub error: Option<String>,
    /// `stop`, `length`, `tool_calls`, `content_filter`, `error` or `other`.
    pub finish_reason: Option<&'static str>,
    pub usage: Usage,
    pub cost_usd: Option<f64>,
    pub cost_source: CostSource,
    /// Milliseconds to the first frame carrying generated content (streams only).
    pub ttft_ms: Option<u64>,
    /// Tool definitions sent with the request.
    pub tools: Option<Content>,
    pub parts: Vec<Part>,
    pub parse: Parse,
}

impl Call {
    pub fn tool_calls(&self) -> impl Iterator<Item = &Part> {
        self.parts.iter().filter(|part| part.side == Side::Response && part.kind == PartKind::ToolCall)
    }

    /// Number of user messages in the request: the turn a call belongs to.
    /// An agent resends the conversation, so every step of one turn has the same count.
    pub fn user_messages(&self) -> u32 {
        let mut seen = std::collections::BTreeSet::new();
        for part in &self.parts {
            if part.side == Side::Request && part.role == "user" && part.kind != PartKind::ToolResult {
                seen.insert(part.message);
            }
        }
        seen.len() as u32
    }
}

/// What a format parser fills in; [`process`] adds the transport-level facts.
#[derive(Default)]
pub(crate) struct Parsed {
    pub provider: Option<String>,
    pub model_requested: Option<String>,
    pub model: Option<String>,
    pub stream: bool,
    pub error: Option<String>,
    /// The provider signalled an error in the body (also inside a 200).
    pub error_kind: Option<Outcome>,
    pub finish_reason: Option<&'static str>,
    /// The provider said it finished (stop reason, `[DONE]`, `message_stop`).
    pub finished: bool,
    pub usage: Usage,
    pub cost_usd: Option<f64>,
    pub ttft_ms: Option<u64>,
    pub tools: Option<Content>,
    pub parts: Vec<Part>,
    pub notes: Vec<&'static str>,
}

/// Processes one exchange. Never fails; see [`Call::parse`].
pub fn process(exchange: &StoredExchange) -> Call {
    let (host, path) = split_url(&exchange.url);
    let api = detect_api(&path);
    let gateway = gateway(&host);
    let request = exchange.frames.iter().find(|frame| frame.dir == Direction::Out).map(|frame| frame.data.as_str());
    let responses: Vec<&traumfaenger_api::Frame> =
        exchange.frames.iter().filter(|frame| frame.dir == Direction::In).collect();
    let status = exchange.status;

    let result = match api {
        Api::OpenAiChat | Api::Transcription | Api::Embeddings => {
            openai::parse(api, exchange.transport, request, &responses)
        }
        Api::AnthropicMessages => anthropic::parse(exchange.transport, request, &responses),
        Api::GeminiGenerate => gemini::parse(exchange.transport, &path, request, &responses),
        Api::Unknown => Err("unknown_api".to_owned()),
    };
    let (mut parsed, parse) = match result {
        Ok(parsed) => {
            let parse = if parsed.notes.is_empty() {
                Parse::Parsed
            } else {
                let mut notes = parsed.notes.clone();
                notes.sort_unstable();
                notes.dedup();
                Parse::Partial(notes.join(","))
            };
            (parsed, parse)
        }
        Err(reason) => (Parsed::default(), Parse::Unparsed(reason)),
    };

    let outcome = outcome(exchange, status, &parsed);
    if parsed.error.is_none()
        && let Some(end) = &exchange.end
        && end.reason == EndReason::Error
    {
        parsed.error = end.error.clone();
    }
    let provider = parsed.provider.take().or_else(|| default_provider(api, &gateway, &path));
    let model = parsed.model.take().or_else(|| parsed.model_requested.clone());
    let (cost_usd, cost_source) = match parsed.cost_usd {
        Some(cost) => (Some(cost), CostSource::Reported),
        None => match model.as_deref().and_then(|model| pricing::price(model, &parsed.usage)) {
            Some(cost) => (Some(cost), CostSource::Priced),
            None => (None, CostSource::Unknown),
        },
    };
    Call {
        api,
        gateway,
        provider,
        model_requested: parsed.model_requested,
        model,
        stream: parsed.stream || exchange.transport == Transport::Sse,
        outcome,
        error: parsed.error.map(|error| clip(&error, 500)),
        finish_reason: parsed.finish_reason,
        usage: parsed.usage,
        cost_usd,
        cost_source,
        ttft_ms: parsed.ttft_ms,
        tools: parsed.tools,
        parts: parsed.parts,
        parse,
    }
}

fn outcome(exchange: &StoredExchange, status: Option<u16>, parsed: &Parsed) -> Outcome {
    match exchange.end.as_ref().map(|end| end.reason) {
        Some(EndReason::Cancelled) => return Outcome::Cancelled,
        Some(EndReason::Error) if status.is_none() => return Outcome::NetworkError,
        None => return Outcome::Incomplete,
        _ => {}
    }
    match status {
        Some(429) => return Outcome::RateLimited,
        Some(status) if (400..500).contains(&status) => return Outcome::ClientError,
        Some(status) if status >= 500 => return Outcome::ProviderError,
        None if exchange.transport != Transport::Ws => return Outcome::NetworkError,
        _ => {}
    }
    if let Some(kind) = parsed.error_kind {
        return kind;
    }
    if exchange.end.as_ref().is_some_and(|end| end.reason == EndReason::Error) {
        return Outcome::NetworkError;
    }
    if exchange.transport == Transport::Sse && !parsed.finished {
        return Outcome::Incomplete;
    }
    Outcome::Ok
}

/// `(host, path)` of a URL; the query string is already dropped by the clients.
fn split_url(url: &str) -> (String, String) {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let (host, path) = rest.split_once('/').map_or((rest, ""), |(host, path)| (host, path));
    let path = path.split(['?', '#']).next().unwrap_or("");
    (host.to_ascii_lowercase(), format!("/{path}"))
}

fn detect_api(path: &str) -> Api {
    if path.ends_with("/chat/completions") {
        Api::OpenAiChat
    } else if path.ends_with("/messages") {
        Api::AnthropicMessages
    } else if path.contains(":generateContent") || path.contains(":streamGenerateContent") {
        Api::GeminiGenerate
    } else if path.ends_with("/audio/transcriptions") {
        Api::Transcription
    } else if path.ends_with("/embeddings") {
        Api::Embeddings
    } else {
        Api::Unknown
    }
}

fn gateway(host: &str) -> String {
    let host = host.split(':').next().unwrap_or(host);
    let known = [
        ("openrouter.ai", "openrouter"),
        ("langdock.com", "langdock"),
        ("anthropic.com", "anthropic"),
        ("openai.com", "openai"),
        ("googleapis.com", "google"),
        ("deepseek.com", "deepseek"),
        ("mistral.ai", "mistral"),
        ("x.ai", "xai"),
        ("groq.com", "groq"),
    ];
    for (suffix, name) in known {
        if host == suffix || host.ends_with(&format!(".{suffix}")) {
            return name.to_owned();
        }
    }
    host.to_owned()
}

/// The vendor behind a gateway that does not name the upstream provider.
fn default_provider(api: Api, gateway: &str, path: &str) -> Option<String> {
    if gateway == "langdock" {
        // https://api.langdock.com/<vendor>/<region>/...
        return path.split('/').nth(1).filter(|vendor| !vendor.is_empty()).map(str::to_owned);
    }
    match (gateway, api) {
        ("openrouter", _) => None,
        (_, Api::AnthropicMessages) => Some("anthropic".to_owned()),
        (_, Api::GeminiGenerate) => Some("google".to_owned()),
        _ => Some(gateway.to_owned()),
    }
}

fn clip(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// Normalizes a finish or stop reason of any API.
pub(crate) fn finish_reason(raw: &str) -> &'static str {
    match raw {
        "stop" | "end_turn" | "stop_sequence" | "STOP" | "FINISH_REASON_STOP" => "stop",
        "length" | "max_tokens" | "MAX_TOKENS" | "model_context_window_exceeded" => "length",
        "tool_calls" | "function_call" | "tool_use" => "tool_calls",
        "content_filter" | "refusal" | "SAFETY" | "RECITATION" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII"
        | "IMAGE_SAFETY" => "content_filter",
        "error" | "MALFORMED_FUNCTION_CALL" => "error",
        _ => "other",
    }
}

/// Whether a tool result reports a failure. A heuristic for APIs without an
/// error flag: a JSON object with a non-null `error`, `ok: false` or
/// `success: false`, or text starting with "error".
pub(crate) fn looks_like_error(text: &str) -> bool {
    let trimmed = text.trim_start();
    if trimmed.starts_with('{')
        && let Ok(serde_json::Value::Object(object)) = serde_json::from_str::<serde_json::Value>(trimmed)
    {
        return object.get("error").is_some_and(|error| !error.is_null() && error != false)
            || object.get("ok") == Some(&serde_json::Value::Bool(false))
            || object.get("success") == Some(&serde_json::Value::Bool(false));
    }
    trimmed.get(..5).is_some_and(|start| start.eq_ignore_ascii_case("error"))
}

/// Maps a provider error code or HTTP-like status inside a body to an outcome.
pub(crate) fn error_outcome(code: Option<i64>, kind: Option<&str>) -> Outcome {
    match (code, kind) {
        (Some(429), _) | (_, Some("rate_limit_error" | "RESOURCE_EXHAUSTED" | "rate_limit_exceeded")) => {
            Outcome::RateLimited
        }
        (Some(code), _) if (400..500).contains(&code) => Outcome::ClientError,
        _ => Outcome::ProviderError,
    }
}
