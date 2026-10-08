//! The ai-ledger wire contract.
//!
//! ai-ledger collects AI model calls in two steps:
//!
//! 1. **Collection** (this crate today): clients record every provider call
//!    as a [`CallRecord`] (the HTTP request and response exactly as they went
//!    over the wire, plus context nothing in them contains) and send it to the
//!    ingestion API. The server stores records unparsed in a queue table.
//! 2. **Processing** (later): the server extracts models, usage, prices,
//!    costs, messages and tool calls from the queued records into derived
//!    tables. Because the raw records are kept, processing can be rerun when a
//!    parser improves.
//!
//! The service exposes two HTTP APIs on separate listeners with separate
//! bearer tokens: **ingestion** (write-only) and **query** (read-only).
//!
//! Every type that crosses the HTTP boundary is defined here once. The
//! TypeScript packages use declarations generated from it by
//! [`typescript_bindings`].

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use ts_rs::TS;

mod bindings;

pub use bindings::typescript_bindings;

/// HTTP routes. Ingestion and query routes are served on different listeners.
pub mod routes {
    /// `GET`, unauthenticated, on both listeners.
    pub const HEALTH: &str = "/health";
    /// Ingestion listener: `POST` a batch of call records.
    pub const INGEST_CALLS: &str = "/v1/calls";
    /// Query listener: `GET` a filtered page of call envelopes, newest first.
    pub const LIST_CALLS: &str = "/v1/calls";
    /// Query listener: `GET` one call including request and response.
    pub const GET_CALL: &str = "/v1/calls/{id}";
    /// Query listener: `GET` collection statistics grouped by dimensions.
    pub const SUMMARY: &str = "/v1/summary";
}

/// Limits enforced by the server. Exported to TypeScript as `LIMITS`.
pub mod limits {
    /// Maximum number of records in one ingestion request.
    pub const MAX_BATCH_SIZE: usize = 500;
    /// Maximum size of one ingestion request body after gzip decompression.
    pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
    /// Maximum length of `id`, in bytes.
    pub const MAX_ID_LEN: usize = 128;
    /// Maximum length of `service`, `environment`, `operation` and `session_id`.
    pub const MAX_NAME_LEN: usize = 256;
    pub const MAX_URL_LEN: usize = 2_048;
    /// Maximum size of `request`, in bytes.
    pub const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
    /// Maximum size of `response`, in bytes (sum of all events for streams).
    pub const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
    pub const MAX_ERROR_LEN: usize = 4_000;
    pub const MAX_METADATA_ENTRIES: usize = 32;
    pub const MAX_METADATA_KEY_LEN: usize = 64;
    pub const MAX_METADATA_VALUE_LEN: usize = 512;
    /// Upper bound for `latency_ms` and `ttft_ms`: one day.
    pub const MAX_DURATION_MS: u64 = 86_400_000;
    /// Default and maximum page size of `GET /v1/calls`.
    pub const DEFAULT_LIST_LIMIT: u32 = 100;
    pub const MAX_LIST_LIMIT: u32 = 1_000;
    /// Default and maximum number of rows of `GET /v1/summary`.
    pub const DEFAULT_SUMMARY_LIMIT: u32 = 1_000;
    pub const MAX_SUMMARY_LIMIT: u32 = 10_000;
}

/// The provider's response as received.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
#[serde(untagged)]
pub enum RawResponse {
    /// The complete response body of a non-streaming call.
    Body(String),
    /// A streaming (SSE) response: one entry per event, verbatim (the text
    /// between blank lines, including `event:` and `data:` lines).
    Events(Vec<String>),
}

impl RawResponse {
    pub fn byte_len(&self) -> usize {
        match self {
            Self::Body(body) => body.len(),
            Self::Events(events) => events.iter().map(String::len).sum(),
        }
    }
}

/// One AI provider call as recorded by a client.
///
/// Only what is not contained in the request or response is sent as a
/// separate field. Provider, model, usage, finish reason, messages, tool calls
/// and cost are extracted on the server from `url`, `request` and `response`.
///
/// `id` is the idempotency key: re-sending a record whose id is already stored
/// is accepted, counted as a duplicate and leaves the stored record unchanged.
///
/// Unknown fields are ignored, so a newer client can talk to an older server.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct CallRecord {
    /// Client-generated unique id, for example a UUID. 1–128 bytes.
    pub id: String,
    /// The emitting service, for example `traumwohnung-web`.
    pub service: String,
    /// For example `production`, `dev` or `local`.
    pub environment: String,
    /// The kind of work, for example `listing.parse`.
    pub operation: String,
    /// Groups the calls of one conversation, for example one support chat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub session_id: Option<String>,
    /// Flat key-value pairs, for example `{ "listing_id": "123" }`. At most 32
    /// entries; keys are 1–64 characters of `A-Z a-z 0-9 _ . -`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "Record<string, string>")]
    pub metadata: Option<BTreeMap<String, String>>,
    /// The request URL. The server drops query string and fragment.
    pub url: String,
    /// The request body exactly as sent.
    pub request: String,
    /// The response as received. Absent when no response arrived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub response: Option<RawResponse>,
    /// HTTP status of the response. Absent when no response arrived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub http_status: Option<u16>,
    /// Client-side exception, for example a network failure or an abort.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub error: Option<String>,
    /// True when the client changed anything to fit the limits: clipped
    /// `request` or `response`, or cleaned context (dropped invalid metadata
    /// entries, clipped values, an empty operation recorded as `unattributed`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    #[ts(optional, as = "Option<bool>")]
    pub truncated: bool,
    /// When the request was sent, RFC 3339.
    #[ts(type = "string")]
    pub started_at: DateTime<Utc>,
    /// Streaming calls only: time until the first response chunk, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "number")]
    pub ttft_ms: Option<u64>,
    /// Time until the response was complete (or failed), in milliseconds.
    #[ts(type = "number")]
    pub latency_ms: u64,
}

impl CallRecord {
    /// Checks every field against [`limits`]. Returns a human-readable reason.
    pub fn validate(&self) -> Result<(), String> {
        check_line("id", &self.id, limits::MAX_ID_LEN)?;
        check_line("service", &self.service, limits::MAX_NAME_LEN)?;
        check_line("environment", &self.environment, limits::MAX_NAME_LEN)?;
        check_line("operation", &self.operation, limits::MAX_NAME_LEN)?;
        if let Some(session_id) = &self.session_id {
            check_line("session_id", session_id, limits::MAX_NAME_LEN)?;
        }
        if let Some(metadata) = &self.metadata {
            if metadata.len() > limits::MAX_METADATA_ENTRIES {
                return Err(format!("metadata must have at most {} entries", limits::MAX_METADATA_ENTRIES));
            }
            for (key, value) in metadata {
                if !is_metadata_key(key) {
                    return Err(format!(
                        "metadata key `{key}` must be 1–{} characters of A-Z a-z 0-9 _ . -",
                        limits::MAX_METADATA_KEY_LEN
                    ));
                }
                if value.len() > limits::MAX_METADATA_VALUE_LEN || value.contains('\0') {
                    return Err(format!(
                        "metadata value of `{key}` must be at most {} bytes without NUL",
                        limits::MAX_METADATA_VALUE_LEN
                    ));
                }
            }
        }
        check_line("url", self.stored_url(), limits::MAX_URL_LEN)?;
        if !(self.url.starts_with("https://") || self.url.starts_with("http://")) {
            return Err("url must be an http(s) URL".into());
        }
        if self.request.len() > limits::MAX_REQUEST_BYTES {
            return Err(format!("request must be at most {} bytes", limits::MAX_REQUEST_BYTES));
        }
        if let Some(response) = &self.response {
            if response.byte_len() > limits::MAX_RESPONSE_BYTES {
                return Err(format!("response must be at most {} bytes", limits::MAX_RESPONSE_BYTES));
            }
            if self.http_status.is_none() {
                return Err("response requires http_status".into());
            }
        }
        if let Some(status) = self.http_status
            && !(100..=599).contains(&status)
        {
            return Err("http_status must be between 100 and 599".into());
        }
        if let Some(error) = &self.error
            && (error.trim().is_empty() || error.len() > limits::MAX_ERROR_LEN)
        {
            return Err(format!("error must be 1–{} bytes", limits::MAX_ERROR_LEN));
        }
        if self.latency_ms > limits::MAX_DURATION_MS {
            return Err(format!("latency_ms must be at most {}", limits::MAX_DURATION_MS));
        }
        if self.ttft_ms.is_some_and(|ttft| ttft > self.latency_ms) {
            return Err("ttft_ms must not exceed latency_ms".into());
        }
        Ok(())
    }

    /// `url` without query string and fragment, which can carry API keys.
    pub fn stored_url(&self) -> &str {
        let end = self.url.find(['?', '#']).unwrap_or(self.url.len());
        &self.url[..end]
    }
}

fn check_line(name: &str, value: &str, max: usize) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if value.len() > max {
        return Err(format!("{name} must be at most {max} bytes"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{name} must not contain control characters"));
    }
    Ok(())
}

fn is_metadata_key(key: &str) -> bool {
    (1..=limits::MAX_METADATA_KEY_LEN).contains(&key.len())
        && key.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

/// Body of `POST /v1/calls` on the ingestion listener. The body may be
/// gzip-compressed (`Content-Encoding: gzip`).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct IngestRequest {
    /// 1–500 records. Each record is validated on its own: invalid records are
    /// reported in `rejected`, the valid ones are stored.
    pub calls: Vec<CallRecord>,
}

/// A record of an ingestion request that was not stored.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct RejectedCall {
    /// Position of the record in `calls`.
    pub index: u32,
    pub reason: String,
}

/// Response of `POST /v1/calls`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct IngestResponse {
    /// Records stored by this request.
    pub accepted: u32,
    /// Records whose id was already stored, or repeated within the request.
    pub duplicates: u32,
    /// Records that failed validation. Retrying them will not succeed.
    pub rejected: Vec<RejectedCall>,
}

/// A stored call without its request and response bodies.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct CallEnvelope {
    /// Position in the ingestion queue; increases with every stored record.
    #[ts(type = "number")]
    pub seq: u64,
    pub id: String,
    /// When the server stored the record, RFC 3339.
    #[ts(type = "string")]
    pub received_at: DateTime<Utc>,
    pub service: String,
    pub environment: String,
    pub operation: String,
    pub session_id: Option<String>,
    #[ts(type = "Record<string, string>")]
    pub metadata: BTreeMap<String, String>,
    /// Without query string and fragment.
    pub url: String,
    pub http_status: Option<u16>,
    pub error: Option<String>,
    pub truncated: bool,
    #[ts(type = "string")]
    pub started_at: DateTime<Utc>,
    #[ts(type = "number | null")]
    pub ttft_ms: Option<u64>,
    #[ts(type = "number")]
    pub latency_ms: u64,
    #[ts(type = "number")]
    pub request_bytes: u64,
    #[ts(type = "number")]
    pub response_bytes: u64,
}

/// Response of `GET /v1/calls/{id}`: the envelope plus request and response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct StoredCall {
    #[serde(flatten)]
    pub envelope: CallEnvelope,
    pub request: String,
    pub response: Option<RawResponse>,
}

/// Filters shared by `GET /v1/calls` and `GET /v1/summary`. Every filter is
/// optional and they combine with AND. `from` (inclusive) and `to`
/// (exclusive) apply to `started_at`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize, TS)]
#[serde(deny_unknown_fields)]
pub struct ListCallsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub environment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub session_id: Option<String>,
    /// True: only calls without a response, with status ≥ 400 or with a
    /// client error. False: only the others.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub failed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "string")]
    pub from: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "string")]
    pub to: Option<DateTime<Utc>>,
    /// Page size, 1–1000. Defaults to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub limit: Option<u32>,
    /// `next_cursor` of the previous page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub cursor: Option<String>,
}

/// Response of `GET /v1/calls`, most recently stored first.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct ListCallsResponse {
    pub calls: Vec<CallEnvelope>,
    /// Pass as `cursor` to fetch the next page. `null` on the last page.
    pub next_cursor: Option<String>,
}

/// A column `GET /v1/summary` can group by. `day` and `hour` bucket
/// `started_at` in UTC.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    Service,
    Environment,
    Operation,
    SessionId,
    HttpStatus,
    Day,
    Hour,
}

impl Dimension {
    pub const ALL: [Self; 7] =
        [Self::Service, Self::Environment, Self::Operation, Self::SessionId, Self::HttpStatus, Self::Day, Self::Hour];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::Environment => "environment",
            Self::Operation => "operation",
            Self::SessionId => "session_id",
            Self::HttpStatus => "http_status",
            Self::Day => "day",
            Self::Hour => "hour",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|dimension| dimension.as_str() == value)
    }
}

/// Query of `GET /v1/summary`: the filters of [`ListCallsQuery`] plus grouping.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SummaryQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub environment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub failed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "string")]
    pub from: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "string")]
    pub to: Option<DateTime<Utc>>,
    /// Dimensions to group by, at most once each. Sent as a comma-separated
    /// query parameter (`group_by=service,operation`). Empty means one total row.
    #[serde(
        default,
        deserialize_with = "deserialize_dimensions",
        serialize_with = "serialize_dimensions",
        skip_serializing_if = "Vec::is_empty"
    )]
    #[ts(optional, as = "Option<Vec<Dimension>>")]
    pub group_by: Vec<Dimension>,
    /// Maximum number of rows, 1–10000. Defaults to 1000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub limit: Option<u32>,
}

fn deserialize_dimensions<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Dimension>, D::Error> {
    let raw = String::deserialize(deserializer)?;
    let mut dimensions = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|part| !part.is_empty()) {
        let dimension =
            Dimension::parse(part).ok_or_else(|| serde::de::Error::custom(format!("unknown dimension `{part}`")))?;
        if dimensions.contains(&dimension) {
            return Err(serde::de::Error::custom(format!("dimension `{part}` repeated")));
        }
        dimensions.push(dimension);
    }
    Ok(dimensions)
}

fn serialize_dimensions<S: serde::Serializer>(dimensions: &[Dimension], serializer: S) -> Result<S::Ok, S::Error> {
    let joined: Vec<&str> = dimensions.iter().map(|dimension| dimension.as_str()).collect();
    serializer.serialize_str(&joined.join(","))
}

/// One aggregated row of `GET /v1/summary`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct SummaryRow {
    /// Value of each requested dimension. `null` when the calls lack it.
    pub group: BTreeMap<Dimension, Option<String>>,
    #[ts(type = "number")]
    pub calls: u64,
    /// Calls without a response, with status ≥ 400 or with a client error.
    #[ts(type = "number")]
    pub failed: u64,
    #[ts(type = "number")]
    pub request_bytes: u64,
    #[ts(type = "number")]
    pub response_bytes: u64,
    pub latency_ms_avg: Option<f64>,
    pub latency_ms_p95: Option<f64>,
    pub latency_ms_p99: Option<f64>,
    pub ttft_ms_p95: Option<f64>,
}

/// Response of `GET /v1/summary`, most calls first.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct SummaryResponse {
    pub rows: Vec<SummaryRow>,
    /// True when more groups exist than `limit` returned.
    pub truncated: bool,
}

/// Response of `GET /health`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct HealthResponse {
    pub status: String,
    /// Which listener answered: `ingest` or `query`.
    pub api: String,
    pub version: String,
}

/// Machine-readable error kind.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRequest,
    Unauthorized,
    NotFound,
    MethodNotAllowed,
    PayloadTooLarge,
    Internal,
}

/// Body of every non-2xx response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct ApiError {
    pub error: ErrorCode,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> CallRecord {
        serde_json::from_value(serde_json::json!({
            "id": "c1",
            "service": "web",
            "environment": "production",
            "operation": "listing.parse",
            "metadata": { "listing_id": "123" },
            "url": "https://openrouter.ai/api/v1/chat/completions?key=secret#x",
            "request": "{\"model\":\"openai/gpt-5-mini\",\"messages\":[]}",
            "response": "{\"choices\":[]}",
            "http_status": 200,
            "started_at": "2026-10-08T10:00:00Z",
            "ttft_ms": 120,
            "latency_ms": 900,
        }))
        .unwrap()
    }

    #[test]
    fn accepts_a_record_and_strips_the_query_string() {
        let record = record();
        assert_eq!(record.validate(), Ok(()));
        assert_eq!(record.stored_url(), "https://openrouter.ai/api/v1/chat/completions");
        assert!(!record.truncated);
    }

    #[test]
    fn response_is_a_body_or_a_list_of_events() {
        let stream: CallRecord = serde_json::from_value(serde_json::json!({
            "id": "c1", "service": "s", "environment": "e", "operation": "o", "url": "https://x/y",
            "request": "{}", "response": ["data: {\"a\":1}", "data: [DONE]"], "http_status": 200,
            "started_at": "2026-10-08T10:00:00Z", "latency_ms": 1,
        }))
        .unwrap();
        assert_eq!(stream.response, Some(RawResponse::Events(vec!["data: {\"a\":1}".into(), "data: [DONE]".into()])));
        assert_eq!(stream.response.as_ref().unwrap().byte_len(), 25);
        assert_eq!(record().response, Some(RawResponse::Body("{\"choices\":[]}".into())));
    }

    #[test]
    fn ignores_unknown_fields_and_rejects_invalid_values() {
        let mut value = serde_json::to_value(record()).unwrap();
        value["added_in_a_later_version"] = serde_json::json!({});
        assert_eq!(serde_json::from_value::<CallRecord>(value).unwrap(), record());

        // Only the stored part of the URL counts against the limit.
        let mut long_query = record();
        long_query.url = format!("https://x/y?{}", "k".repeat(limits::MAX_URL_LEN));
        assert_eq!(long_query.validate(), Ok(()));

        let invalid: [fn(&mut CallRecord); 9] = [
            |r| r.operation = " ".into(),
            |r| r.service = "a\nb".into(),
            |r| r.url = "ftp://x".into(),
            |r| r.http_status = Some(42),
            |r| r.http_status = None,
            |r| r.ttft_ms = Some(901),
            |r| r.latency_ms = limits::MAX_DURATION_MS + 1,
            |r| r.metadata = Some(BTreeMap::from([("bad key".into(), "v".into())])),
            |r| r.request = "x".repeat(limits::MAX_REQUEST_BYTES + 1),
        ];
        for (index, mutate) in invalid.iter().enumerate() {
            let mut bad = record();
            mutate(&mut bad);
            assert!(bad.validate().is_err(), "case {index} should be invalid");
        }
    }

    #[test]
    fn group_by_round_trips_as_a_comma_list() {
        let query = SummaryQuery { group_by: vec![Dimension::Operation, Dimension::Day], ..SummaryQuery::default() };
        let encoded = serde_json::to_value(&query).unwrap();
        assert_eq!(encoded, serde_json::json!({ "group_by": "operation,day" }));
        let decoded: SummaryQuery = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, query);
        assert!(serde_json::from_value::<SummaryQuery>(serde_json::json!({ "group_by": "day,day" })).is_err());
        assert!(serde_json::from_value::<SummaryQuery>(serde_json::json!({ "group_by": "model" })).is_err());
    }

    #[test]
    fn committed_typescript_bindings_are_current() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../packages/api/src/generated.ts");
        let committed = std::fs::read_to_string(path).unwrap_or_default();
        assert!(
            committed == typescript_bindings(),
            "packages/api/src/generated.ts is stale; run `cargo run -p ai-ledger-api --bin gen-ts`"
        );
    }
}
