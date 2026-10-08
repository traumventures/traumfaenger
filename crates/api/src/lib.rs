//! The ai-ledger wire contract.
//!
//! Every type that crosses the HTTP boundary is defined here once. The server
//! uses these types directly; the TypeScript packages use the declarations that
//! [`typescript_bindings`] generates from them, so both sides share one source
//! of truth.
//!
//! The service exposes two independent HTTP APIs on separate listeners:
//!
//! - **Ingestion** (write-only): `POST /v1/calls` appends AI calls.
//! - **Query** (read-only): list, fetch and summarise recorded calls.
//!
//! Each API has its own bearer token, so an emitter can never read the ledger
//! and a reader can never write to it.

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
    /// Ingestion listener: `POST` a batch of calls.
    pub const INGEST_CALLS: &str = "/v1/calls";
    /// Query listener: `GET` a filtered, paginated list of calls.
    pub const LIST_CALLS: &str = "/v1/calls";
    /// Query listener: `GET` one call by id. `{id}` is the path parameter.
    pub const GET_CALL: &str = "/v1/calls/{id}";
    /// Query listener: `GET` aggregated usage grouped by dimensions.
    pub const SUMMARY: &str = "/v1/summary";
}

/// Limits enforced by the server. Exported to TypeScript as `LIMITS`.
pub mod limits {
    /// Maximum number of calls in one ingestion request.
    pub const MAX_BATCH_SIZE: usize = 1_000;
    /// Maximum request body size of an ingestion request, in bytes.
    pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
    /// Maximum length of `id`, in bytes.
    pub const MAX_ID_LEN: usize = 128;
    /// Maximum length of every other string field except `error`, in bytes.
    pub const MAX_TEXT_LEN: usize = 512;
    /// Maximum length of `error`, in bytes. Longer messages are rejected.
    pub const MAX_ERROR_LEN: usize = 4_000;
    /// Maximum size of `metadata` serialised as JSON, in bytes.
    pub const MAX_METADATA_BYTES: usize = 8 * 1024;
    /// Upper bound for every token count and for `latency_ms`.
    pub const MAX_COUNT: u64 = 1_000_000_000_000;
    /// Upper bound for `attempt`.
    pub const MAX_ATTEMPT: u32 = 1_000;
    /// Upper bound for `cost_usd` of one call, in USD.
    pub const MAX_COST_USD: f64 = 1_000_000.0;
    /// Default and maximum page size of `GET /v1/calls`.
    pub const DEFAULT_LIST_LIMIT: u32 = 100;
    pub const MAX_LIST_LIMIT: u32 = 1_000;
    /// Default and maximum number of rows of `GET /v1/summary`.
    pub const DEFAULT_SUMMARY_LIMIT: u32 = 1_000;
    pub const MAX_SUMMARY_LIMIT: u32 = 10_000;
}

/// Outcome of one AI call.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CallStatus {
    Ok,
    Error,
}

impl CallStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "ok" => Some(Self::Ok),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

/// One AI call as sent to the ingestion API.
///
/// `id` is the idempotency key: re-sending a call with an id that already
/// exists is accepted and counted as a duplicate, and the stored call is left
/// unchanged (first write wins).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
#[serde(deny_unknown_fields)]
pub struct NewAiCall {
    /// Caller-generated unique id, for example a UUID. 1–128 bytes.
    pub id: String,
    /// When the call started, RFC 3339.
    #[ts(type = "string")]
    pub occurred_at: DateTime<Utc>,
    /// What the call was for, for example `listing.parse`.
    pub feature: String,
    /// Kind of the object the call worked on, for example `listing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub entity_type: Option<String>,
    /// Id of the object the call worked on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub entity_id: Option<String>,
    /// Groups calls that belong to one logical operation, for example retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub trace_id: Option<String>,
    /// Who served the call, for example `openrouter`.
    pub provider: String,
    /// Model identifier as reported by the provider.
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub reasoning_effort: Option<String>,
    pub status: CallStatus,
    /// Error message when `status` is `error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub error: Option<String>,
    /// 1-based attempt number within `trace_id`. Defaults to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub attempt: Option<u32>,
    /// Prompt tokens, including cached ones. Defaults to 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "number")]
    pub input_tokens: Option<u64>,
    /// Completion tokens, including reasoning ones. Defaults to 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "number")]
    pub output_tokens: Option<u64>,
    /// Subset of `input_tokens` served from the provider cache. Defaults to 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "number")]
    pub cached_input_tokens: Option<u64>,
    /// Subset of `output_tokens` spent on reasoning. Defaults to 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "number")]
    pub reasoning_tokens: Option<u64>,
    /// Cost billed by the provider in USD. Omit when unknown; never estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub cost_usd: Option<f64>,
    /// Wall-clock duration of the call in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "number")]
    pub latency_ms: Option<u64>,
    /// Free-form JSON object for anything else. At most 8 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "Record<string, unknown>")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

impl NewAiCall {
    /// Checks every field against [`limits`]. Returns a human-readable reason.
    pub fn validate(&self) -> Result<(), String> {
        check_text("id", &self.id, limits::MAX_ID_LEN, true)?;
        check_text("feature", &self.feature, limits::MAX_TEXT_LEN, true)?;
        check_text("provider", &self.provider, limits::MAX_TEXT_LEN, true)?;
        check_text("model", &self.model, limits::MAX_TEXT_LEN, true)?;
        for (name, value) in [
            ("entity_type", &self.entity_type),
            ("entity_id", &self.entity_id),
            ("trace_id", &self.trace_id),
            ("reasoning_effort", &self.reasoning_effort),
        ] {
            if let Some(value) = value {
                check_text(name, value, limits::MAX_TEXT_LEN, true)?;
            }
        }
        if let Some(error) = &self.error {
            check_text("error", error, limits::MAX_ERROR_LEN, false)?;
        }
        if self.attempt.is_some_and(|attempt| !(1..=limits::MAX_ATTEMPT).contains(&attempt)) {
            return Err(format!("attempt must be between 1 and {}", limits::MAX_ATTEMPT));
        }
        for (name, value) in [
            ("input_tokens", self.input_tokens),
            ("output_tokens", self.output_tokens),
            ("cached_input_tokens", self.cached_input_tokens),
            ("reasoning_tokens", self.reasoning_tokens),
            ("latency_ms", self.latency_ms),
        ] {
            if value.is_some_and(|value| value > limits::MAX_COUNT) {
                return Err(format!("{name} must be at most {}", limits::MAX_COUNT));
            }
        }
        if self.cached_input_tokens.unwrap_or(0) > self.input_tokens.unwrap_or(0) {
            return Err("cached_input_tokens must not exceed input_tokens".into());
        }
        if self.reasoning_tokens.unwrap_or(0) > self.output_tokens.unwrap_or(0) {
            return Err("reasoning_tokens must not exceed output_tokens".into());
        }
        if let Some(cost) = self.cost_usd
            && !(0.0..=limits::MAX_COST_USD).contains(&cost)
        {
            return Err(format!("cost_usd must be between 0 and {}", limits::MAX_COST_USD));
        }
        if let Some(metadata) = &self.metadata {
            let size = serde_json::to_vec(metadata).map_or(usize::MAX, |bytes| bytes.len());
            if size > limits::MAX_METADATA_BYTES {
                return Err(format!("metadata must be at most {} bytes as JSON", limits::MAX_METADATA_BYTES));
            }
        }
        Ok(())
    }
}

fn check_text(name: &str, value: &str, max: usize, single_line: bool) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if value.len() > max {
        return Err(format!("{name} must be at most {max} bytes"));
    }
    if value.contains('\0') || (single_line && value.chars().any(char::is_control)) {
        return Err(format!("{name} must not contain control characters"));
    }
    Ok(())
}

/// One AI call as stored by the ledger. Defaults of [`NewAiCall`] are resolved.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct AiCall {
    pub id: String,
    #[ts(type = "string")]
    pub occurred_at: DateTime<Utc>,
    /// When the ledger first accepted the call, RFC 3339.
    #[ts(type = "string")]
    pub received_at: DateTime<Utc>,
    pub feature: String,
    pub entity_type: Option<String>,
    pub entity_id: Option<String>,
    pub trace_id: Option<String>,
    pub provider: String,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub status: CallStatus,
    pub error: Option<String>,
    pub attempt: u32,
    #[ts(type = "number")]
    pub input_tokens: u64,
    #[ts(type = "number")]
    pub output_tokens: u64,
    #[ts(type = "number")]
    pub cached_input_tokens: u64,
    #[ts(type = "number")]
    pub reasoning_tokens: u64,
    pub cost_usd: Option<f64>,
    #[ts(type = "number | null")]
    pub latency_ms: Option<u64>,
    #[ts(type = "Record<string, unknown> | null")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Body of `POST /v1/calls` on the ingestion listener.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
#[serde(deny_unknown_fields)]
pub struct IngestRequest {
    /// 1–1000 calls. The batch is validated as a whole: one invalid call
    /// rejects the request and nothing is stored.
    pub calls: Vec<NewAiCall>,
}

/// Response of `POST /v1/calls`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct IngestResponse {
    /// Calls stored by this request.
    pub accepted: u32,
    /// Calls whose id was already stored, or repeated within the batch.
    pub duplicates: u32,
}

/// Filters shared by `GET /v1/calls` and `GET /v1/summary`. Every filter is
/// optional and they combine with AND. `from` is inclusive, `to` exclusive.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize, TS)]
#[serde(deny_unknown_fields)]
pub struct ListCallsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub feature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub entity_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub entity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub status: Option<CallStatus>,
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

/// Response of `GET /v1/calls`, newest call first.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, TS)]
pub struct ListCallsResponse {
    pub calls: Vec<AiCall>,
    /// Pass as `cursor` to fetch the next page. `null` on the last page.
    pub next_cursor: Option<String>,
}

/// A column `GET /v1/summary` can group by. `day` and `hour` bucket
/// `occurred_at` in UTC.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Dimension {
    Feature,
    EntityType,
    EntityId,
    TraceId,
    Provider,
    Model,
    ReasoningEffort,
    Status,
    Day,
    Hour,
}

impl Dimension {
    pub const ALL: [Self; 10] = [
        Self::Feature,
        Self::EntityType,
        Self::EntityId,
        Self::TraceId,
        Self::Provider,
        Self::Model,
        Self::ReasoningEffort,
        Self::Status,
        Self::Day,
        Self::Hour,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Feature => "feature",
            Self::EntityType => "entity_type",
            Self::EntityId => "entity_id",
            Self::TraceId => "trace_id",
            Self::Provider => "provider",
            Self::Model => "model",
            Self::ReasoningEffort => "reasoning_effort",
            Self::Status => "status",
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
    pub feature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub entity_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub entity_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub status: Option<CallStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "string")]
    pub from: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "string")]
    pub to: Option<DateTime<Utc>>,
    /// Dimensions to group by, at most once each. Sent as a comma-separated
    /// query parameter (`group_by=feature,model`). Empty means one total row.
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
    #[ts(type = "number")]
    pub errors: u64,
    #[ts(type = "number")]
    pub input_tokens: u64,
    #[ts(type = "number")]
    pub output_tokens: u64,
    #[ts(type = "number")]
    pub cached_input_tokens: u64,
    #[ts(type = "number")]
    pub reasoning_tokens: u64,
    /// Sum of the known billed costs.
    pub cost_usd: f64,
    /// Calls without a billed cost; `cost_usd` excludes them.
    #[ts(type = "number")]
    pub calls_without_cost: u64,
    /// Mean and 95th percentile of the known latencies.
    pub latency_ms_avg: Option<f64>,
    pub latency_ms_p95: Option<f64>,
}

/// Response of `GET /v1/summary`, highest `cost_usd` first.
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

    fn call() -> NewAiCall {
        serde_json::from_value(serde_json::json!({
            "id": "c1",
            "occurred_at": "2026-10-08T10:00:00Z",
            "feature": "listing.parse",
            "provider": "openrouter",
            "model": "openai/gpt-5",
            "status": "ok",
            "input_tokens": 100,
            "cached_input_tokens": 40,
            "output_tokens": 20,
        }))
        .unwrap()
    }

    #[test]
    fn accepts_a_minimal_call() {
        assert_eq!(call().validate(), Ok(()));
    }

    #[test]
    fn rejects_unknown_fields() {
        let result = serde_json::from_value::<NewAiCall>(serde_json::json!({
            "id": "c1", "occurred_at": "2026-10-08T10:00:00Z", "feature": "f",
            "provider": "p", "model": "m", "status": "ok", "tokens": 1,
        }));
        assert!(result.is_err());
    }

    #[test]
    fn rejects_inconsistent_counts_and_costs() {
        let mut bad = call();
        bad.cached_input_tokens = Some(101);
        assert!(bad.validate().is_err());

        let mut bad = call();
        bad.cost_usd = Some(-0.1);
        assert!(bad.validate().is_err());

        let mut bad = call();
        bad.cost_usd = Some(f64::NAN);
        assert!(bad.validate().is_err());

        let mut bad = call();
        bad.cost_usd = Some(1e308);
        assert!(bad.validate().is_err());

        let mut bad = call();
        bad.attempt = Some(0);
        assert!(bad.validate().is_err());

        let mut bad = call();
        bad.attempt = Some(3_000_000_000);
        assert!(bad.validate().is_err());

        let mut bad = call();
        bad.feature = " ".into();
        assert!(bad.validate().is_err());

        let mut bad = call();
        bad.entity_id = Some("a\nb".into());
        assert!(bad.validate().is_err());
    }

    #[test]
    fn group_by_round_trips_as_a_comma_list() {
        let query = SummaryQuery { group_by: vec![Dimension::Feature, Dimension::Day], ..SummaryQuery::default() };
        let encoded = serde_json::to_value(&query).unwrap();
        assert_eq!(encoded, serde_json::json!({ "group_by": "feature,day" }));
        let decoded: SummaryQuery = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, query);
        assert!(serde_json::from_value::<SummaryQuery>(serde_json::json!({ "group_by": "feature,feature" })).is_err());
        assert!(serde_json::from_value::<SummaryQuery>(serde_json::json!({ "group_by": "nope" })).is_err());
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
