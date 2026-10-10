//! `GET /v1/analytics/{source}`: metrics over processed data.
//!
//! A fixed vocabulary, not SQL: every dimension and metric maps to a SQL
//! expression defined here, and request values are only ever bound as
//! parameters. Metadata keys are restricted to `[A-Za-z0-9_-]`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use duckdb::{params_from_iter, types::Value};
use traumfaenger_api::AnalyticsResponse;

use crate::store::{Store, StoreError, text};

pub const DEFAULT_LIMIT: usize = 1000;
pub const MAX_LIMIT: usize = 10_000;

struct Source {
    name: &'static str,
    from: &'static str,
    time: &'static str,
    /// Alias of the `calls` row whose metadata the source can use.
    metadata: Option<&'static str>,
    dimensions: &'static [(&'static str, &'static str)],
    metrics: &'static [(&'static str, &'static str)],
}

const CALL_DIMENSIONS: &[(&str, &str)] = &[
    ("call_id", "c.id"),
    ("service", "c.service"),
    ("operation", "c.operation"),
    ("session_id", "c.session_id"),
    ("gateway", "c.gateway"),
    ("provider", "c.provider"),
    ("model", "c.model"),
    ("model_requested", "c.model_requested"),
    ("api", "c.api"),
    ("outcome", "c.outcome"),
    ("finish_reason", "c.finish_reason"),
    ("transport", "c.transport"),
    ("stream", "CAST(c.stream AS VARCHAR)"),
    ("turn_start", "CAST(c.turn_start AS VARCHAR)"),
    ("status", "CAST(c.status AS VARCHAR)"),
    ("cost_source", "c.cost_source"),
    ("parse_status", "c.parse_status"),
    ("parse_reason", "c.parse_reason"),
];

const SOURCES: &[Source] = &[
    Source {
        name: "calls",
        from: "calls c",
        time: "c.started_at",
        metadata: Some("c"),
        dimensions: CALL_DIMENSIONS,
        metrics: &[
            ("calls", "count(*)"),
            ("failed_calls", "count(*) FILTER (WHERE c.outcome <> 'ok')"),
            ("error_rate", "avg(CASE WHEN c.outcome <> 'ok' THEN 1.0 ELSE 0.0 END)"),
            ("rate_limited_calls", "count(*) FILTER (WHERE c.outcome = 'rate_limited')"),
            ("sessions", "count(DISTINCT c.session_id)"),
            ("turns", "count(*) FILTER (WHERE c.turn_start)"),
            ("agent_turns", "count(*) FILTER (WHERE c.turn_start AND c.session_id IS NOT NULL)"),
            ("input_tokens", "sum(c.input_tokens)"),
            ("cached_tokens", "sum(c.cached_tokens)"),
            ("cache_write_tokens", "sum(c.cache_write_tokens)"),
            (
                "uncached_input_tokens",
                "sum(c.input_tokens - coalesce(c.cached_tokens, 0) - coalesce(c.cache_write_tokens, 0))",
            ),
            ("output_tokens", "sum(c.output_tokens)"),
            ("reasoning_tokens", "sum(c.reasoning_tokens)"),
            ("total_tokens", "sum(coalesce(c.input_tokens, 0) + coalesce(c.output_tokens, 0))"),
            ("cache_hit_rate", "sum(c.cached_tokens) / nullif(sum(c.input_tokens), 0)"),
            ("cost_usd", "sum(c.cost_usd)"),
            ("input_cost_usd", "sum(c.input_cost_usd)"),
            ("output_cost_usd", "sum(c.output_cost_usd)"),
            ("avg_cost_usd", "avg(c.cost_usd)"),
            ("unpriced_calls", "count(*) FILTER (WHERE c.cost_usd IS NULL AND c.outcome = 'ok')"),
            ("latency_ms_avg", "avg(c.latency_ms)"),
            ("latency_ms_p50", "quantile_cont(c.latency_ms, 0.5)"),
            ("latency_ms_p95", "quantile_cont(c.latency_ms, 0.95)"),
            ("latency_ms_p99", "quantile_cont(c.latency_ms, 0.99)"),
            ("latency_ms_max", "max(c.latency_ms)"),
            ("ttft_ms_avg", "avg(c.ttft_ms)"),
            ("ttft_ms_p50", "quantile_cont(c.ttft_ms, 0.5)"),
            ("ttft_ms_p95", "quantile_cont(c.ttft_ms, 0.95)"),
            ("ttft_ms_p99", "quantile_cont(c.ttft_ms, 0.99)"),
            (
                "output_tokens_per_second",
                "avg(c.output_tokens * 1000.0 / (c.latency_ms - c.ttft_ms)) FILTER (WHERE c.ttft_ms IS NOT NULL \
                 AND c.latency_ms > c.ttft_ms AND c.output_tokens > 0)",
            ),
            ("tool_calls", "sum(c.tool_calls)"),
            ("request_bytes", "sum(c.request_bytes)"),
            ("response_bytes", "sum(c.response_bytes)"),
            ("first_call", "strftime(min(c.started_at), '%Y-%m-%dT%H:%M:%S.%gZ')"),
            ("last_call", "strftime(max(c.started_at), '%Y-%m-%dT%H:%M:%S.%gZ')"),
        ],
    },
    Source {
        name: "tools",
        from: "tool_calls t",
        time: "t.started_at",
        metadata: None,
        dimensions: &[
            ("tool", "t.tool_name"),
            ("call_id", "t.call_id"),
            ("result_call_id", "t.result_call_id"),
            ("service", "t.service"),
            ("operation", "t.operation"),
            ("session_id", "t.session_id"),
            ("model", "t.model"),
            ("provider", "t.provider"),
            (
                "result",
                "CASE WHEN t.result_call_id IS NULL THEN 'unanswered' WHEN t.is_error THEN 'error' ELSE 'ok' END",
            ),
        ],
        metrics: &[
            ("tool_calls", "count(*)"),
            ("answered", "count(t.result_call_id)"),
            ("failed", "count(*) FILTER (WHERE t.is_error)"),
            (
                "error_rate",
                "avg(CASE WHEN t.is_error THEN 1.0 ELSE 0.0 END) FILTER (WHERE t.result_call_id IS NOT NULL)",
            ),
            ("gap_ms_p50", "quantile_cont(t.gap_ms, 0.5)"),
            ("gap_ms_p95", "quantile_cont(t.gap_ms, 0.95)"),
            ("gap_ms_max", "max(t.gap_ms)"),
            ("arguments_bytes", "sum(t.arguments_bytes)"),
            ("result_bytes", "sum(t.result_bytes)"),
            ("sessions", "count(DISTINCT t.session_id)"),
        ],
    },
    Source {
        name: "parts",
        from: "call_parts p JOIN calls c ON c.id = p.call_id AND c.raw_seq = p.raw_seq",
        time: "c.started_at",
        metadata: Some("c"),
        dimensions: &[
            ("side", "p.side"),
            ("role", "p.role"),
            ("kind", "p.kind"),
            ("tool", "p.tool_name"),
            ("call_id", "c.id"),
            ("service", "c.service"),
            ("operation", "c.operation"),
            ("session_id", "c.session_id"),
            ("gateway", "c.gateway"),
            ("provider", "c.provider"),
            ("model", "c.model"),
            ("outcome", "c.outcome"),
        ],
        metrics: &[
            ("parts", "count(*)"),
            ("bytes", "sum(p.bytes)"),
            ("distinct_contents", "count(DISTINCT p.content)"),
            (
                // Each call's input cost spread over its request parts by size,
                // its output cost over its response parts. An estimate: providers
                // bill per call, not per message.
                "allocated_cost_usd",
                "sum(CASE WHEN p.side = 'request' THEN c.input_cost_usd * p.bytes / nullif(c.request_bytes, 0) \
                 ELSE c.output_cost_usd * p.bytes / nullif(c.response_bytes, 0) END)",
            ),
            (
                "allocated_tokens",
                "sum(CASE WHEN p.side = 'request' THEN c.input_tokens * p.bytes / nullif(c.request_bytes, 0) \
                 ELSE c.output_tokens * p.bytes / nullif(c.response_bytes, 0) END)",
            ),
            ("calls", "count(DISTINCT c.id)"),
        ],
    },
];

/// `interval` as `<number><unit>` with unit `ms`, `s`, `m`, `h`, `d` or `w`,
/// as Grafana's `$__interval` produces. Rounded up to whole seconds, at most
/// 366 days. Returns the bucket width in seconds.
fn interval_seconds(value: &str) -> Result<u64, StoreError> {
    let digits = value.find(|c: char| !c.is_ascii_digit()).unwrap_or(value.len());
    let (number, unit) = value.split_at(digits);
    let number: u64 = number.parse().map_err(|_| invalid("`interval` must look like `30s`, `5m`, `1h` or `1d`"))?;
    let seconds = match unit {
        "ms" => Some(number.div_ceil(1000)),
        "s" => Some(number),
        "m" => number.checked_mul(60),
        "h" => number.checked_mul(3600),
        "d" => number.checked_mul(86_400),
        "w" => number.checked_mul(604_800),
        _ => return Err(invalid("`interval` must look like `30s`, `5m`, `1h` or `1d`")),
    }
    .unwrap_or(u64::MAX);
    if !(1..=366 * 86_400).contains(&seconds.max(1)) || number == 0 {
        return Err(invalid("`interval` must be between 1ms and 366d"));
    }
    Ok(seconds.max(1))
}

const RESERVED: &[&str] = &["metrics", "group_by", "interval", "from", "to", "order", "limit"];

fn invalid(message: impl Into<String>) -> StoreError {
    StoreError::Invalid(message.into())
}

fn names(entries: &[(&str, &str)]) -> String {
    entries.iter().map(|(name, _)| *name).collect::<Vec<_>>().join(", ")
}

/// Joins of `call_metadata`, one per metadata key used, as `m0`, `m1`, ….
#[derive(Default)]
struct MetadataJoins(Vec<String>);

impl MetadataJoins {
    fn alias(&mut self, key: &str) -> String {
        let index = self.0.iter().position(|known| known == key).unwrap_or_else(|| {
            self.0.push(key.to_owned());
            self.0.len() - 1
        });
        format!("m{index}")
    }

    /// Keys are validated to `[A-Za-z0-9_.-]`, so inlining them is safe.
    fn sql(&self, calls: &str) -> String {
        self.0
            .iter()
            .enumerate()
            .map(|(index, key)| {
                format!(
                    " LEFT JOIN call_metadata m{index} ON m{index}.call_id = {calls}.id \
                     AND m{index}.raw_seq = {calls}.raw_seq AND m{index}.key = '{key}'"
                )
            })
            .collect()
    }
}

impl Source {
    /// SQL of a dimension name, including `metadata.<key>`.
    fn dimension(&self, name: &str, joins: &mut MetadataJoins) -> Result<String, StoreError> {
        if let Some(key) = name.strip_prefix("metadata.") {
            if self.metadata.is_none() {
                return Err(invalid(format!("`{}` has no metadata", self.name)));
            }
            if key.is_empty()
                || key.len() > 64
                || !key.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
            {
                return Err(invalid("metadata keys may only contain letters, digits, `_`, `.` and `-`"));
            }
            return Ok(format!("{}.value", joins.alias(key)));
        }
        self.dimensions.iter().find(|(known, _)| *known == name).map(|(_, sql)| (*sql).to_owned()).ok_or_else(|| {
            invalid(format!("unknown dimension `{name}` for `{}`; known: {}", self.name, names(self.dimensions)))
        })
    }

    fn metric(&self, name: &str) -> Result<&'static str, StoreError> {
        self.metrics.iter().find(|(known, _)| *known == name).map(|(_, sql)| *sql).ok_or_else(|| {
            invalid(format!("unknown metric `{name}` for `{}`; known: {}", self.name, names(self.metrics)))
        })
    }
}

fn list(value: &str) -> impl Iterator<Item = &str> {
    value.split(',').map(str::trim).filter(|item| !item.is_empty())
}

fn time_param(name: &str, value: &str) -> Result<DateTime<Utc>, StoreError> {
    // Grafana's `${__from}` is epoch milliseconds; also accept RFC 3339.
    if let Ok(millis) = value.parse::<i64>() {
        return DateTime::from_timestamp_millis(millis).ok_or_else(|| invalid(format!("`{name}` is out of range")));
    }
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| invalid(format!("`{name}` must be RFC 3339 or epoch milliseconds")))
}

impl Store {
    pub fn analytics(&self, source: &str, params: &[(String, String)]) -> Result<AnalyticsResponse, StoreError> {
        let source = SOURCES
            .iter()
            .find(|known| known.name == source)
            .ok_or_else(|| invalid(format!("unknown source `{source}`; known: calls, tools, parts")))?;
        let single = |name: &str| -> Result<Option<&str>, StoreError> {
            let mut values = params.iter().filter(|(key, _)| key == name).map(|(_, value)| value.as_str());
            let first = values.next();
            if values.next().is_some() {
                return Err(invalid(format!("`{name}` given more than once")));
            }
            Ok(first)
        };

        let metric_names: Vec<&str> = single("metrics")?.map(|value| list(value).collect()).unwrap_or_default();
        if metric_names.is_empty() {
            return Err(invalid(format!("`metrics` is required; known: {}", names(source.metrics))));
        }
        let group_names: Vec<&str> = single("group_by")?.map(|value| list(value).collect()).unwrap_or_default();
        let interval = single("interval")?.map(interval_seconds).transpose()?;
        let limit = match single("limit")? {
            None => DEFAULT_LIMIT,
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|limit| (1..=MAX_LIMIT).contains(limit))
                .ok_or_else(|| invalid(format!("`limit` must be between 1 and {MAX_LIMIT}")))?,
        };

        let mut joins = MetadataJoins::default();
        let mut columns: Vec<(String, String)> = Vec::new();
        if let Some(interval) = interval {
            columns.push((
                "time".into(),
                format!("strftime(time_bucket(INTERVAL '{interval} seconds', {}), '%Y-%m-%dT%H:%M:%SZ')", source.time),
            ));
        }
        for name in &group_names {
            if columns.iter().any(|(existing, _)| existing == name) {
                return Err(invalid(format!("`{name}` grouped twice")));
            }
            columns.push(((*name).to_owned(), source.dimension(name, &mut joins)?));
        }
        let grouped = columns.len();
        for name in &metric_names {
            if columns.iter().any(|(existing, _)| existing == name) {
                return Err(invalid(format!("`{name}` requested twice")));
            }
            columns.push(((*name).to_owned(), source.metric(name)?.to_owned()));
        }

        let mut clauses = Vec::new();
        let mut values = Vec::new();
        if let Some(from) = single("from")? {
            clauses.push(format!("{} >= make_timestamp(?)", source.time));
            values.push(Value::BigInt(time_param("from", from)?.timestamp_micros()));
        }
        if let Some(to) = single("to")? {
            clauses.push(format!("{} < make_timestamp(?)", source.time));
            values.push(Value::BigInt(time_param("to", to)?.timestamp_micros()));
        }
        let mut filters: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (key, value) in params {
            if !RESERVED.contains(&key.as_str()) {
                filters.entry(key.as_str()).or_default().extend(list(value));
            }
        }
        for (name, accepted) in filters {
            let sql = source.dimension(name, &mut joins)?;
            if accepted.is_empty() {
                return Err(invalid(format!("filter `{name}` has no value")));
            }
            // `*` accepts every value: what a dashboard variable set to "All" sends.
            if accepted.contains(&"*") {
                continue;
            }
            clauses.push(format!("{sql} IN ({})", vec!["?"; accepted.len()].join(", ")));
            values.extend(accepted.into_iter().map(text));
        }

        let order = match single("order")? {
            Some(name) => {
                let position = columns
                    .iter()
                    .skip(grouped)
                    .position(|(column, _)| column == name)
                    .ok_or_else(|| invalid("`order` must be one of the requested metrics"))?;
                format!("{} DESC NULLS LAST", grouped + position + 1)
            }
            None if interval.is_some() => "1 ASC".into(),
            None => format!("{} DESC NULLS LAST", grouped + 1),
        };
        let positions: Vec<String> = (1..=grouped).map(|position| position.to_string()).collect();
        let mut order_by = vec![order];
        order_by.extend(positions.iter().map(|position| format!("{position} ASC NULLS LAST")));
        let sql = format!(
            "SELECT {} FROM {}{}{}{} ORDER BY {} LIMIT {}",
            columns.iter().map(|(name, sql)| format!("{sql} AS \"{name}\"")).collect::<Vec<_>>().join(", "),
            source.from,
            joins.sql(source.metadata.unwrap_or("c")),
            if clauses.is_empty() { String::new() } else { format!(" WHERE {}", clauses.join(" AND ")) },
            if grouped == 0 { String::new() } else { format!(" GROUP BY {}", positions.join(", ")) },
            order_by.join(", "),
            limit + 1,
        );

        let connection = self.read();
        let mut statement = connection.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(values))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut record = BTreeMap::new();
            for (index, (name, _)) in columns.iter().enumerate() {
                record.insert(name.clone(), json(row.get::<_, Value>(index)?));
            }
            out.push(record);
        }
        let truncated = out.len() > limit;
        out.truncate(limit);
        Ok(AnalyticsResponse { source: source.name.to_owned(), rows: out, truncated })
    }
}

/// A DuckDB value as JSON. Counts above 2^53 lose precision as floats, which
/// no token or byte sum here will reach.
pub(crate) fn json(value: Value) -> serde_json::Value {
    use serde_json::Value as J;
    let float = |number: f64| serde_json::Number::from_f64(number).map_or(J::Null, J::Number);
    match value {
        Value::Null => J::Null,
        Value::Boolean(value) => J::Bool(value),
        Value::TinyInt(value) => J::from(value),
        Value::SmallInt(value) => J::from(value),
        Value::Int(value) => J::from(value),
        Value::BigInt(value) => J::from(value),
        Value::UTinyInt(value) => J::from(value),
        Value::USmallInt(value) => J::from(value),
        Value::UInt(value) => J::from(value),
        Value::UBigInt(value) => J::from(value),
        Value::HugeInt(value) => i64::try_from(value).map_or_else(|_| float(value as f64), J::from),
        Value::Float(value) => float(f64::from(value)),
        Value::Double(value) => float(value),
        Value::Decimal(value) => value.to_string().parse::<f64>().map_or(J::Null, float),
        Value::Text(value) => J::String(value),
        other => J::String(format!("{other:?}")),
    }
}
