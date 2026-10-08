//! DuckDB storage. One table, `ai_calls`, keyed by the caller's call id.
//!
//! DuckDB connections are not `Sync`. The store keeps one writer connection and
//! one reader connection to the same database, each behind its own mutex, so a
//! slow summary never delays ingestion. HTTP handlers call store methods on a
//! blocking thread.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::{Mutex, MutexGuard},
};

use ai_ledger_api::{
    AiCall, CallStatus, Dimension, IngestResponse, ListCallsQuery, ListCallsResponse, NewAiCall, SummaryQuery,
    SummaryResponse, SummaryRow, limits,
};
use chrono::{DateTime, Utc};
use duckdb::{
    Connection, Row, appender_params_from_iter, params_from_iter,
    types::{TimeUnit, Value},
};

/// Ordered schema migrations. Append only; never edit a released entry.
const MIGRATIONS: &[&str] = &["CREATE TABLE ai_calls (
        id VARCHAR PRIMARY KEY,
        occurred_at TIMESTAMP NOT NULL,
        received_at TIMESTAMP NOT NULL,
        feature VARCHAR NOT NULL,
        entity_type VARCHAR,
        entity_id VARCHAR,
        trace_id VARCHAR,
        provider VARCHAR NOT NULL,
        model VARCHAR NOT NULL,
        reasoning_effort VARCHAR,
        status VARCHAR NOT NULL,
        error VARCHAR,
        attempt INTEGER NOT NULL,
        input_tokens BIGINT NOT NULL,
        output_tokens BIGINT NOT NULL,
        cached_input_tokens BIGINT NOT NULL,
        reasoning_tokens BIGINT NOT NULL,
        cost_usd DOUBLE,
        latency_ms BIGINT,
        metadata VARCHAR
    )"];

/// Timestamps are stored as UTC `TIMESTAMP` and exchanged as epoch microseconds.
const CALL_COLUMNS: &str = "id, epoch_us(occurred_at), epoch_us(received_at), feature, entity_type, entity_id, \
     trace_id, provider, model, reasoning_effort, status, error, attempt, input_tokens, output_tokens, \
     cached_input_tokens, reasoning_tokens, cost_usd, latency_ms, metadata";

#[derive(Debug)]
pub enum StoreError {
    /// The request is well-formed but semantically invalid.
    Invalid(String),
    Database(duckdb::Error),
    Corrupt(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::Database(error) => write!(f, "database error: {error}"),
            Self::Corrupt(message) => write!(f, "stored data is corrupt: {message}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<duckdb::Error> for StoreError {
    fn from(error: duckdb::Error) -> Self {
        Self::Database(error)
    }
}

pub struct Store {
    /// Owns the temp staging table, so all writes go through it.
    writer: Mutex<Connection>,
    reader: Mutex<Connection>,
}

impl Store {
    /// Opens (or creates) the database and applies pending migrations.
    pub fn open(path: &str, memory_limit: Option<&str>) -> Result<Self, StoreError> {
        if let Some(limit) = memory_limit
            && !is_size(limit)
        {
            return Err(StoreError::Invalid(format!("invalid memory limit `{limit}`, expected e.g. `512MB`")));
        }
        let connection = if path == ":memory:" { Connection::open_in_memory()? } else { Connection::open(path)? };
        // Never download extensions at runtime: the service must start offline.
        // Nothing here needs one (plain TIMESTAMP columns, no ICU time zones).
        connection.execute_batch("SET autoinstall_known_extensions = false; SET autoload_known_extensions = false;")?;
        if let Some(limit) = memory_limit {
            // SET takes no bind parameters; `is_size` restricts the value to `<digits><unit>`.
            connection.execute_batch(&format!("SET memory_limit = '{limit}'"))?;
        }
        migrate(&connection)?;
        // Per-connection staging table for bulk inserts; see `insert`.
        connection.execute_batch("CREATE TEMP TABLE ai_calls_staging AS SELECT * FROM ai_calls LIMIT 0")?;
        let reader = connection.try_clone()?;
        Ok(Self { writer: Mutex::new(connection), reader: Mutex::new(reader) })
    }

    fn write(&self) -> MutexGuard<'_, Connection> {
        lock(&self.writer)
    }

    fn read(&self) -> MutexGuard<'_, Connection> {
        lock(&self.reader)
    }

    /// Flushes the WAL into the database file.
    pub fn checkpoint(&self) -> Result<(), StoreError> {
        self.write().execute_batch("CHECKPOINT")?;
        Ok(())
    }

    /// Stores validated calls in one transaction. Ids that already exist, or
    /// repeat within the batch, are counted as duplicates and left unchanged.
    ///
    /// Rows go through DuckDB's appender into a temporary staging table and
    /// then into `ai_calls` with one `INSERT ... SELECT`; row-by-row inserts
    /// against the primary-key index are orders of magnitude slower.
    pub fn insert(&self, calls: &[NewAiCall], received_at: DateTime<Utc>) -> Result<IngestResponse, StoreError> {
        let mut seen = HashSet::with_capacity(calls.len());
        let unique: Vec<&NewAiCall> = calls.iter().filter(|call| seen.insert(call.id.as_str())).collect();

        let mut connection = self.write();
        let transaction = connection.transaction()?;
        transaction.execute_batch("DELETE FROM temp.ai_calls_staging")?;
        {
            let mut appender = transaction.appender_to_catalog_and_db("ai_calls_staging", "temp", "main")?;
            for call in &unique {
                appender.append_row(appender_params_from_iter(row_values(call, received_at)))?;
            }
            appender.flush()?;
        }
        let inserted = transaction
            .execute("INSERT INTO ai_calls SELECT * FROM temp.ai_calls_staging ON CONFLICT (id) DO NOTHING", [])?;
        transaction.execute_batch("DELETE FROM temp.ai_calls_staging")?;
        transaction.commit()?;

        // Batches are capped at `limits::MAX_BATCH_SIZE`, so both counts fit in u32.
        let accepted = u32::try_from(inserted).unwrap_or(u32::MAX);
        let duplicates = u32::try_from(calls.len().saturating_sub(inserted)).unwrap_or(u32::MAX);
        Ok(IngestResponse { accepted, duplicates })
    }

    pub fn get(&self, id: &str) -> Result<Option<AiCall>, StoreError> {
        let connection = self.read();
        let mut statement = connection.prepare(&format!("SELECT {CALL_COLUMNS} FROM ai_calls WHERE id = ?"))?;
        let mut rows = statement.query([id])?;
        match rows.next()? {
            Some(row) => Ok(Some(read_call(row)?)),
            None => Ok(None),
        }
    }

    /// Newest first, keyset-paginated on `(occurred_at, id)`.
    pub fn list(&self, query: &ListCallsQuery) -> Result<ListCallsResponse, StoreError> {
        let limit = page_size(query.limit, limits::DEFAULT_LIST_LIMIT, limits::MAX_LIST_LIMIT)?;
        let mut filter = Filter::new(&Filters::from_list(query))?;
        if let Some(cursor) = &query.cursor {
            let (occurred_at_us, id) = decode_cursor(cursor)?;
            filter
                .clauses
                .push("(occurred_at < make_timestamp(?) OR (occurred_at = make_timestamp(?) AND id < ?))".into());
            filter.params.extend([Value::BigInt(occurred_at_us), Value::BigInt(occurred_at_us), text(&id)]);
        }
        filter.params.push(Value::BigInt(i64::from(limit) + 1));

        let connection = self.read();
        let mut statement = connection.prepare(&format!(
            "SELECT {CALL_COLUMNS} FROM ai_calls{} ORDER BY occurred_at DESC, id DESC LIMIT ?",
            filter.where_sql()
        ))?;
        let mut rows = statement.query(params_from_iter(filter.params))?;
        let mut calls = Vec::new();
        while let Some(row) = rows.next()? {
            calls.push(read_call(row)?);
        }
        let next_cursor = if calls.len() > limit as usize {
            calls.truncate(limit as usize);
            calls.last().map(encode_cursor)
        } else {
            None
        };
        Ok(ListCallsResponse { calls, next_cursor })
    }

    /// Aggregates by the requested dimensions, highest cost first.
    pub fn summary(&self, query: &SummaryQuery) -> Result<SummaryResponse, StoreError> {
        let limit = page_size(query.limit, limits::DEFAULT_SUMMARY_LIMIT, limits::MAX_SUMMARY_LIMIT)?;
        let mut filter = Filter::new(&Filters::from_summary(query))?;
        filter.params.push(Value::BigInt(i64::from(limit) + 1));

        let dimensions = &query.group_by;
        let mut select: Vec<String> = dimensions.iter().map(|dimension| dimension_sql(*dimension).to_owned()).collect();
        select.extend(
            [
                "count(*)",
                "count(*) FILTER (WHERE status = 'error')",
                "CAST(coalesce(sum(input_tokens), 0) AS BIGINT)",
                "CAST(coalesce(sum(output_tokens), 0) AS BIGINT)",
                "CAST(coalesce(sum(cached_input_tokens), 0) AS BIGINT)",
                "CAST(coalesce(sum(reasoning_tokens), 0) AS BIGINT)",
                "CAST(coalesce(sum(cost_usd), 0) AS DOUBLE)",
                "count(*) FILTER (WHERE cost_usd IS NULL)",
                "CAST(avg(latency_ms) AS DOUBLE)",
                "CAST(quantile_cont(latency_ms, 0.95) AS DOUBLE)",
            ]
            .map(str::to_owned),
        );
        // ORDER BY and GROUP BY reference select positions; dimensions come first.
        let positions: Vec<String> = (1..=dimensions.len()).map(|position| position.to_string()).collect();
        let group_by =
            if dimensions.is_empty() { String::new() } else { format!(" GROUP BY {}", positions.join(", ")) };
        let mut order = vec![format!("{} DESC", dimensions.len() + 7), format!("{} DESC", dimensions.len() + 1)];
        order.extend(positions.iter().map(|position| format!("{position} ASC NULLS LAST")));
        let sql = format!(
            "SELECT {} FROM ai_calls{}{group_by} ORDER BY {} LIMIT ?",
            select.join(", "),
            filter.where_sql(),
            order.join(", ")
        );

        let connection = self.read();
        let mut statement = connection.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(filter.params))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut group = BTreeMap::new();
            for (index, dimension) in dimensions.iter().enumerate() {
                group.insert(*dimension, row.get::<_, Option<String>>(index)?);
            }
            let base = dimensions.len();
            let unsigned = |offset: usize| -> Result<u64, StoreError> {
                let value: i64 = row.get(base + offset)?;
                u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("negative aggregate {value}")))
            };
            out.push(SummaryRow {
                group,
                calls: unsigned(0)?,
                errors: unsigned(1)?,
                input_tokens: unsigned(2)?,
                output_tokens: unsigned(3)?,
                cached_input_tokens: unsigned(4)?,
                reasoning_tokens: unsigned(5)?,
                cost_usd: row.get(base + 6)?,
                calls_without_cost: unsigned(7)?,
                latency_ms_avg: row.get(base + 8)?,
                latency_ms_p95: row.get(base + 9)?,
            });
        }
        let truncated = out.len() > limit as usize;
        out.truncate(limit as usize);
        Ok(SummaryResponse { rows: out, truncated })
    }
}

fn lock(mutex: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    // Writes are transactional, so a panic while holding the lock cannot leave
    // a half-applied batch. Keep serving instead of failing forever.
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn migrate(connection: &Connection) -> Result<(), StoreError> {
    connection.execute_batch("CREATE TABLE IF NOT EXISTS ai_ledger_migrations (version INTEGER PRIMARY KEY)")?;
    let applied: i64 =
        connection.query_row("SELECT coalesce(max(version), 0) FROM ai_ledger_migrations", [], |row| row.get(0))?;
    let applied = usize::try_from(applied).unwrap_or(usize::MAX);
    if applied > MIGRATIONS.len() {
        return Err(StoreError::Corrupt(format!(
            "database is at schema version {applied} but this server knows {}; refusing to run an older binary",
            MIGRATIONS.len()
        )));
    }
    for (index, sql) in MIGRATIONS.iter().enumerate().skip(applied) {
        connection.execute_batch(&format!(
            "BEGIN TRANSACTION; {sql}; INSERT INTO ai_ledger_migrations VALUES ({}); COMMIT;",
            index + 1
        ))?;
    }
    Ok(())
}

/// `<digits>[.<digits>]<unit>` with an optional space, e.g. `512MB` or `1.5 GiB`.
fn is_size(value: &str) -> bool {
    let digits_end = value.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(value.len());
    let (number, unit) = value.split_at(digits_end);
    let unit = unit.trim_start();
    number.parse::<f64>().is_ok_and(|n| n > 0.0)
        && (1..=3).contains(&unit.len())
        && unit.chars().all(|c| c.is_ascii_alphabetic())
}

/// The filter fields shared by list and summary queries.
struct Filters<'a> {
    /// Column name and value. Column names are fixed here, never from a request.
    text: [(&'static str, Option<&'a str>); 6],
    status: Option<CallStatus>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

macro_rules! filters_from {
    ($query:expr) => {
        Filters {
            text: [
                ("feature", $query.feature.as_deref()),
                ("entity_type", $query.entity_type.as_deref()),
                ("entity_id", $query.entity_id.as_deref()),
                ("trace_id", $query.trace_id.as_deref()),
                ("provider", $query.provider.as_deref()),
                ("model", $query.model.as_deref()),
            ],
            status: $query.status,
            from: $query.from,
            to: $query.to,
        }
    };
}

impl<'a> Filters<'a> {
    fn from_list(query: &'a ListCallsQuery) -> Self {
        filters_from!(query)
    }

    fn from_summary(query: &'a SummaryQuery) -> Self {
        filters_from!(query)
    }
}

struct Filter {
    clauses: Vec<String>,
    params: Vec<Value>,
}

impl Filter {
    fn new(filters: &Filters<'_>) -> Result<Self, StoreError> {
        if let (Some(from), Some(to)) = (filters.from, filters.to)
            && from >= to
        {
            return Err(StoreError::Invalid("`from` must be before `to`".into()));
        }
        let mut filter = Self { clauses: Vec::new(), params: Vec::new() };
        for (column, value) in filters.text {
            if let Some(value) = value {
                filter.clauses.push(format!("{column} = ?"));
                filter.params.push(text(value));
            }
        }
        if let Some(status) = filters.status {
            filter.clauses.push("status = ?".into());
            filter.params.push(text(status.as_str()));
        }
        if let Some(from) = filters.from {
            filter.clauses.push("occurred_at >= make_timestamp(?)".into());
            filter.params.push(Value::BigInt(from.timestamp_micros()));
        }
        if let Some(to) = filters.to {
            filter.clauses.push("occurred_at < make_timestamp(?)".into());
            filter.params.push(Value::BigInt(to.timestamp_micros()));
        }
        Ok(filter)
    }

    fn where_sql(&self) -> String {
        if self.clauses.is_empty() { String::new() } else { format!(" WHERE {}", self.clauses.join(" AND ")) }
    }
}

fn dimension_sql(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::Feature => "feature",
        Dimension::EntityType => "entity_type",
        Dimension::EntityId => "entity_id",
        Dimension::TraceId => "trace_id",
        Dimension::Provider => "provider",
        Dimension::Model => "model",
        Dimension::ReasoningEffort => "reasoning_effort",
        Dimension::Status => "status",
        Dimension::Day => "strftime(occurred_at, '%Y-%m-%d')",
        Dimension::Hour => "strftime(occurred_at, '%Y-%m-%dT%H:00:00Z')",
    }
}

fn page_size(requested: Option<u32>, default: u32, max: u32) -> Result<u32, StoreError> {
    match requested {
        None => Ok(default),
        Some(limit) if (1..=max).contains(&limit) => Ok(limit),
        Some(_) => Err(StoreError::Invalid(format!("`limit` must be between 1 and {max}"))),
    }
}

/// Cursor format: `<occurred_at as epoch microseconds>.<id>`. Opaque to clients.
fn encode_cursor(call: &AiCall) -> String {
    format!("{}.{}", call.occurred_at.timestamp_micros(), call.id)
}

fn decode_cursor(cursor: &str) -> Result<(i64, String), StoreError> {
    let invalid = || StoreError::Invalid("invalid `cursor`".into());
    let (micros, id) = cursor.split_once('.').ok_or_else(invalid)?;
    let micros = micros.parse().map_err(|_| invalid())?;
    if id.is_empty() {
        return Err(invalid());
    }
    Ok((micros, id.to_owned()))
}

/// Column values of one call, in `ai_calls` column order.
fn row_values(call: &NewAiCall, received_at: DateTime<Utc>) -> [Value; 20] {
    let metadata = call.metadata.as_ref().map(|map| serde_json::Value::Object(map.clone()).to_string());
    [
        text(&call.id),
        Value::Timestamp(TimeUnit::Microsecond, call.occurred_at.timestamp_micros()),
        Value::Timestamp(TimeUnit::Microsecond, received_at.timestamp_micros()),
        text(&call.feature),
        optional_text(call.entity_type.as_deref()),
        optional_text(call.entity_id.as_deref()),
        optional_text(call.trace_id.as_deref()),
        text(&call.provider),
        text(&call.model),
        optional_text(call.reasoning_effort.as_deref()),
        text(call.status.as_str()),
        optional_text(call.error.as_deref()),
        Value::Int(i32::try_from(call.attempt.unwrap_or(1)).unwrap_or(i32::MAX)),
        count(call.input_tokens.unwrap_or(0)),
        count(call.output_tokens.unwrap_or(0)),
        count(call.cached_input_tokens.unwrap_or(0)),
        count(call.reasoning_tokens.unwrap_or(0)),
        call.cost_usd.map_or(Value::Null, Value::Double),
        call.latency_ms.map_or(Value::Null, count),
        metadata.map_or(Value::Null, Value::Text),
    ]
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn optional_text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, text)
}

/// Counts are validated against `limits::MAX_COUNT`, far below `i64::MAX`.
fn count(value: u64) -> Value {
    Value::BigInt(i64::try_from(value).unwrap_or(i64::MAX))
}

fn read_call(row: &Row<'_>) -> Result<AiCall, StoreError> {
    let timestamp = |index: usize| -> Result<DateTime<Utc>, StoreError> {
        let micros: i64 = row.get(index)?;
        DateTime::from_timestamp_micros(micros).ok_or_else(|| StoreError::Corrupt(format!("timestamp {micros}")))
    };
    let unsigned = |index: usize| -> Result<u64, StoreError> {
        let value: i64 = row.get(index)?;
        u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("negative count {value}")))
    };
    let status: String = row.get(10)?;
    let attempt: i32 = row.get(12)?;
    let latency_ms: Option<i64> = row.get(18)?;
    let metadata: Option<String> = row.get(19)?;
    Ok(AiCall {
        id: row.get(0)?,
        occurred_at: timestamp(1)?,
        received_at: timestamp(2)?,
        feature: row.get(3)?,
        entity_type: row.get(4)?,
        entity_id: row.get(5)?,
        trace_id: row.get(6)?,
        provider: row.get(7)?,
        model: row.get(8)?,
        reasoning_effort: row.get(9)?,
        status: CallStatus::parse(&status).ok_or_else(|| StoreError::Corrupt(format!("status `{status}`")))?,
        error: row.get(11)?,
        attempt: u32::try_from(attempt).map_err(|_| StoreError::Corrupt(format!("attempt {attempt}")))?,
        input_tokens: unsigned(13)?,
        output_tokens: unsigned(14)?,
        cached_input_tokens: unsigned(15)?,
        reasoning_tokens: unsigned(16)?,
        cost_usd: row.get(17)?,
        latency_ms: latency_ms
            .map(|value| u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("latency {value}"))))
            .transpose()?,
        metadata: metadata
            .map(|json| match serde_json::from_str(&json) {
                Ok(serde_json::Value::Object(map)) => Ok(map),
                _ => Err(StoreError::Corrupt("metadata is not a JSON object".into())),
            })
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Store {
        Store::open(":memory:", None).unwrap()
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().with_timezone(&Utc)
    }

    fn call(id: &str, occurred_at: &str, feature: &str, model: &str, cost: Option<f64>) -> NewAiCall {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "occurred_at": occurred_at,
            "feature": feature,
            "provider": "openrouter",
            "model": model,
            "status": "ok",
            "input_tokens": 100,
            "cached_input_tokens": 10,
            "output_tokens": 50,
            "reasoning_tokens": 5,
            "cost_usd": cost,
            "latency_ms": 200,
            "entity_type": "listing",
            "entity_id": "123",
            "metadata": { "prompt_version": 3 },
        }))
        .unwrap()
    }

    #[test]
    fn round_trips_a_call_and_resolves_defaults() {
        let store = store();
        let received = at("2026-10-08T12:00:00.123456Z");
        let mut minimal = call("a", "2026-10-08T10:00:00.5Z", "parse", "m1", Some(0.002));
        minimal.input_tokens = None;
        minimal.cached_input_tokens = None;
        minimal.attempt = None;
        minimal.latency_ms = None;
        minimal.metadata = None;
        store.insert(&[minimal], received).unwrap();

        let stored = store.get("a").unwrap().unwrap();
        assert_eq!(stored.occurred_at, at("2026-10-08T10:00:00.5Z"));
        assert_eq!(stored.received_at, received);
        assert_eq!((stored.attempt, stored.input_tokens, stored.cached_input_tokens), (1, 0, 0));
        assert_eq!(stored.output_tokens, 50);
        assert_eq!(stored.latency_ms, None);
        assert_eq!(stored.metadata, None);
        assert_eq!(stored.cost_usd, Some(0.002));
        assert_eq!(store.get("missing").unwrap(), None);

        store.insert(&[call("b", "2026-10-08T10:00:00Z", "parse", "m1", None)], received).unwrap();
        let stored = store.get("b").unwrap().unwrap();
        assert_eq!(stored.metadata.unwrap()["prompt_version"], 3);
        assert_eq!(stored.latency_ms, Some(200));
        assert_eq!(stored.cost_usd, None);
    }

    #[test]
    fn duplicate_ids_are_counted_and_first_write_wins() {
        let store = store();
        let now = Utc::now();
        let first = call("a", "2026-10-08T10:00:00Z", "parse", "m1", Some(1.0));
        let changed = call("a", "2026-10-08T10:00:00Z", "parse", "m2", Some(9.0));
        let result = store.insert(&[first, changed.clone()], now).unwrap();
        assert_eq!((result.accepted, result.duplicates), (1, 1));
        let result = store.insert(&[changed, call("b", "2026-10-08T10:00:00Z", "parse", "m1", None)], now).unwrap();
        assert_eq!((result.accepted, result.duplicates), (1, 1));
        assert_eq!(store.get("a").unwrap().unwrap().model, "m1");
    }

    #[test]
    fn lists_newest_first_with_filters_and_stable_cursor() {
        let store = store();
        let calls: Vec<NewAiCall> = (0..5)
            .map(|index| call(&format!("c{index}"), "2026-10-08T10:00:00Z", "parse", "m1", None))
            .chain([call("late", "2026-10-08T11:00:00Z", "greet", "m1", None)])
            .collect();
        store.insert(&calls, Utc::now()).unwrap();

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                store.list(&ListCallsQuery { limit: Some(2), cursor: cursor.clone(), ..Default::default() }).unwrap();
            seen.extend(page.calls.into_iter().map(|call| call.id));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, ["late", "c4", "c3", "c2", "c1", "c0"]);

        let only_parse = store.list(&ListCallsQuery { feature: Some("parse".into()), ..Default::default() }).unwrap();
        assert_eq!(only_parse.calls.len(), 5);
        let window = store
            .list(&ListCallsQuery {
                from: Some(at("2026-10-08T10:30:00Z")),
                to: Some(at("2026-10-08T12:00:00Z")),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(window.calls.len(), 1);

        let invalid = |query: ListCallsQuery| matches!(store.list(&query), Err(StoreError::Invalid(_)));
        assert!(invalid(ListCallsQuery { cursor: Some("garbage".into()), ..Default::default() }));
        assert!(invalid(ListCallsQuery { limit: Some(0), ..Default::default() }));
        assert!(invalid(ListCallsQuery {
            from: Some(at("2026-10-09T00:00:00Z")),
            to: Some(at("2026-10-08T00:00:00Z")),
            ..Default::default()
        }));
    }

    #[test]
    fn summarises_by_dimensions_with_unknown_costs_kept_separate() {
        let store = store();
        let mut failed = call("e", "2026-10-09T09:00:00Z", "parse", "m2", None);
        failed.status = CallStatus::Error;
        store
            .insert(
                &[
                    call("a", "2026-10-08T10:00:00Z", "parse", "m1", Some(0.25)),
                    call("b", "2026-10-08T23:00:00Z", "parse", "m1", Some(0.5)),
                    call("c", "2026-10-08T10:00:00Z", "greet", "m1", Some(0.1)),
                    failed,
                ],
                Utc::now(),
            )
            .unwrap();

        let total = store.summary(&SummaryQuery::default()).unwrap();
        assert_eq!(total.rows.len(), 1);
        let row = &total.rows[0];
        assert_eq!((row.calls, row.errors, row.calls_without_cost), (4, 1, 1));
        assert_eq!(
            (row.input_tokens, row.cached_input_tokens, row.output_tokens, row.reasoning_tokens),
            (400, 40, 200, 20)
        );
        assert!((row.cost_usd - 0.85).abs() < 1e-9);
        assert_eq!(row.latency_ms_avg, Some(200.0));
        assert!(row.group.is_empty());

        let by_feature_day = store
            .summary(&SummaryQuery { group_by: vec![Dimension::Feature, Dimension::Day], ..Default::default() })
            .unwrap();
        let keys: Vec<(Option<String>, Option<String>, u64)> = by_feature_day
            .rows
            .iter()
            .map(|row| (row.group[&Dimension::Feature].clone(), row.group[&Dimension::Day].clone(), row.calls))
            .collect();
        assert_eq!(
            keys,
            [
                (Some("parse".into()), Some("2026-10-08".into()), 2),
                (Some("greet".into()), Some("2026-10-08".into()), 1),
                (Some("parse".into()), Some("2026-10-09".into()), 1),
            ]
        );
        assert!(!by_feature_day.truncated);

        let top_hour = store
            .summary(&SummaryQuery { group_by: vec![Dimension::Hour], limit: Some(1), ..Default::default() })
            .unwrap();
        assert_eq!(top_hour.rows.len(), 1);
        assert!(top_hour.truncated);
        assert_eq!(top_hour.rows[0].group[&Dimension::Hour].as_deref(), Some("2026-10-08T23:00:00Z"));

        let empty = store.summary(&SummaryQuery { feature: Some("none".into()), ..Default::default() }).unwrap();
        assert_eq!(empty.rows[0].calls, 0);
        assert_eq!(empty.rows[0].latency_ms_avg, None);
    }

    #[test]
    fn reopening_a_file_keeps_data_and_does_not_reapply_migrations() {
        let dir = std::env::temp_dir().join(format!("ai-ledger-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.duckdb");
        let path = path.to_str().unwrap();
        {
            let store = Store::open(path, Some("256MB")).unwrap();
            store.insert(&[call("a", "2026-10-08T10:00:00Z", "parse", "m1", None)], Utc::now()).unwrap();
            store.checkpoint().unwrap();
        }
        let store = Store::open(path, None).unwrap();
        assert!(store.get("a").unwrap().is_some());
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn never_autoloads_extensions() {
        let store = store();
        let autoload: bool = store
            .read()
            .query_row("SELECT current_setting('autoload_known_extensions')", [], |row| row.get(0))
            .unwrap();
        assert!(!autoload);
    }

    #[test]
    fn rejects_memory_limits_that_are_not_sizes() {
        assert!(is_size("512MB") && is_size("1.5 GiB") && is_size("2GB"));
        assert!(!is_size("") && !is_size("MB") && !is_size("1GB'; DROP TABLE x; --") && !is_size("0MB"));
        assert!(Store::open(":memory:", Some("1GB'; --")).is_err());
    }
}
