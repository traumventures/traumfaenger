//! DuckDB storage, split CQRS-style.
//!
//! - **Write side:** `raw_calls` is an append-only log of the records exactly
//!   as clients sent them. Ingestion only appends; it never parses.
//! - **Read side:** projections (added later) each read the log in `seq`
//!   order, maintain their own tables (calls, messages, tool calls, metrics per
//!   time bucket) and record their position in `projection_checkpoints`. A
//!   projection is rebuilt by resetting its checkpoint.
//!
//! `seq` order equals commit order: there is one writer connection and every
//! batch is one transaction. A reader of `seq > last_seq` never skips a record.
//! This holds only while every write to `raw_calls` goes through `Store::write`;
//! never open a second writing connection.
//!
//! DuckDB connections are not `Sync`. The store keeps one writer and one
//! reader connection to the same database, each behind its own mutex, so a
//! slow query never delays ingestion. HTTP handlers call store methods on a
//! blocking thread.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::{Mutex, MutexGuard},
};

use ai_ledger_api::{
    CallEnvelope, CallRecord, Dimension, ListCallsQuery, ListCallsResponse, RawResponse, StoredCall, SummaryQuery,
    SummaryResponse, SummaryRow, limits,
};
use chrono::{DateTime, Utc};
use duckdb::{
    Connection, Row, appender_params_from_iter, params_from_iter,
    types::{TimeUnit, Value},
};

/// Ordered schema migrations. Append only; never edit a released entry.
const MIGRATIONS: &[&str] = &["CREATE SEQUENCE raw_calls_seq START 1;
    CREATE TABLE raw_calls (
        seq BIGINT NOT NULL DEFAULT nextval('raw_calls_seq'),
        id VARCHAR PRIMARY KEY,
        received_at TIMESTAMP NOT NULL,
        service VARCHAR NOT NULL,
        environment VARCHAR NOT NULL,
        operation VARCHAR NOT NULL,
        session_id VARCHAR,
        metadata VARCHAR NOT NULL,
        url VARCHAR NOT NULL,
        request VARCHAR NOT NULL,
        response VARCHAR,
        response_is_stream BOOLEAN,
        http_status INTEGER,
        error VARCHAR,
        truncated BOOLEAN NOT NULL,
        started_at TIMESTAMP NOT NULL,
        ttft_ms BIGINT,
        latency_ms BIGINT NOT NULL,
        request_bytes BIGINT NOT NULL,
        response_bytes BIGINT NOT NULL
    );
    CREATE TABLE projection_checkpoints (
        projection VARCHAR PRIMARY KEY,
        last_seq BIGINT NOT NULL
    )"];

/// Columns written on ingestion, in this order. `seq` takes its default.
const INSERT_COLUMNS: &str = "id, received_at, service, environment, operation, session_id, metadata, url, \
     request, response, response_is_stream, http_status, error, truncated, started_at, ttft_ms, latency_ms, \
     request_bytes, response_bytes";

/// Envelope columns. Timestamps are exchanged as epoch microseconds.
const ENVELOPE_COLUMNS: &str = "seq, id, epoch_us(received_at), service, environment, operation, session_id, \
     metadata, url, http_status, error, truncated, epoch_us(started_at), ttft_ms, latency_ms, request_bytes, \
     response_bytes";

/// A call failed when it got no response, a status of 400 or more, or a
/// client-side error.
const FAILED_SQL: &str = "(error IS NOT NULL OR http_status IS NULL OR http_status >= 400)";

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

/// Result of storing a batch of validated records.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Inserted {
    pub accepted: usize,
    pub duplicates: usize,
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
            // SET takes no bind parameters; `is_size` restricts the value to `<number><unit>`.
            connection.execute_batch(&format!("SET memory_limit = '{limit}'"))?;
        }
        migrate(&connection)?;
        // Per-connection staging table for bulk inserts; see `insert`.
        connection.execute_batch(&format!(
            "CREATE TEMP TABLE raw_calls_staging AS SELECT {INSERT_COLUMNS} FROM raw_calls LIMIT 0"
        ))?;
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

    /// Stores validated records in one transaction. Ids that already exist, or
    /// repeat within the batch, are counted as duplicates and left unchanged.
    ///
    /// Rows go through DuckDB's appender into a temporary staging table and
    /// then into `raw_calls` with one `INSERT ... SELECT`; row-by-row inserts
    /// against the primary-key index are orders of magnitude slower.
    pub fn insert(&self, records: &[CallRecord], received_at: DateTime<Utc>) -> Result<Inserted, StoreError> {
        let mut seen = HashSet::with_capacity(records.len());
        let unique: Vec<&CallRecord> = records.iter().filter(|record| seen.insert(record.id.as_str())).collect();

        let mut connection = self.write();
        let transaction = connection.transaction()?;
        transaction.execute_batch("DELETE FROM temp.raw_calls_staging")?;
        {
            let mut appender = transaction.appender_to_catalog_and_db("raw_calls_staging", "temp", "main")?;
            for record in &unique {
                appender.append_row(appender_params_from_iter(row_values(record, received_at)))?;
            }
            appender.flush()?;
        }
        let accepted = transaction.execute(
            &format!(
                "INSERT INTO raw_calls ({INSERT_COLUMNS}) SELECT {INSERT_COLUMNS} FROM temp.raw_calls_staging \
                 ON CONFLICT (id) DO NOTHING"
            ),
            [],
        )?;
        transaction.execute_batch("DELETE FROM temp.raw_calls_staging")?;
        transaction.commit()?;
        Ok(Inserted { accepted, duplicates: records.len() - accepted })
    }

    /// One stored call including request and response.
    pub fn get(&self, id: &str) -> Result<Option<StoredCall>, StoreError> {
        let connection = self.read();
        let mut statement = connection.prepare(&format!(
            "SELECT {ENVELOPE_COLUMNS}, request, response, response_is_stream FROM raw_calls WHERE id = ?"
        ))?;
        let mut rows = statement.query([id])?;
        let Some(row) = rows.next()? else { return Ok(None) };
        let envelope = read_envelope(row)?;
        let base = ENVELOPE_LEN;
        let request: String = row.get(base)?;
        let response: Option<String> = row.get(base + 1)?;
        let is_stream: Option<bool> = row.get(base + 2)?;
        let response = match (response, is_stream) {
            (None, _) => None,
            (Some(body), Some(false) | None) => Some(RawResponse::Body(body)),
            (Some(events), Some(true)) => Some(RawResponse::Events(
                serde_json::from_str(&events).map_err(|_| StoreError::Corrupt("response events".into()))?,
            )),
        };
        Ok(Some(StoredCall { envelope, request, response }))
    }

    /// Envelopes, most recently stored first, keyset-paginated on `seq`.
    pub fn list(&self, query: &ListCallsQuery) -> Result<ListCallsResponse, StoreError> {
        let limit = page_size(query.limit, limits::DEFAULT_LIST_LIMIT, limits::MAX_LIST_LIMIT)?;
        let mut filter = Filter::new(&filters_from!(query))?;
        if let Some(cursor) = &query.cursor {
            let seq: i64 = cursor.parse().map_err(|_| StoreError::Invalid("invalid `cursor`".into()))?;
            filter.clauses.push("seq < ?".into());
            filter.params.push(Value::BigInt(seq));
        }
        filter.params.push(Value::BigInt(i64::from(limit) + 1));

        let connection = self.read();
        let mut statement = connection.prepare(&format!(
            "SELECT {ENVELOPE_COLUMNS} FROM raw_calls{} ORDER BY seq DESC LIMIT ?",
            filter.where_sql()
        ))?;
        let mut rows = statement.query(params_from_iter(filter.params))?;
        let mut calls = Vec::new();
        while let Some(row) = rows.next()? {
            calls.push(read_envelope(row)?);
        }
        let next_cursor = if calls.len() > limit as usize {
            calls.truncate(limit as usize);
            calls.last().map(|call| call.seq.to_string())
        } else {
            None
        };
        Ok(ListCallsResponse { calls, next_cursor })
    }

    /// Collection statistics grouped by the requested dimensions, most calls first.
    pub fn summary(&self, query: &SummaryQuery) -> Result<SummaryResponse, StoreError> {
        let limit = page_size(query.limit, limits::DEFAULT_SUMMARY_LIMIT, limits::MAX_SUMMARY_LIMIT)?;
        let mut filter = Filter::new(&filters_from!(query))?;
        filter.params.push(Value::BigInt(i64::from(limit) + 1));

        let dimensions = &query.group_by;
        let mut select: Vec<String> = dimensions.iter().map(|dimension| dimension_sql(*dimension).to_owned()).collect();
        select.extend([
            "count(*)".to_owned(),
            format!("count(*) FILTER (WHERE {FAILED_SQL})"),
            "CAST(coalesce(sum(request_bytes), 0) AS BIGINT)".to_owned(),
            "CAST(coalesce(sum(response_bytes), 0) AS BIGINT)".to_owned(),
            "CAST(avg(latency_ms) AS DOUBLE)".to_owned(),
            "CAST(quantile_cont(latency_ms, 0.95) AS DOUBLE)".to_owned(),
            "CAST(quantile_cont(latency_ms, 0.99) AS DOUBLE)".to_owned(),
            "CAST(quantile_cont(ttft_ms, 0.95) AS DOUBLE)".to_owned(),
        ]);
        // GROUP BY and ORDER BY reference select positions; dimensions come first.
        let positions: Vec<String> = (1..=dimensions.len()).map(|position| position.to_string()).collect();
        let group_by =
            if dimensions.is_empty() { String::new() } else { format!(" GROUP BY {}", positions.join(", ")) };
        let mut order = vec![format!("{} DESC", dimensions.len() + 1)];
        order.extend(positions.iter().map(|position| format!("{position} ASC NULLS LAST")));
        let sql = format!(
            "SELECT {} FROM raw_calls{}{group_by} ORDER BY {} LIMIT ?",
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
            out.push(SummaryRow {
                group,
                calls: unsigned(row, base)?,
                failed: unsigned(row, base + 1)?,
                request_bytes: unsigned(row, base + 2)?,
                response_bytes: unsigned(row, base + 3)?,
                latency_ms_avg: row.get(base + 4)?,
                latency_ms_p95: row.get(base + 5)?,
                latency_ms_p99: row.get(base + 6)?,
                ttft_ms_p95: row.get(base + 7)?,
            });
        }
        let truncated = out.len() > limit as usize;
        out.truncate(limit as usize);
        Ok(SummaryResponse { rows: out, truncated })
    }

    /// Deletes records received before `cutoff` that every projection has
    /// already processed. Without any projection nothing counts as processed,
    /// so nothing is deleted: the log is what future projections will read.
    ///
    /// Deletes in chunks and releases the writer between them, so ingestion
    /// is not blocked for long. Returns how many records were deleted.
    pub fn delete_received_before(&self, cutoff: DateTime<Utc>) -> Result<usize, StoreError> {
        const CHUNK: i64 = 10_000;
        let mut total = 0;
        loop {
            let deleted = self.write().execute(
                "DELETE FROM raw_calls WHERE seq IN (SELECT seq FROM raw_calls \
                 WHERE received_at < make_timestamp(?) \
                 AND seq <= (SELECT coalesce(min(last_seq), 0) FROM projection_checkpoints) \
                 ORDER BY seq LIMIT ?)",
                [cutoff.timestamp_micros(), CHUNK],
            )?;
            total += deleted;
            if deleted < CHUNK as usize {
                return Ok(total);
            }
        }
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
    text: [(&'static str, Option<&'a str>); 4],
    failed: Option<bool>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

macro_rules! filters_from {
    ($query:expr) => {
        Filters {
            text: [
                ("service", $query.service.as_deref()),
                ("environment", $query.environment.as_deref()),
                ("operation", $query.operation.as_deref()),
                ("session_id", $query.session_id.as_deref()),
            ],
            failed: $query.failed,
            from: $query.from,
            to: $query.to,
        }
    };
}
use filters_from;

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
        match filters.failed {
            Some(true) => filter.clauses.push(FAILED_SQL.into()),
            Some(false) => filter.clauses.push(format!("NOT {FAILED_SQL}")),
            None => {}
        }
        if let Some(from) = filters.from {
            filter.clauses.push("started_at >= make_timestamp(?)".into());
            filter.params.push(Value::BigInt(from.timestamp_micros()));
        }
        if let Some(to) = filters.to {
            filter.clauses.push("started_at < make_timestamp(?)".into());
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
        Dimension::Service => "service",
        Dimension::Environment => "environment",
        Dimension::Operation => "operation",
        Dimension::SessionId => "session_id",
        Dimension::HttpStatus => "CAST(http_status AS VARCHAR)",
        Dimension::Day => "strftime(started_at, '%Y-%m-%d')",
        Dimension::Hour => "strftime(started_at, '%Y-%m-%dT%H:00:00Z')",
    }
}

fn page_size(requested: Option<u32>, default: u32, max: u32) -> Result<u32, StoreError> {
    match requested {
        None => Ok(default),
        Some(limit) if (1..=max).contains(&limit) => Ok(limit),
        Some(_) => Err(StoreError::Invalid(format!("`limit` must be between 1 and {max}"))),
    }
}

/// Column values of one record, in [`INSERT_COLUMNS`] order.
fn row_values(record: &CallRecord, received_at: DateTime<Utc>) -> [Value; 19] {
    let metadata = serde_json::to_string(&record.metadata.clone().unwrap_or_default()).unwrap_or_else(|_| "{}".into());
    let (response, is_stream) = match &record.response {
        None => (Value::Null, Value::Null),
        Some(RawResponse::Body(body)) => (text(body), Value::Boolean(false)),
        Some(RawResponse::Events(events)) => {
            (Value::Text(serde_json::to_string(events).unwrap_or_default()), Value::Boolean(true))
        }
    };
    [
        text(&record.id),
        timestamp(received_at),
        text(&record.service),
        text(&record.environment),
        text(&record.operation),
        optional_text(record.session_id.as_deref()),
        Value::Text(metadata),
        text(record.stored_url()),
        text(&record.request),
        response,
        is_stream,
        record.http_status.map_or(Value::Null, |status| Value::Int(i32::from(status))),
        optional_text(record.error.as_deref()),
        Value::Boolean(record.truncated),
        timestamp(record.started_at),
        record.ttft_ms.map_or(Value::Null, count),
        count(record.latency_ms),
        count(record.request.len() as u64),
        count(record.response.as_ref().map_or(0, RawResponse::byte_len) as u64),
    ]
}

fn timestamp(value: DateTime<Utc>) -> Value {
    Value::Timestamp(TimeUnit::Microsecond, value.timestamp_micros())
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn optional_text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, text)
}

/// Durations and byte counts are bounded by the limits, far below `i64::MAX`.
fn count(value: u64) -> Value {
    Value::BigInt(i64::try_from(value).unwrap_or(i64::MAX))
}

const ENVELOPE_LEN: usize = 17;

fn unsigned(row: &Row<'_>, index: usize) -> Result<u64, StoreError> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("negative value {value} in column {index}")))
}

fn read_envelope(row: &Row<'_>) -> Result<CallEnvelope, StoreError> {
    let timestamp = |index: usize| -> Result<DateTime<Utc>, StoreError> {
        let micros: i64 = row.get(index)?;
        DateTime::from_timestamp_micros(micros).ok_or_else(|| StoreError::Corrupt(format!("timestamp {micros}")))
    };
    let metadata: String = row.get(7)?;
    let http_status: Option<i32> = row.get(9)?;
    let ttft_ms: Option<i64> = row.get(13)?;
    Ok(CallEnvelope {
        seq: unsigned(row, 0)?,
        id: row.get(1)?,
        received_at: timestamp(2)?,
        service: row.get(3)?,
        environment: row.get(4)?,
        operation: row.get(5)?,
        session_id: row.get(6)?,
        metadata: serde_json::from_str(&metadata).map_err(|_| StoreError::Corrupt("metadata".into()))?,
        url: row.get(8)?,
        http_status: http_status
            .map(|status| u16::try_from(status).map_err(|_| StoreError::Corrupt(format!("http_status {status}"))))
            .transpose()?,
        error: row.get(10)?,
        truncated: row.get(11)?,
        started_at: timestamp(12)?,
        ttft_ms: ttft_ms
            .map(|value| u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("ttft_ms {value}"))))
            .transpose()?,
        latency_ms: unsigned(row, 14)?,
        request_bytes: unsigned(row, 15)?,
        response_bytes: unsigned(row, 16)?,
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

    fn record(id: &str, started_at: &str, operation: &str) -> CallRecord {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "service": "web",
            "environment": "production",
            "operation": operation,
            "session_id": "s1",
            "metadata": { "listing_id": "123" },
            "url": "https://openrouter.ai/api/v1/chat/completions?key=secret",
            "request": "{\"model\":\"m\",\"messages\":[{\"role\":\"user\",\"content\":\"Hallo\"}]}",
            "response": "{\"choices\":[]}",
            "http_status": 200,
            "started_at": started_at,
            "ttft_ms": 100,
            "latency_ms": 200,
        }))
        .unwrap()
    }

    #[test]
    fn round_trips_bodies_streams_and_failures_verbatim() {
        let store = store();
        let received = at("2026-10-08T12:00:00.123456Z");
        let body = record("body", "2026-10-08T10:00:00.5Z", "parse");
        let mut stream = record("stream", "2026-10-08T10:00:01Z", "chat");
        stream.response = Some(RawResponse::Events(vec![
            "event: message_start\ndata: {\"type\":\"message_start\"}".into(),
            "data: [DONE]".into(),
        ]));
        let mut failed = record("failed", "2026-10-08T10:00:02Z", "chat");
        failed.response = None;
        failed.http_status = None;
        failed.ttft_ms = None;
        failed.metadata = None;
        failed.session_id = None;
        failed.error = Some("TypeError: fetch failed".into());
        failed.truncated = true;
        store.insert(&[body.clone(), stream.clone(), failed], received).unwrap();

        let stored = store.get("body").unwrap().unwrap();
        assert_eq!(stored.request, body.request);
        assert_eq!(stored.response, body.response);
        assert_eq!(stored.envelope.url, "https://openrouter.ai/api/v1/chat/completions");
        assert_eq!(stored.envelope.received_at, received);
        assert_eq!(stored.envelope.started_at, at("2026-10-08T10:00:00.5Z"));
        assert_eq!(stored.envelope.metadata["listing_id"], "123");
        assert_eq!(stored.envelope.request_bytes, body.request.len() as u64);
        assert_eq!((stored.envelope.http_status, stored.envelope.ttft_ms), (Some(200), Some(100)));

        assert_eq!(store.get("stream").unwrap().unwrap().response, stream.response);

        let failed = store.get("failed").unwrap().unwrap();
        assert_eq!(failed.response, None);
        assert!(failed.envelope.metadata.is_empty());
        assert!(failed.envelope.truncated);
        assert_eq!(failed.envelope.error.as_deref(), Some("TypeError: fetch failed"));
        assert_eq!(store.get("missing").unwrap(), None);
    }

    #[test]
    fn duplicate_ids_are_counted_and_first_write_wins() {
        let store = store();
        let first = record("a", "2026-10-08T10:00:00Z", "parse");
        let mut changed = first.clone();
        changed.operation = "other".into();
        let result = store.insert(&[first, changed.clone()], Utc::now()).unwrap();
        assert_eq!(result, Inserted { accepted: 1, duplicates: 1 });
        let result = store.insert(&[changed, record("b", "2026-10-08T10:00:00Z", "parse")], Utc::now()).unwrap();
        assert_eq!(result, Inserted { accepted: 1, duplicates: 1 });
        assert_eq!(store.get("a").unwrap().unwrap().envelope.operation, "parse");
    }

    #[test]
    fn lists_newest_first_with_filters_and_cursor() {
        let store = store();
        for index in 0..5 {
            store.insert(&[record(&format!("c{index}"), "2026-10-08T10:00:00Z", "parse")], Utc::now()).unwrap();
        }
        let mut failed = record("late", "2026-10-08T11:00:00Z", "chat");
        failed.http_status = Some(500);
        store.insert(&[failed], Utc::now()).unwrap();

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

        let count = |query: ListCallsQuery| store.list(&query).unwrap().calls.len();
        assert_eq!(count(ListCallsQuery { operation: Some("parse".into()), ..Default::default() }), 5);
        assert_eq!(count(ListCallsQuery { failed: Some(true), ..Default::default() }), 1);
        assert_eq!(count(ListCallsQuery { failed: Some(false), ..Default::default() }), 5);
        assert_eq!(
            count(ListCallsQuery {
                from: Some(at("2026-10-08T10:30:00Z")),
                to: Some(at("2026-10-08T12:00:00Z")),
                ..Default::default()
            }),
            1
        );

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
    fn summarises_collection_by_dimensions() {
        let store = store();
        let mut failed = record("e", "2026-10-09T09:00:00Z", "parse");
        failed.http_status = Some(429);
        store
            .insert(
                &[
                    record("a", "2026-10-08T10:00:00Z", "parse"),
                    record("b", "2026-10-08T23:00:00Z", "parse"),
                    record("c", "2026-10-08T10:00:00Z", "chat"),
                    failed,
                ],
                Utc::now(),
            )
            .unwrap();

        let total = store.summary(&SummaryQuery::default()).unwrap();
        let row = &total.rows[0];
        assert_eq!((row.calls, row.failed), (4, 1));
        assert_eq!(row.latency_ms_avg, Some(200.0));
        assert_eq!(row.ttft_ms_p95, Some(100.0));
        assert!(row.request_bytes > 0 && row.group.is_empty());

        let by = store
            .summary(&SummaryQuery { group_by: vec![Dimension::Operation, Dimension::Day], ..Default::default() })
            .unwrap();
        let keys: Vec<(Option<String>, Option<String>, u64)> = by
            .rows
            .iter()
            .map(|row| (row.group[&Dimension::Operation].clone(), row.group[&Dimension::Day].clone(), row.calls))
            .collect();
        assert_eq!(
            keys,
            [
                (Some("parse".into()), Some("2026-10-08".into()), 2),
                (Some("chat".into()), Some("2026-10-08".into()), 1),
                (Some("parse".into()), Some("2026-10-09".into()), 1),
            ]
        );

        let by_status = store
            .summary(&SummaryQuery { group_by: vec![Dimension::HttpStatus], limit: Some(1), ..Default::default() })
            .unwrap();
        assert_eq!(by_status.rows[0].group[&Dimension::HttpStatus].as_deref(), Some("200"));
        assert!(by_status.truncated);

        let empty = store.summary(&SummaryQuery { service: Some("none".into()), ..Default::default() }).unwrap();
        assert_eq!(empty.rows[0].calls, 0);
        assert_eq!(empty.rows[0].latency_ms_avg, None);
    }

    #[test]
    fn deletes_records_received_before_a_cutoff() {
        let store = store();
        store.insert(&[record("old", "2026-10-01T00:00:00Z", "o")], at("2026-10-01T00:00:00Z")).unwrap();
        store.insert(&[record("new", "2026-10-08T00:00:00Z", "o")], at("2026-10-08T00:00:00Z")).unwrap();
        let cutoff = at("2026-10-05T00:00:00Z");

        // Without projections nothing has been processed, so nothing is deleted.
        assert_eq!(store.delete_received_before(cutoff).unwrap(), 0);
        // A projection that has not processed anything protects every record.
        store.write().execute_batch("INSERT INTO projection_checkpoints VALUES ('calls', 0)").unwrap();
        assert_eq!(store.delete_received_before(cutoff).unwrap(), 0);
        // The slowest projection decides.
        store.write().execute_batch("UPDATE projection_checkpoints SET last_seq = 2").unwrap();
        store.write().execute_batch("INSERT INTO projection_checkpoints VALUES ('metrics', 1)").unwrap();
        assert_eq!(store.delete_received_before(cutoff).unwrap(), 1);
        assert!(store.get("old").unwrap().is_none());
        assert!(store.get("new").unwrap().is_some());
    }

    #[test]
    fn reopening_a_file_keeps_data_and_does_not_reapply_migrations() {
        let dir = std::env::temp_dir().join(format!("ai-ledger-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ledger.duckdb");
        let path = path.to_str().unwrap();
        {
            let store = Store::open(path, Some("256MB")).unwrap();
            store.insert(&[record("a", "2026-10-08T10:00:00Z", "o")], Utc::now()).unwrap();
            store.checkpoint().unwrap();
        }
        let store = Store::open(path, None).unwrap();
        assert!(store.get("a").unwrap().is_some());
        store.insert(&[record("b", "2026-10-08T10:00:00Z", "o")], Utc::now()).unwrap();
        let seqs: Vec<u64> = store.list(&ListCallsQuery::default()).unwrap().calls.iter().map(|c| c.seq).collect();
        assert!(seqs[0] > seqs[1], "seq keeps increasing across restarts: {seqs:?}");
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
