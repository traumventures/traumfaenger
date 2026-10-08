//! DuckDB storage, split CQRS-style.
//!
//! - **Write side (this version):** the raw log. `raw_exchanges` has one row
//!   per received record (an exchange, or one segment of a WebSocket
//!   session) and `raw_frames` one row per frame, keyed by the record's
//!   `seq`. Ingestion only appends; it never parses frame data.
//! - **Read side (later):** projections read the log in `seq` order, maintain
//!   their own tables and record their position in `projection_checkpoints`.
//!
//! `seq` order equals commit order: there is one writer connection and every
//! batch is one transaction. A reader of `seq > last_seq` never skips a record.
//! This holds only while every write goes through `Store::write`; never open a
//! second writing connection.
//!
//! Frames are keyed by `seq`, which only increases, so DuckDB's per-block
//! min/max statistics find the frames of one record without an index.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::{Mutex, MutexGuard},
};

use chrono::{DateTime, Utc};
use duckdb::{
    Connection, Row, appender_params_from_iter, params_from_iter,
    types::{TimeUnit, Value},
};
use traumfaenger_api::{
    Dimension, Direction, EndReason, Exchange, ExchangeEnd, Frame, ListQuery, ListResponse, LogEntry, StoredExchange,
    SummaryQuery, SummaryResponse, SummaryRow, Transport, limits,
};

/// Ordered schema migrations. Append only once released; nothing is deployed yet.
const MIGRATIONS: &[&str] = &["CREATE SEQUENCE raw_seq START 1;
    CREATE TABLE raw_exchanges (
        seq BIGINT NOT NULL DEFAULT nextval('raw_seq'),
        id VARCHAR NOT NULL,
        segment INTEGER NOT NULL,
        is_final BOOLEAN NOT NULL,
        received_at TIMESTAMP NOT NULL,
        service VARCHAR NOT NULL,
        operation VARCHAR NOT NULL,
        session_id VARCHAR,
        metadata VARCHAR NOT NULL,
        transport VARCHAR NOT NULL,
        url VARCHAR NOT NULL,
        started_at TIMESTAMP NOT NULL,
        status INTEGER,
        end_at_ms BIGINT,
        end_reason VARCHAR,
        end_error VARCHAR,
        close_code INTEGER,
        truncated BOOLEAN NOT NULL,
        frame_count INTEGER NOT NULL,
        bytes BIGINT NOT NULL,
        PRIMARY KEY (id, segment)
    );
    CREATE TABLE raw_frames (
        seq BIGINT NOT NULL,
        idx INTEGER NOT NULL,
        t_ms BIGINT NOT NULL,
        dir VARCHAR NOT NULL,
        data VARCHAR NOT NULL
    );
    CREATE TABLE projection_checkpoints (
        projection VARCHAR PRIMARY KEY,
        last_seq BIGINT NOT NULL
    )"];

/// Columns written on ingestion, in this order. `seq` takes its default.
const INSERT_COLUMNS: &str = "id, segment, is_final, received_at, service, operation, session_id, \
     metadata, transport, url, started_at, status, end_at_ms, end_reason, end_error, close_code, truncated, \
     frame_count, bytes";

/// Log entry columns. Timestamps are exchanged as epoch microseconds.
const ENTRY_COLUMNS: &str = "seq, id, segment, is_final, epoch_us(received_at), service, operation, \
     session_id, metadata, transport, url, epoch_us(started_at), status, end_at_ms, end_reason, end_error, \
     close_code, truncated, frame_count, bytes";

/// A final record of an exchange that failed.
const FAILED_SQL: &str = "(is_final AND (end_reason = 'error' OR status IS NULL OR status >= 400))";

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
    /// Owns the temp staging tables, so all writes go through it.
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
        connection.execute_batch("SET autoinstall_known_extensions = false; SET autoload_known_extensions = false;")?;
        if let Some(limit) = memory_limit {
            // SET takes no bind parameters; `is_size` restricts the value to `<number><unit>`.
            connection.execute_batch(&format!("SET memory_limit = '{limit}'"))?;
        }
        migrate(&connection)?;
        // Per-connection staging tables for bulk inserts; see `insert`.
        connection.execute_batch(&format!(
            "CREATE TEMP TABLE staging_exchanges AS SELECT {INSERT_COLUMNS} FROM raw_exchanges LIMIT 0;
             CREATE TEMP TABLE staging_frames (id VARCHAR, segment INTEGER, idx INTEGER, t_ms BIGINT, dir VARCHAR, \
             data VARCHAR);"
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

    /// Stores validated records in one transaction. Records whose
    /// `(id, segment)` already exists, or repeats within the batch, are counted
    /// as duplicates and change nothing.
    ///
    /// Rows go through DuckDB's appender into temporary staging tables and
    /// then into the log with `INSERT ... SELECT`; row-by-row inserts against
    /// the primary-key index are orders of magnitude slower.
    pub fn insert(&self, records: &[Exchange], received_at: DateTime<Utc>) -> Result<Inserted, StoreError> {
        let mut seen = HashSet::with_capacity(records.len());
        let unique: Vec<&Exchange> =
            records.iter().filter(|record| seen.insert((record.id.as_str(), record.segment))).collect();

        let mut connection = self.write();
        let transaction = connection.transaction()?;
        transaction.execute_batch("DELETE FROM temp.staging_exchanges; DELETE FROM temp.staging_frames;")?;
        {
            let mut exchanges = transaction.appender_to_catalog_and_db("staging_exchanges", "temp", "main")?;
            for record in &unique {
                exchanges.append_row(appender_params_from_iter(row_values(record, received_at)))?;
            }
            exchanges.flush()?;
            let mut frames = transaction.appender_to_catalog_and_db("staging_frames", "temp", "main")?;
            for record in &unique {
                for (index, frame) in record.frames.iter().enumerate() {
                    frames.append_row(appender_params_from_iter([
                        text(&record.id),
                        Value::Int(record.segment as i32),
                        Value::Int(index as i32),
                        count(frame.t_ms),
                        text(direction_str(frame.dir)),
                        text(&frame.data),
                    ]))?;
                }
            }
            frames.flush()?;
        }
        // Records inserted now get a seq above every existing one (single writer).
        let before: i64 =
            transaction.query_row("SELECT coalesce(max(seq), 0) FROM raw_exchanges", [], |row| row.get(0))?;
        let accepted = transaction.execute(
            &format!(
                "INSERT INTO raw_exchanges ({INSERT_COLUMNS}) SELECT {INSERT_COLUMNS} FROM temp.staging_exchanges \
                 ON CONFLICT (id, segment) DO NOTHING"
            ),
            [],
        )?;
        transaction.execute(
            "INSERT INTO raw_frames SELECT e.seq, f.idx, f.t_ms, f.dir, f.data FROM temp.staging_frames f \
             JOIN raw_exchanges e ON e.id = f.id AND e.segment = f.segment AND e.seq > ? ORDER BY e.seq, f.idx",
            [before],
        )?;
        transaction.execute_batch("DELETE FROM temp.staging_exchanges; DELETE FROM temp.staging_frames;")?;
        transaction.commit()?;
        Ok(Inserted { accepted, duplicates: records.len() - accepted })
    }

    /// All stored segments of one exchange, joined.
    pub fn get(&self, id: &str) -> Result<Option<StoredExchange>, StoreError> {
        let connection = self.read();
        let mut statement =
            connection.prepare(&format!("SELECT {ENTRY_COLUMNS} FROM raw_exchanges WHERE id = ? ORDER BY segment"))?;
        let mut rows = statement.query([id])?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push(read_entry(row)?);
        }
        let Some(first) = entries.first() else { return Ok(None) };

        let mut frames = Vec::new();
        let mut statement = connection.prepare("SELECT t_ms, dir, data FROM raw_frames WHERE seq = ? ORDER BY idx")?;
        for entry in &entries {
            let mut rows = statement.query([entry.seq as i64])?;
            while let Some(row) = rows.next()? {
                let dir: String = row.get(1)?;
                frames.push(Frame { t_ms: unsigned(row, 0)?, dir: parse_direction(&dir)?, data: row.get(2)? });
            }
        }
        let last = entries.last().unwrap_or(first);
        let has_final = last.is_final;
        let gapless = entries.iter().enumerate().all(|(index, entry)| entry.segment as usize == index);
        Ok(Some(StoredExchange {
            id: first.id.clone(),
            service: first.service.clone(),
            operation: first.operation.clone(),
            session_id: first.session_id.clone(),
            metadata: first.metadata.clone(),
            transport: first.transport,
            url: first.url.clone(),
            started_at: first.started_at,
            status: entries.iter().rev().find_map(|entry| entry.status),
            segments: entries.len() as u32,
            complete: has_final && gapless,
            truncated: entries.iter().any(|entry| entry.truncated),
            frames,
            end: if has_final { last.end.clone() } else { None },
        }))
    }

    /// Raw log rows, most recently stored first, keyset-paginated on `seq`.
    pub fn list(&self, query: &ListQuery) -> Result<ListResponse, StoreError> {
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
            "SELECT {ENTRY_COLUMNS} FROM raw_exchanges{} ORDER BY seq DESC LIMIT ?",
            filter.where_sql()
        ))?;
        let mut rows = statement.query(params_from_iter(filter.params))?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push(read_entry(row)?);
        }
        let next_cursor = if entries.len() > limit as usize {
            entries.truncate(limit as usize);
            entries.last().map(|entry| entry.seq.to_string())
        } else {
            None
        };
        Ok(ListResponse { entries, next_cursor })
    }

    /// Collection statistics grouped by the requested dimensions, most exchanges first.
    pub fn summary(&self, query: &SummaryQuery) -> Result<SummaryResponse, StoreError> {
        let limit = page_size(query.limit, limits::DEFAULT_SUMMARY_LIMIT, limits::MAX_SUMMARY_LIMIT)?;
        let mut filter = Filter::new(&filters_from!(query))?;
        filter.params.push(Value::BigInt(i64::from(limit) + 1));

        let dimensions = &query.group_by;
        let mut select: Vec<String> = dimensions.iter().map(|dimension| dimension_sql(*dimension).to_owned()).collect();
        select.extend([
            "count(DISTINCT id)".to_owned(),
            format!("count(DISTINCT id) FILTER (WHERE {FAILED_SQL})"),
            "count(*)".to_owned(),
            "CAST(coalesce(sum(frame_count), 0) AS BIGINT)".to_owned(),
            "CAST(coalesce(sum(bytes), 0) AS BIGINT)".to_owned(),
            "CAST(avg(end_at_ms) FILTER (WHERE is_final) AS DOUBLE)".to_owned(),
            "CAST(quantile_cont(end_at_ms, 0.95) FILTER (WHERE is_final) AS DOUBLE)".to_owned(),
            "CAST(quantile_cont(end_at_ms, 0.99) FILTER (WHERE is_final) AS DOUBLE)".to_owned(),
        ]);
        // GROUP BY and ORDER BY reference select positions; dimensions come first.
        let positions: Vec<String> = (1..=dimensions.len()).map(|position| position.to_string()).collect();
        let group_by =
            if dimensions.is_empty() { String::new() } else { format!(" GROUP BY {}", positions.join(", ")) };
        let mut order = vec![format!("{} DESC", dimensions.len() + 1)];
        order.extend(positions.iter().map(|position| format!("{position} ASC NULLS LAST")));
        let sql = format!(
            "SELECT {} FROM raw_exchanges{}{group_by} ORDER BY {} LIMIT ?",
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
                exchanges: unsigned(row, base)?,
                failed: unsigned(row, base + 1)?,
                records: unsigned(row, base + 2)?,
                frames: unsigned(row, base + 3)?,
                bytes: unsigned(row, base + 4)?,
                duration_ms_avg: row.get(base + 5)?,
                duration_ms_p95: row.get(base + 6)?,
                duration_ms_p99: row.get(base + 7)?,
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
        let eligible = "received_at < make_timestamp(?) \
             AND seq <= (SELECT coalesce(min(last_seq), 0) FROM projection_checkpoints)";
        let mut total = 0;
        loop {
            let mut connection = self.write();
            let transaction = connection.transaction()?;
            let upper: Option<i64> = transaction.query_row(
                &format!("SELECT max(seq) FROM (SELECT seq FROM raw_exchanges WHERE {eligible} ORDER BY seq LIMIT ?)"),
                duckdb::params![cutoff.timestamp_micros(), CHUNK],
                |row| row.get(0),
            )?;
            let Some(upper) = upper else { return Ok(total) };
            transaction.execute(
                &format!(
                    "DELETE FROM raw_frames WHERE seq IN (SELECT seq FROM raw_exchanges WHERE {eligible} AND seq <= ?)"
                ),
                duckdb::params![cutoff.timestamp_micros(), upper],
            )?;
            let deleted = transaction.execute(
                &format!("DELETE FROM raw_exchanges WHERE {eligible} AND seq <= ?"),
                duckdb::params![cutoff.timestamp_micros(), upper],
            )?;
            transaction.commit()?;
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
    connection.execute_batch("CREATE TABLE IF NOT EXISTS traumfaenger_migrations (version INTEGER PRIMARY KEY)")?;
    let applied: i64 =
        connection.query_row("SELECT coalesce(max(version), 0) FROM traumfaenger_migrations", [], |row| row.get(0))?;
    let applied = usize::try_from(applied).unwrap_or(usize::MAX);
    if applied > MIGRATIONS.len() {
        return Err(StoreError::Corrupt(format!(
            "database is at schema version {applied} but this server knows {}; refusing to run an older binary",
            MIGRATIONS.len()
        )));
    }
    for (index, sql) in MIGRATIONS.iter().enumerate().skip(applied) {
        connection.execute_batch(&format!(
            "BEGIN TRANSACTION; {sql}; INSERT INTO traumfaenger_migrations VALUES ({}); COMMIT;",
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
                ("operation", $query.operation.as_deref()),
                ("session_id", $query.session_id.as_deref()),
                ("transport", $query.transport.map(Transport::as_str)),
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
        Dimension::Operation => "operation",
        Dimension::SessionId => "session_id",
        Dimension::Transport => "transport",
        Dimension::Status => "CAST(status AS VARCHAR)",
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

fn direction_str(direction: Direction) -> &'static str {
    match direction {
        Direction::Out => "out",
        Direction::In => "in",
    }
}

fn parse_direction(value: &str) -> Result<Direction, StoreError> {
    match value {
        "out" => Ok(Direction::Out),
        "in" => Ok(Direction::In),
        other => Err(StoreError::Corrupt(format!("direction `{other}`"))),
    }
}

/// Column values of one record, in [`INSERT_COLUMNS`] order.
fn row_values(record: &Exchange, received_at: DateTime<Utc>) -> [Value; 19] {
    let metadata = serde_json::to_string(&record.metadata.clone().unwrap_or_default()).unwrap_or_else(|_| "{}".into());
    let end = record.end.as_ref();
    [
        text(&record.id),
        Value::Int(record.segment as i32),
        Value::Boolean(record.is_final),
        timestamp(received_at),
        text(&record.service),
        text(&record.operation),
        optional_text(record.session_id.as_deref()),
        Value::Text(metadata),
        text(record.transport.as_str()),
        text(record.stored_url()),
        timestamp(record.started_at),
        record.status.map_or(Value::Null, |status| Value::Int(i32::from(status))),
        end.map_or(Value::Null, |end| count(end.at_ms)),
        end.map_or(Value::Null, |end| text(end.reason.as_str())),
        optional_text(end.and_then(|end| end.error.as_deref())),
        end.and_then(|end| end.close_code).map_or(Value::Null, |code| Value::Int(i32::from(code))),
        Value::Boolean(record.truncated),
        Value::Int(record.frames.len() as i32),
        count(record.data_bytes() as u64),
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

fn unsigned(row: &Row<'_>, index: usize) -> Result<u64, StoreError> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("negative value {value} in column {index}")))
}

fn small<T: TryFrom<i32>>(value: Option<i32>, name: &str) -> Result<Option<T>, StoreError> {
    value.map(|value| T::try_from(value).map_err(|_| StoreError::Corrupt(format!("{name} {value}")))).transpose()
}

fn read_entry(row: &Row<'_>) -> Result<LogEntry, StoreError> {
    let timestamp = |index: usize| -> Result<DateTime<Utc>, StoreError> {
        let micros: i64 = row.get(index)?;
        DateTime::from_timestamp_micros(micros).ok_or_else(|| StoreError::Corrupt(format!("timestamp {micros}")))
    };
    let segment: i32 = row.get(2)?;
    let metadata: String = row.get(8)?;
    let transport: String = row.get(9)?;
    let end_at_ms: Option<i64> = row.get(13)?;
    let end_reason: Option<String> = row.get(14)?;
    let frame_count: i32 = row.get(18)?;
    let end = match (end_at_ms, end_reason) {
        (Some(at_ms), Some(reason)) => Some(ExchangeEnd {
            at_ms: u64::try_from(at_ms).map_err(|_| StoreError::Corrupt(format!("end_at_ms {at_ms}")))?,
            reason: EndReason::parse(&reason).ok_or_else(|| StoreError::Corrupt(format!("end_reason `{reason}`")))?,
            error: row.get(15)?,
            close_code: small(row.get(16)?, "close_code")?,
        }),
        _ => None,
    };
    Ok(LogEntry {
        seq: unsigned(row, 0)?,
        id: row.get(1)?,
        segment: u32::try_from(segment).map_err(|_| StoreError::Corrupt(format!("segment {segment}")))?,
        is_final: row.get(3)?,
        received_at: timestamp(4)?,
        service: row.get(5)?,
        operation: row.get(6)?,
        session_id: row.get(7)?,
        metadata: serde_json::from_str(&metadata).map_err(|_| StoreError::Corrupt("metadata".into()))?,
        transport: Transport::parse(&transport)
            .ok_or_else(|| StoreError::Corrupt(format!("transport `{transport}`")))?,
        url: row.get(10)?,
        started_at: timestamp(11)?,
        status: small(row.get(12)?, "status")?,
        end,
        truncated: row.get(17)?,
        frame_count: u32::try_from(frame_count).map_err(|_| StoreError::Corrupt(format!("frames {frame_count}")))?,
        bytes: unsigned(row, 19)?,
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

    fn exchange(id: &str, started_at: &str, operation: &str) -> Exchange {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "service": "web",
            "operation": operation,
            "session_id": "s1",
            "metadata": { "listing_id": "123" },
            "transport": "sse",
            "url": "https://openrouter.ai/api/v1/chat/completions?key=secret",
            "started_at": started_at,
            "status": 200,
            "frames": [
                { "t_ms": 0, "dir": "out", "data": "{\"model\":\"m\",\"stream\":true}" },
                { "t_ms": 800, "dir": "in", "data": "data: {\"choices\":[{\"delta\":{\"content\":\"Hallo\"}}]}" },
                { "t_ms": 950, "dir": "in", "data": "data: [DONE]" },
            ],
            "end": { "at_ms": 1000, "reason": "complete" },
        }))
        .unwrap()
    }

    fn ws_segment(id: &str, segment: u32, is_final: bool, data: &str) -> Exchange {
        let mut record = exchange(id, "2026-10-08T10:00:00Z", "voice.interview");
        record.transport = Transport::Ws;
        record.url = "wss://api.voice.example/v1/realtime".into();
        record.status = Some(101);
        record.segment = segment;
        record.is_final = is_final;
        let base = u64::from(segment) * 10_000;
        record.frames = vec![
            Frame { t_ms: base, dir: Direction::Out, data: format!("{{\"type\":\"say\",\"text\":\"{data}\"}}") },
            Frame { t_ms: base + 500, dir: Direction::In, data: format!("{{\"type\":\"reply\",\"text\":\"{data}\"}}") },
        ];
        record.end = is_final.then(|| ExchangeEnd {
            at_ms: base + 600,
            reason: EndReason::Closed,
            error: None,
            close_code: Some(1000),
        });
        record
    }

    #[test]
    fn round_trips_frames_verbatim_and_strips_the_query_string() {
        let store = store();
        let received = at("2026-10-08T12:00:00.123456Z");
        let record = exchange("a", "2026-10-08T10:00:00.5Z", "chat");
        let mut failed = exchange("b", "2026-10-08T10:00:01Z", "chat");
        failed.status = None;
        failed.frames.truncate(1);
        failed.metadata = None;
        failed.session_id = None;
        failed.truncated = true;
        failed.end = Some(ExchangeEnd {
            at_ms: 30,
            reason: EndReason::Error,
            error: Some("TypeError: fetch failed".into()),
            close_code: None,
        });
        assert_eq!(store.insert(&[record.clone(), failed], received).unwrap(), Inserted { accepted: 2, duplicates: 0 });

        let stored = store.get("a").unwrap().unwrap();
        assert_eq!(stored.frames, record.frames);
        assert_eq!(stored.end, record.end);
        assert_eq!(stored.url, "https://openrouter.ai/api/v1/chat/completions");
        assert_eq!(stored.started_at, at("2026-10-08T10:00:00.5Z"));
        assert_eq!((stored.segments, stored.complete, stored.status), (1, true, Some(200)));
        assert_eq!(stored.metadata["listing_id"], "123");

        let failed = store.get("b").unwrap().unwrap();
        assert_eq!(failed.frames.len(), 1);
        assert!(failed.metadata.is_empty() && failed.truncated);
        assert_eq!(failed.end.unwrap().error.as_deref(), Some("TypeError: fetch failed"));
        assert_eq!(store.get("missing").unwrap(), None);

        let entry = &store.list(&ListQuery::default()).unwrap().entries[1];
        assert_eq!((entry.frame_count, entry.bytes, entry.received_at), (3, record.data_bytes() as u64, received));
    }

    #[test]
    fn joins_websocket_segments_and_reports_gaps() {
        let store = store();
        store.insert(&[ws_segment("ws", 0, false, "eins"), ws_segment("ws", 2, true, "drei")], Utc::now()).unwrap();
        let partial = store.get("ws").unwrap().unwrap();
        assert_eq!((partial.segments, partial.complete), (2, false));

        store.insert(&[ws_segment("ws", 1, false, "zwei")], Utc::now()).unwrap();
        let joined = store.get("ws").unwrap().unwrap();
        assert_eq!((joined.segments, joined.complete, joined.status), (3, true, Some(101)));
        let texts: Vec<&str> = joined.frames.iter().map(|frame| frame.data.as_str()).collect();
        assert_eq!(texts.len(), 6);
        assert!(texts[0].contains("eins") && texts[2].contains("zwei") && texts[5].contains("drei"));
        assert_eq!(joined.end.unwrap().close_code, Some(1000));

        let summary = store.summary(&SummaryQuery::default()).unwrap();
        assert_eq!((summary.rows[0].exchanges, summary.rows[0].records, summary.rows[0].frames), (1, 3, 6));
        assert_eq!(summary.rows[0].duration_ms_avg, Some(20_600.0));
    }

    #[test]
    fn duplicates_are_counted_and_first_write_wins() {
        let store = store();
        let first = exchange("a", "2026-10-08T10:00:00Z", "parse");
        let mut changed = first.clone();
        changed.operation = "other".into();
        changed.frames.pop();
        let result = store.insert(&[first, changed.clone()], Utc::now()).unwrap();
        assert_eq!(result, Inserted { accepted: 1, duplicates: 1 });
        let result = store.insert(&[changed, exchange("b", "2026-10-08T10:00:00Z", "parse")], Utc::now()).unwrap();
        assert_eq!(result, Inserted { accepted: 1, duplicates: 1 });
        let stored = store.get("a").unwrap().unwrap();
        assert_eq!((stored.operation.as_str(), stored.frames.len()), ("parse", 3));
        // Same id, other segment, is a new record.
        let mut other_segment = ws_segment("a", 1, false, "x");
        other_segment.transport = Transport::Ws;
        assert_eq!(store.insert(&[other_segment], Utc::now()).unwrap().accepted, 1);
    }

    #[test]
    fn lists_newest_first_with_filters_and_cursor() {
        let store = store();
        for index in 0..5 {
            store.insert(&[exchange(&format!("c{index}"), "2026-10-08T10:00:00Z", "parse")], Utc::now()).unwrap();
        }
        let mut failed = exchange("late", "2026-10-08T11:00:00Z", "chat");
        failed.status = Some(500);
        store.insert(&[failed], Utc::now()).unwrap();
        store.insert(&[ws_segment("ws", 0, false, "x")], Utc::now()).unwrap();

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = store.list(&ListQuery { limit: Some(2), cursor: cursor.clone(), ..Default::default() }).unwrap();
            seen.extend(page.entries.into_iter().map(|entry| entry.id));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, ["ws", "late", "c4", "c3", "c2", "c1", "c0"]);

        let count = |query: ListQuery| store.list(&query).unwrap().entries.len();
        assert_eq!(count(ListQuery { operation: Some("parse".into()), ..Default::default() }), 5);
        assert_eq!(count(ListQuery { transport: Some(Transport::Ws), ..Default::default() }), 1);
        assert_eq!(count(ListQuery { failed: Some(true), ..Default::default() }), 1);
        assert_eq!(count(ListQuery { failed: Some(false), ..Default::default() }), 6);
        assert_eq!(
            count(ListQuery {
                from: Some(at("2026-10-08T10:30:00Z")),
                to: Some(at("2026-10-08T12:00:00Z")),
                ..Default::default()
            }),
            1
        );

        let invalid = |query: ListQuery| matches!(store.list(&query), Err(StoreError::Invalid(_)));
        assert!(invalid(ListQuery { cursor: Some("garbage".into()), ..Default::default() }));
        assert!(invalid(ListQuery { limit: Some(0), ..Default::default() }));
        assert!(invalid(ListQuery {
            from: Some(at("2026-10-09T00:00:00Z")),
            to: Some(at("2026-10-08T00:00:00Z")),
            ..Default::default()
        }));
    }

    #[test]
    fn summarises_collection_by_dimensions() {
        let store = store();
        let mut failed = exchange("e", "2026-10-09T09:00:00Z", "parse");
        failed.status = Some(429);
        store
            .insert(
                &[
                    exchange("a", "2026-10-08T10:00:00Z", "parse"),
                    exchange("b", "2026-10-08T23:00:00Z", "parse"),
                    exchange("c", "2026-10-08T10:00:00Z", "chat"),
                    failed,
                ],
                Utc::now(),
            )
            .unwrap();

        let total = &store.summary(&SummaryQuery::default()).unwrap().rows[0];
        assert_eq!((total.exchanges, total.failed, total.records, total.frames), (4, 1, 4, 12));
        assert_eq!(total.duration_ms_avg, Some(1000.0));
        assert!(total.bytes > 0 && total.group.is_empty());

        let by = store
            .summary(&SummaryQuery { group_by: vec![Dimension::Operation, Dimension::Day], ..Default::default() })
            .unwrap();
        let keys: Vec<(Option<String>, Option<String>, u64)> = by
            .rows
            .iter()
            .map(|row| (row.group[&Dimension::Operation].clone(), row.group[&Dimension::Day].clone(), row.exchanges))
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
            .summary(&SummaryQuery { group_by: vec![Dimension::Status], limit: Some(1), ..Default::default() })
            .unwrap();
        assert_eq!(by_status.rows[0].group[&Dimension::Status].as_deref(), Some("200"));
        assert!(by_status.truncated);

        let empty = store.summary(&SummaryQuery { service: Some("none".into()), ..Default::default() }).unwrap();
        assert_eq!(empty.rows[0].exchanges, 0);
        assert_eq!(empty.rows[0].duration_ms_avg, None);
    }

    #[test]
    fn deletes_only_records_every_projection_has_processed() {
        let store = store();
        store.insert(&[exchange("old", "2026-10-01T00:00:00Z", "o")], at("2026-10-01T00:00:00Z")).unwrap();
        store.insert(&[exchange("new", "2026-10-08T00:00:00Z", "o")], at("2026-10-08T00:00:00Z")).unwrap();
        let cutoff = at("2026-10-05T00:00:00Z");

        assert_eq!(store.delete_received_before(cutoff).unwrap(), 0);
        store.write().execute_batch("INSERT INTO projection_checkpoints VALUES ('calls', 0)").unwrap();
        assert_eq!(store.delete_received_before(cutoff).unwrap(), 0);
        store.write().execute_batch("UPDATE projection_checkpoints SET last_seq = 2").unwrap();
        store.write().execute_batch("INSERT INTO projection_checkpoints VALUES ('metrics', 1)").unwrap();
        assert_eq!(store.delete_received_before(cutoff).unwrap(), 1);
        assert!(store.get("old").unwrap().is_none());
        assert_eq!(store.get("new").unwrap().unwrap().frames.len(), 3);
        let orphaned: i64 =
            store.read().query_row("SELECT count(*) FROM raw_frames WHERE seq = 1", [], |row| row.get(0)).unwrap();
        assert_eq!(orphaned, 0);
    }

    #[test]
    fn reopening_a_file_keeps_data_and_seq_keeps_increasing() {
        let dir = std::env::temp_dir().join(format!("traumfaenger-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("database.duckdb");
        let path = path.to_str().unwrap();
        {
            let store = Store::open(path, Some("256MB")).unwrap();
            store.insert(&[exchange("a", "2026-10-08T10:00:00Z", "o")], Utc::now()).unwrap();
            store.checkpoint().unwrap();
        }
        let store = Store::open(path, None).unwrap();
        assert_eq!(store.get("a").unwrap().unwrap().frames.len(), 3);
        store.insert(&[exchange("b", "2026-10-08T10:00:00Z", "o")], Utc::now()).unwrap();
        assert_eq!(store.get("b").unwrap().unwrap().frames.len(), 3);
        let seqs: Vec<u64> = store.list(&ListQuery::default()).unwrap().entries.iter().map(|e| e.seq).collect();
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
