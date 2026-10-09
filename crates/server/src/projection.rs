//! The read side: processes the raw log into calls, message parts, tool calls
//! and deduplicated contents.
//!
//! Reads final records in `seq` order and writes each batch, together with its
//! checkpoint, in one transaction, so every exchange is processed exactly once.
//! A new [`PARSER_VERSION`] starts over from the beginning of the raw log and
//! replaces the rows of every exchange it reprocesses; rows of exchanges whose
//! raw records are already gone stay as they were.
//!
//! Contents are stored once, zstd-compressed when that saves space. Nothing
//! counts references while writing: a content is garbage once no part points
//! to it, and [`Store::sweep_contents`] removes those. Hot contents (one system
//! prompt is in thousands of calls) therefore never become a write hotspot.

use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use duckdb::{appender_params_from_iter, types::Value};
use traumfaenger_api::StoredExchange;
use traumfaenger_process::{Call, PARSER_VERSION, PartKind, Side, process};

use crate::store::{Store, StoreError, count, optional_text, text, timestamp};

/// Exchanges per transaction.
pub const BATCH: usize = 500;
/// Contents shorter than this are stored raw; zstd frames have overhead.
const COMPRESS_FROM: usize = 64;
const CODEC_RAW: u8 = 0;
const CODEC_ZSTD: u8 = 1;

fn checkpoint_name() -> String {
    format!("calls@{PARSER_VERSION}")
}

/// Content hash as stored: the 16 hash bytes as a little-endian HUGEINT.
pub fn hash_value(hash: [u8; 16]) -> i128 {
    i128::from_le_bytes(hash)
}

pub fn hash_hex(hash: i128) -> String {
    hash.to_le_bytes().iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn parse_hash_hex(hex: &str) -> Option<i128> {
    if hex.len() != 32 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(i128::from_le_bytes(bytes))
}

pub fn decode_content(codec: u8, data: &[u8]) -> Result<String, StoreError> {
    let bytes = match codec {
        CODEC_RAW => data.to_vec(),
        CODEC_ZSTD => zstd::decode_all(data).map_err(|error| StoreError::Corrupt(format!("content: {error}")))?,
        other => return Err(StoreError::Corrupt(format!("unknown content codec {other}"))),
    };
    String::from_utf8(bytes).map_err(|_| StoreError::Corrupt("content is not UTF-8".into()))
}

fn encode_content(text: &str) -> (u8, Vec<u8>) {
    if text.len() >= COMPRESS_FROM
        && let Ok(compressed) = zstd::encode_all(text.as_bytes(), 3)
        && compressed.len() < text.len()
    {
        return (CODEC_ZSTD, compressed);
    }
    (CODEC_RAW, text.as_bytes().to_vec())
}

/// Result of one [`Store::process_batch`].
#[derive(Debug, Default, PartialEq)]
pub struct Progress {
    pub processed: usize,
    /// Nothing newer is waiting.
    pub caught_up: bool,
}

impl Store {
    /// Drops the checkpoints of other parser versions, so processing starts
    /// over. Returns whether it did (a full reprocessing is due).
    pub fn prepare_processing(&self) -> Result<bool, StoreError> {
        let connection = self.write();
        let removed = connection.execute(
            "DELETE FROM projection_checkpoints WHERE projection LIKE 'calls@%' AND projection <> ?",
            [checkpoint_name()],
        )?;
        let current: i64 = connection.query_row(
            "SELECT count(*) FROM projection_checkpoints WHERE projection = ?",
            [checkpoint_name()],
            |row| row.get(0),
        )?;
        if current == 0 {
            connection.execute("INSERT INTO projection_checkpoints VALUES (?, 0)", [checkpoint_name()])?;
        }
        Ok(removed > 0)
    }

    /// Processes the whole raw log again, for example after a price change.
    /// Rows are replaced as their exchanges are reprocessed.
    pub fn restart_processing(&self) -> Result<(), StoreError> {
        self.write()
            .execute("UPDATE projection_checkpoints SET last_seq = 0 WHERE projection = ?", [checkpoint_name()])?;
        Ok(())
    }

    /// Processes up to `limit` exchanges whose final record is past the checkpoint.
    pub fn process_batch(&self, limit: usize) -> Result<Progress, StoreError> {
        let after: i64 = self
            .read()
            .query_row("SELECT last_seq FROM projection_checkpoints WHERE projection = ?", [checkpoint_name()], |row| {
                row.get(0)
            })
            .unwrap_or(0);
        let loaded = self.load_finished(after, limit)?;
        let Some(&(last_seq, _)) = loaded.last() else { return Ok(Progress { processed: 0, caught_up: true }) };
        let first_seq = loaded.iter().map(|(seq, _)| *seq).min().unwrap_or(last_seq);
        let count = loaded.len();
        let processed: Vec<(i64, StoredExchange, Call)> = loaded
            .into_iter()
            .map(|(seq, exchange)| {
                let call = process(&exchange);
                (seq, exchange, call)
            })
            .collect();
        self.write_batch(&processed, first_seq, last_seq)?;
        Ok(Progress { processed: count, caught_up: count < limit })
    }

    fn write_batch(
        &self,
        batch: &[(i64, StoredExchange, Call)],
        first_seq: i64,
        last_seq: i64,
    ) -> Result<(), StoreError> {
        let mut connection = self.write();
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS staging_calls AS SELECT * FROM calls LIMIT 0;
             CREATE TEMP TABLE IF NOT EXISTS staging_parts AS SELECT * FROM call_parts LIMIT 0;
             CREATE TEMP TABLE IF NOT EXISTS staging_tool_calls AS SELECT * FROM tool_calls LIMIT 0;
             CREATE TEMP TABLE IF NOT EXISTS staging_contents AS SELECT * FROM contents LIMIT 0;
             CREATE TEMP TABLE IF NOT EXISTS staging_metadata AS SELECT * FROM call_metadata LIMIT 0;
             CREATE TEMP TABLE IF NOT EXISTS staging_results (call_id VARCHAR, started_at TIMESTAMP, \
                 tool_call_id VARCHAR, operation VARCHAR, session_id VARCHAR, content HUGEINT, bytes INTEGER, \
                 is_error BOOLEAN);
             DELETE FROM temp.staging_calls; DELETE FROM temp.staging_parts; DELETE FROM temp.staging_tool_calls;
             DELETE FROM temp.staging_contents; DELETE FROM temp.staging_results;
             DELETE FROM temp.staging_metadata;",
        )?;
        {
            let mut calls = transaction.appender_to_catalog_and_db("staging_calls", "temp", "main")?;
            let mut parts = transaction.appender_to_catalog_and_db("staging_parts", "temp", "main")?;
            let mut tools = transaction.appender_to_catalog_and_db("staging_tool_calls", "temp", "main")?;
            let mut contents = transaction.appender_to_catalog_and_db("staging_contents", "temp", "main")?;
            let mut results = transaction.appender_to_catalog_and_db("staging_results", "temp", "main")?;
            let mut metadata = transaction.appender_to_catalog_and_db("staging_metadata", "temp", "main")?;
            let mut seen = HashSet::new();
            let mut content = |hash: [u8; 16], body: &str| -> Result<(), duckdb::Error> {
                if seen.insert(hash) {
                    let (codec, data) = encode_content(body);
                    contents.append_row(appender_params_from_iter([
                        Value::HugeInt(hash_value(hash)),
                        Value::Int(body.len() as i32),
                        Value::UTinyInt(codec),
                        Value::Blob(data),
                    ]))?;
                }
                Ok(())
            };

            for (seq, exchange, call) in batch {
                for (key, value) in &exchange.metadata {
                    metadata.append_row(appender_params_from_iter([
                        text(&exchange.id),
                        Value::BigInt(*seq),
                        text(key),
                        text(value),
                    ]))?;
                }
                let ended_at =
                    exchange.end.as_ref().map(|end| exchange.started_at + Duration::milliseconds(end.at_ms as i64));
                let mut request_bytes = 0u64;
                let mut response_bytes = 0u64;
                let mut part_row = |side: &str,
                                    message: i32,
                                    index: i32,
                                    role: &str,
                                    kind: &str,
                                    hash: [u8; 16],
                                    bytes: usize,
                                    tool_call_id: Option<&str>,
                                    tool_name: Option<&str>,
                                    is_error: bool| {
                    parts.append_row(appender_params_from_iter([
                        text(&exchange.id),
                        Value::BigInt(*seq),
                        text(side),
                        Value::Int(message),
                        Value::Int(index),
                        text(role),
                        text(kind),
                        Value::HugeInt(hash_value(hash)),
                        Value::Int(bytes as i32),
                        optional_text(tool_call_id),
                        optional_text(tool_name),
                        Value::Boolean(is_error),
                    ]))
                };
                if let Some(tools) = &call.tools {
                    content(tools.hash, &tools.text)?;
                    request_bytes += tools.text.len() as u64;
                    part_row(
                        "request",
                        -1,
                        0,
                        "system",
                        "tool_definitions",
                        tools.hash,
                        tools.text.len(),
                        None,
                        None,
                        false,
                    )?;
                }
                for part in &call.parts {
                    content(part.content.hash, &part.content.text)?;
                    let bytes = part.content.text.len();
                    match part.side {
                        Side::Request => request_bytes += bytes as u64,
                        Side::Response => response_bytes += bytes as u64,
                    }
                    part_row(
                        part.side.as_str(),
                        part.message as i32,
                        part.index as i32,
                        part.role,
                        part.kind.as_str(),
                        part.content.hash,
                        bytes,
                        part.tool_call_id.as_deref(),
                        part.tool_name.as_deref(),
                        part.is_error,
                    )?;
                    if part.kind == PartKind::ToolCall && part.side == Side::Response {
                        tools.append_row(appender_params_from_iter([
                            text(&exchange.id),
                            Value::BigInt(*seq),
                            optional_text(part.tool_call_id.as_deref()),
                            optional_text(part.tool_name.as_deref()),
                            Value::HugeInt(hash_value(part.content.hash)),
                            Value::Int(bytes as i32),
                            timestamp(exchange.started_at),
                            ended_at.map_or(Value::Null, timestamp),
                            text(&exchange.service),
                            text(&exchange.operation),
                            optional_text(exchange.session_id.as_deref()),
                            optional_text(call.model.as_deref()),
                            optional_text(call.provider.as_deref()),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ]))?;
                    }
                    if part.kind == PartKind::ToolResult
                        && part.side == Side::Request
                        && let Some(tool_call_id) = &part.tool_call_id
                    {
                        results.append_row(appender_params_from_iter([
                            text(&exchange.id),
                            timestamp(exchange.started_at),
                            text(tool_call_id),
                            text(&exchange.operation),
                            optional_text(exchange.session_id.as_deref()),
                            Value::HugeInt(hash_value(part.content.hash)),
                            Value::Int(bytes as i32),
                            Value::Boolean(part.is_error),
                        ]))?;
                    }
                }
                calls.append_row(appender_params_from_iter(call_row(
                    *seq,
                    exchange,
                    call,
                    ended_at,
                    request_bytes,
                    response_bytes,
                )))?;
            }
            for appender in [&mut calls, &mut parts, &mut tools, &mut results, &mut metadata] {
                appender.flush()?;
            }
            contents.flush()?;
        }

        // Replace earlier rows of these exchanges (reprocessing after a parser
        // change). `raw_seq` only grows, so block statistics skip everything
        // outside the batch's range.
        for table in ["call_parts", "tool_calls", "call_metadata"] {
            transaction.execute(
                &format!(
                    "DELETE FROM {table} WHERE raw_seq BETWEEN ? AND ? \
                     AND call_id IN (SELECT id FROM temp.staging_calls)"
                ),
                [first_seq, last_seq],
            )?;
        }
        transaction.execute(
            "DELETE FROM calls WHERE raw_seq BETWEEN ? AND ? AND id IN (SELECT id FROM temp.staging_calls)",
            [first_seq, last_seq],
        )?;
        transaction.execute_batch(
            "INSERT INTO calls SELECT * FROM temp.staging_calls ORDER BY raw_seq;
             INSERT INTO call_parts SELECT * FROM temp.staging_parts ORDER BY raw_seq, side, message, idx;
             INSERT INTO tool_calls SELECT * FROM temp.staging_tool_calls ORDER BY raw_seq;
             INSERT INTO call_metadata SELECT * FROM temp.staging_metadata ORDER BY raw_seq, key;
             INSERT OR IGNORE INTO contents SELECT * FROM temp.staging_contents;",
        )?;
        // Each tool call is answered by the first later request that carries
        // its result (agents resend the conversation, so later ones repeat it).
        transaction.execute_batch(
            "UPDATE tool_calls t SET result_call_id = r.call_id, result = r.content, result_bytes = r.bytes, \
                 is_error = r.is_error, gap_ms = date_diff('millisecond', t.ended_at, r.started_at)
             FROM (
                 SELECT tool_call_id, operation, session_id, arg_min(call_id, started_at) AS call_id,
                        min(started_at) AS started_at, arg_min(content, started_at) AS content,
                        arg_min(bytes, started_at) AS bytes, arg_min(is_error, started_at) AS is_error
                 FROM temp.staging_results GROUP BY ALL
             ) r
             WHERE t.tool_call_id = r.tool_call_id AND t.operation = r.operation
               AND t.session_id IS NOT DISTINCT FROM r.session_id
               AND t.result_call_id IS NULL AND r.started_at >= t.started_at;",
        )?;
        transaction.execute(
            "UPDATE projection_checkpoints SET last_seq = ? WHERE projection = ?",
            duckdb::params![last_seq, checkpoint_name()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Deletes contents no part refers to. Returns how many.
    pub fn sweep_contents(&self) -> Result<usize, StoreError> {
        let connection = self.write();
        Ok(connection
            .execute("DELETE FROM contents WHERE hash NOT IN (SELECT DISTINCT content FROM call_parts)", [])?)
    }
}

fn call_row(
    seq: i64,
    exchange: &StoredExchange,
    call: &Call,
    ended_at: Option<DateTime<Utc>>,
    request_bytes: u64,
    response_bytes: u64,
) -> Vec<Value> {
    let tokens = |value: Option<u64>| value.map_or(Value::Null, count);
    let money = |value: Option<f64>| value.map_or(Value::Null, Value::Double);
    let metadata = serde_json::to_string(&exchange.metadata).unwrap_or_else(|_| "{}".into());
    vec![
        text(&exchange.id),
        Value::BigInt(seq),
        timestamp(exchange.started_at),
        ended_at.map_or(Value::Null, timestamp),
        text(&exchange.service),
        text(&exchange.operation),
        optional_text(exchange.session_id.as_deref()),
        text(&metadata),
        Value::Boolean(call.starts_turn()),
        text(exchange.transport.as_str()),
        exchange.status.map_or(Value::Null, |status| Value::Int(i32::from(status))),
        text(call.api.as_str()),
        text(&call.gateway),
        optional_text(call.provider.as_deref()),
        optional_text(call.model_requested.as_deref()),
        optional_text(call.model.as_deref()),
        Value::Boolean(call.stream),
        text(call.outcome.as_str()),
        optional_text(call.error.as_deref()),
        optional_text(call.finish_reason),
        tokens(call.usage.input),
        tokens(call.usage.cached),
        tokens(call.usage.cache_write),
        tokens(call.usage.output),
        tokens(call.usage.reasoning),
        money(call.cost_usd),
        money(call.input_cost_usd),
        money(call.output_cost_usd),
        text(call.cost_source.as_str()),
        tokens(exchange.end.as_ref().map(|end| end.at_ms)),
        tokens(call.ttft_ms),
        Value::Int(call.tool_calls().count() as i32),
        count(request_bytes),
        count(response_bytes),
        text(call.parse.status()),
        optional_text(call.parse.reason()),
        Value::Int(PARSER_VERSION as i32),
    ]
}
