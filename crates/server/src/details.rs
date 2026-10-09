//! Single calls, session trajectories, stored texts and processing status.

use std::collections::{BTreeMap, HashSet};

use chrono::DateTime;
use duckdb::Row;
use traumfaenger_api::{CallDetail, CallPart, ContentResponse, ParseCoverage, ProcessingStatus, SessionResponse};
use traumfaenger_process::PARSER_VERSION;

use crate::{
    projection::{decode_content, hash_hex, parse_hash_hex},
    store::{Store, StoreError},
};

pub const DEFAULT_MAX_CHARS: usize = 2000;
pub const MAX_MAX_CHARS: usize = 1_000_000;
/// Calls of one session returned at most.
pub const MAX_SESSION_CALLS: usize = 2000;

const CALL_COLUMNS: &str = "id, epoch_us(started_at), service, operation, session_id, metadata, turn_start, api, gateway, \
     provider, model, outcome, error, finish_reason, input_tokens, cached_tokens, output_tokens, reasoning_tokens, \
     cost_usd, cost_source, latency_ms, ttft_ms, tool_calls, parse_status, parse_reason, raw_seq";

fn read_call(row: &Row<'_>) -> Result<(CallDetail, i64), StoreError> {
    let micros: i64 = row.get(1)?;
    let metadata: String = row.get(5)?;
    Ok((
        CallDetail {
            id: row.get(0)?,
            started_at: DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| StoreError::Corrupt("started_at".into()))?,
            service: row.get(2)?,
            operation: row.get(3)?,
            session_id: row.get(4)?,
            metadata: serde_json::from_str(&metadata).unwrap_or_default(),
            turn_start: row.get(6)?,
            api: row.get(7)?,
            gateway: row.get(8)?,
            provider: row.get(9)?,
            model: row.get(10)?,
            outcome: row.get(11)?,
            error: row.get(12)?,
            finish_reason: row.get(13)?,
            input_tokens: row.get(14)?,
            cached_tokens: row.get(15)?,
            output_tokens: row.get(16)?,
            reasoning_tokens: row.get(17)?,
            cost_usd: row.get(18)?,
            cost_source: row.get(19)?,
            latency_ms: row.get(20)?,
            ttft_ms: row.get(21)?,
            tool_calls: row.get(22)?,
            parse_status: row.get(23)?,
            parse_reason: row.get(24)?,
            parts: Vec::new(),
        },
        row.get(25)?,
    ))
}

fn clip(text: String, max_chars: usize) -> (String, bool) {
    match text.char_indices().nth(max_chars) {
        Some((end, _)) => (text[..end].to_owned(), true),
        None => (text, false),
    }
}

impl Store {
    /// Parts of the given calls, in conversation order, with texts.
    fn parts_of(
        &self,
        calls: &[(String, i64)],
        max_chars: usize,
    ) -> Result<BTreeMap<String, Vec<CallPart>>, StoreError> {
        let mut out: BTreeMap<String, Vec<CallPart>> = BTreeMap::new();
        if calls.is_empty() {
            return Ok(out);
        }
        let connection = self.read();
        let (low, high) =
            calls.iter().fold((i64::MAX, i64::MIN), |(low, high), (_, seq)| (low.min(*seq), high.max(*seq)));
        let mut statement = connection.prepare(&format!(
            "SELECT p.call_id, p.side, p.message, p.idx, p.role, p.kind, p.tool_call_id, p.tool_name, p.is_error, \
                    p.content, p.bytes, c.codec, c.data
             FROM call_parts p JOIN contents c ON c.hash = p.content
             WHERE p.raw_seq BETWEEN ? AND ? AND p.call_id IN ({})
             ORDER BY p.raw_seq, p.side, p.message, p.idx",
            vec!["?"; calls.len()].join(", ")
        ))?;
        let mut values = vec![duckdb::types::Value::BigInt(low), duckdb::types::Value::BigInt(high)];
        values.extend(calls.iter().map(|(id, _)| duckdb::types::Value::Text(id.clone())));
        let mut rows = statement.query(duckdb::params_from_iter(values))?;
        while let Some(row) = rows.next()? {
            let codec: u8 = row.get(11)?;
            let data: Vec<u8> = row.get(12)?;
            let (text, clipped) = clip(decode_content(codec, &data)?, max_chars);
            let hash: i128 = row.get(9)?;
            let bytes: i32 = row.get(10)?;
            out.entry(row.get(0)?).or_default().push(CallPart {
                side: row.get(1)?,
                message: row.get(2)?,
                index: row.get(3)?,
                role: row.get(4)?,
                kind: row.get(5)?,
                tool_call_id: row.get(6)?,
                tool_name: row.get(7)?,
                is_error: row.get(8)?,
                hash: hash_hex(hash),
                bytes: bytes.max(0) as u64,
                text,
                clipped,
            });
        }
        Ok(out)
    }

    pub fn call(&self, id: &str, max_chars: usize) -> Result<Option<CallDetail>, StoreError> {
        let found = {
            let connection = self.read();
            let mut statement = connection.prepare(&format!("SELECT {CALL_COLUMNS} FROM calls WHERE id = ?"))?;
            let mut rows = statement.query([id])?;
            match rows.next()? {
                Some(row) => Some(read_call(row)?),
                None => None,
            }
        };
        let Some((mut call, seq)) = found else { return Ok(None) };
        call.parts = self.parts_of(&[(call.id.clone(), seq)], max_chars)?.remove(&call.id).unwrap_or_default();
        Ok(Some(call))
    }

    /// A session's calls in start order. With `new_only`, each call lists only
    /// the parts no earlier call of the session carried, which reads as the
    /// conversation's timeline: what was added, step by step.
    pub fn session(&self, id: &str, max_chars: usize, new_only: bool) -> Result<Option<SessionResponse>, StoreError> {
        let mut calls = Vec::new();
        {
            let connection = self.read();
            let mut statement = connection.prepare(&format!(
                "SELECT {CALL_COLUMNS} FROM calls WHERE session_id = ? ORDER BY started_at, raw_seq LIMIT ?"
            ))?;
            let mut rows = statement.query(duckdb::params![id, MAX_SESSION_CALLS as i64])?;
            while let Some(row) = rows.next()? {
                calls.push(read_call(row)?);
            }
        }
        if calls.is_empty() {
            return Ok(None);
        }
        let keys: Vec<(String, i64)> = calls.iter().map(|(call, seq)| (call.id.clone(), *seq)).collect();
        let mut parts = self.parts_of(&keys, max_chars)?;
        let mut seen = HashSet::new();
        let calls = calls
            .into_iter()
            .map(|(mut call, _)| {
                let all = parts.remove(&call.id).unwrap_or_default();
                call.parts = all
                    .into_iter()
                    .filter(|part| {
                        // Response parts come back in the next request as history.
                        let fresh = seen.insert((part.kind.clone(), part.hash.clone(), part.tool_call_id.clone()));
                        !new_only || fresh
                    })
                    .collect();
                call
            })
            .collect();
        Ok(Some(SessionResponse { session_id: id.to_owned(), calls }))
    }

    pub fn content(&self, hash: &str) -> Result<Option<ContentResponse>, StoreError> {
        let value =
            parse_hash_hex(hash).ok_or_else(|| StoreError::Invalid("a content hash is 32 hex digits".into()))?;
        let connection = self.read();
        let mut statement = connection.prepare("SELECT codec, data FROM contents WHERE hash = ?")?;
        let mut rows = statement.query([duckdb::types::Value::HugeInt(value)])?;
        let Some(row) = rows.next()? else { return Ok(None) };
        let text = decode_content(row.get(0)?, &row.get::<_, Vec<u8>>(1)?)?;
        Ok(Some(ContentResponse { hash: hash.to_ascii_lowercase(), bytes: text.len() as u64, text }))
    }

    pub fn processing_status(&self) -> Result<ProcessingStatus, StoreError> {
        let connection = self.read();
        let processed: i64 = connection
            .query_row(
                "SELECT last_seq FROM projection_checkpoints WHERE projection = ?",
                [format!("calls@{PARSER_VERSION}")],
                |row| row.get(0),
            )
            .unwrap_or(0);
        let latest: i64 =
            connection.query_row("SELECT coalesce(max(seq), 0) FROM raw_exchanges", [], |row| row.get(0))?;
        let calls: i64 = connection.query_row("SELECT count(*) FROM calls", [], |row| row.get(0))?;
        let (contents, content_bytes, stored): (i64, i64, i64) = connection.query_row(
            "SELECT count(*), coalesce(sum(bytes), 0)::BIGINT, coalesce(sum(octet_length(data)), 0)::BIGINT FROM contents",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let mut statement = connection
            .prepare("SELECT api, parse_status, parse_reason, count(*) FROM calls GROUP BY ALL ORDER BY 4 DESC")?;
        let coverage = statement
            .query_map([], |row| {
                Ok(ParseCoverage {
                    api: row.get(0)?,
                    status: row.get(1)?,
                    reason: row.get(2)?,
                    calls: row.get::<_, i64>(3)?.max(0) as u64,
                })
            })?
            .collect::<Result<_, _>>()?;
        let positive = |value: i64| value.max(0) as u64;
        Ok(ProcessingStatus {
            parser_version: PARSER_VERSION,
            processed_seq: positive(processed),
            latest_seq: positive(latest),
            calls: positive(calls),
            contents: positive(contents),
            content_bytes: positive(content_bytes),
            content_stored_bytes: positive(stored),
            coverage,
        })
    }
}
