//! Generates `packages/api/src/generated.ts` from the Rust contract.

use std::fmt::Write;

use ts_rs::{Config, TS};

use crate::{
    ApiError, ClientMessage, Dimension, Direction, EndReason, ErrorCode, Exchange, ExchangeEnd, Frame, HealthResponse,
    IngestRequest, IngestResult, ListQuery, ListResponse, LogEntry, RejectedExchange, ServerMessage, StoredExchange,
    SummaryQuery, SummaryResponse, SummaryRow, Transport, limits, protocols, routes,
};

/// The complete TypeScript module: every wire type plus `ROUTES`, `PROTOCOLS`
/// and `LIMITS`. Deterministic, so a test can assert that the committed file
/// is current.
pub fn typescript_bindings() -> String {
    let mut out =
        String::from("// Generated from crates/api by `cargo run -p ai-ledger-api --bin gen-ts`. Do not edit.\n");
    // Fixed defaults instead of `Config::from_env()`: output must not depend on the environment.
    let cfg = Config::new();
    for declaration in [
        Transport::decl(&cfg),
        Direction::decl(&cfg),
        Frame::decl(&cfg),
        EndReason::decl(&cfg),
        ExchangeEnd::decl(&cfg),
        Exchange::decl(&cfg),
        ClientMessage::decl(&cfg),
        ServerMessage::decl(&cfg),
        IngestRequest::decl(&cfg),
        RejectedExchange::decl(&cfg),
        IngestResult::decl(&cfg),
        LogEntry::decl(&cfg),
        StoredExchange::decl(&cfg),
        ListQuery::decl(&cfg),
        ListResponse::decl(&cfg),
        Dimension::decl(&cfg),
        SummaryQuery::decl(&cfg),
        SummaryRow::decl(&cfg),
        SummaryResponse::decl(&cfg),
        HealthResponse::decl(&cfg),
        ErrorCode::decl(&cfg),
        ApiError::decl(&cfg),
    ] {
        write!(out, "\n{}export {}\n", leading_docs(&declaration), strip_docs(&declaration)).unwrap();
    }

    let dimensions: Vec<String> =
        Dimension::ALL.iter().map(|dimension| format!("\"{}\"", dimension.as_str())).collect();
    write!(out, "\nexport const DIMENSIONS: readonly Dimension[] = [{}];\n", dimensions.join(", ")).unwrap();

    let route_entries = [
        ("health", routes::HEALTH),
        ("ingestWs", routes::INGEST_WS),
        ("ingestHttp", routes::INGEST_HTTP),
        ("listExchanges", routes::LIST_EXCHANGES),
        ("getExchange", routes::GET_EXCHANGE),
        ("summary", routes::SUMMARY),
    ];
    write_constants(&mut out, "ROUTES", route_entries.iter().map(|(name, path)| (*name, format!("\"{path}\""))));
    write_constants(
        &mut out,
        "PROTOCOLS",
        [("json", format!("\"{}\"", protocols::JSON)), ("msgpack", format!("\"{}\"", protocols::MSGPACK))].into_iter(),
    );

    let limit_entries = [
        ("maxBatchSize", limits::MAX_BATCH_SIZE as u64),
        ("maxBatchBytes", limits::MAX_BATCH_BYTES as u64),
        ("maxInFlightBatches", limits::MAX_IN_FLIGHT_BATCHES as u64),
        ("maxIdLen", limits::MAX_ID_LEN as u64),
        ("maxNameLen", limits::MAX_NAME_LEN as u64),
        ("maxUrlLen", limits::MAX_URL_LEN as u64),
        ("maxFrames", limits::MAX_FRAMES as u64),
        ("maxFrameBytes", limits::MAX_FRAME_BYTES as u64),
        ("maxRecordBytes", limits::MAX_RECORD_BYTES as u64),
        ("maxSegment", u64::from(limits::MAX_SEGMENT)),
        ("maxErrorLen", limits::MAX_ERROR_LEN as u64),
        ("maxMetadataEntries", limits::MAX_METADATA_ENTRIES as u64),
        ("maxMetadataKeyLen", limits::MAX_METADATA_KEY_LEN as u64),
        ("maxMetadataValueLen", limits::MAX_METADATA_VALUE_LEN as u64),
        ("maxDurationMs", limits::MAX_DURATION_MS),
        ("defaultListLimit", u64::from(limits::DEFAULT_LIST_LIMIT)),
        ("maxListLimit", u64::from(limits::MAX_LIST_LIMIT)),
        ("defaultSummaryLimit", u64::from(limits::DEFAULT_SUMMARY_LIMIT)),
        ("maxSummaryLimit", u64::from(limits::MAX_SUMMARY_LIMIT)),
    ];
    write_constants(&mut out, "LIMITS", limit_entries.iter().map(|(name, value)| (*name, value.to_string())));
    out
}

fn write_constants<'a>(out: &mut String, name: &str, entries: impl Iterator<Item = (&'a str, String)>) {
    writeln!(out, "\nexport const {name} = {{").unwrap();
    for (key, value) in entries {
        writeln!(out, "    {key}: {value},").unwrap();
    }
    out.push_str("} as const;\n");
}

/// ts-rs places JSDoc before `type`; keep it before `export type`.
fn leading_docs(declaration: &str) -> &str {
    declaration.find("type ").map_or("", |index| &declaration[..index])
}

fn strip_docs(declaration: &str) -> &str {
    declaration.find("type ").map_or(declaration, |index| &declaration[index..])
}
