//! Generates `packages/api/src/generated.ts` from the Rust contract.

use std::fmt::Write;

use ts_rs::{Config, TS};

use crate::{
    ApiError, CallEnvelope, CallRecord, Dimension, ErrorCode, HealthResponse, IngestRequest, IngestResponse,
    ListCallsQuery, ListCallsResponse, RawResponse, RejectedCall, StoredCall, SummaryQuery, SummaryResponse,
    SummaryRow, limits, routes,
};

/// The complete TypeScript module: every wire type plus `ROUTES` and `LIMITS`.
/// Deterministic, so a test can assert that the committed file is current.
pub fn typescript_bindings() -> String {
    let mut out =
        String::from("// Generated from crates/api by `cargo run -p ai-ledger-api --bin gen-ts`. Do not edit.\n");
    // Fixed defaults instead of `Config::from_env()`: output must not depend on the environment.
    let cfg = Config::new();
    for declaration in [
        RawResponse::decl(&cfg),
        CallRecord::decl(&cfg),
        IngestRequest::decl(&cfg),
        RejectedCall::decl(&cfg),
        IngestResponse::decl(&cfg),
        CallEnvelope::decl(&cfg),
        StoredCall::decl(&cfg),
        ListCallsQuery::decl(&cfg),
        ListCallsResponse::decl(&cfg),
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
        ("ingestCalls", routes::INGEST_CALLS),
        ("listCalls", routes::LIST_CALLS),
        ("getCall", routes::GET_CALL),
        ("summary", routes::SUMMARY),
    ];
    out.push_str("\nexport const ROUTES = {\n");
    for (name, path) in route_entries {
        writeln!(out, "    {name}: \"{path}\",").unwrap();
    }
    out.push_str("} as const;\n");

    let limit_entries = [
        ("maxBatchSize", limits::MAX_BATCH_SIZE as u64),
        ("maxBodyBytes", limits::MAX_BODY_BYTES as u64),
        ("maxIdLen", limits::MAX_ID_LEN as u64),
        ("maxNameLen", limits::MAX_NAME_LEN as u64),
        ("maxUrlLen", limits::MAX_URL_LEN as u64),
        ("maxRequestBytes", limits::MAX_REQUEST_BYTES as u64),
        ("maxResponseBytes", limits::MAX_RESPONSE_BYTES as u64),
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
    out.push_str("\nexport const LIMITS = {\n");
    for (name, value) in limit_entries {
        writeln!(out, "    {name}: {value},").unwrap();
    }
    out.push_str("} as const;\n");
    out
}

/// ts-rs places JSDoc before `type`; keep it before `export type`.
fn leading_docs(declaration: &str) -> &str {
    declaration.find("type ").map_or("", |index| &declaration[..index])
}

fn strip_docs(declaration: &str) -> &str {
    declaration.find("type ").map_or(declaration, |index| &declaration[index..])
}
