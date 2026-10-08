// Generated from crates/api by `cargo run -p ai-ledger-api --bin gen-ts`. Do not edit.

export type RawResponse = string | Array<string>;

export type CallRecord = { 
/**
 * Client-generated unique id, for example a UUID. 1–128 bytes.
 */
id: string, 
/**
 * The emitting service, for example `traumwohnung-web`.
 */
service: string, 
/**
 * For example `production`, `dev` or `local`.
 */
environment: string, 
/**
 * The kind of work, for example `listing.parse`.
 */
operation: string, 
/**
 * Groups the calls of one conversation, for example one support chat.
 */
session_id?: string, 
/**
 * Flat key-value pairs, for example `{ "listing_id": "123" }`. At most 32
 * entries; keys are 1–64 characters of `A-Z a-z 0-9 _ . -`.
 */
metadata?: Record<string, string>, 
/**
 * The request URL. The server drops query string and fragment.
 */
url: string, 
/**
 * The request body exactly as sent.
 */
request: string, 
/**
 * The response as received. Absent when no response arrived.
 */
response?: RawResponse, 
/**
 * HTTP status of the response. Absent when no response arrived.
 */
http_status?: number, 
/**
 * Client-side exception, for example a network failure or an abort.
 */
error?: string, 
/**
 * True when the client changed anything to fit the limits: clipped
 * `request` or `response`, or cleaned context (dropped invalid metadata
 * entries, clipped values, an empty operation recorded as `unattributed`).
 */
truncated?: boolean, 
/**
 * When the request was sent, RFC 3339.
 */
started_at: string, 
/**
 * Streaming calls only: time until the first response chunk, in milliseconds.
 */
ttft_ms?: number, 
/**
 * Time until the response was complete (or failed), in milliseconds.
 */
latency_ms: number, };

export type IngestRequest = { 
/**
 * 1–500 records. Each record is validated on its own: invalid records are
 * reported in `rejected`, the valid ones are stored.
 */
calls: Array<CallRecord>, };

export type RejectedCall = { 
/**
 * Position of the record in `calls`.
 */
index: number, reason: string, };

export type IngestResponse = { 
/**
 * Records stored by this request.
 */
accepted: number, 
/**
 * Records whose id was already stored, or repeated within the request.
 */
duplicates: number, 
/**
 * Records that failed validation. Retrying them will not succeed.
 */
rejected: Array<RejectedCall>, };

export type CallEnvelope = { 
/**
 * Position in the ingestion queue; increases with every stored record.
 */
seq: number, id: string, 
/**
 * When the server stored the record, RFC 3339.
 */
received_at: string, service: string, environment: string, operation: string, session_id: string | null, metadata: Record<string, string>, 
/**
 * Without query string and fragment.
 */
url: string, http_status: number | null, error: string | null, truncated: boolean, started_at: string, ttft_ms: number | null, latency_ms: number, request_bytes: number, response_bytes: number, };

export type StoredCall = { request: string, response: RawResponse | null, 
/**
 * Position in the ingestion queue; increases with every stored record.
 */
seq: number, id: string, 
/**
 * When the server stored the record, RFC 3339.
 */
received_at: string, service: string, environment: string, operation: string, session_id: string | null, metadata: Record<string, string>, 
/**
 * Without query string and fragment.
 */
url: string, http_status: number | null, error: string | null, truncated: boolean, started_at: string, ttft_ms: number | null, latency_ms: number, request_bytes: number, response_bytes: number, };

export type ListCallsQuery = { service?: string, environment?: string, operation?: string, session_id?: string, 
/**
 * True: only calls without a response, with status ≥ 400 or with a
 * client error. False: only the others.
 */
failed?: boolean, from?: string, to?: string, 
/**
 * Page size, 1–1000. Defaults to 100.
 */
limit?: number, 
/**
 * `next_cursor` of the previous page.
 */
cursor?: string, };

export type ListCallsResponse = { calls: Array<CallEnvelope>, 
/**
 * Pass as `cursor` to fetch the next page. `null` on the last page.
 */
next_cursor: string | null, };

export type Dimension = "service" | "environment" | "operation" | "session_id" | "http_status" | "day" | "hour";

export type SummaryQuery = { service?: string, environment?: string, operation?: string, session_id?: string, failed?: boolean, from?: string, to?: string, 
/**
 * Dimensions to group by, at most once each. Sent as a comma-separated
 * query parameter (`group_by=service,operation`). Empty means one total row.
 */
group_by?: Array<Dimension>, 
/**
 * Maximum number of rows, 1–10000. Defaults to 1000.
 */
limit?: number, };

export type SummaryRow = { 
/**
 * Value of each requested dimension. `null` when the calls lack it.
 */
group: { [key in Dimension]?: string | null }, calls: number, 
/**
 * Calls without a response, with status ≥ 400 or with a client error.
 */
failed: number, request_bytes: number, response_bytes: number, latency_ms_avg: number | null, latency_ms_p95: number | null, latency_ms_p99: number | null, ttft_ms_p95: number | null, };

export type SummaryResponse = { rows: Array<SummaryRow>, 
/**
 * True when more groups exist than `limit` returned.
 */
truncated: boolean, };

export type HealthResponse = { status: string, 
/**
 * Which listener answered: `ingest` or `query`.
 */
api: string, version: string, };

export type ErrorCode = "invalid_request" | "unauthorized" | "not_found" | "method_not_allowed" | "payload_too_large" | "internal";

export type ApiError = { error: ErrorCode, message: string, };

export const DIMENSIONS: readonly Dimension[] = ["service", "environment", "operation", "session_id", "http_status", "day", "hour"];

export const ROUTES = {
    health: "/health",
    ingestCalls: "/v1/calls",
    listCalls: "/v1/calls",
    getCall: "/v1/calls/{id}",
    summary: "/v1/summary",
} as const;

export const LIMITS = {
    maxBatchSize: 500,
    maxBodyBytes: 33554432,
    maxIdLen: 128,
    maxNameLen: 256,
    maxUrlLen: 2048,
    maxRequestBytes: 4194304,
    maxResponseBytes: 4194304,
    maxErrorLen: 4000,
    maxMetadataEntries: 32,
    maxMetadataKeyLen: 64,
    maxMetadataValueLen: 512,
    maxDurationMs: 86400000,
    defaultListLimit: 100,
    maxListLimit: 1000,
    defaultSummaryLimit: 1000,
    maxSummaryLimit: 10000,
} as const;
