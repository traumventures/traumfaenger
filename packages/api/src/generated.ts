// Generated from crates/api by `cargo run -p traumfaenger-api --bin gen-ts`. Do not edit.

export type Transport = "http" | "sse" | "ws";

export type Direction = "out" | "in";

export type Frame = { 
/**
 * Milliseconds since `started_at` when the frame was sent or complete.
 */
t_ms: number, dir: Direction, data: string, };

export type EndReason = "complete" | "error" | "cancelled" | "closed";

export type ExchangeEnd = { 
/**
 * Milliseconds since `started_at`.
 */
at_ms: number, reason: EndReason, error?: string, 
/**
 * WebSocket close code.
 */
close_code?: number, };

export type Exchange = { 
/**
 * Client-generated unique id, for example a UUID. 1–128 bytes.
 */
id: string, 
/**
 * 0 unless a WebSocket session is sent in parts.
 */
segment?: number, 
/**
 * False on every segment but the last. Defaults to true.
 */
final?: boolean, 
/**
 * The emitting service, for example `my-service`. Environments are kept
 * apart by running one database per environment.
 */
service: string, 
/**
 * The kind of work, for example `listing.parse`.
 */
operation: string, 
/**
 * Groups the exchanges of one conversation, for example one support chat.
 */
session_id?: string, 
/**
 * Flat key-value pairs, for example `{ "listing_id": "123" }`. At most 32
 * entries; keys are 1–64 characters of `A-Z a-z 0-9 _ . -`.
 */
metadata?: Record<string, string>, transport: Transport, 
/**
 * The request URL. The server drops query string and fragment.
 */
url: string, 
/**
 * When the exchange started, RFC 3339. The same for every segment.
 */
started_at: string, 
/**
 * HTTP status of the response; 101 once a WebSocket is open. Absent
 * when no response arrived.
 */
status?: number, 
/**
 * The frames of this record, in wire order. For HTTP and SSE the request
 * body is the first `out` frame.
 */
frames: Array<Frame>, 
/**
 * Present exactly on final records.
 */
end?: ExchangeEnd, 
/**
 * True when the client changed anything to fit the limits: clipped or
 * dropped frames, or cleaned context (invalid metadata entries dropped,
 * values clipped, an empty operation recorded as `unattributed`).
 */
truncated?: boolean, };

export type ClientMessage = { "type": "batch", 
/**
 * Chosen by the client, echoed in the ack.
 */
id: number, exchanges: Array<Exchange>, };

export type ServerMessage = { "type": "ack", batch: number, 
/**
 * Records stored by this batch.
 */
accepted: number, 
/**
 * Records already stored, or repeated within the batch.
 */
duplicates: number, 
/**
 * Records that failed validation. Resending them will not succeed.
 */
rejected: Array<RejectedExchange>, } | { "type": "error", batch: number | null, error: ErrorCode, message: string, };

export type IngestRequest = { exchanges: Array<Exchange>, };

export type RejectedExchange = { 
/**
 * Position in the batch.
 */
index: number, reason: string, };

export type IngestResult = { 
/**
 * Records stored by this batch.
 */
accepted: number, 
/**
 * Records already stored, or repeated within the batch.
 */
duplicates: number, 
/**
 * Records that failed validation. Resending them will not succeed.
 */
rejected: Array<RejectedExchange>, };

export type LogEntry = { 
/**
 * Position in the raw log; increases with every stored record.
 */
seq: number, id: string, segment: number, final: boolean, 
/**
 * When the server stored the record, RFC 3339.
 */
received_at: string, service: string, operation: string, session_id: string | null, metadata: Record<string, string>, transport: Transport, 
/**
 * Without query string and fragment.
 */
url: string, started_at: string, status: number | null, end: ExchangeEnd | null, truncated: boolean, frame_count: number, 
/**
 * Bytes of frame data.
 */
bytes: number, };

export type StoredExchange = { id: string, service: string, operation: string, session_id: string | null, metadata: Record<string, string>, transport: Transport, url: string, started_at: string, 
/**
 * Latest status reported by any segment.
 */
status: number | null, 
/**
 * Number of stored segments.
 */
segments: number, 
/**
 * True once the final segment is stored and no segment is missing.
 */
complete: boolean, truncated: boolean, 
/**
 * Frames of all segments in order.
 */
frames: Array<Frame>, end: ExchangeEnd | null, };

export type ListQuery = { service?: string, operation?: string, session_id?: string, transport?: Transport, 
/**
 * True: only final records of failed exchanges (end reason `error`, no
 * status, or status ≥ 400). False: everything else.
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

export type ListResponse = { entries: Array<LogEntry>, 
/**
 * Pass as `cursor` to fetch the next page. `null` on the last page.
 */
next_cursor: string | null, };

export type Dimension = "service" | "operation" | "session_id" | "transport" | "status" | "day" | "hour";

export type SummaryQuery = { service?: string, operation?: string, session_id?: string, transport?: Transport, failed?: boolean, from?: string, to?: string, 
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
 * Value of each requested dimension. `null` when the records lack it.
 */
group: { [key in Dimension]?: string | null }, 
/**
 * Distinct exchanges.
 */
exchanges: number, 
/**
 * Failed exchanges (final record with end reason `error`, no status or status ≥ 400).
 */
failed: number, 
/**
 * Stored records; more than `exchanges` when WebSocket sessions were segmented.
 */
records: number, frames: number, bytes: number, 
/**
 * Duration (`end.at_ms`) of finished exchanges.
 */
duration_ms_avg: number | null, duration_ms_p95: number | null, duration_ms_p99: number | null, };

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

export const DIMENSIONS: readonly Dimension[] = ["service", "operation", "session_id", "transport", "status", "day", "hour"];

export const ROUTES = {
    health: "/health",
    ingestWs: "/v1/ingest",
    ingestHttp: "/v1/exchanges",
    listExchanges: "/v1/exchanges",
    getExchange: "/v1/exchanges/{id}",
    summary: "/v1/summary",
} as const;

export const PROTOCOLS = {
    json: "traumfaenger.v1.json",
    msgpack: "traumfaenger.v1.msgpack",
} as const;

export const LIMITS = {
    maxBatchSize: 500,
    maxBatchBytes: 33554432,
    maxInFlightBatches: 4,
    maxIdLen: 128,
    maxNameLen: 256,
    maxUrlLen: 2048,
    maxFrames: 10000,
    maxFrameBytes: 4194304,
    maxRecordBytes: 8388608,
    maxSegment: 100000,
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
