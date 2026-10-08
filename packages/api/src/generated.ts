// Generated from crates/api by `cargo run -p ai-ledger-api --bin gen-ts`. Do not edit.

export type CallStatus = "ok" | "error";

export type NewAiCall = { 
/**
 * Caller-generated unique id, for example a UUID. 1–128 bytes.
 */
id: string, 
/**
 * When the call started, RFC 3339.
 */
occurred_at: string, 
/**
 * What the call was for, for example `listing.parse`.
 */
feature: string, 
/**
 * Kind of the object the call worked on, for example `listing`.
 */
entity_type?: string, 
/**
 * Id of the object the call worked on.
 */
entity_id?: string, 
/**
 * Groups calls that belong to one logical operation, for example retries.
 */
trace_id?: string, 
/**
 * Who served the call, for example `openrouter`.
 */
provider: string, 
/**
 * Model identifier as reported by the provider.
 */
model: string, reasoning_effort?: string, status: CallStatus, 
/**
 * Error message when `status` is `error`.
 */
error?: string, 
/**
 * 1-based attempt number within `trace_id`. Defaults to 1.
 */
attempt?: number, 
/**
 * Prompt tokens, including cached ones. Defaults to 0.
 */
input_tokens?: number, 
/**
 * Completion tokens, including reasoning ones. Defaults to 0.
 */
output_tokens?: number, 
/**
 * Subset of `input_tokens` served from the provider cache. Defaults to 0.
 */
cached_input_tokens?: number, 
/**
 * Subset of `output_tokens` spent on reasoning. Defaults to 0.
 */
reasoning_tokens?: number, 
/**
 * Cost billed by the provider in USD. Omit when unknown; never estimate.
 */
cost_usd?: number, 
/**
 * Wall-clock duration of the call in milliseconds.
 */
latency_ms?: number, 
/**
 * Free-form JSON object for anything else. At most 8 KiB.
 */
metadata?: Record<string, unknown>, };

export type AiCall = { id: string, occurred_at: string, 
/**
 * When the ledger first accepted the call, RFC 3339.
 */
received_at: string, feature: string, entity_type: string | null, entity_id: string | null, trace_id: string | null, provider: string, model: string, reasoning_effort: string | null, status: CallStatus, error: string | null, attempt: number, input_tokens: number, output_tokens: number, cached_input_tokens: number, reasoning_tokens: number, cost_usd: number | null, latency_ms: number | null, metadata: Record<string, unknown> | null, };

export type IngestRequest = { 
/**
 * 1–1000 calls. The batch is validated as a whole: one invalid call
 * rejects the request and nothing is stored.
 */
calls: Array<NewAiCall>, };

export type IngestResponse = { 
/**
 * Calls stored by this request.
 */
accepted: number, 
/**
 * Calls whose id was already stored, or repeated within the batch.
 */
duplicates: number, };

export type ListCallsQuery = { feature?: string, entity_type?: string, entity_id?: string, trace_id?: string, provider?: string, model?: string, status?: CallStatus, from?: string, to?: string, 
/**
 * Page size, 1–1000. Defaults to 100.
 */
limit?: number, 
/**
 * `next_cursor` of the previous page.
 */
cursor?: string, };

export type ListCallsResponse = { calls: Array<AiCall>, 
/**
 * Pass as `cursor` to fetch the next page. `null` on the last page.
 */
next_cursor: string | null, };

export type Dimension = "feature" | "entity_type" | "entity_id" | "trace_id" | "provider" | "model" | "reasoning_effort" | "status" | "day" | "hour";

export type SummaryQuery = { feature?: string, entity_type?: string, entity_id?: string, trace_id?: string, provider?: string, model?: string, status?: CallStatus, from?: string, to?: string, 
/**
 * Dimensions to group by, at most once each. Sent as a comma-separated
 * query parameter (`group_by=feature,model`). Empty means one total row.
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
group: { [key in Dimension]?: string | null }, calls: number, errors: number, input_tokens: number, output_tokens: number, cached_input_tokens: number, reasoning_tokens: number, 
/**
 * Sum of the known billed costs.
 */
cost_usd: number, 
/**
 * Calls without a billed cost; `cost_usd` excludes them.
 */
calls_without_cost: number, 
/**
 * Mean and 95th percentile of the known latencies.
 */
latency_ms_avg: number | null, latency_ms_p95: number | null, };

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

export const DIMENSIONS: readonly Dimension[] = ["feature", "entity_type", "entity_id", "trace_id", "provider", "model", "reasoning_effort", "status", "day", "hour"];

export const ROUTES = {
    health: "/health",
    ingestCalls: "/v1/calls",
    listCalls: "/v1/calls",
    getCall: "/v1/calls/{id}",
    summary: "/v1/summary",
} as const;

export const LIMITS = {
    maxBatchSize: 1000,
    maxBodyBytes: 8388608,
    maxIdLen: 128,
    maxTextLen: 512,
    maxErrorLen: 4000,
    maxMetadataBytes: 8192,
    maxCount: 1000000000000,
    maxAttempt: 1000,
    maxCostUsd: 1000000,
    defaultListLimit: 100,
    maxListLimit: 1000,
    defaultSummaryLimit: 1000,
    maxSummaryLimit: 10000,
} as const;
