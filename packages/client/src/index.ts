import {
    type AiCall,
    type ApiError,
    type ErrorCode,
    type HealthResponse,
    type IngestResponse,
    LIMITS,
    type ListCallsQuery,
    type ListCallsResponse,
    type NewAiCall,
    ROUTES,
    type SummaryQuery,
    type SummaryResponse,
} from "@traumwohnung/ai-ledger-api";

export type * from "@traumwohnung/ai-ledger-api";
export { DIMENSIONS, LIMITS, ROUTES } from "@traumwohnung/ai-ledger-api";

export interface ClientOptions {
    /** Base URL of the listener, for example `http://ai-ledger:7600`. */
    baseUrl: string;
    /** Bearer token of this API. Omit only when the server runs with auth disabled. */
    token?: string;
    /** Per-request timeout. Defaults to 10 seconds. */
    timeoutMs?: number;
    /** Custom fetch, for tests or runtimes without a global one. */
    fetch?: typeof fetch;
}

/** A non-2xx response, or a request that never got one (`code` is `network`). */
export class AiLedgerError extends Error {
    readonly status: number;
    readonly code: ErrorCode | "network";

    constructor(status: number, code: ErrorCode | "network", message: string, options?: ErrorOptions) {
        super(message, options);
        this.name = "AiLedgerError";
        this.status = status;
        this.code = code;
    }
}

class Transport {
    readonly #baseUrl: string;
    readonly #token: string | undefined;
    readonly #timeoutMs: number;
    readonly #fetch: typeof fetch;

    constructor(options: ClientOptions) {
        this.#baseUrl = options.baseUrl.replace(/\/+$/, "");
        this.#token = options.token;
        this.#timeoutMs = options.timeoutMs ?? 10_000;
        this.#fetch = options.fetch ?? globalThis.fetch;
    }

    async request<T>(method: "GET" | "POST", path: string, body?: unknown): Promise<T> {
        const headers: Record<string, string> = { accept: "application/json" };
        if (this.#token !== undefined) headers.authorization = `Bearer ${this.#token}`;
        if (body !== undefined) headers["content-type"] = "application/json";

        let response: Response;
        try {
            response = await this.#fetch(`${this.#baseUrl}${path}`, {
                method,
                headers,
                body: body === undefined ? null : JSON.stringify(body),
                signal: AbortSignal.timeout(this.#timeoutMs),
            });
        } catch (cause) {
            throw new AiLedgerError(0, "network", `ai-ledger ${method} ${path} failed: ${String(cause)}`, { cause });
        }
        if (response.ok) return (await response.json()) as T;

        const text = await response.text();
        let error: ApiError | undefined;
        try {
            error = JSON.parse(text) as ApiError;
        } catch {
            // Not an ai-ledger error body, for example from a proxy in front of it.
        }
        throw new AiLedgerError(
            response.status,
            error?.error ?? (response.status >= 500 ? "internal" : "invalid_request"),
            error?.message ?? `ai-ledger ${method} ${path} returned ${response.status}: ${text.slice(0, 200)}`
        );
    }
}

/** Client of the write-only ingestion API. */
export class IngestClient {
    readonly #transport: Transport;

    constructor(options: ClientOptions) {
        this.#transport = new Transport(options);
    }

    /**
     * Records calls. Inputs are sent in sequential batches that respect both
     * `LIMITS.maxBatchSize` and `LIMITS.maxBodyBytes`; each batch is atomic,
     * and retrying is safe because call ids are idempotency keys.
     */
    async ingest(calls: readonly NewAiCall[]): Promise<IngestResponse> {
        const total: IngestResponse = { accepted: 0, duplicates: 0 };
        for (const batch of batches(calls)) {
            const result = await this.#transport.request<IngestResponse>("POST", ROUTES.ingestCalls, { calls: batch });
            total.accepted += result.accepted;
            total.duplicates += result.duplicates;
        }
        return total;
    }

    health(): Promise<HealthResponse> {
        return this.#transport.request("GET", ROUTES.health);
    }
}

/** Client of the read-only query API. */
export class QueryClient {
    readonly #transport: Transport;

    constructor(options: ClientOptions) {
        this.#transport = new Transport(options);
    }

    /** One page of calls, newest first. */
    listCalls(query: ListCallsQuery = {}): Promise<ListCallsResponse> {
        return this.#transport.request("GET", `${ROUTES.listCalls}${toSearch(query)}`);
    }

    /** Every call matching `query`, following `next_cursor` page by page. */
    async *iterateCalls(query: Omit<ListCallsQuery, "cursor"> = {}): AsyncGenerator<AiCall> {
        let cursor: string | null = null;
        do {
            const page: ListCallsResponse = await this.listCalls(cursor === null ? query : { ...query, cursor });
            yield* page.calls;
            cursor = page.next_cursor;
        } while (cursor !== null);
    }

    /** One call, or `null` when no call has this id. */
    async getCall(id: string): Promise<AiCall | null> {
        try {
            return await this.#transport.request("GET", ROUTES.getCall.replace("{id}", encodeURIComponent(id)));
        } catch (error) {
            if (error instanceof AiLedgerError && error.code === "not_found") return null;
            throw error;
        }
    }

    /** Aggregated usage, highest cost first. */
    summary(query: SummaryQuery = {}): Promise<SummaryResponse> {
        return this.#transport.request("GET", `${ROUTES.summary}${toSearch(query)}`);
    }

    health(): Promise<HealthResponse> {
        return this.#transport.request("GET", ROUTES.health);
    }
}

const encoder = new TextEncoder();
/** `{"calls":[` + `]}` */
const ENVELOPE_BYTES = 12;

/** Splits calls into request-sized batches by count and serialized size. */
export function* batches(calls: readonly NewAiCall[]): Generator<NewAiCall[]> {
    let batch: NewAiCall[] = [];
    let bytes = ENVELOPE_BYTES;
    for (const call of calls) {
        // +1 for the separating comma.
        const size = encoder.encode(JSON.stringify(call)).byteLength + 1;
        if (batch.length > 0 && (batch.length >= LIMITS.maxBatchSize || bytes + size > LIMITS.maxBodyBytes)) {
            yield batch;
            batch = [];
            bytes = ENVELOPE_BYTES;
        }
        batch.push(call);
        bytes += size;
    }
    if (batch.length > 0) yield batch;
}

/** Encodes a query object the way the server's `Query` extractor expects. */
export function toSearch(query: ListCallsQuery | SummaryQuery): string {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(query)) {
        if (value === undefined) continue;
        if (Array.isArray(value)) {
            if (value.length > 0) params.set(key, value.join(","));
        } else {
            params.set(key, String(value));
        }
    }
    const search = params.toString();
    return search === "" ? "" : `?${search}`;
}
