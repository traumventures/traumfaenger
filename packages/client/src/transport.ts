import type {
    ApiError,
    CallRecord,
    ErrorCode,
    HealthResponse,
    IngestResponse,
    ListCallsQuery,
    ListCallsResponse,
    StoredCall,
    SummaryQuery,
    SummaryResponse,
} from "@traumwohnung/ai-ledger-api";
import { ROUTES } from "@traumwohnung/ai-ledger-api";

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

export interface RequestOptions {
    body?: BodyInit;
    headers?: Record<string, string>;
    /** Aborts the request in addition to the timeout. */
    signal?: AbortSignal | undefined;
}

export class Transport {
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

    async request<T>(method: "GET" | "POST", path: string, options: RequestOptions = {}): Promise<T> {
        const headers: Record<string, string> = { accept: "application/json", ...options.headers };
        if (this.#token !== undefined) headers.authorization = `Bearer ${this.#token}`;

        let response: Response;
        try {
            response = await this.#fetch(`${this.#baseUrl}${path}`, {
                method,
                headers,
                body: options.body ?? null,
                signal:
                    options.signal === undefined
                        ? AbortSignal.timeout(this.#timeoutMs)
                        : AbortSignal.any([AbortSignal.timeout(this.#timeoutMs), options.signal]),
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

/**
 * Low-level client of the write-only ingestion API. Most code should use
 * `createLedger`, which records calls and sends them in the background.
 */
export class IngestClient {
    readonly #transport: Transport;

    constructor(options: ClientOptions) {
        this.#transport = new Transport(options);
    }

    /** Sends one batch of at most `LIMITS.maxBatchSize` records. */
    ingest(calls: readonly CallRecord[]): Promise<IngestResponse> {
        return this.#transport.request("POST", ROUTES.ingestCalls, {
            body: JSON.stringify({ calls }),
            headers: { "content-type": "application/json" },
        });
    }

    /** Sends an already serialized (and optionally gzip-compressed) batch body. */
    ingestSerialized(body: Uint8Array<ArrayBuffer>, gzip: boolean, signal?: AbortSignal): Promise<IngestResponse> {
        const headers: Record<string, string> = { "content-type": "application/json" };
        if (gzip) headers["content-encoding"] = "gzip";
        return this.#transport.request("POST", ROUTES.ingestCalls, { body, headers, signal });
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

    /** One page of call envelopes (without bodies), most recently stored first. */
    listCalls(query: ListCallsQuery = {}): Promise<ListCallsResponse> {
        return this.#transport.request("GET", `${ROUTES.listCalls}${toSearch(query)}`);
    }

    /** Every envelope matching `query`, following `next_cursor` page by page. */
    async *iterateCalls(query: Omit<ListCallsQuery, "cursor"> = {}): AsyncGenerator<ListCallsResponse["calls"][number]> {
        let cursor: string | null = null;
        do {
            const page: ListCallsResponse = await this.listCalls(cursor === null ? query : { ...query, cursor });
            yield* page.calls;
            cursor = page.next_cursor;
        } while (cursor !== null);
    }

    /** One call including request and response, or `null` when no call has this id. */
    async getCall(id: string): Promise<StoredCall | null> {
        try {
            return await this.#transport.request("GET", ROUTES.getCall.replace("{id}", encodeURIComponent(id)));
        } catch (error) {
            if (error instanceof AiLedgerError && error.code === "not_found") return null;
            throw error;
        }
    }

    /** Collection statistics, most calls first. */
    summary(query: SummaryQuery = {}): Promise<SummaryResponse> {
        return this.#transport.request("GET", `${ROUTES.summary}${toSearch(query)}`);
    }

    health(): Promise<HealthResponse> {
        return this.#transport.request("GET", ROUTES.health);
    }
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
