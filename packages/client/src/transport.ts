import type {
    ApiError,
    ErrorCode,
    HealthResponse,
    ListQuery,
    ListResponse,
    LogEntry,
    StoredExchange,
    SummaryQuery,
    SummaryResponse,
} from "@traumwohnung/ai-ledger-api";
import { ROUTES } from "@traumwohnung/ai-ledger-api";

export interface ClientOptions {
    /** Base URL of the query listener, for example `http://ai-ledger:7601`. */
    baseUrl: string;
    /** Bearer token of the query API. Omit only when the server runs with auth disabled. */
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

/** Client of the read-only query API. */
export class QueryClient {
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

    /** One page of the raw log (records without frames), most recently stored first. */
    list(query: ListQuery = {}): Promise<ListResponse> {
        return this.#get(`${ROUTES.listExchanges}${toSearch(query)}`);
    }

    /** Every log entry matching `query`, following `next_cursor` page by page. */
    async *iterate(query: Omit<ListQuery, "cursor"> = {}): AsyncGenerator<LogEntry> {
        let cursor: string | null = null;
        do {
            const page: ListResponse = await this.list(cursor === null ? query : { ...query, cursor });
            yield* page.entries;
            cursor = page.next_cursor;
        } while (cursor !== null);
    }

    /** One exchange with all segments and frames, or `null` when the id is unknown. */
    async getExchange(id: string): Promise<StoredExchange | null> {
        try {
            return await this.#get(ROUTES.getExchange.replace("{id}", encodeURIComponent(id)));
        } catch (error) {
            if (error instanceof AiLedgerError && error.code === "not_found") return null;
            throw error;
        }
    }

    /** Collection statistics, most exchanges first. */
    summary(query: SummaryQuery = {}): Promise<SummaryResponse> {
        return this.#get(`${ROUTES.summary}${toSearch(query)}`);
    }

    health(): Promise<HealthResponse> {
        return this.#get(ROUTES.health);
    }

    async #get<T>(path: string): Promise<T> {
        const headers: Record<string, string> = { accept: "application/json" };
        if (this.#token !== undefined) headers.authorization = `Bearer ${this.#token}`;
        let response: Response;
        try {
            response = await this.#fetch(`${this.#baseUrl}${path}`, {
                headers,
                signal: AbortSignal.timeout(this.#timeoutMs),
            });
        } catch (cause) {
            throw new AiLedgerError(0, "network", `ai-ledger GET ${path} failed: ${String(cause)}`, { cause });
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
            error?.message ?? `ai-ledger GET ${path} returned ${response.status}: ${text.slice(0, 200)}`
        );
    }
}

/** Encodes a query object the way the server's `Query` extractor expects. */
export function toSearch(query: ListQuery | SummaryQuery): string {
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
