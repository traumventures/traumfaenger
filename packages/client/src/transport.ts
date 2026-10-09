import type {
    AnalyticsResponse,
    ApiError,
    CallDetail,
    ContentResponse,
    ErrorCode,
    HealthResponse,
    ListQuery,
    ListResponse,
    LogEntry,
    ProcessingStatus,
    SessionResponse,
    StoredExchange,
    SummaryQuery,
    SummaryResponse,
} from "@traumfaenger/api";
import { ROUTES } from "@traumfaenger/api";

export interface ClientOptions {
    /** Base URL of the query listener, for example `http://traumfaenger:7601`. */
    baseUrl: string;
    /** Bearer token of the query API. Omit only when the server runs with auth disabled. */
    token?: string;
    /** Per-request timeout. Defaults to 10 seconds. */
    timeoutMs?: number;
    /** Custom fetch, for tests or runtimes without a global one. */
    fetch?: typeof fetch;
}

/** A non-2xx response, or a request that never got one (`code` is `network`). */
export class TraumfaengerError extends Error {
    readonly status: number;
    readonly code: ErrorCode | "network";

    constructor(status: number, code: ErrorCode | "network", message: string, options?: ErrorOptions) {
        super(message, options);
        this.name = "TraumfaengerError";
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
            if (error instanceof TraumfaengerError && error.code === "not_found") return null;
            throw error;
        }
    }

    /** Collection statistics, most exchanges first. */
    summary(query: SummaryQuery = {}): Promise<SummaryResponse> {
        return this.#get(`${ROUTES.summary}${toSearch(query)}`);
    }

    /**
     * Metrics over processed data; see `AnalyticsResponse` for every parameter.
     * Dimension and `metadata.<key>` filters go into `filters`.
     */
    analytics(source: AnalyticsSource, query: AnalyticsQuery): Promise<AnalyticsResponse> {
        const params = new URLSearchParams();
        params.set("metrics", query.metrics.join(","));
        if (query.groupBy?.length) params.set("group_by", query.groupBy.join(","));
        if (query.interval !== undefined) params.set("interval", query.interval);
        if (query.from !== undefined) params.set("from", toTime(query.from));
        if (query.to !== undefined) params.set("to", toTime(query.to));
        if (query.order !== undefined) params.set("order", query.order);
        if (query.limit !== undefined) params.set("limit", String(query.limit));
        for (const [name, value] of Object.entries(query.filters ?? {})) {
            params.set(name, Array.isArray(value) ? value.join(",") : value);
        }
        return this.#get(`${ROUTES.analytics.replace("{source}", source)}?${params}`);
    }

    /** One processed call with its message parts, or `null` when unknown. */
    call(id: string, options: { maxChars?: number } = {}): Promise<CallDetail | null> {
        const search = options.maxChars === undefined ? "" : `?max_chars=${options.maxChars}`;
        return this.#orNull(`${ROUTES.call.replace("{id}", encodeURIComponent(id))}${search}`);
    }

    /**
     * A session's calls in order. By default each call lists only the parts it
     * added to the conversation; `newOnly: false` lists every part.
     */
    session(id: string, options: { maxChars?: number; newOnly?: boolean } = {}): Promise<SessionResponse | null> {
        const params = new URLSearchParams();
        if (options.maxChars !== undefined) params.set("max_chars", String(options.maxChars));
        if (options.newOnly !== undefined) params.set("new_only", String(options.newOnly));
        const search = params.size === 0 ? "" : `?${params}`;
        return this.#orNull(`${ROUTES.session.replace("{id}", encodeURIComponent(id))}${search}`);
    }

    /** A stored text by its hash, or `null` when unknown. */
    content(hash: string): Promise<ContentResponse | null> {
        return this.#orNull(ROUTES.content.replace("{hash}", encodeURIComponent(hash)));
    }

    /** Processing progress and parse coverage. */
    processing(): Promise<ProcessingStatus> {
        return this.#get(ROUTES.processing);
    }

    health(): Promise<HealthResponse> {
        return this.#get(ROUTES.health);
    }

    async #orNull<T>(path: string): Promise<T | null> {
        try {
            return await this.#get<T>(path);
        } catch (error) {
            if (error instanceof TraumfaengerError && error.code === "not_found") return null;
            throw error;
        }
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
            throw new TraumfaengerError(0, "network", `traumfaenger GET ${path} failed: ${String(cause)}`, { cause });
        }
        if (response.ok) return (await response.json()) as T;
        const text = await response.text();
        let error: ApiError | undefined;
        try {
            error = JSON.parse(text) as ApiError;
        } catch {
            // Not an traumfaenger error body, for example from a proxy in front of it.
        }
        throw new TraumfaengerError(
            response.status,
            error?.error ?? (response.status >= 500 ? "internal" : "invalid_request"),
            error?.message ?? `traumfaenger GET ${path} returned ${response.status}: ${text.slice(0, 200)}`
        );
    }
}

export type AnalyticsSource = "calls" | "tools" | "parts";

export interface AnalyticsQuery {
    metrics: string[];
    groupBy?: string[];
    /** Bucket width such as `5m`, `1h`, `1d`; adds a `time` column. */
    interval?: string;
    from?: Date | string;
    to?: Date | string;
    /** A requested metric, sorted descending. */
    order?: string;
    limit?: number;
    /** Dimension (or `metadata.<key>`) to accepted values. */
    filters?: Record<string, string | string[]>;
}

function toTime(value: Date | string): string {
    return value instanceof Date ? value.toISOString() : value;
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
