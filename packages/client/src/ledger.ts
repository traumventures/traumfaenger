import { AsyncLocalStorage } from "node:async_hooks";
import { promisify } from "node:util";
import { gzip as gzipCallback } from "node:zlib";
import { type CallRecord, LIMITS } from "@traumwohnung/ai-ledger-api";
import { AiLedgerError, IngestClient } from "./transport.ts";

const gzip = promisify(gzipCallback);
const encoder = new TextEncoder();

/** Operation name of calls made outside `ledger.operation(...)`. */
export const UNATTRIBUTED = "unattributed";

export interface LedgerOptions {
    /** Base URL of the ingestion listener, for example `http://ai-ledger:7600`. */
    ingestUrl: string;
    /** Bearer token of the ingestion API. */
    token?: string;
    /** Name of this service, for example `traumwohnung-web`. */
    service: string;
    /** For example `production`, `dev` or `local`. */
    environment: string;
    /** How often queued records are sent. Defaults to 2 seconds. */
    flushIntervalMs?: number;
    /** Send early once this many bytes are queued. Defaults to 4 MiB. */
    maxBatchBytes?: number;
    /** Oldest records are dropped beyond this many queued bytes. Defaults to 64 MiB. */
    maxQueueBytes?: number;
    /** Gzip batch bodies. Defaults to true. */
    compress?: boolean;
    /** Called when sending fails or records are rejected or dropped. Defaults to `console.warn`. */
    onError?: (error: unknown) => void;
    /** Fetch used to reach ai-ledger itself, for tests. */
    fetch?: typeof fetch;
}

/** Context of the calls made inside `ledger.operation(...)`. */
export interface OperationContext {
    /** The kind of work, for example `listing.parse`. */
    operation: string;
    /** Groups the calls of one conversation, for example one support chat. */
    sessionId?: string;
    /** Flat key-value pairs, for example `{ listing_id: "123" }`. */
    metadata?: Record<string, string>;
}

export interface LedgerStats {
    /** Records waiting to be sent. */
    queued: number;
    /** Records the server stored (including duplicates of earlier sends). */
    sent: number;
    /** Records the server rejected as invalid. */
    rejected: number;
    /** Records dropped: queue full, record too large, or batch refused by the server. */
    dropped: number;
    /** Batch sends that failed and will be retried. */
    failedSends: number;
}

export interface Ledger {
    /**
     * Wraps a fetch function so every call through it is recorded. Pass the
     * result as the `fetch` option of an AI SDK provider, or call it directly.
     * Recording never changes what the caller receives and never delays it.
     */
    fetch(inner?: typeof fetch): typeof fetch;
    /**
     * Runs `fn` with an operation context; every call recorded inside it,
     * including in awaited async code, carries it. Nested operations override
     * `operation` and `sessionId` and merge `metadata`.
     */
    operation<T>(context: OperationContext, fn: () => T): T;
    /** Sends everything queued, unless a failed send is still backing off. Never throws. */
    flush(): Promise<void>;
    /**
     * Stops the background timer and keeps sending until the queue is empty
     * or `timeoutMs` (default 5 seconds) has passed. Never throws; records
     * still queued afterwards are reported to `onError` and in `stats()`.
     */
    close(timeoutMs?: number): Promise<void>;
    stats(): LedgerStats;
}

type Context = Pick<CallRecord, "service" | "environment" | "operation" | "session_id" | "metadata"> & {
    /** Context had to be shortened or partly dropped to fit the limits. */
    clipped: boolean;
};

export function createLedger(options: LedgerOptions): Ledger {
    for (const [name, value] of [
        ["service", options.service],
        ["environment", options.environment],
    ] as const) {
        if (!isLine(value)) throw new Error(`ai-ledger: \`${name}\` must be a non-empty single line of at most ${LIMITS.maxNameLen} bytes`);
    }
    const storage = new AsyncLocalStorage<OperationContext>();
    const sender = new Sender(options);

    const context = (): Context => {
        const current = storage.getStore();
        const operation = cleanLine(current?.operation);
        const sessionId = cleanLine(current?.sessionId);
        const metadata = cleanMetadata(current?.metadata);
        const fields: Context = {
            service: options.service,
            environment: options.environment,
            operation: operation.value ?? UNATTRIBUTED,
            clipped: operation.clipped || sessionId.clipped || metadata.clipped,
        };
        if (sessionId.value !== undefined) fields.session_id = sessionId.value;
        if (metadata.value !== undefined) fields.metadata = metadata.value;
        return fields;
    };

    return {
        fetch(inner = globalThis.fetch) {
            const recorded = (input: RequestInfo | URL, init?: RequestInit) =>
                recordCall(inner, input, init, context(), (record) => sender.push(record));
            return Object.assign(recorded, { preconnect: inner.preconnect }) as typeof fetch;
        },
        operation(next, fn) {
            const outer = storage.getStore();
            const merged: OperationContext = { operation: next.operation };
            const sessionId = next.sessionId ?? outer?.sessionId;
            if (sessionId !== undefined) merged.sessionId = sessionId;
            if (outer?.metadata !== undefined || next.metadata !== undefined) {
                merged.metadata = { ...outer?.metadata, ...next.metadata };
            }
            return storage.run(merged, fn);
        },
        flush: () => sender.flush(),
        close: (timeoutMs) => sender.close(timeoutMs),
        stats: () => sender.stats(),
    };
}

/** What is known about the request before the response arrives. */
interface PendingRequest {
    url: string;
    body: () => Promise<string>;
}

function captureRequest(input: RequestInfo | URL, init: RequestInit | undefined): PendingRequest | undefined {
    const rawUrl =
        typeof input === "string" ? input : input instanceof URL ? input.href : (input as { url?: unknown }).url;
    const url = typeof rawUrl === "string" ? cleanUrl(rawUrl) : undefined;
    if (url === undefined) return undefined;

    const body = init?.body;
    if (body === undefined || body === null) {
        // Clone before `inner` consumes the original; read the copy later.
        const request = input instanceof Request && input.body !== null ? input.clone() : undefined;
        return { url, body: async () => (request === undefined ? "" : await request.text()) };
    }
    if (typeof body === "string") return { url, body: async () => body };
    if (body instanceof URLSearchParams) return { url, body: async () => body.toString() };
    if (body instanceof ArrayBuffer || ArrayBuffer.isView(body)) {
        const copy = body instanceof ArrayBuffer ? body.slice(0) : new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice();
        return { url, body: async () => new TextDecoder().decode(copy) };
    }
    // Streams, Blobs and FormData cannot be read without consuming them.
    const kind = (body as { constructor?: { name?: string } }).constructor?.name ?? "body";
    return { url, body: async () => `[ai-ledger: unrecorded ${kind}]` };
}

async function recordCall(
    inner: typeof fetch,
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    context: Context,
    emit: (record: CallRecord) => void
): Promise<Response> {
    const startedAt = new Date().toISOString();
    const start = performance.now();
    let pending: PendingRequest | undefined;
    try {
        pending = captureRequest(input, init);
    } catch {
        pending = undefined;
    }
    // The request goes out first; everything below runs alongside or after it.
    const responsePromise = inner(input, init);
    if (pending === undefined) return responsePromise;
    const request = pending;
    const elapsed = () => Math.round(performance.now() - start);

    const record = async (fields: {
        response?: string | string[];
        httpStatus?: number;
        error?: unknown;
        ttftMs?: number;
        latencyMs: number;
        responseClipped?: boolean;
    }) => {
        let body: Clipped;
        try {
            body = prepareRequest(await request.body());
        } catch (error) {
            body = { text: `[ai-ledger: unreadable request: ${describe(error)}]`, truncated: false };
        }
        const { clipped, ...ctx } = context;
        const call: CallRecord = {
            id: crypto.randomUUID(),
            ...ctx,
            url: request.url,
            request: body.text,
            started_at: startedAt,
            latency_ms: Math.min(fields.latencyMs, LIMITS.maxDurationMs),
        };
        let responseClipped = fields.responseClipped ?? false;
        if (fields.response !== undefined) {
            // The byte cap above counts raw bytes; decoding can grow invalid UTF-8 up to 3x.
            const clippedResponse = clipResponse(fields.response);
            call.response = clippedResponse.value;
            responseClipped ||= clippedResponse.truncated;
        }
        if (fields.httpStatus !== undefined) call.http_status = fields.httpStatus;
        if (fields.error !== undefined) call.error = describe(fields.error);
        if (fields.ttftMs !== undefined) call.ttft_ms = Math.min(fields.ttftMs, call.latency_ms);
        if (clipped || body.truncated || responseClipped) call.truncated = true;
        emit(call);
    };
    const recordSafely = (fields: Parameters<typeof record>[0]) => {
        record(fields).catch(() => {
            // Recording must never break the caller; the record is lost, the call is not.
        });
    };

    let response: Response;
    try {
        response = await responsePromise;
    } catch (error) {
        recordSafely({ error, latencyMs: elapsed() });
        throw error;
    }

    try {
        const contentType = response.headers.get("content-type") ?? "";
        if (contentType.includes("text/event-stream") && response.body !== null) {
            return passThroughStream(response, start, recordSafely);
        }
        if (!isTextual(contentType)) {
            const length = response.headers.get("content-length");
            recordSafely({
                response: `[ai-ledger: unrecorded ${contentType || "body"}${length === null ? "" : `, ${length} bytes`}]`,
                httpStatus: response.status,
                latencyMs: elapsed(),
            });
            return response;
        }
        // Read a copy in the background; the caller gets the original untouched.
        const copy = response.clone();
        void readCapped(copy.body).then(({ text, clipped, error }) =>
            recordSafely({ response: text, httpStatus: response.status, error, latencyMs: elapsed(), responseClipped: clipped })
        );
        return response;
    } catch {
        return response;
    }
}

/** Streams are passed through chunk by chunk (no tee), so a caller that cancels still cancels the provider request. */
function passThroughStream(
    response: Response,
    start: number,
    recordSafely: (fields: {
        response?: string[];
        httpStatus?: number;
        error?: unknown;
        ttftMs?: number;
        latencyMs: number;
        responseClipped?: boolean;
    }) => void
): Response {
    const reader = (response.body as ReadableStream<Uint8Array>).getReader();
    const decoder = new TextDecoder();
    let collected = "";
    let collectedBytes = 0;
    let clipped = false;
    let ttft: number | undefined;
    let finished = false;
    const finish = (error?: unknown) => {
        if (finished) return;
        finished = true;
        collected += decoder.decode();
        const fields: Parameters<typeof recordSafely>[0] = {
            response: splitEvents(collected),
            httpStatus: response.status,
            latencyMs: Math.round(performance.now() - start),
            responseClipped: clipped,
        };
        if (ttft !== undefined) fields.ttftMs = ttft;
        if (error !== undefined) fields.error = error;
        recordSafely(fields);
    };

    const body = new ReadableStream<Uint8Array>({
        async pull(controller) {
            let chunk: Awaited<ReturnType<typeof reader.read>>;
            try {
                chunk = await reader.read();
            } catch (error) {
                finish(error);
                controller.error(error);
                return;
            }
            if (chunk.done) {
                finish();
                controller.close();
                return;
            }
            const value = chunk.value;
            ttft ??= Math.round(performance.now() - start);
            if (!clipped) {
                if (collectedBytes + value.byteLength > LIMITS.maxResponseBytes) {
                    clipped = true;
                } else {
                    collectedBytes += value.byteLength;
                    collected += decoder.decode(value, { stream: true });
                }
            }
            controller.enqueue(value);
        },
        async cancel(reason) {
            finish(new CancelledError(reason));
            await reader.cancel(reason);
        },
    });
    const wrapped = new Response(body, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
    });
    Object.defineProperty(wrapped, "url", { value: response.url });
    Object.defineProperty(wrapped, "redirected", { value: response.redirected });
    return wrapped;
}

function isTextual(contentType: string): boolean {
    const type = contentType.toLowerCase();
    return type === "" || type.startsWith("text/") || type.includes("json") || type.includes("xml");
}

/** Reads a body as text up to `LIMITS.maxResponseBytes`, then stops reading this copy. */
async function readCapped(
    body: ReadableStream<Uint8Array> | null
): Promise<{ text: string; clipped: boolean; error?: unknown }> {
    if (body === null) return { text: "", clipped: false };
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let text = "";
    let bytes = 0;
    try {
        for (;;) {
            const { done, value } = await reader.read();
            if (done) return { text: text + decoder.decode(), clipped: false };
            if (bytes + value.byteLength > LIMITS.maxResponseBytes) {
                await reader.cancel().catch(() => {});
                return { text: text + decoder.decode(), clipped: true };
            }
            bytes += value.byteLength;
            text += decoder.decode(value, { stream: true });
        }
    } catch (error) {
        return { text: text + decoder.decode(), clipped: false, error };
    }
}

class CancelledError extends Error {
    constructor(reason: unknown) {
        super(reason === undefined ? "stream cancelled by caller" : `stream cancelled by caller: ${String(reason)}`);
        this.name = "CancelledError";
    }
}

/** Splits an SSE body into its events, each verbatim without the separating blank line. */
export function splitEvents(text: string): string[] {
    return text
        .replace(/\r\n?/g, "\n")
        .split(/\n{2,}/)
        .map((event) => event.replace(/\n+$/, ""))
        .filter((event) => event.trim() !== "");
}

interface Clipped {
    text: string;
    truncated: boolean;
}

/** Inline base64 data of at least 1 KiB, as in `data:image/png;base64,...`. */
const INLINE_DATA = /data:([\w.+-]+\/[\w.+-]+);base64,[A-Za-z0-9+/]{1024,}={0,2}/g;

/**
 * Replaces inline base64 data (images, files) with a placeholder and clips
 * the result to `LIMITS.maxRequestBytes`. Everything else stays byte for byte.
 */
export function prepareRequest(text: string): Clipped {
    const replaced = text.includes(";base64,")
        ? text.replace(INLINE_DATA, (match, mime: string) => {
              const payload = match.length - match.indexOf(",") - 1;
              return `[ai-ledger: omitted ${mime} data, ${Math.floor((payload * 3) / 4)} bytes]`;
          })
        : text;
    return clipBytes(wellFormed(replaced), LIMITS.maxRequestBytes);
}

/** Clips a response to `LIMITS.maxResponseBytes`, measured like the server does. */
function clipResponse(response: string | string[]): { value: string | string[]; truncated: boolean } {
    if (typeof response === "string") {
        const clipped = clipBytes(wellFormed(response), LIMITS.maxResponseBytes);
        return { value: clipped.text, truncated: clipped.truncated };
    }
    const events: string[] = [];
    let remaining = LIMITS.maxResponseBytes;
    for (const event of response) {
        const clipped = clipBytes(wellFormed(event), remaining);
        if (clipped.text !== "") events.push(clipped.text);
        if (clipped.truncated) return { value: events, truncated: true };
        remaining -= encoder.encode(clipped.text).byteLength;
    }
    return { value: events, truncated: false };
}

/** Replaces lone UTF-16 surrogates, which JSON parsers on the server reject. */
function wellFormed(text: string): string {
    return typeof text.toWellFormed === "function"
        ? text.toWellFormed()
        : text.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "\uFFFD");
}

function clipBytes(text: string, maxBytes: number): Clipped {
    // Each UTF-16 code unit is at most 3 UTF-8 bytes; skip encoding when it cannot exceed.
    if (text.length * 3 <= maxBytes) return { text, truncated: false };
    const bytes = encoder.encode(text);
    if (bytes.byteLength <= maxBytes) return { text, truncated: false };
    // Cut before a UTF-8 continuation byte so no code point is split.
    let end = maxBytes;
    while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--;
    return { text: new TextDecoder().decode(bytes.subarray(0, end)), truncated: true };
}

/** A single line of at most `maxNameLen` bytes, or undefined when empty. */
function cleanLine(value: unknown): { value?: string; clipped: boolean } {
    if (value === undefined || value === null) return { clipped: false };
    const line = wellFormed(String(value)).replace(/\p{Cc}+/gu, " ").trim();
    if (line === "") return { clipped: true };
    const clipped = clipBytes(line, LIMITS.maxNameLen);
    return { value: clipped.text.trim(), clipped: clipped.truncated };
}

function isLine(value: unknown): boolean {
    const cleaned = cleanLine(value);
    return cleaned.value !== undefined && !cleaned.clipped && cleaned.value === value;
}

const METADATA_KEY = new RegExp(`^[A-Za-z0-9_.-]{1,${LIMITS.maxMetadataKeyLen}}$`);

function cleanMetadata(metadata: Record<string, unknown> | undefined): {
    value?: Record<string, string>;
    clipped: boolean;
} {
    if (metadata === undefined || metadata === null) return { clipped: false };
    const out: Record<string, string> = {};
    let count = 0;
    let clipped = false;
    for (const [key, raw] of Object.entries(metadata)) {
        if (raw === undefined || raw === null) continue;
        if (!METADATA_KEY.test(key) || count >= LIMITS.maxMetadataEntries) {
            clipped = true;
            continue;
        }
        const value = clipBytes(wellFormed(String(raw)).replaceAll("\0", ""), LIMITS.maxMetadataValueLen);
        clipped ||= value.truncated;
        out[key] = value.text;
        count++;
    }
    return count === 0 ? { clipped } : { value: out, clipped };
}

/** Absolute http(s) URL without query string and fragment, which can carry keys. */
function cleanUrl(raw: string): string | undefined {
    let url: URL;
    try {
        url = new URL(raw);
    } catch {
        return undefined;
    }
    if (url.protocol !== "https:" && url.protocol !== "http:") return undefined;
    url.search = "";
    url.hash = "";
    url.username = "";
    url.password = "";
    const text = url.href;
    return text.length <= LIMITS.maxUrlLen ? text : text.slice(0, LIMITS.maxUrlLen);
}

function describe(error: unknown): string {
    let text: string;
    try {
        text = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    } catch {
        text = "unknown error";
    }
    text = wellFormed(text).trim() || "unknown error";
    return clipBytes(text, LIMITS.maxErrorLen).text;
}

interface Queued {
    json: string;
    bytes: number;
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
        const timer = setTimeout(resolve, ms);
        (timer as { unref?: () => void }).unref?.();
    });
}

const MIN_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 60_000;

/** Queues records and sends them in batches in the background. */
class Sender {
    readonly #client: IngestClient;
    readonly #maxBatchBytes: number;
    readonly #maxQueueBytes: number;
    readonly #compress: boolean;
    readonly #onError: (error: unknown) => void;
    readonly #timer: ReturnType<typeof setInterval>;
    /** Serialized records, oldest first. */
    #queue: Queued[] = [];
    #queuedBytes = 0;
    #draining: Promise<void> | undefined;
    /** Set by close(): aborts sends at its deadline. */
    #signal: AbortSignal | undefined;
    #backoffMs = 0;
    #retryAt = 0;
    #stats = { sent: 0, rejected: 0, dropped: 0, failedSends: 0 };

    constructor(options: LedgerOptions) {
        const clientOptions: ConstructorParameters<typeof IngestClient>[0] = { baseUrl: options.ingestUrl };
        if (options.token !== undefined) clientOptions.token = options.token;
        if (options.fetch !== undefined) clientOptions.fetch = options.fetch;
        this.#client = new IngestClient(clientOptions);
        // Leave room for the envelope under the server limit.
        this.#maxBatchBytes = Math.min(options.maxBatchBytes ?? 4 * 1024 * 1024, LIMITS.maxBodyBytes - 1024);
        this.#maxQueueBytes = options.maxQueueBytes ?? 64 * 1024 * 1024;
        this.#compress = options.compress ?? true;
        const onError = options.onError ?? ((error: unknown) => console.warn("[ai-ledger]", error));
        this.#onError = (error) => {
            try {
                onError(error);
            } catch {
                // A throwing handler must not lose records or reject flush().
            }
        };
        this.#timer = setInterval(() => void this.flush(), options.flushIntervalMs ?? 2_000);
        (this.#timer as { unref?: () => void }).unref?.();
    }

    push(record: CallRecord): void {
        const json = JSON.stringify(record);
        const bytes = encoder.encode(json).byteLength;
        // A single record may exceed `maxBatchBytes` (it is then sent alone), not the server limit.
        if (bytes > LIMITS.maxBodyBytes - 1024) {
            this.#stats.dropped++;
            this.#onError(new Error(`ai-ledger: dropped a record of ${bytes} bytes, larger than a request may be`));
            return;
        }
        this.#queue.push({ json, bytes });
        this.#queuedBytes += bytes;
        let dropped = 0;
        while (this.#queuedBytes > this.#maxQueueBytes && this.#queue.length > 1) {
            this.#queuedBytes -= this.#queue.shift()?.bytes ?? 0;
            dropped++;
        }
        if (dropped > 0) {
            this.#stats.dropped += dropped;
            this.#onError(new Error(`ai-ledger: queue full, dropped ${dropped} oldest records`));
        }
        if (this.#queuedBytes >= this.#maxBatchBytes || this.#queue.length >= LIMITS.maxBatchSize) void this.flush();
    }

    flush(force = false): Promise<void> {
        if (!force && Date.now() < this.#retryAt) return this.#draining ?? Promise.resolve();
        this.#draining ??= this.#drain().finally(() => {
            this.#draining = undefined;
        });
        return this.#draining;
    }

    async close(timeoutMs = 5_000): Promise<void> {
        clearInterval(this.#timer);
        const deadline = Date.now() + timeoutMs;
        // Sends started from here on are aborted at the deadline.
        this.#signal = AbortSignal.timeout(Math.max(timeoutMs, 1));
        const remaining = () => deadline - Date.now();
        while ((this.#queue.length > 0 || this.#draining !== undefined) && remaining() > 0) {
            // A send that started before close() cannot be aborted; stop waiting at the deadline.
            await Promise.race([this.flush(true), sleep(remaining())]);
            if (this.#queue.length > 0 && this.#draining === undefined && remaining() > 0) {
                await sleep(Math.min(Math.max(this.#backoffMs, 50), remaining()));
            }
        }
        if (this.#queue.length > 0 || this.#draining !== undefined) {
            const inFlight = this.#draining === undefined ? "" : " (plus a batch still in flight)";
            this.#onError(new Error(`ai-ledger: closed with ${this.#queue.length} records unsent${inFlight}`));
        }
    }

    stats(): LedgerStats {
        return { queued: this.#queue.length, ...this.#stats };
    }

    async #drain(): Promise<void> {
        while (this.#queue.length > 0) {
            const batch = this.#takeBatch();
            const outcome = await this.#send(batch);
            if (outcome === "retry") {
                // Put the batch back in front and wait before trying again.
                this.#queue.unshift(...batch.records);
                this.#queuedBytes += batch.bytes;
                this.#backoffMs = Math.min(Math.max(this.#backoffMs * 2, MIN_BACKOFF_MS), MAX_BACKOFF_MS);
                this.#retryAt = Date.now() + this.#backoffMs;
                return;
            }
            this.#backoffMs = 0;
            this.#retryAt = 0;
        }
    }

    #takeBatch(): { records: Queued[]; bytes: number } {
        const records: Queued[] = [];
        let bytes = 0;
        for (const next of this.#queue) {
            if (records.length >= LIMITS.maxBatchSize) break;
            if (records.length > 0 && bytes + next.bytes + 1 > this.#maxBatchBytes) break;
            records.push(next);
            bytes += next.bytes;
        }
        this.#queue.splice(0, records.length);
        this.#queuedBytes -= bytes;
        return { records, bytes };
    }

    async #send(batch: { records: Queued[] }): Promise<"done" | "retry"> {
        let result: Awaited<ReturnType<IngestClient["ingestSerialized"]>>;
        try {
            const json = encoder.encode(`{"calls":[${batch.records.map((record) => record.json).join(",")}]}`);
            const body = this.#compress ? new Uint8Array(await gzip(json)) : json;
            result = await this.#client.ingestSerialized(body, this.#compress, this.#signal);
        } catch (error) {
            const permanent =
                error instanceof AiLedgerError && error.status >= 400 && error.status < 500 && error.status !== 429;
            if (permanent) {
                // Retrying the same batch cannot succeed (bad token, malformed body).
                this.#stats.dropped += batch.records.length;
                this.#onError(error);
                return "done";
            }
            this.#stats.failedSends++;
            this.#onError(error);
            return "retry";
        }
        this.#stats.sent += result.accepted + result.duplicates;
        if (result.rejected.length > 0) {
            this.#stats.rejected += result.rejected.length;
            const reasons = result.rejected.slice(0, 3).map((r) => `#${r.index}: ${r.reason}`);
            this.#onError(new Error(`ai-ledger rejected ${result.rejected.length} records (${reasons.join("; ")})`));
        }
        return "done";
    }
}
