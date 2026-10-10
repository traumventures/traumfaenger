// Recording: wraps `fetch` (HTTP and SSE) and provider WebSockets, turns
// what goes over the wire into canonical `Exchange` records and hands them to
// the sender. Recording never changes what the caller sends or receives.
import { AsyncLocalStorage } from "node:async_hooks";
import { type Direction, type Exchange, type ExchangeEnd, type Frame, LIMITS, type Transport } from "@traumfaenger/api";
import { type Encoding, Sender, type SenderStats, type SocketFactory } from "./sender.ts";
import {
    binaryPlaceholder,
    byteLength,
    cleanLine,
    cleanMetadata,
    cleanUrl,
    clipBytes,
    describe,
    isLine,
    prepareData,
    SseSplitter,
} from "./text.ts";

/** Operation name of exchanges recorded outside `database.operation(...)`. */
export const UNATTRIBUTED = "unattributed";

export interface DatabaseOptions {
    /** Base URL of the ingestion listener, for example `http://traumfaenger:7600`. */
    ingestUrl: string;
    /** Bearer token of the ingestion API. */
    token?: string;
    /** Name of this service, for example `my-service`. Use one database per environment. */
    service: string;
    /** Encoding of the ingestion socket. `msgpack` (default) or `json`. */
    encoding?: Encoding;
    /** How often queued exchanges are sent. Defaults to 2 seconds. */
    flushIntervalMs?: number;
    /** Send early once this many bytes are queued. Defaults to 4 MiB. */
    maxBatchBytes?: number;
    /** Oldest exchanges are dropped beyond this many queued bytes. Defaults to 64 MiB. */
    maxQueueBytes?: number;
    /** A recorded WebSocket session is sent in segments at least this often. Defaults to 10 seconds. */
    segmentIntervalMs?: number;
    /** ...or once a segment has this many frames. Defaults to 500. */
    maxSegmentFrames?: number;
    /** Give up on connecting to traumfaenger after this long. Defaults to 10 seconds. */
    connectTimeoutMs?: number;
    /** Reconnect when a batch has no ack after this long. Defaults to 30 seconds. */
    ackTimeoutMs?: number;
    /** Called when sending fails or exchanges are rejected or dropped. Defaults to `console.warn`. */
    onError?: (error: unknown) => void;
    /** Opens the ingestion socket; defaults to the global `WebSocket`. */
    socketFactory?: SocketFactory;
}

/** Context of the exchanges recorded inside `database.operation(...)`. */
export interface OperationContext {
    /** The kind of work, for example `listing.parse`. */
    operation: string;
    /** Groups the exchanges of one conversation, for example one support chat. */
    sessionId?: string;
    /** Flat key-value pairs, for example `{ listing_id: "123" }`. */
    metadata?: Record<string, string>;
}

export type DatabaseStats = SenderStats;

/** The subset of the WebSocket API `database.socket` needs (standard `WebSocket`, `ws`, Bun). */
export interface RecordableSocket {
    readonly url?: string;
    readonly readyState: number;
    send(data: any, ...rest: any[]): void;
    addEventListener(type: "open" | "message" | "close" | "error", listener: (event: any) => void): void;
}

export interface Database {
    /**
     * Wraps a fetch function so every call through it is recorded, as `http`
     * or, for `text/event-stream` responses, as `sse`. Pass the result as the
     * `fetch` option of an AI SDK provider, or call it directly.
     */
    fetch(inner?: typeof fetch): typeof fetch;
    /**
     * Records a WebSocket session with a provider: every message in both
     * directions. Call it right after creating the socket; it returns the
     * same socket. The session is sent in segments while it runs.
     */
    socket<T extends RecordableSocket>(socket: T, options?: { url?: string }): T;
    /**
     * Runs `fn` with an operation context; everything recorded inside it,
     * including in awaited async code, carries it. Nested operations override
     * `operation` and `sessionId` and merge `metadata`.
     */
    operation<T>(context: OperationContext, fn: () => T): T;
    /** Sends everything queued and waits for the acks, unless the connection is failing. Never throws. */
    flush(): Promise<void>;
    /**
     * Sends the open part of recorded sockets, then keeps sending until
     * everything is acknowledged or `timeoutMs` (default 5 seconds) passed.
     * Never throws; what is left is reported to `onError` and in `stats()`.
     */
    close(timeoutMs?: number): Promise<void>;
    stats(): DatabaseStats;
}

type Context = Pick<Exchange, "service" | "operation" | "session_id" | "metadata"> & {
    /** Context had to be shortened or partly dropped to fit the limits. */
    clipped: boolean;
};

export function createDatabase(options: DatabaseOptions): Database {
    if (!isLine(options.service)) {
        throw new Error(`traumfaenger: \`service\` must be a non-empty single line of at most ${LIMITS.maxNameLen} bytes`);
    }
    const storage = new AsyncLocalStorage<OperationContext>();
    const senderOptions: ConstructorParameters<typeof Sender>[0] = {
        ingestUrl: options.ingestUrl,
        onError: options.onError ?? ((error: unknown) => console.warn("[traumfaenger]", error)),
    };
    for (const key of [
        "token",
        "encoding",
        "flushIntervalMs",
        "maxBatchBytes",
        "maxQueueBytes",
        "connectTimeoutMs",
        "ackTimeoutMs",
        "socketFactory",
    ] as const) {
        if (options[key] !== undefined) Object.assign(senderOptions, { [key]: options[key] });
    }
    const sender = new Sender(senderOptions);
    const emit = (exchange: Exchange) => sender.push(exchange);
    const sockets = new Set<SocketRecorder>();

    const context = (): Context => {
        const current = storage.getStore();
        const operation = cleanLine(current?.operation);
        const sessionId = cleanLine(current?.sessionId);
        const metadata = cleanMetadata(current?.metadata);
        const fields: Context = {
            service: options.service,
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
                recordFetch(inner, input, init, context(), emit);
            return Object.assign(recorded, { preconnect: inner.preconnect }) as typeof fetch;
        },
        socket(socket, socketOptions = {}) {
            try {
                const url = cleanUrl(socketOptions.url ?? socket.url ?? "");
                if (url === undefined) return socket;
                const recorder = new SocketRecorder(socket, url, context(), emit, {
                    segmentIntervalMs: options.segmentIntervalMs ?? 10_000,
                    maxSegmentFrames: Math.min(options.maxSegmentFrames ?? 500, LIMITS.maxFrames),
                    onDone: (done) => sockets.delete(done),
                });
                if (!recorder.finished) sockets.add(recorder);
            } catch {
                // Recording must never break the caller.
            }
            return socket;
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
        async close(timeoutMs) {
            for (const recorder of sockets) recorder.flushSegment();
            await sender.close(timeoutMs);
        },
        stats: () => sender.stats(),
    };
}

/** Collects the frames of one record within the size limits. */
class Frames {
    readonly frames: Frame[] = [];
    bytes = 0;
    truncated = false;

    add(t_ms: number, dir: Direction, data: string): void {
        if (this.frames.length >= LIMITS.maxFrames) {
            this.truncated = true;
            return;
        }
        const prepared = prepareData(data);
        const room = Math.min(LIMITS.maxFrameBytes, LIMITS.maxRecordBytes - this.bytes);
        const clipped = clipBytes(prepared, room);
        if (clipped.truncated) this.truncated = true;
        if (clipped.text === "" && prepared !== "") return;
        this.frames.push({ t_ms, dir, data: clipped.text });
        this.bytes += clipped.truncated ? room : byteLength(clipped.text);
    }
}

function exchangeOf(
    context: Context,
    fields: Pick<Exchange, "transport" | "url" | "started_at"> & {
        id?: string;
        segment?: number;
        final?: boolean;
        status?: number | undefined;
        frames: Frames;
        end?: ExchangeEnd | undefined;
        truncated?: boolean;
    }
): Exchange {
    const { clipped, ...ctx } = context;
    const exchange: Exchange = {
        id: fields.id ?? crypto.randomUUID(),
        ...ctx,
        transport: fields.transport,
        url: fields.url,
        started_at: fields.started_at,
        frames: fields.frames.frames,
    };
    if (fields.segment !== undefined && fields.segment > 0) exchange.segment = fields.segment;
    if (fields.final === false) exchange.final = false;
    if (fields.status !== undefined) exchange.status = fields.status;
    if (fields.end !== undefined) exchange.end = fields.end;
    if (clipped || fields.frames.truncated || fields.truncated) exchange.truncated = true;
    return exchange;
}

function endOf(atMs: number, reason: ExchangeEnd["reason"], error?: unknown, closeCode?: number): ExchangeEnd {
    const end: ExchangeEnd = { at_ms: Math.min(atMs, LIMITS.maxDurationMs), reason };
    if (error !== undefined) end.error = describe(error);
    if (closeCode !== undefined && Number.isInteger(closeCode) && closeCode >= 0 && closeCode <= 65_535) {
        end.close_code = closeCode;
    }
    return end;
}

/** Recording must never break the caller. */
function safely(fn: () => void): void {
    try {
        fn();
    } catch {
        // Ignored: the record is lost, the call is not.
    }
}

// ---------------------------------------------------------------- fetch ---

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
        const copy =
            body instanceof ArrayBuffer
                ? body.slice(0)
                : new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice();
        return { url, body: async () => new TextDecoder().decode(copy) };
    }
    // Streams, Blobs and FormData cannot be read without consuming them.
    const kind = (body as { constructor?: { name?: string } }).constructor?.name ?? "body";
    return { url, body: async () => binaryPlaceholder(kind, undefined) };
}

interface InFrame {
    t: number;
    data: string;
}

async function recordFetch(
    inner: typeof fetch,
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    context: Context,
    emit: (exchange: Exchange) => void
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
    // Gains the response's metadata once the headers are in.
    let recordContext = context;

    const record = (transport: Transport, status: number | undefined, inFrames: InFrame[], end: ExchangeEnd, truncated = false) => {
        request
            .body()
            .catch((error: unknown) => `[traumfaenger: unreadable request: ${describe(error)}]`)
            .then((body) => {
                const frames = new Frames();
                frames.add(0, "out", body);
                for (const frame of inFrames) frames.add(frame.t, "in", frame.data);
                emit(
                    exchangeOf(recordContext, { transport, url: request.url, started_at: startedAt, status, frames, end, truncated })
                );
            })
            .catch(() => {
                // Recording must never break the caller.
            });
    };

    let response: Response;
    try {
        response = await responsePromise;
    } catch (error) {
        safely(() => record("http", undefined, [], endOf(elapsed(), "error", error)));
        throw error;
    }

    safely(() => {
        recordContext = withResponseMetadata(context, response.headers, elapsed());
    });

    try {
        const contentType = response.headers.get("content-type") ?? "";
        if (contentType.includes("text/event-stream") && response.body !== null) {
            return passThroughStream(response, elapsed, (inFrames, end, truncated) =>
                record("sse", response.status, inFrames, end, truncated)
            );
        }
        if (!isTextual(contentType)) {
            const length = response.headers.get("content-length");
            const at = elapsed();
            const placeholder = binaryPlaceholder(contentType || "body", length === null ? undefined : Number(length));
            record("http", response.status, [{ t: at, data: placeholder }], endOf(at, "complete"));
            return response;
        }
        // Read a copy in the background; the caller gets the original untouched.
        void readCapped(response.clone().body).then(({ text, clipped, error }) => {
            const at = elapsed();
            const end = error === undefined ? endOf(at, "complete") : endOf(at, "error", error);
            safely(() => record("http", response.status, [{ t: at, data: text }], end, clipped));
        });
        return response;
    } catch {
        return response;
    }
}

/** Headers that identify a provider request; recorded as `response.header.<name>`. */
const ID_HEADERS = new Set(["request-id", "x-request-id", "x-generation-id", "x-provider-name", "cf-ray"]);
/** Rate-limit headers (`x-ratelimit-*`, `ratelimit-*`, `anthropic-ratelimit-*`, `retry-after`). */
const RATE_LIMIT_HEADER = /^(x-ratelimit-|ratelimit|anthropic-ratelimit-|retry-after)/;
const METADATA_KEY = new RegExp(`^[A-Za-z0-9_.-]{1,${LIMITS.maxMetadataKeyLen}}$`);

/**
 * Adds what the response headers say about the call to the metadata: when
 * they arrived (`response.headers_ms`, since the request went out), the
 * provider's request ids and its rate-limit state. Never pushes the metadata
 * past its limits; the caller's own entries come first.
 */
function withResponseMetadata(context: Context, headers: Headers, headersMs: number): Context {
    const metadata: Record<string, string> = { ...context.metadata };
    let room = LIMITS.maxMetadataEntries - Object.keys(metadata).length;
    const add = (key: string, value: string) => {
        if (room <= 0 || key in metadata || !METADATA_KEY.test(key)) return;
        metadata[key] = clipBytes(value, LIMITS.maxMetadataValueLen).text;
        room--;
    };
    add("response.headers_ms", String(headersMs));
    headers.forEach((value, rawName) => {
        const name = rawName.toLowerCase();
        if (ID_HEADERS.has(name) || RATE_LIMIT_HEADER.test(name)) add(`response.header.${name}`, value);
    });
    return { ...context, metadata };
}

function isTextual(contentType: string): boolean {
    const type = contentType.toLowerCase();
    return type === "" || type.startsWith("text/") || type.includes("json") || type.includes("xml");
}

/** Reads a body as text up to `LIMITS.maxRecordBytes`, then stops reading this copy. */
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
            if (bytes + value.byteLength > LIMITS.maxRecordBytes) {
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

/**
 * Passes a stream through chunk by chunk (no tee), so a caller that cancels
 * still cancels the provider request. Each SSE event becomes a frame stamped
 * with the time its last chunk arrived.
 */
/**
 * Collects the events of a stream within the record limits. When a stream is
 * too long, the middle is dropped: the start and the end (where usage and the
 * finish reason arrive) are kept, with one placeholder frame in between.
 */
class StreamEvents {
    /** Leaves room for the request frame (at most `maxFrameBytes`) and the placeholder. */
    static readonly halfBytes = Math.floor((LIMITS.maxRecordBytes - LIMITS.maxFrameBytes) / 2) - 1024;
    static readonly halfFrames = Math.floor((LIMITS.maxFrames - 2) / 2);
    readonly head: InFrame[] = [];
    readonly tail: InFrame[] = [];
    #headBytes = 0;
    #tailBytes = 0;
    #droppedEvents = 0;
    #droppedBytes = 0;
    #droppedAt = 0;

    get truncated(): boolean {
        return this.#droppedEvents > 0;
    }

    push(t: number, data: string): void {
        const bytes = byteLength(data);
        if (this.tail.length === 0 && this.#droppedEvents === 0) {
            if (this.head.length < StreamEvents.halfFrames && this.#headBytes + bytes <= StreamEvents.halfBytes) {
                this.head.push({ t, data });
                this.#headBytes += bytes;
                return;
            }
        }
        const clipped = clipBytes(data, StreamEvents.halfBytes);
        this.tail.push({ t, data: clipped.text });
        this.#tailBytes += byteLength(clipped.text);
        while (
            this.tail.length > 1 &&
            (this.tail.length > StreamEvents.halfFrames || this.#tailBytes > StreamEvents.halfBytes)
        ) {
            const dropped = this.tail.shift();
            if (dropped === undefined) break;
            const droppedBytes = byteLength(dropped.data);
            this.#tailBytes -= droppedBytes;
            this.#droppedEvents++;
            this.#droppedBytes += droppedBytes;
            this.#droppedAt = dropped.t;
        }
    }

    frames(): InFrame[] {
        if (this.#droppedEvents === 0) return [...this.head, ...this.tail];
        const marker = {
            t: this.#droppedAt,
            data: `[traumfaenger: omitted ${this.#droppedEvents} events, ${this.#droppedBytes} bytes]`,
        };
        return [...this.head, marker, ...this.tail];
    }
}

/**
 * Passes a stream through chunk by chunk (no tee), so a caller that cancels
 * still cancels the provider request. Each SSE event becomes a frame stamped
 * with the time its last chunk arrived.
 */
function passThroughStream(
    response: Response,
    elapsed: () => number,
    done: (frames: InFrame[], end: ExchangeEnd, truncated: boolean) => void
): Response {
    const reader = (response.body as ReadableStream<Uint8Array>).getReader();
    const decoder = new TextDecoder();
    const splitter = new SseSplitter();
    const events = new StreamEvents();
    let finished = false;
    const collect = (text: string) => {
        const t = elapsed();
        for (const data of splitter.push(text)) events.push(t, data);
    };
    const finish = (reason: ExchangeEnd["reason"], error?: unknown) => {
        if (finished) return;
        finished = true;
        safely(() => {
            collect(decoder.decode());
            const t = elapsed();
            for (const data of splitter.end()) events.push(t, data);
        });
        safely(() => done(events.frames(), endOf(elapsed(), reason, error), events.truncated));
    };

    const body = new ReadableStream<Uint8Array>({
        async pull(controller) {
            let chunk: Awaited<ReturnType<typeof reader.read>>;
            try {
                chunk = await reader.read();
            } catch (error) {
                finish("error", error);
                controller.error(error);
                return;
            }
            if (chunk.done) {
                finish("complete");
                controller.close();
                return;
            }
            const value = chunk.value;
            safely(() => collect(decoder.decode(value, { stream: true })));
            controller.enqueue(value);
        },
        async cancel(reason) {
            finish("cancelled", new CancelledError(reason));
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

// --------------------------------------------------------------- socket ---

const OPEN = 1;
const CLOSED = 3;

function messageData(data: unknown): string {
    if (typeof data === "string") return data;
    if (data instanceof ArrayBuffer) return binaryPlaceholder("binary message", data.byteLength);
    if (ArrayBuffer.isView(data)) return binaryPlaceholder("binary message", data.byteLength);
    if (typeof Blob !== "undefined" && data instanceof Blob) return binaryPlaceholder("binary message", data.size);
    if (Array.isArray(data)) {
        const bytes = data.reduce((sum: number, part: { byteLength?: number }) => sum + (part.byteLength ?? 0), 0);
        return binaryPlaceholder("binary message", bytes);
    }
    return String(data);
}

/** Records one provider WebSocket session and sends it in segments. */
class SocketRecorder {
    readonly #id = crypto.randomUUID();
    readonly #url: string;
    readonly #context: Context;
    readonly #emit: (exchange: Exchange) => void;
    readonly #startedAt = new Date().toISOString();
    readonly #start = performance.now();
    readonly #maxSegmentFrames: number;
    readonly #onDone: (recorder: SocketRecorder) => void;
    readonly #timer: ReturnType<typeof setInterval>;
    #segment = 0;
    #frames = new Frames();
    #status: number | undefined;
    #error: unknown;
    #finished = false;

    constructor(
        socket: RecordableSocket,
        url: string,
        context: Context,
        emit: (exchange: Exchange) => void,
        options: { segmentIntervalMs: number; maxSegmentFrames: number; onDone: (recorder: SocketRecorder) => void }
    ) {
        this.#url = url;
        this.#context = context;
        this.#emit = emit;
        this.#maxSegmentFrames = options.maxSegmentFrames;
        this.#onDone = options.onDone;
        if (socket.readyState === OPEN) this.#status = 101;

        const send = socket.send as (...args: unknown[]) => unknown;
        // Pass every argument through: `ws` takes `send(data, options, callback)`.
        (socket as { send: (...args: unknown[]) => unknown }).send = (...args: unknown[]) => {
            const result = send.apply(socket, args);
            safely(() => this.#add("out", args[0]));
            return result;
        };
        socket.addEventListener("open", () => {
            this.#status = 101;
        });
        socket.addEventListener("message", (event: { data: unknown }) => safely(() => this.#add("in", event.data)));
        socket.addEventListener("error", (event: { message?: unknown; error?: unknown }) => {
            this.#error ??= event.error ?? event.message ?? "WebSocket error";
        });
        socket.addEventListener("close", (event: { code?: number; reason?: string }) =>
            safely(() => this.#finish(event.code))
        );
        this.#timer = setInterval(() => safely(() => this.#tick()), options.segmentIntervalMs);
        (this.#timer as { unref?: () => void }).unref?.();
        // Attached too late: the close event already happened.
        if (socket.readyState === CLOSED) this.#finish(undefined);
    }

    #tick(): void {
        if (this.#elapsed() >= LIMITS.maxDurationMs) this.#stop();
        else this.flushSegment();
    }

    /** Ends the recording after `maxDurationMs`; the socket itself is left alone. */
    #stop(): void {
        this.#error ??= `recording stopped after ${LIMITS.maxDurationMs} ms`;
        this.#finish(undefined);
    }

    get finished(): boolean {
        return this.#finished;
    }

    #elapsed(): number {
        return Math.round(performance.now() - this.#start);
    }

    #add(dir: Direction, data: unknown): void {
        if (this.#finished) return;
        if (this.#elapsed() >= LIMITS.maxDurationMs) {
            this.#stop();
            return;
        }
        const text = messageData(data);
        // Start a new segment instead of clipping when this one is full.
        const full =
            this.#frames.frames.length >= this.#maxSegmentFrames ||
            this.#frames.bytes + byteLength(text) > LIMITS.maxRecordBytes;
        if (full && this.#frames.frames.length > 0) this.flushSegment();
        this.#frames.add(this.#elapsed(), dir, text);
    }

    /** Sends the frames collected so far as a non-final segment. */
    flushSegment(): void {
        if (this.#finished || this.#frames.frames.length === 0) return;
        if (this.#segment >= LIMITS.maxSegment) return;
        this.#emit(this.#record(false));
        this.#segment++;
        this.#frames = new Frames();
    }

    #finish(closeCode: number | undefined): void {
        if (this.#finished) return;
        const end = endOf(
            this.#elapsed(),
            this.#error === undefined ? "closed" : "error",
            this.#error,
            closeCode
        );
        this.#emit(this.#record(true, end));
        this.#finished = true;
        clearInterval(this.#timer);
        this.#onDone(this);
    }

    #record(final: boolean, end?: ExchangeEnd): Exchange {
        return exchangeOf(this.#context, {
            id: this.#id,
            segment: this.#segment,
            final,
            transport: "ws",
            url: this.#url,
            started_at: this.#startedAt,
            status: this.#status,
            frames: this.#frames,
            end,
        });
    }
}
