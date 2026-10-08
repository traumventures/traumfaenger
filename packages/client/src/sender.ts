// The ingestion connection: one WebSocket to traumfaenger that carries batches
// of exchanges, pipelined, with in-order acks.
import { decode, encode } from "@msgpack/msgpack";
import { type Exchange, LIMITS, PROTOCOLS, ROUTES, type ServerMessage } from "@traumfaenger/api";
import { byteLength, clipBytes } from "./text.ts";

/** The subset of the WebSocket API the sender uses (standard `WebSocket`, `ws`, Bun, undici). */
export interface IngestSocket {
    readonly protocol: string;
    binaryType: string;
    send(data: string | Uint8Array): void;
    close(code?: number, reason?: string): void;
    addEventListener(type: "open" | "message" | "close" | "error", listener: (event: any) => void): void;
}

/** Opens the ingestion socket. The default uses the global `WebSocket` with headers (Bun, Node ≥ 22). */
export type SocketFactory = (url: string, protocols: string[], headers: Record<string, string>) => IngestSocket;

export const defaultSocketFactory: SocketFactory = (url, protocols, headers) =>
    new (globalThis.WebSocket as unknown as new (url: string, init: unknown) => IngestSocket)(url, {
        protocols,
        headers,
    });

export type Encoding = "msgpack" | "json";

export interface SenderOptions {
    /** Base URL of the ingestion listener, for example `http://traumfaenger:7600` or `ws://traumfaenger:7600`. */
    ingestUrl: string;
    token?: string;
    /** Preferred encoding. `msgpack` (default) offers both and lets the server choose; `json` offers only JSON. */
    encoding?: Encoding;
    flushIntervalMs?: number;
    maxBatchBytes?: number;
    maxQueueBytes?: number;
    /** Close the connection after this long without traffic. Defaults to 30 seconds. */
    idleTimeoutMs?: number;
    /** Give up on a connection attempt after this long. Defaults to 10 seconds. */
    connectTimeoutMs?: number;
    /** Reconnect when the oldest batch has no ack after this long. Defaults to 30 seconds. */
    ackTimeoutMs?: number;
    socketFactory?: SocketFactory;
    onError: (error: unknown) => void;
}

export interface SenderStats {
    /** Exchanges waiting to be sent or acknowledged. */
    queued: number;
    /** Exchanges the server stored (including duplicates of earlier sends). */
    sent: number;
    /** Exchanges the server rejected as invalid. */
    rejected: number;
    /** Exchanges dropped: queue full, record too large, or batch refused by the server. */
    dropped: number;
    /** Failed connections, timeouts and server errors; their batches are sent again later. */
    failedSends: number;
}

interface Pending {
    exchange: Exchange;
    bytes: number;
}

interface InFlight {
    id: number;
    items: Pending[];
    sentAt: number;
}

const MIN_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 60_000;
const WATCHDOG_MS = 1_000;

function unref<T>(timer: T): T {
    (timer as { unref?: () => void }).unref?.();
    return timer;
}

/** Sleeps without keeping the process alive. */
export function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => unref(setTimeout(resolve, Math.max(ms, 0))));
}

export class Sender {
    readonly #url: string;
    readonly #protocols: string[];
    readonly #headers: Record<string, string>;
    readonly #factory: SocketFactory;
    readonly #maxBatchBytes: number;
    readonly #maxQueueBytes: number;
    readonly #idleTimeoutMs: number;
    readonly #connectTimeoutMs: number;
    readonly #ackTimeoutMs: number;
    readonly #onError: (error: unknown) => void;
    readonly #flushTimer: ReturnType<typeof setInterval>;
    readonly #watchdog: ReturnType<typeof setInterval>;

    #queue: Pending[] = [];
    #queuedBytes = 0;
    #inFlight: InFlight[] = [];
    #nextBatch = 1;
    #socket: IngestSocket | undefined;
    #state: "idle" | "connecting" | "open" = "idle";
    #connectingSince = 0;
    #lastActivity = 0;
    #encoding: Encoding = "json";
    #backoffMs = 0;
    #retryAt = 0;
    /** Resolved when everything is acknowledged or the connection fails. */
    #waiters: (() => void)[] = [];
    #stats = { sent: 0, rejected: 0, dropped: 0, failedSends: 0 };
    #closed = false;

    constructor(options: SenderOptions) {
        const base = options.ingestUrl.replace(/\/+$/, "").replace(/^http/, "ws");
        this.#url = `${base}${ROUTES.ingestWs}`;
        this.#protocols = options.encoding === "json" ? [PROTOCOLS.json] : [PROTOCOLS.msgpack, PROTOCOLS.json];
        this.#headers = options.token === undefined ? {} : { authorization: `Bearer ${options.token}` };
        this.#factory = options.socketFactory ?? defaultSocketFactory;
        // Leave room for the batch envelope under the server limit.
        this.#maxBatchBytes = Math.min(options.maxBatchBytes ?? 4 * 1024 * 1024, LIMITS.maxBatchBytes - 1024);
        this.#maxQueueBytes = options.maxQueueBytes ?? 64 * 1024 * 1024;
        this.#idleTimeoutMs = options.idleTimeoutMs ?? 30_000;
        this.#connectTimeoutMs = options.connectTimeoutMs ?? 10_000;
        this.#ackTimeoutMs = options.ackTimeoutMs ?? 30_000;
        const onError = options.onError;
        this.#onError = (error) => {
            try {
                onError(error);
            } catch {
                // A throwing handler must not lose exchanges or break the connection.
            }
        };
        this.#flushTimer = unref(setInterval(() => this.#guard(() => this.#kick()), options.flushIntervalMs ?? 2_000));
        this.#watchdog = unref(setInterval(() => this.#guard(() => this.#watch()), WATCHDOG_MS));
    }

    push(exchange: Exchange): void {
        if (this.#closed) {
            this.#stats.dropped++;
            this.#onError(new Error("traumfaenger: exchange recorded after close() was dropped"));
            return;
        }
        // JSON size is an upper bound of the MessagePack size, so batches fit either encoding.
        const bytes = byteLength(JSON.stringify(exchange));
        if (bytes > LIMITS.maxBatchBytes - 1024) {
            this.#stats.dropped++;
            this.#onError(new Error(`traumfaenger: dropped an exchange of ${bytes} bytes, larger than a batch may be`));
            return;
        }
        this.#queue.push({ exchange, bytes });
        this.#queuedBytes += bytes;
        let dropped = 0;
        while (this.#queuedBytes > this.#maxQueueBytes && this.#queue.length > 1) {
            this.#queuedBytes -= this.#queue.shift()?.bytes ?? 0;
            dropped++;
        }
        if (dropped > 0) {
            this.#stats.dropped += dropped;
            this.#onError(new Error(`traumfaenger: queue full, dropped ${dropped} oldest exchanges`));
        }
        if (this.#queuedBytes >= this.#maxBatchBytes || this.#queue.length >= LIMITS.maxBatchSize) this.#kick();
    }

    stats(): SenderStats {
        const inFlight = this.#inFlight.reduce((sum, batch) => sum + batch.items.length, 0);
        return { queued: this.#queue.length + inFlight, ...this.#stats };
    }

    /**
     * Sends everything queued and waits for the acks. Resolves early when the
     * connection fails or is backing off after a failure (unless `force`).
     * Never rejects.
     */
    flush(force = false): Promise<void> {
        if (this.#isIdle()) return Promise.resolve();
        if (!force && this.#state !== "open" && Date.now() < this.#retryAt) return Promise.resolve();
        const done = new Promise<void>((resolve) => this.#waiters.push(resolve));
        this.#kick(force);
        return done;
    }

    /**
     * Keeps sending until everything is acknowledged or `timeoutMs` passed,
     * then closes the connection. Tries at least once even while backing off.
     * Never rejects; what is left is reported.
     */
    async close(timeoutMs = 5_000): Promise<void> {
        this.#closed = true;
        clearInterval(this.#flushTimer);
        const deadline = Date.now() + timeoutMs;
        const remaining = () => deadline - Date.now();
        // Referenced timer: a process awaiting close() at exit must wait for it.
        const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, Math.max(ms, 0)));
        let forced = false;
        while (!this.#isIdle() && remaining() > 0) {
            const force = !forced;
            forced = true;
            await Promise.race([this.flush(force), wait(remaining())]);
            if (!this.#isIdle() && this.#state === "idle" && remaining() > 0) {
                await wait(Math.min(Math.max(this.#retryAt - Date.now(), 50), remaining()));
            }
        }
        clearInterval(this.#watchdog);
        const left = this.stats().queued;
        if (left > 0) this.#onError(new Error(`traumfaenger: closed with ${left} exchanges unsent`));
        this.#lose("closed by the client", false);
    }

    #guard(fn: () => void): void {
        try {
            fn();
        } catch (error) {
            this.#onError(error);
        }
    }

    #isIdle(): boolean {
        return this.#queue.length === 0 && this.#inFlight.length === 0;
    }

    #kick(force = false): void {
        if (this.#state === "open") this.#pump();
        else if (this.#queue.length > 0 && this.#state === "idle" && (force || Date.now() >= this.#retryAt)) {
            this.#connect();
        }
    }

    /** Timeouts for connecting and for acks, and closing an idle connection. */
    #watch(): void {
        const now = Date.now();
        if (this.#state === "connecting" && now - this.#connectingSince > this.#connectTimeoutMs) {
            this.#lose(`connecting took longer than ${this.#connectTimeoutMs} ms`, true);
        } else if (this.#state === "open") {
            const oldest = this.#inFlight[0];
            if (oldest !== undefined && now - oldest.sentAt > this.#ackTimeoutMs) {
                this.#lose(`no ack within ${this.#ackTimeoutMs} ms`, true);
            } else if (this.#isIdle() && now - this.#lastActivity > this.#idleTimeoutMs) {
                this.#lose("idle", false);
            }
        }
    }

    #connect(): void {
        this.#state = "connecting";
        this.#connectingSince = Date.now();
        let socket: IngestSocket;
        try {
            socket = this.#factory(this.#url, this.#protocols, this.#headers);
            socket.binaryType = "arraybuffer";
        } catch (error) {
            this.#state = "idle";
            this.#failed(error);
            return;
        }
        this.#socket = socket;
        socket.addEventListener("open", () =>
            this.#guard(() => {
                if (this.#socket !== socket) return;
                if (socket.protocol === PROTOCOLS.msgpack) this.#encoding = "msgpack";
                else if (socket.protocol === PROTOCOLS.json) this.#encoding = "json";
                else {
                    this.#lose(`server chose unknown subprotocol \`${socket.protocol}\``, true);
                    return;
                }
                this.#state = "open";
                this.#lastActivity = Date.now();
                this.#pump();
            })
        );
        socket.addEventListener("message", (event: { data: unknown }) =>
            this.#guard(() => {
                if (this.#socket === socket) this.#receive(event.data);
            })
        );
        socket.addEventListener("close", (event: { code?: number; reason?: string }) =>
            this.#guard(() => {
                if (this.#socket !== socket) return;
                const why = event.reason ? `: ${event.reason}` : "";
                this.#lose(`ingestion connection closed (${event.code ?? "?"})${why}`, true);
            })
        );
        socket.addEventListener("error", () => {
            // A close event follows and does the cleanup.
        });
    }

    /**
     * Drops the current connection. Unacknowledged batches go back to the
     * front of the queue, in order. `failure` starts a backoff when work is left.
     */
    #lose(reason: string, failure: boolean): void {
        const socket = this.#socket;
        this.#socket = undefined;
        this.#state = "idle";
        const unacked = this.#inFlight.flatMap((batch) => batch.items);
        this.#inFlight = [];
        this.#requeue(unacked);
        if (socket !== undefined) {
            try {
                // A close reason may be at most 123 bytes.
                socket.close(failure ? 4000 : 1000, clipBytes(reason, 120).text);
            } catch {
                // Already closed.
            }
        }
        if (failure && !this.#isIdle()) this.#failed(new Error(`traumfaenger: ${reason}`));
        else this.#notify();
    }

    #failed(error: unknown): void {
        this.#stats.failedSends++;
        this.#backoffMs = Math.min(Math.max(this.#backoffMs * 2, MIN_BACKOFF_MS), MAX_BACKOFF_MS);
        this.#retryAt = Date.now() + this.#backoffMs;
        this.#onError(error);
        this.#notify();
    }

    #requeue(items: Pending[]): void {
        this.#queue.unshift(...items);
        this.#queuedBytes += items.reduce((sum, item) => sum + item.bytes, 0);
    }

    #pump(): void {
        const socket = this.#socket;
        if (socket === undefined || this.#state !== "open") return;
        while (this.#queue.length > 0 && this.#inFlight.length < LIMITS.maxInFlightBatches) {
            const items = this.#takeBatch();
            const id = this.#nextBatch++;
            const message = { type: "batch", id, exchanges: items.map((item) => item.exchange) };
            try {
                socket.send(
                    this.#encoding === "msgpack" ? encode(message, { ignoreUndefined: true }) : JSON.stringify(message)
                );
            } catch (error) {
                this.#requeue(items);
                this.#lose(`sending failed: ${String(error)}`, true);
                return;
            }
            this.#lastActivity = Date.now();
            this.#inFlight.push({ id, items, sentAt: this.#lastActivity });
        }
        if (this.#isIdle()) this.#notify();
    }

    #takeBatch(): Pending[] {
        const items: Pending[] = [];
        let bytes = 0;
        for (const next of this.#queue) {
            if (items.length >= LIMITS.maxBatchSize) break;
            if (items.length > 0 && bytes + next.bytes + 1 > this.#maxBatchBytes) break;
            items.push(next);
            bytes += next.bytes;
        }
        this.#queue.splice(0, items.length);
        this.#queuedBytes -= bytes;
        return items;
    }

    #receive(data: unknown): void {
        let message: ServerMessage;
        try {
            message = (
                typeof data === "string" ? JSON.parse(data) : decode(new Uint8Array(data as ArrayBuffer))
            ) as ServerMessage;
        } catch (error) {
            this.#onError(error);
            return;
        }
        this.#lastActivity = Date.now();
        // The server answers batches in order; an error without a batch id is for the oldest one.
        const index =
            message.batch === null || message.batch === undefined
                ? 0
                : this.#inFlight.findIndex((batch) => batch.id === message.batch);
        const batch = index === -1 ? undefined : this.#inFlight.splice(index, 1)[0];

        if (message.type === "ack") {
            this.#backoffMs = 0;
            this.#stats.sent += (message.accepted ?? 0) + (message.duplicates ?? 0);
            const rejected = Array.isArray(message.rejected) ? message.rejected : [];
            if (rejected.length > 0) {
                this.#stats.rejected += rejected.length;
                const reasons = rejected.slice(0, 3).map((r) => `#${r.index}: ${r.reason}`);
                this.#onError(new Error(`traumfaenger rejected ${rejected.length} exchanges (${reasons.join("; ")})`));
            }
        } else if (message.error === "internal") {
            // Not stored; send it again after a backoff instead of in a tight loop.
            if (batch !== undefined) this.#requeue(batch.items);
            this.#lose(`server error: ${message.message}`, true);
            return;
        } else {
            if (batch !== undefined) this.#stats.dropped += batch.items.length;
            this.#onError(new Error(`traumfaenger refused a batch: ${message.error}: ${message.message}`));
        }
        this.#pump();
    }

    #notify(): void {
        const waiters = this.#waiters;
        this.#waiters = [];
        for (const resolve of waiters) resolve();
    }
}
