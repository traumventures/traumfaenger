// End-to-end: a mock AI provider (JSON, SSE, WebSocket), the recorder in
// between, and the real Rust server over the ingestion WebSocket.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { join } from "node:path";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { generateText, streamText } from "ai";
import {
    createDatabase,
    LIMITS,
    type Database,
    type DatabaseOptions,
    prepareData,
    QueryClient,
    type SocketFactory,
    SseSplitter,
    type StoredExchange,
    splitEvents,
    toSearch,
    UNATTRIBUTED,
} from "./index.ts";

const root = join(import.meta.dir, "../../..");
const ingestToken = "ingest-token-0123456789";
const queryToken = "query-token-0123456789";
const ingestPort = 20_000 + Math.floor(Math.random() * 20_000);
const queryPort = ingestPort + 1;
const ingestUrl = `http://127.0.0.1:${ingestPort}`;
let server: ReturnType<typeof Bun.spawn> | undefined;

const query = new QueryClient({ baseUrl: `http://127.0.0.1:${queryPort}`, token: queryToken });
const errors: unknown[] = [];

function makeDatabase(overrides: Partial<DatabaseOptions> = {}): Database {
    return createDatabase({
        ingestUrl,
        token: ingestToken,
        service: "test-service",
        flushIntervalMs: 60_000,
        onError: (error) => errors.push(error),
        ...overrides,
    });
}
const database = makeDatabase();

const SSE_EVENTS = [
    'data: {"choices":[{"delta":{"content":"Hal"}}]}',
    'data: {"choices":[{"delta":{"content":"lo"}}],"usage":{"prompt_tokens":5,"completion_tokens":2}}',
    "data: [DONE]",
];

// A fake provider: JSON, SSE (optionally slow or endless) and a realtime WebSocket.
const provider = Bun.serve({
    port: 0,
    async fetch(request, srv) {
        if (new URL(request.url).pathname === "/realtime") {
            return srv.upgrade(request) ? undefined : new Response("upgrade failed", { status: 400 });
        }
        const body = (await request.json()) as {
            stream?: boolean;
            endless?: boolean;
            slow?: boolean;
            many?: number;
            model?: string;
            headers?: Record<string, string>;
        };
        if (body.headers !== undefined) {
            return Response.json({ ok: true }, { status: 429, headers: body.headers });
        }
        if (body.many !== undefined) {
            const events = Array.from({ length: body.many }, (_, n) => `data: {"n":${n}}\n\n`).join("");
            return new Response(`${events}data: {"usage":{"prompt_tokens":9}}\n\ndata: [DONE]\n\n`, {
                headers: { "content-type": "text/event-stream" },
            });
        }
        if (body.model === "sdk-model") return openAiResponse(body.stream === true);
        if (body.endless || body.slow) {
            let index = 0;
            return new Response(
                new ReadableStream({
                    async pull(controller) {
                        await Bun.sleep(body.slow ? 60 : 5);
                        if (body.slow && index === 3) return controller.close();
                        controller.enqueue(new TextEncoder().encode(`data: {"n":${index++}}\n\n`));
                    },
                }),
                { headers: { "content-type": "text/event-stream" } }
            );
        }
        if (body.stream) {
            return new Response(SSE_EVENTS.map((event) => `${event}\n\n`).join(""), {
                headers: { "content-type": "text/event-stream; charset=utf-8" },
            });
        }
        return Response.json({ choices: [{ message: { role: "assistant", content: "Hallo" } }] });
    },
    websocket: {
        message(ws, message) {
            if (typeof message !== "string") {
                ws.send(new Uint8Array(32));
                return;
            }
            const event = JSON.parse(message) as { type: string };
            ws.send(JSON.stringify({ type: "transcript.delta", delta: `got ${event.type}` }));
            if (event.type === "bye") ws.close(1000, "done");
        },
    },
});
const providerUrl = `http://127.0.0.1:${provider.port}/api/v1/chat/completions`;

function openAiResponse(stream: boolean): Response {
    const usage = { prompt_tokens: 7, completion_tokens: 2, total_tokens: 9 };
    const base = { id: "gen-1", created: 0, model: "sdk-model" };
    if (!stream) {
        return Response.json({
            ...base,
            object: "chat.completion",
            choices: [{ index: 0, message: { role: "assistant", content: "Hallo" }, finish_reason: "stop" }],
            usage,
        });
    }
    const chunk = (delta: object, finish: string | null = null, extra: object = {}) =>
        `data: ${JSON.stringify({ ...base, object: "chat.completion.chunk", choices: [{ index: 0, delta, finish_reason: finish }], ...extra })}\n\n`;
    const body =
        chunk({ role: "assistant", content: "Hal" }) + chunk({ content: "lo" }) + chunk({}, "stop", { usage }) + "data: [DONE]\n\n";
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
}

beforeAll(async () => {
    const build = Bun.spawnSync(["cargo", "build", "-q", "-p", "traumfaenger-server"], { cwd: root, stderr: "inherit" });
    if (build.exitCode !== 0) throw new Error("cargo build failed");
    server = Bun.spawn([join(root, "target/debug/traumfaenger-server")], {
        cwd: root,
        env: {
            ...process.env,
            TRAUMFAENGER_DATABASE: ":memory:",
            TRAUMFAENGER_INGEST_ADDR: `127.0.0.1:${ingestPort}`,
            TRAUMFAENGER_QUERY_ADDR: `127.0.0.1:${queryPort}`,
            TRAUMFAENGER_INGEST_TOKEN: ingestToken,
            TRAUMFAENGER_QUERY_TOKEN: queryToken,
            RUST_LOG: "warn",
        },
        stderr: "inherit",
    });
    for (let attempt = 0; attempt < 100; attempt++) {
        try {
            await query.health();
            return;
        } catch {
            await Bun.sleep(50);
        }
    }
    throw new Error("traumfaenger-server did not become healthy");
}, 300_000);

afterAll(async () => {
    await database.close();
    provider.stop(true);
    server?.kill();
});

const recorded = database.fetch();

/** Waits for background reads to finish, then sends everything. */
async function settle(target: Database = database): Promise<void> {
    await Bun.sleep(30);
    await target.flush();
}

async function only(operation: string): Promise<StoredExchange> {
    const page = await query.list({ operation });
    const ids = new Set(page.entries.map((entry) => entry.id));
    expect(ids.size).toBe(1);
    const exchange = await query.getExchange([...ids][0] ?? "");
    expect(exchange).not.toBeNull();
    return exchange!;
}

test("records a JSON call as an http exchange with its context", async () => {
    const requestBody = JSON.stringify({ model: "m", messages: [{ role: "user", content: "Hi" }] });
    const response = await database.operation(
        { operation: "listing.parse", sessionId: "s-1", metadata: { listing_id: "123" } },
        () => recorded(`${providerUrl}?key=secret`, { method: "POST", body: requestBody })
    );
    expect(await response.json()).toEqual({ choices: [{ message: { role: "assistant", content: "Hallo" } }] });
    await settle();

    const exchange = await only("listing.parse");
    expect(exchange).toMatchObject({
        service: "test-service",
        session_id: "s-1",
        metadata: { listing_id: "123" },
        transport: "http",
        url: providerUrl,
        status: 200,
        segments: 1,
        complete: true,
        truncated: false,
        end: { reason: "complete" },
    });
    expect(exchange.frames.map((frame) => [frame.dir, frame.data])).toEqual([
        ["out", requestBody],
        ["in", JSON.stringify({ choices: [{ message: { role: "assistant", content: "Hallo" } }] })],
    ]);
    expect(exchange.frames[0]?.t_ms).toBe(0);
});

test("records a stream as sse frames and passes it through unchanged", async () => {
    const response = await database.operation({ operation: "support.chat" }, () =>
        recorded(providerUrl, { method: "POST", body: JSON.stringify({ stream: true }) })
    );
    expect(response.headers.get("content-type")).toContain("text/event-stream");
    expect(await response.text()).toBe(SSE_EVENTS.map((event) => `${event}\n\n`).join(""));
    await settle();

    const exchange = await only("support.chat");
    expect(exchange.transport).toBe("sse");
    expect(exchange.frames.slice(1).map((frame) => frame.data)).toEqual(SSE_EVENTS);
    expect(exchange.frames.slice(1).every((frame) => frame.dir === "in")).toBe(true);
});

test("stamps each event with the time it arrived", async () => {
    await database.operation({ operation: "slow.stream" }, async () => {
        await (await recorded(providerUrl, { method: "POST", body: JSON.stringify({ slow: true }) })).text();
    });
    await settle();

    const exchange = await only("slow.stream");
    const times = exchange.frames.slice(1).map((frame) => frame.t_ms);
    expect(times).toHaveLength(3);
    expect((times[1] ?? 0) - (times[0] ?? 0)).toBeGreaterThanOrEqual(40);
    expect((times[2] ?? 0) - (times[1] ?? 0)).toBeGreaterThanOrEqual(40);
    expect(exchange.end?.at_ms).toBeGreaterThanOrEqual(times[2] ?? 0);
});

test("records a cancelled stream with its partial events", async () => {
    const response = await database.operation({ operation: "cancelled.chat" }, () =>
        recorded(providerUrl, { method: "POST", body: JSON.stringify({ endless: true }) })
    );
    const reader = response.body!.getReader();
    await reader.read();
    await reader.read();
    await reader.cancel("user closed the chat");
    await settle();

    const exchange = await only("cancelled.chat");
    expect(exchange.end).toMatchObject({
        reason: "cancelled",
        error: "CancelledError: stream cancelled by caller: user closed the chat",
    });
    expect(exchange.frames[1]?.data).toBe('data: {"n":0}');
});

test("records network failures and rethrows them to the caller", async () => {
    const failure = await database
        .operation({ operation: "unreachable" }, () => recorded("http://127.0.0.1:1/v1/chat", { method: "POST", body: "{}" }))
        .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(Error);
    await settle();

    const exchange = await only("unreachable");
    expect(exchange.status).toBeNull();
    expect(exchange.frames.map((frame) => frame.dir)).toEqual(["out"]);
    expect(exchange.end?.reason).toBe("error");
});

test("nested operations merge metadata; calls outside any operation are unattributed", async () => {
    await database.operation({ operation: "outer", sessionId: "s-2", metadata: { a: "1", b: "1" } }, () =>
        database.operation({ operation: "inner.step", metadata: { b: "2" } }, async () => {
            await (await recorded(providerUrl, { method: "POST", body: "{}" })).text();
        })
    );
    await (await recorded(new Request(providerUrl, { method: "POST", body: '{"x":1}' }))).text();
    await settle();

    const inner = await only("inner.step");
    expect(inner.session_id).toBe("s-2");
    expect(inner.metadata).toMatchObject({ a: "1", b: "2" });
    expect((await only(UNATTRIBUTED)).frames[0]?.data).toBe('{"x":1}');
});

test("records when the headers arrived, the provider's request ids and rate limits", async () => {
    const headers = {
        "x-generation-id": "gen-42",
        "request-id": "req-7",
        "x-ratelimit-remaining-requests": "0",
        "retry-after": "3",
        "x-unrelated": "dropped",
    };
    const response = await database.operation({ operation: "rate.limited", metadata: { user_id: "9" } }, () =>
        recorded(providerUrl, { method: "POST", body: JSON.stringify({ headers }) })
    );
    expect(response.status).toBe(429);
    expect(response.headers.get("x-unrelated")).toBe("dropped");
    await response.text();
    await settle();

    const exchange = await only("rate.limited");
    const { "response.headers_ms": headersMs, ...rest } = exchange.metadata;
    expect(Number(headersMs)).toBeGreaterThanOrEqual(0);
    expect(Number(headersMs)).toBeLessThanOrEqual(exchange.end?.at_ms ?? 0);
    expect(rest).toEqual({
        user_id: "9",
        "response.header.x-generation-id": "gen-42",
        "response.header.request-id": "req-7",
        "response.header.x-ratelimit-remaining-requests": "0",
        "response.header.retry-after": "3",
    });
});

test("response metadata never pushes the caller's metadata past the limit", async () => {
    const own = Object.fromEntries(Array.from({ length: LIMITS.maxMetadataEntries }, (_, n) => [`k${n}`, "v"]));
    await database.operation({ operation: "full.metadata", metadata: own }, async () => {
        await (await recorded(providerUrl, { method: "POST", body: JSON.stringify({ headers: { "request-id": "r" } }) })).text();
    });
    await settle();

    const exchange = await only("full.metadata");
    expect(exchange.metadata).toEqual(own);
    expect(exchange.truncated).toBe(false);
});

test("works as the fetch of an AI SDK provider", async () => {
    const sdk = createOpenAICompatible({
        name: "test",
        baseURL: `http://127.0.0.1:${provider.port}/api/v1`,
        fetch: database.fetch(),
        includeUsage: true,
    });
    const generated = await database.operation({ operation: "sdk.generate" }, () =>
        generateText({ model: sdk("sdk-model"), prompt: "Hi" })
    );
    expect(generated.text).toBe("Hallo");
    const streamed = await database.operation({ operation: "sdk.stream", sessionId: "chat-9" }, async () => {
        return await streamText({ model: sdk("sdk-model"), prompt: "Hi" }).text;
    });
    expect(streamed).toBe("Hallo");
    await settle();

    const generate = await only("sdk.generate");
    expect(JSON.parse(generate.frames[0]?.data ?? "")).toMatchObject({ messages: [{ role: "user", content: "Hi" }] });
    expect(JSON.parse(generate.frames[1]?.data ?? "").usage.prompt_tokens).toBe(7);
    const stream = await only("sdk.stream");
    expect(JSON.parse(stream.frames[0]?.data ?? "")).toMatchObject({ stream: true, stream_options: { include_usage: true } });
    expect(stream.frames.at(-1)?.data).toBe("data: [DONE]");
});

test("records a provider WebSocket session in segments", async () => {
    const segmented = makeDatabase({ maxSegmentFrames: 3, encoding: "json" });
    const audio = "A".repeat(4_000);
    const ws = segmented.operation({ operation: "voice.interview", sessionId: "iv-1" }, () =>
        segmented.socket(new WebSocket(`ws://127.0.0.1:${provider.port}/realtime?token=secret`))
    );
    await new Promise((resolve) => ws.addEventListener("open", resolve));
    const closed = new Promise((resolve) => ws.addEventListener("close", resolve));
    let replies = 0;
    const replied = (count: number) =>
        new Promise<void>((resolve) => {
            const check = () => (replies >= count ? resolve() : setTimeout(check, 5));
            check();
        });
    ws.addEventListener("message", () => replies++);
    ws.send(JSON.stringify({ type: "session.update" }));
    ws.send(JSON.stringify({ type: "input_audio_buffer.append", audio }));
    ws.send(new Uint8Array(16));
    await replied(3);
    ws.send(JSON.stringify({ type: "bye" }));
    await closed;
    await Bun.sleep(30);
    await segmented.close();

    const exchange = await only("voice.interview");
    expect(exchange).toMatchObject({
        transport: "ws",
        url: `ws://127.0.0.1:${provider.port}/realtime`,
        status: 101,
        session_id: "iv-1",
        complete: true,
        end: { reason: "closed", close_code: 1000 },
    });
    expect(exchange.segments).toBeGreaterThanOrEqual(3);
    const data = exchange.frames.map((frame) => `${frame.dir} ${frame.data}`);
    expect(data).toEqual([
        // Wire order: all three sends go out before the first reply arrives.
        'out {"type":"session.update"}',
        'out {"type":"input_audio_buffer.append","audio":"[traumfaenger: omitted base64, 3000 bytes]"}',
        "out [traumfaenger: unrecorded binary message, 16 bytes]",
        'in {"type":"transcript.delta","delta":"got session.update"}',
        'in {"type":"transcript.delta","delta":"got input_audio_buffer.append"}',
        "in [traumfaenger: unrecorded binary message, 32 bytes]",
        'out {"type":"bye"}',
        'in {"type":"transcript.delta","delta":"got bye"}',
    ]);
    const times = exchange.frames.map((frame) => frame.t_ms);
    expect([...times].sort((a, b) => a - b)).toEqual(times);
});

test("broken strings and invalid context are cleaned instead of losing the exchange", async () => {
    const cut = "😀".slice(0, 1); // a lone surrogate
    await database.operation(
        {
            operation: `odd${cut}`,
            sessionId: "",
            metadata: { ok: `v${cut}`, "bad key": "dropped", long: "x".repeat(LIMITS.maxMetadataValueLen + 10) },
        },
        async () => {
            await (await recorded(providerUrl, { method: "POST", body: `{"text":"${cut}"}` })).text();
        }
    );
    await settle();

    const exchange = await only("odd\uFFFD");
    expect(exchange.truncated).toBe(true);
    expect(exchange.session_id).toBeNull();
    expect(exchange.metadata.ok).toBe("v\uFFFD");
    expect(exchange.metadata["bad key"]).toBeUndefined();
    expect(exchange.metadata.long?.length).toBe(LIMITS.maxMetadataValueLen);
    expect(exchange.frames[0]?.data).toBe('{"text":"\uFFFD"}');
    expect(database.stats().rejected).toBe(0);
});

test("binary responses are not buffered; oversized text is clipped to the limits", async () => {
    const audio = (async () =>
        new Response(new Uint8Array(16), { headers: { "content-type": "audio/mpeg", "content-length": "16" } })) as unknown as typeof fetch;
    const latin1 = (async () =>
        new Response(new Uint8Array(LIMITS.maxFrameBytes).fill(0xe4), {
            headers: { "content-type": "text/plain" },
        })) as unknown as typeof fetch;
    await database.operation({ operation: "speech" }, async () => {
        const response = await database.fetch(audio)(`${providerUrl}/speech`, { method: "POST", body: "{}" });
        expect((await response.arrayBuffer()).byteLength).toBe(16);
    });
    await database.operation({ operation: "latin1" }, async () => {
        await (await database.fetch(latin1)(providerUrl, { method: "POST", body: "{}" })).arrayBuffer();
    });
    await settle();

    expect((await only("speech")).frames[1]?.data).toBe("[traumfaenger: unrecorded audio/mpeg, 16 bytes]");
    const clipped = await only("latin1");
    expect(clipped.truncated).toBe(true);
    expect(new TextEncoder().encode(clipped.frames[1]?.data).byteLength).toBeLessThanOrEqual(LIMITS.maxFrameBytes);
});

/** An in-memory ingestion socket that answers every batch with `reply`. */
function fakeSocket(
    reply: (batch: { id: number; exchanges: unknown[] }) => object | undefined,
    options: { opens?: boolean; onSend?: () => void } = {}
): SocketFactory {
    return (_url, protocols) => {
        const listeners = new Map<string, ((event: any) => void)[]>();
        const emit = (type: string, event: object) => {
            for (const listener of listeners.get(type) ?? []) listener(event);
        };
        const socket = {
            protocol: protocols.includes("traumfaenger.v1.json") ? "traumfaenger.v1.json" : "",
            binaryType: "blob",
            send(data: string | Uint8Array) {
                options.onSend?.();
                const batch = JSON.parse(String(data));
                const answer = reply(batch);
                if (answer !== undefined) setTimeout(() => emit("message", { data: JSON.stringify(answer) }), 1);
            },
            close(code = 1000) {
                setTimeout(() => emit("close", { code }), 1);
            },
            addEventListener(type: string, listener: (event: any) => void) {
                listeners.set(type, [...(listeners.get(type) ?? []), listener]);
            },
        };
        if (options.opens !== false) setTimeout(() => emit("open", {}), 1);
        return socket;
    };
}

test("server rejections and a throwing onError never reach the caller", async () => {
    const rejecting = makeDatabase({
        encoding: "json",
        socketFactory: fakeSocket((batch) => ({
            type: "ack",
            batch: batch.id,
            accepted: 0,
            duplicates: 0,
            rejected: [{ index: 0, reason: "nope" }],
        })),
        onError: () => {
            throw new Error("handler bug");
        },
    });
    await (await rejecting.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
    await settle(rejecting);
    await rejecting.close(200);
    expect(rejecting.stats()).toMatchObject({ rejected: 1, queued: 0 });
});

test("an unreachable database backs off and keeps the exchanges queued", async () => {
    const offline = makeDatabase({ ingestUrl: "http://127.0.0.1:1", onError: () => {} });
    const response = await offline.fetch()(providerUrl, { method: "POST", body: "{}" });
    expect(response.status).toBe(200);
    await response.text();
    await settle(offline);
    await offline.flush(); // backing off: returns at once
    expect(offline.stats()).toMatchObject({ queued: 1, failedSends: 1, sent: 0 });
    const start = performance.now();
    await offline.close(1_500);
    expect(performance.now() - start).toBeLessThan(2_500);
    expect(offline.stats().queued).toBe(1);
    expect(offline.stats().failedSends).toBeGreaterThanOrEqual(2);
});

test("close() honours its deadline against a database that never acknowledges", async () => {
    const silent = makeDatabase({ encoding: "json", socketFactory: fakeSocket(() => undefined), onError: () => {} });
    await (await silent.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
    await Bun.sleep(20);
    const start = performance.now();
    await silent.close(300);
    expect(performance.now() - start).toBeLessThan(1_000);
    expect(silent.stats().queued).toBe(1);
    // Nothing is sent after close(); late records are counted as dropped.
    await (await silent.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
    await Bun.sleep(30);
    expect(silent.stats()).toMatchObject({ queued: 1, dropped: 1 });
});

test("a server error backs off instead of resending in a loop", async () => {
    let sends = 0;
    const failing = makeDatabase({
        encoding: "json",
        onError: () => {},
        socketFactory: fakeSocket((batch) => ({ type: "error", batch: batch.id, error: "internal", message: "db down" }), {
            onSend: () => sends++,
        }),
    });
    await (await failing.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
    await settle(failing);
    await Bun.sleep(300);
    expect(sends).toBe(1);
    expect(failing.stats()).toMatchObject({ queued: 1, failedSends: 1 });
    await failing.close(100);
});

test("an error without a batch id settles the oldest batch", async () => {
    const refusing = makeDatabase({
        encoding: "json",
        onError: () => {},
        socketFactory: fakeSocket(() => ({ type: "error", batch: null, error: "invalid_request", message: "bad" })),
    });
    await (await refusing.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
    await settle(refusing);
    expect(refusing.stats()).toMatchObject({ queued: 0, dropped: 1 });
    await refusing.close(100);
});

test("missing acks and hanging connects time out and back off", async () => {
    for (const [name, socketFactory] of [
        ["ack", fakeSocket(() => undefined)],
        ["connect", fakeSocket(() => undefined, { opens: false })],
    ] as const) {
        const stuck = makeDatabase({ encoding: "json", ackTimeoutMs: 100, connectTimeoutMs: 100, socketFactory, onError: () => {} });
        await (await stuck.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
        await Bun.sleep(30);
        const start = performance.now();
        await stuck.flush();
        expect(performance.now() - start, name).toBeLessThan(2_500);
        expect(stuck.stats(), name).toMatchObject({ queued: 1, failedSends: 1 });
        await stuck.close(50);
    }
});

test("long streams keep their start and end", async () => {
    await database.operation({ operation: "long.stream" }, async () => {
        await (await recorded(providerUrl, { method: "POST", body: JSON.stringify({ many: 12_000 }) })).text();
    });
    await settle();
    const exchange = await only("long.stream");
    expect(exchange.truncated).toBe(true);
    expect(exchange.frames.length).toBeLessThanOrEqual(LIMITS.maxFrames);
    expect(exchange.frames[1]?.data).toBe('data: {"n":0}');
    expect(exchange.frames.some((frame) => /^\[traumfaenger: omitted \d+ events/.test(frame.data))).toBe(true);
    expect(exchange.frames.slice(-2).map((frame) => frame.data)).toEqual([
        'data: {"usage":{"prompt_tokens":9}}',
        "data: [DONE]",
    ]);
});

test("socket recording keeps send arguments and handles late attachment", async () => {
    let callback = false;
    const fake = {
        url: "wss://api.example.com/v1/realtime",
        readyState: 1,
        send(_data: unknown, _options?: unknown, cb?: () => void) {
            cb?.();
        },
        addEventListener() {},
    };
    database.socket(fake).send("{}", { binary: false }, () => {
        callback = true;
    });
    expect(callback).toBe(true);

    database.operation({ operation: "late.socket" }, () =>
        database.socket({ ...fake, url: "wss://api.example.com/late", readyState: 3 })
    );
    await settle();
    const late = await only("late.socket");
    expect(late).toMatchObject({ transport: "ws", complete: true, frames: [], end: { reason: "closed" } });
});

test("summarises collected exchanges", async () => {
    const summary = await query.summary({ group_by: ["transport"], service: "test-service" });
    const transports = Object.fromEntries(summary.rows.map((row) => [row.group.transport, row]));
    expect(Object.keys(transports).sort()).toEqual(["http", "sse", "ws"]);
    expect(transports.ws?.records).toBeGreaterThan(transports.ws?.exchanges ?? 0);
    const failed = await query.summary({ operation: "unreachable" });
    expect(failed.rows[0]?.failed).toBe(1);
    expect(await query.getExchange("does/not exist")).toBeNull();
});

test("processes recorded exchanges and serves analytics", async () => {
    let status = await query.processing();
    for (let tries = 0; status.processed_seq < status.latest_seq && tries < 50; tries++) {
        await Bun.sleep(200);
        status = await query.processing();
    }
    expect(status.processed_seq).toBe(status.latest_seq);
    expect(status.calls).toBeGreaterThan(0);
    const byTransport = await query.analytics("calls", {
        metrics: ["calls", "output_tokens"],
        groupBy: ["transport"],
        filters: { service: "test-service", transport: ["sse"] },
    });
    expect(byTransport.rows.length).toBe(1);
    expect(byTransport.rows[0]?.transport).toBe("sse");
    expect(Number(byTransport.rows[0]?.calls)).toBeGreaterThan(0);
    const hourly = await query.analytics("calls", { metrics: ["calls"], interval: "1h", from: new Date(0) });
    expect(typeof hourly.rows[0]?.time).toBe("string");
    await expect(query.analytics("calls", { metrics: ["nope"] })).rejects.toMatchObject({ code: "invalid_request" });
    expect(await query.call("does/not exist")).toBeNull();
    expect(await query.session("no-such-session")).toBeNull();
});

test("splits SSE text into events across chunk boundaries", () => {
    expect(splitEvents("event: a\ndata: 1\n\ndata: 2\r\n\r\n\n\n: ping\n\n")).toEqual(["event: a\ndata: 1", "data: 2", ": ping"]);
    const splitter = new SseSplitter();
    expect(splitter.push("data: 1\r")).toEqual([]);
    expect(splitter.push("\n\r")).toEqual([]);
    expect(splitter.push("\ndata: 2")).toEqual(["data: 1"]);
    expect(splitter.end()).toEqual(["data: 2"]);
    const boundary = new SseSplitter();
    expect(boundary.push("data: 1\n\n")).toEqual(["data: 1"]);
    expect(boundary.push("\ndata: 2\n")).toEqual([]);
    expect(boundary.push("\n")).toEqual(["data: 2"]);
});

test("replaces inline base64 payloads but keeps everything else", () => {
    const image = `data:image/png;base64,${"A".repeat(4000)}`;
    expect(prepareData(JSON.stringify({ image_url: { url: image } }))).toBe(
        JSON.stringify({ image_url: { url: "[traumfaenger: omitted image/png data, 3000 bytes]" } })
    );
    expect(prepareData(`{"audio":"${"B".repeat(1024)}"}`)).toBe('{"audio":"[traumfaenger: omitted base64, 768 bytes]"}');
    const short = `{"id":"${"C".repeat(1023)}","n":12345678901234567890}`;
    expect(prepareData(short)).toBe(short);
});

test("encodes queries like the server expects", () => {
    expect(toSearch({})).toBe("");
    expect(toSearch({ group_by: ["operation", "day"], limit: 5, failed: true, transport: "ws" })).toBe(
        "?group_by=operation%2Cday&limit=5&failed=true&transport=ws"
    );
});
