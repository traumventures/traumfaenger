// End-to-end: a mock AI provider, the collector in between, and the real Rust server.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { join } from "node:path";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { generateText, streamText } from "ai";
import {
    AiLedgerError,
    createLedger,
    LIMITS,
    prepareRequest,
    QueryClient,
    splitEvents,
    toSearch,
    UNATTRIBUTED,
} from "./index.ts";

const root = join(import.meta.dir, "../../..");
const ingestToken = "ingest-token-0123456789";
const queryToken = "query-token-0123456789";
const ingestPort = 20_000 + Math.floor(Math.random() * 20_000);
const queryPort = ingestPort + 1;
let server: ReturnType<typeof Bun.spawn> | undefined;

const query = new QueryClient({ baseUrl: `http://127.0.0.1:${queryPort}`, token: queryToken });
const errors: unknown[] = [];
const ledger = createLedger({
    ingestUrl: `http://127.0.0.1:${ingestPort}/`,
    token: ingestToken,
    service: "test-service",
    environment: "test",
    flushIntervalMs: 60_000,
    onError: (error) => errors.push(error),
});

const SSE_EVENTS = [
    'data: {"choices":[{"delta":{"content":"Hal"}}]}',
    'data: {"choices":[{"delta":{"content":"lo"}}],"usage":{"prompt_tokens":5,"completion_tokens":2}}',
    "data: [DONE]",
];

// A fake provider: JSON for normal requests, SSE when `stream` is set, and a
// slow endless stream for cancellation.
const provider = Bun.serve({
    port: 0,
    async fetch(request) {
        const body = (await request.json()) as { stream?: boolean; endless?: boolean; model?: string };
        if (body.model === "sdk-model") return openAiResponse(body.stream === true);
        if (body.endless) {
            let index = 0;
            return new Response(
                new ReadableStream({
                    async pull(controller) {
                        await Bun.sleep(5);
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
        return Response.json({ choices: [{ message: { role: "assistant", content: "Hallo" } }] }, { status: 200 });
    },
});
const providerUrl = `http://127.0.0.1:${provider.port}/api/v1/chat/completions`;

function openAiResponse(stream: boolean): Response {
    const usage = { prompt_tokens: 7, completion_tokens: 2, total_tokens: 9 };
    if (!stream) {
        return Response.json({
            id: "gen-1",
            object: "chat.completion",
            created: 0,
            model: "sdk-model",
            choices: [{ index: 0, message: { role: "assistant", content: "Hallo" }, finish_reason: "stop" }],
            usage,
        });
    }
    const chunk = (delta: object, extra: object = {}) =>
        `data: ${JSON.stringify({ id: "gen-2", object: "chat.completion.chunk", created: 0, model: "sdk-model", choices: [{ index: 0, delta, finish_reason: null }], ...extra })}\n\n`;
    const body =
        chunk({ role: "assistant", content: "Hal" }) +
        chunk({ content: "lo" }) +
        `data: ${JSON.stringify({ id: "gen-2", object: "chat.completion.chunk", created: 0, model: "sdk-model", choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage })}\n\n` +
        "data: [DONE]\n\n";
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
}

beforeAll(async () => {
    const build = Bun.spawnSync(["cargo", "build", "-q", "-p", "ai-ledger-server"], { cwd: root, stderr: "inherit" });
    if (build.exitCode !== 0) throw new Error("cargo build failed");
    server = Bun.spawn([join(root, "target/debug/ai-ledger-server")], {
        cwd: root,
        env: {
            ...process.env,
            AI_LEDGER_DATABASE: ":memory:",
            AI_LEDGER_INGEST_ADDR: `127.0.0.1:${ingestPort}`,
            AI_LEDGER_QUERY_ADDR: `127.0.0.1:${queryPort}`,
            AI_LEDGER_INGEST_TOKEN: ingestToken,
            AI_LEDGER_QUERY_TOKEN: queryToken,
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
    throw new Error("ai-ledger-server did not become healthy");
}, 300_000);

afterAll(async () => {
    await ledger.close();
    provider.stop(true);
    server?.kill();
});

const recorded = ledger.fetch();

/** Waits for background reads to finish, then sends everything. */
async function settle(): Promise<void> {
    await Bun.sleep(20);
    await ledger.flush();
}

async function onlyCall(operation: string) {
    const page = await query.listCalls({ operation });
    expect(page.calls).toHaveLength(1);
    const call = await query.getCall(page.calls[0]?.id ?? "");
    expect(call).not.toBeNull();
    return call!;
}

test("records a JSON call with its operation context, verbatim", async () => {
    const requestBody = JSON.stringify({ model: "m", messages: [{ role: "user", content: "Hi" }] });
    const response = await ledger.operation(
        { operation: "listing.parse", sessionId: "s-1", metadata: { listing_id: "123" } },
        () => recorded(`${providerUrl}?key=secret`, { method: "POST", body: requestBody })
    );
    // The caller gets the provider's response unchanged.
    expect(await response.json()).toEqual({ choices: [{ message: { role: "assistant", content: "Hallo" } }] });
    await settle();

    const call = await onlyCall("listing.parse");
    // The query string is removed before the record leaves the process.
    expect(call).toMatchObject({
        service: "test-service",
        environment: "test",
        session_id: "s-1",
        metadata: { listing_id: "123" },
        url: providerUrl,
        request: requestBody,
        response: JSON.stringify({ choices: [{ message: { role: "assistant", content: "Hallo" } }] }),
        http_status: 200,
        error: null,
        truncated: false,
        ttft_ms: null,
    });
});

test("passes a stream through unchanged and records its events", async () => {
    const response = await ledger.operation({ operation: "support.chat" }, () =>
        recorded(providerUrl, { method: "POST", body: JSON.stringify({ stream: true }) })
    );
    expect(response.headers.get("content-type")).toContain("text/event-stream");
    expect(await response.text()).toBe(SSE_EVENTS.map((event) => `${event}\n\n`).join(""));
    await settle();

    const call = await onlyCall("support.chat");
    expect(call.response).toEqual(SSE_EVENTS);
    expect(call.ttft_ms).not.toBeNull();
    expect(call.error).toBeNull();
});

test("records a cancelled stream with its partial events and stops the provider stream", async () => {
    const response = await ledger.operation({ operation: "cancelled.chat" }, () =>
        recorded(providerUrl, { method: "POST", body: JSON.stringify({ endless: true }) })
    );
    const reader = response.body!.getReader();
    await reader.read();
    await reader.read();
    await reader.cancel("user closed the chat");
    await settle();

    const call = await onlyCall("cancelled.chat");
    expect(call.error).toBe("CancelledError: stream cancelled by caller: user closed the chat");
    expect((call.response as string[]).length).toBeGreaterThanOrEqual(2);
    expect((call.response as string[])[0]).toBe('data: {"n":0}');
});

test("records network failures and rethrows them to the caller", async () => {
    const failure = await ledger
        .operation({ operation: "unreachable" }, () => recorded("http://127.0.0.1:1/v1/chat", { method: "POST", body: "{}" }))
        .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(Error);
    await settle();

    const call = await onlyCall("unreachable");
    expect(call.http_status).toBeNull();
    expect(call.response).toBeNull();
    expect(call.error).toBeTruthy();
});

test("nested operations merge metadata; calls outside any operation are unattributed", async () => {
    await ledger.operation({ operation: "outer", sessionId: "s-2", metadata: { a: "1", b: "1" } }, () =>
        ledger.operation({ operation: "inner.step", metadata: { b: "2" } }, async () => {
            await (await recorded(providerUrl, { method: "POST", body: "{}" })).text();
        })
    );
    await (await recorded(new Request(providerUrl, { method: "POST", body: '{"x":1}' }))).text();
    await settle();

    const inner = await onlyCall("inner.step");
    expect(inner.session_id).toBe("s-2");
    expect(inner.metadata).toEqual({ a: "1", b: "2" });
    const unattributed = await onlyCall(UNATTRIBUTED);
    expect(unattributed.request).toBe('{"x":1}');
});

test("works as the fetch of an AI SDK provider", async () => {
    const sdk = createOpenAICompatible({
        name: "test",
        baseURL: `http://127.0.0.1:${provider.port}/api/v1`,
        fetch: ledger.fetch(),
        includeUsage: true,
    });
    const generated = await ledger.operation({ operation: "sdk.generate" }, () =>
        generateText({ model: sdk("sdk-model"), prompt: "Hi" })
    );
    expect(generated.text).toBe("Hallo");

    const streamed = await ledger.operation({ operation: "sdk.stream", sessionId: "chat-9" }, async () => {
        const result = streamText({ model: sdk("sdk-model"), prompt: "Hi" });
        return await result.text;
    });
    expect(streamed).toBe("Hallo");
    await settle();

    const generate = await onlyCall("sdk.generate");
    expect(JSON.parse(generate.request)).toMatchObject({ model: "sdk-model", messages: [{ role: "user", content: "Hi" }] });
    expect(JSON.parse(generate.response as string).usage.prompt_tokens).toBe(7);

    const stream = await onlyCall("sdk.stream");
    expect(stream.session_id).toBe("chat-9");
    expect(JSON.parse(stream.request)).toMatchObject({ stream: true, stream_options: { include_usage: true } });
    const events = stream.response as string[];
    expect(events.at(-1)).toBe("data: [DONE]");
    expect(events.some((event) => event.includes('"prompt_tokens":7'))).toBe(true);
});

test("summarises collected calls", async () => {
    const summary = await query.summary({ group_by: ["service", "operation"], service: "test-service" });
    const operations = summary.rows.map((row) => row.group.operation);
    expect(operations).toContain("listing.parse");
    expect(summary.rows.find((row) => row.group.operation === "unreachable")?.failed).toBe(1);

    const ids: string[] = [];
    for await (const call of query.iterateCalls({ limit: 2 })) ids.push(call.id);
    expect(ids.length).toBe(summary.rows.reduce((sum, row) => sum + row.calls, 0));
    expect(await query.getCall("does/not exist")).toBeNull();
});

test("broken strings and invalid context are cleaned instead of losing the call", async () => {
    const cut = "😀".slice(0, 1); // a lone surrogate
    await ledger.operation(
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

    const call = await onlyCall("odd\uFFFD");
    expect(call.truncated).toBe(true);
    expect(call.session_id).toBeNull();
    expect(call.metadata.ok).toBe("v\uFFFD");
    expect(call.metadata["bad key"]).toBeUndefined();
    expect(call.metadata.long?.length).toBe(LIMITS.maxMetadataValueLen);
    expect(call.request).toBe('{"text":"\uFFFD"}');
    expect(ledger.stats().rejected).toBe(0);
});

test("server rejections, a throwing onError and an unreachable ledger never reach the caller", async () => {
    const rejecting = createLedger({
        ingestUrl: "http://ledger.invalid",
        service: "s",
        environment: "e",
        flushIntervalMs: 60_000,
        fetch: (async () =>
            Response.json({ accepted: 0, duplicates: 0, rejected: [{ index: 0, reason: "nope" }] })) as unknown as typeof fetch,
        onError: () => {
            throw new Error("handler bug");
        },
    });
    await (await rejecting.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
    await Bun.sleep(20);
    await rejecting.close(100);
    expect(rejecting.stats()).toMatchObject({ rejected: 1, queued: 0 });

    const offline = createLedger({
        ingestUrl: "http://127.0.0.1:1",
        service: "s",
        environment: "e",
        flushIntervalMs: 60_000,
        onError: () => {
            throw new Error("handler bug");
        },
    });
    const response = await offline.fetch()(providerUrl, { method: "POST", body: "{}" });
    expect(response.status).toBe(200);
    await response.text();
    await Bun.sleep(20);
    // Backs off after a failure instead of retrying on every call.
    await offline.flush();
    await offline.flush();
    expect(offline.stats()).toMatchObject({ queued: 1, failedSends: 1, sent: 0 });
    await offline.close(1_500);
    expect(offline.stats().queued).toBe(1);
    expect(offline.stats().failedSends).toBeGreaterThanOrEqual(2);
});

test("close() honours its deadline against a ledger that never answers", async () => {
    const hanging = Bun.serve({ port: 0, fetch: () => new Promise<Response>(() => {}) });
    const stuck = createLedger({
        ingestUrl: `http://127.0.0.1:${hanging.port}`,
        service: "s",
        environment: "e",
        flushIntervalMs: 60_000,
        onError: () => {},
    });
    await (await stuck.fetch()(providerUrl, { method: "POST", body: "{}" })).text();
    await Bun.sleep(20);
    const start = performance.now();
    await stuck.close(300);
    expect(performance.now() - start).toBeLessThan(1_500);
    hanging.stop(true);
});

test("responses that grow when decoded are clipped to the server limit", async () => {
    const latin1 = (async () =>
        new Response(new Uint8Array(2 * 1024 * 1024).fill(0xe4), {
            headers: { "content-type": "text/plain" },
        })) as unknown as typeof fetch;
    await ledger.operation({ operation: "latin1" }, async () => {
        await (await ledger.fetch(latin1)(providerUrl, { method: "POST", body: "{}" })).arrayBuffer();
    });
    await settle();
    const call = await onlyCall("latin1");
    expect(call.truncated).toBe(true);
    expect(call.response_bytes).toBeLessThanOrEqual(LIMITS.maxResponseBytes);
});

test("binary responses are not buffered", async () => {
    const binary = createLedger({
        ingestUrl: `http://127.0.0.1:${ingestPort}`,
        token: ingestToken,
        service: "s",
        environment: "e",
        flushIntervalMs: 60_000,
    });
    const audio = (async () =>
        new Response(new Uint8Array(16), {
            headers: { "content-type": "audio/mpeg", "content-length": "16" },
        })) as unknown as typeof fetch;
    await binary.operation({ operation: "speech" }, async () => {
        const response = await binary.fetch(audio)(`${providerUrl}/speech`, { method: "POST", body: "{}" });
        expect((await response.arrayBuffer()).byteLength).toBe(16);
    });
    await Bun.sleep(20);
    await binary.close();
    const call = await onlyCall("speech");
    expect(call.response).toBe("[ai-ledger: unrecorded audio/mpeg, 16 bytes]");
});

test("the wrong token is reported as a typed error", async () => {
    const wrong = new QueryClient({ baseUrl: `http://127.0.0.1:${queryPort}`, token: ingestToken });
    const error = await wrong.listCalls().catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(AiLedgerError);
    expect(error).toMatchObject({ status: 401, code: "unauthorized" });
});

test("splits SSE bodies into verbatim events", () => {
    expect(splitEvents("event: a\ndata: 1\n\ndata: 2\r\n\r\n\n\n: ping\n\n")).toEqual([
        "event: a\ndata: 1",
        "data: 2",
        ": ping",
    ]);
    expect(splitEvents("")).toEqual([]);
});

test("replaces inline base64 data and clips oversized requests", () => {
    const image = `data:image/png;base64,${"A".repeat(4000)}`;
    const prepared = prepareRequest(JSON.stringify({ messages: [{ content: [{ image_url: { url: image } }] }] }));
    expect(prepared.truncated).toBe(false);
    expect(prepared.text).toBe(
        JSON.stringify({ messages: [{ content: [{ image_url: { url: "[ai-ledger: omitted image/png data, 3000 bytes]" } }] }] })
    );
    expect(prepareRequest("short;base64,not json").text).toBe("short;base64,not json");

    const huge = prepareRequest("ä".repeat(LIMITS.maxRequestBytes));
    expect(huge.truncated).toBe(true);
    expect(new TextEncoder().encode(huge.text).byteLength).toBeLessThanOrEqual(LIMITS.maxRequestBytes);
});

test("encodes queries like the server expects", () => {
    expect(toSearch({})).toBe("");
    expect(toSearch({ group_by: [] })).toBe("");
    expect(toSearch({ group_by: ["operation", "day"], limit: 5, failed: true })).toBe(
        "?group_by=operation%2Cday&limit=5&failed=true"
    );
});
