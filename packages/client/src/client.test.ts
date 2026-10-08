// End-to-end: the generated types and this client against the real Rust server.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { join } from "node:path";
import { AiLedgerError, batches, IngestClient, LIMITS, type NewAiCall, QueryClient, toSearch } from "./index.ts";

const root = join(import.meta.dir, "../../..");
const ingestToken = "ingest-token-0123456789";
const queryToken = "query-token-0123456789";
const ingestPort = 20_000 + Math.floor(Math.random() * 20_000);
const queryPort = ingestPort + 1;
let server: ReturnType<typeof Bun.spawn> | undefined;

const ingest = new IngestClient({ baseUrl: `http://127.0.0.1:${ingestPort}/`, token: ingestToken });
const query = new QueryClient({ baseUrl: `http://127.0.0.1:${queryPort}`, token: queryToken });

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
            await ingest.health();
            return;
        } catch {
            await Bun.sleep(50);
        }
    }
    throw new Error("ai-ledger-server did not become healthy");
}, 300_000);

afterAll(() => {
    server?.kill();
});

function call(id: string, overrides: Partial<NewAiCall> = {}): NewAiCall {
    return {
        id,
        occurred_at: "2026-10-08T10:00:00Z",
        feature: "listing.parse",
        entity_type: "listing",
        entity_id: "42",
        trace_id: "trace-1",
        provider: "openrouter",
        model: "openai/gpt-5-mini",
        status: "ok",
        input_tokens: 1200,
        cached_input_tokens: 200,
        output_tokens: 300,
        cost_usd: 0.004,
        latency_ms: 850,
        metadata: { prompt_version: 2 },
        ...overrides,
    };
}

test("ingests, lists, fetches and summarises through the typed client", async () => {
    const calls = [
        call("a"),
        call("b", { occurred_at: "2026-10-08T10:00:01Z", attempt: 2, status: "error", error: "timeout" }),
        call("c", { occurred_at: "2026-10-08T11:00:00Z", feature: "application.greeting", entity_id: "7" }),
    ];
    expect(await ingest.ingest(calls)).toEqual({ accepted: 3, duplicates: 0 });
    expect(await ingest.ingest(calls)).toEqual({ accepted: 0, duplicates: 3 });

    const page = await query.listCalls({ entity_id: "42", limit: 1 });
    expect(page.calls.map((c) => c.id)).toEqual(["b"]);
    expect(page.next_cursor).not.toBeNull();

    const ids: string[] = [];
    for await (const stored of query.iterateCalls({ limit: 1 })) ids.push(stored.id);
    expect(ids).toEqual(["c", "b", "a"]);

    const stored = await query.getCall("b");
    expect(stored).toMatchObject({ attempt: 2, status: "error", error: "timeout", metadata: { prompt_version: 2 } });
    expect(await query.getCall("does/not exist")).toBeNull();

    const summary = await query.summary({ group_by: ["feature"], from: "2026-10-08T00:00:00Z" });
    expect(summary.truncated).toBe(false);
    expect(summary.rows.map((row) => [row.group.feature, row.calls, row.errors])).toEqual([
        ["listing.parse", 2, 1],
        ["application.greeting", 1, 0],
    ]);
    expect(summary.rows[0]?.cost_usd).toBeCloseTo(0.008, 9);
});

test("splits large inputs into server-sized batches", async () => {
    const calls = Array.from({ length: 1001 }, (_, index) => call(`bulk-${index}`, { feature: "bulk" }));
    expect(await ingest.ingest(calls)).toEqual({ accepted: 1001, duplicates: 0 });
    const [row] = (await query.summary({ feature: "bulk" })).rows;
    expect(row?.calls).toBe(1001);
});

test("surfaces typed server errors", async () => {
    const rejected = await ingest.ingest([call("bad", { cost_usd: -1 })]).catch((error: unknown) => error);
    expect(rejected).toBeInstanceOf(AiLedgerError);
    expect(rejected).toMatchObject({ status: 400, code: "invalid_request" });

    const wrongToken = new QueryClient({ baseUrl: `http://127.0.0.1:${queryPort}`, token: ingestToken });
    expect(await wrongToken.listCalls().catch((error: unknown) => error)).toMatchObject({
        status: 401,
        code: "unauthorized",
    });

    const unreachable = new QueryClient({ baseUrl: "http://127.0.0.1:1", timeoutMs: 1_000 });
    expect(await unreachable.health().catch((error: unknown) => error)).toMatchObject({ status: 0, code: "network" });
});

test("encodes queries like the server expects", () => {
    expect(toSearch({})).toBe("");
    expect(toSearch({ group_by: [] })).toBe("");
    expect(toSearch({ group_by: ["feature", "day"], limit: 5, model: "a/b c" })).toBe(
        "?group_by=feature%2Cday&limit=5&model=a%2Fb+c"
    );
});

test("batches by count and by serialized size", () => {
    const small = Array.from({ length: 2500 }, (_, index) => call(`s-${index}`));
    expect([...batches(small)].map((batch) => batch.length)).toEqual([1000, 1000, 500]);

    // ~16 KiB per call: 1000 of them would exceed the body limit.
    const big = Array.from({ length: 1000 }, (_, index) =>
        call(`b-${index}`, { error: "e".repeat(4000), metadata: { pad: "m".repeat(8000) }, feature: "f".repeat(512) })
    );
    const sizes = [...batches(big)];
    expect(sizes.length).toBeGreaterThan(1);
    expect(sizes.flat().length).toBe(1000);
    for (const batch of sizes) {
        expect(JSON.stringify({ calls: batch }).length).toBeLessThanOrEqual(LIMITS.maxBodyBytes);
    }
});
