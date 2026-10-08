# ai-ledger

Records every AI model call your services make, exactly as it went over the wire, so you can later trace cost, tokens, latency, messages and tool calls back to the single operation that caused them.

- **Collection:** a TypeScript `fetch` wrapper records each provider call (the request and response bodies plus context) and sends it in the background. It works as the `fetch` option of any Vercel AI SDK provider, or around a plain `fetch`.
- **Rust server:** one binary, with DuckDB as embedded storage and no UI. It runs two separate HTTP APIs, each on its own port with its own bearer token:
  - **ingestion** (write-only, `:7600`): `POST /v1/calls`
  - **query** (read-only, `:7601`): `GET /v1/calls`, `GET /v1/calls/{id}`, `GET /v1/summary`
- **End-to-end types:** the wire contract is defined once in Rust (`crates/api`). [ts-rs](https://github.com/Aleph-Alpha/ts-rs) generates `@traumwohnung/ai-ledger-api` from it; `@traumwohnung/ai-ledger-client` builds on that.

Nothing here is specific to one project.

## Design: CQRS

```
clients ──POST /v1/calls──▶ raw_calls (append-only log, ordered by seq)
                                 │
                                 ▼  projections (not built yet)
               calls · messages · tool_calls · metrics per hour/day · …
                                 │
                                 ▼
                           query API / dashboards
```

- **Write side:** ingestion validates the envelope and appends the record to `raw_calls` unparsed. It never interprets provider formats, so a parser bug can't lose data.
- **Read side:** each projection reads `raw_calls` in `seq` order and maintains its own tables. It stores how far it got in `projection_checkpoints`, and rebuilding one means resetting its checkpoint.
  - Projections will extract provider, model, usage, prices and cost, finish reasons, deduplicated messages and tool calls, and aggregates per time bucket. A new field or a parser fix is a server change plus a replay, with no client release.
- **Today:** only collection exists. `GET /v1/summary` reports collection statistics computed directly from the log: calls, failures, bytes and latency.

## What a client sends

A client sends only what isn't in the request or response. Everything else is derived on the server.

```ts
type CallRecord = {
    id: string;                         // idempotency key
    service: string;                    // "traumwohnung-web"
    environment: string;                // "production"
    operation: string;                  // "listing.parse"
    session_id?: string;                // groups one conversation
    metadata?: Record<string, string>;  // { listing_id: "123" }
    url: string;                        // query string is dropped on the server
    request: string;                    // request body, verbatim
    response?: string | string[];       // body, or one entry per SSE event
    http_status?: number;               // absent when no response arrived
    error?: string;                     // client-side exception, e.g. an abort
    truncated?: boolean;                // client clipped request/response to the limits
    started_at: string;
    ttft_ms?: number;                   // streams: time to the first chunk
    latency_ms: number;
};
```

## TypeScript

```ts
import { createLedger } from "@traumwohnung/ai-ledger-client";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

export const ledger = createLedger({
    ingestUrl: "http://ai-ledger:7600",
    token: process.env.AI_LEDGER_INGEST_TOKEN,
    service: "traumwohnung-web",
    environment: "production",
});

const openrouter = createOpenAICompatible({
    name: "openrouter",
    baseURL: "https://openrouter.ai/api/v1",
    apiKey,
    fetch: ledger.fetch(),          // or ledger.fetch(existingFetchWrapper)
    includeUsage: true,             // streams then carry usage in their last event
});

await ledger.operation({ operation: "listing.parse", metadata: { listing_id: id } }, () =>
    generateText({ model: openrouter(modelId), prompt }),
);
```

- **Context:** `ledger.operation` puts the context in `AsyncLocalStorage`, so every call inside it is tagged, including every step of an agent loop. Nested operations override `operation` and merge `metadata`. Calls outside any operation are recorded as `unattributed`.
- **Recording never changes or delays the call:**
  - Non-streaming responses are read from a `clone()` in the background.
  - Streams are passed through chunk by chunk (no tee), so a caller that cancels still cancels the provider request. The cancellation is recorded as `error`.
  - Inline base64 data (images, files) is replaced by a placeholder, and bodies over 4 MiB are clipped (`truncated: true`). Binary responses (audio, images) are not read; only their type and size are recorded.
  - The request goes out before anything is recorded; the request body is prepared after the response.
  - Context is cleaned instead of failing the record: the URL loses its query string, invalid metadata keys are dropped, and broken characters are replaced. Any such change sets `truncated: true`.
- **Sending:** records are queued and sent gzip-compressed every 2 seconds, or earlier once 4 MiB or 500 records are queued.
  - Failed sends are retried with exponential backoff (1 s up to 60 s). Duplicates are harmless because `id` is the idempotency key.
  - Beyond 64 MiB queued, the oldest records are dropped.
  - Records the server rejects are reported to `onError`; `ledger.stats()` shows the counts. A throwing `onError` is ignored.
  - On shutdown, `await ledger.close(timeoutMs)` keeps sending for up to `timeoutMs` (default 5 s) and reports anything left unsent.

`QueryClient` reads the query API: `listCalls`, `iterateCalls`, `getCall` (with bodies) and `summary`.

Both packages ship TypeScript sources, so consumers need a TS-aware runtime or bundler (Bun, Vite, Astro, tsx). The collector uses `node:async_hooks` and `node:zlib`, so it runs server-side only.

## Run

```sh
AI_LEDGER_INGEST_TOKEN=$(openssl rand -hex 32) \
AI_LEDGER_QUERY_TOKEN=$(openssl rand -hex 32) \
cargo run --release -p ai-ledger-server
```

Docker: `docker build -t ai-ledger .`. The image stores data in the `/data` volume and listens on `0.0.0.0:7600` and `0.0.0.0:7601`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AI_LEDGER_DATABASE` | `ai-ledger.duckdb` | DuckDB file, or `:memory:` |
| `AI_LEDGER_INGEST_ADDR` | `127.0.0.1:7600` | Ingestion listener |
| `AI_LEDGER_QUERY_ADDR` | `127.0.0.1:7601` | Query listener |
| `AI_LEDGER_INGEST_TOKEN` | required | Bearer token of the ingestion API, at least 16 characters |
| `AI_LEDGER_QUERY_TOKEN` | required | Bearer token of the query API; must differ from the ingestion token |
| `AI_LEDGER_AUTH` | `required` | Set `disabled` to run without tokens, for local development only |
| `AI_LEDGER_RETENTION_DAYS` | unset (keep) | Delete raw records this many days after receipt, but only once every projection has processed them. While no projection exists, nothing is deleted |
| `AI_LEDGER_MEMORY_LIMIT` | DuckDB default | DuckDB `memory_limit`, e.g. `512MB` |
| `RUST_LOG` | `info` | Log filter |

**Operating notes:**
- Run a single instance per database file; DuckDB allows only one writing process.
- The server never downloads DuckDB extensions, so it runs offline.
- `GET /health` is unauthenticated.
- Raw records contain full prompts and answers, which can include personal data. Keep the query listener on a private network and set a retention period.

## API

All bodies are JSON. Errors are `{ "error": ErrorCode, "message": string }` with status 400, 401, 404, 405, 413 or 500. `crates/api/src/lib.rs` documents every field and limit.

**`POST /v1/calls`** takes `{ "calls": CallRecord[] }` with 1–500 records and at most 32 MiB after gzip decompression (`Content-Encoding: gzip` is accepted).
- Each record is parsed and validated on its own: invalid records come back in `rejected` with their index and reason, and the rest are stored. Unknown fields are ignored, so newer clients work with older servers.
- At most 4 ingestion requests are processed at once; further requests wait.
- The response is `{ accepted, duplicates, rejected }`.

**Query API:**
- **`GET /v1/calls`** returns envelopes without bodies, most recently stored first.
  - Filters: `service`, `environment`, `operation`, `session_id`, `failed`, `from` and `to` (on `started_at`).
  - Paging: `limit` and `cursor`.
- **`GET /v1/calls/{id}`** returns the envelope plus `request` and `response`.
- **`GET /v1/summary`** takes the same filters plus `group_by`, a comma-separated list of `service`, `environment`, `operation`, `session_id`, `http_status`, `day` and `hour`.
  - Each row has calls, failed, request and response bytes, latency avg/p95/p99 and TTFT p95.

In hand-written URLs, write timestamps with `Z`, or encode `+` as `%2B`.

## Changing the contract

1. Edit `crates/api/src/lib.rs`.
2. Run `bun run gen` to rewrite `packages/api/src/generated.ts`. `cargo test` fails while the two differ.
3. Schema changes are appended to `MIGRATIONS` in `crates/server/src/store.rs`; never edit a released entry.

## Develop

```sh
cargo test                                          # contract, store and HTTP tests
bun install && bun run typecheck && bun run test    # collector + AI SDK against a mock provider and the real server
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```
