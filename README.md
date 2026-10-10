# traumfaenger

Records the traffic between your services and AI providers: every HTTP call, SSE stream and WebSocket session, exactly as it went over the wire and timestamped per frame. The server then processes the raw log into calls with model, provider, outcome, tokens, cost and timings, plus message parts and tool calls, and serves analytics over them, split by model, workload, provider, tool, session or any metadata key.

- **Recording:** clients record exchanges and stream them to the database in the background.
  - **TypeScript** (`@traumfaenger/client`): `database.fetch()` wraps `fetch` and works as the `fetch` option of any Vercel AI SDK provider; `database.socket()` wraps provider WebSockets, for example realtime voice APIs.
  - **Rust** (`traumfaenger-client`): a `Database` handle with `HttpRecording` and `SocketRecording` for any transport, plus a typed `QueryClient`.
  - **rig** (`traumfaenger-rig`): `Recorded` wraps the HTTP and WebSocket client of any rig provider.
- **Rust server:** one binary, with DuckDB as embedded storage and no UI. It runs two separate APIs, each on its own port with its own bearer token:
  - **ingestion** (write-only, `:7600`): a WebSocket at `GET /v1/ingest` that speaks JSON or MessagePack, negotiated as a subprotocol, plus `POST /v1/exchanges` for one JSON batch.
  - **query** (read-only, `:7601`): analytics (`GET /v1/analytics/{calls|tools|parts}`), call and session details, stored texts, processing status, and the raw log.
- **End-to-end types:** the wire contract is defined once in Rust (`crates/api`). [ts-rs](https://github.com/Aleph-Alpha/ts-rs) generates `@traumfaenger/api` from it; `@traumfaenger/client` builds on that.

Nothing here is specific to one project. Each traumfaenger server is one **database**; run one per environment (production, dev, …) instead of tagging records with an environment.

## Design: CQRS

```
clients ──WS /v1/ingest──▶ raw log: raw_exchanges + raw_frames (append-only, ordered by seq)
                                 │
                                 ▼  processing (traumfaenger-process), batches of 500 in seq order
        calls · call_parts · call_metadata · tool_calls · contents (each distinct text once)
                                 │
                                 ▼
             query API: /v1/analytics/{calls,tools,parts} · /v1/sessions · /v1/calls → Grafana
```

- **Write side:** ingestion validates each record's envelope and appends it unparsed. It never interprets provider formats, so a parser bug can't lose data.
- **Read side:** processing reads the raw log in `seq` order, 500 records at a time, and writes each window with its checkpoint in one transaction (`projection_checkpoints`, `calls@<parser version>`). Every exchange becomes exactly one `calls` row.
  - An exchange is processed once its final record is read, from all its segments. A segment arriving after the final record reprocesses it.
  - A parser panic stores that call as `unparsed` (`parse_reason = panic`). A window that keeps failing is retried in smaller steps; a single record that keeps panicking is skipped and logged.
  - A new parser version starts over from the beginning of the raw log and replaces the rows of every exchange it reprocesses. The processed rows of exchanges whose raw records are gone stay.
  - Raw retention (`TRAUMFAENGER_RETENTION_DAYS`) deletes only exchanges that have been processed; segments of an exchange that never finished go a week later.

### Processing

`traumfaenger-process` turns one exchange into one call and never fails: unknown formats are kept as `unparsed`, with whatever could still be read (status, timing, outcome).

| | |
|---|---|
| APIs | OpenAI-style chat completions (OpenRouter, OpenAI, Langdock), Anthropic messages, Gemini `generateContent`, transcriptions, embeddings; plain or SSE |
| Call | gateway (`openrouter`, `langdock`, …), upstream provider, requested and served model, outcome (`ok`, `rate_limited`, `client_error`, `provider_error`, `network_error`, `cancelled`, `incomplete`), finish reason, error |
| Usage | input (including cached reads and cache writes), cached, cache write, output (including reasoning), reasoning |
| Cost | `reported` when the gateway says what it charged (OpenRouter `usage.cost`), else `priced` from `crates/process/src/pricing.rs`, else `unknown`; split into input and output cost |
| Timing | latency; time to first output, from the first event that carries text, reasoning or a tool call (keep-alives and empty deltas do not count) |
| Parts | every message part (`text`, `reasoning`, `tool_call`, `tool_result`, `tool_definitions`, `image`, …) with role and side; tool results are named after the call they answer and flagged on error |
| Turns | a call whose request ends with a user message starts a turn; calls ending with tool results continue it |

**Storage.** Every distinct text (system prompt, message, tool definitions, arguments, result) is stored once in `contents`, keyed by the first 16 bytes of its BLAKE3 hash and zstd-compressed when that saves space; parts only point to it. On the first production export (5,607 calls), 58.5 MB of message text referenced 7.0 MB of distinct text, stored in 3.8 MB. Nothing counts references while writing, so a system prompt in thousands of calls is never a write hotspot; contents no part points to are swept after reprocessing.

**Tool calls.** Each tool call the model makes is linked to the first later request of the same operation and session that carries its result, preferring one that resent the call with the same arguments (compared as canonical JSON). Apps resend arguments re-serialized and with defaults filled in, so an id only one tool call uses links to the nearest result regardless; an id several use (`functions.lookup:0` repeats across conversations, Gemini calls without id match by tool name) only links to results that resent the same arguments or none. Recorded per link: whether it errored, its size, and the gap between the call that requested it and the one that carried the result.

## The canonical record## The canonical record

HTTP, SSE and WebSocket share one shape: an exchange is an ordered list of timestamped frames, plus the context that isn't on the wire.

```ts
type Exchange = {
    id: string;                          // idempotency key, with `segment`
    segment?: number;                    // WebSocket sessions are sent in parts: 0, 1, 2, …
    final?: boolean;                     // false on every part but the last (default true)
    service: string;                     // "my-service"; one database per environment
    operation: string;                   // "listing.parse"
    session_id?: string;                 // groups one conversation
    metadata?: Record<string, string>;   // { listing_id: "123" }
    transport: "http" | "sse" | "ws";
    url: string;                         // query string is dropped
    started_at: string;
    status?: number;                     // HTTP status; 101 once a WebSocket is open
    frames: { t_ms: number; dir: "out" | "in"; data: string }[];
    end?: {                              // present exactly on the final record
        at_ms: number;
        reason: "complete" | "error" | "cancelled" | "closed";
        error?: string;
        close_code?: number;             // WebSocket only
    };
    truncated?: boolean;                 // the client clipped or cleaned something
};
```

| Transport | Frames |
|---|---|
| `http` | `out` request body at 0, then one `in` frame with the response body |
| `sse` | `out` request body, then one `in` frame per event, stamped when it arrived |
| `ws` | one frame per message in each direction |

Per-frame times replace a single TTFT number. Later processing derives:
- the time to the first token, read from the first event that actually carries content (not a keep-alive);
- output speed;
- stalls.

## TypeScript

```ts
import { createDatabase } from "@traumfaenger/client";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";

export const database = createDatabase({
    ingestUrl: "http://traumfaenger:7600",
    token: process.env.TRAUMFAENGER_INGEST_TOKEN,
    service: "my-service",
});

const openrouter = createOpenAICompatible({
    name: "openrouter",
    baseURL: "https://openrouter.ai/api/v1",
    apiKey,
    fetch: database.fetch(),        // or database.fetch(existingFetchWrapper)
    includeUsage: true,             // streams then carry usage in their last event
});

await database.operation({ operation: "listing.parse", metadata: { listing_id: id } }, () =>
    generateText({ model: openrouter(modelId), prompt }),
);

// Provider WebSockets: wrap right after creating them.
const realtime = database.operation({ operation: "voice.interview", sessionId }, () =>
    database.socket(new WebSocket(url, { headers })),
);
```

**Context:** `database.operation` keeps the context in `AsyncLocalStorage`, so every exchange inside it is tagged, including every step of an agent loop. Nested operations override `operation` and merge `metadata`. Exchanges outside any operation are `unattributed`.

**Response metadata:** an HTTP or SSE exchange also carries `response.headers_ms` (when the response headers arrived, in ms since the request went out) and the provider's request-id and rate-limit headers as `response.header.<name>` (`request-id`, `x-request-id`, `x-generation-id`, `x-provider-name`, `cf-ray`, `x-ratelimit-*`, `ratelimit*`, `anthropic-ratelimit-*`, `retry-after*`). They only fill the room the caller's own metadata leaves within the entry limit.

**Recording never changes or delays the call:**
- The request goes out first.
- Non-streaming responses are read from a `clone()` in the background.
- Streams are passed through chunk by chunk without a tee, so a caller that cancels still cancels the provider request. The cancel is recorded as `end.reason: "cancelled"`.
- `database.socket` wraps `send` and listens to `message` and `close`. It works with the standard `WebSocket`, Bun and the `ws` package.

**Size and content handling:**
- Binary messages and responses (audio, images) become a placeholder with their type and size.
- Inline base64 inside text is replaced: `data:` URLs, and JSON strings that are at least 1 KiB of pure base64, such as realtime audio chunks.
- Frames are clipped at 4 MiB, a record at 8 MiB and 10,000 frames. When a stream is longer, its middle is replaced by one `[traumfaenger: omitted N events, M bytes]` frame; the start and the end, where usage and the finish reason arrive, are kept.
- Context is cleaned instead of failing the record: the URL loses its query string, invalid metadata keys are dropped and broken characters are replaced. Any such change sets `truncated`.

**WebSocket sessions** are sent while they run, as segments under the same `id`: every 10 s (`segmentIntervalMs`), after 500 frames (`maxSegmentFrames`), or before a segment would exceed 8 MiB. The last segment has `final: true` and `end`. A crash therefore loses at most the last few seconds. Recording stops after 24 h (`end.reason: "error"`); the socket itself is left alone.

**Sending:** exchanges go over one ingestion WebSocket that the client opens on demand.
- **Encoding:** MessagePack by default; set `encoding: "json"` for JSON.
- **Batching:** batches are sent every 2 s, or earlier at 4 MiB or 500 exchanges, with up to 4 batches in flight.
- **Acks:** they come back in order. When the connection drops, unacknowledged batches are resent after reconnecting, with exponential backoff (1 s to 60 s). The same happens when a connection attempt takes longer than 10 s (`connectTimeoutMs`), a batch gets no ack within 30 s (`ackTimeoutMs`), or the server reports an `internal` error. Resending is safe because `(id, segment)` is the idempotency key.
- **Queue limit:** beyond 64 MiB queued, the oldest exchanges are dropped.
- **Errors:** rejected records are reported to `onError`, and `database.stats()` shows the counts. A throwing `onError` is ignored.
- **Idle and shutdown:** the connection closes after 30 s idle. `await database.close(timeoutMs)` sends the open part of recorded sockets, keeps sending until the deadline (default 5 s) and reports anything left. Exchanges recorded after `close()` are dropped and counted.

`QueryClient` reads the query API: `list`, `iterate`, `getExchange` (all segments joined) and `summary`.

**Runtime requirements:**
- The packages ship TypeScript sources, so consumers need a TS-aware runtime or bundler (Bun, Vite, Astro, tsx).
- The recorder runs server-side only: it uses `node:async_hooks`.
- The ingestion socket needs a `WebSocket` that accepts headers, which Bun and Node ≥ 22 provide. Any other implementation can be plugged in with `socketFactory`.

## Rust

```rust
use traumfaenger_client::{Database, DatabaseOptions, Operation, operation};

let database = Database::new(DatabaseOptions::new("http://traumfaenger:7600", "my-service").token(token))?;
```

`Database` is cheap to clone and must be created inside a tokio runtime. It sends over the same ingestion WebSocket with the same batching, acks, backoff, timeouts and deadline-bound `close()` as the TypeScript client.

- **Context:** `operation(Operation::new("listing.parse").session(id).meta("listing_id", 123), future)` tags everything recorded inside `future`. It lives in a tokio task-local, so it follows the awaited code of one task; spawned tasks need their own `operation` (or `current()` + `scope()`).
- **Any transport:** `database.record_http(url)` returns an `HttpRecording`: feed it the request body, the status and either the whole body or the stream's chunks (SSE events are split and stamped as they complete), then `finish`. A recording dropped unfinished records itself as `cancelled`. `database.record_socket(url)` returns a `SocketRecording` that sends a WebSocket session in segments.
- **Query:** `QueryClient::new(url, token)` with `list`, `get_exchange` and `summary`.

### rig

```rust
use rig_core::providers::openai::OpenAIConfig;
use traumfaenger_rig::Recorded;

let openai = OpenAIConfig::from_env()?.connect(Recorded::new(rig_reqwest::ReqwestClient::default(), database.clone()));
let reply = operation(Operation::new("listing.parse"), openai.chat("gpt-5-mini").call("Hi")).await?;
```

`Recorded<C>` implements rig's `HttpClientExt` (unary, multipart and streaming requests) and `WebSocketClientExt`, so it fits any rig provider configuration (`connect(...)` or `with_http(...)`) and realtime sessions. Non-success replies are recorded with their status and body. rig stops reading a stream after `data: [DONE]`, so streamed replies end as `cancelled` with that reason; the events, including usage, are all there.

## Ingestion protocol

Connect to `GET /v1/ingest` with `Authorization: Bearer <ingest token>` and offer one or both subprotocols. The server picks MessagePack when it's offered.
- `traumfaenger.v1.msgpack`: binary messages; maps with field names.
- `traumfaenger.v1.json`: text messages.

```jsonc
// client → server
{ "type": "batch", "id": 7, "exchanges": [ /* 1–500 Exchange */ ] }
// server → client, in batch order
{ "type": "ack", "batch": 7, "accepted": 3, "duplicates": 0, "rejected": [{ "index": 1, "reason": "…" }] }
{ "type": "error", "batch": 8, "error": "invalid_request", "message": "a batch must contain 1 to 500 exchanges" }
```

- Each record is decoded and validated on its own; invalid records are listed in `rejected` and the rest are stored. Unknown fields are ignored, so newer clients work with older servers.
- An `error` with code `internal` means the batch wasn't stored and can be resent. Other error codes mean resending won't help.
- A batch without an ack wasn't stored.
- Batches are at most 32 MiB. At most 4 are processed at once across all connections; the rest wait.
- `POST /v1/exchanges` takes `{ "exchanges": [...] }` as JSON (optionally gzip) and answers like an ack. It's meant for scripts and other languages.

## Query API

All responses are JSON and need `Authorization: Bearer $TRAUMFAENGER_QUERY_TOKEN`. Errors are `{ "error": ErrorCode, "message": string }` with status 400, 401, 404, 405, 413 or 500.

### Analytics

`GET /v1/analytics/{source}` returns `{ source, rows, truncated }`: one flat object per group and time bucket. Metrics and dimensions are a fixed vocabulary (the error message of an unknown name lists the known ones); values are only ever bound as SQL parameters.

| Parameter | |
|---|---|
| `metrics` | required, comma-separated |
| `group_by` | comma-separated dimensions; `metadata.<key>` groups by a metadata value |
| `interval` | bucket width such as `30s`, `5m`, `1h`, `1d`; adds a `time` column (RFC 3339). Grafana's `$__interval` works as is |
| `from`, `to` | RFC 3339 or epoch milliseconds (Grafana's `${__from}`, `${__to}`), on the call start |
| `order` | a requested metric, sorted descending (default: `time`, else the first metric) |
| `limit` | 1–10000, default 1000 |
| any dimension | a filter: `model=a,b`, `metadata.user=42`; `*` accepts every value (a dashboard variable set to "All") |

| Source | Dimensions | Metrics |
|---|---|---|
| `calls` | `call_id`, `service`, `operation`, `session_id`, `gateway`, `provider`, `model`, `model_requested`, `api`, `outcome`, `finish_reason`, `transport`, `stream`, `turn_start`, `status`, `cost_source`, `parse_status`, `parse_reason`, `metadata.<key>` | `calls`, `failed_calls`, `error_rate`, `rate_limited_calls`, `sessions`, `turns`, `agent_turns`, `input_tokens`, `cached_tokens`, `cache_write_tokens`, `uncached_input_tokens`, `output_tokens`, `reasoning_tokens`, `total_tokens`, `cache_hit_rate`, `cost_usd`, `input_cost_usd`, `output_cost_usd`, `avg_cost_usd`, `unpriced_calls`, `latency_ms_{avg,p50,p95,p99,max}`, `ttft_ms_{avg,p50,p95,p99}`, `output_tokens_per_second`, `tool_calls`, `request_bytes`, `response_bytes`, `first_call`, `last_call` |
| `tools` | `tool`, `call_id`, `result_call_id`, `service`, `operation`, `session_id`, `model`, `provider`, `result` (`ok`, `error`, `unanswered`) | `tool_calls`, `answered`, `failed`, `error_rate`, `gap_ms_{p50,p95,max}`, `arguments_bytes`, `result_bytes`, `sessions` |
| `parts` | `side`, `role`, `kind`, `tool`, `call_id`, `service`, `operation`, `session_id`, `gateway`, `provider`, `model`, `outcome`, `metadata.<key>` | `parts`, `bytes`, `distinct_contents`, `allocated_cost_usd`, `allocated_tokens`, `calls` |

`allocated_cost_usd` spreads each call's input cost over its request parts by size, and its output cost over its response parts. It answers what system prompts, tool definitions, history and each tool's results cost. It is an estimate: providers bill per call.

```sh
curl -H "Authorization: Bearer $T" 'http://traumfaenger:7601/v1/analytics/calls?metrics=cost_usd,calls,cache_hit_rate&group_by=model,provider'
curl -H "Authorization: Bearer $T" 'http://traumfaenger:7601/v1/analytics/calls?metrics=input_tokens,cached_tokens,output_tokens&interval=1h&operation=listing-parse'
curl -H "Authorization: Bearer $T" 'http://traumfaenger:7601/v1/analytics/parts?metrics=allocated_cost_usd&group_by=kind,tool&side=request'
```

**Grafana:** add an Infinity data source with the query URL and *Bearer token* auth, then in each panel use type JSON, the URL `/v1/analytics/calls?metrics=…&interval=$__interval&from=${__from}&to=${__to}`, and root selector `rows`.

### Details

- **`GET /v1/calls/{id}`**: one processed call with every message part. Texts are clipped to `max_chars` (default 2000); the full text is at `GET /v1/contents/{hash}`.
- **`GET /v1/sessions/{id}`**: a session's calls in order. By default each call lists only the parts it added to the conversation, which reads as the trajectory; `new_only=false` lists all.
- **`GET /v1/processing`**: parser version, processed and latest `seq`, stored contents and their size, and parse coverage per API, status and reason.

### Raw log

- **`GET /v1/exchanges`** returns raw log rows, one per record or segment, without frames, most recently stored first.
  - Filters: `service`, `operation`, `session_id`, `transport`, `failed`, and `from` and `to` (on `started_at`).
  - Paging: `limit` and `cursor`.
- **`GET /v1/exchanges/{id}`** returns one exchange with all its segments joined: frames in order, `segments`, and `complete` (the final segment is stored and nothing is missing).
- **`GET /v1/summary`** takes the same filters plus `group_by`, a comma-separated list of `service`, `operation`, `session_id`, `transport`, `status`, `day` and `hour`.
  - Each row has exchanges, failed, records, frames, bytes and duration avg/p95/p99.

In hand-written URLs, write timestamps with `Z`, or encode `+` as `%2B`.

## Run

```sh
TRAUMFAENGER_INGEST_TOKEN=$(openssl rand -hex 32) \
TRAUMFAENGER_QUERY_TOKEN=$(openssl rand -hex 32) \
cargo run --release -p traumfaenger-server
```

Docker: `docker build -t traumfaenger .`. The image stores data in the `/data` volume and listens on `0.0.0.0:7600` and `0.0.0.0:7601`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `TRAUMFAENGER_DATABASE` | `traumfaenger.duckdb` | DuckDB file, or `:memory:` |
| `TRAUMFAENGER_INGEST_ADDR` | `127.0.0.1:7600` | Ingestion listener |
| `TRAUMFAENGER_QUERY_ADDR` | `127.0.0.1:7601` | Query listener |
| `TRAUMFAENGER_INGEST_TOKEN` | required | Bearer token of the ingestion API, at least 16 characters |
| `TRAUMFAENGER_QUERY_TOKEN` | required | Bearer token of the query API; must differ from the ingestion token |
| `TRAUMFAENGER_AUTH` | `required` | Set `disabled` to run without tokens, for local development only |
| `TRAUMFAENGER_RETENTION_DAYS` | unset (keep) | Delete raw records this many days after receipt, but only once they are processed. Processed calls and contents stay |
| `TRAUMFAENGER_MEMORY_LIMIT` | DuckDB default | DuckDB `memory_limit`, e.g. `512MB` |
| `RUST_LOG` | `info` | Log filter |

**Operating notes:**
- Run a single instance per database file; DuckDB allows only one writing process.
- The server never downloads DuckDB extensions, so it runs offline.
- `GET /health` is unauthenticated.
- The raw log contains full prompts and answers, which can include personal data. Keep the query listener on a private network.

## Changing the contract

1. Edit `crates/api/src/lib.rs`.
2. Run `bun run gen` to rewrite `packages/api/src/generated.ts`. `cargo test` fails while the two differ.
3. Schema changes are appended to `MIGRATIONS` in `crates/server/src/store.rs`; never edit a released entry.

## Working on processing

Export a database, then iterate on the parser against it:

```sh
# Export: every exchange as JSON Lines (keep it out of any repository; it holds real prompts).
#   for id in GET /v1/exchanges?limit=1000 (follow next_cursor): GET /v1/exchanges/{id}
cargo run --release -p traumfaenger-process --bin traumfaenger-replay -- export.jsonl.gz --show outcome
cargo run --release -p traumfaenger-server -- import export.jsonl.gz local.duckdb   # raw log + processing
TRAUMFAENGER_DATABASE=local.duckdb TRAUMFAENGER_AUTH=disabled cargo run --release -p traumfaenger-server
```

The replay report shows parse coverage by API, status and reason, how often each field is filled, contents and their deduplication, tool call linking, and cost per operation. `import` loads and processes 5,607 calls in a few seconds.

Changing what processing produces means bumping `PARSER_VERSION` in `crates/process/src/lib.rs`; the server then reprocesses the raw log on start.

## Develop

```sh
cargo test                                          # contract, store, decoding, WebSocket/HTTP, Rust client and rig tests
bun install && bun run typecheck && bun run test    # recorder + AI SDK + WebSocket against a mock provider and the real server
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```
