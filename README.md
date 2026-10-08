# ai-ledger

A small, standalone ledger of AI model calls. Every call (what it was for,
which object it worked on, model, tokens, billed cost, latency, outcome) is
one row, so you can trace a cost to the single operation that caused it and
aggregate it by any dimension.

- **Rust server**: one binary on Axum, with storage in an embedded DuckDB file. No UI.
- **Two separate HTTP APIs** on two listeners with two bearer tokens:
  - **ingestion** (write-only), port 7600: `POST /v1/calls`
  - **query** (read-only), port 7601: `GET /v1/calls`, `GET /v1/calls/{id}`, `GET /v1/summary`

  An emitter holding the ingestion token cannot read the ledger, and a reader cannot write to it.
- **End-to-end types**: the wire contract is defined once in Rust (`crates/api`)
  and translated to TypeScript with [ts-rs](https://github.com/Aleph-Alpha/ts-rs).
  - `@traumwohnung/ai-ledger-api` contains the generated types, `ROUTES` and `LIMITS`.
  - `@traumwohnung/ai-ledger-client` is a dependency-free `fetch` client built on them.

Nothing here is project-specific. `feature`, `entity_type` and `metadata` are free-form.

## Layout

```
crates/api/        wire contract (serde types, routes, limits) + TS generator
crates/server/     HTTP server and DuckDB store
packages/api/      generated TypeScript contract (src/generated.ts is committed)
packages/client/   TypeScript client
```

## Run

```sh
AI_LEDGER_INGEST_TOKEN=$(openssl rand -hex 32) \
AI_LEDGER_QUERY_TOKEN=$(openssl rand -hex 32) \
cargo run --release -p ai-ledger-server
```

Or with Docker. The image stores data in the `/data` volume and listens on `0.0.0.0`:

```sh
docker build -t ai-ledger .
docker run -v ai-ledger:/data -p 127.0.0.1:7600:7600 -p 127.0.0.1:7601:7601 \
  -e AI_LEDGER_INGEST_TOKEN=... -e AI_LEDGER_QUERY_TOKEN=... ai-ledger
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `AI_LEDGER_DATABASE` | `ai-ledger.duckdb` | DuckDB file, or `:memory:` |
| `AI_LEDGER_INGEST_ADDR` | `127.0.0.1:7600` | Ingestion listener |
| `AI_LEDGER_QUERY_ADDR` | `127.0.0.1:7601` | Query listener |
| `AI_LEDGER_INGEST_TOKEN` | required | Bearer token of the ingestion API, at least 16 characters |
| `AI_LEDGER_QUERY_TOKEN` | required | Bearer token of the query API; must differ from the ingestion token |
| `AI_LEDGER_AUTH` | `required` | Set `disabled` to run without tokens, for local development only |
| `AI_LEDGER_MEMORY_LIMIT` | DuckDB default | DuckDB `memory_limit`, e.g. `512MB` |
| `RUST_LOG` | `info` | Log filter |

DuckDB allows one writing process per file, so run a single instance per
database. The server never downloads DuckDB extensions, so it runs offline.
Ingestion and queries use separate connections, so a slow summary does not
block ingestion. `GET /health` is unauthenticated on both listeners. Expose the query
listener only on a private network.

## API

All bodies are JSON. Errors are `{ "error": ErrorCode, "message": string }` with
status 400, 401, 404, 405, 413 or 500. `crates/api/src/lib.rs` documents every field.

### Ingestion: `POST /v1/calls`

```json
{
  "calls": [
    {
      "id": "0192f3c4-…",
      "occurred_at": "2026-10-08T10:00:00Z",
      "feature": "listing.parse",
      "entity_type": "listing",
      "entity_id": "123",
      "trace_id": "parse-123-2026-10-08",
      "provider": "openrouter",
      "model": "openai/gpt-5-mini",
      "reasoning_effort": "low",
      "status": "ok",
      "attempt": 1,
      "input_tokens": 1200,
      "cached_input_tokens": 200,
      "output_tokens": 300,
      "reasoning_tokens": 120,
      "cost_usd": 0.0042,
      "latency_ms": 850,
      "metadata": { "prompt_version": 3 }
    }
  ]
}
```

Required fields are `id`, `occurred_at`, `feature`, `provider`, `model` and `status`.

- **Idempotency**: `id` is the idempotency key. Retries are safe. A known id is counted under `duplicates` and the first write wins. The response is `{ "accepted": n, "duplicates": n }`.
- **Atomic batches**: a batch holds up to 1000 calls in at most 8 MiB, and one invalid call rejects the whole batch.
- **Cost**: `cost_usd` is the cost the provider billed (for OpenRouter, `usage.cost`). Omit it when it is unknown and never estimate it. Summaries report such calls as `calls_without_cost`.

### Query

- **`GET /v1/calls`** returns calls newest first.
  - Filters: `feature`, `entity_type`, `entity_id`, `trace_id`, `provider`, `model`, `status`, `from` (inclusive), `to` (exclusive).
  - Paging: `limit` (1–1000, default 100) and `cursor`. The response is `{ calls, next_cursor }`.
  - Timestamps are RFC 3339. In a hand-written URL use `Z` or encode `+` as `%2B`; a raw `+` becomes a space.
- **`GET /v1/calls/{id}`** returns one call, or 404.
- **`GET /v1/summary`** returns token, cost, error and latency totals (avg/p95). It takes the same filters plus:
  - `group_by`, a comma-separated list of `feature`, `entity_type`, `entity_id`, `trace_id`, `provider`, `model`, `reasoning_effort`, `status`, `day` and `hour` (UTC);
  - `limit` (default 1000).

  Rows come highest cost first, and `truncated` says whether more groups exist.

For example, "What did listing 123's parse cost, and why were there three attempts?":

```sh
curl -H "Authorization: Bearer $QUERY_TOKEN" \
  'localhost:7601/v1/calls?feature=listing.parse&entity_type=listing&entity_id=123'
curl -H "Authorization: Bearer $QUERY_TOKEN" \
  'localhost:7601/v1/summary?group_by=feature,day&from=2026-10-01T00:00:00Z'
```

## TypeScript

```ts
import { IngestClient, QueryClient } from "@traumwohnung/ai-ledger-client";

const ledger = new IngestClient({ baseUrl: "http://ai-ledger:7600", token: process.env.AI_LEDGER_INGEST_TOKEN });
await ledger.ingest([{ id: crypto.randomUUID(), occurred_at: new Date().toISOString(), feature: "listing.parse",
    provider: "openrouter", model, status: "ok", input_tokens, output_tokens, cost_usd: usage.cost }]);

const reader = new QueryClient({ baseUrl: "http://ai-ledger:7601", token: process.env.AI_LEDGER_QUERY_TOKEN });
const { rows } = await reader.summary({ group_by: ["feature", "model"], from: "2026-10-01T00:00:00Z" });
for await (const call of reader.iterateCalls({ entity_id: "123" })) console.log(call.cost_usd);
```

`ingest()` splits inputs into sequential batches that fit both the 1000-call and the 8 MiB limit. The
client throws `AiLedgerError` with `status` and a typed `code`. Network
failures have `code: "network"` and `status: 0`. The packages ship TypeScript
sources (`exports` points at `src/index.ts`), so consumers need a TS-aware
runtime or bundler (Bun, Vite, Astro, tsx).

## Changing the contract

1. Edit the types in `crates/api/src/lib.rs`, and validation in `NewAiCall::validate`.
2. Run `bun run gen`, which runs `cargo run -p ai-ledger-api --bin gen-ts`, to rewrite `packages/api/src/generated.ts`.
3. Schema changes are new entries appended to `MIGRATIONS` in `crates/server/src/store.rs`. Never edit a released one.

`cargo test` fails while the committed TypeScript differs from the Rust types.

## Develop

```sh
cargo test                           # contract, store and HTTP tests
bun install && bun run typecheck && bun run test   # TS client against the real server
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```
