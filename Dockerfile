FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p ai-ledger-server \
    && cp target/release/ai-ledger-server /ai-ledger-server

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /data ai-ledger \
    && mkdir -p /data && chown ai-ledger /data
COPY --from=build /ai-ledger-server /usr/local/bin/ai-ledger-server
USER ai-ledger
WORKDIR /data
ENV AI_LEDGER_DATABASE=/data/ai-ledger.duckdb \
    AI_LEDGER_INGEST_ADDR=0.0.0.0:7600 \
    AI_LEDGER_QUERY_ADDR=0.0.0.0:7601
VOLUME /data
EXPOSE 7600 7601
ENTRYPOINT ["/usr/local/bin/ai-ledger-server"]
