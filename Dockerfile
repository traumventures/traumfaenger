FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p traumfaenger-server \
    && cp target/release/traumfaenger-server /traumfaenger-server

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /data traumfaenger \
    && mkdir -p /data && chown traumfaenger /data
COPY --from=build /traumfaenger-server /usr/local/bin/traumfaenger-server
USER traumfaenger
WORKDIR /data
ENV TRAUMFAENGER_DATABASE=/data/traumfaenger.duckdb \
    TRAUMFAENGER_INGEST_ADDR=0.0.0.0:7600 \
    TRAUMFAENGER_QUERY_ADDR=0.0.0.0:7601
VOLUME /data
EXPOSE 7600 7601
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/traumfaenger-server", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/traumfaenger-server"]
