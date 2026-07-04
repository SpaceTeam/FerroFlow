# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

WORKDIR /usr/src/ferroflow

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --locked --release --bin ferro_flow

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /app ferroflow

WORKDIR /app

COPY --from=builder /usr/src/ferroflow/target/release/ferro_flow /usr/local/bin/ferro_flow

USER ferroflow

CMD ["ferro_flow"]
