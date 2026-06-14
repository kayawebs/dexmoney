# syntax=docker/dockerfile:1

FROM rust:1.90-bookworm AS builder

ARG APP_BIN=searcher
ARG APP_PACKAGE=${APP_BIN}
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p ${APP_PACKAGE} --bin ${APP_BIN}

FROM debian:bookworm-slim

ARG APP_BIN=searcher
ARG APP_PACKAGE=${APP_BIN}
ENV APP_BIN=${APP_BIN}
ENV APP_PACKAGE=${APP_PACKAGE}
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/${APP_BIN} /usr/local/bin/app
COPY migrations ./migrations
COPY .env.example ./.env.example

CMD ["app"]
