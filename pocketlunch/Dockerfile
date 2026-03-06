FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/pocketlunch /app/pocketlunch
COPY migrations /app/migrations

ENTRYPOINT ["/app/pocketlunch"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8080"]
