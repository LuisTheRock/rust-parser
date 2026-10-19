# syntax=docker/dockerfile:1

ARG APP_NAME=service-mqtt-ccpm-parser-v2-rs

FROM rust:1.98-bookworm AS builder
ARG APP_NAME
WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      pkg-config \
      cmake \
      build-essential \
      protobuf-compiler \
      libssl-dev \
      libsasl2-dev \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release \
 && install -Dm755 "target/release/${APP_NAME}" /out/server

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
WORKDIR /app

COPY --from=builder /out/server .
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libssl.so.3 /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcrypto.so.3 /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libsasl2.so.2 /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/sasl2/ /usr/lib/x86_64-linux-gnu/sasl2/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libz.so.1 /usr/lib/x86_64-linux-gnu/

USER nonroot:nonroot

EXPOSE 3000

CMD ["./server"]
