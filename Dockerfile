# SPDX-FileCopyrightText: 2025 The superseedr Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

# syntax=docker/dockerfile:1

# --- Stage 1: The Cross-Builder ---
FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder

ARG TARGETPLATFORM
ARG TARGETARCH
ARG BUILDPLATFORM
ARG PRIVATE_BUILD=false

# 1. Install 'xx' - The Cross-Compilation Helper
COPY --from=tonistiigi/xx / /

# 2. Install Host Build Tools (running on Intel/AMD)
# 'pkg-config' here is the driver that xx-cargo will wrap.
RUN apt-get update && apt-get install -y clang lld pkg-config git

# 3. Install Target Libraries (ARM64/AMD64)
# [CRITICAL] Use 'xx-apt-get'. This installs libssl-dev for the TARGET architecture.
# We also install 'gcc' so the crate can run C-code tests during the build.
RUN xx-apt-get install -y libssl-dev gcc

WORKDIR /app

# 4. Copy source files
COPY Cargo.toml Cargo.lock ./
COPY ./src ./src
COPY ./fuzz/Cargo.toml ./fuzz/Cargo.toml
COPY ./fuzz/fuzz_targets ./fuzz/fuzz_targets

# 5. Fix for OpenSSL Cross-Compilation
# [CRITICAL FIX] The openssl-sys crate is paranoid. It detects cross-compilation
# and refuses to run pkg-config unless this variable is set.
# Since 'xx' is handling the paths, it is safe to force this to 1.
ENV PKG_CONFIG_ALLOW_CROSS=1

# 6. Build with xx-cargo
RUN --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/cache \
    --mount=type=cache,target=/usr/local/cargo/registry/index \
    --mount=type=cache,target=/app/target \
    TRIPLE=$(xx-cargo --print-target-triple) && \
    if [ "$PRIVATE_BUILD" = "true" ]; then \
        xx-cargo build --release --no-default-features --target "$TRIPLE" --target-dir ./target; \
    else \
        xx-cargo build --release --target "$TRIPLE" --target-dir ./target; \
    fi && \
    cp ./target/$TRIPLE/release/superseedr /app/superseedr

# --- Stage 2: The Final Image ---
FROM debian:bookworm-slim AS final

# Install runtime dependencies (OpenSSL 3 runtime)
RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/superseedr /usr/local/bin/superseedr

ENTRYPOINT ["/usr/local/bin/superseedr"]
