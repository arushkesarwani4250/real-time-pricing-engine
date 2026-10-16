# ==========================================
# Stage 1: Builder
# ==========================================
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git and ca-certificates for downloading dependencies
RUN apk add --no-cache git ca-certificates

# Copy dependency manifests first to leverage Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Compile statically linked binary with stripped debug symbols (-s -w) for minimal size
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/pricing-engine ./cmd/server

# ==========================================
# Stage 2: Lightweight Runtime Image
# ==========================================
FROM alpine:3.20

WORKDIR /app

# Install ca-certificates and curl for healthchecks
RUN apk add --no-cache ca-certificates curl

# Create non-root system user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Create persistent data directory and assign permissions
RUN mkdir -p /app/data && chown -R appuser:appgroup /app

# Copy binary from builder
COPY --from=builder /app/pricing-engine /app/pricing-engine

# Switch to unprivileged user
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/pricing-engine"]
