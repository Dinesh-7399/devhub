# ==============================================================================
#                 DEVHUB MULTI-STAGE HIGH-PERFORMANCE DOCKERFILE
# ==============================================================================

# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install root certs
RUN apk add --no-cache ca-certificates git

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy full source tree
COPY . .

# Build statically-linked zero-dependency binary with stripped debug tables
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/devhub ./cmd/main.go

# Runtime Stage (Minimal Alpine ~15MB)
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata curl

WORKDIR /workspace

# Copy compiled binary
COPY --from=builder /app/devhub /usr/local/bin/devhub

# Ingress Doorway (:4000) & Developer Cockpit (:4040)
EXPOSE 4000 4040

# Healthcheck
HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:4040/api/traces || exit 1

ENTRYPOINT ["devhub"]
CMD ["--port", "4000", "--dashboard", "4040"]
