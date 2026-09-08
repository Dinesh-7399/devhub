# ⚡ DevHub v2.6 Pro
### High-Performance Microservice Switchboard, Zero-Config Mesh & Developer Cockpit

DevHub is an industrial-grade developer platform that combines the capabilities of **Ngrok, Traefik, Postman, and OpenTelemetry** into a single zero-dependency binary for local and containerized microservice development.

```
  ┌────────────────────────────────────────────────────────────────────────┐
  │  ██████╗ ███████╗██╗   ██╗██╗  ██╗██╗   ██╗██████╗                     │
  │  ██╔══██╗██╔════╝██║   ██║██║  ██║██║   ██║██╔══██╗   DEVHUB v2.6 PRO  │
  │  ██║  ██║█████╗  ██║   ██║███████║██║   ██║██████╔╝   Mesh Switchboard │
  │  ██║  ██║██╔══╝  ╚██╗ ██╔╝██╔══██║██║   ██║██╔══██╗   & Live Debugger  │
  │  ██████╔╝███████╗ ╚████╔╝ ██║  ██║╚██████╔╝██████╔╝                    │
  │  ╚═════╝ ╚══════╝  ╚═══╝  ╚═╝  ╚═╝ ╚═════╝ ╚═════╝                     │
  └────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Core Features

- **⚡ Zero-Allocation L7 Reverse Proxy:** Segment-aware Longest-Prefix Matching router using persistent path-copying with atomic pointer swap (`atomic.Pointer[TrieNode]`), benchmarking at **54M+ ops/s** with $0\text{ B/op}$ and $0\text{ allocs/op}$ on lookups, supporting dynamic concurrent `Add()` and `Remove()`.
- **🔀 Dual Ingress & Streaming Forward Proxy:** Serves as an Ingress Gateway (`:4000`) and an Egress Forward Proxy (`HTTP_PROXY=http://localhost:4000`), enabling zero-code downstream cascading (`Service A -> Service B`). Features bounded 64KB tee-capture with per-chunk `http.Flusher` (zero memory bloat for multi-GB payloads, instant LLM/SSE tokens) and isolated `Proxy: nil` transport loops.
- **🔒 Hardened Developer Cockpit:** Bound securely to `127.0.0.1` by default with strict WebSocket origin verification, optional token authentication (`dashboard_auth_token`), 1-click replay composer, and live cURL export.
- **🐳 Cross-Platform Docker Auto-Discovery:** Native Windows Named Pipe (`\\.\pipe\docker_engine`) and Unix Domain Socket (`/var/run/docker.sock`) clients with deterministic label-based routing and automatic route teardown on container shutdown.
- **📜 8-Byte Stream Demuxer:** High-performance binary demultiplexer for Docker's multiplexed container logs (`stdout` vs `stderr`).
- **🪝 Webhook HMAC Re-Signer & Bypass:** Automatically recalculates cryptographic HMAC signatures (`Stripe-Signature`, `X-Hub-Signature-256`, `X-Razorpay-Signature`, `X-Shopify-Hmac-Sha256`, `X-Slack-Signature`) when payloads are edited in the replay composer.
- **🎭 Mock Mode Fallback:** Instant 200 OK mock response interceptor (`always` or `on_error`) to unblock frontend developers when downstream microservices are down or in development.
- **💓 Passive Health Checks & Circuit Breaker:** Passive observation mode by default for resilient local developer flow, with an opt-in active circuit breaker (`enforce_circuit_breaker`) and TCP fallback.
- **⚡ Scheme-Aware WebSocket & WSS Tunneling:** Full-duplex connection hijacking with scheme-aware dialing (`ws://` and `wss://` over TLS) and full connection lifecycle telemetry.
- **🕵️ Recursive PII Redactor & Failure Observability:** AST-level JSON parsing and form-urlencoded scrubbing for credentials and credit cards, alongside guaranteed capture of 404, 502, and 503 gateway events.

---

## 🏎️ Performance Benchmarks

Executed on Go 1.24+ (`windows/amd64`, 12th Gen Intel Core i5-1240P, 16 threads):

| Subsystem | Benchmark Test | Throughput | Latency | Memory / Op | Allocations |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Path-Copying Trie** | `BenchmarkRouter_Match-16` | **54.64 Million ops/s** | **20.79 ns** | **0 B/op** | **0 allocs/op** |
| **Ring Buffer** | `BenchmarkRingBuffer_Push-16` | **10.25 Million ops/s** | **117.3 ns** | **0 B/op** | **0 allocs/op** |
| **Full Reverse Proxy** | `BenchmarkEngine_ServeHTTP-16` | **5,898 req/s** | **0.20 ms** | *Pooled Stream* | *Tuned Pool* |

---

## 📦 Quickstart

### 1. Run via Go CLI
```bash
# Build & Install globally
go install ./cmd/main.go

# Start DevHub daemon
devhub
```

### 2. CLI Options
```bash
devhub --port 4000 --dashboard 4040 --config devhub.yaml --tunnel
```

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--port <int>` | `4000` | Ingress Proxy & Egress Forward Proxy Port |
| `--dashboard <int>` | `4040` | Developer Cockpit Web UI Port |
| `--config <path>` | auto-detect | Path to `.devhub.yaml` or `devhub.yaml` |
| `--tunnel` | `false` | Enable public ingress tunnel bridge |
| `--no-docker` | `false` | Disable Docker daemon auto-discovery |

---

## 🐳 Docker Deployment

### Run Standalone Container
```bash
docker build -t devhub:latest .

docker run -d --name devhub \
  -p 4000:4000 -p 4040:4040 \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  devhub:latest
```

### Run via Docker Compose
```yaml
# docker-compose.yml
services:
  devhub:
    image: devhub:latest
    build: .
    container_name: devhub-mesh
    ports:
      - "4000:4000"
      - "4040:4040"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - .:/workspace:ro
    restart: unless-stopped
```

---

## 🛠️ Configuration (`.devhub.yaml`)

```yaml
version: "1.0"

ingress:
  port: 4000
  dashboard: 4040

docker:
  enabled: true
  mode: "strict" # "strict" (requires devhub.route), "opt_in" (devhub.enable=true), or "automatic"
  poll_logs: true

egress:
  enabled: true
  mode: "allow_all_except_metadata" # "allow_all_except_metadata", "allow_all_except_private", or "strict"
  allowed_hosts:
    - "api.stripe.com"
    - "*.github.com"
  denied_cidrs:
    - "169.254.169.254/32"
    - "fd00:ec2::254/128"

tracing:
  enabled: true
  redact_pii: true
  ring_size: 500

routes:
  - prefix: "/auth"
    target: "http://localhost:50051"
    strip_prefix: false
    health_check_path: "/health"

  - prefix: "/api/v1"
    target: "http://localhost:8085"
    strip_prefix: true

overrides:
  /api/payment:
    target: "http://localhost:50067"
    strip_prefix: true
    mock:
      enabled: false
      mode: "on_error"
      status: 200
      body: '{"status":"paid","tx_id":"tx_mock_123"}'
```

---

## 🧪 Testing

```bash
# Run unit & integration test suite
go test -v -count=1 ./...

# Run micro-benchmarks
go test -bench . -benchmem ./...
```

---

## 📄 License
MIT License.
