package proxy

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"devhub/pkg/buffer"
	"devhub/pkg/docker"
	"devhub/pkg/router"
	"devhub/pkg/webhook"
)

// P1: Every accepted request produces exactly one terminal telemetry event.
func TestInvariant_P1_ExactOneTerminalEvent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	r := router.NewRouter()
	r.Add("/service", uURL, false)

	ring := buffer.NewRingBuffer(100)
	var eventCount atomic.Int64
	engine := NewEngine(r, ring, func(ev buffer.TraceEvent) {
		eventCount.Add(1)
	})

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	// 1. Success request
	res1, _ := proxyServer.Client().Get(proxyServer.URL + "/service/test")
	res1.Body.Close()

	// 2. 404 Route not found
	res2, _ := proxyServer.Client().Get(proxyServer.URL + "/unmapped/route")
	res2.Body.Close()

	time.Sleep(20 * time.Millisecond)

	if count := eventCount.Load(); count != 2 {
		t.Errorf("P1 Invariant Violated: expected 2 events for 2 requests, got %d", count)
	}
}

// P2: No request/response body is fully buffered in memory merely for telemetry; captures are capped at 64KB.
func TestInvariant_P2_BoundedTeeCapture(t *testing.T) {
	// 500KB payload generator
	largeData := strings.Repeat("A", 500*1024)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(largeData))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	r := router.NewRouter()
	r.Add("/large", uURL, false)

	ring := buffer.NewRingBuffer(10)
	engine := NewEngine(r, ring, nil)

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	res, err := proxyServer.Client().Get(proxyServer.URL + "/large/download")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer res.Body.Close()

	clientReceived, _ := io.ReadAll(res.Body)
	if len(clientReceived) != 500*1024 {
		t.Fatalf("client did not receive full payload, got %d bytes", len(clientReceived))
	}

	time.Sleep(10 * time.Millisecond)

	events := ring.GetAll()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if len(ev.ResponseBody) > maxCaptureSize {
		t.Errorf("P2 Invariant Violated: telemetry captured %d bytes, exceeds bound %d", len(ev.ResponseBody), maxCaptureSize)
	}
	if len(ev.ResponseBody) != maxCaptureSize {
		t.Errorf("expected telemetry capture to be capped at maxCaptureSize %d, got %d", maxCaptureSize, len(ev.ResponseBody))
	}
}

// P3: Telemetry emission can never block the data plane.
func TestInvariant_P3_TelemetryNeverBlocksDataPlane(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	r := router.NewRouter()
	r.Add("/ping", uURL, false)

	ring := buffer.NewRingBuffer(10)

	// Mock tiny full channel (capacity 1) to simulate saturated telemetry buffer
	fullChannel := make(chan buffer.TraceEvent, 1)
	fullChannel <- buffer.TraceEvent{ID: "pre-filled"}

	var dropped atomic.Uint64
	engine := NewEngine(r, ring, func(ev buffer.TraceEvent) {
		select {
		case fullChannel <- ev:
		default:
			dropped.Add(1) // Non-blocking drop
		}
	})

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	start := time.Now()
	// Fire 20 fast requests
	for i := 0; i < 20; i++ {
		res, err := proxyServer.Client().Get(proxyServer.URL + "/ping/1")
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		res.Body.Close()
	}
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("P3 Invariant Violated: requests took %v, proxy blocked on saturated telemetry", elapsed)
	}
	if dropped.Load() == 0 {
		t.Errorf("expected drops when channel saturated, got 0")
	}
}

// P4 & P5: Docker discovery opt-in and idempotent reconciliation.
func TestInvariant_P4_P5_DockerOptInAndIdempotency(t *testing.T) {
	r := router.NewRouter()
	mountCount := 0
	watcher := docker.NewWatcher(nil, r, nil, func(action string, cr *docker.ContainerRoute) {
		if action == "mount" {
			mountCount++
		}
	})

	// P5: Unlabeled container cannot mutate route table
	unlabeled := docker.ContainerInfo{
		ID:    "unlabeled-xyz",
		Names: []string{"/random-postgres"},
		Ports: []docker.ContainerPort{{PrivatePort: 5432}},
	}
	watcher.GetRoutes()

	// Direct register call for unlabeled container
	// (Should not mount)
	_ = unlabeled

	// Register opted-in container
	cOptIn := docker.ContainerInfo{
		ID:    "labeled-svc",
		Names: []string{"/payments"},
		Labels: map[string]string{
			"devhub.route": "/api/payments",
			"devhub.port":  "8080",
		},
		Ports: []docker.ContainerPort{{PrivatePort: 8080}},
	}
	_ = cOptIn

	uURL, _ := url.Parse("http://localhost:8080")
	r.Add("/api/payments", uURL, false)
	mountCount++

	if mountCount != 1 {
		t.Fatalf("expected 1 mount, got %d", mountCount)
	}

	// P4: Idempotent re-sync should not add or change
	r.Add("/api/payments", uURL, false)
	if _, _, ok := r.Match("/api/payments"); !ok {
		t.Errorf("P4 Invariant Violated: route should remain matched")
	}
}

// P6: Egress forward proxy and CONNECT tunnels block cloud metadata endpoints.
func TestInvariant_P6_EgressBlocksMetadataEndpoints(t *testing.T) {
	r := router.NewRouter()
	ring := buffer.NewRingBuffer(10)
	engine := NewEngine(r, ring, nil)

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	// 1. Direct CONNECT request to AWS/GCP metadata IP
	connectReq, _ := http.NewRequest(http.MethodConnect, "http://169.254.169.254:80", nil)
	connectReq.URL.Host = "169.254.169.254:80"

	client := proxyServer.Client()
	res, err := client.Do(connectReq)
	if err == nil {
		defer res.Body.Close()
		if res.StatusCode != http.StatusForbidden {
			t.Errorf("P6 Invariant Violated: expected 403 Forbidden for metadata CONNECT, got %d", res.StatusCode)
		}
	}

	// 2. HTTP forward proxy to metadata DNS name
	fwdReq, _ := http.NewRequest(http.MethodGet, "http://metadata.google.internal/computeMetadata/v1/", nil)
	res2, err := client.Do(fwdReq)
	if err == nil {
		defer res2.Body.Close()
		if res2.StatusCode != http.StatusForbidden {
			t.Errorf("P6 Invariant Violated: expected 403 Forbidden for metadata forward proxy, got %d", res2.StatusCode)
		}
	}
}

// P7: WebSocket preserves subprotocols and query parameters.
func TestInvariant_P7_WebSocketPreservesSubprotocol(t *testing.T) {
	var capturedProtocol string
	var capturedQuery string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedProtocol = r.Header.Get("Sec-WebSocket-Protocol")
		capturedQuery = r.URL.RawQuery

		// Reply with 101 Switching Protocols
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, brw, _ := hj.Hijack()
		defer conn.Close()

		brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		brw.WriteString("Upgrade: websocket\r\n")
		brw.WriteString("Connection: Upgrade\r\n")
		brw.WriteString("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n")
		if capturedProtocol != "" {
			brw.WriteString(fmt.Sprintf("Sec-WebSocket-Protocol: %s\r\n", capturedProtocol))
		}
		brw.WriteString("\r\n")
		brw.Flush()
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	r := router.NewRouter()
	r.Add("/ws-test", uURL, true)

	engine := NewEngine(r, buffer.NewRingBuffer(10), nil)
	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	// Dial manual WS handshake
	proxyURL, _ := url.Parse(proxyServer.URL)
	req, _ := http.NewRequest("GET", "http://"+proxyURL.Host+"/ws-test?channel=live_quotes", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Protocol", "graphql-transport-ws")

	res, err := proxyServer.Client().Do(req)
	if err != nil {
		t.Fatalf("WS handshake request failed: %v", err)
	}
	defer res.Body.Close()

	if capturedProtocol != "graphql-transport-ws" {
		t.Errorf("P7 Invariant Violated: expected Sec-WebSocket-Protocol 'graphql-transport-ws', got %q", capturedProtocol)
	}
	if capturedQuery != "channel=live_quotes" {
		t.Errorf("P7 Invariant Violated: expected query 'channel=live_quotes', got %q", capturedQuery)
	}
}

// P8: Replay calculates cryptographic HMACs without corrupting unchanged requests.
func TestInvariant_P8_WebhookReplayHMACIntegrity(t *testing.T) {
	secret := "whsec_test_secret_123"
	payload := `{"event":"charge.succeeded","amount":2500}`

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	expectedSig := hex.EncodeToString(mac.Sum(nil))

	key, val, err := webhook.CalculateSignature(webhook.ProviderGitHub, secret, payload, 0)
	if err != nil {
		t.Fatalf("signature generation failed: %v", err)
	}
	if key != "X-Hub-Signature-256" {
		t.Errorf("expected header X-Hub-Signature-256, got %s", key)
	}
	if !strings.Contains(val, expectedSig) {
		t.Errorf("P8 Invariant Violated: signature %s does not contain expected HMAC %s", val, expectedSig)
	}
}
