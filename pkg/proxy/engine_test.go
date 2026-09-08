package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"devhub/pkg/buffer"
	"devhub/pkg/mock"
	"devhub/pkg/router"
	"devhub/pkg/webhook"
)

func TestEngine_ServeHTTP_RoutingAndCapture(t *testing.T) {
	// Upstream API mock server
	var receivedUpstreamBody string
	var receivedUpstreamHeader http.Header
	var receivedUpstreamPath string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedUpstreamBody = string(b)
		receivedUpstreamHeader = r.Header.Clone()
		receivedUpstreamPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Custom-Response", "test-resp")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"status":"created","id":123}`))
	}))
	defer upstream.Close()

	upstreamURL, _ := url.Parse(upstream.URL)

	r := router.NewRouter()
	r.Add("/api/v1", upstreamURL, true) // strip prefix

	ring := buffer.NewRingBuffer(10)
	var broadcastEvents []buffer.TraceEvent
	var mu sync.Mutex

	engine := NewEngine(r, ring, func(ev buffer.TraceEvent) {
		mu.Lock()
		defer mu.Unlock()
		broadcastEvents = append(broadcastEvents, ev)
	})

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	// Perform request to proxy
	reqBody := `{"name":"alice","action":"checkout"}`
	req, err := http.NewRequest(http.MethodPost, proxyServer.URL+"/api/v1/orders/submit?urgent=true", strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevHub-Trace-ID", "trace-custom-999")

	client := proxyServer.Client()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	// Verify client response
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status %d, got %d", http.StatusCreated, resp.StatusCode)
	}
	if string(respBody) != `{"status":"created","id":123}` {
		t.Errorf("expected resp body, got %s", string(respBody))
	}

	// Verify upstream received the rewritten path and full request
	if receivedUpstreamPath != "/orders/submit" {
		t.Errorf("expected upstream path /orders/submit, got %s", receivedUpstreamPath)
	}
	if receivedUpstreamBody != reqBody {
		t.Errorf("expected upstream body %s, got %s", reqBody, receivedUpstreamBody)
	}
	if receivedUpstreamHeader.Get("X-DevHub-Trace-ID") == "" {
		t.Errorf("expected trace id header in upstream request")
	}

	// Verify ring buffer event
	for i := 0; i < 50; i++ {
		if ring.Count() > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	events := ring.GetAll()
	if len(events) != 1 {
		t.Fatalf("expected 1 event in ring buffer, got %d", len(events))
	}
	ev := events[0]
	if ev.Method != "POST" {
		t.Errorf("expected POST, got %s", ev.Method)
	}
	if ev.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201, got %d", ev.StatusCode)
	}
	var actualReq, expectedReq map[string]any
	if err := json.Unmarshal([]byte(ev.RequestBody), &actualReq); err != nil {
		t.Fatalf("failed to unmarshal actual RequestBody %s: %v", ev.RequestBody, err)
	}
	if err := json.Unmarshal([]byte(reqBody), &expectedReq); err != nil {
		t.Fatalf("failed to unmarshal expected RequestBody %s: %v", reqBody, err)
	}
	if !reflect.DeepEqual(actualReq, expectedReq) {
		t.Errorf("expected event reqBody %v, got %v", expectedReq, actualReq)
	}

	var actualResp, expectedResp map[string]any
	if err := json.Unmarshal([]byte(ev.ResponseBody), &actualResp); err != nil {
		t.Fatalf("failed to unmarshal actual ResponseBody %s: %v", ev.ResponseBody, err)
	}
	if err := json.Unmarshal([]byte(`{"status":"created","id":123}`), &expectedResp); err != nil {
		t.Fatalf("failed to unmarshal expected ResponseBody: %v", err)
	}
	if !reflect.DeepEqual(actualResp, expectedResp) {
		t.Errorf("expected event respBody %v, got %v", expectedResp, actualResp)
	}

	// Verify broadcast
	mu.Lock()
	if len(broadcastEvents) != 1 {
		t.Errorf("expected 1 broadcast event, got %d", len(broadcastEvents))
	}
	mu.Unlock()
}

func TestEngine_ForwardProxy_EgressCascading(t *testing.T) {
	// Downstream Service B (e.g. Payment Service)
	var downstreamReceivedPath string
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamReceivedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"payment_confirmed","tx_id":"tx_777"}`))
	}))
	defer downstream.Close()

	ring := buffer.NewRingBuffer(10)
	engine := NewEngine(router.NewRouter(), ring, nil)

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	// Service A calls Service B using DevHub as HTTP_PROXY
	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
	}

	req, _ := http.NewRequest(http.MethodPost, downstream.URL+"/charge/confirm", strings.NewReader(`{"amount":5000}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevHub-Trace-ID", "trace-cascade-101")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("forward proxy request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "payment_confirmed") {
		t.Errorf("unexpected body: %s", string(body))
	}
	if downstreamReceivedPath != "/charge/confirm" {
		t.Errorf("expected /charge/confirm on downstream, got %s", downstreamReceivedPath)
	}

	// Check that DevHub recorded the Egress Cascade in its ring buffer
	for i := 0; i < 50; i++ {
		if ring.Count() > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	events := ring.GetAll()
	if len(events) != 1 {
		t.Fatalf("expected 1 cascade event captured, got %d", len(events))
	}
	if !strings.Contains(events[0].Target, "[Egress Cascade]") {
		t.Errorf("expected [Egress Cascade] target tag, got %s", events[0].Target)
	}
	if !strings.HasPrefix(events[0].TraceID, "trace-cascade-101") {
		t.Errorf("expected trace ID prefix preserved, got %s", events[0].TraceID)
	}
}

func TestEngine_MockMode(t *testing.T) {
	r := router.NewRouter()
	deadTarget, _ := url.Parse("http://127.0.0.1:59997")
	r.Add("/api/unready", deadTarget, false)

	engine := NewEngine(r, buffer.NewRingBuffer(5), nil)

	// Set Mock Rule
	engine.Mocks.SetRule(mock.MockRule{
		Prefix:     "/api/unready",
		Enabled:    true,
		Mode:       mock.ModeAlways,
		StatusCode: 200,
		Body:       `{"status":"mocked_success","data":"unblocked_frontend"}`,
	})

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	resp, err := proxyServer.Client().Get(proxyServer.URL + "/api/unready/profile")
	if err != nil {
		t.Fatalf("mock request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 from mock mode, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "unblocked_frontend") {
		t.Errorf("unexpected mock response body: %s", string(body))
	}
	if resp.Header.Get("X-DevHub-Mock-Response") != "true" {
		t.Errorf("expected X-DevHub-Mock-Response header")
	}
}

func TestEngine_ReplayDispatcher_WebhookResigning(t *testing.T) {
	var receivedStripeSig string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedStripeSig = r.Header.Get("Stripe-Signature")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received":true}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	r := router.NewRouter()
	r.Add("/webhook/stripe", uURL, false)

	engine := NewEngine(r, buffer.NewRingBuffer(10), nil)
	dispatcher := NewReplayDispatcher(engine)

	secret := "whsec_test_secret_123"
	payload := `{"type":"charge.captured","id":"ch_999"}`

	res, err := dispatcher.Execute(ReplayRequest{
		Method:         "POST",
		URL:            "/webhook/stripe",
		Headers:        map[string]string{"Content-Type": "application/json"},
		Body:           payload,
		ResignProvider: webhook.ProviderStripe,
		WebhookSecret:  secret,
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", res.StatusCode)
	}

	if !strings.HasPrefix(receivedStripeSig, "t=") || !strings.Contains(receivedStripeSig, "v1=") {
		t.Errorf("expected recalculated Stripe signature, got %s", receivedStripeSig)
	}
}

func TestEngine_LargePayload_StreamingAndCapping(t *testing.T) {
	payloadSize := 256 * 1024
	largeData := bytes.Repeat([]byte("A"), payloadSize)

	var upstreamReceivedLen int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upstreamReceivedLen = len(b)

		w.WriteHeader(http.StatusOK)
		w.Write(largeData)
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	r := router.NewRouter()
	r.Add("/upload", uURL, false)

	ring := buffer.NewRingBuffer(5)
	engine := NewEngine(r, ring, nil)

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/upload", bytes.NewReader(largeData))
	resp, err := proxyServer.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	if upstreamReceivedLen != payloadSize {
		t.Errorf("expected upstream to receive %d bytes, got %d", payloadSize, upstreamReceivedLen)
	}
	if len(respBytes) != payloadSize {
		t.Errorf("expected client to receive %d bytes, got %d", payloadSize, len(respBytes))
	}

	events := ring.GetAll()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if len(events[0].RequestBody) != maxCaptureSize {
		t.Errorf("expected captured request body to cap at %d, got %d", maxCaptureSize, len(events[0].RequestBody))
	}
	if len(events[0].ResponseBody) != maxCaptureSize {
		t.Errorf("expected captured response body to cap at %d, got %d", maxCaptureSize, len(events[0].ResponseBody))
	}
}

func TestEngine_CircuitBreaker(t *testing.T) {
	deadTarget, _ := url.Parse("http://127.0.0.1:59998")
	r := router.NewRouter()
	r.Add("/failing", deadTarget, false)

	engine := NewEngine(r, buffer.NewRingBuffer(5), nil)

	// Trip breaker manually
	engine.Health.RegisterTarget(deadTarget, "/health")
	engine.Health.RecordFailure(deadTarget)
	engine.Health.RecordFailure(deadTarget)
	engine.Health.RecordFailure(deadTarget)

	if engine.Health.IsHealthy(deadTarget) {
		t.Fatalf("target should be marked unhealthy")
	}

	// 1. By default, passive observation mode should NOT block with 503
	recPassive := httptest.NewRecorder()
	reqPassive, _ := http.NewRequest("GET", "/failing/item", nil)
	engine.ServeHTTP(recPassive, reqPassive)
	if recPassive.Code == http.StatusServiceUnavailable {
		t.Errorf("passive mode should not reject with 503")
	}

	// 2. Active enforcement mode SHOULD block with 503
	engine.Health.SetCircuitBreakerEnforcement(true)
	recActive := httptest.NewRecorder()
	reqActive, _ := http.NewRequest("GET", "/failing/item", nil)
	engine.ServeHTTP(recActive, reqActive)

	if recActive.Code != http.StatusServiceUnavailable {
		t.Errorf("expected circuit breaker 503 when enforced, got %d", recActive.Code)
	}
}

func TestEngine_ForwardProxy_StreamingSSE(t *testing.T) {
	// Downstream SSE Server emitting 3 chunks with pauses
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		for i := 1; i <= 3; i++ {
			w.Write([]byte(fmt.Sprintf("data: token_%d\n\n", i)))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer downstream.Close()

	ring := buffer.NewRingBuffer(10)
	engine := NewEngine(router.NewRouter(), ring, nil)

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	// Client uses DevHub as HTTP forward proxy
	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
	}

	req, _ := http.NewRequest(http.MethodGet, downstream.URL+"/v1/chat/stream", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("streaming forward proxy request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}

	// Read chunks as they arrive
	buf := make([]byte, 1024)
	totalRead := ""
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			totalRead += string(buf[:n])
		}
		if err != nil {
			break
		}
	}

	if !strings.Contains(totalRead, "token_1") || !strings.Contains(totalRead, "token_3") {
		t.Errorf("expected all SSE tokens streamed through forward proxy, got: %s", totalRead)
	}
}

func TestEngine_FailureTelemetry_404(t *testing.T) {
	r := router.NewRouter()
	ring := buffer.NewRingBuffer(5)
	engine := NewEngine(r, ring, nil)

	rec := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/unmatched/path", nil)
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}

	events := ring.GetAll()
	if len(events) != 1 {
		t.Fatalf("expected 1 failure trace event in ring buffer, got %d", len(events))
	}
	if events[0].StatusCode != http.StatusNotFound {
		t.Errorf("expected trace event status 404, got %d", events[0].StatusCode)
	}
	if events[0].Target != "[No Route Matched]" {
		t.Errorf("expected target [No Route Matched], got %s", events[0].Target)
	}
}

func TestEngine_ForwardProxy_NoRecursion(t *testing.T) {
	// Destination server
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("reached destination"))
	}))
	defer dest.Close()

	ring := buffer.NewRingBuffer(5)
	engine := NewEngine(router.NewRouter(), ring, nil)

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	// Emulate environment where HTTP_PROXY points to DevHub
	t.Setenv("HTTP_PROXY", proxyServer.URL)
	t.Setenv("http_proxy", proxyServer.URL)

	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
		Timeout: 2 * time.Second, // Must not hang indefinitely
	}

	req, _ := http.NewRequest(http.MethodGet, dest.URL+"/test/recursion", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed with self-recursion error: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "reached destination" {
		t.Errorf("expected 'reached destination', got %s", string(body))
	}
}
