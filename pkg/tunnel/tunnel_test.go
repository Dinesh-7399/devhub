package tunnel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestTunnel_LoopbackRelay(t *testing.T) {
	// 1. Mock Local Service (e.g. localhost:4000)
	localService := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received_stream":"` + r.Header.Get("X-DevHub-Tunnel-Stream") + `"}`))
	}))
	defer localService.Close()

	// 2. Mock Cloud Relay Server (WebSocket endpoint)
	var relayUpgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var capturedResponse TunnelResponse
	var wg sync.WaitGroup
	wg.Add(1)

	relayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := relayUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send simulated public webhook request to tunnel client
		reqPayload := TunnelRequest{
			StreamID: "stream-test-777",
			Method:   "POST",
			Path:     "/webhook",
			Headers:  map[string]string{"Content-Type": "application/json"},
			Body:     `{"event":"payment_success"}`,
		}
		reqBytes, _ := json.Marshal(reqPayload)
		_ = conn.WriteMessage(websocket.TextMessage, reqBytes)

		// Wait for tunnel client response
		_, msg, err := conn.ReadMessage()
		if err == nil {
			_ = json.Unmarshal(msg, &capturedResponse)
			wg.Done()
		}
	}))
	defer relayServer.Close()

	wsURL := "ws" + strings.TrimPrefix(relayServer.URL, "http")

	// 3. Start Tunnel Client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(wsURL, "test-subdomain", "auth-token-123", localService.URL)
	_ = client.Start(ctx)

	// Wait for response roundtrip
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		if capturedResponse.StreamID != "stream-test-777" {
			t.Errorf("expected streamID stream-test-777, got %s", capturedResponse.StreamID)
		}
		if capturedResponse.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", capturedResponse.StatusCode)
		}
		if !strings.Contains(capturedResponse.Body, `"received_stream":"stream-test-777"`) {
			t.Errorf("unexpected body: %s", capturedResponse.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("tunnel test timed out")
	}
}
