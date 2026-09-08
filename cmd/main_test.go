package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"devhub/pkg/buffer"
	"devhub/pkg/mock"
	"devhub/pkg/proxy"
	"devhub/pkg/router"
	"devhub/pkg/webhook"
)

func TestFullSupervisorIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Upstream API mock
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"online","service":"auth"}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)

	r := router.NewRouter()
	r.Add("/auth", uURL, false)

	ring := buffer.NewRingBuffer(20)
	hub := newHub()
	go hub.run(ctx)

	engine := proxy.NewEngine(r, ring, func(ev buffer.TraceEvent) {
		hub.broadcastJSON(ev)
	})
	engine.Health.RegisterTarget(uURL, "/health")

	// Dashboard Mux
	dashMux := http.NewServeMux()
	dashMux.HandleFunc("/api/health", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(engine.Health.GetAllStatus())
	})
	dashMux.HandleFunc("/api/mocks", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.Method == http.MethodPost {
			var rule mock.MockRule
			json.NewDecoder(req.Body).Decode(&rule)
			engine.Mocks.SetRule(rule)
		}
		json.NewEncoder(w).Encode(engine.Mocks.ListRules())
	})

	dashServer := httptest.NewServer(dashMux)
	defer dashServer.Close()

	// 1. Test Health API
	res, err := dashServer.Client().Get(dashServer.URL + "/api/health")
	if err != nil {
		t.Fatalf("health api failed: %v", err)
	}
	defer res.Body.Close()
	var targets []proxy.TargetHealth
	json.NewDecoder(res.Body).Decode(&targets)
	if len(targets) == 0 {
		t.Errorf("expected registered health targets, got 0")
	}

	// 2. Test Mock Rule Creation & Interception
	mockPayload := `{"prefix":"/api/payment","enabled":true,"mode":"always","status_code":201,"body":"{\"status\":\"mock_paid\"}"}`
	res2, err := dashServer.Client().Post(dashServer.URL+"/api/mocks", "application/json", strings.NewReader(mockPayload))
	if err != nil {
		t.Fatalf("mock creation failed: %v", err)
	}
	res2.Body.Close()

	proxyServer := httptest.NewServer(engine)
	defer proxyServer.Close()

	res3, err := proxyServer.Client().Get(proxyServer.URL + "/api/payment/charge")
	if err != nil {
		t.Fatalf("mock interception failed: %v", err)
	}
	defer res3.Body.Close()

	body3, _ := io.ReadAll(res3.Body)
	if res3.StatusCode != 201 {
		t.Errorf("expected status 201 from mock, got %d", res3.StatusCode)
	}
	if !strings.Contains(string(body3), "mock_paid") {
		t.Errorf("expected mock body, got %s", string(body3))
	}
}

func TestReplayHandler_WebhookResigning(t *testing.T) {
	var capturedStripeHeader string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedStripeHeader = r.Header.Get("Stripe-Signature")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received":true}`))
	}))
	defer upstream.Close()

	uURL, _ := url.Parse(upstream.URL)
	r := router.NewRouter()
	r.Add("/webhook", uURL, false)

	engine := proxy.NewEngine(r, buffer.NewRingBuffer(10), nil)
	dispatcher := proxy.NewReplayDispatcher(engine)

	replayReq := proxy.ReplayRequest{
		Method:         "POST",
		URL:            "/webhook/events",
		Headers:        map[string]string{"Content-Type": "application/json"},
		Body:           `{"id":"evt_999","amount":5000}`,
		ResignProvider: webhook.ProviderStripe,
		WebhookSecret:  "whsec_prod_secret_456",
	}

	res, err := dispatcher.Execute(replayReq)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", res.StatusCode)
	}
	if !strings.HasPrefix(capturedStripeHeader, "t=") {
		t.Errorf("expected Stripe-Signature with t= and v1=, got %s", capturedStripeHeader)
	}
}

func TestIsAuthorizedDashboardRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set("Authorization", "Token test-token")
	if !isAuthorizedDashboardRequest(req, "test-token") {
		t.Fatalf("expected bearer token to authorize request")
	}

	req2 := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req2.Header.Set("X-DevHub-Token", "alt-token")
	if !isAuthorizedDashboardRequest(req2, "alt-token") {
		t.Fatalf("expected X-DevHub-Token to authorize request")
	}
}

func TestIsAllowedDashboardOrigin_Defaults(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("Origin", "http://localhost:4040")
	if !isAllowedDashboardOrigin(req, nil) {
		t.Fatalf("expected localhost origin to be allowed by default")
	}

	req.Header.Set("Origin", "https://evil.example")
	if isAllowedDashboardOrigin(req, nil) {
		t.Fatalf("expected non-local origin to be blocked by default")
	}

	req.Header.Set("Origin", "https://dash.example")
	if !isAllowedDashboardOrigin(req, []string{"https://dash.example"}) {
		t.Fatalf("expected explicitly allowed origin to pass")
	}
}
