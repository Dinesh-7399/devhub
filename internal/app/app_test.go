package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"devhub/pkg/config"
	"devhub/pkg/mock"
)

func TestApp_DashboardAuthAndSecurity(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			ProxyPort:          19001,
			DashboardPort:      19002,
			DashboardHost:      "127.0.0.1",
			DashboardAuthToken: "secret123",
		},
		Routes: []config.RouteConfig{
			{
				Prefix:      "/auth",
				Target:      "http://127.0.0.1:8081",
				StripPrefix: false,
			},
		},
	}

	application, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create app: %v", err)
	}

	dashMux := http.NewServeMux()
	application.setupDashboardRoutes(dashMux)
	dashServer := httptest.NewServer(dashMux)
	defer dashServer.Close()

	// 1. Request without token should be 401 Unauthorized
	res, err := dashServer.Client().Get(dashServer.URL + "/api/routes")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", res.StatusCode)
	}
	res.Body.Close()

	// 2. Request with invalid token should be 401
	req, _ := http.NewRequest(http.MethodGet, dashServer.URL+"/api/routes", nil)
	req.Header.Set("Authorization", "Bearer wrongtoken")
	res2, err := dashServer.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if res2.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong token, got %d", res2.StatusCode)
	}
	res2.Body.Close()

	// 3. Request with valid header token should be 200 OK
	reqValid, _ := http.NewRequest(http.MethodGet, dashServer.URL+"/api/routes", nil)
	reqValid.Header.Set("Authorization", "Bearer secret123")
	res3, err := dashServer.Client().Do(reqValid)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if res3.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK with valid token, got %d", res3.StatusCode)
	}
	res3.Body.Close()

	// 4. Request with valid query parameter token should be 200 OK
	res4, err := dashServer.Client().Get(dashServer.URL + "/api/routes?token=secret123")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if res4.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK with query token, got %d", res4.StatusCode)
	}
	var routes []map[string]any
	json.NewDecoder(res4.Body).Decode(&routes)
	res4.Body.Close()
	if len(routes) == 0 {
		t.Errorf("expected configured routes, got 0")
	}
}

func TestApp_MockRulesViaDashboard(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			ProxyPort:     19003,
			DashboardPort: 19004,
		},
		Overrides: map[string]config.OverrideConfig{
			"/api/billing": {
				Mock: mock.MockRule{
					Enabled:    true,
					Mode:       "always",
					StatusCode: 200,
					Body:       `{"status":"billed"}`,
				},
			},
		},
	}

	application, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create app: %v", err)
	}

	// Verify mock was loaded from config
	rule, exists := application.Mocks.GetRule("/api/billing")
	if !exists || !rule.Enabled {
		t.Errorf("expected /api/billing mock to be active")
	}

	dashMux := http.NewServeMux()
	application.setupDashboardRoutes(dashMux)
	dashServer := httptest.NewServer(dashMux)
	defer dashServer.Close()

	// Add new mock rule via API
	ruleJSON := `{"prefix":"/api/users","enabled":true,"mode":"always","status_code":201,"body":"{\"id\":1}"}`
	res, err := dashServer.Client().Post(dashServer.URL+"/api/mocks", "application/json", strings.NewReader(ruleJSON))
	if err != nil {
		t.Fatalf("failed to post mock: %v", err)
	}
	res.Body.Close()

	r2, exists2 := application.Mocks.GetRule("/api/users")
	if !exists2 || r2.StatusCode != 201 {
		t.Errorf("expected /api/users mock to be set via API, exists=%v", exists2)
	}
}

func TestApp_TelemetryEventChannel(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			ProxyPort:     19005,
			DashboardPort: 19006,
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create app: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go app.Hub.run(ctx)

	// Simulate event flow
	mockTarget, _ := url.Parse("http://127.0.0.1:9999")
	app.Router.Add("/test", mockTarget, false)

	req := httptest.NewRequest(http.MethodGet, "http://localhost:19005/test/ping", nil)
	rec := httptest.NewRecorder()

	app.Engine.ServeHTTP(rec, req)

	// Verify an event was emitted into EventChan or Ring
	select {
	case ev := <-app.EventChan:
		if ev.Path != "/test/ping" {
			t.Errorf("expected /test/ping path, got %s", ev.Path)
		}
	case <-time.After(100 * time.Millisecond):
		if app.Ring.Count() == 0 {
			t.Errorf("expected event in ring buffer or EventChan")
		}
	}
}

func TestApp_TicketAuthAndTelemetryStats(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			ProxyPort:          19007,
			DashboardPort:      19008,
			DashboardHost:      "127.0.0.1",
			DashboardAuthToken: "admintoken456",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create app: %v", err)
	}

	dashMux := http.NewServeMux()
	app.setupDashboardRoutes(dashMux)
	dashServer := httptest.NewServer(dashMux)
	defer dashServer.Close()

	// 1. Get Ticket with Auth
	ticketReq, _ := http.NewRequest(http.MethodGet, dashServer.URL+"/api/auth/ticket", nil)
	ticketReq.Header.Set("Authorization", "Bearer admintoken456")
	res, err := dashServer.Client().Do(ticketReq)
	if err != nil {
		t.Fatalf("ticket request failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for ticket, got %d", res.StatusCode)
	}

	var ticketData map[string]string
	json.NewDecoder(res.Body).Decode(&ticketData)
	ticket := ticketData["ticket"]
	if ticket == "" {
		t.Fatalf("expected non-empty ticket")
	}

	// Verify ticket stored in tickets map
	app.ticketMu.Lock()
	expiry, exists := app.tickets[ticket]
	app.ticketMu.Unlock()
	if !exists || time.Now().After(expiry) {
		t.Errorf("expected valid ticket stored in memory")
	}

	// 2. Query Telemetry Stats
	statsReq, _ := http.NewRequest(http.MethodGet, dashServer.URL+"/api/telemetry/stats", nil)
	statsReq.Header.Set("Authorization", "Bearer admintoken456")
	res2, err := dashServer.Client().Do(statsReq)
	if err != nil {
		t.Fatalf("stats request failed: %v", err)
	}
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for stats, got %d", res2.StatusCode)
	}

	var stats map[string]any
	json.NewDecoder(res2.Body).Decode(&stats)
	if _, ok := stats["total_events"]; !ok {
		t.Errorf("expected total_events in stats")
	}
	if _, ok := stats["dropped_events"]; !ok {
		t.Errorf("expected dropped_events in stats")
	}
	if capVal, ok := stats["queue_capacity"].(float64); !ok || capVal != 4096 {
		t.Errorf("expected queue_capacity 4096, got %v", stats["queue_capacity"])
	}
}
