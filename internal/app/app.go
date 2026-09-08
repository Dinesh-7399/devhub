package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devhub/pkg/buffer"
	"devhub/pkg/config"
	"devhub/pkg/docker"
	"devhub/pkg/mock"
	"devhub/pkg/proxy"
	"devhub/pkg/router"
	"devhub/pkg/tracing"
	"devhub/pkg/tunnel"
	"devhub/pkg/ui"

	"github.com/gorilla/websocket"
)

// ANSI Color constants for modern terminal output
const (
	colReset    = "\033[0m"
	colBold     = "\033[1m"
	colCyan     = "\033[38;5;51m"
	colSky      = "\033[38;5;45m"
	colGreen    = "\033[38;5;82m"
	colEmerald  = "\033[38;5;48m"
	colYellow   = "\033[38;5;220m"
	colAmber    = "\033[38;5;214m"
	colRed      = "\033[38;5;197m"
	colPurple   = "\033[38;5;141m"
	colGray     = "\033[38;5;244m"
	colDarkGray = "\033[38;5;238m"

	bgGet  = "\033[48;5;24m\033[38;5;51m\033[1m"
	bgPost = "\033[48;5;22m\033[38;5;82m\033[1m"
	bgPut  = "\033[48;5;58m\033[38;5;220m\033[1m"
	bgDel  = "\033[48;5;52m\033[38;5;197m\033[1m"
	bgWS   = "\033[48;5;54m\033[38;5;141m\033[1m"
)

// Client represents an active WebSocket connection to the Developer Cockpit.
type Client struct {
	conn *websocket.Conn
	send chan []byte
}

// Hub manages WebSocket telemetry subscriptions.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	closed     atomic.Bool
}

func newHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte, 2048),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.Close()
			return
		case client := <-h.register:
			if h.closed.Load() {
				continue
			}
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			if h.closed.Load() {
				continue
			}
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// Drop frame for lagging client without stalling proxy
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) Close() {
	if h.closed.CompareAndSwap(false, true) {
		h.mu.Lock()
		defer h.mu.Unlock()
		for client := range h.clients {
			delete(h.clients, client)
			close(client.send)
		}
	}
}

func (h *Hub) broadcastJSON(v any) {
	if h.closed.Load() {
		return
	}
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case h.broadcast <- data:
	default:
	}
}


// App encapsulates DevHub control plane, data plane, and telemetry pipeline.
type App struct {
	Config           *config.Config
	Router           *router.Router
	Ring             *buffer.RingBuffer
	Health           *proxy.HealthMonitor
	Mocks            *mock.Manager
	Engine           *proxy.Engine
	ReplayDispatcher *proxy.ReplayDispatcher
	DockerWatcher    *docker.Watcher
	TunnelClient     *tunnel.Client
	ProxyServer      *http.Server
	DashboardServer  *http.Server
	Hub              *Hub
	EventChan        chan buffer.TraceEvent // Decoupled asynchronous telemetry queue
	EventsTotal      atomic.Uint64
	EventsDropped    atomic.Uint64
	ticketMu         sync.Mutex
	tickets          map[string]time.Time
	wg               sync.WaitGroup
}

// New initializes an App instance from configuration.
func New(cfg *config.Config) (*App, error) {
	r := router.NewRouter()
	ring := buffer.NewRingBuffer(cfg.Tracing.RingSize)
	hub := newHub()
	eventChan := make(chan buffer.TraceEvent, 4096)

	app := &App{
		Config:    cfg,
		Router:    r,
		Ring:      ring,
		Hub:       hub,
		EventChan: eventChan,
		tickets:   make(map[string]time.Time),
	}

	overflowPolicy := cfg.Tracing.OverflowPolicy
	if overflowPolicy == "" {
		overflowPolicy = "drop"
	}

	// Decoupled telemetry worker: data plane pushes non-blocking to channel
	engine := proxy.NewEngine(r, ring, func(ev buffer.TraceEvent) {
		app.EventsTotal.Add(1)
		if overflowPolicy == "block" {
			select {
			case eventChan <- ev:
			case <-time.After(100 * time.Millisecond):
				app.EventsDropped.Add(1)
			}
		} else {
			select {
			case eventChan <- ev:
			default:
				app.EventsDropped.Add(1)
			}
		}
	})
	engine.RedactPII = cfg.Tracing.RedactPII
	engine.Health.SetCircuitBreakerEnforcement(cfg.Server.EnforceCircuitBreaker)
	if cfg.Egress.Enabled {
		engine.SecurityPolicy = &proxy.EgressSecurityPolicy{
			BlockMetadata: cfg.Server.BlockMetadataEndpoints,
			Mode:          cfg.Egress.Mode,
			AllowedHosts:  cfg.Egress.AllowedHosts,
			DeniedCIDRs:   proxy.ParsePrefixes(cfg.Egress.DeniedCIDRs),
		}
	} else if engine.SecurityPolicy != nil {
		engine.SecurityPolicy.BlockMetadata = cfg.Server.BlockMetadataEndpoints
	}

	// Hook health status updates into WebSocket hub
	engine.Health.SetOnStatusChange(func(th proxy.TargetHealth) {
		hub.broadcastJSON(map[string]any{
			"type":   "health",
			"target": th,
		})
	})

	// Mount configured static routes
	for _, routeCfg := range cfg.Routes {
		targetURL, err := url.Parse(routeCfg.Target)
		if err == nil {
			r.AddWithTimeout(routeCfg.Prefix, targetURL, routeCfg.StripPrefix, routeCfg.TimeoutMs)
			engine.Health.RegisterTarget(targetURL, routeCfg.HealthCheckPath)
		}
	}

	// Mount overrides and mock rules from config
	for prefix, ov := range cfg.Overrides {
		if ov.Mock.Enabled || ov.Mock.Body != "" {
			rule := ov.Mock
			rule.Prefix = prefix
			engine.Mocks.SetRule(rule)
		}
	}

	replayDispatcher := proxy.NewReplayDispatcher(engine)

	app.Health = engine.Health
	app.Mocks = engine.Mocks
	app.Engine = engine
	app.ReplayDispatcher = replayDispatcher

	return app, nil
}

// Start boots all servers, background workers, and auto-discovery components.
func (a *App) Start(ctx context.Context) error {
	// 1. Run telemetry worker
	go a.Hub.run(ctx)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-a.EventChan:
				a.Hub.broadcastJSON(ev)
				printLiveTerminalLog(ev)
			}
		}
	}()

	// 2. Start Health probing
	a.Health.Start(ctx, 3*time.Second)

	// 3. Start Docker discovery if enabled
	if a.Config.Docker.Enabled {
		dockerClient := docker.NewClient(a.Config.Docker.SocketPath)
		a.DockerWatcher = docker.NewWatcher(dockerClient, a.Router, func(logEntry docker.LogEntry) {
			a.Hub.broadcastJSON(map[string]any{
				"type":      "log",
				"container": logEntry.Container,
				"stream":    logEntry.Stream,
				"message":   logEntry.Message,
				"timestamp": logEntry.Timestamp,
			})
		}, func(action string, cr *docker.ContainerRoute) {
			if cr != nil && cr.Target != nil {
				if action == "mount" {
					a.Health.RegisterTarget(cr.Target, "tcp")
				}
			}
		})
		a.DockerWatcher.SetMode(a.Config.Docker.Mode)
		a.DockerWatcher.SetAutoRouteAll(a.Config.Docker.AutoRouteAll)
		_ = a.DockerWatcher.Start(ctx)
	}

	// 4. Start public tunnel if enabled
	if a.Config.Tunnel.Enabled {
		a.TunnelClient = tunnel.NewClient(
			a.Config.Tunnel.ServerURL,
			a.Config.Tunnel.Subdomain,
			a.Config.Tunnel.AuthToken,
			fmt.Sprintf("http://localhost:%d", a.Config.Server.ProxyPort),
		)
		_ = a.TunnelClient.Start(ctx)
	}

	// 5. Start L7 Reverse Proxy Listener synchronously
	proxyAddr := fmt.Sprintf("%s:%d", a.Config.Server.Host, a.Config.Server.ProxyPort)
	if a.Config.Server.Host == "" {
		proxyAddr = fmt.Sprintf(":%d", a.Config.Server.ProxyPort)
	}
	proxyListener, err := net.Listen("tcp", proxyAddr)
	if err != nil {
		return fmt.Errorf("failed to bind proxy port %s: %w", proxyAddr, err)
	}

	a.ProxyServer = &http.Server{
		Addr:              proxyAddr,
		Handler:           a.Engine,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		if err := a.ProxyServer.Serve(proxyListener); err != nil && err != http.ErrServerClosed {
			log.Printf("Proxy server error: %v", err)
		}
	}()

	// 6. Start Developer Cockpit Server with Origin & Auth Security
	dashboardMux := http.NewServeMux()
	a.setupDashboardRoutes(dashboardMux)

	dashHost := a.Config.Server.DashboardHost
	if dashHost == "" {
		dashHost = "127.0.0.1" // Secure binding by default
	}
	dashAddr := fmt.Sprintf("%s:%d", dashHost, a.Config.Server.DashboardPort)
	dashListener, err := net.Listen("tcp", dashAddr)
	if err != nil {
		_ = proxyListener.Close()
		return fmt.Errorf("failed to bind dashboard port %s: %w", dashAddr, err)
	}

	a.DashboardServer = &http.Server{
		Addr:              dashAddr,
		Handler:           dashboardMux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		if err := a.DashboardServer.Serve(dashListener); err != nil && err != http.ErrServerClosed {
			log.Printf("Dashboard server error: %v", err)
		}
	}()

	// Print banner
	printHeaderBanner(a.Config)

	return nil
}

// Shutdown gracefully terminates all servers and background listeners.
func (a *App) Shutdown(ctx context.Context) error {
	var firstErr error
	if a.ProxyServer != nil {
		if err := a.ProxyServer.Shutdown(ctx); err != nil {
			firstErr = err
		}
	}
	if a.DashboardServer != nil {
		if err := a.DashboardServer.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if a.Hub != nil {
		a.Hub.Close()
	}
	if a.DockerWatcher != nil {
		a.DockerWatcher.Stop()
	}
	a.wg.Wait()
	return firstErr
}

func (a *App) setupDashboardRoutes(mux *http.ServeMux) {
	// WebSocket Upgrader with secure origin verification
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true // direct non-browser client
			}
			u, err := url.Parse(origin)
			if err != nil {
				return false
			}
			if u.Scheme != "http" && u.Scheme != "https" {
				return false
			}
			reqHost := r.Host
			dashPort := fmt.Sprintf("%d", a.Config.Server.DashboardPort)
			expectedHost1 := "localhost:" + dashPort
			expectedHost2 := "127.0.0.1:" + dashPort

			if strings.EqualFold(u.Host, reqHost) ||
				strings.EqualFold(u.Host, expectedHost1) ||
				strings.EqualFold(u.Host, expectedHost2) ||
				(dashPort == "80" && (u.Host == "localhost" || u.Host == "127.0.0.1")) {
				return true
			}
			return false
		},
	}

	authMiddleware := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			token := a.Config.Server.DashboardAuthToken
			if token != "" {
				authHeader := req.Header.Get("Authorization")
				queryToken := req.URL.Query().Get("token")
				if queryToken != token && authHeader != "Bearer "+token {
					http.Error(w, "Unauthorized: invalid dashboard token", http.StatusUnauthorized)
					return
				}
			}
			next(w, req)
		}
	}

	// Ticket endpoint: exchange bearer token for single-use short-lived (60s) WebSocket ticket
	mux.HandleFunc("/api/auth/ticket", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		ticket := hex.EncodeToString(b)

		now := time.Now()
		a.ticketMu.Lock()
		for t, exp := range a.tickets {
			if now.After(exp) {
				delete(a.tickets, t)
			}
		}
		if len(a.tickets) < 1000 {
			a.tickets[ticket] = now.Add(60 * time.Second)
		} else {
			a.ticketMu.Unlock()
			http.Error(w, "Too many active tickets", http.StatusServiceUnavailable)
			return
		}
		a.ticketMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"ticket": ticket,
		})
	}))

	// WebSocket Live Stream Hub
	mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		token := a.Config.Server.DashboardAuthToken
		if token != "" {
			authorized := false
			authHeader := req.Header.Get("Authorization")
			if authHeader == "Bearer "+token {
				authorized = true
			}

			// Validate single-use ticket
			if !authorized {
				ticket := req.URL.Query().Get("ticket")
				if ticket != "" {
					a.ticketMu.Lock()
					expiry, exists := a.tickets[ticket]
					if exists && time.Now().Before(expiry) {
						delete(a.tickets, ticket) // Invalidate single-use ticket
						authorized = true
					}
					a.ticketMu.Unlock()
				}
			}

			// Fallback to token query param for backward compatibility
			if !authorized && req.URL.Query().Get("token") == token {
				authorized = true
			}

			if !authorized {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}

		client := &Client{
			conn: conn,
			send: make(chan []byte, 256),
		}

		if a.Hub.closed.Load() {
			conn.Close()
			return
		}
		a.Hub.register <- client

		go func() {
			defer func() {
				if !a.Hub.closed.Load() {
					select {
					case a.Hub.unregister <- client:
					case <-time.After(100 * time.Millisecond):
					}
				}
				conn.Close()
			}()
			for message := range client.send {
				if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
					break
				}
			}
		}()

		// Send buffer history on connect
		history := a.Ring.GetAll()
		for i := len(history) - 1; i >= 0; i-- {
			d, _ := json.Marshal(history[i])
			select {
			case client.send <- d:
			default:
			}
		}

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	})

	// Active Routes API
	mux.HandleFunc("/api/routes", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.Config.Routes)
	}))

	// Telemetry Queue Stats & Drop Observability API
	mux.HandleFunc("/api/telemetry/stats", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		policy := a.Config.Tracing.OverflowPolicy
		if policy == "" {
			policy = "drop"
		}
		json.NewEncoder(w).Encode(map[string]any{
			"total_events":    a.EventsTotal.Load(),
			"dropped_events":  a.EventsDropped.Load(),
			"queue_depth":     len(a.EventChan),
			"queue_capacity":  cap(a.EventChan),
			"overflow_policy": policy,
		})
	}))

	// Upstream Health Snapshot API
	mux.HandleFunc("/api/health", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.Health.GetAllStatus())
	}))

	// Mock Rules API
	mux.HandleFunc("/api/mocks", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.Method == http.MethodPost {
			var rule mock.MockRule
			if err := json.NewDecoder(req.Body).Decode(&rule); err == nil {
				a.Mocks.SetRule(rule)
			}
		}
		json.NewEncoder(w).Encode(a.Mocks.ListRules())
	}))

	// Mock Toggle API
	mux.HandleFunc("/api/mocks/toggle", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Prefix  string `json:"prefix"`
			Enabled bool   `json:"enabled"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err == nil {
			a.Mocks.ToggleRule(payload.Prefix, payload.Enabled)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))

	// 1-Click Request Replay API
	mux.HandleFunc("/api/replay", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var replayReq proxy.ReplayRequest
		if err := json.NewDecoder(req.Body).Decode(&replayReq); err != nil {
			http.Error(w, "Invalid JSON replay request", http.StatusBadRequest)
			return
		}

		res, err := a.ReplayDispatcher.Execute(replayReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	}))

	// Trace Waterfall API
	mux.HandleFunc("/api/waterfall", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		traceID := req.URL.Query().Get("trace_id")
		w.Header().Set("Content-Type", "application/json")
		waterfall := tracing.BuildWaterfall(a.Ring.GetAll(), traceID)
		json.NewEncoder(w).Encode(waterfall)
	}))

	// Traces Snapshot API
	mux.HandleFunc("/api/traces", authMiddleware(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.Ring.GetAll())
	}))

	// Embedded Developer Cockpit Single-Page App
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(ui.DashboardHTML))
	})
}

func printHeaderBanner(cfg *config.Config) {
	fmt.Println()
	fmt.Printf("%s%s  ┌────────────────────────────────────────────────────────────────────────┐%s\n", colBold, colCyan, colReset)
	fmt.Printf("%s%s  │  ██████╗ ███████╗██╗   ██╗██╗  ██╗██╗   ██╗██████╗                     │%s\n", colBold, colCyan, colReset)
	fmt.Printf("%s%s  │  ██╔══██╗██╔════╝██║   ██║██║  ██║██║   ██║██╔══██╗   DEVHUB v2.6 PRO  │%s\n", colBold, colCyan, colReset)
	fmt.Printf("%s%s  │  ██║  ██║█████╗  ██║   ██║███████║██║   ██║██████╔╝   Mesh Switchboard │%s\n", colBold, colSky, colReset)
	fmt.Printf("%s%s  │  ██║  ██║██╔══╝  ╚██╗ ██╔╝██╔══██║██║   ██║██╔══██╗   & Live Debugger  │%s\n", colBold, colSky, colReset)
	fmt.Printf("%s%s  │  ██████╔╝███████╗ ╚████╔╝ ██║  ██║╚██████╔╝██████╔╝                    │%s\n", colBold, colPurple, colReset)
	fmt.Printf("%s%s  │  ╚═════╝ ╚══════╝  ╚═══╝  ╚═╝  ╚═╝ ╚═════╝ ╚═════╝                     │%s\n", colBold, colPurple, colReset)
	fmt.Printf("%s%s  └────────────────────────────────────────────────────────────────────────┘%s\n", colBold, colCyan, colReset)
	fmt.Println()
	fmt.Printf("  %s• Ingress Gateway%s   : %shttp://localhost:%d%s\n", colBold, colReset, colBold+colCyan, cfg.Server.ProxyPort, colReset)
	fmt.Printf("  %s• Developer Cockpit%s : %shttp://%s:%d%s\n", colBold, colReset, colBold+colEmerald, cfg.Server.DashboardHost, cfg.Server.DashboardPort, colReset)
	fmt.Printf("  %s• WebSocket Stream%s  : %sws://%s:%d/ws%s\n", colBold, colReset, colBold+colPurple, cfg.Server.DashboardHost, cfg.Server.DashboardPort, colReset)
	if cfg.Docker.Enabled {
		fmt.Printf("  %s• Docker Discovery%s  : %sActive (listening on IPC socket)%s\n", colBold, colReset, colSky, colReset)
	}
	fmt.Printf("  %s• Forward Proxy%s     : %sHTTP_PROXY=http://localhost:%d%s %s(Zero-Code Cascades)%s\n",
		colBold, colReset, colYellow, cfg.Server.ProxyPort, colReset, colGray, colReset)

	fmt.Println()
	fmt.Printf("%s┌─── Active Mesh Route Table ──────────────────────────────────────────────┐%s\n", colDarkGray, colReset)
	for _, rc := range cfg.Routes {
		mode := colGray + "[pass-through]" + colReset
		if rc.StripPrefix {
			mode = colAmber + "[strip-prefix]" + colReset
		}
		fmt.Printf("│  %s%-20s%s ➔  %s%-28s%s %s │\n", colCyan, rc.Prefix, colReset, colSky, rc.Target, colReset, mode)
	}
	fmt.Printf("%s└─── Live Traffic Intercept Stream ────────────────────────────────────────┘%s\n", colDarkGray, colReset)
	fmt.Printf("%s⚡ Listening for incoming requests, webhooks, and egress cascades...%s\n\n", colEmerald, colReset)
}

func printLiveTerminalLog(ev buffer.TraceEvent) {
	methodBadge := bgGet + " GET " + colReset
	switch ev.Method {
	case "POST":
		methodBadge = bgPost + " POST " + colReset
	case "PUT":
		methodBadge = bgPut + " PUT " + colReset
	case "DELETE":
		methodBadge = bgDel + " DEL " + colReset
	case "PATCH":
		methodBadge = bgPut + " PATCH " + colReset
	case "WS":
		methodBadge = bgWS + "  WS  " + colReset
	case "CONNECT":
		methodBadge = bgWS + " CONN " + colReset
	}

	statusColor := colEmerald
	if ev.StatusCode >= 500 {
		statusColor = colRed
	} else if ev.StatusCode >= 400 {
		statusColor = colAmber
	} else if ev.StatusCode >= 300 {
		statusColor = colSky
	}

	latencyColor := colEmerald
	if ev.DurationMs >= 500 {
		latencyColor = colRed
	} else if ev.DurationMs >= 150 {
		latencyColor = colYellow
	}

	traceShort := ev.TraceID
	if len(traceShort) > 8 {
		traceShort = traceShort[:8]
	}

	targetDisp := ev.Target
	if len(targetDisp) > 34 {
		targetDisp = targetDisp[:31] + "..."
	}

	fmt.Printf("%s[%s]%s %s %-24s %s%3d%s %s%4dms%s ➔ %s%-34s%s %s[%s]%s\n",
		colDarkGray, ev.Timestamp, colReset,
		methodBadge,
		ev.Path,
		statusColor+colBold, ev.StatusCode, colReset,
		latencyColor, ev.DurationMs, colReset,
		colGray, targetDisp, colReset,
		colPurple, traceShort, colReset,
	)
}
