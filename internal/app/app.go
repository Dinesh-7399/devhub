package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
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
	"gopkg.in/yaml.v3"
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
			return
		case client := <-h.register:
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

func (h *Hub) broadcastJSON(v any) {
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
	wg               sync.WaitGroup
}

// New initializes an App instance from configuration.
func New(cfg *config.Config) (*App, error) {
	r := router.NewRouter()
	ring := buffer.NewRingBuffer(cfg.Tracing.RingSize)
	hub := newHub()
	eventChan := make(chan buffer.TraceEvent, 4096)

	// Decoupled telemetry worker: data plane pushes non-blocking to channel
	engine := proxy.NewEngine(r, ring, func(ev buffer.TraceEvent) {
		select {
		case eventChan <- ev:
		default:
			// Non-blocking telemetry drop if queue fills under extreme stress
		}
	})
	engine.RedactPII = cfg.Tracing.RedactPII
	engine.Health.SetCircuitBreakerEnforcement(cfg.Server.EnforceCircuitBreaker)

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

	// Auto-detect Docker Compose files deterministically
	autoDetectComposeFiles(r, engine.Health)

	replayDispatcher := proxy.NewReplayDispatcher(engine)

	app := &App{
		Config:           cfg,
		Router:           r,
		Ring:             ring,
		Health:           engine.Health,
		Mocks:            engine.Mocks,
		Engine:           engine,
		ReplayDispatcher: replayDispatcher,
		Hub:              hub,
		EventChan:        eventChan,
	}

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

	// 5. Start L7 Reverse Proxy Listener
	proxyAddr := fmt.Sprintf("%s:%d", a.Config.Server.Host, a.Config.Server.ProxyPort)
	if a.Config.Server.Host == "" {
		proxyAddr = fmt.Sprintf(":%d", a.Config.Server.ProxyPort)
	}
	a.ProxyServer = &http.Server{
		Addr:              proxyAddr,
		Handler:           a.Engine,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	go func() {
		if err := a.ProxyServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Proxy server failed: %v", err)
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
	a.DashboardServer = &http.Server{
		Addr:              dashAddr,
		Handler:           dashboardMux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	go func() {
		if err := a.DashboardServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Dashboard server failed: %v", err)
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
			// Allow origin matching host or localhost / 127.0.0.1
			reqHost := r.Host
			if strings.EqualFold(u.Host, reqHost) ||
				strings.HasPrefix(u.Host, "localhost:") ||
				strings.HasPrefix(u.Host, "127.0.0.1:") ||
				u.Host == "localhost" ||
				u.Host == "127.0.0.1" {
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

	// WebSocket Live Stream Hub
	mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		token := a.Config.Server.DashboardAuthToken
		if token != "" {
			if req.URL.Query().Get("token") != token {
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
		a.Hub.register <- client

		go func() {
			defer func() {
				a.Hub.unregister <- client
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
	fmt.Printf("%s%s  │  ██╔══██╗██╔════╝██║   ██║██║  ██║██║   ██║██╔══██╗   DEVHUB v2.5 PRO  │%s\n", colBold, colCyan, colReset)
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

type rawCompose struct {
	Services map[string]struct {
		Ports []string `yaml:"ports"`
	} `yaml:"services"`
}

func autoDetectComposeFiles(r *router.Router, health *proxy.HealthMonitor) {
	composeFiles := []string{"docker-compose.yml", "docker-compose.yaml", "docker-compose.infra.yml"}
	for _, f := range composeFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}

		var comp rawCompose
		if err := yaml.Unmarshal(data, &comp); err != nil {
			continue
		}

		for serviceName, svc := range comp.Services {
			if len(svc.Ports) == 0 {
				continue
			}

			hostPort := parseHostPort(svc.Ports[0])
			if hostPort == 0 {
				continue
			}

			targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", hostPort))
			routePrefix := "/api/" + serviceName
			r.Add(routePrefix, targetURL, true)
			if health != nil {
				health.RegisterTarget(targetURL, "tcp")
			}
		}
	}
}

func parseHostPort(portDef string) int {
	parts := strings.Split(portDef, ":")
	if len(parts) >= 2 {
		pStr := parts[0]
		if len(parts) == 3 {
			pStr = parts[1] // e.g. "127.0.0.1:8080:80"
		}
		if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
			return p
		}
	} else if len(parts) == 1 {
		if p, err := strconv.Atoi(parts[0]); err == nil && p > 0 {
			return p
		}
	}
	return 0
}
