package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

// ANSI Color constants for high-contrast, modern terminal output
const (
	colReset    = "\033[0m"
	colBold     = "\033[1m"
	colDim      = "\033[2m"
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

	// Background badges
	bgGet  = "\033[48;5;24m\033[38;5;51m\033[1m"
	bgPost = "\033[48;5;22m\033[38;5;82m\033[1m"
	bgPut  = "\033[48;5;58m\033[38;5;220m\033[1m"
	bgDel  = "\033[48;5;52m\033[38;5;197m\033[1m"
	bgWS   = "\033[48;5;54m\033[38;5;141m\033[1m"
)

// Hub maintains the set of active dashboard clients and broadcasts telemetry non-blockingly.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

type Client struct {
	conn *websocket.Conn
	send chan []byte
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

func main() {
	configPath := flag.String("config", "", "Path to devhub.yaml configuration file")
	proxyPort := flag.Int("port", 0, "Ingress proxy port (default: 4000)")
	dashboardPort := flag.Int("dashboard", 0, "Inspection dashboard port (default: 4040)")
	noDocker := flag.Bool("no-docker", false, "Disable Docker container auto-discovery")
	enableTunnel := flag.Bool("tunnel", false, "Enable public ingress tunnel")
	flag.Parse()

	if *configPath == "" {
		if envCfg := os.Getenv("DEVHUB_CONFIG"); envCfg != "" {
			*configPath = envCfg
		}
	}

	// 1. Load Configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("%s❌ Failed to load config: %v%s\n", colRed, err, colReset)
		os.Exit(1)
	}

	if *proxyPort > 0 {
		cfg.Server.ProxyPort = *proxyPort
	} else if pStr := os.Getenv("PORT"); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
			cfg.Server.ProxyPort = p
		}
	} else if pStr := os.Getenv("DEVHUB_PORT"); pStr != "" {
		if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
			cfg.Server.ProxyPort = p
		}
	}

	if *dashboardPort > 0 {
		cfg.Server.DashboardPort = *dashboardPort
	} else if dStr := os.Getenv("DASHBOARD"); dStr != "" {
		if d, err := strconv.Atoi(dStr); err == nil && d > 0 {
			cfg.Server.DashboardPort = d
		}
	} else if dStr := os.Getenv("DEVHUB_DASHBOARD"); dStr != "" {
		if d, err := strconv.Atoi(dStr); err == nil && d > 0 {
			cfg.Server.DashboardPort = d
		}
	}

	if dh := os.Getenv("DOCKER_HOST"); dh != "" {
		cfg.Docker.SocketPath = dh
	}
	if *noDocker {
		cfg.Docker.Enabled = false
	}
	if *enableTunnel {
		cfg.Tunnel.Enabled = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Initialize Routing & Buffering
	r := router.NewRouter()
	ring := buffer.NewRingBuffer(cfg.Tracing.RingSize)
	hub := newHub()
	go hub.run(ctx)

	// 3. Initialize Proxy & Replay Engine with Live Colorized Terminal Logger
	engine := proxy.NewEngine(r, ring, func(ev buffer.TraceEvent) {
		hub.broadcastJSON(ev)
		printLiveTerminalLog(ev)
	})
	engine.RedactPII = cfg.Tracing.RedactPII
	engine.ConfigureEgressPolicy(cfg.Security.AllowedEgressHost, !cfg.Security.AllowPrivateEgress)

	// Hook health status updates into WebSocket hub
	engine.Health.SetOnStatusChange(func(th proxy.TargetHealth) {
		hub.broadcastJSON(map[string]any{
			"type":   "health",
			"target": th,
		})
	})
	engine.Health.Start(ctx, 3*time.Second)

	// Mount configured static routes and register health probes
	for _, routeCfg := range cfg.Routes {
		targetURL, err := url.Parse(routeCfg.Target)
		if err == nil {
			r.Add(routeCfg.Prefix, targetURL, routeCfg.StripPrefix)
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

	// Auto-detect Docker Compose files (e.g. docker-compose.yml, docker-compose.infra.yml)
	autoDetectComposeFiles(r, engine.Health)

	// Auto-detect local Monorepo services directory if present
	autoDetectLocalServices(r, engine.Health)

	replayDispatcher := proxy.NewReplayDispatcher(engine)

	// Print Beautiful Box-Framed Colored CLI Banner
	printHeaderBanner(cfg)

	// 4. Initialize Docker Engine Auto-Discovery
	if cfg.Docker.Enabled {
		dockerClient := docker.NewClient(cfg.Docker.SocketPath)
		dockerWatcher := docker.NewWatcher(dockerClient, r, func(logEntry docker.LogEntry) {
			hub.broadcastJSON(map[string]any{
				"type":      "log",
				"container": logEntry.Container,
				"stream":    logEntry.Stream,
				"message":   logEntry.Message,
				"timestamp": logEntry.Timestamp,
			})
		}, func(action string, cr *docker.ContainerRoute) {
			if cr != nil && cr.Target != nil {
				engine.Health.RegisterTarget(cr.Target, "/health")
			}
		})

		_ = dockerWatcher.Start(ctx)
	}

	// 5. Initialize Public Ingress Tunnel (if enabled)
	if cfg.Tunnel.Enabled {
		tunnelClient := tunnel.NewClient(
			cfg.Tunnel.ServerURL,
			cfg.Tunnel.Subdomain,
			cfg.Tunnel.AuthToken,
			fmt.Sprintf("http://localhost:%d", cfg.Server.ProxyPort),
		)
		_ = tunnelClient.Start(ctx)
	}

	// 6. Start L7 Reverse Proxy Listener
	proxyAddr := fmt.Sprintf(":%d", cfg.Server.ProxyPort)
	proxyServer := &http.Server{
		Addr:              proxyAddr,
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	go func() {
		if err := proxyServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Proxy server failed: %v", err)
		}
	}()

	// 7. Start Developer Cockpit & Real-Time Inspection Server
	dashboardMux := http.NewServeMux()
	authToken := strings.TrimSpace(cfg.Security.AuthToken)
	dashboardBind := strings.TrimSpace(cfg.Security.DashboardBind)
	if dashboardBind == "" {
		dashboardBind = "127.0.0.1"
	}
	if !isLocalBindAddress(dashboardBind) && authToken == "" {
		log.Fatalf("dashboard_bind=%q requires security.auth_token to be set", dashboardBind)
	}
	requireDashboardAuth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			if authToken == "" {
				if !isLocalRequest(req) {
					http.Error(w, "dashboard access denied", http.StatusForbidden)
					return
				}
			} else if !isAuthorizedDashboardRequest(req, authToken) {
				w.Header().Set("WWW-Authenticate", "Token realm=\"devhub-dashboard\"")
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, req)
		}
	}
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return isAllowedDashboardOrigin(r, cfg.Security.AllowedOrigins)
		},
	}

	// WebSocket Live Stream Hub
	dashboardMux.HandleFunc("/ws", requireDashboardAuth(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}

		client := &Client{
			conn: conn,
			send: make(chan []byte, 256),
		}
		hub.register <- client

		go func() {
			defer func() {
				hub.unregister <- client
				conn.Close()
			}()
			for message := range client.send {
				if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
					break
				}
			}
		}()

		// Send buffer history on connect
		history := ring.GetAll()
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
	}))

	// Upstream Health Snapshot API
	dashboardMux.HandleFunc("/api/health", requireDashboardAuth(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(engine.Health.GetAllStatus())
	}))

	// Mock Rules API
	dashboardMux.HandleFunc("/api/mocks", requireDashboardAuth(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.Method == http.MethodPost {
			var rule mock.MockRule
			if err := json.NewDecoder(req.Body).Decode(&rule); err == nil {
				engine.Mocks.SetRule(rule)
			}
		}
		json.NewEncoder(w).Encode(engine.Mocks.ListRules())
	}))

	// Mock Toggle API
	dashboardMux.HandleFunc("/api/mocks/toggle", requireDashboardAuth(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Prefix  string `json:"prefix"`
			Enabled bool   `json:"enabled"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err == nil {
			engine.Mocks.ToggleRule(payload.Prefix, payload.Enabled)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))

	// 1-Click Request Replay API
	dashboardMux.HandleFunc("/api/replay", requireDashboardAuth(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var replayReq proxy.ReplayRequest
		if err := json.NewDecoder(req.Body).Decode(&replayReq); err != nil {
			http.Error(w, "Invalid JSON replay request", http.StatusBadRequest)
			return
		}

		res, err := replayDispatcher.Execute(replayReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	}))

	// Trace Waterfall API
	dashboardMux.HandleFunc("/api/waterfall", requireDashboardAuth(func(w http.ResponseWriter, req *http.Request) {
		traceID := req.URL.Query().Get("trace_id")
		w.Header().Set("Content-Type", "application/json")
		waterfall := tracing.BuildWaterfall(ring.GetAll(), traceID)
		json.NewEncoder(w).Encode(waterfall)
	}))

	// Traces Snapshot API
	dashboardMux.HandleFunc("/api/traces", requireDashboardAuth(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ring.GetAll())
	}))

	// Embedded Developer Cockpit Single-Page App
	dashboardMux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(ui.DashboardHTML))
	})

	dashAddr := net.JoinHostPort(dashboardBind, strconv.Itoa(cfg.Server.DashboardPort))
	dashServer := &http.Server{
		Addr:              dashAddr,
		Handler:           dashboardMux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	go func() {
		if err := dashServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Dashboard server failed: %v", err)
		}
	}()

	// 8. Graceful Shutdown Listener
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Printf("\n%s🛑 Shutting down DevHub mesh gracefully...%s\n", colYellow, colReset)
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()

	_ = proxyServer.Shutdown(shutdownCtx)
	_ = dashServer.Shutdown(shutdownCtx)
	fmt.Printf("%s✨ DevHub stopped successfully.%s\n", colGreen, colReset)
}

func isLocalBindAddress(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return true
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isLocalRequest(req *http.Request) bool {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isAuthorizedDashboardRequest(req *http.Request, token string) bool {
	authHeader := strings.TrimSpace(req.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return strings.TrimSpace(authHeader[7:]) == token
	}
	if strings.HasPrefix(strings.ToLower(authHeader), "token ") {
		return strings.TrimSpace(authHeader[6:]) == token
	}
	return strings.TrimSpace(req.Header.Get("X-DevHub-Token")) == token
}

func isAllowedDashboardOrigin(req *http.Request, allowedOrigins []string) bool {
	origin := strings.TrimSpace(req.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	originURL, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if len(allowedOrigins) == 0 {
		host := strings.ToLower(originURL.Hostname())
		return host == "localhost" || host == "127.0.0.1" || host == "::1"
	}
	for _, allowed := range allowedOrigins {
		allowed = strings.TrimSpace(strings.ToLower(allowed))
		if allowed == "" {
			continue
		}
		if strings.EqualFold(origin, allowed) {
			return true
		}
	}
	return false
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
	fmt.Printf("  %s• Developer Cockpit%s : %shttp://localhost:%d%s\n", colBold, colReset, colBold+colEmerald, cfg.Server.DashboardPort, colReset)
	fmt.Printf("  %s• WebSocket Stream%s  : %sws://localhost:%d/ws%s\n", colBold, colReset, colBold+colPurple, cfg.Server.DashboardPort, colReset)
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

			// Extract first host port (e.g. "8085:8080" -> 8085)
			hostPort := parseHostPort(svc.Ports[0])
			if hostPort == 0 {
				continue
			}

			targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", hostPort))
			routePrefix := "/api/" + serviceName
			if serviceName == "auth" {
				routePrefix = "/auth"
			} else if serviceName == "gateway" {
				r.Add("/api/v1", targetURL, false)
			}
			r.Add(routePrefix, targetURL, true)
			if health != nil {
				health.RegisterTarget(targetURL, "/health")
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

func autoDetectLocalServices(r *router.Router, health *proxy.HealthMonitor) {
	servicesDir := "services"
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return
	}

	basePort := 5001
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		serviceName := entry.Name()
		serviceFolder := filepath.Join(servicesDir, serviceName)

		// Try finding port from .env or package.json
		port := detectServicePort(serviceFolder, basePort)
		basePort++

		targetURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", port))
		routePrefix := "/api/" + serviceName
		if serviceName == "auth" {
			routePrefix = "/auth"
		}
		r.Add(routePrefix, targetURL, true)
		if health != nil {
			health.RegisterTarget(targetURL, "/health")
		}
	}
}

func detectServicePort(folder string, defaultPort int) int {
	portRegex := regexp.MustCompile(`(?i)PORT\s*=\s*(\d+)`)

	// 1. Try .env
	envPath := filepath.Join(folder, ".env")
	if data, err := os.ReadFile(envPath); err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			matches := portRegex.FindStringSubmatch(scanner.Text())
			if len(matches) > 1 {
				if p, err := strconv.Atoi(matches[1]); err == nil && p > 0 {
					return p
				}
			}
		}
	}
	return defaultPort
}
