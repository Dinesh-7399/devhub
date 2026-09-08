package docker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"devhub/pkg/router"
)

// ContainerRoute holds active routing information for a Docker container.
type ContainerRoute struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Prefix      string   `json:"prefix"`
	Target      *url.URL `json:"target"`
	StripPrefix bool     `json:"strip_prefix"`
	Port        int      `json:"port"`
}

// Watcher supervises Docker container lifecycle and maps routes automatically.
type Watcher struct {
	client       *Client
	router       *router.Router
	mode         string // "strict", "opt_in", "automatic"
	autoRouteAll bool
	mu           sync.RWMutex
	routes       map[string]*ContainerRoute
	logStreams   map[string]context.CancelFunc
	onLog        func(entry LogEntry)
	onRoute      func(action string, r *ContainerRoute)
}

// NewWatcher initializes a container discovery watcher.
func NewWatcher(client *Client, r *router.Router, onLog func(LogEntry), onRoute func(string, *ContainerRoute)) *Watcher {
	return &Watcher{
		client:       client,
		router:       r,
		mode:         "strict", // Strict opt-in devhub.route label required by default
		autoRouteAll: false,
		routes:       make(map[string]*ContainerRoute),
		logStreams:   make(map[string]context.CancelFunc),
		onLog:        onLog,
		onRoute:      onRoute,
	}
}

// SetMode configures the container discovery mode ("strict", "opt_in", "automatic").
func (w *Watcher) SetMode(mode string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mode = mode
}

// SetAutoRouteAll controls whether unlabeled containers should be automatically routed by container name.
func (w *Watcher) SetAutoRouteAll(enable bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.autoRouteAll = enable
	if enable {
		w.mode = "automatic"
	}
}

// Start begins initial discovery and long-running event listening.
func (w *Watcher) Start(ctx context.Context) error {
	// 1. Initial container sweep
	if err := w.syncContainers(ctx); err != nil {
		// Log warning but continue so non-docker routes work fine
		fmt.Printf("⚠️ Docker discovery warning: %v\n", err)
	}

	// 2. Start event loop in background
	go w.eventLoop(ctx)

	return nil
}

// syncContainers scans all running containers, mounts active routes, and unmounts stale ones.
func (w *Watcher) syncContainers(ctx context.Context) error {
	if w.client == nil {
		return nil
	}
	containers, err := w.client.ListContainers(ctx)
	if err != nil {
		return err
	}

	activeIDs := make(map[string]bool, len(containers))
	for _, c := range containers {
		activeIDs[c.ID] = true
		w.registerContainer(ctx, c)
	}

	// Reconcile: identify any routes for containers that are no longer active
	w.mu.RLock()
	var deadIDs []string
	for id := range w.routes {
		if !activeIDs[id] {
			deadIDs = append(deadIDs, id)
		}
	}
	w.mu.RUnlock()

	for _, deadID := range deadIDs {
		w.unregisterContainer(deadID)
	}

	return nil
}

func (w *Watcher) registerContainer(ctx context.Context, c ContainerInfo) {
	w.mu.RLock()
	mode := w.mode
	w.mu.RUnlock()
	if mode == "" {
		mode = "strict"
	}

	routePrefix := c.Labels["devhub.route"]
	if routePrefix == "" {
		routePrefix = c.Labels["devhub.path"]
	}

	// In strict mode, explicit devhub.route or devhub.path is strictly required
	name := strings.TrimPrefix(strings.Join(c.Names, ","), "/")
	if routePrefix == "" && len(c.Names) > 0 {
		cleanName := strings.TrimPrefix(c.Names[0], "/")
		if cleanName != "" && !strings.HasPrefix(cleanName, "devhub") {
			switch mode {
			case "strict":
				// Strictly requires explicit route label; do not auto-name
			case "opt_in":
				// Allows devhub.enable=true with container name derived route
				if c.Labels["devhub.enable"] == "true" {
					routePrefix = "/" + cleanName
				}
			case "automatic":
				// Automatic mode: route any active container
				routePrefix = "/" + cleanName
			}
		}
	}

	// Container is not opted in or has no route prefix
	if routePrefix == "" {
		return
	}

	stripPrefix := c.Labels["devhub.strip_prefix"] == "true"

	// Resolve target port
	targetPort := 80
	if pStr, exists := c.Labels["devhub.port"]; exists {
		if p, err := strconv.Atoi(pStr); err == nil && p > 0 {
			targetPort = p
		}
	} else if len(c.Ports) > 0 {
		if c.Ports[0].PublicPort > 0 {
			targetPort = c.Ports[0].PublicPort
		} else if c.Ports[0].PrivatePort > 0 {
			targetPort = c.Ports[0].PrivatePort
		}
	}

	targetHost := "localhost"
	if explicitHost, exists := c.Labels["devhub.host"]; exists && explicitHost != "" {
		targetHost = explicitHost
	}

	targetURL, err := url.Parse(fmt.Sprintf("http://%s:%d", targetHost, targetPort))
	if err != nil {
		return
	}

	cRoute := &ContainerRoute{
		ID:          c.ID,
		Name:        name,
		Prefix:      routePrefix,
		Target:      targetURL,
		StripPrefix: stripPrefix,
		Port:        targetPort,
	}

	// Idempotency check: if container already registered and route parameters are unchanged, skip tree update
	w.mu.Lock()
	existing, exists := w.routes[c.ID]
	if exists {
		if existing.Prefix == routePrefix && existing.Target.String() == targetURL.String() && existing.StripPrefix == stripPrefix {
			w.mu.Unlock()
			return // Idempotent: completely unchanged
		}
		// Prefix changed: prune the old route first
		if existing.Prefix != routePrefix {
			w.router.Remove(existing.Prefix)
		}
	}
	w.routes[c.ID] = cRoute
	_, logStreaming := w.logStreams[c.ID]
	w.mu.Unlock()

	w.router.Add(routePrefix, targetURL, stripPrefix)
	fmt.Printf("🐳 [Docker Discovery] Mounted: %s -> %s (Container: %s)\n", routePrefix, targetURL.String(), name)

	if w.onRoute != nil {
		w.onRoute("mount", cRoute)
	}

	// Start live log streaming for container only if not already active
	if !logStreaming {
		w.startLogStream(ctx, c.ID, name)
	}
}

func (w *Watcher) unregisterContainer(containerID string) {
	w.mu.Lock()
	cRoute, exists := w.routes[containerID]
	if exists {
		delete(w.routes, containerID)
	}
	if cancel, ok := w.logStreams[containerID]; ok {
		cancel()
		delete(w.logStreams, containerID)
	}
	w.mu.Unlock()

	if exists {
		w.router.Remove(cRoute.Prefix)
		fmt.Printf("🐳 [Docker Discovery] Unmounted route for dead container %s (%s)\n", cRoute.Name, cRoute.Prefix)
		if w.onRoute != nil {
			w.onRoute("unmount", cRoute)
		}
	}
}

func (w *Watcher) startLogStream(ctx context.Context, containerID, name string) {
	if w.client == nil {
		return
	}
	w.mu.Lock()
	if _, active := w.logStreams[containerID]; active {
		w.mu.Unlock()
		return
	}
	logCtx, cancel := context.WithCancel(ctx)
	w.logStreams[containerID] = cancel
	w.mu.Unlock()

	go func() {
		defer func() {
			w.mu.Lock()
			delete(w.logStreams, containerID)
			w.mu.Unlock()
		}()

		reader, err := w.client.StreamLogs(logCtx, containerID, true, 50)
		if err != nil {
			return
		}
		defer reader.Close()

		_ = DemuxStream(reader, func(streamType int, payload []byte) {
			streamName := "stdout"
			if streamType == StreamTypeStderr {
				streamName = "stderr"
			}
			msg := strings.TrimRight(string(payload), "\r\n")
			if msg != "" && w.onLog != nil {
				w.onLog(LogEntry{
					Container: name,
					Stream:    streamName,
					Message:   msg,
					Timestamp: time.Now().Format("15:04:05.000"),
				})
			}
		})
	}()
}

func (w *Watcher) eventLoop(ctx context.Context) {
	if w.client == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		stream, err := w.client.StreamEvents(ctx)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}

		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			line := scanner.Bytes()
			var ev DockerEvent
			if err := json.Unmarshal(line, &ev); err != nil {
				continue
			}

			if ev.Type == "container" {
				switch ev.Action {
				case "start":
					// Re-scan container
					_ = w.syncContainers(ctx)
				case "die", "kill", "stop", "destroy":
					w.unregisterContainer(ev.Actor.ID)
				}
			}
		}
		stream.Close()
		time.Sleep(1 * time.Second)
		_ = w.syncContainers(ctx)
	}
}

// GetRoutes returns a snapshot of all actively discovered container routes.
func (w *Watcher) GetRoutes() []*ContainerRoute {
	w.mu.RLock()
	defer w.mu.RUnlock()

	list := make([]*ContainerRoute, 0, len(w.routes))
	for _, r := range w.routes {
		list = append(list, r)
	}
	return list
}

// Stop terminates all active container log streams.
func (w *Watcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cancel := range w.logStreams {
		cancel()
	}
	w.logStreams = make(map[string]context.CancelFunc)
}

