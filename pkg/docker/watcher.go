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
	client     *Client
	router     *router.Router
	mu         sync.RWMutex
	routes     map[string]*ContainerRoute
	logStreams map[string]context.CancelFunc
	onLog      func(entry LogEntry)
	onRoute    func(action string, r *ContainerRoute)
}

// NewWatcher initializes a container discovery watcher.
func NewWatcher(client *Client, r *router.Router, onLog func(LogEntry), onRoute func(string, *ContainerRoute)) *Watcher {
	return &Watcher{
		client:     client,
		router:     r,
		routes:     make(map[string]*ContainerRoute),
		logStreams: make(map[string]context.CancelFunc),
		onLog:      onLog,
		onRoute:    onRoute,
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
	routePrefix := c.Labels["devhub.route"]
	if routePrefix == "" {
		routePrefix = c.Labels["devhub.path"]
	}

	// If no explicit route label, check if container name has a default route
	name := strings.TrimPrefix(strings.Join(c.Names, ","), "/")
	if routePrefix == "" && len(c.Names) > 0 {
		cleanName := strings.TrimPrefix(c.Names[0], "/")
		if cleanName != "" && !strings.HasPrefix(cleanName, "devhub") {
			// Auto-route by container name e.g. /service-name
			routePrefix = "/" + cleanName
		}
	}

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

	w.mu.Lock()
	w.routes[c.ID] = cRoute
	w.mu.Unlock()

	w.router.Add(routePrefix, targetURL, stripPrefix)
	fmt.Printf("🐳 [Docker Discovery] Mounted: %s -> %s (Container: %s)\n", routePrefix, targetURL.String(), name)

	if w.onRoute != nil {
		w.onRoute("mount", cRoute)
	}

	// Start live log streaming for container
	w.startLogStream(ctx, c.ID, name)
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
