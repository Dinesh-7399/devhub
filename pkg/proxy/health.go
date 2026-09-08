package proxy

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type HealthState string

const (
	HealthUnknown   HealthState = "UNKNOWN"
	HealthHealthy   HealthState = "HEALTHY"
	HealthDegraded  HealthState = "DEGRADED"
	HealthUnhealthy HealthState = "UNHEALTHY"
)

// TargetHealth tracks the health status of an upstream microservice.
type TargetHealth struct {
	Name             string    `json:"name"`
	URL              string    `json:"url"`
	Host             string    `json:"host"`
	Healthy          bool      `json:"healthy"`
	State            HealthState `json:"state"`
	ConsecutiveFails int       `json:"consecutive_fails"`
	LastChecked      time.Time `json:"last_checked"`
	LatencyMs        int64     `json:"latency_ms"`
	HealthPath       string    `json:"health_path"`
}

// HealthMonitor conducts active and passive health checks on registered upstreams.
type HealthMonitor struct {
	mu             sync.RWMutex
	targets        map[string]*TargetHealth
	client         *http.Client
	onStatusChange func(th TargetHealth)
	enforce        bool
}

// NewHealthMonitor initializes an upstream health monitor.
func NewHealthMonitor() *HealthMonitor {
	return &HealthMonitor{
		targets: make(map[string]*TargetHealth),
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

// SetOnStatusChange sets the callback for broadcasting health state flips to WebSockets.
func (h *HealthMonitor) SetOnStatusChange(fn func(TargetHealth)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onStatusChange = fn
}

// RegisterTarget adds a target URL to active health monitoring.
func (h *HealthMonitor) RegisterTarget(u *url.URL, healthPath string) {
	if u == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	key := u.String()
	if _, exists := h.targets[key]; !exists {
		if healthPath == "" {
			healthPath = "/health"
		}
		h.targets[key] = &TargetHealth{
			Name:       u.Host,
			URL:        u.String(),
			Host:       u.Host,
			Healthy:    true,
			State:      HealthUnknown,
			HealthPath: healthPath,
		}
	}
}

func (h *HealthMonitor) SetEnforcement(enforce bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.enforce = enforce
}

// IsHealthy returns whether a target URL is considered available.
func (h *HealthMonitor) IsHealthy(u *url.URL) bool {
	if u == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	if target, exists := h.targets[u.String()]; exists {
		if !h.enforce {
			return true
		}
		return target.Healthy
	}
	return true // Default to healthy if unmonitored
}

// RecordSuccess registers a successful response from an upstream target.
func (h *HealthMonitor) RecordSuccess(u *url.URL) {
	if u == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if target, exists := h.targets[u.String()]; exists {
		wasUnhealthy := !target.Healthy
		target.ConsecutiveFails = 0
		target.Healthy = true
		target.State = HealthHealthy
		target.LastChecked = time.Now()
		if wasUnhealthy && h.onStatusChange != nil {
			go h.onStatusChange(*target)
		}
	}
}

// RecordFailure registers a failure or connection error, tripping the circuit breaker after 3 failures.
func (h *HealthMonitor) RecordFailure(u *url.URL) {
	if u == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if target, exists := h.targets[u.String()]; exists {
		wasHealthy := target.Healthy
		target.ConsecutiveFails++
		target.LastChecked = time.Now()
		if target.ConsecutiveFails >= 3 {
			target.Healthy = false
			target.State = HealthUnhealthy
			if wasHealthy && h.onStatusChange != nil {
				go h.onStatusChange(*target)
			}
		} else {
			target.State = HealthDegraded
		}
	}
}

// GetAllStatus returns a snapshot of all monitored services for the UI.
func (h *HealthMonitor) GetAllStatus() []TargetHealth {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make([]TargetHealth, 0, len(h.targets))
	for _, t := range h.targets {
		out = append(out, *t)
	}
	return out
}

// Start runs background active probing for all registered targets every 3 seconds.
func (h *HealthMonitor) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		// Initial probe
		h.probeAll()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.probeAll()
			}
		}
	}()
}

func (h *HealthMonitor) probeAll() {
	h.mu.RLock()
	targetsList := make([]*TargetHealth, 0, len(h.targets))
	for _, t := range h.targets {
		targetsList = append(targetsList, t)
	}
	h.mu.RUnlock()

	for _, target := range targetsList {
		start := time.Now()
		isOnline := false

		// 1. First try fast TCP socket probe (works for HTTP, gRPC, and DBs)
		host := target.Host
		if !hasPort(host) {
			host = host + ":80"
		}

		conn, err := net.DialTimeout("tcp", host, 1*time.Second)
		latency := time.Since(start).Milliseconds()

		if err == nil {
			conn.Close()
			isOnline = true
		} else {
			// 2. Try HTTP check if TCP failed
			checkURL := target.URL + target.HealthPath
			resp, httpErr := h.client.Get(checkURL)
			if httpErr == nil {
				resp.Body.Close()
				if resp.StatusCode < 500 {
					isOnline = true
				}
			}
		}

		h.mu.Lock()
		wasState := target.Healthy
		target.LastChecked = time.Now()
		target.LatencyMs = latency

		if isOnline {
			target.ConsecutiveFails = 0
			target.Healthy = true
			target.State = HealthHealthy
		} else {
			target.ConsecutiveFails++
			if target.ConsecutiveFails >= 2 {
				target.Healthy = false
				target.State = HealthUnhealthy
			} else {
				target.State = HealthDegraded
			}
		}

		stateChanged := (wasState != target.Healthy)
		snapshot := *target
		h.mu.Unlock()

		if stateChanged && h.onStatusChange != nil {
			h.onStatusChange(snapshot)
		}
	}
}

func hasPort(host string) bool {
	_, _, err := net.SplitHostPort(host)
	return err == nil
}
