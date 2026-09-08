package proxy

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Health status classifications
const (
	StatusHealthy   = "HEALTHY"
	StatusDegraded  = "DEGRADED"
	StatusUnhealthy = "UNHEALTHY"
	StatusUnknown   = "UNKNOWN"
)

// TargetHealth tracks the health status of an upstream microservice.
type TargetHealth struct {
	Name             string    `json:"name"`
	URL              string    `json:"url"`
	Host             string    `json:"host"`
	Healthy          bool      `json:"healthy"`
	Status           string    `json:"status"` // HEALTHY, DEGRADED, UNHEALTHY, UNKNOWN
	ConsecutiveFails int       `json:"consecutive_fails"`
	LastChecked      time.Time `json:"last_checked"`
	LatencyMs        int64     `json:"latency_ms"`
	HealthPath       string    `json:"health_path"`
	Message          string    `json:"message,omitempty"`
}

// HealthMonitor conducts active and passive health checks on registered upstreams.
// By default, it operates in passive observation mode, updating telemetry without blocking traffic.
type HealthMonitor struct {
	mu                     sync.RWMutex
	targets                map[string]*TargetHealth
	client                 *http.Client
	onStatusChange         func(th TargetHealth)
	EnforceCircuitBreaker  bool // If false, health monitor observes without returning 503
}

// NewHealthMonitor initializes an upstream health monitor.
func NewHealthMonitor() *HealthMonitor {
	return &HealthMonitor{
		targets: make(map[string]*TargetHealth),
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		EnforceCircuitBreaker: false, // Default: passive observation only
	}
}

// SetCircuitBreakerEnforcement enables or disables active 503 blocking on unhealthy upstreams.
func (h *HealthMonitor) SetCircuitBreakerEnforcement(enabled bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.EnforceCircuitBreaker = enabled
}

// SetOnStatusChange sets the callback for broadcasting health state flips to WebSockets.
func (h *HealthMonitor) SetOnStatusChange(fn func(TargetHealth)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onStatusChange = fn
}

// RegisterTarget adds a target URL to health monitoring.
func (h *HealthMonitor) RegisterTarget(u *url.URL, healthPath string) {
	if u == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	key := u.String()
	if _, exists := h.targets[key]; !exists {
		// If no health path is provided, default to TCP liveness probe
		if healthPath == "" {
			healthPath = "tcp"
		}
		h.targets[key] = &TargetHealth{
			Name:       u.Host,
			URL:        u.String(),
			Host:       u.Host,
			Healthy:    true,
			Status:     StatusHealthy,
			HealthPath: healthPath,
		}
	}
}

// IsHealthy returns whether a target URL is considered available.
func (h *HealthMonitor) IsHealthy(u *url.URL) bool {
	if u == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	if target, exists := h.targets[u.String()]; exists {
		return target.Healthy
	}
	return true // Default to healthy if unmonitored
}

// IsCircuitBroken returns whether the circuit breaker should actively reject requests.
// Returns false if circuit breaker enforcement is disabled.
func (h *HealthMonitor) IsCircuitBroken(u *url.URL) bool {
	if u == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.EnforceCircuitBreaker {
		return false // Passive observation mode: never block
	}

	if target, exists := h.targets[u.String()]; exists {
		return !target.Healthy
	}
	return false
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
		target.Status = StatusHealthy
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
			target.Status = StatusUnhealthy
			if wasHealthy && h.onStatusChange != nil {
				go h.onStatusChange(*target)
			}
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

// Start runs background active probing for all registered targets.
func (h *HealthMonitor) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		h.probeAll(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.probeAll(ctx)
			}
		}
	}()
}

func (h *HealthMonitor) probeAll(ctx context.Context) {
	h.mu.RLock()
	targetsList := make([]*TargetHealth, 0, len(h.targets))
	for _, t := range h.targets {
		targetsList = append(targetsList, t)
	}
	h.mu.RUnlock()

	for _, target := range targetsList {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		var isSuccess bool
		var status string
		var msg string

		if target.HealthPath != "" && target.HealthPath != "tcp" {
			// Direct HTTP Health Probe (no redundant TCP pre-probe)
			checkURL := target.URL + target.HealthPath
			req, err := http.NewRequestWithContext(ctx, "GET", checkURL, nil)
			if err != nil {
				isSuccess = false
				status = StatusDegraded
				msg = err.Error()
			} else {
				resp, httpErr := h.client.Do(req)
				if httpErr != nil {
					isSuccess = false
					status = StatusDegraded
					msg = httpErr.Error()
				} else {
					resp.Body.Close()
					if resp.StatusCode >= 200 && resp.StatusCode < 400 {
						isSuccess = true
						status = StatusHealthy
					} else if resp.StatusCode == http.StatusNotFound {
						// 404 means service is alive, but specific health path is not mapped
						isSuccess = true
						status = StatusUnknown
						msg = "Service port active; health path returned 404"
					} else {
						// 5xx or error code
						isSuccess = false
						status = StatusDegraded
						msg = http.StatusText(resp.StatusCode)
					}
				}
			}
		} else {
			// TCP Socket Liveness Probe for targets without dedicated HTTP health paths
			host := target.Host
			if !hasPort(host) {
				host = host + ":80"
			}
			conn, tcpErr := net.DialTimeout("tcp", host, 1*time.Second)
			if tcpErr == nil {
				conn.Close()
				isSuccess = true
				status = StatusHealthy
			} else {
				isSuccess = false
				status = StatusDegraded
				msg = tcpErr.Error()
			}
		}

		latency := time.Since(start).Milliseconds()

		h.mu.Lock()
		wasState := target.Healthy
		target.LastChecked = time.Now()
		target.LatencyMs = latency
		target.Message = msg

		if isSuccess {
			target.ConsecutiveFails = 0
			target.Healthy = true
			target.Status = status
		} else {
			target.ConsecutiveFails++
			if target.ConsecutiveFails >= 3 {
				target.Healthy = false
				target.Status = StatusUnhealthy
			} else {
				target.Status = StatusDegraded
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
