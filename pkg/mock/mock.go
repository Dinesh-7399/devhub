package mock

import (
	"strings"
	"sync"
)

const (
	ModeAlways  = "always"
	ModeOnError = "on_error"
)

// MockRule defines mock response configuration for a route prefix.
type MockRule struct {
	Prefix     string            `json:"prefix" yaml:"prefix"`
	Enabled    bool              `json:"enabled" yaml:"enabled"`
	Mode       string            `json:"mode" yaml:"mode"` // "always" or "on_error"
	StatusCode int               `json:"status_code" yaml:"status_code"`
	Status     int               `json:"status,omitempty" yaml:"status,omitempty"` // alias for status_code
	Headers    map[string]string `json:"headers" yaml:"headers"`
	Body       string            `json:"body" yaml:"body"`
	DelayMs    int64             `json:"delay_ms" yaml:"delay_ms"`
}

// Manager stores and manages mock rules thread-safely.
type Manager struct {
	mu    sync.RWMutex
	rules map[string]*MockRule
}

// NewManager constructs a MockManager.
func NewManager() *Manager {
	return &Manager{
		rules: make(map[string]*MockRule),
	}
}

// SetRule adds or updates a mock rule.
func (m *Manager) SetRule(rule MockRule) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cleanPrefix := "/" + strings.Trim(rule.Prefix, "/")
	if rule.Prefix == "/" {
		cleanPrefix = "/"
	}

	if rule.StatusCode <= 0 && rule.Status > 0 {
		rule.StatusCode = rule.Status
	}
	if rule.StatusCode <= 0 {
		rule.StatusCode = 200
	}
	if rule.Mode == "" {
		rule.Mode = ModeAlways
	}
	if rule.Headers == nil {
		rule.Headers = map[string]string{
			"Content-Type": "application/json",
		}
	}
	if rule.Body == "" {
		rule.Body = `{"status":"mocked_ok","message":"DevHub Mock Response"}`
	}

	rule.Prefix = cleanPrefix
	m.rules[cleanPrefix] = &rule
}

// GetRule matches path against registered mock prefixes (longest prefix match).
func (m *Manager) GetRule(path string) (*MockRule, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cleanPath := "/" + strings.Trim(path, "/")
	if path == "/" {
		cleanPath = "/"
	}

	var bestMatch *MockRule
	bestLen := -1

	for prefix, rule := range m.rules {
		if !rule.Enabled {
			continue
		}
		if prefix == "/" || strings.HasPrefix(cleanPath, prefix) {
			if len(prefix) > bestLen {
				bestMatch = rule
				bestLen = len(prefix)
			}
		}
	}

	if bestMatch != nil {
		return bestMatch, true
	}
	return nil, false
}

// ToggleRule toggles mock state for a prefix.
func (m *Manager) ToggleRule(prefix string, enabled bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	cleanPrefix := "/" + strings.Trim(prefix, "/")
	if prefix == "/" {
		cleanPrefix = "/"
	}

	if rule, exists := m.rules[cleanPrefix]; exists {
		rule.Enabled = enabled
		return true
	}

	// Create default enabled rule if none existed
	m.rules[cleanPrefix] = &MockRule{
		Prefix:     cleanPrefix,
		Enabled:    enabled,
		Mode:       ModeAlways,
		StatusCode: 200,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       `{"status":"mocked_ok"}`,
	}
	return true
}

// ListRules returns all configured mock rules.
func (m *Manager) ListRules() []MockRule {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]MockRule, 0, len(m.rules))
	for _, r := range m.rules {
		if r.StatusCode <= 0 && r.Status > 0 {
			r.StatusCode = r.Status
		}
		out = append(out, *r)
	}
	return out
}
