package config

import (
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"

	"devhub/pkg/mock"
	"gopkg.in/yaml.v3"
)

// Config represents the complete DevHub daemon configuration.
type Config struct {
	Version   string                    `yaml:"version"`
	Server    ServerConfig              `yaml:"server"`
	Ingress   IngressConfig             `yaml:"ingress"` // Alternative shorthand
	Docker    DockerConfig              `yaml:"docker"`
	Tunnel    TunnelConfig              `yaml:"tunnel"`
	Tracing   TracingConfig             `yaml:"tracing"`
	Routes    []RouteConfig             `yaml:"routes"`
	Overrides map[string]OverrideConfig `yaml:"overrides"`
}

// IngressConfig provides shorthand syntax for port configuration.
type IngressConfig struct {
	Port      int `yaml:"port"`
	Dashboard int `yaml:"dashboard"`
}

// OverrideConfig provides fine-grained per-route customization and mock response overrides.
type OverrideConfig struct {
	Target      string        `yaml:"target"`
	StripPrefix bool          `yaml:"strip_prefix"`
	Mock        mock.MockRule `yaml:"mock"`
}

// ServerConfig configures proxy, dashboard HTTP ports, binding interfaces, and security options.
type ServerConfig struct {
	ProxyPort             int    `yaml:"proxy_port"`
	DashboardPort         int    `yaml:"dashboard_port"`
	Host                  string `yaml:"host"`
	DashboardHost         string `yaml:"dashboard_host"`
	DashboardAuthToken    string `yaml:"dashboard_auth_token"`
	EnforceCircuitBreaker bool   `yaml:"enforce_circuit_breaker"`
}

// DockerConfig configures Docker Engine auto-discovery.
type DockerConfig struct {
	Enabled    bool   `yaml:"enabled"`
	SocketPath string `yaml:"socket_path"`
	Network    string `yaml:"network"`
	PollLogs   bool   `yaml:"poll_logs"`
}

// TunnelConfig configures public ingress tunneling.
type TunnelConfig struct {
	Enabled   bool   `yaml:"enabled"`
	ServerURL string `yaml:"server_url"`
	Subdomain string `yaml:"subdomain"`
	AuthToken string `yaml:"auth_token"`
}

// TracingConfig controls W3C distributed tracing and PII redaction.
type TracingConfig struct {
	Enabled   bool `yaml:"enabled"`
	RedactPII bool `yaml:"redact_pii"`
	RingSize  int  `yaml:"ring_size"`
}

// RouteConfig defines a static path route.
type RouteConfig struct {
	Prefix          string `yaml:"prefix"`
	Target          string `yaml:"target"`
	StripPrefix     bool   `yaml:"strip_prefix"`
	HealthCheckPath string `yaml:"health_check_path"`
	TimeoutMs       int64  `yaml:"timeout_ms"`
}

// ValidateRouteTarget ensures the target URL has a valid scheme (http/https) and non-empty host.
func ValidateRouteTarget(target string) error {
	u, err := url.Parse(target)
	if err != nil {
		return fmt.Errorf("invalid target URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("target URL scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("target URL host cannot be empty")
	}
	return nil
}

// DefaultConfig returns production-ready default settings.
func DefaultConfig() *Config {
	var defaultDockerSocket string
	if runtime.GOOS == "windows" {
		defaultDockerSocket = `\\.\pipe\docker_engine`
	} else {
		defaultDockerSocket = `/var/run/docker.sock`
	}

	return &Config{
		Version: "1.0",
		Server: ServerConfig{
			ProxyPort:             4000,
			DashboardPort:         4040,
			Host:                  "0.0.0.0",
			DashboardHost:         "127.0.0.1", // Secure localhost binding by default
			DashboardAuthToken:    "",
			EnforceCircuitBreaker: false, // Passive health observation by default
		},
		Docker: DockerConfig{
			Enabled:    true,
			SocketPath: defaultDockerSocket,
			PollLogs:   true,
		},
		Tunnel: TunnelConfig{
			Enabled:   false,
			ServerURL: "wss://relay.devhub.live/tunnel",
		},
		Tracing: TracingConfig{
			Enabled:   true,
			RedactPII: true,
			RingSize:  500,
		},
		Overrides: make(map[string]OverrideConfig),
		Routes: []RouteConfig{
			{
				Prefix:          "/auth",
				Target:          "http://localhost:5001",
				StripPrefix:     false,
				HealthCheckPath: "/health",
				TimeoutMs:       5000,
			},
			{
				Prefix:          "/api/v1",
				Target:          "http://localhost:5002",
				StripPrefix:     true,
				HealthCheckPath: "/health",
				TimeoutMs:       10000,
			},
			{
				Prefix:          "/",
				Target:          "http://localhost:3000",
				StripPrefix:     false,
				HealthCheckPath: "/",
				TimeoutMs:       5000,
			},
		},
	}
}

// LoadConfig reads config from a file or falls back to defaults.
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		candidates := []string{".devhub.yaml", ".devhub.yml", "devhub.yaml", "devhub.yml"}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				path = c
				break
			}
		}
	}

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML config: %w", err)
	}

	// Handle shorthand Ingress config
	if cfg.Ingress.Port > 0 {
		cfg.Server.ProxyPort = cfg.Ingress.Port
	}
	if cfg.Ingress.Dashboard > 0 {
		cfg.Server.DashboardPort = cfg.Ingress.Dashboard
	}

	// Validate and register routes
	for i, r := range cfg.Routes {
		if r.Prefix == "" {
			return nil, fmt.Errorf("route #%d prefix cannot be empty", i)
		}
		if !strings.HasPrefix(r.Prefix, "/") {
			cfg.Routes[i].Prefix = "/" + r.Prefix
		}
		if r.Target == "" {
			return nil, fmt.Errorf("route #%d target cannot be empty", i)
		}
		if err := ValidateRouteTarget(r.Target); err != nil {
			return nil, fmt.Errorf("route #%d invalid target: %w", i, err)
		}
	}

	// Process overrides into routes
	for prefix, ov := range cfg.Overrides {
		if ov.Mock.StatusCode <= 0 && ov.Mock.Status > 0 {
			ov.Mock.StatusCode = ov.Mock.Status
			cfg.Overrides[prefix] = ov
		}
		cleanPrefix := "/" + strings.Trim(prefix, "/")
		if ov.Target != "" {
			if err := ValidateRouteTarget(ov.Target); err != nil {
				return nil, fmt.Errorf("override %q invalid target: %w", prefix, err)
			}
			found := false
			for idx, r := range cfg.Routes {
				if r.Prefix == cleanPrefix {
					cfg.Routes[idx].Target = ov.Target
					cfg.Routes[idx].StripPrefix = ov.StripPrefix
					found = true
					break
				}
			}
			if !found {
				cfg.Routes = append(cfg.Routes, RouteConfig{
					Prefix:      cleanPrefix,
					Target:      ov.Target,
					StripPrefix: ov.StripPrefix,
				})
			}
		}
	}

	if cfg.Tracing.RingSize <= 0 {
		cfg.Tracing.RingSize = 500
	}
	if cfg.Server.ProxyPort <= 0 || cfg.Server.ProxyPort >= 65536 {
		cfg.Server.ProxyPort = 4000
	}
	if cfg.Server.DashboardPort <= 0 || cfg.Server.DashboardPort >= 65536 {
		cfg.Server.DashboardPort = 4040
	}
	if cfg.Server.DashboardHost == "" {
		cfg.Server.DashboardHost = "127.0.0.1"
	}

	return cfg, nil
}
