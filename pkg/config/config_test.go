package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig("non_existent_file.yaml")
	if err != nil {
		t.Fatalf("unexpected error loading non-existent config: %v", err)
	}

	if cfg.Server.ProxyPort != 4000 {
		t.Errorf("expected default proxy port 4000, got %d", cfg.Server.ProxyPort)
	}
	if cfg.Server.DashboardPort != 4040 {
		t.Errorf("expected default dashboard port 4040, got %d", cfg.Server.DashboardPort)
	}
	if !cfg.Docker.Enabled {
		t.Errorf("expected docker to be enabled by default")
	}
	if cfg.Security.DashboardBind != "127.0.0.1" {
		t.Errorf("expected default dashboard bind 127.0.0.1, got %q", cfg.Security.DashboardBind)
	}
	if cfg.Security.AllowPrivateEgress {
		t.Errorf("expected private egress to be disabled by default")
	}
	if len(cfg.Routes) == 0 {
		t.Errorf("expected default sample routes")
	}
}

func TestLoadConfig_DotDevhubYamlWithOverrides(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, ".devhub.yaml")

	yamlContent := `
version: "1.0"
ingress:
  port: 8000
  dashboard: 8080

overrides:
  /api/payment:
    target: "http://localhost:50067"
    strip_prefix: true
    mock:
      enabled: true
      mode: "on_error"
      status: 200
      body: '{"status":"mocked_paid"}'
`
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load valid config: %v", err)
	}

	if cfg.Server.ProxyPort != 8000 {
		t.Errorf("expected proxy port 8000, got %d", cfg.Server.ProxyPort)
	}
	if cfg.Server.DashboardPort != 8080 {
		t.Errorf("expected dashboard port 8080, got %d", cfg.Server.DashboardPort)
	}

	if len(cfg.Overrides) != 1 {
		t.Fatalf("expected 1 override, got %d", len(cfg.Overrides))
	}

	ov, exists := cfg.Overrides["/api/payment"]
	if !exists {
		t.Fatalf("expected override for /api/payment")
	}
	if !ov.Mock.Enabled || ov.Mock.StatusCode != 200 {
		t.Errorf("unexpected mock settings in override: %+v", ov.Mock)
	}
}
