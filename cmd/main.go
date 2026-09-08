package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"devhub/internal/app"
	"devhub/pkg/config"
)

const (
	colReset  = "\033[0m"
	colGreen  = "\033[38;5;82m"
	colYellow = "\033[38;5;220m"
	colRed    = "\033[38;5;197m"
)

func main() {
	configPath := flag.String("config", "", "Path to devhub.yaml configuration file")
	proxyPort := flag.Int("port", 0, "Ingress proxy port (default: 4000)")
	dashboardPort := flag.Int("dashboard", 0, "Inspection dashboard port (default: 4040)")
	dashboardHost := flag.String("dashboard-host", "", "Inspection dashboard bind interface (default: 127.0.0.1)")
	dashboardToken := flag.String("dashboard-token", "", "Optional authentication token for dashboard access")
	circuitBreaker := flag.Bool("enforce-circuit-breaker", false, "Actively reject traffic to unhealthy services (default: false)")
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

	// Environment variable overrides
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

	if *dashboardHost != "" {
		cfg.Server.DashboardHost = *dashboardHost
	} else if envHost := os.Getenv("DEVHUB_DASHBOARD_HOST"); envHost != "" {
		cfg.Server.DashboardHost = envHost
	}

	if *dashboardToken != "" {
		cfg.Server.DashboardAuthToken = *dashboardToken
	} else if envToken := os.Getenv("DEVHUB_DASHBOARD_TOKEN"); envToken != "" {
		cfg.Server.DashboardAuthToken = envToken
	}

	if *circuitBreaker {
		cfg.Server.EnforceCircuitBreaker = true
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

	// 2. Initialize application
	application, err := app.New(cfg)
	if err != nil {
		fmt.Printf("%s❌ Application initialization error: %v%s\n", colRed, err, colReset)
		os.Exit(1)
	}

	// 3. Start application
	if err := application.Start(ctx); err != nil {
		fmt.Printf("%s❌ Application startup error: %v%s\n", colRed, err, colReset)
		os.Exit(1)
	}

	// 4. Graceful Shutdown Listener
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Printf("\n%s🛑 Shutting down DevHub mesh gracefully...%s\n", colYellow, colReset)
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()

	_ = application.Shutdown(shutdownCtx)
	fmt.Printf("%s✨ DevHub stopped successfully.%s\n", colGreen, colReset)
}
