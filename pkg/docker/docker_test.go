package docker

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"devhub/pkg/router"
)

func TestDemuxStream_MultiFrame(t *testing.T) {
	// Construct simulated multiplexed Docker log stream
	var buf bytes.Buffer

	frame1 := EncodeDockerFrame(StreamTypeStdout, []byte("Server listening on port 8080\n"))
	frame2 := EncodeDockerFrame(StreamTypeStderr, []byte("WARN: DB pool high latency\n"))
	frame3 := EncodeDockerFrame(StreamTypeStdout, []byte("Handled request GET /users 200 OK\n"))

	buf.Write(frame1)
	buf.Write(frame2)
	buf.Write(frame3)

	var capturedStdout []string
	var capturedStderr []string

	err := DemuxStream(&buf, func(streamType int, payload []byte) {
		text := strings.TrimSpace(string(payload))
		if streamType == StreamTypeStdout {
			capturedStdout = append(capturedStdout, text)
		} else if streamType == StreamTypeStderr {
			capturedStderr = append(capturedStderr, text)
		}
	})

	if err != nil {
		t.Fatalf("demux failed: %v", err)
	}

	if len(capturedStdout) != 2 {
		t.Fatalf("expected 2 stdout lines, got %d", len(capturedStdout))
	}
	if capturedStdout[0] != "Server listening on port 8080" {
		t.Errorf("unexpected stdout line 0: %s", capturedStdout[0])
	}
	if capturedStdout[1] != "Handled request GET /users 200 OK" {
		t.Errorf("unexpected stdout line 1: %s", capturedStdout[1])
	}

	if len(capturedStderr) != 1 {
		t.Fatalf("expected 1 stderr line, got %d", len(capturedStderr))
	}
	if capturedStderr[0] != "WARN: DB pool high latency" {
		t.Errorf("unexpected stderr line 0: %s", capturedStderr[0])
	}
}

func TestWatcher_RegisterContainerLabels(t *testing.T) {
	r := router.NewRouter()

	var routedActions []string
	watcher := NewWatcher(nil, r, nil, func(action string, cr *ContainerRoute) {
		routedActions = append(routedActions, action+":"+cr.Prefix)
	})

	c := ContainerInfo{
		ID:    "container-12345",
		Names: []string{"/checkout-microservice"},
		Labels: map[string]string{
			"devhub.route":        "/api/checkout",
			"devhub.strip_prefix": "true",
			"devhub.port":         "8088",
		},
		Ports: []ContainerPort{
			{PrivatePort: 8088, PublicPort: 8088},
		},
	}

	watcher.registerContainer(context.Background(), c)

	routes := watcher.GetRoutes()
	if len(routes) != 1 {
		t.Fatalf("expected 1 active container route, got %d", len(routes))
	}

	target, outbound, matched := r.Match("/api/checkout/pay")
	if !matched {
		t.Fatalf("expected route /api/checkout/pay to match")
	}
	if target.Target.Port() != "8088" {
		t.Errorf("expected target port 8088, got %s", target.Target.Port())
	}
	if outbound != "/pay" {
		t.Errorf("expected stripped outbound path /pay, got %s", outbound)
	}

	// Unregister
	watcher.unregisterContainer("container-12345")
	if len(watcher.GetRoutes()) != 0 {
		t.Errorf("expected 0 active routes after unregister")
	}
}

func TestWatcher_OptInAndIdempotency(t *testing.T) {
	r := router.NewRouter()
	mountCount := 0
	watcher := NewWatcher(nil, r, nil, func(action string, cr *ContainerRoute) {
		if action == "mount" {
			mountCount++
		}
	})

	// 1. Unlabeled container with autoRouteAll = false (default) should NOT be registered
	unlabeled := ContainerInfo{
		ID:    "unlabeled-1",
		Names: []string{"/arbitrary-redis"},
		Ports: []ContainerPort{{PrivatePort: 6379}},
	}
	watcher.registerContainer(context.Background(), unlabeled)
	if len(watcher.GetRoutes()) != 0 {
		t.Errorf("expected unlabeled container to be ignored by default, got %d routes", len(watcher.GetRoutes()))
	}
	if mountCount != 0 {
		t.Errorf("expected 0 mounts, got %d", mountCount)
	}

	// 2. Opt-in container with devhub.route
	labeled := ContainerInfo{
		ID:    "orders-svc-1",
		Names: []string{"/orders-service"},
		Labels: map[string]string{
			"devhub.route": "/api/orders",
			"devhub.port":  "9000",
		},
		Ports: []ContainerPort{{PrivatePort: 9000}},
	}
	watcher.registerContainer(context.Background(), labeled)
	if len(watcher.GetRoutes()) != 1 {
		t.Fatalf("expected 1 route, got %d", len(watcher.GetRoutes()))
	}
	if mountCount != 1 {
		t.Errorf("expected exactly 1 mount, got %d", mountCount)
	}

	// 3. Idempotent re-sync with unchanged container should NOT re-mount or call router.Add
	watcher.registerContainer(context.Background(), labeled)
	if mountCount != 1 {
		t.Errorf("reconciliation should be idempotent! expected mountCount 1, got %d", mountCount)
	}
}

func TestWatcher_Modes(t *testing.T) {
	// Container with devhub.enable=true but no explicit devhub.route
	enableOnly := ContainerInfo{
		ID:    "enable-only-1",
		Names: []string{"/inventory-service"},
		Labels: map[string]string{
			"devhub.enable": "true",
			"devhub.port":   "8085",
		},
		Ports: []ContainerPort{{PrivatePort: 8085}},
	}

	// 1. In strict mode (default), enable-only should NOT be mounted
	rStrict := router.NewRouter()
	wStrict := NewWatcher(nil, rStrict, nil, nil)
	wStrict.SetMode("strict")
	wStrict.registerContainer(context.Background(), enableOnly)
	if len(wStrict.GetRoutes()) != 0 {
		t.Errorf("strict mode should ignore container without explicit devhub.route label, got %d", len(wStrict.GetRoutes()))
	}

	// 2. In opt_in mode, enable-only SHOULD be mounted via container name
	rOptIn := router.NewRouter()
	wOptIn := NewWatcher(nil, rOptIn, nil, nil)
	wOptIn.SetMode("opt_in")
	wOptIn.registerContainer(context.Background(), enableOnly)
	if len(wOptIn.GetRoutes()) != 1 {
		t.Fatalf("opt_in mode should mount container with devhub.enable=true, got %d", len(wOptIn.GetRoutes()))
	}
	if wOptIn.GetRoutes()[0].Prefix != "/inventory-service" {
		t.Errorf("expected route /inventory-service, got %s", wOptIn.GetRoutes()[0].Prefix)
	}

	// 3. In automatic mode, unlabeled container SHOULD be mounted
	unlabeled := ContainerInfo{
		ID:    "unlabeled-2",
		Names: []string{"/billing-service"},
		Ports: []ContainerPort{{PrivatePort: 9090}},
	}
	rAuto := router.NewRouter()
	wAuto := NewWatcher(nil, rAuto, nil, nil)
	wAuto.SetMode("automatic")
	wAuto.registerContainer(context.Background(), unlabeled)
	if len(wAuto.GetRoutes()) != 1 {
		t.Fatalf("automatic mode should mount unlabeled container, got %d", len(wAuto.GetRoutes()))
	}
	if wAuto.GetRoutes()[0].Prefix != "/billing-service" {
		t.Errorf("expected route /billing-service, got %s", wAuto.GetRoutes()[0].Prefix)
	}

	// Verify Stop()
	wAuto.Stop()
}

