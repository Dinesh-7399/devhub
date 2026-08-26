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
