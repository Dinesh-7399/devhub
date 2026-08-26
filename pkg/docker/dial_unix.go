//go:build !windows

package docker

import (
	"context"
	"net"
	"strings"
	"time"
)

func dialDocker(ctx context.Context, socketPath string) (net.Conn, error) {
	if socketPath == "" {
		socketPath = "/var/run/docker.sock"
	}

	cleanPath := strings.TrimPrefix(socketPath, "unix://")
	if strings.HasPrefix(socketPath, "tcp://") || strings.HasPrefix(socketPath, "http://") {
		cleanAddr := strings.TrimPrefix(socketPath, "tcp://")
		cleanAddr = strings.TrimPrefix(cleanAddr, "http://")
		return net.DialTimeout("tcp", cleanAddr, 2*time.Second)
	}

	var d net.Dialer
	return d.DialContext(ctx, "unix", cleanPath)
}
