//go:build windows

package docker

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/Microsoft/go-winio"
)

func dialDocker(ctx context.Context, socketPath string) (net.Conn, error) {
	cleanPath := socketPath
	if cleanPath == "" || cleanPath == "/var/run/docker.sock" {
		cleanPath = `\\.\pipe\docker_engine`
	}

	cleanPath = strings.ReplaceAll(cleanPath, "/", `\`)
	if strings.HasPrefix(cleanPath, `\\.\pipe\`) {
		return winio.DialPipeContext(ctx, cleanPath)
	}

	if strings.HasPrefix(socketPath, "tcp://") || strings.HasPrefix(socketPath, "http://") {
		cleanAddr := strings.TrimPrefix(socketPath, "tcp://")
		cleanAddr = strings.TrimPrefix(cleanAddr, "http://")
		return net.DialTimeout("tcp", cleanAddr, 2*time.Second)
	}

	// Default fallback to Windows named pipe
	return winio.DialPipeContext(ctx, `\\.\pipe\docker_engine`)
}
