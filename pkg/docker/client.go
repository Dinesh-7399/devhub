package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
)

// ContainerPort represents port bindings in Docker API.
type ContainerPort struct {
	IP          string `json:"IP"`
	PrivatePort int    `json:"PrivatePort"`
	PublicPort  int    `json:"PublicPort"`
	Type        string `json:"Type"`
}

// ContainerInfo represents summary data returned from GET /containers/json.
type ContainerInfo struct {
	ID      string            `json:"Id"`
	Names   []string          `json:"Names"`
	Image   string            `json:"Image"`
	State   string            `json:"State"`
	Status  string            `json:"Status"`
	Labels  map[string]string `json:"Labels"`
	Ports   []ContainerPort   `json:"Ports"`
	Created int64             `json:"Created"`
}

// DockerEvent represents lifecycle notifications from GET /events.
type DockerEvent struct {
	Type   string `json:"Type"`
	Action string `json:"Action"`
	Actor  struct {
		ID         string            `json:"ID"`
		Attributes map[string]string `json:"Attributes"`
	} `json:"Actor"`
	Time     int64 `json:"time"`
	TimeNano int64 `json:"timeNano"`
}

// Client interacts directly with the Docker Engine over native IPC.
type Client struct {
	httpClient *http.Client
	socketPath string
}

// NewClient constructs a Docker API client for Unix socket or Windows named pipe.
func NewClient(socketPath string) *Client {
	if socketPath == "" {
		if runtime.GOOS == "windows" {
			socketPath = `\\.\pipe\docker_engine`
		} else {
			socketPath = `/var/run/docker.sock`
		}
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, proto, addr string) (net.Conn, error) {
			return dialDocker(ctx, socketPath)
		},
	}

	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   0, // Allow long streaming connections for events and logs
		},
		socketPath: socketPath,
	}
}

// ListContainers retrieves running containers from GET /containers/json.
func (c *Client) ListContainers(ctx context.Context) ([]ContainerInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/json", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to contact Docker daemon at %s: %w", c.socketPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("docker API returned status %d: %s", resp.StatusCode, string(b))
	}

	var containers []ContainerInfo
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return nil, fmt.Errorf("failed to decode container list: %w", err)
	}

	return containers, nil
}

// StreamEvents listens to Docker lifecycle events (container:start, container:die).
func (c *Client) StreamEvents(ctx context.Context) (io.ReadCloser, error) {
	params := url.Values{}
	params.Set("filters", `{"type":["container"]}`)

	reqURL := "http://docker/events?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to stream Docker events: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("docker events API returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// StreamLogs opens a real-time multiplexed stdout/stderr stream from a container.
func (c *Client) StreamLogs(ctx context.Context, containerID string, follow bool, tail int) (io.ReadCloser, error) {
	params := url.Values{}
	params.Set("stdout", "1")
	params.Set("stderr", "1")
	if follow {
		params.Set("follow", "1")
	}
	if tail > 0 {
		params.Set("tail", fmt.Sprintf("%d", tail))
	} else {
		params.Set("tail", "100")
	}

	reqURL := fmt.Sprintf("http://docker/containers/%s/logs?%s", containerID, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to stream container logs for %s: %w", containerID, err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("docker logs API returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
}
