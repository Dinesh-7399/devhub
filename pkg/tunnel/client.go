package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// TunnelRequest represents an inbound HTTP request proxied from the public internet relay.
type TunnelRequest struct {
	StreamID string            `json:"stream_id"`
	Method   string            `json:"method"`
	Path     string            `json:"path"`
	Headers  map[string]string `json:"headers"`
	Body     string            `json:"body"`
}

// TunnelResponse represents the response frame sent back through the tunnel to the relay server.
type TunnelResponse struct {
	StreamID   string            `json:"stream_id"`
	StatusCode int               `json:"status_code"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
}

// Client manages the persistent multiplexed tunnel to the public cloud relay.
type Client struct {
	serverURL string
	localAddr string
	subdomain string
	authToken string
	client    *http.Client
	mu        sync.Mutex
	active    bool
}

// NewClient initializes a public ingress tunnel client.
func NewClient(serverURL, subdomain, authToken, localAddr string) *Client {
	if localAddr == "" {
		localAddr = "http://localhost:4000"
	}
	return &Client{
		serverURL: serverURL,
		localAddr: localAddr,
		subdomain: subdomain,
		authToken: authToken,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Start opens a persistent connection to the cloud relay server and handles incoming forwarded requests.
func (c *Client) Start(ctx context.Context) error {
	if c.serverURL == "" {
		return fmt.Errorf("tunnel server URL is empty")
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			err := c.connectAndServe(ctx)
			if err != nil {
				time.Sleep(3 * time.Second)
			}
		}
	}()

	return nil
}

func (c *Client) connectAndServe(ctx context.Context) error {
	headers := http.Header{}
	if c.authToken != "" {
		headers.Set("Authorization", "Bearer "+c.authToken)
	}
	if c.subdomain != "" {
		headers.Set("X-DevHub-Subdomain", c.subdomain)
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.serverURL, headers)
	if err != nil {
		return err
	}
	defer conn.Close()

	c.mu.Lock()
	c.active = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.active = false
		c.mu.Unlock()
	}()

	fmt.Printf("🌐 [Tunnel Active] Public ingress bridged -> %s\n", c.localAddr)

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var req TunnelRequest
		if err := json.Unmarshal(message, &req); err != nil {
			continue
		}

		// Dispatch request to local proxy in goroutine
		go func(tReq TunnelRequest) {
			respFrame := c.forwardToLocal(tReq)
			respData, err := json.Marshal(respFrame)
			if err == nil {
				c.mu.Lock()
				_ = conn.WriteMessage(websocket.TextMessage, respData)
				c.mu.Unlock()
			}
		}(req)
	}
}

func (c *Client) forwardToLocal(tReq TunnelRequest) TunnelResponse {
	targetURL := c.localAddr + tReq.Path
	var bodyReader io.Reader
	if tReq.Body != "" {
		bodyReader = strings.NewReader(tReq.Body)
	}

	httpReq, err := http.NewRequest(tReq.Method, targetURL, bodyReader)
	if err != nil {
		return TunnelResponse{
			StreamID:   tReq.StreamID,
			StatusCode: http.StatusBadGateway,
			Body:       err.Error(),
		}
	}

	for k, v := range tReq.Headers {
		httpReq.Header.Set(k, v)
	}
	httpReq.Header.Set("X-DevHub-Tunnel-Stream", tReq.StreamID)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return TunnelResponse{
			StreamID:   tReq.StreamID,
			StatusCode: http.StatusBadGateway,
			Body:       err.Error(),
		}
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	respHeaders := make(map[string]string, len(resp.Header))
	for k, v := range resp.Header {
		if len(v) > 0 {
			respHeaders[k] = v[0]
		}
	}

	return TunnelResponse{
		StreamID:   tReq.StreamID,
		StatusCode: resp.StatusCode,
		Headers:    respHeaders,
		Body:       string(respBytes),
	}
}

// IsActive returns the current tunnel connection status.
func (c *Client) IsActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}
