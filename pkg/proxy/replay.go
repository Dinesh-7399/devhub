package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"devhub/pkg/webhook"
)

// ReplayRequest defines the payload for an on-demand request replay.
type ReplayRequest struct {
	Method           string            `json:"method"`
	URL              string            `json:"url"`
	Headers          map[string]string `json:"headers"`
	Body             string            `json:"body"`
	ResignProvider   string            `json:"resign_provider,omitempty"`
	WebhookSecret    string            `json:"webhook_secret,omitempty"`
	BypassSignatures bool              `json:"bypass_signatures,omitempty"`
}

// ReplayResponse contains the result of a replayed request.
type ReplayResponse struct {
	StatusCode int               `json:"status_code"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
	DurationMs int64             `json:"duration_ms"`
	TraceID    string            `json:"trace_id"`
}

// ReplayDispatcher dispatches recorded or modified requests back through the proxy engine.
type ReplayDispatcher struct {
	engine http.Handler
}

// NewReplayDispatcher initializes a replay dispatcher.
func NewReplayDispatcher(handler http.Handler) *ReplayDispatcher {
	return &ReplayDispatcher{
		engine: handler,
	}
}

// Execute fires a replayed request synchronously through the proxy pipeline, handling webhook re-signing if requested.
func (d *ReplayDispatcher) Execute(reqPayload ReplayRequest) (*ReplayResponse, error) {
	if reqPayload.Method == "" {
		reqPayload.Method = http.MethodGet
	}
	if reqPayload.URL == "" {
		reqPayload.URL = "/"
	}

	headers := reqPayload.Headers
	if headers == nil {
		headers = make(map[string]string)
	}

	// 1. Handle Webhook Signature Re-signing or Bypassing
	if reqPayload.BypassSignatures {
		headers = webhook.BypassHeaders(headers)
	} else if reqPayload.ResignProvider != "" && reqPayload.WebhookSecret != "" {
		resigned, err := webhook.ResignHeaders(headers, reqPayload.ResignProvider, reqPayload.WebhookSecret, reqPayload.Body)
		if err == nil {
			headers = resigned
		}
	} else {
		// Auto-detect known webhook if secret is provided
		if reqPayload.WebhookSecret != "" {
			detected := webhook.DetectProvider(headers)
			if detected != "" {
				resigned, err := webhook.ResignHeaders(headers, detected, reqPayload.WebhookSecret, reqPayload.Body)
				if err == nil {
					headers = resigned
				}
			}
		}
	}

	var bodyReader io.Reader
	if reqPayload.Body != "" {
		bodyReader = strings.NewReader(reqPayload.Body)
	}

	httpReq, err := http.NewRequest(reqPayload.Method, reqPayload.URL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create replay request: %w", err)
	}

	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}

	// Stamp with replay trace tag
	traceID := httpReq.Header.Get("X-DevHub-Trace-ID")
	if traceID == "" {
		traceID = fastTraceID()
		httpReq.Header.Set("X-DevHub-Trace-ID", traceID)
	}
	httpReq.Header.Set("X-DevHub-Replayed", "true")

	rec := httptest.NewRecorder()
	start := time.Now()

	d.engine.ServeHTTP(rec, httpReq)

	duration := time.Since(start).Milliseconds()
	resp := rec.Result()
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	return &ReplayResponse{
		StatusCode: resp.StatusCode,
		Headers:    extractHeaders(resp.Header),
		Body:       string(respBytes),
		DurationMs: duration,
		TraceID:    traceID,
	}, nil
}
