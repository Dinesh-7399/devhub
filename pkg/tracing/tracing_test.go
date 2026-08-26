package tracing

import (
	"net/http"
	"strings"
	"testing"

	"devhub/pkg/buffer"
)

func TestContext_W3CTraceparent(t *testing.T) {
	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	spanID := "00f067aa0ba902b7"
	raw := "00-" + traceID + "-" + spanID + "-01"

	v, tID, sID, sampled, ok := ParseTraceparent(raw)
	if !ok {
		t.Fatalf("failed to parse valid traceparent")
	}
	if v != "00" || tID != traceID || sID != spanID || !sampled {
		t.Errorf("unexpected parsed values: v=%s t=%s s=%s sampled=%v", v, tID, sID, sampled)
	}

	h := http.Header{}
	h.Set("traceparent", raw)

	outTraceID, parentSpanID, newSpanID, outTP := ExtractOrGenerateContext(h)
	if outTraceID != traceID {
		t.Errorf("expected traceID %s, got %s", traceID, outTraceID)
	}
	if parentSpanID != spanID {
		t.Errorf("expected parentSpanID %s, got %s", spanID, parentSpanID)
	}
	if newSpanID == "" || newSpanID == spanID {
		t.Errorf("expected new spanID to be generated, got %s", newSpanID)
	}
	if !strings.HasPrefix(outTP, "00-"+traceID+"-") {
		t.Errorf("unexpected outbound traceparent: %s", outTP)
	}
}

func TestRedactor_HeadersAndBody(t *testing.T) {
	headers := map[string]string{
		"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.secretpayload",
		"Cookie":        "session_id=abcdef123456",
		"X-Api-Key":     "prod_api_key_secret_value",
		"Content-Type":  "application/json",
	}

	redactedHeaders := RedactHeaders(headers)
	if !strings.Contains(redactedHeaders["Authorization"], "***REDACTED***") {
		t.Errorf("expected redacted Authorization, got %s", redactedHeaders["Authorization"])
	}
	if redactedHeaders["Cookie"] != "***REDACTED_COOKIE***" {
		t.Errorf("expected redacted Cookie, got %s", redactedHeaders["Cookie"])
	}
	if redactedHeaders["X-Api-Key"] != "***REDACTED***" {
		t.Errorf("expected redacted Api-Key, got %s", redactedHeaders["X-Api-Key"])
	}
	if redactedHeaders["Content-Type"] != "application/json" {
		t.Errorf("Content-Type should not be redacted, got %s", redactedHeaders["Content-Type"])
	}

	jsonBody := `{"user":"alice","password":"SuperSecretPassword123!","token":"jwt-token-val","credit_card":"4111222233334444","amount":100}`
	redactedBody := RedactBody(jsonBody)

	if strings.Contains(redactedBody, "SuperSecretPassword123!") {
		t.Errorf("password was not redacted in body: %s", redactedBody)
	}
	if strings.Contains(redactedBody, "4111222233334444") {
		t.Errorf("credit card was not redacted in body: %s", redactedBody)
	}
	if !strings.Contains(redactedBody, `"amount":100`) {
		t.Errorf("non-sensitive field was corrupted: %s", redactedBody)
	}
}

func TestWaterfall_Build(t *testing.T) {
	traceID := "test-trace-12345"

	events := []buffer.TraceEvent{
		{
			ID:         "span-root",
			TraceID:    traceID,
			Path:       "/api/v1/orders",
			DurationMs: 45,
			ReqHeaders: map[string]string{
				"X-DevHub-Span-ID": "span-1",
			},
		},
		{
			ID:         "span-child-1",
			TraceID:    traceID,
			Path:       "/auth/verify",
			DurationMs: 12,
			ReqHeaders: map[string]string{
				"X-DevHub-Span-ID":        "span-2",
				"X-DevHub-Parent-Span-ID": "span-1",
			},
		},
		{
			ID:         "span-child-2",
			TraceID:    traceID,
			Path:       "/payment/charge",
			DurationMs: 25,
			ReqHeaders: map[string]string{
				"X-DevHub-Span-ID":        "span-3",
				"X-DevHub-Parent-Span-ID": "span-1",
			},
		},
	}

	waterfall := BuildWaterfall(events, traceID)
	if len(waterfall) != 1 {
		t.Fatalf("expected 1 root span, got %d", len(waterfall))
	}

	root := waterfall[0]
	if root.ID != "span-root" {
		t.Errorf("expected root span ID span-root, got %s", root.ID)
	}
	if len(root.Children) != 2 {
		t.Fatalf("expected 2 children under root span, got %d", len(root.Children))
	}
}
