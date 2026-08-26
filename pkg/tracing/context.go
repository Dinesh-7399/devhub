package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"sync/atomic"
)

var (
	traceSeq uint64
	spanSeq  uint64
)

// GenerateTraceID generates a 32-character hexadecimal W3C compliant Trace ID (16 bytes).
func GenerateTraceID() string {
	val := atomic.AddUint64(&traceSeq, 1)
	var b [16]byte
	_, _ = rand.Read(b[:8])
	b[8] = byte(val >> 56)
	b[9] = byte(val >> 48)
	b[10] = byte(val >> 40)
	b[11] = byte(val >> 32)
	b[12] = byte(val >> 24)
	b[13] = byte(val >> 16)
	b[14] = byte(val >> 8)
	b[15] = byte(val)
	return hex.EncodeToString(b[:])
}

// GenerateSpanID generates a 16-character hexadecimal W3C compliant Span ID (8 bytes).
func GenerateSpanID() string {
	val := atomic.AddUint64(&spanSeq, 1)
	var b [8]byte
	b[0] = byte(val >> 56)
	b[1] = byte(val >> 48)
	b[2] = byte(val >> 40)
	b[3] = byte(val >> 32)
	b[4] = byte(val >> 24)
	b[5] = byte(val >> 16)
	b[6] = byte(val >> 8)
	b[7] = byte(val)
	return hex.EncodeToString(b[:])
}

// FormatTraceparent creates a W3C traceparent header: 00-{trace_id}-{span_id}-01
func FormatTraceparent(traceID, spanID string) string {
	return "00-" + traceID + "-" + spanID + "-01"
}

// ParseTraceparent parses a W3C traceparent string into traceID, spanID, and sampled flag.
func ParseTraceparent(raw string) (version, traceID, spanID string, sampled bool, ok bool) {
	raw = strings.TrimSpace(raw)
	parts := strings.Split(raw, "-")
	if len(parts) != 4 {
		return "", "", "", false, false
	}

	version = parts[0]
	traceID = parts[1]
	spanID = parts[2]
	flags := parts[3]

	if len(version) != 2 || len(traceID) != 32 || len(spanID) != 16 || len(flags) != 2 {
		return "", "", "", false, false
	}

	sampled = (flags == "01")
	return version, traceID, spanID, sampled, true
}

// ExtractOrGenerateContext inspects incoming headers for W3C traceparent or X-DevHub-Trace-ID.
func ExtractOrGenerateContext(h http.Header) (traceID string, parentSpanID string, spanID string, traceparent string) {
	spanID = GenerateSpanID()

	// Check W3C traceparent first
	rawTP := h.Get("traceparent")
	if rawTP != "" {
		if _, tID, pSpanID, _, ok := ParseTraceparent(rawTP); ok {
			traceID = tID
			parentSpanID = pSpanID
			traceparent = FormatTraceparent(traceID, spanID)
			return traceID, parentSpanID, spanID, traceparent
		}
	}

	// Check X-DevHub-Trace-ID header fallback
	traceID = h.Get("X-DevHub-Trace-ID")
	if traceID == "" {
		traceID = GenerateTraceID()
	} else if len(traceID) < 32 {
		// Pad to 32 chars if short ID was supplied
		traceID = traceID + strings.Repeat("0", 32-len(traceID))
	} else if len(traceID) > 32 {
		traceID = traceID[:32]
	}

	parentSpanID = h.Get("X-DevHub-Span-ID")
	traceparent = FormatTraceparent(traceID, spanID)
	return traceID, parentSpanID, spanID, traceparent
}
