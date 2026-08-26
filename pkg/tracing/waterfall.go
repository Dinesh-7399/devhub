package tracing

import (
	"devhub/pkg/buffer"
)

// SpanNode represents a node in the microservice latency waterfall tree.
type SpanNode struct {
	ID           string      `json:"id"`
	ParentSpanID string      `json:"parent_span_id,omitempty"`
	SpanID       string      `json:"span_id,omitempty"`
	TraceID      string      `json:"trace_id"`
	Timestamp    string      `json:"timestamp"`
	Method       string      `json:"method"`
	Path         string      `json:"path"`
	Target       string      `json:"target"`
	StatusCode   int         `json:"status_code"`
	DurationMs   int64       `json:"duration_ms"`
	Children     []*SpanNode `json:"children,omitempty"`
}

// BuildWaterfall takes a slice of TraceEvents and constructs a causal hierarchy for a given TraceID.
func BuildWaterfall(events []buffer.TraceEvent, targetTraceID string) []*SpanNode {
	var matching []buffer.TraceEvent
	for _, ev := range events {
		if ev.TraceID == targetTraceID {
			matching = append(matching, ev)
		}
	}

	if len(matching) == 0 {
		return nil
	}

	nodes := make(map[string]*SpanNode, len(matching))
	var roots []*SpanNode

	for _, ev := range matching {
		parentSpanID := ev.ReqHeaders["X-DevHub-Parent-Span-ID"]
		spanID := ev.ReqHeaders["X-DevHub-Span-ID"]

		node := &SpanNode{
			ID:           ev.ID,
			ParentSpanID: parentSpanID,
			SpanID:       spanID,
			TraceID:      ev.TraceID,
			Timestamp:    ev.Timestamp,
			Method:       ev.Method,
			Path:         ev.Path,
			Target:       ev.Target,
			StatusCode:   ev.StatusCode,
			DurationMs:   ev.DurationMs,
			Children:     make([]*SpanNode, 0),
		}
		nodes[ev.ID] = node
	}

	// Link children to parents
	for _, node := range nodes {
		if node.ParentSpanID == "" {
			roots = append(roots, node)
		} else {
			// Find parent by SpanID
			var parentFound bool
			for _, potentialParent := range nodes {
				if potentialParent.SpanID != "" && potentialParent.SpanID == node.ParentSpanID {
					potentialParent.Children = append(potentialParent.Children, node)
					parentFound = true
					break
				}
			}
			if !parentFound {
				roots = append(roots, node)
			}
		}
	}

	return roots
}
