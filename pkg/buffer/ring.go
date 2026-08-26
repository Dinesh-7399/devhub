package buffer

import (
	"sync"
)

// TraceEvent represents a captured HTTP request/response transaction.
type TraceEvent struct {
	ID           string            `json:"id"`
	TraceID      string            `json:"trace_id"`
	Timestamp    string            `json:"timestamp"`
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	Target       string            `json:"target"`
	StatusCode   int               `json:"status_code"`
	DurationMs   int64             `json:"duration_ms"`
	ReqHeaders   map[string]string `json:"req_headers"`
	RespHeaders  map[string]string `json:"resp_headers"`
	RequestBody  string            `json:"request_body"`
	ResponseBody string            `json:"response_body"`
}

// RingBuffer provides a fixed-size, thread-safe circular buffer for storing TraceEvents.
// Oldest items are overwritten automatically when capacity is exceeded, guaranteeing O(1) space.
type RingBuffer struct {
	mu     sync.RWMutex
	events []TraceEvent
	size   int
	head   int
	count  int
}

// NewRingBuffer allocates a circular buffer with the specified fixed capacity.
func NewRingBuffer(size int) *RingBuffer {
	if size <= 0 {
		size = 100
	}
	return &RingBuffer{
		events: make([]TraceEvent, size),
		size:   size,
	}
}

// Push adds a new TraceEvent to the ring buffer, overwriting the oldest event if full.
func (r *RingBuffer) Push(event TraceEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events[r.head] = event
	r.head = (r.head + 1) % r.size
	if r.count < r.size {
		r.count++
	}
}

// GetAll returns a snapshot of all stored events in reverse chronological order (newest first).
func (r *RingBuffer) GetAll() []TraceEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]TraceEvent, 0, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head - 1 - i + r.size) % r.size
		result = append(result, r.events[idx])
	}
	return result
}

// Count returns the number of events currently stored in the buffer.
func (r *RingBuffer) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.count
}

// Capacity returns the maximum capacity of the ring buffer.
func (r *RingBuffer) Capacity() int {
	return r.size
}

// Clear resets the buffer to empty state.
func (r *RingBuffer) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.head = 0
	r.count = 0
	for i := range r.events {
		r.events[i] = TraceEvent{}
	}
}
