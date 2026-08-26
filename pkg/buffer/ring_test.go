package buffer

import (
	"fmt"
	"sync"
	"testing"
)

func TestRingBuffer_PushAndGetAll(t *testing.T) {
	rb := NewRingBuffer(3)

	if rb.Count() != 0 {
		t.Fatalf("expected count 0, got %d", rb.Count())
	}

	rb.Push(TraceEvent{ID: "1", Method: "GET"})
	rb.Push(TraceEvent{ID: "2", Method: "POST"})

	if rb.Count() != 2 {
		t.Fatalf("expected count 2, got %d", rb.Count())
	}

	events := rb.GetAll()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].ID != "2" || events[1].ID != "1" {
		t.Fatalf("expected [2, 1], got [%s, %s]", events[0].ID, events[1].ID)
	}

	// Push 3rd event (fills capacity)
	rb.Push(TraceEvent{ID: "3", Method: "PUT"})
	if rb.Count() != 3 {
		t.Fatalf("expected count 3, got %d", rb.Count())
	}

	// Push 4th event (evicts 1)
	rb.Push(TraceEvent{ID: "4", Method: "DELETE"})
	if rb.Count() != 3 {
		t.Fatalf("expected count 3, got %d", rb.Count())
	}

	events = rb.GetAll()
	expectedIDs := []string{"4", "3", "2"}
	for i, exp := range expectedIDs {
		if events[i].ID != exp {
			t.Errorf("at index %d: expected ID %s, got %s", i, exp, events[i].ID)
		}
	}
}

func TestRingBuffer_Concurrent(t *testing.T) {
	rb := NewRingBuffer(50)
	var wg sync.WaitGroup

	numWriters := 20
	writesPerWorker := 100

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				rb.Push(TraceEvent{
					ID:     fmt.Sprintf("w%d-%d", workerID, i),
					Method: "GET",
				})
			}
		}(w)
	}

	numReaders := 10
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = rb.GetAll()
				_ = rb.Count()
			}
		}()
	}

	wg.Wait()

	if rb.Count() != 50 {
		t.Fatalf("expected count to cap at 50, got %d", rb.Count())
	}
	events := rb.GetAll()
	if len(events) != 50 {
		t.Fatalf("expected 50 events, got %d", len(events))
	}
}

func BenchmarkRingBuffer_Push(b *testing.B) {
	rb := NewRingBuffer(500)
	ev := TraceEvent{
		ID:         "test",
		TraceID:    "trace-1",
		Method:     "GET",
		Path:       "/api/v1/test",
		StatusCode: 200,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb.Push(ev)
		}
	})
}
