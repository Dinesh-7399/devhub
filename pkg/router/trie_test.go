package router

import (
	"net/url"
	"sync"
	"testing"
)

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestRouter_Match(t *testing.T) {
	r := NewRouter()

	authTarget := mustParseURL("http://localhost:5001")
	apiTarget := mustParseURL("http://localhost:5002")
	webTarget := mustParseURL("http://localhost:3000")
	userTarget := mustParseURL("http://localhost:5003")

	r.Add("/auth", authTarget, false)
	r.Add("/api/v1", apiTarget, true)
	r.Add("/api/v1/users", userTarget, true)
	r.Add("/", webTarget, false)

	tests := []struct {
		name             string
		inPath           string
		wantMatched      bool
		wantTargetHost   string
		wantOutboundPath string
	}{
		{
			name:             "Root fallback",
			inPath:           "/dashboard",
			wantMatched:      true,
			wantTargetHost:   "localhost:3000",
			wantOutboundPath: "/dashboard",
		},
		{
			name:             "Root exact",
			inPath:           "/",
			wantMatched:      true,
			wantTargetHost:   "localhost:3000",
			wantOutboundPath: "/",
		},
		{
			name:             "Auth no strip prefix",
			inPath:           "/auth/login",
			wantMatched:      true,
			wantTargetHost:   "localhost:5001",
			wantOutboundPath: "/auth/login",
		},
		{
			name:             "API v1 with strip prefix",
			inPath:           "/api/v1/checkout",
			wantMatched:      true,
			wantTargetHost:   "localhost:5002",
			wantOutboundPath: "/checkout",
		},
		{
			name:             "API v1 exact with strip prefix",
			inPath:           "/api/v1",
			wantMatched:      true,
			wantTargetHost:   "localhost:5002",
			wantOutboundPath: "/",
		},
		{
			name:             "API v1 trailing slash with strip prefix",
			inPath:           "/api/v1/",
			wantMatched:      true,
			wantTargetHost:   "localhost:5002",
			wantOutboundPath: "/",
		},
		{
			name:             "Longest prefix match /api/v1/users vs /api/v1",
			inPath:           "/api/v1/users/42/profile",
			wantMatched:      true,
			wantTargetHost:   "localhost:5003",
			wantOutboundPath: "/42/profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, outboundPath, matched := r.Match(tt.inPath)
			if matched != tt.wantMatched {
				t.Fatalf("Match() matched = %v, want %v", matched, tt.wantMatched)
			}
			if !matched {
				return
			}
			if target.Target.Host != tt.wantTargetHost {
				t.Errorf("Match() target host = %v, want %v", target.Target.Host, tt.wantTargetHost)
			}
			if outboundPath != tt.wantOutboundPath {
				t.Errorf("Match() outboundPath = %v, want %v", outboundPath, tt.wantOutboundPath)
			}
		})
	}
}

func TestRouter_NoMatch(t *testing.T) {
	r := NewRouter()
	r.Add("/auth", mustParseURL("http://localhost:5001"), false)

	_, _, matched := r.Match("/api/unregistered")
	if matched {
		t.Fatalf("expected no match, but got matched")
	}
}

func TestRouter_Concurrent(t *testing.T) {
	r := NewRouter()
	r.Add("/api", mustParseURL("http://localhost:5000"), false)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.Match("/api/v1/resource")
		}(i)
	}
	wg.Wait()
}

func BenchmarkRouter_Match(b *testing.B) {
	r := NewRouter()
	r.Add("/api/v1/users", mustParseURL("http://localhost:5003"), true)
	r.Add("/api/v1", mustParseURL("http://localhost:5002"), true)
	r.Add("/auth", mustParseURL("http://localhost:5001"), false)
	r.Add("/", mustParseURL("http://localhost:3000"), false)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = r.Match("/api/v1/users/42/details")
		}
	})
}
