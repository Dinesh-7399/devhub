package proxy

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devhub/pkg/buffer"
	"devhub/pkg/mock"
	"devhub/pkg/router"
	"devhub/pkg/tracing"
)

const maxCaptureSize = 64 * 1024 // 64KB capture limit for payload inspection

// Global atomic counter for ultra-fast zero-alloc ID generation
var traceCounter uint64

// Buffer pool for recyclable 64KB capture slices
type captureBuffer struct {
	data []byte
	len  int
}

func (c *captureBuffer) reset() {
	c.len = 0
}

func (c *captureBuffer) write(b []byte) {
	if c.len >= maxCaptureSize {
		return
	}
	canWrite := maxCaptureSize - c.len
	if len(b) <= canWrite {
		copy(c.data[c.len:], b)
		c.len += len(b)
	} else {
		copy(c.data[c.len:], b[:canWrite])
		c.len += canWrite
	}
}

func (c *captureBuffer) string() string {
	if c.len == 0 {
		return ""
	}
	return string(c.data[:c.len])
}

var capturePool = sync.Pool{
	New: func() any {
		return &captureBuffer{
			data: make([]byte, maxCaptureSize),
		}
	},
}

// Pooled responseCapturer objects to avoid per-request allocations
var capturerPool = sync.Pool{
	New: func() any {
		return &responseCapturer{
			bodyBuf: make([]byte, maxCaptureSize),
		}
	},
}

// Engine coordinates the reverse proxy, forward proxy (HTTP_PROXY), routing trie, mock manager, and live telemetry.
type Engine struct {
	Router           *router.Router
	Ring             *buffer.RingBuffer
	Health           *HealthMonitor
	Mocks            *mock.Manager
	Broadcast        func(buffer.TraceEvent)
	HTTPProxy        *httputil.ReverseProxy
	Transport        http.RoundTripper // Ingress transport (no env proxy recursion)
	ForwardTransport http.RoundTripper // Egress forward proxy transport
	SecurityPolicy   *EgressSecurityPolicy
	RedactPII        bool
}

// NewEngine initializes a high-performance streaming reverse and forward proxy engine.
func NewEngine(r *router.Router, ring *buffer.RingBuffer, broadcaster func(buffer.TraceEvent)) *Engine {
	health := NewHealthMonitor()
	mocks := mock.NewManager()
	secPolicy := NewEgressSecurityPolicy(EgressModeAllowAllExceptMetadata, true) // Block cloud metadata SSRF by default

	// Ingress transport: explicitly Proxy: nil to prevent recursion if HTTP_PROXY is set
	ingressTransport := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   1000,
		MaxConnsPerHost:       0, // unlimited
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true, // pass raw stream chunks without re-compression CPU penalty
	}

	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	// Forward transport: separate client transport with Proxy: nil and TOCTOU-free validated IP dialer
	forwardTransport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if secPolicy != nil {
				return secPolicy.DialValidatedContext(ctx, dialer, network, addr)
			}
			return dialer.DialContext(ctx, network, addr)
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   1000,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}

	e := &Engine{
		Router:           r,
		Ring:             ring,
		Health:           health,
		Mocks:            mocks,
		Broadcast:        broadcaster,
		Transport:        ingressTransport,
		ForwardTransport: forwardTransport,
		SecurityPolicy:   secPolicy,
		RedactPII:        true,
	}

	e.HTTPProxy = &httputil.ReverseProxy{
		Transport: ingressTransport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			route, outboundPath, matched := e.Router.Match(pr.In.URL.Path)
			if !matched {
				return
			}
			pr.SetURL(route.Target)
			pr.Out.URL.Path = outboundPath
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery

			// W3C Distributed Context Propagation
			traceID, parentSpanID, spanID, traceparent := tracing.ExtractOrGenerateContext(pr.In.Header)
			pr.Out.Header.Set("traceparent", traceparent)
			pr.Out.Header.Set("X-DevHub-Trace-ID", traceID)
			pr.Out.Header.Set("X-DevHub-Span-ID", spanID)
			if parentSpanID != "" {
				pr.Out.Header.Set("X-DevHub-Parent-Span-ID", parentSpanID)
			}
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// Check if On-Error mock fallback exists for this route
			if e.Mocks != nil {
				if mockRule, found := e.Mocks.GetRule(r.URL.Path); found && mockRule.Enabled {
					for k, v := range mockRule.Headers {
						w.Header().Set(k, v)
					}
					w.Header().Set("X-DevHub-Mock-Fallback", "true")
					w.WriteHeader(mockRule.StatusCode)
					w.Write([]byte(mockRule.Body))
					return
				}
			}
			http.Error(w, fmt.Sprintf("DevHub Gateway Error: %v", err), http.StatusBadGateway)
		},
	}

	return e
}

// emitTraceEvent pushes a trace event to the ring buffer and broadcast channel safely.
func (e *Engine) emitTraceEvent(ev buffer.TraceEvent) {
	if e.RedactPII {
		ev.ReqHeaders = tracing.RedactHeaders(ev.ReqHeaders)
		ev.RespHeaders = tracing.RedactHeaders(ev.RespHeaders)
		ev.RequestBody = tracing.RedactBody(ev.RequestBody)
		ev.ResponseBody = tracing.RedactBody(ev.ResponseBody)
	}
	if e.Ring != nil {
		e.Ring.Push(ev)
	}
	if e.Broadcast != nil {
		e.Broadcast(ev)
	}
}

// ServeHTTP handles incoming client requests, forward proxy (HTTP_PROXY), route matches, and WebSockets.
func (e *Engine) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()

	// 1. Forward Proxy CONNECT Tunnel (HTTPS / raw TCP egress cascading)
	if req.Method == http.MethodConnect {
		e.handleConnectTunnel(w, req, start)
		return
	}

	// 2. Forward Proxy HTTP Egress (Zero-code Service A -> Service B cascading via HTTP_PROXY)
	if req.URL.IsAbs() || (req.URL.Host != "" && !strings.HasPrefix(req.URL.Path, "/api/") && !strings.HasPrefix(req.URL.Path, "/auth") && req.URL.Scheme != "") {
		e.handleForwardProxyHTTP(w, req, start)
		return
	}

	// 3. Check Mock Mode (Always Mock) before routing to allow mocking unwritten microservices
	if e.Mocks != nil {
		if mockRule, found := e.Mocks.GetRule(req.URL.Path); found && mockRule.Enabled && mockRule.Mode == mock.ModeAlways {
			if mockRule.DelayMs > 0 {
				time.Sleep(time.Duration(mockRule.DelayMs) * time.Millisecond)
			}
			for k, v := range mockRule.Headers {
				w.Header().Set(k, v)
			}
			w.Header().Set("X-DevHub-Mock-Response", "true")
			w.WriteHeader(mockRule.StatusCode)
			w.Write([]byte(mockRule.Body))

			duration := time.Since(start).Milliseconds()
			traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)

			e.emitTraceEvent(buffer.TraceEvent{
				ID:           fastTraceID(),
				TraceID:      traceID,
				Timestamp:    time.Now().Format("15:04:05.000"),
				Method:       req.Method,
				Path:         req.URL.Path,
				Target:       "[Mock Mode] " + mockRule.Prefix,
				StatusCode:   mockRule.StatusCode,
				DurationMs:   duration,
				ReqHeaders:   extractHeaders(req.Header),
				RespHeaders:  mockRule.Headers,
				ResponseBody: mockRule.Body,
			})
			return
		}
	}

	route, outboundPath, matched := e.Router.Match(req.URL.Path)
	if !matched {
		duration := time.Since(start).Milliseconds()
		traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)
		http.Error(w, "DevHub: No registered route for path "+req.URL.Path, http.StatusNotFound)
		e.emitTraceEvent(buffer.TraceEvent{
			ID:          fastTraceID(),
			TraceID:     traceID,
			Timestamp:   time.Now().Format("15:04:05.000"),
			Method:      req.Method,
			Path:        req.URL.Path,
			Target:      "[No Route Matched]",
			StatusCode:  http.StatusNotFound,
			DurationMs:  duration,
			ReqHeaders:  extractHeaders(req.Header),
			RespHeaders: map[string]string{"Content-Type": "text/plain; charset=utf-8"},
		})
		return
	}

	// 4. Transparent WebSocket Upgrade Interception
	if isWebSocketRequest(req) {
		e.proxyWebSocket(w, req, route.Target, outboundPath, start)
		return
	}

	// 5. Circuit breaker check (only blocks if active enforcement is explicitly enabled)
	if e.Health != nil && e.Health.IsCircuitBroken(route.Target) {
		// If mock fallback exists on error
		if e.Mocks != nil {
			if mockRule, found := e.Mocks.GetRule(req.URL.Path); found && mockRule.Enabled {
				for k, v := range mockRule.Headers {
					w.Header().Set(k, v)
				}
				w.Header().Set("X-DevHub-Mock-Fallback", "true")
				w.WriteHeader(mockRule.StatusCode)
				w.Write([]byte(mockRule.Body))

				traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)
				e.emitTraceEvent(buffer.TraceEvent{
					ID:           fastTraceID(),
					TraceID:      traceID,
					Timestamp:    time.Now().Format("15:04:05.000"),
					Method:       req.Method,
					Path:         req.URL.Path,
					Target:       "[Circuit Broken Fallback] " + route.Target.String(),
					StatusCode:   mockRule.StatusCode,
					DurationMs:   time.Since(start).Milliseconds(),
					ReqHeaders:   extractHeaders(req.Header),
					RespHeaders:  mockRule.Headers,
					ResponseBody: mockRule.Body,
				})
				return
			}
		}

		traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)
		http.Error(w, fmt.Sprintf("DevHub: Upstream %s is currently marked UNHEALTHY (Circuit Broken)", route.Target), http.StatusServiceUnavailable)
		e.emitTraceEvent(buffer.TraceEvent{
			ID:         fastTraceID(),
			TraceID:    traceID,
			Timestamp:  time.Now().Format("15:04:05.000"),
			Method:     req.Method,
			Path:       req.URL.Path,
			Target:     "[Circuit Broken] " + route.Target.String(),
			StatusCode: http.StatusServiceUnavailable,
			DurationMs: time.Since(start).Milliseconds(),
			ReqHeaders: extractHeaders(req.Header),
		})
		return
	}

	// Apply per-route execution timeout if configured
	if route.TimeoutMs > 0 {
		timeoutCtx, cancel := context.WithTimeout(req.Context(), time.Duration(route.TimeoutMs)*time.Millisecond)
		defer cancel()
		req = req.WithContext(timeoutCtx)
	}

	// Extract or stamp W3C Distributed Tracing context
	traceID, parentSpanID, spanID, traceparent := tracing.ExtractOrGenerateContext(req.Header)
	req.Header.Set("traceparent", traceparent)
	req.Header.Set("X-DevHub-Trace-ID", traceID)
	req.Header.Set("X-DevHub-Span-ID", spanID)
	if parentSpanID != "" {
		req.Header.Set("X-DevHub-Parent-Span-ID", parentSpanID)
	}

	// Non-blocking request body sniffer using pooled buffer
	var sniffer *bodySniffer
	var reqCap *captureBuffer
	if req.Body != nil && req.Body != http.NoBody {
		reqCap = capturePool.Get().(*captureBuffer)
		reqCap.reset()
		sniffer = &bodySniffer{
			reader:  req.Body,
			capture: reqCap,
		}
		req.Body = sniffer
	}

	// Borrow pooled response capturer
	rw := capturerPool.Get().(*responseCapturer)
	rw.reset(w)

	// Proxy to upstream
	e.HTTPProxy.ServeHTTP(rw, req)

	duration := time.Since(start).Milliseconds()

	// Update health state based on status code
	if e.Health != nil {
		if rw.statusCode >= 500 {
			e.Health.RecordFailure(route.Target)
		} else {
			e.Health.RecordSuccess(route.Target)
		}
	}

	var reqBodyStr string
	if reqCap != nil {
		reqBodyStr = reqCap.string()
		capturePool.Put(reqCap)
	}

	respBodyStr := rw.string()
	reqHeaders := extractHeaders(req.Header)
	respHeaders := extractHeaders(rw.Header())

	e.emitTraceEvent(buffer.TraceEvent{
		ID:           fastTraceID(),
		TraceID:      traceID,
		Timestamp:    time.Now().Format("15:04:05.000"),
		Method:       req.Method,
		Path:         req.URL.Path,
		Target:       route.Target.String() + outboundPath,
		StatusCode:   rw.statusCode,
		DurationMs:   duration,
		ReqHeaders:   reqHeaders,
		RespHeaders:  respHeaders,
		RequestBody:  reqBodyStr,
		ResponseBody: respBodyStr,
	})

	// Recycle response capturer
	capturerPool.Put(rw)
}

// handleForwardProxyHTTP handles HTTP requests made via HTTP_PROXY environment variables (Service A -> Service B)
// with true streaming, chunk flushing, and bounded payload capture without memory exhaustion.
func (e *Engine) handleForwardProxyHTTP(w http.ResponseWriter, req *http.Request, start time.Time) {
	traceID, parentSpanID, spanID, traceparent := tracing.ExtractOrGenerateContext(req.Header)
	req.Header.Set("traceparent", traceparent)
	req.Header.Set("X-DevHub-Trace-ID", traceID)
	req.Header.Set("X-DevHub-Span-ID", spanID)
	if parentSpanID != "" {
		req.Header.Set("X-DevHub-Parent-Span-ID", parentSpanID)
	}
	req.Header.Set("X-DevHub-Egress", "true")

	// Stream request body with bounded tee capture (does not buffer entire body in RAM)
	var reqCap *captureBuffer
	if req.Body != nil && req.Body != http.NoBody {
		reqCap = capturePool.Get().(*captureBuffer)
		reqCap.reset()
		req.Body = &bodySniffer{
			reader:  req.Body,
			capture: reqCap,
		}
	}

	outboundReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), req.Body)
	if err != nil {
		if reqCap != nil {
			capturePool.Put(reqCap)
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, v := range req.Header {
		outboundReq.Header[k] = v
	}

	// Use dedicated ForwardTransport (no environment proxy recursion)
	resp, err := e.ForwardTransport.RoundTrip(outboundReq)
	if err != nil {
		duration := time.Since(start).Milliseconds()
		var reqBodyStr string
		if reqCap != nil {
			reqBodyStr = reqCap.string()
			capturePool.Put(reqCap)
		}
		http.Error(w, fmt.Sprintf("DevHub Egress Proxy Error: %v", err), http.StatusBadGateway)
		e.emitTraceEvent(buffer.TraceEvent{
			ID:          fastTraceID(),
			TraceID:     traceID,
			Timestamp:   time.Now().Format("15:04:05.000"),
			Method:      req.Method,
			Path:        req.URL.Path,
			Target:      "[Egress Error] " + req.URL.Host + req.URL.Path,
			StatusCode:  http.StatusBadGateway,
			DurationMs:  duration,
			ReqHeaders:  extractHeaders(req.Header),
			RequestBody: reqBodyStr,
		})
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for k, v := range resp.Header {
		for _, val := range v {
			w.Header().Add(k, val)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// Stream response chunks continuously with immediate flushing
	respCap := capturePool.Get().(*captureBuffer)
	respCap.reset()

	buf := make([]byte, 32*1024)
	for {
		n, rErr := resp.Body.Read(buf)
		if n > 0 {
			respCap.write(buf[:n])
			if _, wErr := w.Write(buf[:n]); wErr != nil {
				break
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		if rErr != nil {
			break
		}
	}

	duration := time.Since(start).Milliseconds()

	var reqBodyStr string
	if reqCap != nil {
		reqBodyStr = reqCap.string()
		capturePool.Put(reqCap)
	}
	respBodyStr := respCap.string()
	capturePool.Put(respCap)

	e.emitTraceEvent(buffer.TraceEvent{
		ID:           fastTraceID(),
		TraceID:      traceID,
		Timestamp:    time.Now().Format("15:04:05.000"),
		Method:       req.Method,
		Path:         req.URL.Path,
		Target:       "[Egress Cascade] " + req.URL.Host + req.URL.Path,
		StatusCode:   resp.StatusCode,
		DurationMs:   duration,
		ReqHeaders:   extractHeaders(req.Header),
		RespHeaders:  extractHeaders(resp.Header),
		RequestBody:  reqBodyStr,
		ResponseBody: respBodyStr,
	})
}

// handleConnectTunnel handles HTTPS/TLS CONNECT forward proxy tunnels with full lifecycle duration tracking.
func (e *Engine) handleConnectTunnel(w http.ResponseWriter, req *http.Request, start time.Time) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var upstreamConn net.Conn
	var dialErr error

	if e.SecurityPolicy != nil {
		upstreamConn, dialErr = e.SecurityPolicy.DialValidatedContext(req.Context(), dialer, "tcp", req.URL.Host)
	} else {
		upstreamConn, dialErr = dialer.DialContext(req.Context(), "tcp", req.URL.Host)
	}

	if dialErr != nil {
		statusCode := http.StatusBadGateway
		if strings.Contains(dialErr.Error(), "blocked") || strings.Contains(dialErr.Error(), "forbidden") {
			statusCode = http.StatusForbidden
		}
		http.Error(w, fmt.Sprintf("DevHub CONNECT Error: %v", dialErr), statusCode)
		e.emitTraceEvent(buffer.TraceEvent{
			ID:         fastTraceID(),
			Timestamp:  time.Now().Format("15:04:05.000"),
			Method:     "CONNECT",
			Path:       req.URL.Host,
			Target:     "[Egress Blocked] " + req.URL.Host,
			StatusCode: statusCode,
			DurationMs: time.Since(start).Milliseconds(),
		})
		return
	}
	defer upstreamConn.Close()

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "CONNECT hijack unsupported", http.StatusInternalServerError)
		return
	}

	clientConn, brw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	brw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
	brw.Flush()

	traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)

	// Full-duplex pipe
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstreamConn, clientConn)
		errc <- err
	}()
	go func() {
		_, err := io.Copy(clientConn, upstreamConn)
		errc <- err
	}()
	<-errc

	// Record full tunnel session duration
	e.emitTraceEvent(buffer.TraceEvent{
		ID:          fastTraceID(),
		TraceID:     traceID,
		Timestamp:   time.Now().Format("15:04:05.000"),
		Method:      "CONNECT",
		Path:        req.URL.Host,
		Target:      "[Egress Tunnel] " + req.URL.Host,
		StatusCode:  http.StatusOK,
		DurationMs:  time.Since(start).Milliseconds(),
		ReqHeaders:  extractHeaders(req.Header),
		RespHeaders: map[string]string{"Proxy-Agent": "DevHub"},
	})
}

func isWebSocketRequest(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(req.Header.Get("Connection")), "upgrade")
}

// proxyWebSocket proxies WebSockets with TLS/WSS scheme awareness and verified upstream handshakes.
func (e *Engine) proxyWebSocket(w http.ResponseWriter, req *http.Request, targetURL *url.URL, outboundPath string, start time.Time) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "WebSocket hijack unsupported", http.StatusInternalServerError)
		return
	}

	clientConn, brw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	// Scheme-aware host & port resolution
	isTLS := targetURL.Scheme == "https" || targetURL.Scheme == "wss"
	defaultPort := "80"
	if isTLS {
		defaultPort = "443"
	}

	host := targetURL.Host
	if !hasPort(host) {
		host = host + ":" + defaultPort
	}

	traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)

	// Establish TCP or TLS connection to upstream
	var upstreamConn net.Conn
	if isTLS {
		hostOnly, _, _ := net.SplitHostPort(host)
		tlsConfig := &tls.Config{
			ServerName: hostOnly,
		}
		dialer := &net.Dialer{Timeout: 5 * time.Second}
		conn, dialErr := tls.DialWithDialer(dialer, "tcp", host, tlsConfig)
		if dialErr != nil {
			brw.WriteString("HTTP/1.1 502 Bad Gateway\r\n\r\n")
			brw.Flush()
			e.emitTraceEvent(buffer.TraceEvent{
				ID:         fastTraceID(),
				TraceID:    traceID,
				Timestamp:  time.Now().Format("15:04:05.000"),
				Method:     "WS",
				Path:       req.URL.Path,
				Target:     targetURL.String() + outboundPath,
				StatusCode: http.StatusBadGateway,
				DurationMs: time.Since(start).Milliseconds(),
				ReqHeaders: extractHeaders(req.Header),
			})
			return
		}
		upstreamConn = conn
	} else {
		conn, dialErr := net.DialTimeout("tcp", host, 5*time.Second)
		if dialErr != nil {
			brw.WriteString("HTTP/1.1 502 Bad Gateway\r\n\r\n")
			brw.Flush()
			e.emitTraceEvent(buffer.TraceEvent{
				ID:         fastTraceID(),
				TraceID:    traceID,
				Timestamp:  time.Now().Format("15:04:05.000"),
				Method:     "WS",
				Path:       req.URL.Path,
				Target:     targetURL.String() + outboundPath,
				StatusCode: http.StatusBadGateway,
				DurationMs: time.Since(start).Milliseconds(),
				ReqHeaders: extractHeaders(req.Header),
			})
			return
		}
		upstreamConn = conn
	}
	defer upstreamConn.Close()

	// Forward upgrade handshake request to upstream with preserved query parameters and subprotocols
	outboundReq := req.Clone(req.Context())
	outboundReq.URL.Path = outboundPath
	outboundReq.URL.RawQuery = req.URL.RawQuery
	outboundReq.Host = targetURL.Host

	if proto := req.Header.Get("Sec-WebSocket-Protocol"); proto != "" {
		outboundReq.Header.Set("Sec-WebSocket-Protocol", proto)
	}
	if ext := req.Header.Get("Sec-WebSocket-Extensions"); ext != "" {
		outboundReq.Header.Set("Sec-WebSocket-Extensions", ext)
	}

	if err := outboundReq.Write(upstreamConn); err != nil {
		brw.WriteString("HTTP/1.1 502 Bad Gateway\r\n\r\n")
		brw.Flush()
		return
	}

	// Read and validate upstream handshake response
	upstreamReader := bufio.NewReader(upstreamConn)
	upstreamResp, err := http.ReadResponse(upstreamReader, outboundReq)
	if err != nil {
		brw.WriteString("HTTP/1.1 502 Bad Gateway\r\n\r\n")
		brw.Flush()
		e.emitTraceEvent(buffer.TraceEvent{
			ID:         fastTraceID(),
			TraceID:    traceID,
			Timestamp:  time.Now().Format("15:04:05.000"),
			Method:     "WS",
			Path:       req.URL.Path,
			Target:     targetURL.String() + outboundPath,
			StatusCode: http.StatusBadGateway,
			DurationMs: time.Since(start).Milliseconds(),
			ReqHeaders: extractHeaders(req.Header),
		})
		return
	}

	// If upstream did NOT return 101 Switching Protocols, forward actual status code & abort upgrade
	if upstreamResp.StatusCode != http.StatusSwitchingProtocols {
		_ = upstreamResp.Write(brw)
		brw.Flush()
		e.emitTraceEvent(buffer.TraceEvent{
			ID:          fastTraceID(),
			TraceID:     traceID,
			Timestamp:   time.Now().Format("15:04:05.000"),
			Method:      "WS",
			Path:        req.URL.Path,
			Target:      targetURL.String() + outboundPath,
			StatusCode:  upstreamResp.StatusCode,
			DurationMs:  time.Since(start).Milliseconds(),
			ReqHeaders:  extractHeaders(req.Header),
			RespHeaders: extractHeaders(upstreamResp.Header),
		})
		return
	}

	// Forward successful 101 response to client
	if err := upstreamResp.Write(brw); err != nil {
		return
	}
	brw.Flush()

	// Bidirectional full-duplex copy
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstreamConn, brw)
		errc <- err
	}()
	go func() {
		_, err := io.Copy(clientConn, upstreamReader)
		errc <- err
	}()

	<-errc

	// Record full session lifecycle duration
	e.emitTraceEvent(buffer.TraceEvent{
		ID:          fastTraceID(),
		TraceID:     traceID,
		Timestamp:   time.Now().Format("15:04:05.000"),
		Method:      "WS",
		Path:        req.URL.Path,
		Target:      targetURL.String() + outboundPath,
		StatusCode:  http.StatusSwitchingProtocols,
		DurationMs:  time.Since(start).Milliseconds(),
		ReqHeaders:  extractHeaders(req.Header),
		RespHeaders: extractHeaders(upstreamResp.Header),
	})
}

// bodySniffer tees read bytes into pooled memory up to maxCaptureSize without allocating or truncating.
type bodySniffer struct {
	reader  io.ReadCloser
	capture *captureBuffer
}

func (s *bodySniffer) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)
	if n > 0 && s.capture != nil {
		s.capture.write(p[:n])
	}
	return n, err
}

func (s *bodySniffer) Close() error {
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

// responseCapturer captures HTTP status code and response body while supporting streaming (Flusher/Hijacker).
type responseCapturer struct {
	http.ResponseWriter
	statusCode int
	bodyBuf    []byte
	bodyLen    int
	wroteHead  bool
}

func (r *responseCapturer) reset(w http.ResponseWriter) {
	r.ResponseWriter = w
	r.statusCode = http.StatusOK
	r.bodyLen = 0
	r.wroteHead = false
}

func (r *responseCapturer) string() string {
	if r.bodyLen == 0 {
		return ""
	}
	return string(r.bodyBuf[:r.bodyLen])
}

func (r *responseCapturer) WriteHeader(code int) {
	if !r.wroteHead {
		r.statusCode = code
		r.wroteHead = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *responseCapturer) Write(b []byte) (int, error) {
	if !r.wroteHead {
		r.WriteHeader(http.StatusOK)
	}
	if r.bodyLen < maxCaptureSize {
		canWrite := maxCaptureSize - r.bodyLen
		if len(b) <= canWrite {
			copy(r.bodyBuf[r.bodyLen:], b)
			r.bodyLen += len(b)
		} else {
			copy(r.bodyBuf[r.bodyLen:], b[:canWrite])
			r.bodyLen += canWrite
		}
	}
	n, err := r.ResponseWriter.Write(b)

	// Immediate flush for SSE streams (Server-Sent Events)
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}

	return n, err
}

func (r *responseCapturer) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *responseCapturer) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := r.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not support http.Hijacker")
}

func (r *responseCapturer) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// fastTraceID generates unique trace IDs using atomic counters and fast hex encoding
func fastTraceID() string {
	val := atomic.AddUint64(&traceCounter, 1)
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

// Fallback random hex generator if needed
func cryptoID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// extractHeaders converts http.Header to map[string]string for trace inspection.
func extractHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

func hasPort(host string) bool {
	_, _, err := net.SplitHostPort(host)
	return err == nil
}
