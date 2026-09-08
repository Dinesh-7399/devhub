package proxy

import (
	"bufio"
	"crypto/tls"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/netip"
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
	Router    *router.Router
	Ring      *buffer.RingBuffer
	Health    *HealthMonitor
	Mocks     *mock.Manager
	Broadcast func(buffer.TraceEvent)
	HTTPProxy *httputil.ReverseProxy
	Transport http.RoundTripper
	Egress    http.RoundTripper
	RedactPII bool

	blockPrivateEgress bool
	allowedEgressHosts map[string]struct{}
}

// NewEngine initializes a high-performance streaming reverse and forward proxy engine.
func NewEngine(r *router.Router, ring *buffer.RingBuffer, broadcaster func(buffer.TraceEvent)) *Engine {
	health := NewHealthMonitor()
	mocks := mock.NewManager()

	// Custom high-throughput transport with tuned connection pooling
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
	egressTransport := ingressTransport.Clone()

	e := &Engine{
		Router:    r,
		Ring:      ring,
		Health:    health,
		Mocks:     mocks,
		Broadcast: broadcaster,
		Transport: ingressTransport,
		Egress:    egressTransport,
		RedactPII: true,

		blockPrivateEgress: true,
		allowedEgressHosts: make(map[string]struct{}),
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

// ServeHTTP handles incoming client requests, forward proxy (HTTP_PROXY), route matches, and WebSockets.
func (e *Engine) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()

	// 1. Forward Proxy CONNECT Tunnel (HTTPS / raw TCP egress cascading)
	if req.Method == http.MethodConnect {
		e.handleConnectTunnel(w, req, start)
		return
	}

	// 2. Forward Proxy HTTP Egress (Zero-code Service A -> Service B cascading via HTTP_PROXY)
	if isForwardProxyHTTPRequest(req) {
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

			event := buffer.TraceEvent{
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
			}
			if e.Ring != nil {
				e.Ring.Push(event)
			}
			if e.Broadcast != nil {
				e.Broadcast(event)
			}
			return
		}
	}

	route, outboundPath, matched := e.Router.Match(req.URL.Path)
	if !matched {
		http.Error(w, "DevHub: No registered route for path "+req.URL.Path, http.StatusNotFound)
		return
	}

	// 4. Transparent WebSocket Upgrade Interception
	if isWebSocketRequest(req) {
		e.proxyWebSocket(w, req, route.Target, outboundPath, start)
		return
	}

	// 5. Active circuit breaker check
	if e.Health != nil && !e.Health.IsHealthy(route.Target) {
		// If mock fallback exists on error
		if e.Mocks != nil {
			if mockRule, found := e.Mocks.GetRule(req.URL.Path); found && mockRule.Enabled {
				for k, v := range mockRule.Headers {
					w.Header().Set(k, v)
				}
				w.Header().Set("X-DevHub-Mock-Fallback", "true")
				w.WriteHeader(mockRule.StatusCode)
				w.Write([]byte(mockRule.Body))
				return
			}
		}

		http.Error(w, fmt.Sprintf("DevHub: Upstream %s is currently marked UNHEALTHY (Circuit Broken)", route.Target), http.StatusServiceUnavailable)
		return
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

	if e.RedactPII {
		reqHeaders = tracing.RedactHeaders(reqHeaders)
		respHeaders = tracing.RedactHeaders(respHeaders)
		reqBodyStr = tracing.RedactBody(reqBodyStr)
		respBodyStr = tracing.RedactBody(respBodyStr)
	}

	event := buffer.TraceEvent{
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
	}

	// Recycle response capturer
	capturerPool.Put(rw)

	if e.Ring != nil {
		e.Ring.Push(event)
	}
	if e.Broadcast != nil {
		e.Broadcast(event)
	}
}

// handleForwardProxyHTTP handles HTTP requests made via HTTP_PROXY environment variables (Service A -> Service B).
func (e *Engine) handleForwardProxyHTTP(w http.ResponseWriter, req *http.Request, start time.Time) {
	traceID, parentSpanID, spanID, traceparent := tracing.ExtractOrGenerateContext(req.Header)
	req.Header.Set("traceparent", traceparent)
	req.Header.Set("X-DevHub-Trace-ID", traceID)
	req.Header.Set("X-DevHub-Span-ID", spanID)
	if parentSpanID != "" {
		req.Header.Set("X-DevHub-Parent-Span-ID", parentSpanID)
	}
	req.Header.Set("X-DevHub-Egress", "true")

	// Stream body while capping capture in memory.
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, v := range req.Header {
		outboundReq.Header[k] = v
	}

	resp, err := e.Egress.RoundTrip(outboundReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("DevHub Egress Proxy Error: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		for _, val := range v {
			w.Header().Add(k, val)
		}
	}
	w.WriteHeader(resp.StatusCode)

	respCap := capturePool.Get().(*captureBuffer)
	respCap.reset()
	_, _ = io.Copy(w, io.TeeReader(resp.Body, &captureWriter{capture: respCap}))
	duration := time.Since(start).Milliseconds()

	var reqBodyStr string
	if reqCap != nil {
		reqBodyStr = reqCap.string()
		capturePool.Put(reqCap)
	}
	respBodyStr := respCap.string()
	capturePool.Put(respCap)

	event := buffer.TraceEvent{
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
	}
	if e.Ring != nil {
		e.Ring.Push(event)
	}
	if e.Broadcast != nil {
		e.Broadcast(event)
	}
}

// handleConnectTunnel handles HTTPS/TLS CONNECT forward proxy tunnels.
func (e *Engine) handleConnectTunnel(w http.ResponseWriter, req *http.Request, start time.Time) {
	if !e.isEgressAllowed(req.URL.Host, true) {
		http.Error(w, "CONNECT target blocked by egress policy", http.StatusForbidden)
		return
	}

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

	upstreamConn, err := net.DialTimeout("tcp", req.URL.Host, 10*time.Second)
	if err != nil {
		brw.WriteString("HTTP/1.1 502 Bad Gateway\r\n\r\n")
		brw.Flush()
		return
	}
	defer upstreamConn.Close()

	brw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
	brw.Flush()

	traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)

	event := buffer.TraceEvent{
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
	}
	if e.Ring != nil {
		e.Ring.Push(event)
	}
	if e.Broadcast != nil {
		e.Broadcast(event)
	}

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
}

func isWebSocketRequest(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(req.Header.Get("Connection")), "upgrade")
}

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

	// Dial upstream target TCP socket
	upstreamConn, err := dialWebSocketUpstream(targetURL)
	if err != nil {
		brw.WriteString("HTTP/1.1 502 Bad Gateway\r\n\r\n")
		brw.Flush()
		return
	}
	defer upstreamConn.Close()

	// Forward raw upgrade request to upstream
	req.URL.Path = outboundPath
	req.Write(upstreamConn)

	traceID, _, _, _ := tracing.ExtractOrGenerateContext(req.Header)

	// Record WebSocket session start trace
	event := buffer.TraceEvent{
		ID:          fastTraceID(),
		TraceID:     traceID,
		Timestamp:   time.Now().Format("15:04:05.000"),
		Method:      "WS",
		Path:        req.URL.Path,
		Target:      targetURL.String() + outboundPath,
		StatusCode:  http.StatusSwitchingProtocols,
		DurationMs:  time.Since(start).Milliseconds(),
		ReqHeaders:  extractHeaders(req.Header),
		RespHeaders: map[string]string{"Upgrade": "websocket", "Connection": "Upgrade"},
	}
	if e.Ring != nil {
		e.Ring.Push(event)
	}
	if e.Broadcast != nil {
		e.Broadcast(event)
	}

	// Bidirectional full-duplex copy
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstreamConn, brw)
		errc <- err
	}()
	go func() {
		_, err := io.Copy(clientConn, upstreamConn)
		errc <- err
	}()

	<-errc
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

type captureWriter struct {
	capture *captureBuffer
}

func (w *captureWriter) Write(p []byte) (int, error) {
	if w.capture != nil {
		w.capture.write(p)
	}
	return len(p), nil
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

// ConfigureEgressPolicy updates allowed host patterns and private-network blocking behavior.
func (e *Engine) ConfigureEgressPolicy(allowedHosts []string, blockPrivate bool) {
	e.blockPrivateEgress = blockPrivate
	e.allowedEgressHosts = make(map[string]struct{}, len(allowedHosts))
	for _, host := range allowedHosts {
		trimmed := strings.ToLower(strings.TrimSpace(host))
		if trimmed == "" {
			continue
		}
		e.allowedEgressHosts[trimmed] = struct{}{}
	}
}

func isForwardProxyHTTPRequest(req *http.Request) bool {
	if req == nil || !req.URL.IsAbs() || req.URL.Host == "" || req.Host == "" {
		return false
	}
	return normalizeHost(req.Host) == normalizeHost(req.URL.Host)
}

func normalizeHost(hostport string) string {
	hostport = strings.TrimSpace(strings.ToLower(hostport))
	if hostport == "" {
		return ""
	}
	host := hostport
	port := ""
	if h, p, err := net.SplitHostPort(hostport); err == nil {
		host = h
		port = p
	}
	host = strings.Trim(host, "[]")
	if port == "80" || port == "443" {
		return host
	}
	if port == "" {
		return host
	}
	return net.JoinHostPort(host, port)
}

func dialWebSocketUpstream(targetURL *url.URL) (net.Conn, error) {
	host := targetURL.Hostname()
	port := targetURL.Port()
	secure := strings.EqualFold(targetURL.Scheme, "wss") || strings.EqualFold(targetURL.Scheme, "https")
	if port == "" {
		if secure {
			port = "443"
		} else {
			port = "80"
		}
	}
	addr := net.JoinHostPort(host, port)
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if secure {
		return tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: host,
		})
	}
	return dialer.Dial("tcp", addr)
}

func (e *Engine) isEgressAllowed(target string, assumeHTTPSPort bool) bool {
	host := target
	port := ""
	if h, p, err := net.SplitHostPort(target); err == nil {
		host = h
		port = p
	} else if assumeHTTPSPort {
		port = "443"
	}

	host = strings.Trim(strings.ToLower(host), "[]")
	hostPort := host
	if port != "" {
		hostPort = net.JoinHostPort(host, port)
	}

	if len(e.allowedEgressHosts) > 0 {
		if _, ok := e.allowedEgressHosts[host]; !ok {
			if _, ok := e.allowedEgressHosts[hostPort]; !ok {
				return false
			}
		}
	}

	if !e.blockPrivateEgress {
		return true
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return false
	}

	if ip := net.ParseIP(host); ip != nil {
		return !isPrivateIP(ip)
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}
	for _, ip := range ips {
		if isPrivateIP(ip) {
			return false
		}
	}
	return true
}

func isPrivateIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return true
	}
	return addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() ||
		addr.IsUnspecified()
}
