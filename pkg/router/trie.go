package router

import (
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RouteTarget holds upstream routing information.
type RouteTarget struct {
	Target      *url.URL
	StripPrefix bool
	Prefix      string
	Timeout     time.Duration
}

// TrieNode represents a path segment node in the routing trie.
type TrieNode struct {
	children map[string]*TrieNode
	target   *RouteTarget
	isEnd    bool
}

// cloneNode creates a deep copy of a trie node for lock-free RCU updates.
func (n *TrieNode) clone() *TrieNode {
	if n == nil {
		return &TrieNode{children: make(map[string]*TrieNode)}
	}
	newNode := &TrieNode{
		children: make(map[string]*TrieNode, len(n.children)),
		target:   n.target,
		isEnd:    n.isEnd,
	}
	for k, v := range n.children {
		newNode.children[k] = v
	}
	return newNode
}

// Router provides 100% LOCK-FREE Longest-Prefix-Matching (LPM) route resolution in O(k) time
// using Read-Copy-Update (RCU) atomic pointers. Reads execute with zero mutex locking and zero allocations.
type Router struct {
	root atomic.Pointer[TrieNode]
	mu   sync.Mutex // Mutex is used ONLY during route registration/writes, never on reads
}

// NewRouter constructs an empty lock-free prefix router.
func NewRouter() *Router {
	r := &Router{}
	root := &TrieNode{children: make(map[string]*TrieNode)}
	r.root.Store(root)
	return r
}

// Add registers a URL prefix route to an upstream target using atomic RCU copy.
func (r *Router) Add(prefix string, targetURL *url.URL, stripPrefix bool) {
	r.AddWithTimeout(prefix, targetURL, stripPrefix, 0)
}

// AddWithTimeout registers a URL prefix route with an optional per-route timeout.
func (r *Router) AddWithTimeout(prefix string, targetURL *url.URL, stripPrefix bool, timeout time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	clean := strings.Trim(prefix, "/")
	var segments []string
	if clean != "" {
		segments = strings.Split(clean, "/")
	}

	oldRoot := r.root.Load()
	newRoot := oldRoot.clone()
	currOld := oldRoot
	currNew := newRoot
	for _, seg := range segments {
		var oldChild *TrieNode
		if currOld != nil {
			oldChild = currOld.children[seg]
		}
		var newChild *TrieNode
		if oldChild != nil {
			newChild = oldChild.clone()
		} else {
			newChild = &TrieNode{children: make(map[string]*TrieNode)}
		}
		currNew.children[seg] = newChild
		currNew = newChild
		currOld = oldChild
	}

	currNew.target = &RouteTarget{
		Target:      targetURL,
		StripPrefix: stripPrefix,
		Prefix:      "/" + clean,
		Timeout:     timeout,
	}
	currNew.isEnd = true

	// Atomically swap root pointer
	r.root.Store(newRoot)
}

// Match finds the longest registered prefix matching the path.
// Operates with 100% LOCK-FREE execution and ZERO heap allocations.
func (r *Router) Match(path string) (*RouteTarget, string, bool) {
	curr := r.root.Load()
	if curr == nil {
		return nil, "", false
	}

	if len(path) == 0 {
		path = "/"
	}

	var lastMatchedTarget *RouteTarget
	matchedIndex := 0

	// Check if root '/' route is defined
	if curr.isEnd {
		lastMatchedTarget = curr.target
	}

	i := 0
	// Skip leading slashes
	for i < len(path) && path[i] == '/' {
		i++
	}

	for i < len(path) {
		start := i
		for i < len(path) && path[i] != '/' {
			i++
		}
		segment := path[start:i]

		next, exists := curr.children[segment]
		if !exists {
			break
		}
		curr = next
		if curr.isEnd {
			lastMatchedTarget = curr.target
			matchedIndex = i
		}

		for i < len(path) && path[i] == '/' {
			i++
		}
	}

	if lastMatchedTarget == nil {
		return nil, "", false
	}

	outboundPath := path
	if lastMatchedTarget.StripPrefix && lastMatchedTarget.Prefix != "/" {
		if matchedIndex >= len(path) {
			outboundPath = "/"
		} else {
			outboundPath = path[matchedIndex:]
			if len(outboundPath) == 0 || outboundPath[0] != '/' {
				outboundPath = "/" + outboundPath
			}
		}
	}

	return lastMatchedTarget, outboundPath, true
}
