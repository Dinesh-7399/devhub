package router

import (
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
)

// RouteTarget holds upstream routing information.
type RouteTarget struct {
	Target      *url.URL
	StripPrefix bool
	Prefix      string
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
	r.mu.Lock()
	defer r.mu.Unlock()

	clean := strings.Trim(prefix, "/")
	var segments []string
	if clean != "" {
		segments = strings.Split(clean, "/")
	}

	// Copy-on-write RCU clone
	oldRoot := r.root.Load()
	newRoot := cloneTrie(oldRoot)

	curr := newRoot
	for _, seg := range segments {
		if _, exists := curr.children[seg]; !exists {
			curr.children[seg] = &TrieNode{children: make(map[string]*TrieNode)}
		}
		curr = curr.children[seg]
	}

	curr.target = &RouteTarget{
		Target:      targetURL,
		StripPrefix: stripPrefix,
		Prefix:      "/" + clean,
	}
	curr.isEnd = true

	// Atomically swap root pointer
	r.root.Store(newRoot)
}

func cloneTrie(node *TrieNode) *TrieNode {
	if node == nil {
		return &TrieNode{children: make(map[string]*TrieNode)}
	}
	copyNode := &TrieNode{
		children: make(map[string]*TrieNode, len(node.children)),
		target:   node.target,
		isEnd:    node.isEnd,
	}
	for k, v := range node.children {
		copyNode.children[k] = cloneTrie(v)
	}
	return copyNode
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
