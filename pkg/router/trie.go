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
	TimeoutMs   int64
}

// TrieNode represents a path segment node in the routing trie.
type TrieNode struct {
	children map[string]*TrieNode
	target   *RouteTarget
	isEnd    bool
}

// clone creates a shallow copy of the node's map and metadata for persistent path-copying.
// Children subtrees are shared unless explicitly cloned along the modified path.
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

// Router provides a lock-free read path using atomic pointers with synchronized
// copy-on-write persistent path updates. Route lookup executes with zero mutex locking.
type Router struct {
	root atomic.Pointer[TrieNode]
	mu   sync.Mutex // Mutex guards route updates; reads never acquire locks
}

// NewRouter constructs an empty prefix router.
func NewRouter() *Router {
	r := &Router{}
	root := &TrieNode{children: make(map[string]*TrieNode)}
	r.root.Store(root)
	return r
}

// Add registers a URL prefix route to an upstream target using copy-on-write persistent path-copying.
// Only nodes along the route path are cloned; all untouched branches remain shared.
func (r *Router) Add(prefix string, targetURL *url.URL, stripPrefix bool) {
	r.AddWithTimeout(prefix, targetURL, stripPrefix, 0)
}

// AddWithTimeout registers a URL prefix route with an optional per-route execution timeout.
func (r *Router) AddWithTimeout(prefix string, targetURL *url.URL, stripPrefix bool, timeoutMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	clean := strings.Trim(prefix, "/")
	var segments []string
	if clean != "" {
		segments = strings.Split(clean, "/")
	}

	oldRoot := r.root.Load()
	newRoot := oldRoot.clone()

	curr := newRoot
	for _, seg := range segments {
		child, exists := curr.children[seg]
		var nextNode *TrieNode
		if exists {
			nextNode = child.clone()
		} else {
			nextNode = &TrieNode{children: make(map[string]*TrieNode)}
		}
		curr.children[seg] = nextNode
		curr = nextNode
	}

	curr.target = &RouteTarget{
		Target:      targetURL,
		StripPrefix: stripPrefix,
		Prefix:      "/" + clean,
		TimeoutMs:   timeoutMs,
	}
	curr.isEnd = true

	// Atomically swap root pointer
	r.root.Store(newRoot)
}

// Remove unmounts a URL prefix route using copy-on-write persistent path-copying.
// Returns true if the route was found and removed.
func (r *Router) Remove(prefix string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	clean := strings.Trim(prefix, "/")
	var segments []string
	if clean != "" {
		segments = strings.Split(clean, "/")
	}

	oldRoot := r.root.Load()
	if oldRoot == nil {
		return false
	}

	newRoot := oldRoot.clone()

	// Track cloned path
	type step struct {
		node *TrieNode
		seg  string
	}
	path := []step{{node: newRoot, seg: ""}}

	curr := newRoot
	for _, seg := range segments {
		child, exists := curr.children[seg]
		if !exists {
			return false // Route not present
		}
		newChild := child.clone()
		curr.children[seg] = newChild
		path = append(path, step{node: newChild, seg: seg})
		curr = newChild
	}

	if !curr.isEnd {
		return false
	}

	curr.isEnd = false
	curr.target = nil

	// Prune dead nodes bottom-up
	for i := len(path) - 1; i > 0; i-- {
		n := path[i].node
		seg := path[i].seg
		parent := path[i-1].node

		if len(n.children) == 0 && !n.isEnd {
			delete(parent.children, seg)
		} else {
			break
		}
	}

	r.root.Store(newRoot)
	return true
}

// Match finds the longest registered prefix matching the path.
// Operates lock-free on the read path.
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
		i = start + len(segment)
		// Skip trailing slashes
		for i < len(path) && path[i] == '/' {
			i++
		}

		if curr.isEnd {
			lastMatchedTarget = curr.target
			matchedIndex = i
		}
	}

	if lastMatchedTarget == nil {
		return nil, "", false
	}

	outboundPath := path
	if lastMatchedTarget.StripPrefix {
		if matchedIndex >= len(path) {
			outboundPath = "/"
		} else {
			outboundPath = path[matchedIndex:]
			if !strings.HasPrefix(outboundPath, "/") {
				outboundPath = "/" + outboundPath
			}
		}
	}

	return lastMatchedTarget, outboundPath, true
}
