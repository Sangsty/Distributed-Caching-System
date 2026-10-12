// Package ring implements a consistent-hash ring with virtual nodes.
//
// Nodes and keys are hashed onto the same 64-bit circle. A key belongs to the
// first virtual node at or after the key's position, wrapping around at the
// end. Adding or removing a node therefore only moves the keys adjacent to
// that node's virtual nodes (about 1/N of all keys), instead of remapping
// almost everything like hash(key) % N does.
//
// The package is pure logic: no I/O, no goroutines. A Ring is safe for
// concurrent use.
package ring

import (
	"sort"
	"strconv"
	"sync"
)

// DefaultVNodes is the number of virtual nodes per physical node. More vnodes
// give a more even key distribution at the cost of memory and slower
// AddNode/RemoveNode. See cmd/experiment (balance.csv) for the measurements.
const DefaultVNodes = 150

type entry struct {
	hash uint64
	node string
}

// Ring is a consistent-hash ring with virtual nodes.
type Ring struct {
	mu      sync.RWMutex
	vnodes  int
	entries []entry // sorted by (hash, node); the tie-break makes lookups deterministic
	nodes   map[string]struct{}
}

// New creates an empty ring. vnodes < 1 selects DefaultVNodes.
func New(vnodes int) *Ring {
	if vnodes < 1 {
		vnodes = DefaultVNodes
	}
	return &Ring{vnodes: vnodes, nodes: make(map[string]struct{})}
}

// VNodes returns the number of virtual nodes per physical node.
func (r *Ring) VNodes() int { return r.vnodes }

// AddNode inserts a node. It returns false (and changes nothing) if the node
// is already present.
func (r *Ring) AddNode(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; ok {
		return false
	}
	r.nodes[id] = struct{}{}
	for i := 0; i < r.vnodes; i++ {
		// "<id>#<i>" is unambiguous: the index is digits only, so two different
		// (id, i) pairs can never produce the same label.
		r.entries = append(r.entries, entry{hash: Hash(id + "#" + strconv.Itoa(i)), node: id})
	}
	sort.Slice(r.entries, func(a, b int) bool {
		ea, eb := r.entries[a], r.entries[b]
		if ea.hash != eb.hash {
			return ea.hash < eb.hash
		}
		return ea.node < eb.node
	})
	return true
}

// RemoveNode deletes a node. It returns false if the node was not present.
func (r *Ring) RemoveNode(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; !ok {
		return false
	}
	delete(r.nodes, id)
	kept := make([]entry, 0, len(r.entries))
	for _, e := range r.entries {
		if e.node != id {
			kept = append(kept, e)
		}
	}
	r.entries = kept // filtering keeps the sorted order
	return true
}

// GetNode returns the node that owns key. ok is false if the ring is empty.
func (r *Ring) GetNode(key string) (node string, ok bool) {
	return r.lookup(Hash(key))
}

func (r *Ring) lookup(h uint64) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := len(r.entries)
	if n == 0 {
		return "", false
	}
	// First virtual node whose position is >= h; past the end wraps to index 0.
	i := sort.Search(n, func(i int) bool { return r.entries[i].hash >= h })
	if i == n {
		i = 0
	}
	return r.entries[i].node, true
}

// Has reports whether the node is on the ring.
func (r *Ring) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.nodes[id]
	return ok
}

// GetNodes returns the node IDs in sorted order.
func (r *Ring) GetNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// NodeCount returns the number of physical nodes.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Ownership returns, for each node, the fraction (0..1) of the hash space it
// owns. Fractions sum to 1 for a non-empty ring. The dashboard uses this for
// the ring visualisation.
func (r *Ring) Ownership() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]float64, len(r.nodes))
	for id := range r.nodes {
		out[id] = 0
	}
	n := len(r.entries)
	if n == 0 {
		return out
	}
	if len(r.nodes) == 1 {
		for id := range r.nodes {
			out[id] = 1
		}
		return out
	}
	const space = 18446744073709551616.0 // 2^64
	for i, e := range r.entries {
		// Entry i owns the arc (previous position, its own position]. The
		// uint64 subtraction wraps correctly for the first entry's arc.
		prev := r.entries[(i+n-1)%n].hash
		out[e.node] += float64(e.hash-prev) / space
	}
	return out
}
