// Package ring places nodes on a consistent-hashing ring so that adding or
// removing one node only moves the keys adjacent to it.
package ring

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type point struct {
	hash uint64
	node string
}

type Ring struct {
	vnodes int

	mu     sync.RWMutex
	points []point
	nodes  map[string]struct{}
}

func New(vnodes int) *Ring {
	return &Ring{vnodes: vnodes, nodes: map[string]struct{}{}}
}

// Hash uses SHA-1 rather than a fast non-cryptographic hash: short, similar
// node names such as "catalog1#0" and "catalog2#0" must spread evenly.
func Hash(s string) uint64 {
	sum := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; ok {
		return
	}
	r.nodes[node] = struct{}{}
	for i := 0; i < r.vnodes; i++ {
		r.points = append(r.points, point{hash: Hash(fmt.Sprintf("%s#%d", node, i)), node: node})
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
}

func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; !ok {
		return
	}
	delete(r.nodes, node)
	kept := r.points[:0]
	for _, p := range r.points {
		if p.node != node {
			kept = append(kept, p)
		}
	}
	r.points = kept
}

func (r *Ring) Has(node string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.nodes[node]
	return ok
}

func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Preference returns up to n distinct nodes, walking clockwise from the key's
// position. The first entry is the key's owner.
func (r *Ring) Preference(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	h := Hash(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		if _, dup := seen[p.node]; dup {
			continue
		}
		seen[p.node] = struct{}{}
		out = append(out, p.node)
	}
	return out
}

func (r *Ring) Owner(key string) string {
	pref := r.Preference(key, 1)
	if len(pref) == 0 {
		return ""
	}
	return pref[0]
}
