// Package vclock implements vector clocks: one counter per node that has
// coordinated a write, so replicas can tell causal order from true conflicts.
package vclock

import (
	"fmt"
	"sort"
	"strings"
)

type Clock map[string]uint64

type Order int

const (
	Equal Order = iota
	Before
	After
	Concurrent
)

func (o Order) String() string {
	return [...]string{"equal", "before", "after", "concurrent"}[o]
}

func (c Clock) Copy() Clock {
	out := make(Clock, len(c)+1)
	for k, v := range c {
		out[k] = v
	}
	return out
}

// Increment returns a copy with node's counter advanced by one.
func (c Clock) Increment(node string) Clock {
	out := c.Copy()
	out[node]++
	return out
}

// Compare tells how a relates to b.
func Compare(a, b Clock) Order {
	aAhead, bAhead := false, false
	for node, av := range a {
		if bv := b[node]; av > bv {
			aAhead = true
		} else if av < bv {
			bAhead = true
		}
	}
	for node, bv := range b {
		if _, ok := a[node]; !ok && bv > 0 {
			bAhead = true
		}
	}
	switch {
	case aAhead && bAhead:
		return Concurrent
	case aAhead:
		return After
	case bAhead:
		return Before
	default:
		return Equal
	}
}

// Merge returns the element-wise maximum, the smallest clock that descends
// from both inputs.
func Merge(clocks ...Clock) Clock {
	out := Clock{}
	for _, c := range clocks {
		for node, v := range c {
			if v > out[node] {
				out[node] = v
			}
		}
	}
	return out
}

// Descends reports whether a is causally at or after b.
func Descends(a, b Clock) bool {
	o := Compare(a, b)
	return o == After || o == Equal
}

func (c Clock) String() string {
	nodes := make([]string, 0, len(c))
	for n := range c {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		parts = append(parts, fmt.Sprintf("%s:%d", n, c[n]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
