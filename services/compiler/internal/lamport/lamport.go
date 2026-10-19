// Package lamport implements a Lamport logical clock: a counter that only
// moves forward and that is advanced past any timestamp it witnesses, so the
// happened-before relation between events is preserved without wall clocks.
package lamport

import "sync"

type Clock struct {
	mu   sync.Mutex
	time uint64
}

// Tick stamps a local event.
func (c *Clock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time++
	return c.time
}

// Witness incorporates a timestamp received from another process and returns
// the timestamp of the receive event, which is later than both.
func (c *Clock) Witness(remote uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if remote > c.time {
		c.time = remote
	}
	c.time++
	return c.time
}

func (c *Clock) Now() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.time
}
