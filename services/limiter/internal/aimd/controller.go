// Package aimd implements a token bucket whose refill rate adapts with
// additive-increase/multiplicative-decrease, the same control loop TCP uses
// for congestion: probe upward slowly, back off hard on any sign of pressure.
package aimd

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"
)

type Outcome int

const (
	OK Outcome = iota
	Throttled
	Error
)

type Config struct {
	InitialRate float64
	MinRate     float64
	MaxRate     float64
	Increase    float64       // permits/s added per healthy window
	Decrease    float64       // multiplier applied on pressure, in (0,1)
	Window      time.Duration // adaptation period
	LatencyHigh time.Duration // p95 above this is pressure
	LatencyLow  time.Duration // p95 below this allows growth
	Burst       float64       // bucket capacity in permits
}

func DefaultConfig() Config {
	return Config{
		InitialRate: 2,
		MinRate:     1,
		MaxRate:     50,
		Increase:    0.5,
		Decrease:    0.5,
		Window:      3 * time.Second,
		LatencyHigh: 3 * time.Second,
		LatencyLow:  1500 * time.Millisecond,
		Burst:       2,
	}
}

type Snapshot struct {
	Rate      float64
	P95       time.Duration
	Throttled int
	Errors    int
	Samples   int
}

type window struct {
	latencies []time.Duration
	throttled int
	errors    int
}

type Controller struct {
	cfg Config
	now func() time.Time

	mu         sync.Mutex
	rate       float64
	tokens     float64
	lastRefill time.Time
	windowEnd  time.Time
	cur        window
	last       Snapshot
}

func New(cfg Config, now func() time.Time) *Controller {
	t := now()
	return &Controller{
		cfg:        cfg,
		now:        now,
		rate:       cfg.InitialRate,
		tokens:     cfg.Burst,
		lastRefill: t,
		windowEnd:  t.Add(cfg.Window),
		last:       Snapshot{Rate: cfg.InitialRate},
	}
}

// Acquire reserves one permit, sleeping until the bucket can honour it.
// The reservation is made under the lock; the wait happens outside it so a
// slow caller never blocks the others.
func (c *Controller) Acquire(ctx context.Context) (time.Duration, float64, error) {
	c.mu.Lock()
	c.refill()
	c.tokens--
	wait := time.Duration(0)
	if c.tokens < 0 {
		wait = time.Duration(-c.tokens / c.rate * float64(time.Second))
	}
	rate := c.rate
	c.mu.Unlock()

	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			c.mu.Lock()
			c.tokens++
			c.mu.Unlock()
			return 0, rate, ctx.Err()
		case <-timer.C:
		}
	}
	return wait, rate, nil
}

func (c *Controller) Report(latency time.Duration, outcome Outcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotate()
	c.cur.latencies = append(c.cur.latencies, latency)
	switch outcome {
	case Throttled:
		c.cur.throttled++
	case Error:
		c.cur.errors++
	}
}

// Tick closes the current window if it has elapsed. Call it periodically so
// the rate also adapts when traffic stops arriving.
func (c *Controller) Tick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotate()
}

func (c *Controller) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.last
	s.Rate = c.rate
	return s
}

func (c *Controller) refill() {
	now := c.now()
	elapsed := now.Sub(c.lastRefill).Seconds()
	c.lastRefill = now
	c.tokens = math.Min(c.cfg.Burst, c.tokens+elapsed*c.rate)
}

func (c *Controller) rotate() {
	now := c.now()
	if now.Before(c.windowEnd) {
		return
	}
	snap := Snapshot{
		P95:       p95(c.cur.latencies),
		Throttled: c.cur.throttled,
		Errors:    c.cur.errors,
		Samples:   len(c.cur.latencies),
	}
	switch {
	case snap.Throttled > 0 || snap.Errors > 0 || (snap.Samples > 0 && snap.P95 > c.cfg.LatencyHigh):
		c.rate = math.Max(c.cfg.MinRate, c.rate*c.cfg.Decrease)
	case snap.Samples > 0 && snap.P95 < c.cfg.LatencyLow:
		c.rate = math.Min(c.cfg.MaxRate, c.rate+c.cfg.Increase)
	}
	snap.Rate = c.rate
	c.last = snap
	c.cur = window{}
	// Skip whole windows without traffic instead of replaying them one by one.
	missed := now.Sub(c.windowEnd) / c.cfg.Window
	c.windowEnd = c.windowEnd.Add((missed + 1) * c.cfg.Window)
}

func p95(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(0.95*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}
