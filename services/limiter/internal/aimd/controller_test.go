package aimd

import (
	"context"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }
func (c *clock) nextWindow(cfg Config)   { c.advance(cfg.Window) }
func newTestController() (*Controller, *clock, Config) {
	cfg := DefaultConfig()
	cfg.InitialRate = 4
	cfg.Burst = 2
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	return New(cfg, clk.now), clk, cfg
}

func reportN(c *Controller, n int, latency time.Duration, outcome Outcome) {
	for i := 0; i < n; i++ {
		c.Report(latency, outcome)
	}
}

func TestHealthyWindowIncreasesAdditively(t *testing.T) {
	c, clk, cfg := newTestController()
	reportN(c, 10, 500*time.Millisecond, OK)
	clk.nextWindow(cfg)
	c.Tick()
	if got := c.Snapshot().Rate; got != cfg.InitialRate+cfg.Increase {
		t.Fatalf("rate = %v, want %v", got, cfg.InitialRate+cfg.Increase)
	}
}

func TestHighLatencyHalvesRate(t *testing.T) {
	c, clk, cfg := newTestController()
	reportN(c, 18, 500*time.Millisecond, OK)
	reportN(c, 2, 5*time.Second, OK) // 10% slow samples move the p95
	clk.nextWindow(cfg)
	c.Tick()
	if got := c.Snapshot().Rate; got != cfg.InitialRate*cfg.Decrease {
		t.Fatalf("rate = %v, want %v", got, cfg.InitialRate*cfg.Decrease)
	}
}

func TestThrottledResponseHalvesRateEvenWhenFast(t *testing.T) {
	c, clk, cfg := newTestController()
	reportN(c, 10, 100*time.Millisecond, OK)
	c.Report(100*time.Millisecond, Throttled)
	clk.nextWindow(cfg)
	c.Tick()
	if got := c.Snapshot().Rate; got != cfg.InitialRate*cfg.Decrease {
		t.Fatalf("rate = %v, want %v", got, cfg.InitialRate*cfg.Decrease)
	}
}

func TestRateStaysWithinBounds(t *testing.T) {
	c, clk, cfg := newTestController()
	for i := 0; i < 20; i++ {
		c.Report(100*time.Millisecond, Throttled)
		clk.nextWindow(cfg)
		c.Tick()
	}
	if got := c.Snapshot().Rate; got != cfg.MinRate {
		t.Fatalf("rate = %v, want floor %v", got, cfg.MinRate)
	}
	for i := 0; i < 1000; i++ {
		c.Report(100*time.Millisecond, OK)
		clk.nextWindow(cfg)
		c.Tick()
	}
	if got := c.Snapshot().Rate; got != cfg.MaxRate {
		t.Fatalf("rate = %v, want ceiling %v", got, cfg.MaxRate)
	}
}

func TestIdleWindowKeepsRate(t *testing.T) {
	c, clk, cfg := newTestController()
	clk.advance(10 * cfg.Window)
	c.Tick()
	if got := c.Snapshot().Rate; got != cfg.InitialRate {
		t.Fatalf("rate = %v, want unchanged %v", got, cfg.InitialRate)
	}
}

func TestAcquireWaitsOnceBurstIsSpent(t *testing.T) {
	c, _, cfg := newTestController()
	ctx := context.Background()
	for i := 0; i < int(cfg.Burst); i++ {
		if wait, _, err := c.Acquire(ctx); err != nil || wait != 0 {
			t.Fatalf("burst permit %d: wait=%v err=%v", i, wait, err)
		}
	}
	wait, _, err := c.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Second / time.Duration(cfg.InitialRate); wait != want {
		t.Fatalf("wait = %v, want %v", wait, want)
	}
}

func TestAcquireCancelledReturnsPermit(t *testing.T) {
	c, _, cfg := newTestController()
	for i := 0; i < int(cfg.Burst); i++ {
		c.Acquire(context.Background())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := c.Acquire(ctx); err == nil {
		t.Fatal("expected cancellation error")
	}
	c.mu.Lock()
	tokens := c.tokens
	c.mu.Unlock()
	if tokens != 0 {
		t.Fatalf("tokens = %v, want 0 after returning the permit", tokens)
	}
}
