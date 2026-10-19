// Package cache is a cache-aside layer with generation-based invalidation:
// every key embeds the current data generation, and a Gold load bumps the
// generation, so the whole cache is invalidated with one INCR instead of
// enumerating keys, and stale entries simply expire.
package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const generationKey = "gold:generation"

var (
	outcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "api_cache_total", Help: "Cache lookups by outcome.",
	}, []string{"outcome"})
	generationGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "api_cache_generation", Help: "Data generation the cache keys are scoped to.",
	})
)

// Backend is the subset of a Redis client the cache needs; the real one is a cluster client.
type Backend interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Incr(ctx context.Context, key string) (int64, error)
	GetInt(ctx context.Context, key string) (int64, bool, error)
}

type Cache struct {
	backend Backend
	ttl     time.Duration

	mu         sync.Mutex
	generation int64
	checkedAt  time.Time
	genTTL     time.Duration
}

func New(backend Backend, ttl time.Duration) *Cache {
	return &Cache{backend: backend, ttl: ttl, genTTL: time.Second}
}

// Generation reads the data generation, refreshing it from Redis at most once
// per second so a hot path does not pay a round trip per request.
func (c *Cache) Generation(ctx context.Context) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.checkedAt) < c.genTTL {
		return c.generation
	}
	gen, ok, err := c.backend.GetInt(ctx, generationKey)
	if err != nil {
		outcomes.WithLabelValues("error").Inc()
		return c.generation
	}
	if ok {
		c.generation = gen
	}
	c.checkedAt = time.Now()
	generationGauge.Set(float64(c.generation))
	return c.generation
}

// Invalidate bumps the generation; every existing key becomes unreachable.
func (c *Cache) Invalidate(ctx context.Context) (int64, error) {
	gen, err := c.backend.Incr(ctx, generationKey)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.generation, c.checkedAt = gen, time.Now()
	c.mu.Unlock()
	generationGauge.Set(float64(gen))
	return gen, nil
}

// Through returns the cached value for name/params or computes, stores and returns it.
func (c *Cache) Through(ctx context.Context, name, params string, compute func() ([]byte, error)) ([]byte, bool, error) {
	key := fmt.Sprintf("q:%d:%s:%s", c.Generation(ctx), name, params)
	if value, ok, err := c.backend.Get(ctx, key); err == nil && ok {
		outcomes.WithLabelValues("hit").Inc()
		return value, true, nil
	} else if err != nil {
		outcomes.WithLabelValues("error").Inc()
	} else {
		outcomes.WithLabelValues("miss").Inc()
	}
	value, err := compute()
	if err != nil {
		return nil, false, err
	}
	if err := c.backend.Set(ctx, key, value, c.ttl); err != nil && !errors.Is(err, context.Canceled) {
		outcomes.WithLabelValues("error").Inc()
	}
	return value, false, nil
}
