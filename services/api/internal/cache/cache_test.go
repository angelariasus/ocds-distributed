package cache

import (
	"context"
	"errors"
	"testing"
	"time"
)

type memory struct {
	data map[string][]byte
	ints map[string]int64
	down bool
}

func newMemory() *memory { return &memory{data: map[string][]byte{}, ints: map[string]int64{}} }

func (m *memory) Get(_ context.Context, key string) ([]byte, bool, error) {
	if m.down {
		return nil, false, errors.New("redis down")
	}
	v, ok := m.data[key]
	return v, ok, nil
}

func (m *memory) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	if m.down {
		return errors.New("redis down")
	}
	m.data[key] = value
	return nil
}

func (m *memory) Incr(_ context.Context, key string) (int64, error) {
	m.ints[key]++
	return m.ints[key], nil
}

func (m *memory) GetInt(_ context.Context, key string) (int64, bool, error) {
	if m.down {
		return 0, false, errors.New("redis down")
	}
	v, ok := m.ints[key]
	return v, ok, nil
}

func TestSecondLookupIsAHit(t *testing.T) {
	c := New(newMemory(), time.Minute)
	calls := 0
	compute := func() ([]byte, error) { calls++; return []byte("v"), nil }
	_, hit1, _ := c.Through(context.Background(), "top", "2026", compute)
	_, hit2, _ := c.Through(context.Background(), "top", "2026", compute)
	if hit1 || !hit2 || calls != 1 {
		t.Fatalf("hit1=%v hit2=%v calls=%d", hit1, hit2, calls)
	}
}

func TestInvalidationMakesEveryKeyMiss(t *testing.T) {
	m := newMemory()
	c := New(m, time.Minute)
	calls := 0
	compute := func() ([]byte, error) { calls++; return []byte("v"), nil }
	c.Through(context.Background(), "top", "2026", compute)
	c.Through(context.Background(), "buyer", "x", compute)
	if _, err := c.Invalidate(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, hit, _ := c.Through(context.Background(), "top", "2026", compute)
	if hit || calls != 3 {
		t.Fatalf("hit=%v calls=%d after invalidation", hit, calls)
	}
	if len(m.data) != 3 {
		t.Fatalf("old entries should remain until they expire, got %d keys", len(m.data))
	}
}

func TestCacheOutageDegradesToTheDatabase(t *testing.T) {
	m := newMemory()
	c := New(m, time.Minute)
	m.down = true
	value, hit, err := c.Through(context.Background(), "top", "2026", func() ([]byte, error) { return []byte("db"), nil })
	if err != nil || hit || string(value) != "db" {
		t.Fatalf("value=%s hit=%v err=%v", value, hit, err)
	}
}

func TestGenerationIsRefreshedAtMostOncePerSecond(t *testing.T) {
	m := newMemory()
	c := New(m, time.Minute)
	c.Generation(context.Background())
	m.ints[generationKey] = 7
	if g := c.Generation(context.Background()); g != 0 {
		t.Fatalf("generation %d read within the refresh window, want cached 0", g)
	}
	c.checkedAt = time.Time{}
	if g := c.Generation(context.Background()); g != 7 {
		t.Fatalf("generation = %d, want 7 after refresh", g)
	}
}
