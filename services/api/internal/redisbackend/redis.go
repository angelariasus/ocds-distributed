// Package redisbackend adapts a Redis Cluster client to the cache.Backend
// interface. Keys are hashed to one of 16384 slots and the cluster client
// follows MOVED/ASK redirections, so the API sees one logical cache spread
// over three shards, each with a replica.
package redisbackend

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Backend struct {
	client *redis.ClusterClient
}

func New(addrs []string) *Backend {
	return &Backend{client: redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          addrs,
		DialTimeout:    500 * time.Millisecond,
		ReadTimeout:    300 * time.Millisecond,
		WriteTimeout:   300 * time.Millisecond,
		MaxRedirects:   3,
		ReadOnly:       true, // reads may go to replicas
		RouteByLatency: true,
	})}
}

func (b *Backend) Close() error { return b.client.Close() }

func (b *Backend) Ping(ctx context.Context) error { return b.client.Ping(ctx).Err() }

func (b *Backend) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, err := b.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	return v, err == nil, err
}

func (b *Backend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return b.client.Set(ctx, key, value, ttl).Err()
}

func (b *Backend) Incr(ctx context.Context, key string) (int64, error) {
	return b.client.Incr(ctx, key).Result()
}

func (b *Backend) GetInt(ctx context.Context, key string) (int64, bool, error) {
	v, err := b.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil, err
}
