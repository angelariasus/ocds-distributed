package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/angelariasus/ocds-distributed/services/api/gen/mlv1"
	"github.com/angelariasus/ocds-distributed/services/api/gen/queryv1"
	"github.com/angelariasus/ocds-distributed/services/api/internal/cache"
	"github.com/angelariasus/ocds-distributed/services/api/internal/redisbackend"
	"github.com/angelariasus/ocds-distributed/services/api/internal/server"
	"github.com/angelariasus/ocds-distributed/services/api/internal/store"
)

const serviceName = "api"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	host, _ := os.Hostname()
	instance := env("INSTANCE_ID", host)

	tp, err := newTracerProvider(ctx)
	if err != nil {
		return err
	}
	defer tp.Shutdown(context.Background())

	st, err := store.Open(ctx, env("GOLD_DSN", "postgresql://postgres:postgres@localhost:5001/postgres"))
	if err != nil {
		return err
	}
	defer st.Close()

	ttl, err := time.ParseDuration(env("CACHE_TTL", "5m"))
	if err != nil {
		return err
	}
	backend := redisbackend.New(strings.Split(env("REDIS_ADDRS", "localhost:6379"), ","))
	defer backend.Close()
	if err := backend.Ping(ctx); err != nil {
		log.Warn("redis unreachable at start, serving from the database until it is back", "error", err)
	}
	c := cache.New(backend, ttl)

	// Client-side load balancing: dns:///ml resolves to every ML replica and
	// round_robin spreads calls across them without a proxy in between.
	var ml mlv1.PredictorClient
	if target := env("ML_TARGET", ""); target != "" {
		conn, err := grpc.NewClient(target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		)
		if err != nil {
			return err
		}
		defer conn.Close()
		ml = mlv1.NewPredictorClient(conn)
	}

	srv := server.New(instance, st, c, ml)
	go invalidateOnLoads(ctx, log, c, instance)

	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	queryv1.RegisterQueryServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	lis, err := net.Listen("tcp", env("GRPC_ADDR", ":9000"))
	if err != nil {
		return err
	}
	rest := &http.Server{Addr: env("HTTP_ADDR", ":8000"), Handler: srv.REST(), ReadHeaderTimeout: 5 * time.Second}
	metrics := http.NewServeMux()
	metrics.Handle("/metrics", promhttp.Handler())

	go func() { _ = http.ListenAndServe(env("METRICS_ADDR", ":2116"), metrics) }()
	go func() {
		if err := rest.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("rest server failed", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rest.Shutdown(shutdown)
		grpcServer.GracefulStop()
	}()
	log.Info("api listening", "instance", instance, "grpc", lis.Addr().String(), "http", rest.Addr, "cache_ttl", ttl.String(), "ml", ml != nil)
	return grpcServer.Serve(lis)
}

// invalidateOnLoads bumps the cache generation whenever Gold announces a
// committed load. Every replica consumes with its own group so each one
// updates its local view; the generation itself lives in Redis, shared.
func invalidateOnLoads(ctx context.Context, log *slog.Logger, c *cache.Cache, instance string) {
	brokers := env("KAFKA_BROKERS", "")
	if brokers == "" {
		return
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup("api-invalidate-"+instance),
		kgo.ConsumeTopics(env("GOLD_EVENTS_TOPIC", "gold.events")),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Error("invalidation consumer failed", "error", err)
		return
	}
	defer client.Close()
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var event struct {
				ID    int64  `json:"id"`
				Type  string `json:"type"`
				Token int64  `json:"token"`
			}
			if json.Unmarshal(r.Value, &event) != nil || event.Type != "gold.loaded" {
				return
			}
			gen, err := c.Invalidate(ctx)
			log.Info("cache invalidated by gold load", "event", event.ID, "load_token", event.Token, "generation", gen, "error", err)
		})
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newTracerProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}
