package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/angelariasus/ocds-distributed/services/limiter/gen/limiterv1"
	"github.com/angelariasus/ocds-distributed/services/limiter/internal/aimd"
)

const serviceName = "limiter"

var (
	rateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "limiter_rate_permits_per_second", Help: "Current permit rate per resource.",
	}, []string{"resource"})
	p95Gauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "limiter_window_p95_seconds", Help: "Upstream p95 latency observed in the last window.",
	}, []string{"resource"})
	waitHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "limiter_acquire_wait_seconds", Help: "Time callers waited for a permit.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"resource"})
	reports = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "limiter_reports_total", Help: "Outcomes reported by clients.",
	}, []string{"resource", "outcome"})
)

type server struct {
	limiterv1.UnimplementedLimiterServer
	cfg  aimd.Config
	mu   sync.Mutex
	ctrl map[string]*aimd.Controller
}

func (s *server) controller(resource string) *aimd.Controller {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.ctrl[resource]
	if !ok {
		c = aimd.New(s.cfg, time.Now)
		s.ctrl[resource] = c
	}
	return c
}

func (s *server) Acquire(ctx context.Context, req *limiterv1.AcquireRequest) (*limiterv1.AcquireResponse, error) {
	if req.Resource == "" {
		return nil, status.Error(codes.InvalidArgument, "resource is required")
	}
	wait, rate, err := s.controller(req.Resource).Acquire(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}
	waitHist.WithLabelValues(req.Resource).Observe(wait.Seconds())
	return &limiterv1.AcquireResponse{WaitedMs: wait.Milliseconds(), Rate: rate}, nil
}

func (s *server) Report(_ context.Context, req *limiterv1.ReportRequest) (*limiterv1.ReportResponse, error) {
	if req.Resource == "" {
		return nil, status.Error(codes.InvalidArgument, "resource is required")
	}
	outcome := aimd.OK
	switch req.Outcome {
	case limiterv1.Outcome_OUTCOME_THROTTLED:
		outcome = aimd.Throttled
	case limiterv1.Outcome_OUTCOME_ERROR:
		outcome = aimd.Error
	}
	s.controller(req.Resource).Report(time.Duration(req.LatencyMs)*time.Millisecond, outcome)
	reports.WithLabelValues(req.Resource, req.Outcome.String()).Inc()
	return &limiterv1.ReportResponse{}, nil
}

func (s *server) tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for resource, c := range s.ctrl {
		c.Tick()
		snap := c.Snapshot()
		rateGauge.WithLabelValues(resource).Set(snap.Rate)
		p95Gauge.WithLabelValues(resource).Set(snap.P95.Seconds())
	}
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("limiter stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	tp, err := newTracerProvider(ctx)
	if err != nil {
		return err
	}
	defer tp.Shutdown(context.Background())

	srv := &server{cfg: cfg, ctrl: map[string]*aimd.Controller{}}
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	limiterv1.RegisterLimiterServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())

	lis, err := net.Listen("tcp", env("GRPC_ADDR", ":7000"))
	if err != nil {
		return err
	}
	go serveMetrics(log, env("METRICS_ADDR", ":2113"))
	go func() {
		ticker := time.NewTicker(cfg.Window / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				srv.tick()
			}
		}
	}()

	log.Info("limiter listening", "addr", lis.Addr().String(), "initial_rate", cfg.InitialRate, "max_rate", cfg.MaxRate)
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
	return grpcServer.Serve(lis)
}

func loadConfig() (aimd.Config, error) {
	cfg := aimd.DefaultConfig()
	var err error
	set := func(key string, dst *float64) {
		if err != nil {
			return
		}
		if v := os.Getenv(key); v != "" {
			*dst, err = strconv.ParseFloat(v, 64)
		}
	}
	setDur := func(key string, dst *time.Duration) {
		if err != nil {
			return
		}
		if v := os.Getenv(key); v != "" {
			*dst, err = time.ParseDuration(v)
		}
	}
	set("LIMITER_INITIAL_RATE", &cfg.InitialRate)
	set("LIMITER_MIN_RATE", &cfg.MinRate)
	set("LIMITER_MAX_RATE", &cfg.MaxRate)
	set("LIMITER_INCREASE", &cfg.Increase)
	set("LIMITER_DECREASE", &cfg.Decrease)
	set("LIMITER_BURST", &cfg.Burst)
	setDur("LIMITER_WINDOW", &cfg.Window)
	setDur("LIMITER_LATENCY_HIGH", &cfg.LatencyHigh)
	setDur("LIMITER_LATENCY_LOW", &cfg.LatencyLow)
	return cfg, err
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

func serveMetrics(log *slog.Logger, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server failed", "error", err)
	}
}
