package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/config"
	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/ocds"
	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/planner"
)

const serviceName = "coordinator"

var (
	planRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "coordinator_plan_runs_total",
		Help: "Planning rounds by outcome.",
	}, []string{"outcome"})
	tasksPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "coordinator_tasks_published_total",
		Help: "Extraction tasks published to Kafka.",
	})
	filesListed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coordinator_files_listed",
		Help: "Bulk files reported by the OCDS portal in the last round.",
	})
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("coordinator stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	tp, err := newTracerProvider(ctx)
	if err != nil {
		return err
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer(serviceName)

	kot := kotel.NewKotel(kotel.WithTracer(kotel.NewTracer(
		kotel.TracerProvider(tp),
		kotel.TracerPropagator(propagation.TraceContext{}),
	)))
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.TasksTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.WithHooks(kot.Hooks()...),
	)
	if err != nil {
		return err
	}
	defer kafka.Close()

	go serveMetrics(log, cfg.MetricsAddr)

	client := ocds.NewClient(cfg.OCDSBaseURL)
	ticker := time.NewTicker(cfg.PlanInterval)
	defer ticker.Stop()

	for {
		if err := planRound(ctx, log, tracer, client, kafka, cfg); err != nil {
			planRuns.WithLabelValues("error").Inc()
			log.Error("plan round failed", "error", err)
		} else {
			planRuns.WithLabelValues("ok").Inc()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func planRound(ctx context.Context, log *slog.Logger, tracer trace.Tracer, client *ocds.Client, kafka *kgo.Client, cfg config.Config) error {
	ctx, span := tracer.Start(ctx, "plan-round")
	defer span.End()

	files, err := client.ListFiles(ctx)
	if err != nil {
		span.RecordError(err)
		return err
	}
	filesListed.Set(float64(len(files)))

	tasks := planner.Plan(files, cfg.Window, time.Now().UTC())
	span.SetAttributes(attribute.Int("ocds.files", len(files)), attribute.Int("ocds.tasks", len(tasks)))

	records := make([]*kgo.Record, 0, len(tasks))
	for _, t := range tasks {
		value, err := json.Marshal(t)
		if err != nil {
			return err
		}
		records = append(records, &kgo.Record{Key: []byte(t.TaskID), Value: value})
	}
	if err := kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		span.RecordError(err)
		return err
	}
	tasksPublished.Add(float64(len(records)))
	log.Info("plan round published", "files", len(files), "tasks", len(tasks))
	return nil
}

func newTracerProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", serviceName),
	))
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
