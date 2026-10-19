package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/angelariasus/ocds-distributed/services/compiler/internal/engine"
)

const serviceName = "compiler"

var restoreSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
	Name: "compiler_restore_seconds", Help: "Time to rebuild a partition from the changelog.",
	Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
})

type config struct {
	brokers     []string
	inputTopic  string
	outputTopic string
	group       string
	metrics     string
	instance    string
}

func loadConfig() config {
	host, _ := os.Hostname()
	return config{
		brokers:     strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		inputTopic:  env("INPUT_TOPIC", "bronze.records"),
		outputTopic: env("OUTPUT_TOPIC", "records.compiled"),
		group:       env("KAFKA_GROUP_ID", "compiler"),
		metrics:     env("METRICS_ADDR", ":2115"),
		instance:    env("INSTANCE_ID", host),
	}
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("compiler stopped", "error", err)
		os.Exit(1)
	}
}

type compiler struct {
	cfg    config
	log    *slog.Logger
	engine *engine.Engine
	client *kgo.Client // consumer group member and producer
	hooks  []kgo.Hook
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := loadConfig()

	tp, err := newTracerProvider(ctx)
	if err != nil {
		return err
	}
	defer tp.Shutdown(context.Background())
	kot := kotel.NewKotel(kotel.WithTracer(kotel.NewTracer(kotel.TracerProvider(tp), kotel.TracerPropagator(propagation.TraceContext{}))))

	c := &compiler{cfg: cfg, log: log, engine: engine.New(), hooks: kot.Hooks()}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(cfg.inputTopic),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onRevoked),
		kgo.DefaultProduceTopic(cfg.outputTopic),
		// Co-partitioning: the output partition must be the input partition the
		// record came from, not a fresh hash of the key (the producer that wrote
		// the input may hash differently). Without this option franz-go ignores
		// Record.Partition.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.WithHooks(c.hooks...),
	)
	if err != nil {
		return err
	}
	defer client.Close()
	c.client = client

	go serveMetrics(log, cfg.metrics)
	log.Info("compiler started", "instance", cfg.instance, "input", cfg.inputTopic, "output", cfg.outputTopic, "group", cfg.group)

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			log.Error("fetch error", "topic", topic, "partition", p, "error", err)
		})
		var toCommit []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if err := c.handle(ctx, r); err != nil {
				log.Warn("observation skipped", "partition", r.Partition, "offset", r.Offset, "error", err)
			}
			toCommit = append(toCommit, r)
		})
		if len(toCommit) == 0 {
			continue
		}
		if err := client.Flush(ctx); err != nil {
			log.Error("flush failed", "error", err)
			continue
		}
		if err := client.CommitRecords(ctx, toCommit...); err != nil {
			log.Error("commit failed", "error", err)
		}
	}
}

func (c *compiler) handle(ctx context.Context, r *kgo.Record) error {
	state, outcome, err := c.engine.Apply(r.Partition, r.Value)
	if err != nil {
		return err
	}
	if outcome != engine.Merged {
		return nil
	}
	value, err := json.Marshal(state)
	if err != nil {
		return err
	}
	out := &kgo.Record{
		Key:       []byte(state.OCID),
		Value:     value,
		Partition: r.Partition, // co-partitioned with the input
		Headers: []kgo.RecordHeader{
			{Key: "lamport", Value: []byte(strconv.FormatUint(state.Lamport, 10))},
			{Key: "compiler", Value: []byte(c.cfg.instance)},
		},
	}
	c.client.Produce(ctx, out, func(_ *kgo.Record, err error) {
		if err != nil {
			c.log.Error("produce failed", "ocid", state.OCID, "error", err)
		}
	})
	return nil
}

// onAssigned rebuilds the state of each newly owned partition by reading its
// changelog partition from the beginning up to the current end offset.
func (c *compiler) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for _, p := range assigned[c.cfg.inputTopic] {
		c.engine.Assign(p)
		start := time.Now()
		n, err := c.restore(ctx, p)
		restoreSeconds.Observe(time.Since(start).Seconds())
		if err != nil {
			c.log.Error("restore failed", "partition", p, "error", err)
			continue
		}
		c.log.Info("partition restored", "partition", p, "changelog_entries", n, "keys", c.engine.KeyCount(), "took", time.Since(start).String())
	}
}

func (c *compiler) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	for _, p := range revoked[c.cfg.inputTopic] {
		c.engine.Revoke(p)
		c.log.Info("partition revoked", "partition", p)
	}
}

func (c *compiler) restore(ctx context.Context, p int32) (int, error) {
	admin := kadm.NewClient(c.client)
	ends, err := admin.ListEndOffsets(ctx, c.cfg.outputTopic)
	if err != nil {
		return 0, err
	}
	end, ok := ends.Lookup(c.cfg.outputTopic, p)
	if !ok {
		return 0, fmt.Errorf("no end offset for %s/%d", c.cfg.outputTopic, p)
	}
	if end.Offset == 0 {
		return 0, nil
	}
	reader, err := kgo.NewClient(
		kgo.SeedBrokers(c.cfg.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{c.cfg.outputTopic: {p: kgo.NewOffset().AtStart()}}),
		kgo.WithHooks(c.hooks...),
	)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	n := 0
	for {
		fetches := reader.PollFetches(ctx)
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return n, errs[0].Err
		}
		reached := false
		fetches.EachRecord(func(r *kgo.Record) {
			if err := c.engine.Restore(p, r.Value); err != nil {
				c.log.Warn("changelog entry skipped", "partition", p, "offset", r.Offset, "error", err)
			}
			n++
			if r.Offset+1 >= end.Offset {
				reached = true
			}
		})
		if reached {
			return n, nil
		}
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

func serveMetrics(log *slog.Logger, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server failed", "error", err)
	}
}
