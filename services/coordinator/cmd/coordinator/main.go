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
	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/raft"
	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/schedule"
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
	raftTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coordinator_raft_term", Help: "Current Raft term seen by this replica.",
	})
	raftLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coordinator_raft_is_leader", Help: "1 if this replica is the Raft leader.",
	})
	lastRound = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "coordinator_last_round", Help: "Last planning round in the replicated schedule.",
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

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return err
	}
	machine := &schedule.Machine{}
	var peerIDs []string
	for id := range cfg.RaftPeers {
		peerIDs = append(peerIDs, id)
	}
	node, err := raft.New(raft.Config{
		ID:                 cfg.RaftID,
		Peers:              peerIDs,
		ElectionTimeoutMin: 1500 * time.Millisecond,
		ElectionTimeoutMax: 3 * time.Second,
		Heartbeat:          300 * time.Millisecond,
		RPCTimeout:         500 * time.Millisecond,
		Transport:          raft.HTTPTransport{Peers: cfg.RaftPeers, Client: &http.Client{Timeout: time.Second}},
		Storage:            raft.FileStorage{Path: cfg.DataDir + "/raft.json"},
		Apply: func(e raft.Entry) {
			if err := machine.Apply(e.Command); err != nil {
				log.Error("apply failed", "index", e.Index, "error", err)
			}
			lastRound.Set(float64(machine.State().LastRound))
		},
		OnRoleChange: func(role raft.Role, term uint64, leader string) {
			log.Info("raft role changed", "role", role.String(), "term", term, "leader", leader)
			raftTerm.Set(float64(term))
			if role == raft.Leader {
				raftLeader.Set(1)
			} else {
				raftLeader.Set(0)
			}
		},
		Logger: log,
	})
	if err != nil {
		return err
	}
	go node.Run(ctx)

	force := make(chan bool, 1) // true: crash after committing the plan (demo hook)
	go serveMetrics(log, cfg.MetricsAddr, node, machine, force)
	raftSrv := &http.Server{Addr: cfg.RaftAddr, Handler: raft.Handler(node), ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := raftSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("raft server failed", "error", err)
		}
	}()
	log.Info("coordinator started", "raft_id", cfg.RaftID, "peers", len(cfg.RaftPeers), "plan_interval", cfg.PlanInterval.String())

	client := ocds.NewClient(cfg.OCDSBaseURL)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		forced, crash := false, false
		select {
		case <-ctx.Done():
			raftSrv.Close()
			return nil
		case <-ticker.C:
		case crash = <-force:
			forced = true
		}
		if leader, _ := node.IsLeader(); !leader {
			continue
		}
		action, round := machine.Next(time.Now().UTC(), cfg.PlanInterval, forced)
		if action == "wait" {
			continue
		}
		if err := leaderRound(ctx, log, tracer, client, kafka, cfg, node, action, round, crash); err != nil {
			planRuns.WithLabelValues("error").Inc()
			log.Error("round failed", "round", round, "action", action, "error", err)
		} else {
			planRuns.WithLabelValues("ok").Inc()
		}
	}
}

// leaderRound commits the decision to plan before acting on it and the fact
// that it published after, so a new leader can tell an unfinished round from
// a finished one. Publishing twice is harmless (extractors are idempotent);
// skipping a round or running two leaders' rounds at once is what this prevents.
func leaderRound(ctx context.Context, log *slog.Logger, tracer trace.Tracer, client *ocds.Client, kafka *kgo.Client,
	cfg config.Config, node *raft.Node, action string, round uint64, crash bool) error {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if action == "plan" {
		cmd := schedule.Command{Type: schedule.RoundPlanned, Round: round, At: time.Now().UTC(), Leader: cfg.RaftID}
		if _, err := node.Propose(pctx, cmd.Encode()); err != nil {
			return fmt.Errorf("commit plan: %w", err)
		}
		if crash {
			log.Error("crashing after committing the plan and before publishing (demo)", "round", round)
			os.Exit(3)
		}
	} else {
		log.Warn("republishing a round the previous leader did not finish", "round", round)
	}
	_, term := node.IsLeader()
	tasks, err := planRound(ctx, log, tracer, client, kafka, cfg, round, term)
	if err != nil {
		return err
	}
	pctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	cmd := schedule.Command{Type: schedule.RoundPublished, Round: round, At: time.Now().UTC(), Tasks: tasks, Leader: cfg.RaftID}
	if _, err := node.Propose(pctx2, cmd.Encode()); err != nil {
		return fmt.Errorf("commit publish: %w", err)
	}
	log.Info("round committed", "round", round, "tasks", tasks, "term", term)
	return nil
}

func planRound(ctx context.Context, log *slog.Logger, tracer trace.Tracer, client *ocds.Client, kafka *kgo.Client, cfg config.Config, round, term uint64) (int, error) {
	ctx, span := tracer.Start(ctx, "plan-round")
	defer span.End()

	files, err := client.ListFiles(ctx)
	if err != nil {
		span.RecordError(err)
		return 0, err
	}
	filesListed.Set(float64(len(files)))

	now := time.Now().UTC()
	tasks := append(planner.Plan(files, cfg.Window, now), planner.PlanRanges(cfg.Ranges, now)...)
	span.SetAttributes(attribute.Int("ocds.files", len(files)), attribute.Int("ocds.tasks", len(tasks)))

	records := make([]*kgo.Record, 0, len(tasks))
	for _, t := range tasks {
		value, err := json.Marshal(t)
		if err != nil {
			return 0, err
		}
		records = append(records, &kgo.Record{Key: []byte(t.TaskID), Value: value, Headers: []kgo.RecordHeader{
			{Key: "round", Value: []byte(strconv.FormatUint(round, 10))},
			{Key: "raft-term", Value: []byte(strconv.FormatUint(term, 10))},
			{Key: "coordinator", Value: []byte(cfg.RaftID)},
		}})
	}
	if err := kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		span.RecordError(err)
		return 0, err
	}
	tasksPublished.Add(float64(len(records)))
	log.Info("plan round published", "round", round, "term", term, "files", len(files), "tasks", len(tasks))
	return len(tasks), nil
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

func serveMetrics(log *slog.Logger, addr string, node *raft.Node, machine *schedule.Machine, force chan<- bool) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"raft": node.Status(), "schedule": machine.State()})
	})
	// POST /plan asks the leader to start a round now (demos); followers redirect the caller.
	mux.HandleFunc("POST /plan", func(w http.ResponseWriter, r *http.Request) {
		if leader, _ := node.IsLeader(); !leader {
			w.WriteHeader(http.StatusMisdirectedRequest)
			json.NewEncoder(w).Encode(map[string]string{"leader": node.Status().Leader})
			return
		}
		select {
		case force <- r.URL.Query().Get("crash") == "1":
		default:
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server failed", "error", err)
	}
}
