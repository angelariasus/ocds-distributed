package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
	"google.golang.org/protobuf/proto"

	"github.com/angelariasus/ocds-distributed/services/catalog/gen/catalogv1"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/membership"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/node"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/store"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/transport"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/vclock"
)

const serviceName = "catalog"

var membersGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "catalog_members", Help: "Cluster members as seen by this node, by state.",
}, []string{"state"})

func main() {
	if len(os.Args) > 1 && os.Args[1] == "ctl" {
		if err := ctl(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("catalog stopped", "error", err)
		os.Exit(1)
	}
}

type config struct {
	node       node.Config
	seeds      map[string]string
	dataDir    string
	grpcAddr   string
	metrics    string
	gossip     time.Duration
	antiEntr   time.Duration
	handoff    time.Duration
	membership membership.Config
}

func loadConfig() (config, error) {
	host, _ := os.Hostname()
	id := env("NODE_ID", host)
	port := env("GRPC_PORT", "7100")
	cfg := config{
		node: node.Config{
			ID:            id,
			Addr:          env("ADVERTISE_ADDR", id+":"+port),
			N:             envInt("REPLICATION_N", 3),
			R:             envInt("READ_QUORUM_R", 2),
			W:             envInt("WRITE_QUORUM_W", 2),
			Vnodes:        envInt("VNODES", 128),
			RPCTimeout:    envDur("RPC_TIMEOUT", 2*time.Second),
			HintedHandoff: env("HINTED_HANDOFF", "true") == "true",
		},
		seeds:      map[string]string{},
		dataDir:    env("DATA_DIR", ""),
		grpcAddr:   ":" + port,
		metrics:    env("METRICS_ADDR", ":2114"),
		gossip:     envDur("GOSSIP_INTERVAL", time.Second),
		antiEntr:   envDur("ANTI_ENTROPY_INTERVAL", 30*time.Second),
		handoff:    envDur("HANDOFF_INTERVAL", 5*time.Second),
		membership: membership.DefaultConfig(),
	}
	cfg.membership.SuspectTimeout = envDur("SUSPECT_TIMEOUT", cfg.membership.SuspectTimeout)
	for _, seed := range strings.Split(env("SEEDS", ""), ",") {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		sid, addr, ok := strings.Cut(seed, "=")
		if !ok {
			return cfg, fmt.Errorf("SEEDS entry %q must be id=host:port", seed)
		}
		cfg.seeds[sid] = addr
	}
	if cfg.node.N < 1 || cfg.node.R > cfg.node.N || cfg.node.W > cfg.node.N {
		return cfg, fmt.Errorf("invalid quorum N=%d R=%d W=%d", cfg.node.N, cfg.node.R, cfg.node.W)
	}
	return cfg, nil
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

	walPath := ""
	if cfg.dataDir != "" {
		if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
			return err
		}
		walPath = cfg.dataDir + "/wal.jsonl"
	}
	st, err := store.Open(walPath)
	if err != nil {
		return err
	}
	defer st.Close()

	client := transport.NewClient()
	defer client.Close()

	var n *node.Node
	// A restarted node must outrank the gossip that declared it dead, so its
	// incarnation starts from the wall clock instead of zero.
	members := membership.New(
		membership.Member{ID: cfg.node.ID, Addr: cfg.node.Addr, Incarnation: uint64(time.Now().Unix())},
		cfg.seeds, client, cfg.membership, time.Now,
		func(m membership.Member) {
			log.Info("member changed", "id", m.ID, "state", m.State.String(), "incarnation", m.Incarnation)
			if n != nil {
				n.OnMember(m)
			}
		},
	)
	n = node.New(cfg.node, st, members, client)

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	catalogv1.RegisterCatalogServer(srv, &catalogServer{node: n, members: members})
	catalogv1.RegisterInternalServer(srv, &internalServer{node: n, members: members, client: client, rpcTimeout: cfg.node.RPCTimeout})
	healthpb.RegisterHealthServer(srv, health.NewServer())

	lis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return err
	}
	go serveMetrics(log, cfg.metrics)
	go loop(ctx, cfg.gossip, func() {
		members.Round(ctx)
		counts := map[string]int{"alive": 0, "suspect": 0, "dead": 0}
		for _, m := range members.View() {
			counts[m.State.String()]++
		}
		for state, c := range counts {
			membersGauge.WithLabelValues(state).Set(float64(c))
		}
	})
	go loop(ctx, cfg.handoff, func() { n.HandoffRound(ctx) })
	go loop(ctx, cfg.antiEntr, func() {
		if repaired, err := n.AntiEntropyRound(ctx); err != nil {
			log.Warn("anti-entropy round failed", "error", err)
		} else if repaired > 0 {
			log.Info("anti-entropy repaired keys", "keys", repaired)
		}
	})

	log.Info("catalog listening", "id", cfg.node.ID, "addr", cfg.node.Addr, "n", cfg.node.N, "r", cfg.node.R, "w", cfg.node.W,
		"seeds", len(cfg.seeds), "keys", st.Len(), "wal", walPath)
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	return srv.Serve(lis)
}

func loop(ctx context.Context, every time.Duration, fn func()) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

type catalogServer struct {
	catalogv1.UnimplementedCatalogServer
	node    *node.Node
	members *membership.List
}

func (s *catalogServer) Get(ctx context.Context, req *catalogv1.GetRequest) (*catalogv1.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	versions, clock, answered, err := s.node.Get(ctx, req.Key, int(req.R))
	if err != nil {
		return nil, quorumStatus(err)
	}
	context, err := proto.Marshal(transport.ToProtoClock(clock))
	if err != nil {
		return nil, err
	}
	return &catalogv1.GetResponse{Versions: transport.ToProtoVersions(versions), Context: context, ReplicasAnswered: uint32(answered)}, nil
}

func (s *catalogServer) Put(ctx context.Context, req *catalogv1.PutRequest) (*catalogv1.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	clock, err := decodeContext(req.Context)
	if err != nil {
		return nil, err
	}
	newClock, acked, hinted, err := s.node.Put(ctx, req.Key, req.Value, clock, int(req.W), false)
	if err != nil {
		return nil, quorumStatus(err)
	}
	context, _ := proto.Marshal(transport.ToProtoClock(newClock))
	return &catalogv1.PutResponse{Context: context, ReplicasAcked: uint32(acked), Hinted: uint32(hinted)}, nil
}

func (s *catalogServer) Delete(ctx context.Context, req *catalogv1.DeleteRequest) (*catalogv1.DeleteResponse, error) {
	clock, err := decodeContext(req.Context)
	if err != nil {
		return nil, err
	}
	_, acked, _, err := s.node.Put(ctx, req.Key, nil, clock, int(req.W), true)
	if err != nil {
		return nil, quorumStatus(err)
	}
	return &catalogv1.DeleteResponse{ReplicasAcked: uint32(acked)}, nil
}

func (s *catalogServer) Status(context.Context, *catalogv1.StatusRequest) (*catalogv1.StatusResponse, error) {
	cfg := s.node.Config()
	resp := &catalogv1.StatusResponse{
		Self: cfg.ID, Ring: s.node.Ring().Nodes(),
		N: uint32(cfg.N), R: uint32(cfg.R), W: uint32(cfg.W),
		Keys: uint64(s.node.KeyCount()), PendingHints: uint64(s.node.PendingHints()),
	}
	for _, m := range s.members.View() {
		resp.Members = append(resp.Members, &catalogv1.Member{Id: m.ID, Addr: m.Addr, State: m.State.String(), Incarnation: m.Incarnation, Heartbeat: m.Heartbeat})
	}
	return resp, nil
}

func decodeContext(raw []byte) (vclock.Clock, error) {
	if len(raw) == 0 {
		return vclock.Clock{}, nil
	}
	var pb catalogv1.VectorClock
	if err := proto.Unmarshal(raw, &pb); err != nil {
		return nil, status.Error(codes.InvalidArgument, "context is not a vector clock")
	}
	return transport.FromProtoClock(&pb), nil
}

func quorumStatus(err error) error {
	if errors.Is(err, node.ErrQuorum) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return err
}

type internalServer struct {
	catalogv1.UnimplementedInternalServer
	node       *node.Node
	members    *membership.List
	client     *transport.Client
	rpcTimeout time.Duration
}

func (s *internalServer) ReplicaGet(_ context.Context, req *catalogv1.ReplicaGetRequest) (*catalogv1.ReplicaGetResponse, error) {
	return &catalogv1.ReplicaGetResponse{Versions: transport.ToProtoVersions(s.node.LocalGet(req.Key))}, nil
}

func (s *internalServer) ReplicaPut(_ context.Context, req *catalogv1.ReplicaPutRequest) (*catalogv1.ReplicaPutResponse, error) {
	if req.Version == nil {
		return nil, status.Error(codes.InvalidArgument, "version is required")
	}
	if err := s.node.LocalPut(req.Key, transport.FromProtoVersion(req.Version), req.HintFor); err != nil {
		return nil, err
	}
	return &catalogv1.ReplicaPutResponse{}, nil
}

func (s *internalServer) Ping(_ context.Context, req *catalogv1.PingRequest) (*catalogv1.PingResponse, error) {
	s.members.Merge(transport.FromProtoMembers(req.Members))
	return &catalogv1.PingResponse{Members: transport.ToProtoMembers(s.members.View())}, nil
}

func (s *internalServer) PingReq(ctx context.Context, req *catalogv1.PingReqRequest) (*catalogv1.PingReqResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()
	_, err := s.client.Ping(ctx, req.TargetAddr, s.members.Self(), nil)
	return &catalogv1.PingReqResponse{Reachable: err == nil}, nil
}

func (s *internalServer) TreeLevel(_ context.Context, req *catalogv1.TreeLevelRequest) (*catalogv1.TreeLevelResponse, error) {
	return &catalogv1.TreeLevelResponse{Hashes: s.node.Tree(req.Peer).Level(int(req.Depth))}, nil
}

func (s *internalServer) BucketKeys(_ context.Context, req *catalogv1.BucketKeysRequest) (*catalogv1.BucketKeysResponse, error) {
	resp := &catalogv1.BucketKeysResponse{}
	for key, versions := range s.node.Bucket(req.Peer, req.Bucket) {
		for _, v := range versions {
			resp.Keys = append(resp.Keys, key)
			resp.Versions = append(resp.Versions, transport.ToProtoVersion(v))
		}
	}
	return resp, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

func envDur(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
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
