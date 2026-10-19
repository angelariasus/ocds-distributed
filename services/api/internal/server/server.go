// Package server implements the Query service once and exposes it twice: as
// gRPC for other services and as REST/JSON for browsers, both behind Envoy.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/angelariasus/ocds-distributed/services/api/gen/mlv1"
	"github.com/angelariasus/ocds-distributed/services/api/gen/queryv1"
	"github.com/angelariasus/ocds-distributed/services/api/internal/cache"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "api_requests_total", Help: "Requests by method, transport and outcome.",
	}, []string{"method", "transport", "outcome"})
	latency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "api_request_seconds", Help: "Request latency.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"method", "source"})
)

// Reader is what the Query service needs from the warehouse.
type Reader interface {
	TopBuyers(ctx context.Context, year, limit int32) ([]*queryv1.Buyer, error)
	BuyerSummary(ctx context.Context, ruc string, year int32) (*queryv1.BuyerSummaryResponse, error)
	SearchItems(ctx context.Context, text string, year, limit int32) ([]*queryv1.Item, error)
}

type Server struct {
	queryv1.UnimplementedQueryServer
	// Authorize wraps each REST handler with the policy for its operation;
	// the default allows everything (development and tests).
	Authorize func(operation string, next http.HandlerFunc) http.HandlerFunc
	instance  string
	reader    Reader
	cache     *cache.Cache
	ml        mlv1.PredictorClient
	mlWait    time.Duration
}

func New(instance string, reader Reader, c *cache.Cache, ml mlv1.PredictorClient) *Server {
	allow := func(_ string, next http.HandlerFunc) http.HandlerFunc { return next }
	return &Server{Authorize: allow, instance: instance, reader: reader, cache: c, ml: ml, mlWait: 2 * time.Second}
}

// cached runs compute through the cache and fills in provenance. Responses are
// cached as serialized protobuf so both transports share entries.
func (s *Server) cached(ctx context.Context, method, params string, out proto.Message, compute func() (proto.Message, error)) error {
	start := time.Now()
	value, hit, err := s.cache.Through(ctx, method, params, func() ([]byte, error) {
		msg, err := compute()
		if err != nil {
			return nil, err
		}
		return proto.Marshal(msg)
	})
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(value, out); err != nil {
		return err
	}
	source := "replica"
	if hit {
		source = "cache"
	}
	latency.WithLabelValues(method, source).Observe(time.Since(start).Seconds())
	setProvenance(out, &queryv1.Provenance{ServedBy: s.instance, CacheHit: hit, Source: source, LatencyMs: time.Since(start).Milliseconds()})
	return nil
}

func setProvenance(msg proto.Message, p *queryv1.Provenance) {
	switch m := msg.(type) {
	case *queryv1.TopBuyersResponse:
		m.Provenance = p
	case *queryv1.BuyerSummaryResponse:
		m.Provenance = p
	case *queryv1.SearchItemsResponse:
		m.Provenance = p
	}
}

func (s *Server) TopBuyers(ctx context.Context, req *queryv1.TopBuyersRequest) (*queryv1.TopBuyersResponse, error) {
	limit := clamp(req.Limit, 10, 100)
	out := &queryv1.TopBuyersResponse{}
	err := s.cached(ctx, "top_buyers", fmt.Sprintf("%d:%d", req.Year, limit), out, func() (proto.Message, error) {
		buyers, err := s.reader.TopBuyers(ctx, req.Year, limit)
		return &queryv1.TopBuyersResponse{Buyers: buyers}, err
	})
	return out, grpcErr(err)
}

func (s *Server) BuyerSummary(ctx context.Context, req *queryv1.BuyerSummaryRequest) (*queryv1.BuyerSummaryResponse, error) {
	if req.Ruc == "" {
		return nil, status.Error(codes.InvalidArgument, "ruc is required")
	}
	out := &queryv1.BuyerSummaryResponse{}
	err := s.cached(ctx, "buyer", fmt.Sprintf("%s:%d", req.Ruc, req.Year), out, func() (proto.Message, error) {
		return s.reader.BuyerSummary(ctx, req.Ruc, req.Year)
	})
	return out, grpcErr(err)
}

func (s *Server) SearchItems(ctx context.Context, req *queryv1.SearchItemsRequest) (*queryv1.SearchItemsResponse, error) {
	text := strings.TrimSpace(req.Texto)
	if len(text) < 3 {
		return nil, status.Error(codes.InvalidArgument, "texto needs at least 3 characters")
	}
	limit := clamp(req.Limit, 20, 200)
	out := &queryv1.SearchItemsResponse{}
	err := s.cached(ctx, "items", fmt.Sprintf("%s:%d:%d", strings.ToLower(text), req.Year, limit), out, func() (proto.Message, error) {
		items, err := s.reader.SearchItems(ctx, text, req.Year, limit)
		return &queryv1.SearchItemsResponse{Items: items}, err
	})
	return out, grpcErr(err)
}

// PredictLeadTime is not cached: it is a call to another service, with a
// deadline, balanced across ML replicas by the gRPC client.
func (s *Server) PredictLeadTime(ctx context.Context, req *queryv1.PredictLeadTimeRequest) (*queryv1.PredictLeadTimeResponse, error) {
	if s.ml == nil {
		return nil, status.Error(codes.Unavailable, "model serving not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, s.mlWait)
	defer cancel()
	start := time.Now()
	resp, err := s.ml.PredictLeadTime(ctx, &mlv1.LeadTimeRequest{
		MontoReferencial: req.MontoReferencial, Metodo: req.Metodo, Categoria: req.Categoria, Departamento: req.Departamento, EsEssalud: req.EsEssalud,
	})
	latency.WithLabelValues("predict", "ml").Observe(time.Since(start).Seconds())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "model serving: %v", err)
	}
	return &queryv1.PredictLeadTimeResponse{LeadTimeDias: resp.LeadTimeDias, ModelVersion: resp.ModelVersion, ServedBy: resp.ServedBy}, nil
}

func clamp(v, def, max int32) int32 {
	if v <= 0 {
		return def
	}
	if v > max {
		return max
	}
	return v
}

func grpcErr(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if strings.Contains(err.Error(), "no rows") {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// REST exposes the same handlers over HTTP/JSON.
func (s *Server) REST() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /v1/top-buyers", s.Authorize("top_buyers", func(w http.ResponseWriter, r *http.Request) {
		resp, err := s.TopBuyers(r.Context(), &queryv1.TopBuyersRequest{Year: queryInt(r, "year"), Limit: queryInt(r, "limit")})
		s.respond(w, "top_buyers", resp, err)
	}))
	mux.HandleFunc("GET /v1/buyers/{ruc}", s.Authorize("buyer", func(w http.ResponseWriter, r *http.Request) {
		resp, err := s.BuyerSummary(r.Context(), &queryv1.BuyerSummaryRequest{Ruc: r.PathValue("ruc"), Year: queryInt(r, "year")})
		s.respond(w, "buyer", resp, err)
	}))
	mux.HandleFunc("GET /v1/items", s.Authorize("items", func(w http.ResponseWriter, r *http.Request) {
		resp, err := s.SearchItems(r.Context(), &queryv1.SearchItemsRequest{Texto: r.URL.Query().Get("q"), Year: queryInt(r, "year"), Limit: queryInt(r, "limit")})
		s.respond(w, "items", resp, err)
	}))
	mux.HandleFunc("POST /v1/predict/lead-time", s.Authorize("predict", func(w http.ResponseWriter, r *http.Request) {
		var req queryv1.PredictLeadTimeRequest
		body, _ := readBody(r)
		if err := protojson.Unmarshal(body, &req); err != nil {
			s.respond(w, "predict", nil, status.Error(codes.InvalidArgument, err.Error()))
			return
		}
		resp, err := s.PredictLeadTime(r.Context(), &req)
		s.respond(w, "predict", resp, err)
	}))
	return mux
}

func (s *Server) respond(w http.ResponseWriter, method string, msg proto.Message, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Served-By", s.instance)
	if err != nil {
		st, _ := status.FromError(err)
		code := map[codes.Code]int{codes.InvalidArgument: 400, codes.NotFound: 404, codes.Unavailable: 503, codes.DeadlineExceeded: 504}[st.Code()]
		if code == 0 {
			code = 500
		}
		requests.WithLabelValues(method, "rest", "error").Inc()
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]string{"error": st.Message()})
		return
	}
	requests.WithLabelValues(method, "rest", "ok").Inc()
	out, _ := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(msg)
	w.Write(out)
}

func queryInt(r *http.Request, key string) int32 {
	v, _ := strconv.Atoi(r.URL.Query().Get(key))
	return int32(v)
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 1024)
	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			return buf, nil
		}
		if len(buf) > 1<<16 {
			return buf, fmt.Errorf("body too large")
		}
	}
}
