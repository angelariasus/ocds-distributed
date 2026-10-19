package portal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const maxPageSize = 100

// Behaviour reproduces what was measured against the real portal: a WAF that
// rejects generic user agents, a hard page-size cap, and latency that grows
// with concurrency instead of an explicit 429.
type Behaviour struct {
	BaseLatency     time.Duration
	PerInflight     time.Duration
	ThrottleAbove   int // inflight requests beyond this get 429; 0 disables
	BlockUserAgents []string
}

var (
	inflightGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ocdsmock_inflight_requests", Help: "Requests currently being served.",
	})
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ocdsmock_requests_total", Help: "Requests by endpoint and status.",
	}, []string{"endpoint", "status"})
	latencyHist = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "ocdsmock_request_seconds", Help: "Simulated service time.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	})
)

type Server struct {
	store     *Store
	behaviour Behaviour
	publicURL string
	inflight  atomic.Int64
}

func NewServer(store *Store, behaviour Behaviour, publicURL string) *Server {
	return &Server{store: store, behaviour: behaviour, publicURL: strings.TrimRight(publicURL, "/")}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/files", s.simulate("files", s.files))
	mux.HandleFunc("GET /api/v1/file/{source}/{kind}/{year}/{month}/", s.simulate("file", s.file))
	mux.HandleFunc("GET /api/v1/recordsAfter", s.simulate("recordsAfter", s.recordsAfter))
	return mux
}

func (s *Server) simulate(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() { requestsTotal.WithLabelValues(endpoint, strconv.Itoa(rec.status)).Inc() }()

		if s.blocked(r.UserAgent()) {
			writeJSON(rec, http.StatusForbidden, map[string]string{"code": "403", "message": "Forbidden"})
			return
		}
		inflight := s.inflight.Add(1)
		inflightGauge.Set(float64(inflight))
		defer func() { inflightGauge.Set(float64(s.inflight.Add(-1))) }()

		if s.behaviour.ThrottleAbove > 0 && int(inflight) > s.behaviour.ThrottleAbove {
			writeJSON(rec, http.StatusTooManyRequests, map[string]string{"code": "429", "message": "Too Many Requests"})
			return
		}
		delay := s.behaviour.BaseLatency + time.Duration(inflight-1)*s.behaviour.PerInflight
		start := time.Now()
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		latencyHist.Observe(time.Since(start).Seconds())
		next(rec, r)
	}
}

func (s *Server) blocked(ua string) bool {
	if ua == "" {
		return true
	}
	for _, prefix := range s.behaviour.BlockUserAgents {
		if strings.HasPrefix(ua, prefix) {
			return true
		}
	}
	return false
}

func (s *Server) files(w http.ResponseWriter, r *http.Request) {
	page := max(queryInt(r, "page", 1), 1)
	perPage := max(queryInt(r, "paginateBy", 5), 1)
	total := len(s.store.Files)
	pages := (total + perPage - 1) / perPage
	start := min((page-1)*perPage, total)
	end := min(start+perPage, total)

	results := make([]map[string]any, 0, end-start)
	for _, f := range s.store.Files[start:end] {
		base := fmt.Sprintf("%s/api/v1/file/%s", s.publicURL, f.Source)
		results = append(results, map[string]any{
			"id": f.ID(), "source": f.Source,
			"year": strconv.Itoa(f.Year), "month": fmt.Sprintf("%02d", f.Month),
			"timestamp": f.Published.Format(time.RFC3339Nano),
			"files": map[string]string{
				"json": fmt.Sprintf("%s/json/%04d/%02d/", base, f.Year, f.Month),
				"sha":  fmt.Sprintf("%s/sha/%04d/%02d/", base, f.Year, f.Month),
			},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pagination": map[string]any{
			"page": page, "num_pages": pages, "total_results": total,
			"has_next": page < pages, "has_previous": page > 1,
		},
		"results": results,
	})
}

func (s *Server) file(w http.ResponseWriter, r *http.Request) {
	id := fmt.Sprintf("%s-%s-%s", r.PathValue("source"), r.PathValue("year"), r.PathValue("month"))
	for _, f := range s.store.Files {
		if f.ID() != id {
			continue
		}
		switch r.PathValue("kind") {
		case "sha":
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, f.SHA256)
		case "json":
			w.Header().Set("Content-Type", "application/zip")
			w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s_json.zip"`, id))
			http.ServeContent(w, r, id+".zip", f.Published, strings.NewReader(string(f.Zip)))
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func (s *Server) recordsAfter(w http.ResponseWriter, r *http.Request) {
	size := queryInt(r, "size", 10)
	if size < 1 || size > maxPageSize {
		http.NotFound(w, r)
		return
	}
	var cursor *Cursor
	if raw := r.URL.Query().Get("searchAfter"); raw != "" {
		c, err := ParseCursor(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cursor = &c
	}
	page := s.store.After(cursor, size)
	records := make([]json.RawMessage, 0, len(page))
	for _, rec := range page {
		records = append(records, rec.Raw)
	}
	var next any
	if len(page) == size {
		q := url.Values{"size": {strconv.Itoa(size)}, "searchAfter": {page[len(page)-1].Cursor().String()}}
		next = s.publicURL + "/api/v1/recordsAfter?" + q.Encode()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":       "1.1",
		"publishedDate": time.Now().Format(time.RFC3339Nano),
		"publisher":     map[string]string{"name": "ocds-mock"},
		"records":       records,
		"links":         map[string]any{"next": next, "prev": nil},
		"uri":           s.publicURL + r.URL.RequestURI(),
	})
}

func queryInt(r *http.Request, key string, fallback int) int {
	v, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil {
		return fallback
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
