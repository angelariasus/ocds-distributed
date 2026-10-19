package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
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
	"github.com/twmb/franz-go/pkg/kgo"

	ledgerlog "github.com/angelariasus/ocds-distributed/services/ledger/internal/log"
	"github.com/angelariasus/ocds-distributed/services/ledger/internal/merkle"
)

var (
	appended = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_appends_total", Help: "Append requests by kind and outcome.",
	}, []string{"kind", "outcome"})
	treeSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ledger_tree_size", Help: "Entries in the log.",
	})
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	var err error
	switch cmd {
	case "serve":
		err = serve(log)
	case "audit":
		err = audit(log, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q (serve | audit)", cmd)
	}
	if err != nil {
		log.Error("ledger stopped", "error", err)
		os.Exit(1)
	}
}

func loadPrivateKey(path string) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("signing key: no PEM block")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	priv, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("signing key is not Ed25519")
	}
	return priv, nil
}

func loadPublicKey(path string) (ed25519.PublicKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("public key: no PEM block")
	}
	k, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("public key is not Ed25519")
	}
	return pub, nil
}

func serve(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	key, err := loadPrivateKey(env("LEDGER_SIGNING_KEY", "/pki/ledger-signing.key"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(env("DATA_DIR", "/data"), 0o755); err != nil {
		return err
	}
	l, err := ledgerlog.Open(env("DATA_DIR", "/data")+"/entries.jsonl", key, time.Now)
	if err != nil {
		return err
	}
	defer l.Close()
	treeSize.Set(float64(l.Size()))
	log.Info("ledger opened", "entries", l.Size(), "root", l.STH().Root)

	if brokers := env("KAFKA_BROKERS", ""); brokers != "" {
		go consume(ctx, log, l, strings.Split(brokers, ","))
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /v1/sth", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, l.STH()) })
	mux.HandleFunc("POST /v1/entries", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Kind   string            `json:"kind"`
			Ref    string            `json:"ref"`
			Digest string            `json:"digest"`
			Meta   map[string]string `json:"meta"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		idx, created, err := append_(l, req.Kind, req.Ref, req.Digest, req.Meta)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"index": idx, "created": created, "sth": l.STH()})
	})
	mux.HandleFunc("GET /v1/entries", func(w http.ResponseWriter, r *http.Request) {
		start, end := atoi(r, "start", 0), atoi(r, "end", l.Size())
		if end > l.Size() {
			end = l.Size()
		}
		if end-start > 1000 {
			end = start + 1000
		}
		out := []map[string]any{}
		for i := start; i < end; i++ {
			e, leaf, _ := l.Entry(i)
			out = append(out, map[string]any{"index": i, "entry": e, "leaf_hash": ledgerlog.Hex(leaf)})
		}
		writeJSON(w, 200, map[string]any{"entries": out})
	})
	mux.HandleFunc("GET /v1/proof/inclusion", func(w http.ResponseWriter, r *http.Request) {
		size := atoi(r, "tree_size", l.Size())
		idx := atoi(r, "index", -1)
		if kind, ref := r.URL.Query().Get("kind"), r.URL.Query().Get("ref"); kind != "" && ref != "" {
			found, ok := l.Find(kind, ref)
			if !ok {
				writeJSON(w, 404, map[string]string{"error": "no entry for " + kind + " " + ref})
				return
			}
			idx = found
		}
		proof, err := l.InclusionProof(idx, size)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		e, leaf, _ := l.Entry(idx)
		writeJSON(w, 200, map[string]any{"index": idx, "tree_size": size, "entry": e, "leaf_hash": ledgerlog.Hex(leaf), "audit_path": hexes(proof)})
	})
	mux.HandleFunc("GET /v1/proof/consistency", func(w http.ResponseWriter, r *http.Request) {
		proof, err := l.ConsistencyProof(atoi(r, "first", 0), atoi(r, "second", l.Size()))
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"consistency": hexes(proof)})
	})

	srv := &http.Server{Addr: env("HTTP_ADDR", ":8100"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("ledger listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func append_(l *ledgerlog.Log, kind, ref, digest string, meta map[string]string) (int, bool, error) {
	idx, created, err := l.Append(kind, ref, digest, meta)
	if err != nil {
		appended.WithLabelValues(kind, "error").Inc()
		return 0, false, err
	}
	outcome := "duplicate"
	if created {
		outcome = "new"
	}
	appended.WithLabelValues(kind, outcome).Inc()
	treeSize.Set(float64(l.Size()))
	return idx, created, nil
}

// consume attests what the pipeline reports: every Bronze object the
// extractors stored and every Gold load that committed. Offsets are committed
// after the entry is fsynced, and appends are idempotent, so redelivery is safe.
func consume(ctx context.Context, log *slog.Logger, l *ledgerlog.Log, brokers []string) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(env("KAFKA_GROUP_ID", "ledger")),
		kgo.ConsumeTopics("extract.results", "gold.events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Error("kafka client", "error", err)
		return
	}
	defer client.Close()
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		var done []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if err := attest(log, l, r); err != nil {
				log.Warn("record not attested", "topic", r.Topic, "offset", r.Offset, "error", err)
			}
			done = append(done, r)
		})
		if len(done) > 0 {
			if err := client.CommitRecords(ctx, done...); err != nil {
				log.Error("commit failed", "error", err)
			}
		}
	}
}

func attest(log *slog.Logger, l *ledgerlog.Log, r *kgo.Record) error {
	switch r.Topic {
	case "extract.results":
		var res struct {
			TaskID    string `json:"task_id"`
			Status    string `json:"status"`
			ObjectKey string `json:"object_key"`
			SHA256    string `json:"sha256"`
		}
		if err := json.Unmarshal(r.Value, &res); err != nil {
			return err
		}
		if res.SHA256 == "" || (res.Status != "stored" && res.Status != "skipped") {
			return nil
		}
		idx, created, err := append_(l, "bronze.object", res.ObjectKey, res.SHA256, map[string]string{"task_id": res.TaskID})
		if err == nil && created {
			log.Info("attested", "kind", "bronze.object", "ref", res.ObjectKey, "index", idx)
		}
		return err
	case "gold.events":
		var ev struct {
			Type     string `json:"type"`
			Token    int64  `json:"token"`
			Digest   string `json:"digest"`
			RowsFact int64  `json:"rows_fact"`
			Years    []int  `json:"years"`
		}
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			return err
		}
		if ev.Type != "gold.loaded" || ev.Digest == "" {
			return nil
		}
		meta := map[string]string{"rows_fact": strconv.FormatInt(ev.RowsFact, 10), "years": fmt.Sprint(ev.Years)}
		idx, created, err := append_(l, "gold.load", fmt.Sprintf("token:%d", ev.Token), ev.Digest, meta)
		if err == nil && created {
			log.Info("attested", "kind", "gold.load", "token", ev.Token, "index", idx)
		}
		return err
	}
	return nil
}

func atoi(r *http.Request, key string, fallback int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(key)); err == nil {
		return v
	}
	return fallback
}

func hexes(hs []merkle.Hash) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = ledgerlog.Hex(h)
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
