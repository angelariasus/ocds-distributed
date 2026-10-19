package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/angelariasus/ocds-distributed/services/ocds-mock/internal/portal"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("ocds-mock stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := portal.Load(env("DATA_DIR", "/data"))
	if err != nil {
		return err
	}
	behaviour, err := loadBehaviour()
	if err != nil {
		return err
	}
	log.Info("portal data loaded", "files", len(store.Files), "records", len(store.Records),
		"base_latency", behaviour.BaseLatency, "per_inflight", behaviour.PerInflight, "throttle_above", behaviour.ThrottleAbove)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/", portal.NewServer(store, behaviour, env("PUBLIC_URL", "http://ocds-mock:8080")).Handler())
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Info("ocds-mock listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func loadBehaviour() (portal.Behaviour, error) {
	base, err := time.ParseDuration(env("BASE_LATENCY", "300ms"))
	if err != nil {
		return portal.Behaviour{}, err
	}
	per, err := time.ParseDuration(env("PER_INFLIGHT_LATENCY", "400ms"))
	if err != nil {
		return portal.Behaviour{}, err
	}
	above, err := strconv.Atoi(env("THROTTLE_ABOVE", "0"))
	if err != nil {
		return portal.Behaviour{}, err
	}
	return portal.Behaviour{
		BaseLatency:     base,
		PerInflight:     per,
		ThrottleAbove:   above,
		BlockUserAgents: strings.Split(env("BLOCK_USER_AGENTS", "curl/,python-requests/,Go-http-client/"), ","),
	}, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
