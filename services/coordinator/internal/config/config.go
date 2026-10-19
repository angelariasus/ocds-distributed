package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/planner"
)

type Config struct {
	Brokers      []string
	TasksTopic   string
	OCDSBaseURL  string
	Window       planner.Window
	PlanInterval time.Duration
	MetricsAddr  string
}

func Load() (Config, error) {
	from, err := planner.ParseYearMonth(env("PLAN_FROM", "2026-08"))
	if err != nil {
		return Config{}, err
	}
	to, err := planner.ParseYearMonth(env("PLAN_TO", "2026-09"))
	if err != nil {
		return Config{}, err
	}
	interval, err := time.ParseDuration(env("PLAN_INTERVAL", "6h"))
	if err != nil {
		return Config{}, fmt.Errorf("PLAN_INTERVAL: %w", err)
	}

	sources := map[string]bool{}
	for _, s := range splitList(env("PLAN_SOURCES", "")) {
		sources[s] = true
	}

	return Config{
		Brokers:      splitList(env("KAFKA_BROKERS", "localhost:19092")),
		TasksTopic:   env("TASKS_TOPIC", "extract.tasks"),
		OCDSBaseURL:  env("OCDS_BASE_URL", "https://contratacionesabiertas.oece.gob.pe/api/v1"),
		Window:       planner.Window{Sources: sources, From: from, To: to},
		PlanInterval: interval,
		MetricsAddr:  env("METRICS_ADDR", ":2112"),
	}, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
