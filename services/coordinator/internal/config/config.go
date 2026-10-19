package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/planner"
)

type Config struct {
	Brokers      []string
	TasksTopic   string
	OCDSBaseURL  string
	Window       planner.Window
	Ranges       planner.RangeWindow
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

	prefixLen, err := strconv.Atoi(env("PLAN_API_PREFIX_LEN", "0"))
	if err != nil {
		return Config{}, fmt.Errorf("PLAN_API_PREFIX_LEN: %w", err)
	}
	prefixes := splitList(env("PLAN_API_PREFIXES", ""))
	if len(prefixes) == 0 {
		prefixes = planner.Prefixes(prefixLen)
	}

	return Config{
		Brokers:      splitList(env("KAFKA_BROKERS", "localhost:19092")),
		TasksTopic:   env("TASKS_TOPIC", "extract.tasks"),
		OCDSBaseURL:  env("OCDS_BASE_URL", "https://contratacionesabiertas.oece.gob.pe/api/v1"),
		Window:       planner.Window{Sources: sources, From: from, To: to},
		Ranges:       planner.RangeWindow{Sources: splitList(env("PLAN_API_SOURCES", "")), Prefixes: prefixes},
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
