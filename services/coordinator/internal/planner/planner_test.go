package planner

import (
	"testing"
	"time"

	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/ocds"
)

func mustYM(t *testing.T, s string) YearMonth {
	t.Helper()
	ym, err := ParseYearMonth(s)
	if err != nil {
		t.Fatal(err)
	}
	return ym
}

func TestPlanFiltersBySourceAndWindow(t *testing.T) {
	files := []ocds.File{
		{Source: "seace_v3", Year: 2026, Month: 10},
		{Source: "seace_v3", Year: 2026, Month: 9},
		{Source: "seace_v3", Year: 2025, Month: 12},
		{Source: "seace_v3", Year: 2025, Month: 11},
		{Source: "seace_v2", Year: 2026, Month: 9},
	}
	w := Window{
		Sources: map[string]bool{"seace_v3": true},
		From:    mustYM(t, "2025-12"),
		To:      mustYM(t, "2026-09"),
	}

	tasks := Plan(files, w, time.Now())

	want := []string{"seace_v3-2025-12", "seace_v3-2026-09"}
	if len(tasks) != len(want) {
		t.Fatalf("got %d tasks, want %d", len(tasks), len(want))
	}
	for i, id := range want {
		if tasks[i].TaskID != id {
			t.Errorf("task %d: got %s, want %s", i, tasks[i].TaskID, id)
		}
	}
}

func TestPlanWithoutSourcesIncludesAll(t *testing.T) {
	files := []ocds.File{
		{Source: "seace_v2", Year: 2010, Month: 1},
		{Source: "seace_v3", Year: 2010, Month: 1},
	}
	w := Window{From: mustYM(t, "2010-01"), To: mustYM(t, "2010-01")}
	if got := len(Plan(files, w, time.Now())); got != 2 {
		t.Fatalf("got %d tasks, want 2", got)
	}
}

func TestParseYearMonthRejectsGarbage(t *testing.T) {
	for _, s := range []string{"", "2026", "2026-13", "26-01"} {
		if _, err := ParseYearMonth(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}
