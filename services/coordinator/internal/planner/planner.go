package planner

import (
	"fmt"
	"sort"
	"time"

	"github.com/angelariasus/ocds-distributed/services/coordinator/internal/ocds"
)

type Task struct {
	TaskID      string    `json:"task_id"`
	Source      string    `json:"source"`
	Year        int       `json:"year"`
	Month       int       `json:"month"`
	URL         string    `json:"url"`
	SHAURL      string    `json:"sha_url"`
	PublishedAt time.Time `json:"published_at"`
	PlannedAt   time.Time `json:"planned_at"`
}

type YearMonth struct {
	Year  int
	Month int
}

func ParseYearMonth(s string) (YearMonth, error) {
	t, err := time.Parse("2006-01", s)
	if err != nil {
		return YearMonth{}, fmt.Errorf("invalid year-month %q, want YYYY-MM", s)
	}
	return YearMonth{Year: t.Year(), Month: int(t.Month())}, nil
}

func (ym YearMonth) index() int { return ym.Year*12 + ym.Month - 1 }

type Window struct {
	Sources map[string]bool
	From    YearMonth
	To      YearMonth
}

func (w Window) Contains(f ocds.File) bool {
	if len(w.Sources) > 0 && !w.Sources[f.Source] {
		return false
	}
	i := YearMonth{f.Year, f.Month}.index()
	return i >= w.From.index() && i <= w.To.index()
}

func Plan(files []ocds.File, w Window, now time.Time) []Task {
	var tasks []Task
	for _, f := range files {
		if !w.Contains(f) {
			continue
		}
		tasks = append(tasks, Task{
			TaskID:      fmt.Sprintf("%s-%04d-%02d", f.Source, f.Year, f.Month),
			Source:      f.Source,
			Year:        f.Year,
			Month:       f.Month,
			URL:         f.JSONURL,
			SHAURL:      f.SHAURL,
			PublishedAt: f.Published,
			PlannedAt:   now,
		})
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].TaskID < tasks[j].TaskID })
	return tasks
}
