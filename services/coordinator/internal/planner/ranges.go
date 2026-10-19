package planner

import (
	"fmt"
	"strings"
	"time"
)

const KindBulkFile = "bulk_file"
const KindRecordsRange = "records_range"

type RangeWindow struct {
	Sources  []string
	Prefixes []string
}

// Prefixes enumerates every tender-id prefix of the given length. Tender ids
// sort lexicographically on the portal, so a prefix is a contiguous slice of
// the cursor keyspace that one worker can walk on its own.
func Prefixes(length int) []string {
	if length <= 0 {
		return nil
	}
	out := []string{""}
	for i := 0; i < length; i++ {
		next := make([]string, 0, len(out)*10)
		for _, p := range out {
			for d := '0'; d <= '9'; d++ {
				next = append(next, p+string(d))
			}
		}
		out = next
	}
	return out
}

func PlanRanges(w RangeWindow, now time.Time) []Task {
	var tasks []Task
	for _, source := range w.Sources {
		for _, prefix := range w.Prefixes {
			prefix = strings.TrimSpace(prefix)
			if prefix == "" {
				continue
			}
			tasks = append(tasks, Task{
				TaskID:    fmt.Sprintf("api-%s-%s", source, prefix),
				Kind:      KindRecordsRange,
				Source:    source,
				Prefix:    prefix,
				PlannedAt: now,
			})
		}
	}
	return tasks
}
