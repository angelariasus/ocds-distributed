package planner

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPrefixesEnumerateDigits(t *testing.T) {
	if got := Prefixes(0); got != nil {
		t.Fatalf("Prefixes(0) = %v", got)
	}
	if got := Prefixes(1); len(got) != 10 || got[0] != "0" || got[9] != "9" {
		t.Fatalf("Prefixes(1) = %v", got)
	}
	if got := Prefixes(2); len(got) != 100 || got[0] != "00" || got[99] != "99" {
		t.Fatalf("Prefixes(2) has %d entries", len(got))
	}
}

func TestPlanRangesCrossesSourcesAndPrefixes(t *testing.T) {
	tasks := PlanRanges(RangeWindow{Sources: []string{"seace_v3", "seace_v2"}, Prefixes: []string{"12", " 9 ", ""}}, time.Now())
	if len(tasks) != 4 {
		t.Fatalf("got %d tasks", len(tasks))
	}
	if tasks[0].TaskID != "api-seace_v3-12" || tasks[1].Prefix != "9" || tasks[0].Kind != KindRecordsRange {
		t.Fatalf("tasks = %+v", tasks)
	}
}

func TestTaskJSONOmitsBulkFieldsForRanges(t *testing.T) {
	raw, err := json.Marshal(PlanRanges(RangeWindow{Sources: []string{"seace_v3"}, Prefixes: []string{"1"}}, time.Now())[0])
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(raw, &m)
	for _, key := range []string{"url", "sha_url", "year", "month", "published_at"} {
		if _, ok := m[key]; ok {
			t.Errorf("unexpected %q in %s", key, raw)
		}
	}
	if m["kind"] != KindRecordsRange || m["prefix"] != "1" {
		t.Fatalf("payload = %s", raw)
	}
}
