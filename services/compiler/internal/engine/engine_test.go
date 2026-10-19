package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/angelariasus/ocds-distributed/services/compiler/internal/crdt"
)

func observation(ocid, origin, date string, lamport uint64) []byte {
	rec, _ := json.Marshal(map[string]any{
		"ocid":            ocid,
		"compiledRelease": map[string]any{"ocid": ocid, "date": date},
		"releases":        []map[string]any{{"id": ocid + "-" + date, "date": date, "tag": []string{"tender"}}},
	})
	raw, _ := json.Marshal(crdt.Observation{
		OCID: ocid, Source: "seace_v3", Origin: origin, WorkerID: "w1",
		ObservedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Lamport: lamport, Record: rec,
	})
	return raw
}

func TestApplyMergesAndDetectsDuplicates(t *testing.T) {
	e := New()
	e.Assign(0)
	s, out, err := e.Apply(0, observation("ocds-1", "bulk:a", "2026-09-01T00:00:00Z", 5))
	if err != nil || out != Merged || s.Lamport <= 5 {
		t.Fatalf("first apply: out=%s lamport=%d err=%v", out, s.Lamport, err)
	}
	_, out, _ = e.Apply(0, observation("ocds-1", "bulk:a", "2026-09-01T00:00:00Z", 5))
	if out != Duplicate {
		t.Fatalf("redelivery reported %s", out)
	}
	s, out, _ = e.Apply(0, observation("ocds-1", "api:p1", "2026-09-03T00:00:00Z", 2))
	if out != Merged || len(s.Origins) != 2 || s.Compiled.Date != "2026-09-03T00:00:00Z" {
		t.Fatalf("second origin: out=%s origins=%d date=%s", out, len(s.Origins), s.Compiled.Date)
	}
}

func TestReobservationIsDuplicateEvenWithNewerTimestamps(t *testing.T) {
	e := New()
	e.Assign(0)
	e.Apply(0, observation("ocds-1", "api:p1", "2026-09-01T00:00:00Z", 1))
	raw := observation("ocds-1", "api:p1", "2026-09-01T00:00:00Z", 900)
	var o crdt.Observation
	json.Unmarshal(raw, &o)
	o.ObservedAt = o.ObservedAt.Add(time.Hour)
	raw, _ = json.Marshal(o)
	s, out, _ := e.Apply(0, raw)
	if out != Duplicate {
		t.Fatalf("re-crawl of an unchanged record reported %s", out)
	}
	if s.Lamport < 900 {
		t.Fatalf("state lamport %d should have witnessed 900", s.Lamport)
	}
}

func TestLamportAdvancesPastEveryObservation(t *testing.T) {
	e := New()
	e.Assign(0)
	e.Apply(0, observation("ocds-1", "a", "2026-09-01T00:00:00Z", 100))
	s, _, _ := e.Apply(0, observation("ocds-2", "a", "2026-09-01T00:00:00Z", 3))
	if s.Lamport <= 100 {
		t.Fatalf("lamport %d did not move past the earlier witnessed 100", s.Lamport)
	}
}

func TestRestoreRebuildsStateFromChangelog(t *testing.T) {
	owner := New()
	owner.Assign(3)
	var changelog [][]byte
	for _, origin := range []string{"bulk:a", "api:p1", "api:p2"} {
		s, out, _ := owner.Apply(3, observation("ocds-9", origin, "2026-09-01T00:00:00Z", 1))
		if out == Merged {
			raw, _ := json.Marshal(s)
			changelog = append(changelog, raw)
		}
	}

	successor := New()
	successor.Assign(3)
	for _, entry := range changelog {
		if err := successor.Restore(3, entry); err != nil {
			t.Fatal(err)
		}
	}
	want, _ := owner.Get(3, "ocds-9")
	got, ok := successor.Get(3, "ocds-9")
	if !ok || got.Fingerprint() != want.Fingerprint() {
		t.Fatal("restored state differs from the owner's")
	}
	if successor.Lamport() < want.Lamport {
		t.Fatal("successor's clock must not fall behind the restored state")
	}
	_, out, _ := successor.Apply(3, observation("ocds-9", "api:p2", "2026-09-01T00:00:00Z", 1))
	if out != Duplicate {
		t.Fatalf("successor re-merged a known sighting as %s", out)
	}
}

func TestRevokeDropsPartitionState(t *testing.T) {
	e := New()
	e.Assign(1)
	e.Apply(1, observation("ocds-1", "a", "2026-09-01T00:00:00Z", 1))
	e.Revoke(1)
	if e.KeyCount() != 0 {
		t.Fatal("state survived revocation")
	}
	if _, _, err := e.Apply(1, observation("ocds-1", "a", "2026-09-01T00:00:00Z", 1)); err == nil {
		t.Fatal("apply on a revoked partition must fail")
	}
}
