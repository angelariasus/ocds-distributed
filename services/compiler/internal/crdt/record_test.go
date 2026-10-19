package crdt

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func record(ocid, compiledDate string, releaseIDs ...string) json.RawMessage {
	releases := make([]map[string]any, 0, len(releaseIDs))
	for _, id := range releaseIDs {
		releases = append(releases, map[string]any{"id": id, "date": compiledDate, "tag": []string{"tender"}})
	}
	raw, _ := json.Marshal(map[string]any{
		"ocid":            ocid,
		"compiledRelease": map[string]any{"ocid": ocid, "date": compiledDate, "tender": map[string]any{"id": "1"}},
		"releases":        releases,
	})
	return raw
}

func obs(origin, worker string, lamport uint64, seen time.Time, rec json.RawMessage) Observation {
	return Observation{OCID: "ocds-1", Source: "seace_v3", Origin: origin, WorkerID: worker, Lamport: lamport, ObservedAt: seen, Record: rec}
}

func mergeAll(t *testing.T, observations []Observation) *State {
	t.Helper()
	var acc *State
	for _, o := range observations {
		s, err := FromObservation(o)
		if err != nil {
			t.Fatal(err)
		}
		if acc == nil {
			acc = s
			continue
		}
		acc.Merge(s)
	}
	return acc
}

func fixture() []Observation {
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	return []Observation{
		obs("bulk:seace_v3-2026-08", "w1", 3, t0, record("ocds-1", "2026-08-20T00:00:00Z", "r1", "r2")),
		obs("api:ocds/api/.../120001.json", "w2", 7, t0.Add(time.Hour), record("ocds-1", "2026-09-05T00:00:00Z", "r1", "r2", "r3")),
		obs("bulk:seace_v3-2026-09", "w3", 5, t0.Add(2*time.Hour), record("ocds-1", "2026-09-05T00:00:00Z", "r1", "r2", "r3")),
		obs("bulk:seace_v3-2026-09", "w1", 9, t0.Add(3*time.Hour), record("ocds-1", "2026-09-07T00:00:00Z", "r1", "r2", "r3", "r4")),
	}
}

func TestMergeIsOrderIndependent(t *testing.T) {
	base := fixture()
	want := mergeAll(t, base).Fingerprint()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		shuffled := append([]Observation(nil), base...)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		if got := mergeAll(t, shuffled).Fingerprint(); got != want {
			t.Fatalf("order %d produced a different state", i)
		}
	}
}

func TestMergeIsIdempotent(t *testing.T) {
	base := fixture()
	s := mergeAll(t, base)
	before := s.Fingerprint()
	for _, o := range base {
		dup, _ := FromObservation(o)
		if s.Merge(dup) {
			t.Fatalf("re-merging %s reported a change", o.Origin)
		}
	}
	if s.Fingerprint() != before {
		t.Fatal("state changed after duplicate merges")
	}
}

func TestMergeIsAssociative(t *testing.T) {
	o := fixture()
	lift := func(i int) *State { s, _ := FromObservation(o[i]); return s }
	left := lift(0)
	left.Merge(lift(1))
	left.Merge(lift(2))
	right12 := lift(1)
	right12.Merge(lift(2))
	right := lift(0)
	right.Merge(right12)
	if left.Fingerprint() != right.Fingerprint() {
		t.Fatal("(a⊔b)⊔c != a⊔(b⊔c)")
	}
}

func TestMergedStateKeepsNewestCompiledAndUnionOfReleases(t *testing.T) {
	s := mergeAll(t, fixture())
	if s.Compiled.Date != "2026-09-07T00:00:00Z" {
		t.Fatalf("compiled date = %s", s.Compiled.Date)
	}
	if len(s.Releases) != 4 || len(s.Origins) != 3 {
		t.Fatalf("releases=%d origins=%d", len(s.Releases), len(s.Origins))
	}
	if s.Lamport != 9 || s.TotalObservations() != 4 || s.Observations()["w1"] != 2 {
		t.Fatalf("lamport=%d observations=%v", s.Lamport, s.Observations())
	}
	if !s.FirstSeen.Equal(fixture()[0].ObservedAt) || !s.LastSeen.Equal(fixture()[3].ObservedAt) {
		t.Fatalf("first=%v last=%v", s.FirstSeen, s.LastSeen)
	}
}

func TestReobservationUpdatesTimestampsWithoutCountingAsChange(t *testing.T) {
	first := fixture()[3]
	s, _ := FromObservation(first)
	again := first
	again.ObservedAt = first.ObservedAt.Add(24 * time.Hour)
	again.Lamport = first.Lamport + 50
	later, _ := FromObservation(again)
	if s.Merge(later) {
		t.Fatal("an unchanged record seen again must not count as a change")
	}
	if !s.LastSeen.Equal(again.ObservedAt) || s.Lamport != again.Lamport {
		t.Fatalf("timestamps not merged: last=%v lamport=%d", s.LastSeen, s.Lamport)
	}
}

func TestStaleObservationIsAbsorbedWithoutRegressing(t *testing.T) {
	o := fixture()
	s, _ := FromObservation(o[3])
	old, _ := FromObservation(o[0])
	s.Merge(old)
	if s.Compiled.Date != "2026-09-07T00:00:00Z" {
		t.Fatal("older compiled release overwrote the newer one")
	}
	if !s.Origins["bulk:seace_v3-2026-08"] {
		t.Fatal("stale origin must still be recorded as provenance")
	}
}

func TestEqualDatesBreakTiesDeterministically(t *testing.T) {
	a := obs("x", "w", 1, time.Now(), record("ocds-1", "2026-09-05T00:00:00Z", "r1"))
	b := obs("y", "w", 1, time.Now(), record("ocds-1", "2026-09-05T00:00:00Z", "r1", "r2"))
	ab := mergeAll(t, []Observation{a, b})
	ba := mergeAll(t, []Observation{b, a})
	if ab.Compiled.Digest != ba.Compiled.Digest {
		t.Fatal("tie-break depends on order")
	}
}

func TestSightingsCountDistinctOriginsNotRedeliveries(t *testing.T) {
	rec := record("ocds-1", "2026-09-05T00:00:00Z", "r1")
	now := time.Now()
	s, _ := FromObservation(obs("page-1", "w1", 1, now, rec))
	for i := 2; i <= 5; i++ {
		next, _ := FromObservation(obs(fmt.Sprintf("page-%d", i), "w1", uint64(i), now, rec))
		s.Merge(next)
	}
	for i := 0; i < 3; i++ {
		dup, _ := FromObservation(obs("page-3", "w1", 3, now, rec))
		s.Merge(dup)
	}
	if s.Observations()["w1"] != 5 || s.TotalObservations() != 5 {
		t.Fatalf("observations = %v", s.Observations())
	}
}

func TestAmendedReleaseConvergesToTheLatestVersion(t *testing.T) {
	early := obs("a", "w", 1, time.Now(), record("ocds-1", "2026-08-01T00:00:00Z", "r1"))
	late := obs("b", "w", 2, time.Now(), record("ocds-1", "2026-09-01T00:00:00Z", "r1"))
	ab := mergeAll(t, []Observation{early, late})
	ba := mergeAll(t, []Observation{late, early})
	if ab.Releases["r1"].Date != "2026-09-01T00:00:00Z" || ab.Fingerprint() != ba.Fingerprint() {
		t.Fatalf("release register did not converge: %+v vs %+v", ab.Releases["r1"], ba.Releases["r1"])
	}
}
