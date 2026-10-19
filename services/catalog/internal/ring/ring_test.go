package ring

import (
	"fmt"
	"testing"
)

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("crawl/api-seace_v3-%05d", i)
	}
	return out
}

func TestLoadIsBalancedAcrossNodes(t *testing.T) {
	r := New(128)
	for i := 1; i <= 5; i++ {
		r.Add(fmt.Sprintf("catalog%d", i))
	}
	counts := map[string]int{}
	ks := keys(100_000)
	for _, k := range ks {
		counts[r.Owner(k)]++
	}
	want := float64(len(ks)) / 5
	for node, c := range counts {
		if dev := float64(c)/want - 1; dev > 0.15 || dev < -0.15 {
			t.Errorf("%s owns %d keys, %.0f%% off the ideal share", node, c, dev*100)
		}
	}
}

func TestAddingANodeMovesOnlyItsShare(t *testing.T) {
	r := New(128)
	for i := 1; i <= 5; i++ {
		r.Add(fmt.Sprintf("catalog%d", i))
	}
	ks := keys(50_000)
	before := make([]string, len(ks))
	for i, k := range ks {
		before[i] = r.Owner(k)
	}
	r.Add("catalog6")
	moved := 0
	for i, k := range ks {
		if r.Owner(k) != before[i] {
			moved++
		}
	}
	share := float64(moved) / float64(len(ks))
	if share < 0.12 || share > 0.22 {
		t.Fatalf("%.1f%% of keys moved, want about 1/6", share*100)
	}
}

func TestPreferenceListHasDistinctNodesInRingOrder(t *testing.T) {
	r := New(64)
	for i := 1; i <= 4; i++ {
		r.Add(fmt.Sprintf("catalog%d", i))
	}
	pref := r.Preference("some-key", 3)
	if len(pref) != 3 {
		t.Fatalf("got %v", pref)
	}
	seen := map[string]bool{}
	for _, n := range pref {
		if seen[n] {
			t.Fatalf("duplicate node in %v", pref)
		}
		seen[n] = true
	}
	if pref[0] != r.Owner("some-key") {
		t.Fatalf("first entry %s is not the owner", pref[0])
	}
	if got := r.Preference("some-key", 10); len(got) != 4 {
		t.Fatalf("asked for more replicas than nodes, got %v", got)
	}
}

func TestRemoveRestoresPreviousOwners(t *testing.T) {
	r := New(64)
	r.Add("a")
	r.Add("b")
	ks := keys(1000)
	owners := map[string]string{}
	for _, k := range ks {
		owners[k] = r.Owner(k)
	}
	r.Add("c")
	r.Remove("c")
	for _, k := range ks {
		if r.Owner(k) != owners[k] {
			t.Fatalf("owner of %s changed after add+remove", k)
		}
	}
	if r.Has("c") || r.Len() != 2 {
		t.Fatal("c still present")
	}
}
