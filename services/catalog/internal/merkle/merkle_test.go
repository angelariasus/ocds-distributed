package merkle

import (
	"fmt"
	"testing"

	"github.com/angelariasus/ocds-distributed/services/catalog/internal/store"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/vclock"
)

func filled(n int) *store.Store {
	s, _ := store.Open("")
	for i := 0; i < n; i++ {
		s.Put(fmt.Sprintf("key-%d", i), store.Version{Value: []byte("v"), Clock: vclock.Clock{"a": 1}})
	}
	return s
}

func remote(t *Tree) func(int) ([][]byte, error) {
	return func(d int) ([][]byte, error) { return t.Level(d), nil }
}

func TestIdenticalStoresHaveEqualRoots(t *testing.T) {
	a, b := Build(filled(500).ForEach), Build(filled(500).ForEach)
	if !equal(a.Root(), b.Root()) {
		t.Fatal("roots differ")
	}
	diff, err := Diff(a, remote(b))
	if err != nil || len(diff) != 0 {
		t.Fatalf("diff = %v, err = %v", diff, err)
	}
}

func TestDiffPinpointsChangedBuckets(t *testing.T) {
	sa, sb := filled(2000), filled(2000)
	sb.Put("key-7", store.Version{Value: []byte("changed"), Clock: vclock.Clock{"a": 2}})
	sb.Put("extra", store.Version{Value: []byte("new"), Clock: vclock.Clock{"b": 1}})

	diff, err := Diff(Build(sa.ForEach), remote(Build(sb.ForEach)))
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint32]bool{BucketOf("key-7"): true, BucketOf("extra"): true}
	if len(diff) != len(want) {
		t.Fatalf("diff = %v, want buckets %v", diff, want)
	}
	for _, b := range diff {
		if !want[b] {
			t.Fatalf("unexpected bucket %d in %v", b, diff)
		}
	}
}

func TestDiffFetchesOnlyWhileMismatched(t *testing.T) {
	a := Build(filled(100).ForEach)
	calls := 0
	Diff(a, func(d int) ([][]byte, error) { calls++; return a.Level(d), nil })
	if calls != 1 {
		t.Fatalf("equal trees should need one fetch, got %d", calls)
	}
}
