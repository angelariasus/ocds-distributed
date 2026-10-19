package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/angelariasus/ocds-distributed/services/catalog/internal/vclock"
)

func v(value string, clock vclock.Clock) Version {
	return Version{Value: []byte(value), Clock: clock, UpdatedAt: time.Unix(0, 0)}
}

func TestDescendantReplacesAncestor(t *testing.T) {
	s, _ := Open("")
	s.Put("k", v("one", vclock.Clock{"a": 1}))
	changed, _ := s.Put("k", v("two", vclock.Clock{"a": 2}))
	if !changed {
		t.Fatal("descendant must be accepted")
	}
	got := s.Get("k")
	if len(got) != 1 || string(got[0].Value) != "two" {
		t.Fatalf("got %+v", got)
	}
}

func TestStaleWriteIsIgnored(t *testing.T) {
	s, _ := Open("")
	s.Put("k", v("two", vclock.Clock{"a": 2}))
	changed, _ := s.Put("k", v("one", vclock.Clock{"a": 1}))
	if changed {
		t.Fatal("ancestor must be ignored")
	}
	if got := s.Get("k"); string(got[0].Value) != "two" {
		t.Fatalf("got %+v", got)
	}
}

func TestConcurrentWritesBecomeSiblings(t *testing.T) {
	s, _ := Open("")
	s.Put("k", v("from-a", vclock.Clock{"a": 1}))
	s.Put("k", v("from-b", vclock.Clock{"b": 1}))
	if got := s.Get("k"); len(got) != 2 {
		t.Fatalf("want 2 siblings, got %+v", got)
	}
	// A write that descends from both resolves the conflict.
	s.Put("k", v("merged", vclock.Clock{"a": 1, "b": 1, "c": 1}))
	if got := s.Get("k"); len(got) != 1 || string(got[0].Value) != "merged" {
		t.Fatalf("got %+v", got)
	}
}

func TestLogReplayRestoresState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.jsonl")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Put("a", v("1", vclock.Clock{"n": 1}))
	s.Put("a", v("2", vclock.Clock{"n": 2}))
	s.Put("b", v("x", vclock.Clock{"n": 1}))
	s.Put("b", v("y", vclock.Clock{"m": 1}))
	s.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Len() != 2 {
		t.Fatalf("len = %d", reopened.Len())
	}
	if got := reopened.Get("a"); len(got) != 1 || string(got[0].Value) != "2" {
		t.Fatalf("a = %+v", got)
	}
	if got := reopened.Get("b"); len(got) != 2 {
		t.Fatalf("b siblings = %+v", got)
	}
}
