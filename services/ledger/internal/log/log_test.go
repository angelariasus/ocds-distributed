package log

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/angelariasus/ocds-distributed/services/ledger/internal/merkle"
)

func fixedClock() func() time.Time {
	t := time.Date(2026, 10, 19, 6, 0, 0, 0, time.UTC)
	return func() time.Time { t = t.Add(time.Second); return t }
}

func open(t *testing.T, path string, key ed25519.PrivateKey) *Log {
	t.Helper()
	l, err := Open(path, key, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestAppendIsIdempotentPerDigest(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	l := open(t, filepath.Join(t.TempDir(), "log.jsonl"), key)
	i1, new1, _ := l.Append("bronze.object", "k1", "aa", nil)
	i2, new2, _ := l.Append("bronze.object", "k1", "aa", nil)
	i3, new3, _ := l.Append("bronze.object", "k1", "bb", nil)
	if !new1 || new2 || !new3 || i1 != i2 || i3 != 1 || l.Size() != 2 {
		t.Fatalf("i=%d,%d,%d new=%v,%v,%v size=%d", i1, i2, i3, new1, new2, new3, l.Size())
	}
	if idx, ok := l.Find("bronze.object", "k1"); !ok || idx != 1 {
		t.Fatalf("latest version index = %d", idx)
	}
}

func TestSignedHeadVerifiesAndProofsCheckAgainstIt(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	l := open(t, filepath.Join(t.TempDir(), "log.jsonl"), key)
	for i := 0; i < 9; i++ {
		l.Append("gold.load", fmt.Sprintf("token:%d", i), fmt.Sprintf("%064x", i), nil)
	}
	sth := l.STH()
	if err := VerifySTH(pub, sth); err != nil {
		t.Fatal(err)
	}
	root, _ := ParseHash(sth.Root)
	_, leaf, _ := l.Entry(4)
	proof, err := l.InclusionProof(4, sth.Size)
	if err != nil || !merkle.VerifyInclusion(leaf, 4, sth.Size, proof, root) {
		t.Fatalf("inclusion failed: %v", err)
	}
	tampered := sth
	tampered.Size++
	if VerifySTH(pub, tampered) == nil {
		t.Fatal("a modified tree head kept a valid signature")
	}
}

func TestReopenRebuildsTheSameTree(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	path := filepath.Join(t.TempDir(), "log.jsonl")
	l := open(t, path, key)
	for i := 0; i < 5; i++ {
		l.Append("k", fmt.Sprintf("r%d", i), "d", nil)
	}
	before := l.STH()
	l.Close()
	again, err := Open(path, key, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if after := again.STH(); after.Root != before.Root || after.Size != before.Size {
		t.Fatalf("root changed across restart: %s vs %s", before.Root, after.Root)
	}
}

func TestEditingTheFileIsDetectedByConsistency(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	path := filepath.Join(t.TempDir(), "log.jsonl")
	l := open(t, path, key)
	for i := 0; i < 6; i++ {
		l.Append("gold.load", fmt.Sprintf("token:%d", i), fmt.Sprintf("%064x", i), nil)
	}
	auditorSaw := l.STH()
	l.Close()

	raw, _ := os.ReadFile(path)
	forged := strings.Replace(string(raw), fmt.Sprintf("%064x", 2), fmt.Sprintf("%064x", 999), 1)
	os.WriteFile(path, []byte(forged), 0o644)

	rewritten, err := Open(path, key, fixedClock())
	if err != nil {
		t.Fatal(err)
	}
	defer rewritten.Close()
	rewritten.Append("gold.load", "token:6", fmt.Sprintf("%064x", 6), nil)
	now := rewritten.STH()
	proof, _ := rewritten.ConsistencyProof(auditorSaw.Size, now.Size)
	oldRoot, _ := ParseHash(auditorSaw.Root)
	newRoot, _ := ParseHash(now.Root)
	if merkle.VerifyConsistency(auditorSaw.Size, now.Size, oldRoot, newRoot, proof) {
		t.Fatal("the auditor accepted a log whose history was edited on disk")
	}
}
