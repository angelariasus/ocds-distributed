// Package log is the append-only transparency log: entries are persisted as
// JSON lines, their leaf hashes kept in memory, and every state of the log is
// summarised by a signed tree head (size, root, timestamp) that auditors can
// store and later hold the log to.
package log

import (
	"bufio"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/angelariasus/ocds-distributed/services/ledger/internal/merkle"
)

// Entry is what gets attested: a kind of artefact, a stable reference to it and
// the digest of its content at that moment.
type Entry struct {
	Kind     string            `json:"kind"`
	Ref      string            `json:"ref"`
	Digest   string            `json:"digest"`
	Meta     map[string]string `json:"meta,omitempty"`
	LoggedAt time.Time         `json:"logged_at"`
}

// Canonical is the byte string that is hashed into the leaf. Meta and the
// timestamp are deliberately part of it: changing either changes the tree.
func (e Entry) Canonical() []byte {
	b, _ := json.Marshal(struct {
		Kind     string            `json:"kind"`
		Ref      string            `json:"ref"`
		Digest   string            `json:"digest"`
		Meta     map[string]string `json:"meta,omitempty"`
		LoggedAt string            `json:"logged_at"`
	}{e.Kind, e.Ref, e.Digest, e.Meta, e.LoggedAt.UTC().Format(time.RFC3339Nano)})
	return b
}

type SignedTreeHead struct {
	Size      int    `json:"tree_size"`
	Root      string `json:"root_hash"`
	Timestamp int64  `json:"timestamp_ms"`
	Signature string `json:"signature"`
}

// SignedBytes is what the signature covers.
func SignedBytes(size int, root merkle.Hash, ts int64) []byte {
	buf := make([]byte, 0, 16+merkle.HashSize)
	buf = binary.BigEndian.AppendUint64(buf, uint64(size))
	buf = binary.BigEndian.AppendUint64(buf, uint64(ts))
	return append(buf, root[:]...)
}

func VerifySTH(pub ed25519.PublicKey, sth SignedTreeHead) error {
	root, err := ParseHash(sth.Root)
	if err != nil {
		return err
	}
	sig, err := hex.DecodeString(sth.Signature)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, SignedBytes(sth.Size, root, sth.Timestamp), sig) {
		return errors.New("signed tree head: bad signature")
	}
	return nil
}

func ParseHash(s string) (merkle.Hash, error) {
	var h merkle.Hash
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != merkle.HashSize {
		return h, fmt.Errorf("invalid hash %q", s)
	}
	copy(h[:], b)
	return h, nil
}

func Hex(h merkle.Hash) string { return hex.EncodeToString(h[:]) }

type Log struct {
	key  ed25519.PrivateKey
	file *os.File
	now  func() time.Time

	mu      sync.RWMutex
	entries []Entry
	leaves  []merkle.Hash
	seen    map[string]int // dedupe key → index
}

func dedupeKey(kind, ref, digest string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + ref + "\x00" + digest))
	return hex.EncodeToString(sum[:])
}

// Open replays the entries file (creating it if needed).
func Open(path string, key ed25519.PrivateKey, now func() time.Time) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{key: key, file: f, now: now, seen: map[string]int{}}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	for scanner.Scan() {
		var e Entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			f.Close()
			return nil, fmt.Errorf("entry %d: %w", len(l.entries), err)
		}
		l.add(e)
	}
	if err := scanner.Err(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

func (l *Log) add(e Entry) int {
	idx := len(l.entries)
	l.entries = append(l.entries, e)
	l.leaves = append(l.leaves, merkle.LeafHash(e.Canonical()))
	l.seen[dedupeKey(e.Kind, e.Ref, e.Digest)] = idx
	return idx
}

func (l *Log) Close() error { return l.file.Close() }

// Append is idempotent on (kind, ref, digest): attesting the same artefact
// twice (a redelivered Kafka message) returns the original index. A new digest
// for the same ref is a new version and gets a new entry.
func (l *Log) Append(kind, ref, digest string, meta map[string]string) (int, bool, error) {
	if kind == "" || ref == "" || digest == "" {
		return 0, false, errors.New("kind, ref and digest are required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if idx, ok := l.seen[dedupeKey(kind, ref, digest)]; ok {
		return idx, false, nil
	}
	e := Entry{Kind: kind, Ref: ref, Digest: digest, Meta: meta, LoggedAt: l.now().UTC()}
	line, err := json.Marshal(e)
	if err != nil {
		return 0, false, err
	}
	if _, err := l.file.Write(append(line, '\n')); err != nil {
		return 0, false, err
	}
	if err := l.file.Sync(); err != nil {
		return 0, false, err
	}
	return l.add(e), true, nil
}

func (l *Log) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

func (l *Log) Entry(i int) (Entry, merkle.Hash, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if i < 0 || i >= len(l.entries) {
		return Entry{}, merkle.Hash{}, errors.New("index out of range")
	}
	return l.entries[i], l.leaves[i], nil
}

// Find returns the index of the latest entry for kind/ref.
func (l *Log) Find(kind, ref string) (int, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Kind == kind && l.entries[i].Ref == ref {
			return i, true
		}
	}
	return 0, false
}

func (l *Log) STH() SignedTreeHead {
	l.mu.RLock()
	size := len(l.leaves)
	root := merkle.Root(l.leaves)
	l.mu.RUnlock()
	ts := l.now().UnixMilli()
	sig := ed25519.Sign(l.key, SignedBytes(size, root, ts))
	return SignedTreeHead{Size: size, Root: Hex(root), Timestamp: ts, Signature: hex.EncodeToString(sig)}
}

func (l *Log) InclusionProof(index, size int) ([]merkle.Hash, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if size <= 0 || size > len(l.leaves) {
		return nil, errors.New("tree size out of range")
	}
	return merkle.InclusionProof(l.leaves[:size], index)
}

func (l *Log) ConsistencyProof(first, second int) ([]merkle.Hash, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if second <= 0 || second > len(l.leaves) {
		return nil, errors.New("tree size out of range")
	}
	return merkle.ConsistencyProof(l.leaves[:second], first)
}
