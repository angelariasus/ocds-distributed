// Package merkle summarises a replica's contents as a hash tree so two nodes
// can find the few buckets that differ by exchanging O(log n) hashes instead
// of every key.
package merkle

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/angelariasus/ocds-distributed/services/catalog/internal/ring"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/store"
)

// Depth fixes the number of leaf buckets to 2^Depth.
const Depth = 10

func Buckets() int { return 1 << Depth }

func BucketOf(key string) uint32 {
	return uint32(ring.Hash(key) >> (64 - Depth))
}

type Tree struct {
	levels [][][]byte // levels[d] holds 2^d hashes; levels[Depth] are the leaves
}

// Build hashes every key into its bucket and folds the buckets upwards.
func Build(forEach func(fn func(key string, versions []store.Version))) *Tree {
	leaves := make([][]byte, Buckets())
	hashers := make([][]byte, Buckets())
	forEach(func(key string, versions []store.Version) {
		b := BucketOf(key)
		hashers[b] = append(hashers[b], versionsDigest(key, versions)...)
	})
	for i := range leaves {
		sum := sha256.Sum256(hashers[i])
		leaves[i] = sum[:]
	}
	t := &Tree{levels: make([][][]byte, Depth+1)}
	t.levels[Depth] = leaves
	for d := Depth - 1; d >= 0; d-- {
		below := t.levels[d+1]
		level := make([][]byte, len(below)/2)
		for i := range level {
			sum := sha256.Sum256(append(append([]byte{}, below[2*i]...), below[2*i+1]...))
			level[i] = sum[:]
		}
		t.levels[d] = level
	}
	return t
}

func versionsDigest(key string, versions []store.Version) []byte {
	h := sha256.New()
	h.Write([]byte(key))
	for _, v := range versions {
		h.Write([]byte(v.Clock.String()))
		h.Write(v.Value)
		if v.Tombstone {
			h.Write([]byte{1})
		}
	}
	return h.Sum(nil)
}

func (t *Tree) Level(depth int) [][]byte {
	if depth < 0 || depth > Depth {
		return nil
	}
	return t.levels[depth]
}

func (t *Tree) Root() []byte { return t.levels[0][0] }

// Diff walks both trees top-down and returns the leaf buckets whose hashes
// differ. fetch returns the remote hashes for a depth; it is only called for
// depths where a mismatch exists above.
func Diff(local *Tree, fetch func(depth int) ([][]byte, error)) ([]uint32, error) {
	remoteRoot, err := fetch(0)
	if err != nil {
		return nil, err
	}
	if len(remoteRoot) != 1 || equal(remoteRoot[0], local.Root()) {
		return nil, nil
	}
	suspects := []uint32{0}
	for d := 1; d <= Depth; d++ {
		remote, err := fetch(d)
		if err != nil {
			return nil, err
		}
		if len(remote) != len(local.levels[d]) {
			return nil, errMismatchedDepth
		}
		var next []uint32
		for _, parent := range suspects {
			for _, child := range []uint32{2 * parent, 2*parent + 1} {
				if !equal(remote[child], local.levels[d][child]) {
					next = append(next, child)
				}
			}
		}
		suspects = next
		if len(suspects) == 0 {
			return nil, nil
		}
	}
	return suspects, nil
}

type depthError struct{}

func (depthError) Error() string { return "merkle: remote tree has a different depth" }

var errMismatchedDepth = depthError{}

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// BucketID packs a bucket for logging and metrics.
func BucketID(b uint32) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, b)
	return out
}
