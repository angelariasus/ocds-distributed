// Package merkle implements the Merkle tree of RFC 6962 (Certificate
// Transparency): leaf and interior hashes are domain-separated so a leaf can
// never be passed off as an interior node, the tree over n leaves splits at
// the largest power of two below n, and two kinds of proof exist:
//
//   - inclusion: leaf i is part of the tree of size n whose root is R;
//   - consistency: the tree of size m is a prefix of the tree of size n, i.e.
//     the log only ever appended between the two signed heads.
//
// Both proofs are O(log n) hashes, which is what lets many independent
// auditors check a log without downloading it.
package merkle

import (
	"bytes"
	"crypto/sha256"
	"errors"
)

const HashSize = sha256.Size

type Hash = [HashSize]byte

func LeafHash(data []byte) Hash {
	return sha256.Sum256(append([]byte{0x00}, data...))
}

func NodeHash(left, right Hash) Hash {
	buf := make([]byte, 0, 1+2*HashSize)
	buf = append(buf, 0x01)
	buf = append(buf, left[:]...)
	buf = append(buf, right[:]...)
	return sha256.Sum256(buf)
}

// EmptyRoot is the root of a tree with no leaves.
func EmptyRoot() Hash { return sha256.Sum256(nil) }

// split returns the largest power of two strictly smaller than n (n >= 2).
func split(n int) int {
	k := 1
	for k<<1 < n {
		k <<= 1
	}
	return k
}

// Root computes MTH(leaves).
func Root(leaves []Hash) Hash {
	switch len(leaves) {
	case 0:
		return EmptyRoot()
	case 1:
		return leaves[0]
	}
	k := split(len(leaves))
	return NodeHash(Root(leaves[:k]), Root(leaves[k:]))
}

// InclusionProof returns the audit path for leaf index in the tree over leaves.
func InclusionProof(leaves []Hash, index int) ([]Hash, error) {
	if index < 0 || index >= len(leaves) {
		return nil, errors.New("merkle: index out of range")
	}
	return path(leaves, index), nil
}

func path(leaves []Hash, m int) []Hash {
	n := len(leaves)
	if n <= 1 {
		return nil
	}
	k := split(n)
	if m < k {
		return append(path(leaves[:k], m), Root(leaves[k:]))
	}
	return append(path(leaves[k:], m-k), Root(leaves[:k]))
}

// VerifyInclusion recomputes the root from a leaf hash and its audit path
// (RFC 9162, section 2.1.3.2).
func VerifyInclusion(leaf Hash, index, size int, proof []Hash, root Hash) bool {
	if index < 0 || index >= size {
		return false
	}
	fn, sn := index, size-1
	r := leaf
	for _, p := range proof {
		if sn == 0 {
			return false
		}
		if fn&1 == 1 || fn == sn {
			r = NodeHash(p, r)
			for fn&1 == 0 && fn != 0 {
				fn >>= 1
				sn >>= 1
			}
		} else {
			r = NodeHash(r, p)
		}
		fn >>= 1
		sn >>= 1
	}
	return sn == 0 && r == root
}

// ConsistencyProof proves the tree of size m is a prefix of the tree over leaves.
func ConsistencyProof(leaves []Hash, m int) ([]Hash, error) {
	n := len(leaves)
	if m <= 0 || m > n {
		return nil, errors.New("merkle: invalid sizes for a consistency proof")
	}
	if m == n {
		return nil, nil
	}
	return subproof(m, leaves, true), nil
}

func subproof(m int, leaves []Hash, complete bool) []Hash {
	n := len(leaves)
	if m == n {
		if complete {
			return nil
		}
		return []Hash{Root(leaves)}
	}
	k := split(n)
	if m <= k {
		return append(subproof(m, leaves[:k], complete), Root(leaves[k:]))
	}
	return append(subproof(m-k, leaves[k:], false), Root(leaves[:k]))
}

// VerifyConsistency checks that oldRoot (size m) and newRoot (size n) belong to
// the same append-only log (RFC 9162, section 2.1.4.2).
func VerifyConsistency(m, n int, oldRoot, newRoot Hash, proof []Hash) bool {
	if m <= 0 || m > n {
		return false
	}
	if m == n {
		return len(proof) == 0 && oldRoot == newRoot
	}
	if m&(m-1) == 0 { // m is a power of two: the old root is a node of the new tree
		proof = append([]Hash{oldRoot}, proof...)
	}
	if len(proof) == 0 {
		return false
	}
	fn, sn := m-1, n-1
	for fn&1 == 1 {
		fn >>= 1
		sn >>= 1
	}
	fr, sr := proof[0], proof[0]
	for _, c := range proof[1:] {
		if sn == 0 {
			return false
		}
		if fn&1 == 1 || fn == sn {
			fr = NodeHash(c, fr)
			sr = NodeHash(c, sr)
			for fn&1 == 0 && fn != 0 {
				fn >>= 1
				sn >>= 1
			}
		} else {
			sr = NodeHash(sr, c)
		}
		fn >>= 1
		sn >>= 1
	}
	return sn == 0 && bytes.Equal(fr[:], oldRoot[:]) && bytes.Equal(sr[:], newRoot[:])
}
