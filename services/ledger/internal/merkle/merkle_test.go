package merkle

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func leaves(n int) []Hash {
	out := make([]Hash, n)
	for i := range out {
		out[i] = LeafHash([]byte(fmt.Sprintf("entry-%d", i)))
	}
	return out
}

// RFC 6962 test vectors: roots of the trees over the first n of these eight
// leaves (from the Certificate Transparency reference implementation).
var rfcLeaves = []string{"", "00", "10", "2021", "3031", "40414243", "5051525354555657", "606162636465666768696a6b6c6d6e6f"}

var rfcRoots = []string{
	"6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
	"fac54203e7cc696cf0dfcb42c92a1d9dbaf70ad9e621f4bd8d98662f00e3c125",
	"aeb6bcfe274b70a14fb067a5e5578264db0fa9b51af5e0ba159158f329e06e77",
	"d37ee418976dd95753c1c73862b9398fa2a2cf9b4ff0fdfe8b30cd95209614b7",
	"4e3bbb1f7b478dcfe71fb631631519a3bca12c9aefca1612bfce4c13a86264d4",
	"76e67dadbcdf1e10e1b74ddc608abd2f98dfb16fbce75277b5232a127f2087ef",
	"ddb89be403809e325750d3d263cd78929c2942b7942a34b77e122c9594a74c8c",
	"5dc9da79a70659a9ad559cb701ded9a2ab9d823aad2f4960cfe370eff4604328",
}

func TestRootsMatchRFC6962Vectors(t *testing.T) {
	var ls []Hash
	for i, h := range rfcLeaves {
		data, _ := hex.DecodeString(h)
		ls = append(ls, LeafHash(data))
		got := Root(ls)
		if hex.EncodeToString(got[:]) != rfcRoots[i] {
			t.Fatalf("root of %d leaves = %x, want %s", i+1, got, rfcRoots[i])
		}
	}
}

func TestInclusionProofsVerifyForEveryLeafAndSize(t *testing.T) {
	all := leaves(70)
	for n := 1; n <= len(all); n++ {
		tree := all[:n]
		root := Root(tree)
		for i := 0; i < n; i++ {
			proof, err := InclusionProof(tree, i)
			if err != nil {
				t.Fatal(err)
			}
			if !VerifyInclusion(tree[i], i, n, proof, root) {
				t.Fatalf("inclusion of %d in %d failed", i, n)
			}
			if n > 1 && VerifyInclusion(tree[(i+1)%n], i, n, proof, root) {
				t.Fatalf("proof for %d accepted a different leaf (size %d)", i, n)
			}
		}
	}
}

func TestConsistencyProofsVerifyForEveryPairOfSizes(t *testing.T) {
	all := leaves(70)
	for n := 1; n <= len(all); n++ {
		newRoot := Root(all[:n])
		for m := 1; m <= n; m++ {
			oldRoot := Root(all[:m])
			proof, err := ConsistencyProof(all[:n], m)
			if err != nil {
				t.Fatal(err)
			}
			if !VerifyConsistency(m, n, oldRoot, newRoot, proof) {
				t.Fatalf("consistency %d -> %d failed", m, n)
			}
		}
	}
}

func TestRewrittenHistoryFailsConsistency(t *testing.T) {
	honest := leaves(20)
	oldRoot := Root(honest[:12])

	// The operator rewrites entry 5 and keeps appending.
	forged := append([]Hash(nil), honest...)
	forged[5] = LeafHash([]byte("rewritten"))
	newRoot := Root(forged)
	proof, _ := ConsistencyProof(forged, 12)
	if VerifyConsistency(12, 20, oldRoot, newRoot, proof) {
		t.Fatal("a log that rewrote history passed the consistency check")
	}
}

func TestLeafAndNodeHashesAreDomainSeparated(t *testing.T) {
	a, b := LeafHash([]byte("a")), LeafHash([]byte("b"))
	node := NodeHash(a, b)
	asLeaf := LeafHash(append(a[:], b[:]...))
	if node == asLeaf {
		t.Fatal("an interior node collides with a leaf over the same bytes")
	}
}
