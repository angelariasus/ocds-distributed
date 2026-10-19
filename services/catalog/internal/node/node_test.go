package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/angelariasus/ocds-distributed/services/catalog/internal/membership"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/store"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/vclock"
)

// fabric connects nodes in-process and lets tests take any of them offline.
type fabric struct {
	mu    sync.Mutex
	nodes map[string]*Node // by addr
	down  map[string]bool
}

func (f *fabric) target(addr string) (*Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[addr] {
		return nil, errors.New("connection refused")
	}
	return f.nodes[addr], nil
}

func (f *fabric) ReplicaGet(_ context.Context, addr, key string) ([]store.Version, error) {
	n, err := f.target(addr)
	if err != nil {
		return nil, err
	}
	return n.LocalGet(key), nil
}

func (f *fabric) ReplicaPut(_ context.Context, addr, key string, v store.Version, hintFor string) error {
	n, err := f.target(addr)
	if err != nil {
		return err
	}
	return n.LocalPut(key, v, hintFor)
}

func (f *fabric) TreeLevel(_ context.Context, addr, peer string, depth int) ([][]byte, error) {
	n, err := f.target(addr)
	if err != nil {
		return nil, err
	}
	return n.Tree(peer).Level(depth), nil
}

func (f *fabric) BucketKeys(_ context.Context, addr, peer string, bucket uint32) (map[string][]store.Version, error) {
	n, err := f.target(addr)
	if err != nil {
		return nil, err
	}
	return n.Bucket(peer, bucket), nil
}

func (f *fabric) setDown(addr string, down bool) {
	f.mu.Lock()
	f.down[addr] = down
	f.mu.Unlock()
}

func cluster(t *testing.T, size int, cfg Config) (*fabric, []*Node) {
	t.Helper()
	f := &fabric{nodes: map[string]*Node{}, down: map[string]bool{}}
	seeds := map[string]string{}
	for i := 0; i < size; i++ {
		seeds[fmt.Sprintf("c%d", i)] = fmt.Sprintf("addr%d", i)
	}
	nodes := make([]*Node, size)
	for i := range nodes {
		id := fmt.Sprintf("c%d", i)
		c := cfg
		c.ID, c.Addr = id, seeds[id]
		st, _ := store.Open("")
		members := membership.New(membership.Member{ID: id, Addr: seeds[id]}, seeds, nil, membership.DefaultConfig(), time.Now, nil)
		nodes[i] = New(c, st, members, f)
		f.nodes[seeds[id]] = nodes[i]
	}
	return f, nodes
}

func base() Config {
	return Config{N: 3, R: 2, W: 2, Vnodes: 32, RPCTimeout: time.Second, HintedHandoff: true}
}

func replicasOf(n *Node, key string) []string { return n.Ring().Preference(key, n.Config().N) }

func addrOf(id string) string { return "addr" + id[1:] }

func TestWriteIsReadableThroughAnyCoordinator(t *testing.T) {
	_, nodes := cluster(t, 5, base())
	ctx := context.Background()
	clock, acked, hinted, err := nodes[0].Put(ctx, "crawl/t1", []byte("page-3"), nil, 0, false)
	if err != nil || acked < 2 || hinted != 0 {
		t.Fatalf("put: clock=%v acked=%d hinted=%d err=%v", clock, acked, hinted, err)
	}
	for _, n := range nodes {
		versions, _, _, err := n.Get(ctx, "crawl/t1", 0)
		if err != nil || len(versions) != 1 || string(versions[0].Value) != "page-3" {
			t.Fatalf("%s get: %+v err=%v", n.Config().ID, versions, err)
		}
	}
}

func TestSloppyQuorumHintsForUnreachableReplica(t *testing.T) {
	f, nodes := cluster(t, 5, base())
	ctx := context.Background()
	key := "crawl/t2"
	replicas := replicasOf(nodes[0], key)
	f.setDown(addrOf(replicas[2]), true)

	_, acked, hinted, err := nodes[0].Put(ctx, key, []byte("v"), nil, 3, false)
	if err != nil || acked != 3 || hinted != 1 {
		t.Fatalf("acked=%d hinted=%d err=%v", acked, hinted, err)
	}

	downNode := f.nodes[addrOf(replicas[2])]
	if len(downNode.LocalGet(key)) != 0 {
		t.Fatal("offline replica should not have the write yet")
	}
	f.setDown(addrOf(replicas[2]), false)
	for _, n := range nodes {
		n.HandoffRound(ctx)
	}
	if got := downNode.LocalGet(key); len(got) != 1 || string(got[0].Value) != "v" {
		t.Fatalf("hint not delivered: %+v", got)
	}
	for _, n := range nodes {
		if n.PendingHints() != 0 {
			t.Fatalf("%s still holds hints", n.Config().ID)
		}
	}
}

func TestStrictQuorumFailsWhenReplicasAreDown(t *testing.T) {
	cfg := base()
	cfg.HintedHandoff = false
	f, nodes := cluster(t, 5, cfg)
	key := "crawl/t3"
	replicas := replicasOf(nodes[0], key)
	coordinator := nodes[0]
	for _, id := range replicas {
		if id == coordinator.Config().ID {
			continue
		}
		f.setDown(addrOf(id), true)
	}
	_, _, _, err := coordinator.Put(context.Background(), key, []byte("v"), nil, 2, false)
	if !errors.Is(err, ErrQuorum) {
		t.Fatalf("err = %v, want quorum failure", err)
	}
}

func TestConcurrentWritesSurfaceAsSiblingsUntilResolved(t *testing.T) {
	_, nodes := cluster(t, 5, base())
	ctx := context.Background()
	key := "crawl/t4"
	nodes[0].Put(ctx, key, []byte("base"), nil, 3, false)
	_, context0, _, _ := nodes[0].Get(ctx, key, 3)

	// Two coordinators write from the same causal context: neither descends from the other.
	nodes[1].Put(ctx, key, []byte("from-1"), context0, 3, false)
	nodes[2].Put(ctx, key, []byte("from-2"), context0, 3, false)

	versions, merged, _, _ := nodes[3].Get(ctx, key, 3)
	if len(versions) != 2 {
		t.Fatalf("want 2 siblings, got %+v", versions)
	}
	if _, _, _, err := nodes[4].Put(ctx, key, []byte("resolved"), merged, 3, false); err != nil {
		t.Fatal(err)
	}
	versions, _, _, _ = nodes[0].Get(ctx, key, 3)
	if len(versions) != 1 || string(versions[0].Value) != "resolved" {
		t.Fatalf("got %+v", versions)
	}
}

func TestReadRepairFixesStaleReplica(t *testing.T) {
	f, nodes := cluster(t, 5, base())
	ctx := context.Background()
	key := "crawl/t5"
	replicas := replicasOf(nodes[0], key)
	stale := f.nodes[addrOf(replicas[2])]
	v := store.Version{Value: []byte("fresh"), Clock: vclock.Clock{"x": 1}, UpdatedAt: time.Now()}
	f.nodes[addrOf(replicas[0])].LocalPut(key, v, "")
	f.nodes[addrOf(replicas[1])].LocalPut(key, v, "")

	versions, _, _, err := nodes[0].Get(ctx, key, 3)
	if err != nil || len(versions) != 1 {
		t.Fatalf("get: %+v err=%v", versions, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(stale.LocalGet(key)) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := stale.LocalGet(key); len(got) != 1 || string(got[0].Value) != "fresh" {
		t.Fatalf("stale replica not repaired: %+v", got)
	}
}

func TestAntiEntropyReconcilesMissedWrites(t *testing.T) {
	f, nodes := cluster(t, 5, base())
	ctx := context.Background()
	// Write 200 keys while one node is offline (hints disabled to force divergence).
	cfg := base()
	cfg.HintedHandoff = false
	for _, n := range nodes {
		n.cfg.HintedHandoff = false
	}
	f.setDown("addr4", true)
	for i := 0; i < 200; i++ {
		nodes[0].Put(ctx, fmt.Sprintf("crawl/k%d", i), []byte("v"), nil, 2, false)
	}
	f.setDown("addr4", false)

	missing := 0
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("crawl/k%d", i)
		for _, id := range replicasOf(nodes[0], key) {
			if id == "c4" && len(nodes[4].LocalGet(key)) == 0 {
				missing++
			}
		}
	}
	if missing == 0 {
		t.Skip("c4 replicated nothing in this ring layout")
	}

	total := 0
	for round := 0; round < 40 && total < missing; round++ {
		repaired, err := nodes[4].AntiEntropyRound(ctx)
		if err != nil {
			t.Fatal(err)
		}
		total += repaired
	}
	stillMissing := 0
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("crawl/k%d", i)
		for _, id := range replicasOf(nodes[0], key) {
			if id == "c4" && len(nodes[4].LocalGet(key)) == 0 {
				stillMissing++
			}
		}
	}
	if stillMissing != 0 {
		t.Fatalf("%d of %d keys still missing after anti-entropy", stillMissing, missing)
	}
}

func TestDeleteLeavesTombstoneHiddenFromReads(t *testing.T) {
	_, nodes := cluster(t, 3, base())
	ctx := context.Background()
	nodes[0].Put(ctx, "k", []byte("v"), nil, 0, false)
	_, c, _, _ := nodes[0].Get(ctx, "k", 0)
	if _, _, _, err := nodes[1].Put(ctx, "k", nil, c, 0, true); err != nil {
		t.Fatal(err)
	}
	versions, _, _, _ := nodes[2].Get(ctx, "k", 0)
	if len(versions) != 0 {
		t.Fatalf("deleted key still visible: %+v", versions)
	}
}
