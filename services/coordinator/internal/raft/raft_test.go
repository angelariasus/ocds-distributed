package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// network routes RPCs between in-process nodes and can cut any node off,
// in both directions, to simulate crashes and partitions.
type network struct {
	mu    sync.Mutex
	nodes map[string]*Node
	cut   map[string]bool
}

var errUnreachable = errors.New("unreachable")

func (nw *network) reachable(a, b string) bool {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return !nw.cut[a] && !nw.cut[b] && nw.nodes[b] != nil
}

func (nw *network) node(id string) *Node {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.nodes[id]
}

type link struct {
	nw   *network
	from string
}

func (l link) RequestVote(_ context.Context, peer string, req VoteRequest) (VoteResponse, error) {
	if !l.nw.reachable(l.from, peer) {
		return VoteResponse{}, errUnreachable
	}
	return l.nw.node(peer).HandleRequestVote(req), nil
}

func (l link) AppendEntries(_ context.Context, peer string, req AppendRequest) (AppendResponse, error) {
	if !l.nw.reachable(l.from, peer) {
		return AppendResponse{}, errUnreachable
	}
	return l.nw.node(peer).HandleAppendEntries(req), nil
}

type cluster struct {
	t       *testing.T
	nw      *network
	ids     []string
	storage map[string]Storage
	applied map[string]*[]string
	mu      sync.Mutex
	cancel  map[string]context.CancelFunc
}

func newCluster(t *testing.T, size int) *cluster {
	c := &cluster{t: t, nw: &network{nodes: map[string]*Node{}, cut: map[string]bool{}}, storage: map[string]Storage{},
		applied: map[string]*[]string{}, cancel: map[string]context.CancelFunc{}}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("c%d", i)
		c.ids = append(c.ids, id)
		c.storage[id] = &MemoryStorage{}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, cancel := range c.cancel {
			cancel()
		}
	})
	return c
}

func (c *cluster) start(id string) {
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	applied := &[]string{}
	c.mu.Lock()
	c.applied[id] = applied
	c.mu.Unlock()
	n, err := New(Config{
		ID: id, Peers: peers,
		ElectionTimeoutMin: 60 * time.Millisecond, ElectionTimeoutMax: 120 * time.Millisecond,
		Heartbeat: 20 * time.Millisecond, RPCTimeout: 50 * time.Millisecond,
		Transport: link{nw: c.nw, from: id}, Storage: c.storage[id],
		Apply: func(e Entry) {
			c.mu.Lock()
			*applied = append(*applied, string(e.Command))
			c.mu.Unlock()
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.nw.mu.Lock()
	c.nw.nodes[id] = n
	c.nw.mu.Unlock()
	c.cancel[id] = cancel
	go n.Run(ctx)
}

func (c *cluster) stop(id string) {
	c.cancel[id]()
	c.nw.mu.Lock()
	delete(c.nw.nodes, id)
	c.nw.mu.Unlock()
}

func (c *cluster) disconnect(id string, cut bool) {
	c.nw.mu.Lock()
	c.nw.cut[id] = cut
	c.nw.mu.Unlock()
}

// leader waits until exactly one connected node claims leadership.
func (c *cluster) leader(among ...string) *Node {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		terms := map[uint64]int{}
		for _, id := range among {
			n := c.nw.node(id)
			if n == nil {
				continue
			}
			if ok, term := n.IsLeader(); ok {
				leaders = append(leaders, n)
				terms[term]++
			}
		}
		for term, count := range terms {
			if count > 1 {
				c.t.Fatalf("%d leaders in term %d", count, term)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v", among)
	return nil
}

func (c *cluster) appliedBy(id string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), *c.applied[id]...)
}

func (c *cluster) waitApplied(id string, want []string) {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fmt.Sprint(c.appliedBy(id)) == fmt.Sprint(want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("%s applied %v, want %v", id, c.appliedBy(id), want)
}

func propose(t *testing.T, n *Node, cmd string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	_, err := n.Propose(ctx, []byte(cmd))
	return err
}

func TestElectsOneLeader(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader()
	if l.Status().Term == 0 {
		t.Fatal("leader in term 0")
	}
}

func TestReplicatesAndAppliesInOrderOnEveryNode(t *testing.T) {
	c := newCluster(t, 5)
	l := c.leader()
	for _, cmd := range []string{"a", "b", "c"} {
		if err := propose(t, l, cmd); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.ids {
		c.waitApplied(id, []string{"a", "b", "c"})
	}
}

func TestFollowerRejectsProposals(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader()
	for _, id := range c.ids {
		if id == l.cfg.ID {
			continue
		}
		if err := propose(t, c.nw.node(id), "x"); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower accepted a proposal: %v", err)
		}
	}
}

func TestIsolatedLeaderCannotCommitAndIsReplaced(t *testing.T) {
	c := newCluster(t, 5)
	old := c.leader()
	propose(t, old, "before")
	oldID := old.cfg.ID
	oldTerm := old.Status().Term

	c.disconnect(oldID, true)
	if err := propose(t, old, "lost"); err == nil {
		t.Fatal("an isolated leader committed without a majority")
	}
	if ok, _ := old.IsLeader(); ok {
		t.Fatal("isolated leader did not step down after losing the majority")
	}

	var rest []string
	for _, id := range c.ids {
		if id != oldID {
			rest = append(rest, id)
		}
	}
	nl := c.leader(rest...)
	if nl.Status().Term <= oldTerm {
		t.Fatalf("new leader term %d not above %d", nl.Status().Term, oldTerm)
	}
	if err := propose(t, nl, "after"); err != nil {
		t.Fatal(err)
	}

	c.disconnect(oldID, false)
	// The old leader steps down, drops its uncommitted "lost" and converges.
	for _, id := range c.ids {
		c.waitApplied(id, []string{"before", "after"})
	}
	if ok, _ := old.IsLeader(); ok {
		t.Fatal("old leader still leads after rejoining")
	}
}

func TestMinorityCannotElectMajorityContinues(t *testing.T) {
	c := newCluster(t, 5)
	c.leader()
	// Partition {c1,c2} | {c3,c4,c5}.
	c.disconnect("c1", true)
	c.disconnect("c2", true)
	nl := c.leader("c3", "c4", "c5")
	if err := propose(t, nl, "majority"); err != nil {
		t.Fatal(err)
	}
	// Whoever led before the cut must have stepped down (CheckQuorum), and nobody
	// in the minority can win an election.
	time.Sleep(300 * time.Millisecond)
	for _, id := range []string{"c1", "c2"} {
		if ok, _ := c.nw.node(id).IsLeader(); ok {
			t.Fatalf("%s leads in the minority", id)
		}
	}
}

func TestRejoiningNodeDoesNotDisruptAHealthyLeader(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader()
	propose(t, l, "x")
	term := l.Status().Term
	var victim string
	for _, id := range c.ids {
		if id != l.cfg.ID {
			victim = id
			break
		}
	}
	// Cut off long enough for many election timeouts to fire on the victim.
	c.disconnect(victim, true)
	time.Sleep(800 * time.Millisecond)
	if got := c.nw.node(victim).Status().Term; got != term {
		t.Fatalf("isolated node inflated its term to %d (cluster at %d)", got, term)
	}
	c.disconnect(victim, false)
	time.Sleep(400 * time.Millisecond)
	if ok, now := l.IsLeader(); !ok || now != term {
		t.Fatalf("rejoin disrupted the leader: leader=%v term %d -> %d", ok, term, now)
	}
	c.waitApplied(victim, []string{"x"})
}

func TestRestartedNodeRecoversTermAndLogFromStorage(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader()
	propose(t, l, "x")
	propose(t, l, "y")
	var victim string
	for _, id := range c.ids {
		if id != l.cfg.ID {
			victim = id
			break
		}
	}
	c.waitApplied(victim, []string{"x", "y"})
	before := c.nw.node(victim).Status().Term
	c.stop(victim)
	c.start(victim)
	n := c.nw.node(victim)
	if n.Status().Term < before || len(n.Log()) < 3 {
		t.Fatalf("restart lost state: term %d (was %d), log %d entries", n.Status().Term, before, len(n.Log()))
	}
	c.waitApplied(victim, []string{"x", "y"})
}

func TestFileStorageRoundTrip(t *testing.T) {
	fs := FileStorage{Path: filepath.Join(t.TempDir(), "raft.json")}
	if st, err := fs.Load(); err != nil || st.CurrentTerm != 0 {
		t.Fatalf("empty load: %+v %v", st, err)
	}
	want := PersistentState{CurrentTerm: 7, VotedFor: "c2", Log: []Entry{{Index: 1, Term: 3, Command: []byte("a")}}}
	if err := fs.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := fs.Load()
	if err != nil || got.CurrentTerm != 7 || got.VotedFor != "c2" || string(got.Log[0].Command) != "a" {
		t.Fatalf("got %+v err %v", got, err)
	}
}
