package membership

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// cluster wires Lists together through an in-memory transport where any node
// can be switched off.
type cluster struct {
	mu    sync.Mutex
	nodes map[string]*List // keyed by addr
	down  map[string]bool
	clock time.Time
}

func (c *cluster) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clock
}

func (c *cluster) advance(d time.Duration) {
	c.mu.Lock()
	c.clock = c.clock.Add(d)
	c.mu.Unlock()
}

func (c *cluster) Ping(_ context.Context, addr string, from Member, view []Member) ([]Member, error) {
	c.mu.Lock()
	target, ok := c.nodes[addr]
	down := c.down[addr] || c.down[from.Addr]
	c.mu.Unlock()
	if !ok || down {
		return nil, errors.New("unreachable")
	}
	target.Merge(view)
	return target.View(), nil
}

func (c *cluster) PingReq(ctx context.Context, via string, target Member) (bool, error) {
	c.mu.Lock()
	viaDown := c.down[via]
	c.mu.Unlock()
	if viaDown {
		return false, errors.New("unreachable")
	}
	_, err := c.Ping(ctx, target.Addr, Member{}, nil)
	return err == nil, nil
}

// perNode is one node's view of the network: when the node itself is cut off,
// nothing it sends gets through, including indirect probes.
type perNode struct {
	*cluster
	self string
}

func (p perNode) isolated() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.down[p.self]
}

func (p perNode) Ping(ctx context.Context, addr string, from Member, view []Member) ([]Member, error) {
	if p.isolated() {
		return nil, errors.New("unreachable")
	}
	return p.cluster.Ping(ctx, addr, from, view)
}

func (p perNode) PingReq(ctx context.Context, via string, target Member) (bool, error) {
	if p.isolated() {
		return false, errors.New("unreachable")
	}
	return p.cluster.PingReq(ctx, via, target)
}

func newCluster(n int) (*cluster, []*List) {
	c := &cluster{nodes: map[string]*List{}, down: map[string]bool{}, clock: time.Unix(1_700_000_000, 0)}
	cfg := DefaultConfig()
	seeds := map[string]string{"n0": "addr0"}
	lists := make([]*List, n)
	for i := range lists {
		id, addr := "n"+string(rune('0'+i)), "addr"+string(rune('0'+i))
		lists[i] = New(Member{ID: id, Addr: addr}, seeds, perNode{c, addr}, cfg, c.now, nil)
		c.nodes[addr] = lists[i]
	}
	return c, lists
}

func rounds(c *cluster, lists []*List, n int) {
	for i := 0; i < n; i++ {
		for _, l := range lists {
			l.Round(context.Background())
		}
		c.advance(time.Second)
	}
}

func TestMembershipConvergesFromASingleSeed(t *testing.T) {
	c, lists := newCluster(5)
	rounds(c, lists, 10)
	for _, l := range lists {
		if got := len(l.Alive()); got != 5 {
			t.Fatalf("%s sees %d alive members, want 5", l.Self().ID, got)
		}
	}
}

func TestUnreachableNodeBecomesSuspectThenDead(t *testing.T) {
	c, lists := newCluster(4)
	rounds(c, lists, 10)
	c.mu.Lock()
	c.down["addr3"] = true
	c.mu.Unlock()
	rounds(c, lists[:3], 6)

	state := func(l *List) State {
		for _, m := range l.View() {
			if m.ID == "n3" {
				return m.State
			}
		}
		return -1
	}
	if s := state(lists[0]); s != Suspect {
		t.Fatalf("n3 is %v after failed probes, want suspect", s)
	}
	c.advance(DefaultConfig().SuspectTimeout)
	rounds(c, lists[:3], 2)
	for _, l := range lists[:3] {
		if s := state(l); s != Dead {
			t.Fatalf("%s sees n3 as %v, want dead", l.Self().ID, s)
		}
	}
}

func TestSuspectedNodeRefutesByBumpingIncarnation(t *testing.T) {
	c, lists := newCluster(3)
	rounds(c, lists, 6)
	rumour := lists[1].Self()
	rumour.State = Suspect
	lists[1].Merge([]Member{rumour})
	if lists[1].Self().Incarnation != rumour.Incarnation+1 {
		t.Fatalf("incarnation = %d", lists[1].Self().Incarnation)
	}
	rounds(c, lists, 6)
	for _, m := range lists[0].View() {
		if m.ID == "n1" && (m.State != Alive || m.Incarnation != rumour.Incarnation+1) {
			t.Fatalf("n0 sees n1 as %+v", m)
		}
	}
}

func TestIndirectProbeKeepsMemberAliveWhenDirectPathFails(t *testing.T) {
	c, lists := newCluster(3)
	rounds(c, lists, 6)
	// Only the direct ping from n0 to n2 fails; n1 can still reach n2.
	direct := &partitionedTransport{cluster: c, blocked: map[string]bool{"addr2": true}}
	n0 := New(Member{ID: "n0", Addr: "addr0"}, nil, direct, DefaultConfig(), c.now, nil)
	n0.Merge(lists[1].View())
	for i := 0; i < 20; i++ {
		n0.Round(context.Background())
		c.advance(time.Second)
	}
	for _, m := range n0.View() {
		if m.ID == "n2" && m.State != Alive {
			t.Fatalf("n2 is %v despite indirect reachability", m.State)
		}
	}
}

type partitionedTransport struct {
	*cluster
	blocked map[string]bool
}

func (p *partitionedTransport) Ping(ctx context.Context, addr string, from Member, view []Member) ([]Member, error) {
	if p.blocked[addr] {
		return nil, errors.New("partitioned")
	}
	return p.cluster.Ping(ctx, addr, from, view)
}

func TestRestartedNodeRejoinsWithHigherIncarnation(t *testing.T) {
	c, lists := newCluster(3)
	rounds(c, lists, 6)
	c.mu.Lock()
	c.down["addr2"] = true
	c.mu.Unlock()
	rounds(c, lists[:2], 6)
	c.advance(DefaultConfig().SuspectTimeout)
	rounds(c, lists[:2], 2)

	// n2 comes back as a fresh process: zero heartbeat, but a wall-clock incarnation.
	reborn := New(Member{ID: "n2", Addr: "addr2", Incarnation: 1_700_000_000}, map[string]string{"n0": "addr0"}, c, DefaultConfig(), c.now, nil)
	c.mu.Lock()
	c.nodes["addr2"] = reborn
	c.down["addr2"] = false
	c.mu.Unlock()
	rounds(c, append(lists[:2], reborn), 6)

	for _, l := range lists[:2] {
		for _, m := range l.View() {
			if m.ID == "n2" && m.State != Alive {
				t.Fatalf("%s still sees n2 as %v", l.Self().ID, m.State)
			}
		}
	}
}

func TestRestartedNodeWithStaleIncarnationStaysDead(t *testing.T) {
	c, lists := newCluster(3)
	rounds(c, lists, 6)
	c.mu.Lock()
	c.down["addr2"] = true
	c.mu.Unlock()
	rounds(c, lists[:2], 6)
	c.advance(DefaultConfig().SuspectTimeout)
	rounds(c, lists[:2], 2)

	reborn := New(Member{ID: "n2", Addr: "addr2"}, map[string]string{"n0": "addr0"}, c, DefaultConfig(), c.now, nil)
	c.mu.Lock()
	c.nodes["addr2"] = reborn
	c.down["addr2"] = false
	c.mu.Unlock()
	rounds(c, append(lists[:2], reborn), 3)

	// Peers tell n2 it is dead at its own incarnation, so it refutes by bumping it...
	if reborn.Self().Incarnation == 0 {
		t.Fatal("n2 should have refuted the dead rumour")
	}
	rounds(c, append(lists[:2], reborn), 6)
	// ...and the refutation eventually wins.
	for _, m := range lists[0].View() {
		if m.ID == "n2" && m.State != Alive {
			t.Fatalf("n0 sees n2 as %v after refutation", m.State)
		}
	}
}

func TestHealedPartitionMergesAfterBothSidesForgotEachOther(t *testing.T) {
	c, lists := newCluster(4)
	rounds(c, lists, 8)
	// n3 is cut off in both directions while every node keeps running rounds.
	c.mu.Lock()
	c.down["addr3"] = true
	c.mu.Unlock()
	cfg := DefaultConfig()
	for elapsed := time.Duration(0); elapsed < cfg.SuspectTimeout+cfg.DeadRetention+10*time.Second; elapsed += time.Second {
		rounds(c, lists, 1)
	}
	if got := len(lists[3].View()); got != 1 {
		t.Fatalf("isolated n3 still knows %d members; the test needs it to have forgotten the others", got)
	}
	for _, m := range lists[0].View() {
		if m.ID == "n3" {
			t.Fatalf("n0 still lists n3 as %v; the test needs it forgotten", m.State)
		}
	}

	c.mu.Lock()
	c.down["addr3"] = false
	c.mu.Unlock()
	rounds(c, lists, 3*cfg.ReconnectEvery)
	for _, l := range lists {
		if got := len(l.Alive()); got != 4 {
			t.Fatalf("%s sees %d alive members after the partition healed, want 4", l.Self().ID, got)
		}
	}
}
