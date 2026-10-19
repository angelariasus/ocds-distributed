// Package node is the Dynamo-style coordinator: it routes every key to its N
// replicas on the ring, runs sloppy quorums with hinted handoff, repairs stale
// replicas on read and reconciles them in the background with Merkle trees.
package node

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/angelariasus/ocds-distributed/services/catalog/internal/membership"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/merkle"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/ring"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/store"
	"github.com/angelariasus/ocds-distributed/services/catalog/internal/vclock"
)

var ErrQuorum = errors.New("quorum not reached")

var (
	opsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "catalog_ops_total", Help: "Coordinated operations by kind and outcome.",
	}, []string{"op", "outcome"})
	opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "catalog_op_seconds", Help: "Coordinated operation latency.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	}, []string{"op"})
	readRepairs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "catalog_read_repairs_total", Help: "Replicas repaired after a divergent read.",
	})
	hintsStored = promauto.NewCounter(prometheus.CounterOpts{
		Name: "catalog_hints_stored_total", Help: "Writes accepted on behalf of an unreachable replica.",
	})
	hintsDelivered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "catalog_hints_delivered_total", Help: "Hinted writes handed back to their replica.",
	})
	hintsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "catalog_hints_pending", Help: "Hinted writes waiting for their replica.",
	})
	antiEntropyKeys = promauto.NewCounter(prometheus.CounterOpts{
		Name: "catalog_anti_entropy_keys_total", Help: "Keys reconciled by Merkle-tree synchronisation.",
	})
	antiEntropyRounds = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "catalog_anti_entropy_rounds_total", Help: "Anti-entropy rounds by outcome.",
	}, []string{"outcome"})
	siblingsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "catalog_conflicting_keys", Help: "Keys currently holding concurrent versions.",
	})
	keysGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "catalog_keys", Help: "Keys held by this replica.",
	})
)

// Peer is the node-to-node transport; the real one speaks gRPC.
type Peer interface {
	ReplicaGet(ctx context.Context, addr, key string) ([]store.Version, error)
	ReplicaPut(ctx context.Context, addr, key string, v store.Version, hintFor string) error
	TreeLevel(ctx context.Context, addr, peer string, depth int) ([][]byte, error)
	BucketKeys(ctx context.Context, addr, peer string, bucket uint32) (map[string][]store.Version, error)
}

type Config struct {
	ID            string
	Addr          string
	N, R, W       int
	Vnodes        int
	RPCTimeout    time.Duration
	HintedHandoff bool
}

type hint struct {
	key     string
	version store.Version
}

type Node struct {
	cfg     Config
	store   *store.Store
	ring    *ring.Ring
	members *membership.List
	peer    Peer
	rand    *rand.Rand

	hintMu sync.Mutex
	hints  map[string][]hint // target node id → pending writes
}

// New builds the node and keeps the ring in sync with the membership list:
// alive and suspect nodes stay on the ring (a sloppy quorum tolerates them
// being briefly unreachable); dead nodes leave it.
func New(cfg Config, st *store.Store, members *membership.List, peer Peer) *Node {
	n := &Node{
		cfg:     cfg,
		store:   st,
		ring:    ring.New(cfg.Vnodes),
		members: members,
		peer:    peer,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		hints:   map[string][]hint{},
	}
	for _, m := range members.View() {
		n.onMember(m)
	}
	return n
}

// OnMember is the membership change hook.
func (n *Node) OnMember(m membership.Member) { n.onMember(m) }

func (n *Node) onMember(m membership.Member) {
	if m.State == membership.Dead {
		n.ring.Remove(m.ID)
	} else {
		n.ring.Add(m.ID)
	}
}

func (n *Node) Ring() *ring.Ring { return n.ring }

func (n *Node) Config() Config { return n.cfg }

func (n *Node) addrOf(id string) (string, bool) {
	if id == n.cfg.ID {
		return n.cfg.Addr, true
	}
	for _, m := range n.members.View() {
		if m.ID == id {
			return m.Addr, true
		}
	}
	return "", false
}

type replicaResult struct {
	id       string
	versions []store.Version
	err      error
}

// Get reads from the key's replicas until r of them answer, merges their
// versions and repairs the ones that returned stale data.
func (n *Node) Get(ctx context.Context, key string, r int) ([]store.Version, vclock.Clock, int, error) {
	timer := prometheus.NewTimer(opLatency.WithLabelValues("get"))
	defer timer.ObserveDuration()
	if r <= 0 {
		r = n.cfg.R
	}
	pref := n.ring.Preference(key, n.cfg.N)
	results := make(chan replicaResult, len(pref))
	for _, id := range pref {
		go func(id string) {
			versions, err := n.replicaGet(ctx, id, key)
			results <- replicaResult{id: id, versions: versions, err: err}
		}(id)
	}

	var answered []replicaResult
	for range pref {
		res := <-results
		if res.err == nil {
			answered = append(answered, res)
		}
		if len(answered) >= r {
			break
		}
	}
	if len(answered) < r {
		opsTotal.WithLabelValues("get", "quorum_failed").Inc()
		return nil, nil, len(answered), fmt.Errorf("%w: %d of %d replicas answered, need %d", ErrQuorum, len(answered), len(pref), r)
	}

	merged := mergeVersions(answered)
	context := vclock.Clock{}
	for _, v := range merged {
		context = vclock.Merge(context, v.Clock)
	}
	go n.readRepair(key, answered, merged)

	var live []store.Version
	for _, v := range merged {
		if !v.Tombstone {
			live = append(live, v)
		}
	}
	opsTotal.WithLabelValues("get", "ok").Inc()
	return live, context, len(answered), nil
}

func mergeVersions(results []replicaResult) []store.Version {
	tmp, _ := store.Open("")
	for _, res := range results {
		for _, v := range res.versions {
			tmp.Put("k", v)
		}
	}
	return tmp.Get("k")
}

func (n *Node) readRepair(key string, answered []replicaResult, merged []store.Version) {
	for _, res := range answered {
		if sameVersions(res.versions, merged) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		for _, v := range merged {
			n.replicaPut(ctx, res.id, key, v, "")
		}
		cancel()
		readRepairs.Inc()
	}
}

func sameVersions(a, b []store.Version) bool {
	if len(a) != len(b) {
		return false
	}
	for _, va := range a {
		found := false
		for _, vb := range b {
			if vclock.Compare(va.Clock, vb.Clock) == vclock.Equal {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Put stamps the write with this node's clock entry and sends it to the N
// replicas. Unreachable replicas are covered by the next nodes on the ring,
// which keep the write as a hint until the replica is back.
func (n *Node) Put(ctx context.Context, key string, value []byte, context vclock.Clock, w int, tombstone bool) (vclock.Clock, int, int, error) {
	op := "put"
	if tombstone {
		op = "delete"
	}
	timer := prometheus.NewTimer(opLatency.WithLabelValues(op))
	defer timer.ObserveDuration()
	if w <= 0 {
		w = n.cfg.W
	}
	clock := context.Increment(n.cfg.ID)
	version := store.Version{Value: value, Clock: clock, Tombstone: tombstone, UpdatedAt: time.Now().UTC()}

	pref := n.ring.Preference(key, n.cfg.N)
	standIns := n.ring.Preference(key, n.ring.Len())[len(pref):]

	type ack struct {
		hinted bool
		err    error
	}
	acks := make(chan ack, len(pref))
	var standInMu sync.Mutex
	nextStandIn := 0

	for _, id := range pref {
		go func(id string) {
			if err := n.replicaPut(ctx, id, key, version, ""); err == nil {
				acks <- ack{}
				return
			}
			if !n.cfg.HintedHandoff {
				acks <- ack{err: fmt.Errorf("replica %s unreachable", id)}
				return
			}
			standInMu.Lock()
			for nextStandIn < len(standIns) {
				candidate := standIns[nextStandIn]
				nextStandIn++
				if err := n.replicaPut(ctx, candidate, key, version, id); err == nil {
					standInMu.Unlock()
					acks <- ack{hinted: true}
					return
				}
			}
			standInMu.Unlock()
			acks <- ack{err: fmt.Errorf("replica %s unreachable and no stand-in left", id)}
		}(id)
	}

	acked, hinted := 0, 0
	for range pref {
		a := <-acks
		if a.err != nil {
			continue
		}
		acked++
		if a.hinted {
			hinted++
		}
		if acked >= w {
			break
		}
	}
	if acked < w {
		opsTotal.WithLabelValues(op, "quorum_failed").Inc()
		return nil, acked, hinted, fmt.Errorf("%w: %d of %d replicas acked, need %d", ErrQuorum, acked, len(pref), w)
	}
	opsTotal.WithLabelValues(op, "ok").Inc()
	return clock, acked, hinted, nil
}

func (n *Node) replicaGet(ctx context.Context, id, key string) ([]store.Version, error) {
	if id == n.cfg.ID {
		return n.store.Get(key), nil
	}
	addr, ok := n.addrOf(id)
	if !ok {
		return nil, fmt.Errorf("unknown node %s", id)
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	return n.peer.ReplicaGet(ctx, addr, key)
}

func (n *Node) replicaPut(ctx context.Context, id, key string, v store.Version, hintFor string) error {
	if id == n.cfg.ID {
		return n.LocalPut(key, v, hintFor)
	}
	addr, ok := n.addrOf(id)
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	return n.peer.ReplicaPut(ctx, addr, key, v, hintFor)
}

// LocalGet serves ReplicaGet for other coordinators.
func (n *Node) LocalGet(key string) []store.Version { return n.store.Get(key) }

// LocalPut serves ReplicaPut: a plain replica write, or a hint kept for an
// unreachable replica.
func (n *Node) LocalPut(key string, v store.Version, hintFor string) error {
	if hintFor != "" && hintFor != n.cfg.ID {
		n.hintMu.Lock()
		n.hints[hintFor] = append(n.hints[hintFor], hint{key: key, version: v})
		n.hintMu.Unlock()
		hintsStored.Inc()
		n.updateGauges()
		return nil
	}
	_, err := n.store.Put(key, v)
	n.updateGauges()
	return err
}

// HandoffRound tries to deliver pending hints to replicas that are alive again.
func (n *Node) HandoffRound(ctx context.Context) {
	n.hintMu.Lock()
	targets := make([]string, 0, len(n.hints))
	for id := range n.hints {
		targets = append(targets, id)
	}
	n.hintMu.Unlock()

	for _, id := range targets {
		n.hintMu.Lock()
		pending := n.hints[id]
		n.hintMu.Unlock()
		var remaining []hint
		for _, h := range pending {
			if err := n.replicaPut(ctx, id, h.key, h.version, ""); err != nil {
				remaining = append(remaining, h)
				continue
			}
			hintsDelivered.Inc()
		}
		n.hintMu.Lock()
		if len(remaining) == 0 {
			delete(n.hints, id)
		} else {
			n.hints[id] = remaining
		}
		n.hintMu.Unlock()
	}
	n.updateGauges()
}

func (n *Node) PendingHints() int {
	n.hintMu.Lock()
	defer n.hintMu.Unlock()
	total := 0
	for _, hs := range n.hints {
		total += len(hs)
	}
	return total
}

// sharedWith reports whether both this node and peer replicate key.
func (n *Node) sharedWith(peer, key string) bool {
	self, other := false, false
	for _, id := range n.ring.Preference(key, n.cfg.N) {
		self = self || id == n.cfg.ID
		other = other || id == peer
	}
	return self && other
}

// Tree builds the Merkle tree over the keys shared with peer.
func (n *Node) Tree(peer string) *merkle.Tree {
	return merkle.Build(func(fn func(string, []store.Version)) {
		n.store.ForEach(func(key string, versions []store.Version) {
			if n.sharedWith(peer, key) {
				fn(key, versions)
			}
		})
	})
}

// Bucket returns the shared keys that fall into a Merkle bucket.
func (n *Node) Bucket(peer string, bucket uint32) map[string][]store.Version {
	out := map[string][]store.Version{}
	n.store.ForEach(func(key string, versions []store.Version) {
		if merkle.BucketOf(key) == bucket && n.sharedWith(peer, key) {
			out[key] = append([]store.Version(nil), versions...)
		}
	})
	return out
}

// AntiEntropyRound compares the shared key range with one random alive peer
// and exchanges whatever differs in both directions.
func (n *Node) AntiEntropyRound(ctx context.Context) (int, error) {
	var peers []membership.Member
	for _, m := range n.members.Alive() {
		if m.ID != n.cfg.ID {
			peers = append(peers, m)
		}
	}
	if len(peers) == 0 {
		return 0, nil
	}
	peer := peers[n.rand.Intn(len(peers))]
	repaired, err := n.syncWith(ctx, peer)
	if err != nil {
		antiEntropyRounds.WithLabelValues("error").Inc()
		return repaired, err
	}
	antiEntropyRounds.WithLabelValues("ok").Inc()
	return repaired, nil
}

func (n *Node) syncWith(ctx context.Context, peer membership.Member) (int, error) {
	local := n.Tree(peer.ID)
	buckets, err := merkle.Diff(local, func(depth int) ([][]byte, error) {
		ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
		defer cancel()
		return n.peer.TreeLevel(ctx, peer.Addr, n.cfg.ID, depth)
	})
	if err != nil {
		return 0, err
	}
	repaired := 0
	for _, b := range buckets {
		rctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
		remote, err := n.peer.BucketKeys(rctx, peer.Addr, n.cfg.ID, b)
		cancel()
		if err != nil {
			return repaired, err
		}
		mine := n.Bucket(peer.ID, b)
		for key, versions := range remote {
			for _, v := range versions {
				if changed, _ := n.store.Put(key, v); changed {
					repaired++
				}
			}
		}
		for key, versions := range mine {
			for _, v := range versions {
				if dominated(v, remote[key]) {
					continue
				}
				if err := n.replicaPut(ctx, peer.ID, key, v, ""); err == nil {
					repaired++
				}
			}
		}
	}
	antiEntropyKeys.Add(float64(repaired))
	n.updateGauges()
	return repaired, nil
}

func dominated(v store.Version, others []store.Version) bool {
	for _, o := range others {
		if vclock.Descends(o.Clock, v.Clock) {
			return true
		}
	}
	return false
}

func (n *Node) updateGauges() {
	keysGauge.Set(float64(n.store.Len()))
	hintsPending.Set(float64(n.PendingHints()))
	conflicts := 0
	n.store.ForEach(func(_ string, versions []store.Version) {
		if len(versions) > 1 {
			conflicts++
		}
	})
	siblingsGauge.Set(float64(conflicts))
}

func (n *Node) KeyCount() int { return n.store.Len() }
