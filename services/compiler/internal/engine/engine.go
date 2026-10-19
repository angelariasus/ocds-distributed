// Package engine keeps the compiled state of every OCID owned by this
// instance, partition by partition, the way a Kafka Streams state store does:
// input and output topics are co-partitioned by OCID, so partition p of the
// compacted output is both the changelog and the restore source for partition
// p of the input.
package engine

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/angelariasus/ocds-distributed/services/compiler/internal/crdt"
	"github.com/angelariasus/ocds-distributed/services/compiler/internal/lamport"
)

var (
	events = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "compiler_events_total", Help: "Observations by outcome.",
	}, []string{"outcome"})
	keysGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "compiler_state_keys", Help: "OCIDs held in memory by this instance.",
	})
	partitionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "compiler_partitions", Help: "Input partitions currently owned.",
	})
	restored = promauto.NewCounter(prometheus.CounterOpts{
		Name: "compiler_restored_keys_total", Help: "Keys rebuilt from the changelog after a rebalance.",
	})
	lamportGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "compiler_lamport_time", Help: "This instance's Lamport clock.",
	})
)

type Outcome string

const (
	Merged    Outcome = "merged"    // state changed and was republished
	Duplicate Outcome = "duplicate" // nothing new: redelivery or identical sighting
	Invalid   Outcome = "invalid"
)

type Engine struct {
	clock lamport.Clock

	mu         sync.Mutex
	partitions map[int32]map[string]*crdt.State
}

func New() *Engine {
	return &Engine{partitions: map[int32]map[string]*crdt.State{}}
}

// Assign starts tracking a partition; Restore fills it from the changelog.
func (e *Engine) Assign(p int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.partitions[p]; !ok {
		e.partitions[p] = map[string]*crdt.State{}
	}
	partitionsGauge.Set(float64(len(e.partitions)))
}

// Revoke drops a partition's state: another instance owns it now and will
// rebuild it from the same changelog.
func (e *Engine) Revoke(p int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.partitions, p)
	partitionsGauge.Set(float64(len(e.partitions)))
	keysGauge.Set(float64(e.keyCountLocked()))
}

// Restore loads one changelog entry. Entries are the states this or a
// previous owner published, so they are taken as-is (merging would be
// equivalent, the changelog is a prefix of what we would compute).
func (e *Engine) Restore(p int32, raw []byte) error {
	var s crdt.State
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	states, ok := e.partitions[p]
	if !ok {
		return fmt.Errorf("partition %d not assigned", p)
	}
	if cur, exists := states[s.OCID]; exists {
		cur.Merge(&s)
	} else {
		states[s.OCID] = &s
	}
	e.clock.Witness(s.Lamport)
	restored.Inc()
	keysGauge.Set(float64(e.keyCountLocked()))
	lamportGauge.Set(float64(e.clock.Now()))
	return nil
}

// Apply merges one observation into its OCID's state. The returned state is
// stamped with the receive event's Lamport time when it changed.
func (e *Engine) Apply(p int32, raw []byte) (*crdt.State, Outcome, error) {
	var o crdt.Observation
	if err := json.Unmarshal(raw, &o); err != nil || o.OCID == "" {
		events.WithLabelValues(string(Invalid)).Inc()
		return nil, Invalid, fmt.Errorf("invalid observation: %v", err)
	}
	incoming, err := crdt.FromObservation(o)
	if err != nil {
		events.WithLabelValues(string(Invalid)).Inc()
		return nil, Invalid, err
	}
	now := e.clock.Witness(o.Lamport)
	lamportGauge.Set(float64(now))

	e.mu.Lock()
	defer e.mu.Unlock()
	states, ok := e.partitions[p]
	if !ok {
		return nil, Invalid, fmt.Errorf("partition %d not assigned", p)
	}
	cur, exists := states[o.OCID]
	if !exists {
		incoming.Lamport = now
		states[o.OCID] = incoming
		keysGauge.Set(float64(e.keyCountLocked()))
		events.WithLabelValues(string(Merged)).Inc()
		return incoming, Merged, nil
	}
	if !cur.Merge(incoming) {
		events.WithLabelValues(string(Duplicate)).Inc()
		return cur, Duplicate, nil
	}
	cur.Lamport = now
	events.WithLabelValues(string(Merged)).Inc()
	return cur, Merged, nil
}

func (e *Engine) Get(p int32, ocid string) (*crdt.State, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.partitions[p][ocid]
	return s, ok
}

func (e *Engine) KeyCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.keyCountLocked()
}

func (e *Engine) keyCountLocked() int {
	n := 0
	for _, states := range e.partitions {
		n += len(states)
	}
	return n
}

func (e *Engine) Lamport() uint64 { return e.clock.Now() }
