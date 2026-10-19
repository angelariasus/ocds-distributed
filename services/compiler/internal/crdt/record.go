// Package crdt defines the per-OCID record state as a state-based CRDT: a join
// of grow-only sets, max-registers and a last-writer-wins register. Merge is
// commutative, associative and idempotent, so observations of the same
// procurement process can arrive from any source, any worker and in any order
// and every replica converges to the same state.
//
// Sightings are a G-Set rather than a G-Counter on purpose: a counter cannot
// tell a redelivered duplicate from a new observation, a set of (origin,
// worker) pairs can.
package crdt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// Observation is one sighting of a record: what a worker saw, where, and when.
type Observation struct {
	OCID       string          `json:"ocid"`
	Source     string          `json:"source"`
	Origin     string          `json:"origin"` // "bulk:<file id>" or "api:<object key>"
	WorkerID   string          `json:"worker_id"`
	ObservedAt time.Time       `json:"observed_at"`
	Lamport    uint64          `json:"lamport"`
	Record     json.RawMessage `json:"record"`
}

type Release struct {
	ID   string   `json:"id"`
	Date string   `json:"date"`
	Tags []string `json:"tags,omitempty"`
}

// Compiled is the LWW register holding the newest compiledRelease seen.
type Compiled struct {
	Date   string          `json:"date"`
	Digest string          `json:"digest"`
	Body   json.RawMessage `json:"body"`
}

type State struct {
	OCID      string             `json:"ocid"`
	Source    string             `json:"source"`
	Lamport   uint64             `json:"lamport"`
	Compiled  Compiled           `json:"compiled"`
	Releases  map[string]Release `json:"releases"`  // max-register per release id
	Origins   map[string]bool    `json:"origins"`   // G-Set
	Sightings map[string]bool    `json:"sightings"` // G-Set of "origin|worker"
	FirstSeen time.Time          `json:"first_seen"`
	LastSeen  time.Time          `json:"last_seen"`
	Merges    uint64             `json:"merges"`
}

func sightingKey(origin, worker string) string { return origin + "|" + worker }

type rawRecord struct {
	OCID     string `json:"ocid"`
	Compiled struct {
		Date string `json:"date"`
	} `json:"compiledRelease"`
	Releases []struct {
		ID   string   `json:"id"`
		Date string   `json:"date"`
		Tag  []string `json:"tag"`
	} `json:"releases"`
}

// FromObservation lifts one observation into a singleton state.
func FromObservation(o Observation) (*State, error) {
	var raw struct {
		rawRecord
		Compiled json.RawMessage `json:"compiledRelease"`
	}
	if err := json.Unmarshal(o.Record, &raw); err != nil {
		return nil, err
	}
	var meta rawRecord
	if err := json.Unmarshal(o.Record, &meta); err != nil {
		return nil, err
	}
	s := &State{
		OCID:      o.OCID,
		Source:    o.Source,
		Lamport:   o.Lamport,
		Releases:  map[string]Release{},
		Origins:   map[string]bool{o.Origin: true},
		Sightings: map[string]bool{sightingKey(o.Origin, o.WorkerID): true},
		FirstSeen: o.ObservedAt,
		LastSeen:  o.ObservedAt,
	}
	if len(raw.Compiled) > 0 {
		sum := sha256.Sum256(raw.Compiled)
		s.Compiled = Compiled{Date: meta.Compiled.Date, Digest: hex.EncodeToString(sum[:]), Body: raw.Compiled}
	}
	for _, r := range meta.Releases {
		if r.ID == "" {
			continue
		}
		tags := append([]string(nil), r.Tag...)
		sort.Strings(tags)
		s.Releases[r.ID] = Release{ID: r.ID, Date: r.Date, Tags: tags}
	}
	return s, nil
}

// Merge joins two states. The receiver is updated in place and the result
// reports whether the record's content changed: a new compiled release, a new
// release, origin or sighting. Timestamps and the Lamport time are merged too
// but do not count, so re-observing an unchanged record (a re-crawl, a
// redelivery) is absorbed without republishing it.
func (s *State) Merge(o *State) bool {
	changed := false
	if o.Lamport > s.Lamport {
		s.Lamport = o.Lamport
	}
	if newer(o.Compiled, s.Compiled) {
		s.Compiled = o.Compiled
		changed = true
	}
	for id, r := range o.Releases {
		if cur, ok := s.Releases[id]; !ok || releaseLess(cur, r) {
			s.Releases[id] = r
			changed = true
		}
	}
	for origin := range o.Origins {
		if !s.Origins[origin] {
			s.Origins[origin] = true
			changed = true
		}
	}
	for sighting := range o.Sightings {
		if !s.Sightings[sighting] {
			s.Sightings[sighting] = true
			changed = true
		}
	}
	if !o.FirstSeen.IsZero() && (s.FirstSeen.IsZero() || o.FirstSeen.Before(s.FirstSeen)) {
		s.FirstSeen = o.FirstSeen
	}
	if o.LastSeen.After(s.LastSeen) {
		s.LastSeen = o.LastSeen
	}
	if s.Source == "" {
		s.Source = o.Source
	}
	if changed {
		s.Merges++
	}
	return changed
}

// releaseLess orders two versions of the same release id so the register
// converges even if a publisher amends a release in place.
func releaseLess(a, b Release) bool {
	if a.Date != b.Date {
		return a.Date < b.Date
	}
	return strings.Join(a.Tags, ",") < strings.Join(b.Tags, ",")
}

// newer decides the LWW register: a later compiledRelease date wins; equal
// dates fall back to the digest so the choice is deterministic on every node.
func newer(candidate, current Compiled) bool {
	if candidate.Body == nil {
		return false
	}
	if current.Body == nil {
		return true
	}
	if candidate.Date != current.Date {
		return candidate.Date > current.Date
	}
	return candidate.Digest > current.Digest
}

// Fingerprint hashes the record's content so convergence can be checked
// across replicas regardless of merge order. Merges counts local work and
// the timestamps depend on which sighting was merged last, so neither is
// part of it.
func (s *State) Fingerprint() string {
	type view struct {
		OCID      string
		Compiled  string
		Releases  []Release
		Origins   []string
		Sightings []string
	}
	v := view{OCID: s.OCID, Compiled: s.Compiled.Digest}
	for k := range s.Sightings {
		v.Sightings = append(v.Sightings, k)
	}
	sort.Strings(v.Sightings)
	for _, r := range s.Releases {
		v.Releases = append(v.Releases, r)
	}
	sort.Slice(v.Releases, func(i, j int) bool { return v.Releases[i].ID < v.Releases[j].ID })
	for o := range s.Origins {
		v.Origins = append(v.Origins, o)
	}
	sort.Strings(v.Origins)
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Observations counts distinct sightings per worker.
func (s *State) Observations() map[string]int {
	out := map[string]int{}
	for k := range s.Sightings {
		if i := strings.LastIndex(k, "|"); i >= 0 {
			out[k[i+1:]]++
		}
	}
	return out
}

func (s *State) TotalObservations() int { return len(s.Sightings) }
