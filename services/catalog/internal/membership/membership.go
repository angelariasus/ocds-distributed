// Package membership is a SWIM-style failure detector: each round a node
// pings one random peer, asks others to probe it indirectly when the ping
// fails, and spreads what it knows about everyone piggybacked on the pings.
package membership

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type State int

const (
	Alive State = iota
	Suspect
	Dead
)

func (s State) String() string { return [...]string{"alive", "suspect", "dead"}[s] }

func ParseState(s string) State {
	switch s {
	case "suspect":
		return Suspect
	case "dead":
		return Dead
	default:
		return Alive
	}
}

type Member struct {
	ID          string
	Addr        string
	State       State
	Incarnation uint64
	Heartbeat   uint64
	lastChange  time.Time
}

type Transport interface {
	// Ping delivers our view and returns the peer's view.
	Ping(ctx context.Context, addr string, from Member, view []Member) ([]Member, error)
	// PingReq asks the node at via to probe target on our behalf.
	PingReq(ctx context.Context, via string, target Member) (bool, error)
}

type Config struct {
	SuspectTimeout time.Duration // suspect → dead
	DeadRetention  time.Duration // dead → forgotten
	IndirectProbes int
	RPCTimeout     time.Duration
	// Every ReconnectEvery rounds the node also probes one seed it does not
	// currently see as alive and one member it has declared dead. Without it a
	// healed partition never merges back: each side has forgotten the other.
	ReconnectEvery int
}

func DefaultConfig() Config {
	return Config{SuspectTimeout: 10 * time.Second, DeadRetention: 2 * time.Minute, IndirectProbes: 2, RPCTimeout: 2 * time.Second, ReconnectEvery: 5}
}

type List struct {
	cfg       Config
	transport Transport
	now       func() time.Time
	rand      *rand.Rand
	onChange  func(Member)

	mu      sync.Mutex
	self    Member
	members map[string]*Member
	seeds   map[string]string
	rounds  int
}

func New(self Member, seeds map[string]string, transport Transport, cfg Config, now func() time.Time, onChange func(Member)) *List {
	self.State = Alive
	self.lastChange = now()
	l := &List{
		cfg:       cfg,
		transport: transport,
		now:       now,
		rand:      rand.New(rand.NewSource(now().UnixNano())),
		onChange:  onChange,
		self:      self,
		members:   map[string]*Member{},
		seeds:     map[string]string{},
	}
	for id, addr := range seeds {
		if id != self.ID {
			l.seeds[id] = addr
			m := &Member{ID: id, Addr: addr, State: Alive, lastChange: now()}
			l.members[id] = m
		}
	}
	return l
}

func (l *List) Self() Member {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.self
}

// View returns every known member including self, sorted by id.
func (l *List) View() []Member {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.viewLocked()
}

func (l *List) viewLocked() []Member {
	out := make([]Member, 0, len(l.members)+1)
	out = append(out, l.self)
	for _, m := range l.members {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (l *List) Alive() []Member {
	var out []Member
	for _, m := range l.View() {
		if m.State == Alive {
			out = append(out, m)
		}
	}
	return out
}

// Round advances the heartbeat, probes one peer and expires stale members.
func (l *List) Round(ctx context.Context) {
	l.mu.Lock()
	l.self.Heartbeat++
	l.rounds++
	target := l.pickLocked()
	var reconnect []Member
	if l.cfg.ReconnectEvery > 0 && l.rounds%l.cfg.ReconnectEvery == 0 {
		reconnect = l.reconnectTargetsLocked()
	}
	view := l.viewLocked()
	self := l.self
	l.mu.Unlock()

	if target != nil {
		l.probe(ctx, *target, self, view)
	}
	for _, m := range reconnect {
		pctx, cancel := context.WithTimeout(ctx, l.cfg.RPCTimeout)
		remote, err := l.transport.Ping(pctx, m.Addr, self, view)
		cancel()
		if err == nil {
			// Their view says they are alive with a fresher heartbeat (or a
			// higher incarnation), which is what revives them in ours.
			l.Merge(remote)
		}
	}
	l.expire()
}

// reconnectTargetsLocked picks one seed not seen alive and one dead member.
func (l *List) reconnectTargetsLocked() []Member {
	var out []Member
	var seedIDs []string
	for id := range l.seeds {
		if m, ok := l.members[id]; !ok || m.State == Dead {
			seedIDs = append(seedIDs, id)
		}
	}
	if len(seedIDs) > 0 {
		id := seedIDs[l.rand.Intn(len(seedIDs))]
		out = append(out, Member{ID: id, Addr: l.seeds[id]})
	}
	var dead []*Member
	for _, m := range l.members {
		if m.State == Dead && l.seeds[m.ID] == "" {
			dead = append(dead, m)
		}
	}
	if len(dead) > 0 {
		out = append(out, *dead[l.rand.Intn(len(dead))])
	}
	return out
}

func (l *List) pickLocked() *Member {
	var candidates []*Member
	for _, m := range l.members {
		if m.State != Dead {
			candidates = append(candidates, m)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[l.rand.Intn(len(candidates))]
}

func (l *List) probe(ctx context.Context, target, self Member, view []Member) {
	pctx, cancel := context.WithTimeout(ctx, l.cfg.RPCTimeout)
	remote, err := l.transport.Ping(pctx, target.Addr, self, view)
	cancel()
	if err == nil {
		l.Merge(remote)
		l.markAlive(target.ID)
		return
	}
	for _, via := range l.indirectCandidates(target.ID) {
		pctx, cancel := context.WithTimeout(ctx, l.cfg.RPCTimeout)
		ok, err := l.transport.PingReq(pctx, via.Addr, target)
		cancel()
		if err == nil && ok {
			l.markAlive(target.ID)
			return
		}
	}
	l.suspect(target.ID)
}

func (l *List) indirectCandidates(exclude string) []Member {
	l.mu.Lock()
	defer l.mu.Unlock()
	var pool []Member
	for _, m := range l.members {
		if m.ID != exclude && m.State == Alive {
			pool = append(pool, *m)
		}
	}
	l.rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	if len(pool) > l.cfg.IndirectProbes {
		pool = pool[:l.cfg.IndirectProbes]
	}
	return pool
}

func (l *List) markAlive(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m, ok := l.members[id]; ok && m.State != Alive {
		l.setState(m, Alive)
	}
}

func (l *List) suspect(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m, ok := l.members[id]; ok && m.State == Alive {
		l.setState(m, Suspect)
	}
}

func (l *List) expire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for id, m := range l.members {
		switch {
		case m.State == Suspect && now.Sub(m.lastChange) >= l.cfg.SuspectTimeout:
			l.setState(m, Dead)
		case m.State == Dead && now.Sub(m.lastChange) >= l.cfg.DeadRetention:
			delete(l.members, id)
		}
	}
}

func (l *List) setState(m *Member, s State) {
	m.State = s
	m.lastChange = l.now()
	if l.onChange != nil {
		l.onChange(*m)
	}
}

// Merge folds a peer's view into ours using SWIM's precedence rules: a higher
// incarnation always wins; at equal incarnation a fresher heartbeat refreshes
// an alive member and suspect/dead override alive. Hearing that we ourselves
// are suspected makes us bump our incarnation to refute it.
func (l *List) Merge(remote []Member) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range remote {
		if r.ID == l.self.ID {
			if r.State != Alive && r.Incarnation >= l.self.Incarnation {
				l.self.Incarnation = r.Incarnation + 1
			}
			continue
		}
		local, known := l.members[r.ID]
		if !known && r.State == Dead {
			// Learning about a node only to record that it is dead would let peers
			// that have not expired it yet re-introduce it forever.
			continue
		}
		if !known {
			m := r
			m.lastChange = l.now()
			l.members[r.ID] = &m
			if l.onChange != nil {
				l.onChange(m)
			}
			continue
		}
		switch {
		case r.Incarnation > local.Incarnation:
			local.Incarnation, local.Heartbeat, local.Addr = r.Incarnation, r.Heartbeat, r.Addr
			if local.State != r.State {
				l.setState(local, r.State)
			} else {
				local.lastChange = l.now()
			}
		case r.Incarnation == local.Incarnation:
			if r.Heartbeat > local.Heartbeat {
				local.Heartbeat = r.Heartbeat
				if r.State == Alive && local.State != Alive {
					l.setState(local, Alive)
				} else if r.State == Alive {
					local.lastChange = l.now()
				}
			}
			if r.State > local.State {
				l.setState(local, r.State)
			}
		}
	}
}
