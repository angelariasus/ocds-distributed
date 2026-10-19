// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// 2014) for a small, fixed cluster: leader election with randomized timeouts,
// log replication with the consistency check on the previous entry, commit by
// majority restricted to entries of the leader's own term, and persistence of
// term, vote and log before answering any RPC. Snapshots and membership
// changes are out of scope; the coordinator's log grows by one entry per
// planning round.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string { return [...]string{"follower", "candidate", "leader"}[r] }

type Entry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"` // nil marks the no-op a new leader appends
}

type VoteRequest struct {
	// PreVote asks "would you vote for me?" without anyone changing state.
	PreVote      bool   `json:"pre_vote,omitempty"`
	Term         uint64 `json:"term"`
	Candidate    string `json:"candidate"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type VoteResponse struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

type AppendRequest struct {
	Term         uint64  `json:"term"`
	Leader       string  `json:"leader"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type AppendResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index"` // where the leader should retry from
}

type Transport interface {
	RequestVote(ctx context.Context, peer string, req VoteRequest) (VoteResponse, error)
	AppendEntries(ctx context.Context, peer string, req AppendRequest) (AppendResponse, error)
}

type PersistentState struct {
	CurrentTerm uint64  `json:"current_term"`
	VotedFor    string  `json:"voted_for"`
	Log         []Entry `json:"log"`
}

type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type Config struct {
	ID                 string
	Peers              []string // other members' IDs
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	Heartbeat          time.Duration
	RPCTimeout         time.Duration
	Transport          Transport
	Storage            Storage
	Apply              func(Entry) // called once per committed entry, in order
	OnRoleChange       func(role Role, term uint64, leader string)
	Logger             *slog.Logger
}

var (
	ErrNotLeader      = errors.New("raft: not the leader")
	ErrLostLeadership = errors.New("raft: entry was overwritten after leadership was lost")
)

type Node struct {
	cfg Config
	log *slog.Logger
	rnd *rand.Rand

	mu          sync.Mutex
	role        Role
	currentTerm uint64
	votedFor    string
	entries     []Entry // entries[0] is a sentinel at index 0, term 0
	commitIndex uint64
	lastApplied uint64
	leaderID    string
	deadline    time.Time
	lastBeat    time.Time
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	lastAck     map[string]time.Time
	heardLeader time.Time // last valid AppendEntries from a leader
	applyCond   *sync.Cond
	waiters     map[uint64][]chan struct{}
	stopped     bool
}

func New(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:         cfg,
		log:         cfg.Logger.With("raft", cfg.ID),
		rnd:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(cfg.ID)))),
		currentTerm: st.CurrentTerm,
		votedFor:    st.VotedFor,
		entries:     append([]Entry{{}}, st.Log...),
		waiters:     map[uint64][]chan struct{}{},
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.resetDeadlineLocked()
	return n, nil
}

// Run drives timers and the apply loop until ctx is cancelled.
func (n *Node) Run(ctx context.Context) {
	go n.applyLoop()
	ticker := time.NewTicker(n.cfg.Heartbeat / 4)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			n.mu.Lock()
			n.stopped = true
			n.applyCond.Broadcast()
			n.mu.Unlock()
			return
		case now := <-ticker.C:
			n.mu.Lock()
			switch {
			case n.role == Leader && !n.hasQuorumLocked(now):
				// CheckQuorum: a leader cut off from the majority steps down instead
				// of believing it still leads while the others elect someone else.
				n.log.Warn("lost contact with the majority, stepping down", "term", n.currentTerm)
				n.setRoleLocked(Follower, "")
				n.resetDeadlineLocked()
			case n.role == Leader && now.Sub(n.lastBeat) >= n.cfg.Heartbeat:
				n.lastBeat = now
				n.broadcastAppendLocked()
			case n.role != Leader && now.After(n.deadline):
				n.startPreVoteLocked()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) hasQuorumLocked(now time.Time) bool {
	alive := 1
	for _, p := range n.cfg.Peers {
		if now.Sub(n.lastAck[p]) <= n.cfg.ElectionTimeoutMax {
			alive++
		}
	}
	return alive >= n.majority()
}

func (n *Node) lastIndexLocked() uint64 { return n.entries[len(n.entries)-1].Index }
func (n *Node) lastTermLocked() uint64  { return n.entries[len(n.entries)-1].Term }
func (n *Node) majority() int           { return (len(n.cfg.Peers)+1)/2 + 1 }

func (n *Node) termAtLocked(index uint64) (uint64, bool) {
	if index > n.lastIndexLocked() {
		return 0, false
	}
	return n.entries[index].Term, true
}

func (n *Node) resetDeadlineLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.deadline = time.Now().Add(n.cfg.ElectionTimeoutMin + time.Duration(n.rnd.Int63n(int64(span)+1)))
}

func (n *Node) persistLocked() {
	st := PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, Log: append([]Entry(nil), n.entries[1:]...)}
	if err := n.cfg.Storage.Save(st); err != nil {
		// Answering an RPC without having persisted would break safety; stop instead.
		panic(fmt.Sprintf("raft: persist: %v", err))
	}
}

func (n *Node) setRoleLocked(role Role, leader string) {
	changed := n.role != role || n.leaderID != leader
	n.role, n.leaderID = role, leader
	if changed && n.cfg.OnRoleChange != nil {
		go n.cfg.OnRoleChange(role, n.currentTerm, leader)
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leader string) {
	if term > n.currentTerm {
		n.currentTerm, n.votedFor = term, ""
		n.persistLocked()
	}
	n.setRoleLocked(Follower, leader)
	n.resetDeadlineLocked()
}

// startPreVoteLocked polls the cluster at term+1 without bumping its own term.
// A node cut off from the others keeps failing this round instead of
// inflating its term, so when it reconnects it cannot depose a healthy leader
// (Ongaro, "Consensus: Bridging Theory and Practice", §9.6).
func (n *Node) startPreVoteLocked() {
	n.resetDeadlineLocked()
	if 1 >= n.majority() {
		n.startElectionLocked()
		return
	}
	term := n.currentTerm
	req := VoteRequest{PreVote: true, Term: term + 1, Candidate: n.cfg.ID, LastLogIndex: n.lastIndexLocked(), LastLogTerm: n.lastTermLocked()}
	grants := 1
	for _, peer := range n.cfg.Peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
			if err != nil || !resp.Granted {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.role == Leader || n.currentTerm != term {
				return // something happened meanwhile; this poll is stale
			}
			grants++
			if grants == n.majority() {
				n.startElectionLocked()
			}
		}(peer)
	}
}

func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.persistLocked()
	n.setRoleLocked(Candidate, "")
	n.resetDeadlineLocked()
	term := n.currentTerm
	req := VoteRequest{Term: term, Candidate: n.cfg.ID, LastLogIndex: n.lastIndexLocked(), LastLogTerm: n.lastTermLocked()}
	n.log.Info("election started", "term", term)

	votes := 1
	if votes >= n.majority() { // single-node cluster
		n.becomeLeaderLocked()
		return
	}
	for _, peer := range n.cfg.Peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				return
			}
			if n.role != Candidate || n.currentTerm != term || !resp.Granted {
				return
			}
			votes++
			if votes >= n.majority() {
				n.becomeLeaderLocked()
			}
		}(peer)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.setRoleLocked(Leader, n.cfg.ID)
	n.nextIndex, n.matchIndex, n.lastAck = map[string]uint64{}, map[string]uint64{}, map[string]time.Time{}
	now := time.Now()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndexLocked() + 1
		n.matchIndex[p] = 0
		n.lastAck[p] = now // grace period: the votes just proved a majority is reachable
	}
	// A leader may only count replicas for entries of its own term, so it
	// appends a no-op to be able to commit (and learn) everything before it.
	n.entries = append(n.entries, Entry{Index: n.lastIndexLocked() + 1, Term: n.currentTerm})
	n.persistLocked()
	n.log.Info("became leader", "term", n.currentTerm, "last_index", n.lastIndexLocked())
	n.lastBeat = time.Now()
	n.broadcastAppendLocked()
	n.advanceCommitLocked()
}

func (n *Node) broadcastAppendLocked() {
	for _, p := range n.cfg.Peers {
		n.replicateLocked(p)
	}
}

func (n *Node) replicateLocked(peer string) {
	term := n.currentTerm
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prev := next - 1
	prevTerm, _ := n.termAtLocked(prev)
	entries := append([]Entry(nil), n.entries[next:]...)
	req := AppendRequest{Term: term, Leader: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: n.commitIndex}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		defer cancel()
		resp, err := n.cfg.Transport.AppendEntries(ctx, peer, req)
		if err != nil {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if resp.Term > n.currentTerm {
			n.log.Info("stepping down", "term", n.currentTerm, "newer_term", resp.Term, "from", peer)
			n.becomeFollowerLocked(resp.Term, "")
			return
		}
		if n.role != Leader || n.currentTerm != term {
			return // stale response from an earlier term
		}
		n.lastAck[peer] = time.Now()
		if resp.Success {
			match := prev + uint64(len(entries))
			if match > n.matchIndex[peer] {
				n.matchIndex[peer] = match
				n.nextIndex[peer] = match + 1
			}
			n.advanceCommitLocked()
			return
		}
		if resp.ConflictIndex >= 1 && resp.ConflictIndex < n.nextIndex[peer] {
			n.nextIndex[peer] = resp.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		n.replicateLocked(peer)
	}()
}

func (n *Node) advanceCommitLocked() {
	for idx := n.lastIndexLocked(); idx > n.commitIndex; idx-- {
		if n.entries[idx].Term != n.currentTerm {
			break // Raft §5.4.2: never count replicas for entries of older terms
		}
		count := 1
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			return
		}
	}
}

// HandleRequestVote is the receiver side of RequestVote.
func (n *Node) HandleRequestVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.PreVote {
		// Leader stickiness: while a leader is alive (or we are it), refuse.
		alive := n.role == Leader || time.Since(n.heardLeader) < n.cfg.ElectionTimeoutMin
		upToDate := req.LastLogTerm > n.lastTermLocked() ||
			(req.LastLogTerm == n.lastTermLocked() && req.LastLogIndex >= n.lastIndexLocked())
		return VoteResponse{Term: n.currentTerm, Granted: req.Term > n.currentTerm && !alive && upToDate}
	}
	if req.Term < n.currentTerm {
		return VoteResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}
	upToDate := req.LastLogTerm > n.lastTermLocked() ||
		(req.LastLogTerm == n.lastTermLocked() && req.LastLogIndex >= n.lastIndexLocked())
	if (n.votedFor == "" || n.votedFor == req.Candidate) && upToDate {
		n.votedFor = req.Candidate
		n.persistLocked()
		n.resetDeadlineLocked()
		return VoteResponse{Term: n.currentTerm, Granted: true}
	}
	return VoteResponse{Term: n.currentTerm}
}

// HandleAppendEntries is the receiver side of AppendEntries (and heartbeats).
func (n *Node) HandleAppendEntries(req AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		return AppendResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm || n.role != Follower || n.leaderID != req.Leader {
		n.becomeFollowerLocked(req.Term, req.Leader)
	}
	n.resetDeadlineLocked()
	n.heardLeader = time.Now()

	if req.PrevLogIndex > n.lastIndexLocked() {
		return AppendResponse{Term: n.currentTerm, ConflictIndex: n.lastIndexLocked() + 1}
	}
	if t, _ := n.termAtLocked(req.PrevLogIndex); t != req.PrevLogTerm {
		// Skip back over the whole conflicting term in one round trip.
		first := req.PrevLogIndex
		for first > 1 && n.entries[first-1].Term == t {
			first--
		}
		return AppendResponse{Term: n.currentTerm, ConflictIndex: first}
	}

	changed := false
	for i, e := range req.Entries {
		if e.Index <= n.lastIndexLocked() {
			if n.entries[e.Index].Term == e.Term {
				continue
			}
			n.entries = n.entries[:e.Index] // conflicting suffix: discard it
		}
		n.entries = append(n.entries, req.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persistLocked()
	}
	if req.LeaderCommit > n.commitIndex {
		last := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = min(req.LeaderCommit, last)
		n.applyCond.Broadcast()
	}
	return AppendResponse{Term: n.currentTerm, Success: true}
}

func (n *Node) applyLoop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for n.lastApplied >= n.commitIndex && !n.stopped {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}
		batch := append([]Entry(nil), n.entries[n.lastApplied+1:n.commitIndex+1]...)
		n.mu.Unlock()
		for _, e := range batch {
			if e.Command != nil && n.cfg.Apply != nil {
				n.cfg.Apply(e)
			}
		}
		n.mu.Lock()
		n.lastApplied = batch[len(batch)-1].Index
		for idx, chans := range n.waiters {
			if idx <= n.lastApplied {
				for _, ch := range chans {
					close(ch)
				}
				delete(n.waiters, idx)
			}
		}
	}
}

// Propose appends a command if this node is the leader and blocks until it is
// applied, or until ctx expires. ErrLostLeadership means another leader's
// entry ended up at that index: the command was never committed.
func (n *Node) Propose(ctx context.Context, cmd []byte) (Entry, error) {
	n.mu.Lock()
	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		return Entry{}, fmt.Errorf("%w (leader is %q)", ErrNotLeader, leader)
	}
	e := Entry{Index: n.lastIndexLocked() + 1, Term: n.currentTerm, Command: cmd}
	n.entries = append(n.entries, e)
	n.persistLocked()
	done := make(chan struct{})
	n.waiters[e.Index] = append(n.waiters[e.Index], done)
	n.broadcastAppendLocked()
	n.advanceCommitLocked()
	n.mu.Unlock()

	select {
	case <-ctx.Done():
		return e, ctx.Err()
	case <-done:
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if t, ok := n.termAtLocked(e.Index); !ok || t != e.Term {
		return e, ErrLostLeadership
	}
	return e, nil
}

type Status struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	Leader      string `json:"leader"`
	LastIndex   uint64 `json:"last_index"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.cfg.ID, Role: n.role.String(), Term: n.currentTerm, Leader: n.leaderID,
		LastIndex: n.lastIndexLocked(), CommitIndex: n.commitIndex, LastApplied: n.lastApplied}
}

func (n *Node) IsLeader() (bool, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader, n.currentTerm
}

// Log returns a copy of the log (without the sentinel), for tests and debugging.
func (n *Node) Log() []Entry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Entry(nil), n.entries[1:]...)
}
