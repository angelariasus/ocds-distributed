// Package schedule is the coordinator's replicated state machine. Every
// replica applies the same committed commands in the same order, so whichever
// replica becomes leader knows which round was planned last, whether its tasks
// were fully published, and when the next round is due.
package schedule

import (
	"encoding/json"
	"sync"
	"time"
)

const (
	RoundPlanned   = "round_planned"
	RoundPublished = "round_published"
)

type Command struct {
	Type   string    `json:"type"`
	Round  uint64    `json:"round"`
	At     time.Time `json:"at"`
	Tasks  int       `json:"tasks,omitempty"`
	Leader string    `json:"leader"`
}

func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

type State struct {
	LastRound   uint64    `json:"last_round"`
	PlannedAt   time.Time `json:"planned_at"`
	PublishedAt time.Time `json:"published_at"`
	Pending     bool      `json:"pending"` // planned but not confirmed published
	PlannedBy   string    `json:"planned_by"`
	Tasks       int       `json:"tasks"`
}

type Machine struct {
	mu    sync.Mutex
	state State
}

func (m *Machine) Apply(raw []byte) error {
	var c Command
	if err := json.Unmarshal(raw, &c); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch c.Type {
	case RoundPlanned:
		if c.Round > m.state.LastRound {
			m.state.LastRound, m.state.PlannedAt, m.state.PlannedBy, m.state.Pending = c.Round, c.At, c.Leader, true
		}
	case RoundPublished:
		if c.Round == m.state.LastRound {
			m.state.PublishedAt, m.state.Pending, m.state.Tasks = c.At, false, c.Tasks
		}
	}
	return nil
}

func (m *Machine) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Next decides what a leader should do now.
func (m *Machine) Next(now time.Time, interval time.Duration, force bool) (action string, round uint64) {
	s := m.State()
	switch {
	case s.Pending:
		return "republish", s.LastRound // the previous leader died between planning and publishing
	case force || s.LastRound == 0 || now.Sub(s.PublishedAt) >= interval:
		return "plan", s.LastRound + 1
	default:
		return "wait", s.LastRound
	}
}
