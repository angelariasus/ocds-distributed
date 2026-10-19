package schedule

import (
	"testing"
	"time"
)

func TestFirstRoundIsDueImmediately(t *testing.T) {
	var m Machine
	if action, round := m.Next(time.Now(), time.Hour, false); action != "plan" || round != 1 {
		t.Fatalf("%s %d", action, round)
	}
}

func TestPublishedRoundWaitsForTheInterval(t *testing.T) {
	var m Machine
	t0 := time.Date(2026, 10, 19, 6, 0, 0, 0, time.UTC)
	m.Apply(Command{Type: RoundPlanned, Round: 1, At: t0, Leader: "c1"}.Encode())
	m.Apply(Command{Type: RoundPublished, Round: 1, At: t0, Tasks: 3}.Encode())
	if action, _ := m.Next(t0.Add(30*time.Minute), time.Hour, false); action != "wait" {
		t.Fatalf("got %s before the interval", action)
	}
	if action, round := m.Next(t0.Add(time.Hour), time.Hour, false); action != "plan" || round != 2 {
		t.Fatalf("got %s %d after the interval", action, round)
	}
	if action, round := m.Next(t0.Add(time.Minute), time.Hour, true); action != "plan" || round != 2 {
		t.Fatalf("forced: %s %d", action, round)
	}
}

func TestUnpublishedRoundIsRepublishedByTheNextLeader(t *testing.T) {
	var m Machine
	m.Apply(Command{Type: RoundPlanned, Round: 4, At: time.Now(), Leader: "c1"}.Encode())
	if action, round := m.Next(time.Now(), time.Hour, true); action != "republish" || round != 4 {
		t.Fatalf("%s %d", action, round)
	}
}

func TestStaleCommandsDoNotRegress(t *testing.T) {
	var m Machine
	now := time.Now()
	m.Apply(Command{Type: RoundPlanned, Round: 5, At: now}.Encode())
	m.Apply(Command{Type: RoundPlanned, Round: 3, At: now}.Encode())
	m.Apply(Command{Type: RoundPublished, Round: 3, At: now}.Encode())
	if s := m.State(); s.LastRound != 5 || !s.Pending {
		t.Fatalf("state regressed: %+v", s)
	}
}
