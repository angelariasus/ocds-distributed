package lamport

import "testing"

func TestTickIsMonotonic(t *testing.T) {
	var c Clock
	if a, b := c.Tick(), c.Tick(); !(a < b) {
		t.Fatalf("ticks %d, %d not increasing", a, b)
	}
}

func TestWitnessJumpsPastRemote(t *testing.T) {
	var c Clock
	c.Tick()
	if got := c.Witness(41); got != 42 {
		t.Fatalf("witness(41) = %d, want 42", got)
	}
	if got := c.Witness(5); got != 43 {
		t.Fatalf("witness(5) = %d, want 43 (older remote must not move the clock back)", got)
	}
}

func TestSendReceiveOrdersCausallyRelatedEvents(t *testing.T) {
	var a, b Clock
	send := a.Tick()
	recv := b.Witness(send)
	reply := b.Tick()
	got := a.Witness(reply)
	if !(send < recv && recv < reply && reply < got) {
		t.Fatalf("causal chain broken: %d %d %d %d", send, recv, reply, got)
	}
}
