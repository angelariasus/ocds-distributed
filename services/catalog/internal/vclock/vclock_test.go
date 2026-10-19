package vclock

import "testing"

func TestCompare(t *testing.T) {
	cases := []struct {
		name string
		a, b Clock
		want Order
	}{
		{"empty", Clock{}, Clock{}, Equal},
		{"same", Clock{"a": 1}, Clock{"a": 1}, Equal},
		{"descendant", Clock{"a": 2}, Clock{"a": 1}, After},
		{"ancestor", Clock{"a": 1}, Clock{"a": 1, "b": 1}, Before},
		{"concurrent", Clock{"a": 1}, Clock{"b": 1}, Concurrent},
		{"concurrent mixed", Clock{"a": 2, "b": 1}, Clock{"a": 1, "b": 2}, Concurrent},
		{"zero entry ignored", Clock{"a": 1, "b": 0}, Clock{"a": 1}, Equal},
	}
	for _, c := range cases {
		if got := Compare(c.a, c.b); got != c.want {
			t.Errorf("%s: Compare(%v, %v) = %v, want %v", c.name, c.a, c.b, got, c.want)
		}
	}
}

func TestIncrementDoesNotMutateInput(t *testing.T) {
	a := Clock{"a": 1}
	b := a.Increment("b")
	if a["b"] != 0 || b["b"] != 1 || b["a"] != 1 {
		t.Fatalf("a=%v b=%v", a, b)
	}
	if Compare(b, a) != After {
		t.Fatal("increment must produce a descendant")
	}
}

func TestMergeDescendsFromAllInputs(t *testing.T) {
	a := Clock{"a": 3, "b": 1}
	b := Clock{"b": 2, "c": 1}
	m := Merge(a, b)
	if !Descends(m, a) || !Descends(m, b) {
		t.Fatalf("merge %v does not descend from both", m)
	}
	if m.String() != "{a:3,b:2,c:1}" {
		t.Fatalf("merge = %s", m)
	}
}
