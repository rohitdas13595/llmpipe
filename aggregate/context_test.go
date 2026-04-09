package aggregate

import (
	"testing"
)

func TestNewLLMContextSystemPrompt(t *testing.T) {
	c := NewLLMContext("be nice")
	msgs := c.Snapshot()
	if len(msgs) != 1 {
		t.Fatalf("len = %d", len(msgs))
	}
	if msgs[0]["role"] != "system" || msgs[0]["content"] != "be nice" {
		t.Fatalf("msg = %#v", msgs[0])
	}
}

func TestNewLLMContextEmptySystem(t *testing.T) {
	c := NewLLMContext("")
	if len(c.Snapshot()) != 0 {
		t.Fatalf("expected no messages, got %d", len(c.Snapshot()))
	}
}

func TestLLMContextSnapshotIsPointInTime(t *testing.T) {
	c := NewLLMContext("")
	c.AppendUser("u1")
	s1 := c.Snapshot()
	c.AppendUser("u2")
	if len(s1) != 1 {
		t.Fatalf("earlier snapshot len = %d, want 1", len(s1))
	}
	s2 := c.Snapshot()
	if len(s2) != 2 {
		t.Fatalf("later snapshot len = %d, want 2", len(s2))
	}
}
