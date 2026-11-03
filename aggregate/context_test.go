package aggregate

import (
	"encoding/json"
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

func TestToolRoundMapView(t *testing.T) {
	c := NewLLMContext("sys")
	c.AppendToolRound("", []ToolCall{{
		ID: "call_1", Name: "get_weather", Args: json.RawMessage(`{"location":"Paris"}`),
	}})
	if !c.SetToolResult("call_1", "sun") {
		t.Fatal("missing placeholder")
	}
	msgs := c.Messages()
	if len(msgs) != 3 || msgs[1].Role != RoleAssistant || len(msgs[1].ToolCalls) != 1 {
		t.Fatalf("messages = %#v", msgs)
	}
	if msgs[2].Role != RoleTool || msgs[2].Content != "sun" || msgs[2].ToolCallID != "call_1" {
		t.Fatalf("tool message = %#v", msgs[2])
	}
	snap := c.Snapshot()
	if snap[1]["tool_calls"] == nil || snap[2]["tool_call_id"] != "call_1" {
		t.Fatalf("snapshot = %#v", snap)
	}
	// Mutating the snapshot must not change the stored transcript.
	snap[2]["content"] = "mutated"
	if c.Messages()[2].Content != "sun" {
		t.Fatal("snapshot alias")
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
