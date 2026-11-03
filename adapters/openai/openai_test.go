package openai

import (
	"encoding/json"
	"testing"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestChatGolden(t *testing.T) {
	msgs, err := Chat{}.Messages([]aggregate.Message{
		{Role: aggregate.RoleSystem, Content: "be nice"},
		{Role: aggregate.RoleUser, Content: "hi"},
		{Role: aggregate.RoleAssistant, Content: "", ToolCalls: []aggregate.ToolCall{{
			ID: "call_1", Name: "get_weather", Args: json.RawMessage(`{"location":"Paris"}`),
		}}},
		{Role: aggregate.RoleTool, ToolCallID: "call_1", Name: "get_weather", Content: `{"condition":"sun"}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(msgs)
	if err != nil {
		t.Fatal(err)
	}
	const want = `[{"content":"be nice","role":"system"},{"content":"hi","role":"user"},{"content":null,"role":"assistant","tool_calls":[{"function":{"arguments":"{\"location\":\"Paris\"}","name":"get_weather"},"id":"call_1","type":"function"}]},{"content":"{\"condition\":\"sun\"}","name":"get_weather","role":"tool","tool_call_id":"call_1"}]`
	if string(b) != want {
		t.Fatalf("messages = %s", b)
	}
	schema, err := Chat{}.Tools(tools.Schema{Functions: []tools.Function{{
		Name: "get_weather", Description: "Weather", Parameters: map[string]any{"type": "object"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err = json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	const toolsWant = `[{"function":{"description":"Weather","name":"get_weather","parameters":{"type":"object"}},"type":"function"}]`
	if string(b) != toolsWant {
		t.Fatalf("tools = %s", b)
	}
}
