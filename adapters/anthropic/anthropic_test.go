package anthropic

import (
	"encoding/json"
	"testing"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestMessagesGolden(t *testing.T) {
	raw, err := Messages{}.Messages([]aggregate.Message{
		{Role: aggregate.RoleSystem, Content: "be nice"},
		{Role: aggregate.RoleUser, Content: "hi"},
		{Role: aggregate.RoleAssistant, Content: "Checking", ToolCalls: []aggregate.ToolCall{
			{ID: "toolu_1", Name: "get_weather", Args: json.RawMessage(`{"location":"Paris"}`)},
			{ID: "toolu_2", Name: "get_time", Args: json.RawMessage(`{"tz":"UTC"}`)},
		}},
		{Role: aggregate.RoleTool, ToolCallID: "toolu_1", Name: "get_weather", Content: "sun"},
		{Role: aggregate.RoleTool, ToolCallID: "toolu_2", Name: "get_time", Content: "noon"},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"system":"be nice","messages":[{"content":"hi","role":"user"},{"content":[{"text":"Checking","type":"text"},{"id":"toolu_1","input":{"location":"Paris"},"name":"get_weather","type":"tool_use"},{"id":"toolu_2","input":{"tz":"UTC"},"name":"get_time","type":"tool_use"}],"role":"assistant"},{"content":[{"content":"sun","tool_use_id":"toolu_1","type":"tool_result"},{"content":"noon","tool_use_id":"toolu_2","type":"tool_result"}],"role":"user"}]}`
	if string(b) != want {
		t.Fatalf("prompt = %s", b)
	}
	toolsRaw, err := Messages{}.Tools(tools.Schema{Functions: []tools.Function{{
		Name: "get_weather", Description: "Weather",
		Parameters: map[string]any{"type": "object", "properties": map[string]any{}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err = json.Marshal(toolsRaw)
	if err != nil {
		t.Fatal(err)
	}
	const toolsWant = `[{"description":"Weather","input_schema":{"properties":{},"type":"object"},"name":"get_weather"}]`
	if string(b) != toolsWant {
		t.Fatalf("tools = %s", b)
	}
}
