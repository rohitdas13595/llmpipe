package gemini

import (
	"encoding/json"
	"testing"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestPromptGolden(t *testing.T) {
	raw, err := API{}.Messages([]aggregate.Message{
		{Role: aggregate.RoleSystem, Content: "be nice"},
		{Role: aggregate.RoleUser, Content: "hi"},
		{Role: aggregate.RoleAssistant, Content: "Checking", ToolCalls: []aggregate.ToolCall{{
			ID: "fn_1", Name: "get_weather", Args: json.RawMessage(`{"location":"Paris"}`),
		}}},
		{Role: aggregate.RoleTool, ToolCallID: "fn_1", Name: "get_weather", Content: `{"condition":"sun"}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"system":{"parts":[{"text":"be nice"}]},"contents":[{"parts":[{"text":"hi"}],"role":"user"},{"parts":[{"text":"Checking"},{"functionCall":{"id":"fn_1","args":{"location":"Paris"},"name":"get_weather"}}],"role":"model"},{"parts":[{"functionResponse":{"id":"fn_1","name":"get_weather","response":{"condition":"sun"}}}],"role":"user"}]}`
	if string(b) != want {
		t.Fatalf("prompt = %s", b)
	}
	toolRaw, err := API{}.Tools(tools.Schema{Functions: []tools.Function{{
		Name: "get_weather", Description: "Weather",
		Parameters: map[string]any{
			"$schema": "https://json-schema.org/draft/2020-12/schema",
			"type":    "object",
			"properties": map[string]any{
				"location": map[string]any{"type": "string"},
			},
			"additionalProperties": false,
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err = json.Marshal(toolRaw)
	if err != nil {
		t.Fatal(err)
	}
	const toolsWant = `[{"functionDeclarations":[{"description":"Weather","name":"get_weather","parametersJsonSchema":{"properties":{"location":{"type":"string"}},"type":"object"}}]}]`
	if string(b) != toolsWant {
		t.Fatalf("tools = %s", b)
	}
}
