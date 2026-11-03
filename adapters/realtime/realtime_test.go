package realtime

import (
	"encoding/json"
	"testing"

	"github.com/rohitdas13595/llmpipe/processors/tools"
)

func TestSessionToolsGolden(t *testing.T) {
	raw, err := Session{}.Tools(tools.Schema{Functions: []tools.Function{{
		Name: "get_weather", Description: "Weather",
		Parameters: map[string]any{"type": "object", "properties": map[string]any{}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	const want = `[{"description":"Weather","name":"get_weather","parameters":{"properties":{},"type":"object"},"type":"function"}]`
	if string(b) != want {
		t.Fatalf("tools = %s", b)
	}
	item := FunctionOutput("call_1", `{"condition":"sun"}`)
	b, err = json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	const itemWant = `{"item":{"call_id":"call_1","output":"{\"condition\":\"sun\"}","type":"function_call_output"},"type":"conversation.item.create"}`
	if string(b) != itemWant {
		t.Fatalf("item = %s", b)
	}
}
