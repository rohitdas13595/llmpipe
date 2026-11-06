// Package realtime converts tools for the OpenAI Realtime session and the
// function_call_output item the socket expects back.
package realtime

import (
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// Session is the Realtime adapter.
// The live socket does not replay the chat transcript, so Messages returns an
// empty list. Tools are the session.update tool list (name at the top level,
// not nested under function).
type Session struct{}

// Messages returns an empty list. Realtime keeps its own server-side transcript.
func (Session) Messages([]aggregate.Message) (any, error) {
	return []any{}, nil
}

// Tools returns the session.update tools array.
func (Session) Tools(s tools.Schema) (any, error) {
	out := make([]map[string]any, len(s.Functions))
	for i, fn := range s.Functions {
		out[i] = map[string]any{
			"type":        "function",
			"name":        fn.Name,
			"description": fn.Description,
			"parameters":  tools.NormalizeParameters(fn.Parameters),
		}
	}
	return out, nil
}

// FunctionOutput is a conversation.item.create for one function result.
func FunctionOutput(callID, output string) map[string]any {
	return map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type":    "function_call_output",
			"call_id": callID,
			"output":  output,
		},
	}
}

// ResponseCreate asks the model to continue after tool outputs.
func ResponseCreate() map[string]any {
	return map[string]any{"type": "response.create"}
}
