// Package openai converts context and tools for the Chat Completions API
// (OpenAI, Groq, Azure, and the other compatible backends).
package openai

import (
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// Chat is the Chat Completions adapter.
type Chat struct{}

// Messages returns the OpenAI messages array, including tool_calls and tool results.
func (Chat) Messages(msgs []aggregate.Message) (any, error) {
	out := make([]map[string]any, len(msgs))
	for i, m := range msgs {
		out[i] = m.Map()
	}
	return out, nil
}

// Tools returns tools[{type:function, function:{name,description,parameters}}].
func (Chat) Tools(s tools.Schema) (any, error) {
	out := make([]map[string]any, len(s.Functions))
	for i, fn := range s.Functions {
		out[i] = map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        fn.Name,
				"description": fn.Description,
				"parameters":  tools.NormalizeParameters(fn.Parameters),
			},
		}
	}
	return out, nil
}
