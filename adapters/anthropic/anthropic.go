// Package anthropic converts context and tools for the Claude Messages API.
package anthropic

import (
	"encoding/json"
	"strings"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// Messages is the Claude adapter.
type Messages struct{}

// Prompt is the system string plus the messages array.
type Prompt struct {
	System   string           `json:"system,omitempty"`
	Messages []map[string]any `json:"messages"`
}

// Messages splits system text out and groups tool results into one user turn.
func (Messages) Messages(msgs []aggregate.Message) (any, error) {
	var sys strings.Builder
	var out []map[string]any
	var pending []any
	flush := func() {
		if len(pending) == 0 {
			return
		}
		out = append(out, map[string]any{"role": "user", "content": pending})
		pending = nil
	}
	for _, m := range msgs {
		switch m.Role {
		case aggregate.RoleSystem:
			if sys.Len() > 0 {
				sys.WriteByte('\n')
			}
			sys.WriteString(m.Content)
		case aggregate.RoleTool:
			pending = append(pending, map[string]any{
				"type":        "tool_result",
				"tool_use_id": m.ToolCallID,
				"content":     m.Content,
			})
		case aggregate.RoleAssistant:
			flush()
			out = append(out, assistantWire(m))
		default:
			flush()
			out = append(out, map[string]any{"role": "user", "content": m.Content})
		}
	}
	flush()
	if out == nil {
		out = []map[string]any{}
	}
	return Prompt{System: sys.String(), Messages: out}, nil
}

func assistantWire(m aggregate.Message) map[string]any {
	if len(m.ToolCalls) == 0 {
		return map[string]any{"role": "assistant", "content": m.Content}
	}
	blocks := make([]any, 0, 1+len(m.ToolCalls))
	if m.Content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": m.Content})
	}
	for _, tc := range m.ToolCalls {
		blocks = append(blocks, map[string]any{
			"type":  "tool_use",
			"id":    tc.ID,
			"name":  tc.Name,
			"input": object(tc.Args),
		})
	}
	return map[string]any{"role": "assistant", "content": blocks}
}

func object(raw json.RawMessage) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil || v == nil {
		return map[string]any{}
	}
	if _, ok := v.(map[string]any); !ok {
		return map[string]any{"value": v}
	}
	return v
}

// Tools returns tools[{name,description,input_schema}].
func (Messages) Tools(s tools.Schema) (any, error) {
	out := make([]map[string]any, len(s.Functions))
	for i, fn := range s.Functions {
		out[i] = map[string]any{
			"name":         fn.Name,
			"description":  fn.Description,
			"input_schema": tools.NormalizeParameters(fn.Parameters),
		}
	}
	return out, nil
}
