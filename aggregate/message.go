package aggregate

import "encoding/json"

// Role is the author of a context message.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolCall is one function call attached to an assistant message.
type ToolCall struct {
	ID   string
	Name string
	Args json.RawMessage
}

// Message is one turn of LLM context. Tool fields are set only for the roles
// that use them: ToolCalls on an assistant message, ToolCallID and Name on a
// tool result.
type Message struct {
	Role       Role
	Content    string
	ToolCalls  []ToolCall
	ToolCallID string
	Name       string
}

// Map is the OpenAI Chat Completions view of m.
// Snapshot keeps returning this shape for one release.
func (m Message) Map() map[string]any {
	out := map[string]any{"role": string(m.Role)}
	if m.Role == RoleAssistant && len(m.ToolCalls) > 0 {
		if m.Content == "" {
			out["content"] = nil
		} else {
			out["content"] = m.Content
		}
		calls := make([]any, len(m.ToolCalls))
		for i, tc := range m.ToolCalls {
			args := string(tc.Args)
			if args == "" {
				args = "{}"
			}
			calls[i] = map[string]any{
				"id":   tc.ID,
				"type": "function",
				"function": map[string]any{
					"name":      tc.Name,
					"arguments": args,
				},
			}
		}
		out["tool_calls"] = calls
		return out
	}
	out["content"] = m.Content
	if m.ToolCallID != "" {
		out["tool_call_id"] = m.ToolCallID
	}
	if m.Name != "" {
		out["name"] = m.Name
	}
	return out
}

func cloneRaw(b json.RawMessage) json.RawMessage {
	if b == nil {
		return nil
	}
	out := make(json.RawMessage, len(b))
	copy(out, b)
	return out
}

func cloneCalls(in []ToolCall) []ToolCall {
	if len(in) == 0 {
		return nil
	}
	out := make([]ToolCall, len(in))
	for i, tc := range in {
		out[i] = ToolCall{ID: tc.ID, Name: tc.Name, Args: cloneRaw(tc.Args)}
	}
	return out
}
