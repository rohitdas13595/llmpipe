// Package aggregate implements LLM context and user/assistant aggregators.
package aggregate

import "sync"

// LLMContext holds the chat transcript for the LLM stage.
// Messages are stored as typed Message values. Snapshot is the deprecated
// map view and stays for one release.
type LLMContext struct {
	mu   sync.Mutex
	msgs []Message
}

func NewLLMContext(systemPrompt string) *LLMContext {
	c := &LLMContext{}
	if systemPrompt != "" {
		c.msgs = append(c.msgs, Message{Role: RoleSystem, Content: systemPrompt})
	}
	return c
}

func (c *LLMContext) AppendUser(text string) {
	c.Append(Message{Role: RoleUser, Content: text})
}

func (c *LLMContext) AppendAssistant(text string) {
	c.Append(Message{Role: RoleAssistant, Content: text})
}

// Append adds one message.
func (c *LLMContext) Append(m Message) {
	if c == nil {
		return
	}
	m.ToolCalls = cloneCalls(m.ToolCalls)
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
}

// AppendToolRound records an assistant message that requested calls, plus one
// empty tool message per call. SetToolResult fills those placeholders.
func (c *LLMContext) AppendToolRound(content string, calls []ToolCall) {
	if c == nil {
		return
	}
	copied := cloneCalls(calls)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, Message{Role: RoleAssistant, Content: content, ToolCalls: copied})
	for _, tc := range copied {
		c.msgs = append(c.msgs, Message{
			Role:       RoleTool,
			ToolCallID: tc.ID,
			Name:       tc.Name,
			Content:    "",
		})
	}
}

// SetToolResult writes the result for a tool message created by AppendToolRound.
// It reports whether a matching tool_call_id was found.
func (c *LLMContext) SetToolResult(id, content string) bool {
	if c == nil || id == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.msgs {
		if c.msgs[i].Role == RoleTool && c.msgs[i].ToolCallID == id {
			c.msgs[i].Content = content
			return true
		}
	}
	return false
}

// Messages returns a copy of the typed transcript.
func (c *LLMContext) Messages() []Message {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Message, len(c.msgs))
	for i, m := range c.msgs {
		m.ToolCalls = cloneCalls(m.ToolCalls)
		out[i] = m
	}
	return out
}

// Snapshot returns a point-in-time OpenAI-style map view.
// Deprecated: use Messages. The map view stays for one release so existing
// callers keep compiling.
func (c *LLMContext) Snapshot() []map[string]any {
	msgs := c.Messages()
	out := make([]map[string]any, len(msgs))
	for i, m := range msgs {
		out[i] = m.Map()
	}
	return out
}
