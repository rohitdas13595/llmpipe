// Package aggregate implements LLM context and user/assistant aggregators.
package aggregate

import "sync"

// LLMContext holds chat messages for the LLM stage.
type LLMContext struct {
	mu       sync.Mutex
	Messages []map[string]any
}

func NewLLMContext(systemPrompt string) *LLMContext {
	c := &LLMContext{Messages: make([]map[string]any, 0)}
	if systemPrompt != "" {
		c.Messages = append(c.Messages, map[string]any{
			"role":    "system",
			"content": systemPrompt,
		})
	}
	return c
}

func (c *LLMContext) AppendUser(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Messages = append(c.Messages, map[string]any{
		"role":    "user",
		"content": text,
	})
}

func (c *LLMContext) AppendAssistant(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Messages = append(c.Messages, map[string]any{
		"role":    "assistant",
		"content": text,
	})
}

func (c *LLMContext) Snapshot() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]map[string]any, len(c.Messages))
	copy(out, c.Messages)
	return out
}
