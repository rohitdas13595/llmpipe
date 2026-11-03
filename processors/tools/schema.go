// Package tools registers provider-neutral functions and runs their handlers.
package tools

import (
	"context"
	"encoding/json"
	"time"
)

// DefaultTimeout is used when Register is called without a positive timeout.
const DefaultTimeout = 10 * time.Second

// Function is the provider-neutral declaration of one callable.
type Function struct {
	Name        string
	Description string
	Parameters  map[string]any
}

// Schema is the set of functions advertised to a model.
// Custom is reserved for provider-specific extensions and is not sent by the
// built-in adapters.
type Schema struct {
	Functions []Function
	Custom    map[string]any
}

// Call is one model-issued invocation.
// Context is the conversation the model was reading, typically *aggregate.LLMContext.
// It is stored as any so this package does not import aggregate (frames already
// reference Schema).
type Call struct {
	ID      string
	Name    string
	Args    json.RawMessage
	Context any
}

// Result is what a handler returns to the model.
// A nil RunLLM means the pipeline should make one follow-up request after the
// rest of the batch finishes. Content "CANCELLED" is reserved for interruption.
type Result struct {
	Content string
	RunLLM  *bool
}

// Handler executes one call. A non-nil error becomes an error result that is
// still sent back to the model. Decode failures should return an error.
type Handler func(ctx context.Context, c Call) (Result, error)

// Options controls one registration.
// When Register is called with no Options, CancelOnInterruption is true and
// Timeout is DefaultTimeout. A provided Options value is used as-is, except a
// non-positive Timeout: zero means DefaultTimeout, and a negative Timeout
// disables the deadline.
type Options struct {
	CancelOnInterruption bool
	Timeout              time.Duration
}

// Bool returns a pointer, for Result.RunLLM.
func Bool(v bool) *bool { return &v }

// NormalizeParameters returns a JSON Schema object for a function with no
// declared parameters.
func NormalizeParameters(p map[string]any) map[string]any {
	if len(p) == 0 {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return p
}
