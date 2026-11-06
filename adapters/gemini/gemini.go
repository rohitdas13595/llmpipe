// Package gemini converts context and tools for the Google GenAI SDK (v1.52).
// Function declarations use ParametersJsonSchema. Calls and results use
// Part.FunctionCall and Part.FunctionResponse.
package gemini

import (
	"encoding/json"

	"google.golang.org/genai"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// API is the Gemini adapter.
type API struct{}

// Prompt is the system instruction plus the contents array.
type Prompt struct {
	System   *genai.Content   `json:"system,omitempty"`
	Contents []*genai.Content `json:"contents"`
}

// Messages groups tool results into one user turn of FunctionResponse parts.
func (API) Messages(msgs []aggregate.Message) (any, error) {
	var sys []*genai.Part
	var contents []*genai.Content
	var pending []*genai.Part
	flush := func() {
		if len(pending) == 0 {
			return
		}
		contents = append(contents, &genai.Content{Role: string(genai.RoleUser), Parts: pending})
		pending = nil
	}
	for _, m := range msgs {
		switch m.Role {
		case aggregate.RoleSystem:
			if m.Content != "" {
				sys = append(sys, &genai.Part{Text: m.Content})
			}
		case aggregate.RoleTool:
			pending = append(pending, &genai.Part{FunctionResponse: &genai.FunctionResponse{
				ID:       m.ToolCallID,
				Name:     m.Name,
				Response: OutputMap(m.Content),
			}})
		case aggregate.RoleAssistant:
			flush()
			contents = append(contents, &genai.Content{Role: string(genai.RoleModel), Parts: modelParts(m)})
		default:
			flush()
			contents = append(contents, &genai.Content{
				Role:  string(genai.RoleUser),
				Parts: []*genai.Part{{Text: m.Content}},
			})
		}
	}
	flush()
	var system *genai.Content
	if len(sys) > 0 {
		system = &genai.Content{Parts: sys}
	}
	return Prompt{System: system, Contents: contents}, nil
}

func modelParts(m aggregate.Message) []*genai.Part {
	var parts []*genai.Part
	if m.Content != "" || len(m.ToolCalls) == 0 {
		parts = append(parts, &genai.Part{Text: m.Content})
	}
	for _, tc := range m.ToolCalls {
		args := map[string]any{}
		if len(tc.Args) > 0 {
			_ = json.Unmarshal(tc.Args, &args)
		}
		parts = append(parts, &genai.Part{FunctionCall: &genai.FunctionCall{
			ID: tc.ID, Name: tc.Name, Args: args,
		}})
	}
	return parts
}

// OutputMap is the FunctionResponse.response object. JSON objects pass through.
// Other strings are wrapped as {"output": ...}.
func OutputMap(content string) map[string]any {
	var m map[string]any
	if json.Unmarshal([]byte(content), &m) == nil && m != nil {
		return m
	}
	if content == "CANCELLED" {
		return map[string]any{"error": "CANCELLED"}
	}
	return map[string]any{"output": content}
}

// Tools returns one Tool whose FunctionDeclarations use ParametersJsonSchema.
// The schema is a sanitized subset: draft keywords Gemini rejects are removed.
func (API) Tools(s tools.Schema) (any, error) {
	if len(s.Functions) == 0 {
		return []*genai.Tool{}, nil
	}
	decls := make([]*genai.FunctionDeclaration, len(s.Functions))
	for i, fn := range s.Functions {
		decls[i] = &genai.FunctionDeclaration{
			Name:                 fn.Name,
			Description:          fn.Description,
			ParametersJsonSchema: Sanitize(tools.NormalizeParameters(fn.Parameters)),
		}
	}
	return []*genai.Tool{{FunctionDeclarations: decls}}, nil
}

// Sanitize drops JSON Schema keywords that are not in the subset accepted by
// ParametersJsonSchema.
func Sanitize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			switch k {
			case "$schema", "$id", "$defs", "definitions", "$comment", "additionalProperties", "$ref":
				continue
			default:
				out[k] = Sanitize(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = Sanitize(t[i])
		}
		return out
	default:
		return v
	}
}
