package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/invopop/jsonschema"
)

// Typed builds a Function whose parameters are the JSON Schema of T, and a
// Handler that decodes the model arguments into T. A decode error is returned
// to the caller; the runner turns it into an error result.
func Typed[T any](name, desc string, fn func(ctx context.Context, args T) (any, error)) (Function, Handler) {
	h := func(ctx context.Context, c Call) (Result, error) {
		var args T
		raw := c.Args
		if len(bytesTrim(raw)) == 0 {
			raw = []byte("{}")
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return Result{}, fmt.Errorf("decode %s: %w", name, err)
		}
		out, err := fn(ctx, args)
		if err != nil {
			return Result{}, err
		}
		b, err := json.Marshal(out)
		if err != nil {
			return Result{}, err
		}
		return Result{Content: string(b)}, nil
	}
	return Function{Name: name, Description: desc, Parameters: schemaOf[T]()}, h
}

func bytesTrim(b []byte) []byte {
	i := 0
	j := len(b)
	for i < j && (b[i] == ' ' || b[i] == '\n' || b[i] == '\t' || b[i] == '\r') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\n' || b[j-1] == '\t' || b[j-1] == '\r') {
		j--
	}
	return b[i:j]
}

func schemaOf[T any]() (out map[string]any) {
	defer func() {
		if recover() != nil {
			out = map[string]any{"type": "object", "properties": map[string]any{}}
		}
	}()
	t := reflect.TypeOf((*T)(nil)).Elem()
	r := &jsonschema.Reflector{
		Anonymous:      true,
		DoNotReference: true,
		ExpandedStruct: t.Name() != "",
	}
	s := r.ReflectFromType(t)
	b, err := json.Marshal(s)
	if err != nil || len(b) == 0 {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	var m map[string]any
	if json.Unmarshal(b, &m) != nil {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	for _, k := range []string{"$schema", "$id", "$defs", "definitions"} {
		delete(m, k)
	}
	return m
}
