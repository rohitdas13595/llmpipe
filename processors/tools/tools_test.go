package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type weatherArgs struct {
	Location string `json:"location" jsonschema:"description=City or place name"`
}

func TestTypedSchemaSnapshot(t *testing.T) {
	fn, h := Typed("get_weather", "Current weather for a place.", func(ctx context.Context, a weatherArgs) (any, error) {
		return map[string]string{"location": a.Location}, nil
	})
	if fn.Name != "get_weather" || fn.Description == "" {
		t.Fatalf("function = %#v", fn)
	}
	b, err := json.Marshal(fn.Parameters)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"additionalProperties":false,"properties":{"location":{"description":"City or place name","type":"string"}},"required":["location"],"type":"object"}`
	if string(b) != want {
		t.Fatalf("schema = %s", b)
	}
	res, err := h(context.Background(), Call{Args: []byte(`{"location":"Paris"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Content, "Paris") {
		t.Fatalf("content = %s", res.Content)
	}
	if _, err := h(context.Background(), Call{Args: []byte(`{`)}); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterDefaultsAndSchemaOrder(t *testing.T) {
	r := NewRegistry()
	a, _ := Typed("a", "first", func(ctx context.Context, in struct{}) (any, error) { return "a", nil })
	b := Function{Name: "b", Description: "second"}
	r.Register(a, func(ctx context.Context, c Call) (Result, error) { return Result{Content: "a"}, nil })
	r.Register(b, func(ctx context.Context, c Call) (Result, error) { return Result{Content: "b"}, nil }, Options{CancelOnInterruption: false})
	s := r.Schema()
	if len(s.Functions) != 2 || s.Functions[0].Name != "a" || s.Functions[1].Name != "b" {
		t.Fatalf("schema = %#v", s.Functions)
	}
	_, _, opt, ok := r.Lookup("a")
	if !ok || !opt.CancelOnInterruption || opt.Timeout != DefaultTimeout {
		t.Fatalf("default options = %#v", opt)
	}
	_, _, opt, ok = r.Lookup("b")
	if !ok || opt.CancelOnInterruption || opt.Timeout != DefaultTimeout {
		t.Fatalf("explicit options = %#v", opt)
	}
}
