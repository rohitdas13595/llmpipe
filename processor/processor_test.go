package processor

import (
	"context"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
)

func TestFuncName(t *testing.T) {
	fn := Func{N: "x", F: func(ctx context.Context, f frames.Frame, dir Direction, emit Emit) error {
		return nil
	}}
	if fn.Name() != "x" {
		t.Fatalf("Name = %q", fn.Name())
	}
}

func TestFuncProcessDelegates(t *testing.T) {
	var saw frames.Frame
	fn := Func{
		N: "fn",
		F: func(ctx context.Context, f frames.Frame, dir Direction, emit Emit) error {
			saw = f
			return nil
		},
	}
	tf := &frames.TextFrame{Text: "ok"}
	if err := fn.Process(context.Background(), tf, Downstream, Emit{}); err != nil {
		t.Fatal(err)
	}
	if saw != tf {
		t.Fatal("delegate did not receive frame")
	}
}
