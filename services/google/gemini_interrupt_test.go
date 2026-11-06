package google

import (
	"context"
	"log/slog"
	"testing"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

type recHandle struct {
	f   frames.Frame
	dir processor.Direction
}

func (h *recHandle) PushFrame(f frames.Frame, dir processor.Direction) error {
	h.f = f
	h.dir = dir
	return nil
}
func (h *recHandle) PushFrameCtx(_ context.Context, f frames.Frame, dir processor.Direction) error {
	return h.PushFrame(f, dir)
}
func (h *recHandle) QueueSelf(frames.Frame) error { return nil }
func (h *recHandle) TurnContext() context.Context { return context.Background() }
func (h *recHandle) Context() context.Context     { return context.Background() }
func (h *recHandle) Name() string                 { return "rec" }
func (h *recHandle) Logger() *slog.Logger         { return slog.Default() }

func TestGeminiInterruptionIsUpstreamTask(t *testing.T) {
	g := NewGeminiLive("g", nil, "m", "", nil, nil)
	h := &recHandle{}
	if err := g.Setup(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	g.signalInterruption(context.Background())
	if _, ok := h.f.(*frames.InterruptionTaskFrame); !ok {
		t.Fatalf("frame %T", h.f)
	}
	if h.dir != processor.Upstream {
		t.Fatalf("dir %v", h.dir)
	}
}
