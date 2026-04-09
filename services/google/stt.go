package google

import (
	"context"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// STT is a placeholder for Google Cloud Speech-to-Text streaming; wire the REST/gRPC
// client here without changing the pipeline graph.
type STT struct {
	name    string
	Reenter services.ReenterFunc
}

func NewSTT(name string, reenter services.ReenterFunc) *STT {
	return &STT{name: name, Reenter: reenter}
}

func (s *STT) Name() string { return s.name }

func (s *STT) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	// MVP: pass frames through; implement Speech-to-Text v1 streaming.
	emit.Down(f)
	return nil
}
