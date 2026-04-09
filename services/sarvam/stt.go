package sarvam

import (
	"context"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
)

// STT is a placeholder for Sarvam streaming STT.
type STT struct {
	name    string
	Reenter services.ReenterFunc
}

func NewSTT(name string, reenter services.ReenterFunc) *STT {
	return &STT{name: name, Reenter: reenter}
}

func (s *STT) Name() string { return s.name }

func (s *STT) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	emit.Down(f)
	return nil
}
