package aws

import (
	"context"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// TTS is a placeholder for Amazon Polly.
type TTS struct {
	name string
}

func NewTTS(name string) *TTS {
	return &TTS{name: name}
}

func (t *TTS) Name() string { return t.name }

func (t *TTS) Process(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
	emit.Down(f)
	return nil
}
