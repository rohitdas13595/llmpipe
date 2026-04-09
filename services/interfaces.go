// Package services defines LLM, STT, and TTS contracts and shared types.
package services

import (
	"context"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// LLM is a language model processor (streaming chat).
type LLM interface {
	processor.Processor
}

// STT is speech-to-text streaming processor.
type STT interface {
	processor.Processor
}

// TTS is text-to-speech streaming processor.
type TTS interface {
	processor.Processor
}

// ReenterFunc injects frames downstream after the named processor finishes async work.
type ReenterFunc func(ctx context.Context, afterProcessorName string, f frames.Frame) error
