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
//
// Retained until v0.4. New code should use processor.Handle from Setup
// (PushFrame / PushFrameCtx). Constructors still accept this so existing
// callers compile; a live Handle takes precedence and this callback is not used.
type ReenterFunc func(ctx context.Context, afterProcessorName string, f frames.Frame) error
