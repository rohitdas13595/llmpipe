package turn

import "context"

// Result is the end-of-turn decision. It is the same type as State.
type Result = State

const (
	Incomplete = StateIncomplete
	Complete   = StateComplete
)

// Analyzer buffers user audio and decides when the turn is finished.
// AppendAudio is synchronous. AnalyzeEndOfTurn may run a model and is safe
// to call off the processor goroutine. Clear drops the buffer.
type Analyzer interface {
	AppendAudio(pcm []byte, sampleRate int, isSpeech bool) Result
	AnalyzeEndOfTurn(ctx context.Context) (Result, float32, error)
	Clear()
}
