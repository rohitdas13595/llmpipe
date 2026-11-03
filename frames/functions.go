package frames

import (
	"encoding/json"

	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// FunctionCallsStartedFrame announces a batch of model-issued calls.
// It is a system frame so mute strategies see it before queued audio, and
// Calls reports the batch size for turns.FunctionCallMute.
type FunctionCallsStartedFrame struct {
	Content string
	Batch   []tools.Call
	System
	Meta
}

func (f *FunctionCallsStartedFrame) FrameKind() string { return KindFunctionCallsStarted }

// Calls is the number of calls in this batch. Mute strategies count one
// started frame as this many in-flight calls.
func (f *FunctionCallsStartedFrame) Calls() int {
	if f == nil {
		return 0
	}
	return len(f.Batch)
}

// FunctionCallInProgressFrame is one call whose handler has been started.
// It is control traffic and survives an interruption drain.
type FunctionCallInProgressFrame struct {
	CallID string
	Name   string
	Args   json.RawMessage
	Control
	Uninterruptible
	Meta
}

func (f *FunctionCallInProgressFrame) FrameKind() string { return KindFunctionCallInProgress }

// FunctionCallCancelFrame marks one call cancelled by an interruption.
// Mute strategies decrement on this frame. The matching result travels
// downstream only, so a single call is not counted twice.
type FunctionCallCancelFrame struct {
	CallID string
	System
	Meta
}

func (f *FunctionCallCancelFrame) FrameKind() string { return KindFunctionCallCancel }
