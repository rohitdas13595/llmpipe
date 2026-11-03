// Package frames defines frame types for llmpipe pipelines.
package frames

import "time"

// Frame is a marker interface for all frames.
type Frame interface {
	FrameKind() string
}

const (
	KindStart                  = "Start"
	KindEnd                    = "End"
	KindCancel                 = "Cancel"
	KindError                  = "Error"
	KindFatalError             = "FatalError"
	KindInputAudio             = "InputAudio"
	KindTTSAudio               = "TTSAudio"
	KindTranscription          = "Transcription"
	KindInterimTranscription   = "InterimTranscription"
	KindText                   = "Text"
	KindLLMText                = "LLMText"
	KindLLMRun                 = "LLMRun"
	KindLLMMessagesAppend      = "LLMMessagesAppend"
	KindInterruption           = "Interruption"
	KindInterruptionTask       = "InterruptionTask"
	KindStop                   = "Stop"
	KindHeartbeat              = "Heartbeat"
	KindUserStartedSpeaking    = "UserStartedSpeaking"
	KindUserStoppedSpeaking    = "UserStoppedSpeaking"
	KindVADUserStartedSpeaking = "VADUserStartedSpeaking"
	KindVADUserStoppedSpeaking = "VADUserStoppedSpeaking"
	KindBotStartedSpeaking     = "BotStartedSpeaking"
	KindBotStoppedSpeaking     = "BotStoppedSpeaking"
	KindLLMFullResponseStart   = "LLMFullResponseStart"
	KindLLMFullResponseEnd     = "LLMFullResponseEnd"
	KindLLMSetTools            = "LLMSetTools"
	KindFunctionCallResult     = "FunctionCallResult"
	KindTurnStarted            = "TurnStarted"
	KindTurnEnded              = "TurnEnded"
	KindTransportMessage       = "TransportMessage"
	KindTTSStarted             = "TTSStarted"
	KindTTSStopped             = "TTSStopped"
	KindTTSText                = "TTSText"
	KindOutputTransportMessage = "OutputTransportMessage"
	KindOutputTransportUrgent  = "OutputTransportMessageUrgent"
)

// TransportClearPayload is the client message that drops queued playback.
const TransportClearPayload = `{"type":"clear"}`

type StartFrame struct {
	SampleRate  int
	NumChannels int
	System
	Meta
}

func (f *StartFrame) FrameKind() string { return KindStart }

type EndFrame struct {
	Control
	Uninterruptible
	Meta
}

func (f *EndFrame) FrameKind() string { return KindEnd }

type CancelFrame struct {
	System
	Meta
}

func (f *CancelFrame) FrameKind() string { return KindCancel }

type ErrorFrame struct {
	Err error
	System
	Meta
}

func (f *ErrorFrame) FrameKind() string { return KindError }

type FatalErrorFrame struct {
	Err error
	System
	Meta
}

func (f *FatalErrorFrame) FrameKind() string { return KindFatalError }

type InputAudioRawFrame struct {
	Audio       []byte
	SampleRate  int
	NumChannels int
	System
	Meta
}

func (f *InputAudioRawFrame) FrameKind() string { return KindInputAudio }

type TTSAudioRawFrame struct {
	Audio       []byte
	SampleRate  int
	NumChannels int
	Meta
}

func (f *TTSAudioRawFrame) FrameKind() string { return KindTTSAudio }

type TranscriptionFrame struct {
	Text string
	// Final is set when the recognizer will not revise Text.
	Final bool
	// Language is a BCP-47 tag when the recognizer reported one.
	Language string
	// Timestamp is when this transcript was produced.
	Timestamp time.Time
	Meta
}

func (f *TranscriptionFrame) FrameKind() string { return KindTranscription }

type InterimTranscriptionFrame struct {
	Text string
	Meta
}

func (f *InterimTranscriptionFrame) FrameKind() string { return KindInterimTranscription }

type TextFrame struct {
	Text string
	Meta
}

func (f *TextFrame) FrameKind() string { return KindText }

type LLMTextFrame struct {
	Text string
	Meta
}

func (f *LLMTextFrame) FrameKind() string { return KindLLMText }

type LLMRunFrame struct {
	Control
	Meta
}

func (f *LLMRunFrame) FrameKind() string { return KindLLMRun }

type LLMMessagesAppendFrame struct {
	Messages []map[string]any
	Meta
}

func (f *LLMMessagesAppendFrame) FrameKind() string { return KindLLMMessagesAppend }

type InterruptionFrame struct {
	System
	Meta
}

func (f *InterruptionFrame) FrameKind() string { return KindInterruption }

// InterruptionTaskFrame is pushed upstream to request a pipeline-wide
// interruption. The source turns it into one downstream InterruptionFrame
// that every node observes. Outputs clear buffered audio when they see it.
type InterruptionTaskFrame struct {
	System
	Meta
}

func (f *InterruptionTaskFrame) FrameKind() string { return KindInterruptionTask }

// StopFrame asks the task to wind down. It is system priority so it is not
// stuck behind a full data lane.
type StopFrame struct {
	System
	Meta
}

func (f *StopFrame) FrameKind() string { return KindStop }

// HeartbeatFrame is a system keepalive. Observers and idle timers may use it;
// processors that do not care should forward it.
type HeartbeatFrame struct {
	System
	Meta
}

func (f *HeartbeatFrame) FrameKind() string { return KindHeartbeat }

type UserStartedSpeakingFrame struct {
	System
	Meta
}

func (f *UserStartedSpeakingFrame) FrameKind() string { return KindUserStartedSpeaking }

type UserStoppedSpeakingFrame struct {
	System
	Meta
}

func (f *UserStoppedSpeakingFrame) FrameKind() string { return KindUserStoppedSpeaking }

type VADUserStartedSpeakingFrame struct {
	System
	Meta
}

func (f *VADUserStartedSpeakingFrame) FrameKind() string { return KindVADUserStartedSpeaking }

type VADUserStoppedSpeakingFrame struct {
	System
	Meta
}

func (f *VADUserStoppedSpeakingFrame) FrameKind() string { return KindVADUserStoppedSpeaking }

type BotStartedSpeakingFrame struct {
	System
	Meta
}

func (f *BotStartedSpeakingFrame) FrameKind() string { return KindBotStartedSpeaking }

type BotStoppedSpeakingFrame struct {
	// PlayedMS is how many milliseconds of audio the listener had heard when
	// playback stopped. Outputs set it; realtime services use it to truncate.
	PlayedMS int
	// Interrupted is set when playback stopped because of a barge-in.
	Interrupted bool
	System
	Meta
}

func (f *BotStoppedSpeakingFrame) FrameKind() string { return KindBotStoppedSpeaking }

type LLMFullResponseStartFrame struct {
	Control
	Meta
}

func (f *LLMFullResponseStartFrame) FrameKind() string { return KindLLMFullResponseStart }

type LLMFullResponseEndFrame struct {
	Control
	Meta
}

func (f *LLMFullResponseEndFrame) FrameKind() string { return KindLLMFullResponseEnd }

type LLMSetToolsFrame struct {
	Tools any // schema per provider
	Control
	Meta
}

func (f *LLMSetToolsFrame) FrameKind() string { return KindLLMSetTools }

type FunctionCallResultFrame struct {
	CallID string
	Result string
	Uninterruptible
	Meta
}

func (f *FunctionCallResultFrame) FrameKind() string { return KindFunctionCallResult }

// TurnStartedFrame marks the start of a user speaking turn (Pipecat turn observer).
type TurnStartedFrame struct {
	Index int
	System
	Meta
}

func (f *TurnStartedFrame) FrameKind() string { return KindTurnStarted }

// TurnEndedFrame marks the end of a user turn after UserStoppedSpeaking or an interruption.
type TurnEndedFrame struct {
	Index    int
	Complete bool // false if interrupted before a full stop
	System
	Meta
}

func (f *TurnEndedFrame) FrameKind() string { return KindTurnEnded }

// TransportMessageFrame carries JSON from Pipecat protobuf MessageFrame / transport control paths.
type TransportMessageFrame struct {
	Data string
	Meta
}

func (f *TransportMessageFrame) FrameKind() string { return KindTransportMessage }
