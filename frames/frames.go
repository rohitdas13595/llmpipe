// Package frames defines frame types for llmpipe pipelines.
package frames

// Frame is a marker interface for all frames.
type Frame interface {
	FrameKind() string
}

const (
	KindStart                    = "Start"
	KindEnd                      = "End"
	KindCancel                   = "Cancel"
	KindError                    = "Error"
	KindInputAudio               = "InputAudio"
	KindTTSAudio                 = "TTSAudio"
	KindTranscription            = "Transcription"
	KindInterimTranscription     = "InterimTranscription"
	KindText                     = "Text"
	KindLLMText                  = "LLMText"
	KindLLMRun                   = "LLMRun"
	KindLLMMessagesAppend        = "LLMMessagesAppend"
	KindInterruption             = "Interruption"
	KindUserStartedSpeaking      = "UserStartedSpeaking"
	KindUserStoppedSpeaking      = "UserStoppedSpeaking"
	KindVADUserStartedSpeaking   = "VADUserStartedSpeaking"
	KindVADUserStoppedSpeaking   = "VADUserStoppedSpeaking"
	KindBotStartedSpeaking       = "BotStartedSpeaking"
	KindBotStoppedSpeaking       = "BotStoppedSpeaking"
	KindLLMFullResponseStart     = "LLMFullResponseStart"
	KindLLMFullResponseEnd       = "LLMFullResponseEnd"
	KindLLMSetTools              = "LLMSetTools"
	KindFunctionCallResult       = "FunctionCallResult"
)

type StartFrame struct {
	SampleRate  int
	NumChannels int
}

func (f *StartFrame) FrameKind() string { return KindStart }

type EndFrame struct{}

func (f *EndFrame) FrameKind() string { return KindEnd }

type CancelFrame struct{}

func (f *CancelFrame) FrameKind() string { return KindCancel }

type ErrorFrame struct {
	Err error
}

func (f *ErrorFrame) FrameKind() string { return KindError }

type FatalErrorFrame struct {
	Err error
}

func (f *FatalErrorFrame) FrameKind() string { return KindError }

type InputAudioRawFrame struct {
	Audio       []byte
	SampleRate  int
	NumChannels int
}

func (f *InputAudioRawFrame) FrameKind() string { return KindInputAudio }

type TTSAudioRawFrame struct {
	Audio       []byte
	SampleRate  int
	NumChannels int
}

func (f *TTSAudioRawFrame) FrameKind() string { return KindTTSAudio }

type TranscriptionFrame struct {
	Text string
}

func (f *TranscriptionFrame) FrameKind() string { return KindTranscription }

type InterimTranscriptionFrame struct {
	Text string
}

func (f *InterimTranscriptionFrame) FrameKind() string { return KindInterimTranscription }

type TextFrame struct {
	Text string
}

func (f *TextFrame) FrameKind() string { return KindText }

type LLMTextFrame struct {
	Text string
}

func (f *LLMTextFrame) FrameKind() string { return KindLLMText }

type LLMRunFrame struct{}

func (f *LLMRunFrame) FrameKind() string { return KindLLMRun }

type LLMMessagesAppendFrame struct {
	Messages []map[string]any
}

func (f *LLMMessagesAppendFrame) FrameKind() string { return KindLLMMessagesAppend }

type InterruptionFrame struct{}

func (f *InterruptionFrame) FrameKind() string { return KindInterruption }

type UserStartedSpeakingFrame struct{}

func (f *UserStartedSpeakingFrame) FrameKind() string { return KindUserStartedSpeaking }

type UserStoppedSpeakingFrame struct{}

func (f *UserStoppedSpeakingFrame) FrameKind() string { return KindUserStoppedSpeaking }

type VADUserStartedSpeakingFrame struct{}

func (f *VADUserStartedSpeakingFrame) FrameKind() string { return KindVADUserStartedSpeaking }

type VADUserStoppedSpeakingFrame struct{}

func (f *VADUserStoppedSpeakingFrame) FrameKind() string { return KindVADUserStoppedSpeaking }

type BotStartedSpeakingFrame struct{}

func (f *BotStartedSpeakingFrame) FrameKind() string { return KindBotStartedSpeaking }

type BotStoppedSpeakingFrame struct{}

func (f *BotStoppedSpeakingFrame) FrameKind() string { return KindBotStoppedSpeaking }

type LLMFullResponseStartFrame struct{}

func (f *LLMFullResponseStartFrame) FrameKind() string { return KindLLMFullResponseStart }

type LLMFullResponseEndFrame struct{}

func (f *LLMFullResponseEndFrame) FrameKind() string { return KindLLMFullResponseEnd }

type LLMSetToolsFrame struct {
	Tools any // schema per provider
}

func (f *LLMSetToolsFrame) FrameKind() string { return KindLLMSetTools }

type FunctionCallResultFrame struct {
	CallID string
	Result string
}

func (f *FunctionCallResultFrame) FrameKind() string { return KindFunctionCallResult }
