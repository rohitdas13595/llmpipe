package frames

import "testing"

func TestCategories(t *testing.T) {
	system := []Frame{
		&StartFrame{}, &CancelFrame{}, &ErrorFrame{}, &FatalErrorFrame{},
		&InputAudioRawFrame{}, &InterruptionFrame{}, &InterruptionTaskFrame{},
		&StopFrame{}, &HeartbeatFrame{},
		&UserStartedSpeakingFrame{}, &UserStoppedSpeakingFrame{},
		&VADUserStartedSpeakingFrame{}, &VADUserStoppedSpeakingFrame{},
		&BotStartedSpeakingFrame{}, &BotStoppedSpeakingFrame{}, &BotSpeakingFrame{},
		&TurnStartedFrame{}, &TurnEndedFrame{},
		&TTSTextFrame{}, &OutputTransportMessageUrgentFrame{},
	}
	for _, f := range system {
		if !IsSystem(f) || Interruptible(f) {
			t.Fatalf("%T: want system, not interruptible", f)
		}
	}
	control := []Frame{
		&EndFrame{}, &LLMFullResponseStartFrame{}, &LLMFullResponseEndFrame{},
		&LLMSetToolsFrame{}, &LLMRunFrame{},
		&TTSStartedFrame{}, &TTSStoppedFrame{},
	}
	for _, f := range control {
		if !IsControl(f) || IsSystem(f) {
			t.Fatalf("%T: want control", f)
		}
	}
	if Interruptible(&EndFrame{}) {
		t.Fatal("End is uninterruptible")
	}
	if Interruptible(&FunctionCallResultFrame{}) {
		t.Fatal("function result is uninterruptible")
	}
	data := []Frame{
		&TextFrame{}, &LLMTextFrame{}, &TTSAudioRawFrame{},
		&TranscriptionFrame{}, &InterimTranscriptionFrame{},
		&LLMMessagesAppendFrame{}, &TransportMessageFrame{},
		&OutputTransportMessageFrame{},
	}
	for _, f := range data {
		if IsSystem(f) || IsControl(f) || !Interruptible(f) {
			t.Fatalf("%T: want interruptible data", f)
		}
	}
	if Interruptible(nil) {
		t.Fatal("nil is not interruptible")
	}
}

func TestEnsureMetaLazy(t *testing.T) {
	a := &TextFrame{Text: "a"}
	b := &TextFrame{Text: "b"}
	if MetaID(a) != 0 || MetaID(b) != 0 {
		t.Fatal("id assigned before EnsureMeta")
	}
	EnsureMeta(a)
	EnsureMeta(a)
	EnsureMeta(b)
	if MetaID(a) == 0 || MetaID(b) == 0 || MetaID(a) == MetaID(b) {
		t.Fatalf("ids a=%d b=%d", MetaID(a), MetaID(b))
	}
	if a.CreatedAt.IsZero() {
		t.Fatal("CreatedAt not set")
	}
}
