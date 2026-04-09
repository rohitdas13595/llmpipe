package frames

import "testing"

func TestFrameKinds(t *testing.T) {
	tests := []struct {
		f    Frame
		want string
	}{
		{&StartFrame{}, KindStart},
		{&EndFrame{}, KindEnd},
		{&CancelFrame{}, KindCancel},
		{&ErrorFrame{}, KindError},
		{&InputAudioRawFrame{}, KindInputAudio},
		{&TTSAudioRawFrame{}, KindTTSAudio},
		{&TranscriptionFrame{}, KindTranscription},
		{&InterimTranscriptionFrame{}, KindInterimTranscription},
		{&TextFrame{}, KindText},
		{&LLMTextFrame{}, KindLLMText},
		{&LLMRunFrame{}, KindLLMRun},
		{&InterruptionFrame{}, KindInterruption},
		{&UserStartedSpeakingFrame{}, KindUserStartedSpeaking},
		{&BotSpeakingFrame{}, KindBotSpeaking},
		{&LLMFullResponseStartFrame{}, KindLLMFullResponseStart},
		{&LLMFullResponseEndFrame{}, KindLLMFullResponseEnd},
		{&LLMMessagesAppendFrame{}, KindLLMMessagesAppend},
		{&LLMSetToolsFrame{}, KindLLMSetTools},
		{&FunctionCallResultFrame{}, KindFunctionCallResult},
		{&FatalErrorFrame{}, KindError},
		{&VADUserStartedSpeakingFrame{}, KindVADUserStartedSpeaking},
		{&VADUserStoppedSpeakingFrame{}, KindVADUserStoppedSpeaking},
		{&UserStoppedSpeakingFrame{}, KindUserStoppedSpeaking},
		{&BotStartedSpeakingFrame{}, KindBotStartedSpeaking},
		{&BotStoppedSpeakingFrame{}, KindBotStoppedSpeaking},
	}
	for _, tc := range tests {
		if g := tc.f.FrameKind(); g != tc.want {
			t.Fatalf("%T: got %q, want %q", tc.f, g, tc.want)
		}
	}
}
