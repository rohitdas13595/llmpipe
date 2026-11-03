package turns

import (
	"context"
	"testing"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/turn"
	"github.com/rohitdas13595/llmpipe/frames"
)

func TestStrategyMatrix(t *testing.T) {
	ctx := context.Background()
	audio200 := &frames.InputAudioRawFrame{Audio: make([]byte, 16000*200/1000*2), SampleRate: 16000}
	audio100 := &frames.InputAudioRawFrame{Audio: make([]byte, 16000*100/1000*2), SampleRate: 16000}

	t.Run("vad immediate when bot is quiet", func(t *testing.T) {
		s := TurnState{}
		if !((VADStart{MinSpeechMS: 300}).ShouldStart(&frames.VADUserStartedSpeakingFrame{}, &s)) {
			t.Fatal("quiet bot should start on the VAD frame")
		}
	})
	t.Run("vad waits 300ms while bot speaks", func(t *testing.T) {
		s := TurnState{BotSpeaking: true, VADSpeaking: true}
		v := VADStart{MinSpeechMS: 300}
		if v.ShouldStart(&frames.VADUserStartedSpeakingFrame{}, &s) {
			t.Fatal("should not start on the VAD frame while the bot speaks")
		}
		s.VADSpeaking = true
		if v.ShouldStart(audio200, &s) {
			t.Fatal("200ms is short of 300")
		}
		if !v.ShouldStart(audio100, &s) {
			t.Fatal("300ms of speech should start")
		}
	})
	t.Run("vad zero waits for nobody", func(t *testing.T) {
		s := TurnState{BotSpeaking: true}
		if !(VADStart{}).ShouldStart(&frames.VADUserStartedSpeakingFrame{}, &s) {
			t.Fatal("MinSpeechMS 0 starts immediately")
		}
	})
	t.Run("transcription", func(t *testing.T) {
		var tr TranscriptionStart
		if tr.ShouldStart(&frames.InterimTranscriptionFrame{}, &TurnState{}) {
			t.Fatal("empty")
		}
		if !tr.ShouldStart(&frames.InterimTranscriptionFrame{Text: "hi"}, &TurnState{}) {
			t.Fatal("interim")
		}
		if !tr.ShouldStart(&frames.TranscriptionFrame{Text: "hi"}, &TurnState{}) {
			t.Fatal("final")
		}
	})
	t.Run("min words", func(t *testing.T) {
		m := MinWordsStart{N: 2, WhenBotSpeaking: true}
		s := TurnState{BotSpeaking: true}
		if m.ShouldStart(&frames.TranscriptionFrame{Text: "hello"}, &s) {
			t.Fatal("one word")
		}
		if !m.ShouldStart(&frames.TranscriptionFrame{Text: "hello there"}, &s) {
			t.Fatal("two words")
		}
		s.BotSpeaking = false
		if m.ShouldStart(&frames.TranscriptionFrame{Text: "hello there"}, &s) {
			t.Fatal("bot is quiet")
		}
		open := MinWordsStart{N: 2}
		if !open.ShouldStart(&frames.InterimTranscriptionFrame{Text: "hello there"}, &s) {
			t.Fatal("without the bot gate")
		}
	})
	t.Run("speech timeout", func(t *testing.T) {
		s := &SpeechTimeoutStop{}
		st := TurnState{}
		if d, stop := s.ShouldStop(ctx, &frames.VADUserStartedSpeakingFrame{}, &st); d || stop {
			t.Fatal("vad start is not a stop")
		}
		d, stop := s.ShouldStop(ctx, &frames.VADUserStoppedSpeakingFrame{}, &st)
		if !d || !stop {
			t.Fatal("timeout 0 stops on VAD stop")
		}
		wait := &SpeechTimeoutStop{Timeout: time.Second, RequireTranscript: true}
		st.HasTranscript = false
		if d, stop := wait.ShouldStop(ctx, &frames.VADUserStoppedSpeakingFrame{}, &st); d || stop {
			t.Fatal("should arm, not stop, when a transcript is required")
		}
		if _, _, ok := wait.Arm(); !ok {
			t.Fatal("timer should be armed")
		}
		fire := &speechFire{gen: wait.gen}
		if d, stop := wait.ShouldStop(ctx, fire, &st); d || stop {
			t.Fatal("timer fired but there is still no transcript")
		}
		st.HasTranscript = true
		if d, stop := wait.ShouldStop(ctx, &frames.TranscriptionFrame{Text: "ok"}, &st); !d || !stop {
			t.Fatal("transcript after the timer should stop")
		}
	})
	t.Run("turn analyzer", func(t *testing.T) {
		fake := &fakeAnalyzer{end: turn.Incomplete}
		a := &TurnAnalyzerStop{Analyzer: fake}
		st := TurnState{VADSpeaking: true}
		if a.Feed(&frames.InputAudioRawFrame{Audio: []byte{1, 0}, SampleRate: 16000}, &st) {
			t.Fatal("incomplete append")
		}
		if fake.appended != 1 {
			t.Fatalf("appended %d", fake.appended)
		}
		st.VADSpeaking = false
		if d, stop := a.ShouldStop(ctx, &frames.VADUserStoppedSpeakingFrame{}, &st); d || stop {
			t.Fatal("vad stop requests analysis, it does not stop by itself")
		}
		an, gen, ok := a.Pending()
		if !ok || an == nil || gen == 0 {
			t.Fatal("expected one analysis")
		}
		if _, _, ok := a.Pending(); ok {
			t.Fatal("analysis should be requested once")
		}
		if d, stop := a.ShouldStop(ctx, &analysisResult{gen: gen, result: turn.Incomplete}, &st); !d || stop {
			t.Fatal("incomplete model result abstains from stopping")
		}
		// A new VAD stop, then a complete result.
		_, _ = a.ShouldStop(ctx, &frames.VADUserStoppedSpeakingFrame{}, &st)
		_, gen, ok = a.Pending()
		if !ok {
			t.Fatal("second analysis")
		}
		if d, stop := a.ShouldStop(ctx, &analysisResult{gen: gen, result: turn.Complete, prob: 0.9}, &st); !d || !stop {
			t.Fatal("complete result should stop")
		}
		fake.end = turn.Complete
		st.VADSpeaking = false
		if !a.Feed(&frames.InputAudioRawFrame{Audio: []byte{0, 0}, SampleRate: 16000}, &st) {
			t.Fatal("silence fallback should stop")
		}
	})
	t.Run("first speech mute", func(t *testing.T) {
		m := &FirstSpeechMute{}
		if !m.Muted(&frames.InputAudioRawFrame{}, &TurnState{}) {
			t.Fatal("muted before the bot has spoken")
		}
		if m.Muted(&frames.BotStoppedSpeakingFrame{}, &TurnState{}) {
			t.Fatal("unmuted after the first bot stop")
		}
	})
	t.Run("function call mute", func(t *testing.T) {
		m := &FunctionCallMute{}
		if m.Muted(&frames.TextFrame{Text: "hi"}, &TurnState{}) {
			t.Fatal("idle")
		}
		started := kindFrame{kind: "FunctionCallsStarted"}
		if !m.Muted(started, &TurnState{}) {
			t.Fatal("muted once a call starts")
		}
		if !m.Muted(&frames.InputAudioRawFrame{}, &TurnState{}) {
			t.Fatal("stays muted")
		}
		if m.Muted(&frames.FunctionCallResultFrame{}, &TurnState{}) {
			t.Fatal("unmuted when the call finishes")
		}
		if !m.Muted(started, &TurnState{}) {
			t.Fatal("first call")
		}
		if !m.Muted(kindFrame{kind: "FunctionCallsStarted"}, &TurnState{}) {
			t.Fatal("second call")
		}
		_ = m.Muted(kindFrame{kind: "FunctionCallCancel"}, &TurnState{})
		if m.Muted(&frames.FunctionCallResultFrame{}, &TurnState{}) {
			t.Fatal("both calls finished")
		}
	})
}

type kindFrame struct {
	kind string
	frames.System
}

func (f kindFrame) FrameKind() string { return f.kind }

type fakeAnalyzer struct {
	end      turn.Result
	appended int
	cleared  int
}

func (f *fakeAnalyzer) AppendAudio([]byte, int, bool) turn.Result {
	f.appended++
	return f.end
}
func (f *fakeAnalyzer) AnalyzeEndOfTurn(context.Context) (turn.Result, float32, error) {
	return f.end, 0.5, nil
}
func (f *fakeAnalyzer) Clear() { f.cleared++ }
