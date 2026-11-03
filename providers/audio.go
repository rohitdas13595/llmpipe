package providers

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/rohitdas13595/llmpipe/audio/onnx"
	"github.com/rohitdas13595/llmpipe/audio/turn/smartturn"
	"github.com/rohitdas13595/llmpipe/audio/vad"
	"github.com/rohitdas13595/llmpipe/audio/vad/silero"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/turns"
)

// BuildVAD returns a Silero state-machine processor when a model is available
// in an onnx-tagged build, and the RMS energy VAD otherwise.
// SILERO_MODEL overrides the path. With the onnx tag, models/silero_vad.onnx
// is used when that variable is unset and the file exists.
func BuildVAD(sampleRate int) processor.Processor {
	_ = sampleRate
	path := os.Getenv("SILERO_MODEL")
	if path == "" && onnx.Linked() {
		if p, ok := onnx.ModelPath("silero_vad.onnx"); ok {
			path = p
		}
	}
	if path != "" {
		m, err := silero.New(path, 16000)
		if err != nil {
			log.Printf("silero: %v; using energy VAD", err)
		} else {
			log.Printf("VAD: silero (%s)", path)
			return vad.NewStateProcessor("vad", vad.NewStateAnalyzer(m, vad.DefaultParams()))
		}
	}
	vadTh := 120.0
	if v := os.Getenv("VAD_RMS_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			vadTh = f
		}
	}
	vadAna := vad.NewEnergyAnalyzer(vadTh, envIntOr("VAD_MIN_SPEECH", 2), envIntOr("VAD_MIN_SILENCE", 6))
	if v := os.Getenv("TURN_SILENCE_MS"); v != "" {
		if ms, err := strconv.ParseFloat(v, 64); err == nil && ms > 0 {
			vadAna.SilenceStopMS = ms
			log.Printf("VAD: TURN_SILENCE_MS=%.0f (time-based end-of-user-turn)", ms)
		}
	}
	return vad.NewProcessor("vad", vadAna)
}

// BuildUserTurns returns the phase-4 user turn processor.
// VAD confirms immediately when the bot is quiet, and after 300ms while the
// bot is speaking. A transcript of at least one word also barges in.
// SMART_TURN_MODEL (or models/smart-turn-v3.2-cpu.onnx on an onnx build)
// selects Smart Turn v3; otherwise the turn ends 600ms after the VAD stops.
func BuildUserTurns() processor.Processor {
	stop := []turns.StopStrategy{turns.NewSpeechTimeoutStop(600 * time.Millisecond)}
	path := os.Getenv("SMART_TURN_MODEL")
	if path == "" && onnx.Linked() {
		if p, ok := onnx.ModelPath("smart-turn-v3.2-cpu.onnx"); ok {
			path = p
		}
	}
	if path != "" {
		a, err := smartturn.NewV3(path, smartturn.DefaultParams())
		if err != nil {
			log.Printf("smart turn: %v; using the speech timeout", err)
		} else {
			log.Printf("turn: smart turn v3 (%s)", path)
			stop = []turns.StopStrategy{&turns.TurnAnalyzerStop{Analyzer: a}}
		}
	}
	return turns.NewUserTurnProcessor("turns",
		[]turns.StartStrategy{
			turns.VADStart{MinSpeechMS: 300},
			turns.TranscriptionStart{},
			turns.MinWordsStart{N: 1, WhenBotSpeaking: true},
		},
		stop,
		nil,
	)
}

func envIntOr(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
