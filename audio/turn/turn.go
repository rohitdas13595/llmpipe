// Package turn provides user turn boundaries and silence-based end-of-utterance helpers
// (Pipecat: pipecat/audio/turn, observers/turn_tracking_observer).
//
// End-of-turn after silence is implemented by:
//   - vad.EnergyAnalyzer with SilenceStopMS > 0 (single pass with VAD), or
//   - SilenceAnalyzer for tests / custom pipelines that supply isSpeech per chunk.
//
// Use TrackingProcessor to emit TurnStartedFrame / TurnEndedFrame for metrics or UX.
package turn

// EndOfTurnML is reserved for future ML-based smart-turn (Pipecat BaseSmartTurn subclasses).
type EndOfTurnML interface {
	Analyze(pcm []byte, sampleRate int, isSpeech bool) (endOfTurn bool)
}
