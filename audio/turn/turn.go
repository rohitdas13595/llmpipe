// Package turn holds optional smart-turn / end-of-utterance analyzers.
// Phase 5: add HTTP or local model analyzers without changing the core frame types.
package turn

// Analyzer is reserved for future smart-turn integration.
type Analyzer interface {
	Analyze(pcm []byte, sampleRate int) (endOfTurn bool)
}
