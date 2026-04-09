// Package interrupt implements interruption strategies (barge-in).
package interrupt

import "strings"

// Strategy decides whether user speech should interrupt bot output.
type Strategy interface {
	ShouldInterrupt(transcriptText string, botSpeaking bool) bool
}

// NoInterrupt never triggers from strategy (VAD-only paths can still use frames elsewhere).
type NoInterrupt struct{}

func (NoInterrupt) ShouldInterrupt(string, bool) bool { return false }

// MinWords triggers interrupt when bot is speaking and transcript has at least N words.
type MinWords struct {
	N int
}

func (m MinWords) ShouldInterrupt(text string, botSpeaking bool) bool {
	if !botSpeaking {
		return false
	}
	n := m.N
	if n <= 0 {
		n = 1
	}
	fields := strings.Fields(strings.TrimSpace(text))
	return len(fields) >= n
}
