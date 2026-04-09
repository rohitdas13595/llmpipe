package aggregate

import "sync/atomic"

// BotState tracks whether the bot is outputting speech (for barge-in).
type BotState struct {
	speaking atomic.Bool
}

// NewBotState creates an idle (not speaking) bot state tracker.
func NewBotState() *BotState { return &BotState{} }

func (b *BotState) SetSpeaking(v bool) { b.speaking.Store(v) }
func (b *BotState) Speaking() bool     { return b.speaking.Load() }
