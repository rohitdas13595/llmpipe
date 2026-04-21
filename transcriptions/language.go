// Package transcriptions provides language codes (BCP 47 / ISO 639) and helpers
// aligned with Pipecat's pipecat.transcriptions.language module.
//
//go:generate python3 ../scripts/gen_transcription_languages.py
package transcriptions

// Language is a language tag string (e.g. "en", "en-US") for STT/TTS/translation services.
type Language string

// String returns the language tag.
func (l Language) String() string { return string(l) }
