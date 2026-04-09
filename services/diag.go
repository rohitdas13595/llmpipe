package services

import "log"

// PipelineLog writes a line prefixed with [llmpipe:component] so STT/LLM/TTS output
// is easy to spot next to WebRTC (Pion / LiveKit) warnings on stderr.
func PipelineLog(component, format string, args ...any) {
	log.Printf("[llmpipe:"+component+"] "+format, args...)
}
