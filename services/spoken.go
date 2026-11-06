package services

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/processor"
)

// EmitTTS sends the playback markers for one synthesized utterance: started,
// one TTSTextFrame per sentence with PTS at the end of that sentence's audio,
// the PCM, then stopped. Bot speaking frames come from the output, which is
// what the user actually hears.
func EmitTTS(emit processor.Emit, text string, pcm []byte, sampleRate int) {
	if emit.Down == nil {
		return
	}
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	emit.Down(&frames.TTSStartedFrame{})
	for _, tf := range TTSTextFrames(text, len(pcm), sampleRate) {
		emit.Down(tf)
	}
	if len(pcm) > 0 {
		emit.Down(&frames.TTSAudioRawFrame{Audio: pcm, SampleRate: sampleRate, NumChannels: 1})
	}
	emit.Down(&frames.TTSStoppedFrame{})
}

// TTSTextFrames splits text on sentence boundaries and stamps each slice with
// the media time at which it has been fully spoken, proportional to rune
// length. The last slice ends at the full audio duration so rounding does
// not leave it past the playback clock.
func TTSTextFrames(text string, pcmBytes, sampleRate int) []*frames.TTSTextFrame {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	parts := splitSentences(text)
	if len(parts) == 0 {
		parts = []string{text}
	}
	dur := pcmDuration(pcmBytes, sampleRate)
	total := 0
	for _, p := range parts {
		total += utf8.RuneCountInString(p)
	}
	if total == 0 {
		total = 1
	}
	var acc int
	out := make([]*frames.TTSTextFrame, 0, len(parts))
	for i, p := range parts {
		acc += utf8.RuneCountInString(p)
		pts := dur
		if i != len(parts)-1 {
			pts = time.Duration(int64(dur) * int64(acc) / int64(total))
		}
		out = append(out, &frames.TTSTextFrame{Text: p, Meta: frames.Meta{PTS: pts}})
	}
	return out
}

func pcmDuration(pcmBytes, sampleRate int) time.Duration {
	if pcmBytes < 2 || sampleRate <= 0 {
		return 0
	}
	samples := pcmBytes / 2
	return time.Duration(samples) * time.Second / time.Duration(sampleRate)
}

func splitSentences(s string) []string {
	runes := []rune(s)
	var out []string
	start := 0
	for i, r := range runes {
		if !sentenceEnd(r) {
			continue
		}
		seg := strings.TrimSpace(string(runes[start : i+1]))
		if seg != "" {
			out = append(out, seg)
		}
		start = i + 1
	}
	if start < len(runes) {
		seg := strings.TrimSpace(string(runes[start:]))
		if seg != "" {
			out = append(out, seg)
		}
	}
	return out
}

func sentenceEnd(r rune) bool {
	switch r {
	case '.', '!', '?', '।', '？', '。':
		return true
	default:
		return false
	}
}
