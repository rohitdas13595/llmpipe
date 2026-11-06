package textagg

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

func FuzzSentenceAggregator(f *testing.F) {
	seeds := []string{
		"Hello world. Next sentence.",
		"Dr. Smith went home. He slept.",
		"The value is 3.14159. Really.",
		"Wait... are you sure? Yes!",
		"Email me at foo.bar@pipecat.ai. Thanks.",
		"Visit https://example.com/a.b/c. Then stop.",
		"नमस्ते। आप कैसे हैं?",
		"你好。今天天气很好！",
		"It is 3:00 a.m. Time to go.",
		"Mr. and Mrs. Jones left. U.S.A. too.",
		"**Hello.** Next _word_.",
		"",
		".",
		"...",
		"A",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 512 {
			s = s[:512]
		}
		checkAgg(t, &SentenceAggregator{}, s)
		checkAgg(t, &SentenceAggregator{MinChars: 8}, s)
		if utf8.ValidString(s) {
			checkChunked(t, s)
		}
	})
}

func checkAgg(t *testing.T, a *SentenceAggregator, s string) {
	t.Helper()
	parts := a.Add(s)
	rest := a.Flush()
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			t.Fatalf("empty sentence from %q", s)
		}
	}
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p)
	}
	b.WriteString(rest)
	if !sameLetters(b.String(), s) {
		t.Fatalf("text changed\nin  %q\nout %q", s, b.String())
	}
	n := MatchEndOfSentence(s)
	if n < 0 || n > len(s) {
		t.Fatalf("index %d for %q", n, s)
	}
	if utf8.ValidString(s) && n > 0 && !utf8.ValidString(s[:n]) {
		t.Fatalf("split inside a rune: %q", s)
	}
}

func checkChunked(t *testing.T, s string) {
	t.Helper()
	whole := collect(&SentenceAggregator{}, s)
	var chunked []string
	a := &SentenceAggregator{}
	for _, r := range s {
		chunked = append(chunked, a.Add(string(r))...)
	}
	if rest := a.Flush(); rest != "" {
		chunked = append(chunked, rest)
	}
	if strings.Join(whole, "\n") != strings.Join(chunked, "\n") {
		t.Fatalf("chunk mismatch\nwhole %q\nchunk %q", whole, chunked)
	}
}

func collect(a *SentenceAggregator, s string) []string {
	out := a.Add(s)
	if rest := a.Flush(); rest != "" {
		out = append(out, rest)
	}
	return out
}

func sameLetters(a, b string) bool {
	return lettersOnly(a) == lettersOnly(b)
}

func lettersOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
