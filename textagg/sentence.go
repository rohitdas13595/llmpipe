// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// src/pipecat/utils/string.py and src/pipecat/utils/text/simple_text_aggregator.py.
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

// Package textagg splits streaming LLM text into sentences for TTS.
package textagg

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Aggregator collects text and releases complete sentences.
// Add returns every sentence completed by this chunk. Flush returns whatever
// is still buffered, including a fragment with no closing punctuation.
// Reset drops the buffer.
type Aggregator interface {
	Add(text string) []string
	Flush() string
	Reset()
}

// SentenceAggregator releases text on sentence boundaries.
// MinChars keeps a detected sentence in the buffer until it is at least that
// many runes long, so a short fragment such as "Hi." can join the next
// sentence. Zero emits every detected sentence.
//
// FilterMarkdown strips common Markdown before boundary detection. The zero
// value leaves the text unchanged. NewSentenceAggregator turns filtering on.
type SentenceAggregator struct {
	MinChars       int
	FilterMarkdown bool

	buf   string
	state lookState
	skip  int
	md    mdStream
}

// NewSentenceAggregator returns an aggregator that strips Markdown.
func NewSentenceAggregator() *SentenceAggregator {
	return &SentenceAggregator{FilterMarkdown: true}
}

type lookState int

const (
	lookIdle lookState = iota
	lookAwaitChar
	lookAwaitWord
	lookInWord
)

// Add appends text and returns any sentences that are now complete.
func (a *SentenceAggregator) Add(text string) []string {
	if text == "" {
		return nil
	}
	if a.FilterMarkdown {
		text = a.md.Push(text)
	}
	var out []string
	for _, r := range text {
		a.buf += string(r)
		out = append(out, a.pull(r)...)
	}
	return out
}

// Flush returns the trimmed remainder and clears the aggregator.
func (a *SentenceAggregator) Flush() string {
	extra := ""
	if a.FilterMarkdown {
		extra = a.md.Flush()
	}
	s := strings.TrimSpace(a.buf + extra)
	a.Reset()
	return s
}

// Reset drops buffered text and lookahead state.
func (a *SentenceAggregator) Reset() {
	a.buf = ""
	a.state = lookIdle
	a.skip = 0
	a.md = mdStream{}
}

func (a *SentenceAggregator) pull(last rune) []string {
	check, punct := a.advance(last)
	if punct {
		// A new terminator starts its own candidate after the check below.
		defer func() { a.state = lookAwaitChar }()
	}
	if !check {
		return nil
	}
	candidate := a.buf
	if punct && len(candidate) > 0 {
		_, size := utf8.DecodeLastRuneInString(candidate)
		candidate = candidate[:len(candidate)-size]
	}
	n := boundaryAfter(candidate, a.skip)
	if n <= 0 {
		return nil
	}
	sent := strings.TrimSpace(candidate[:n])
	if a.MinChars > 0 && utf8.RuneCountInString(sent) < a.MinChars {
		a.skip = n
		return nil
	}
	rest := a.buf[n:]
	a.buf = rest
	a.state = lookIdle
	a.skip = 0
	if sent == "" {
		return nil
	}
	return []string{sent}
}

// advance reports whether the buffer should be checked, and whether last is
// sentence punctuation that still needs its own lookahead afterwards.
func (a *SentenceAggregator) advance(last rune) (check, punct bool) {
	punct = isSentencePunct(last)
	switch a.state {
	case lookIdle:
		return false, punct
	case lookAwaitChar:
		if unicode.IsSpace(last) || punct {
			return false, punct
		}
		if unicode.IsLetter(last) || unicode.IsDigit(last) {
			a.state = lookInWord
		} else {
			a.state = lookAwaitWord
		}
		return true, punct
	case lookAwaitWord:
		if unicode.IsLetter(last) || unicode.IsDigit(last) {
			a.state = lookInWord
		}
		return false, punct
	case lookInWord:
		if unicode.IsSpace(last) || punct {
			a.state = lookIdle
			return true, punct
		}
		return false, punct
	default:
		return false, punct
	}
}

func boundaryAfter(s string, after int) int {
	if after < 0 {
		after = 0
	}
	if after > len(s) {
		return 0
	}
	n := MatchEndOfSentence(s[after:])
	if n <= 0 {
		return 0
	}
	return after + n
}

// MatchEndOfSentence returns the byte index just past the first sentence
// boundary in s, or 0 when s does not contain one.
//
// A trailing terminator completes a sentence ("Hello."). A terminator inside
// an abbreviation, decimal, scientific number, email, or URL does not, unless
// that abbreviation is itself the end of s ("Meet the new Mr. and Mrs.").
// Ellipses, Hindi danda, and CJK punctuation are boundaries. The index is a
// rune boundary, so s[:n] is valid UTF-8 when n > 0.
func MatchEndOfSentence(s string) int {
	s = strings.TrimRightFunc(s, unicode.IsSpace)
	if s == "" {
		return 0
	}
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			i++
			continue
		}
		if isUnambiguousEnd(r) {
			return extendClosers(s, i+size)
		}
		switch r {
		case '…':
			end := i + size
			if confirms(s, end) {
				return extendClosers(s, end)
			}
		case '!':
			end := consumeRun(s, i, func(rr rune) bool { return rr == '!' || rr == '?' })
			return extendClosers(s, end)
		case '?':
			end := consumeRun(s, i, func(rr rune) bool { return rr == '!' || rr == '?' })
			return extendClosers(s, end)
		case ';':
			end := i + size
			if confirms(s, end) {
				return extendClosers(s, end)
			}
		case '.':
			if dots := dotRun(s, i); dots >= 3 {
				end := i + dots
				if confirms(s, end) {
					return extendClosers(s, end)
				}
				i = end
				continue
			}
			end := i + size
			if isDecimalOrSci(s, i) || continuingEmailOrURL(s, i) || abbreviationHolds(s, i) {
				i = end
				continue
			}
			if confirms(s, end) {
				return extendClosers(s, end)
			}
		}
		i += size
	}
	return 0
}

func isSentencePunct(r rune) bool {
	if isUnambiguousEnd(r) {
		return true
	}
	switch r {
	case '.', '!', '?', ';', '…':
		return true
	default:
		return false
	}
}

func isUnambiguousEnd(r rune) bool {
	switch r {
	case '。', '？', '！', '；', '．', '｡',
		'।', '॥',
		'؟', '؛', '۔', '؏',
		'၊', '။',
		'។', '៕',
		'།', '༎',
		'։', '՜', '՞',
		'።', '፧', '፨':
		return true
	default:
		return false
	}
}

func confirms(s string, end int) bool {
	if end >= len(s) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[end:])
	return unicode.IsSpace(r) || isCloser(r)
}

func isCloser(r rune) bool {
	switch r {
	case '"', '\'', ')', ']', '}', '”', '’', '»', '›':
		return true
	default:
		return false
	}
}

func extendClosers(s string, i int) int {
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		if !isCloser(r) {
			break
		}
		i += size
	}
	return i
}

func consumeRun(s string, i int, ok func(rune) bool) int {
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		if !ok(r) {
			break
		}
		i += size
	}
	return i
}

func dotRun(s string, i int) int {
	n := 0
	for i+n < len(s) && s[i+n] == '.' {
		n++
	}
	return n
}

func isDecimalOrSci(s string, dot int) bool {
	if dot <= 0 || dot+1 >= len(s) {
		return false
	}
	prev, _ := utf8.DecodeLastRuneInString(s[:dot])
	if !unicode.IsDigit(prev) {
		return false
	}
	next, _ := utf8.DecodeRuneInString(s[dot+1:])
	return unicode.IsDigit(next) || next == 'e' || next == 'E'
}

func continuingEmailOrURL(s string, dot int) bool {
	start, end := tokenBounds(s, dot)
	if end <= dot+1 {
		return false
	}
	return linkish(s[start:end])
}

func linkishToken(s string, dot int) bool {
	start, end := tokenBounds(s, dot)
	return linkish(s[start:end])
}

func linkish(tok string) bool {
	tok = strings.ToLower(tok)
	return strings.Contains(tok, "@") || strings.Contains(tok, "://") || strings.HasPrefix(tok, "www.")
}

func tokenBounds(s string, dot int) (start, end int) {
	start = dot
	for start > 0 {
		r, size := utf8.DecodeLastRuneInString(s[:start])
		if unicode.IsSpace(r) {
			break
		}
		start -= size
	}
	end = dot + 1
	for end < len(s) {
		r, size := utf8.DecodeRuneInString(s[end:])
		if unicode.IsSpace(r) {
			break
		}
		end += size
	}
	return start, end
}

// abbreviationHolds reports whether the period at dot is an abbreviation
// marker that should not end the sentence. A dictionary abbreviation
// (Dr, Mr, Fig) holds even when the next word is capitalized. A pure
// initialism (U.S, a.m, a single letter other than I) holds only when the
// next word is not a capitalized sentence start, so "3:00 a.m. We left"
// still splits. A period at the end of s does not hold; it completes the
// sentence.
func abbreviationHolds(s string, dot int) bool {
	if linkishToken(s, dot) {
		return false
	}
	tok := wordBefore(s, dot)
	if tok == "" || strings.EqualFold(tok, "i") {
		return false
	}
	low := strings.ToLower(tok)
	_, dict := abbreviations[low]
	if !dict && strings.Contains(low, ".") {
		_, dict = abbreviations[strings.ReplaceAll(low, ".", "")]
	}
	initial := isInitialism(tok)
	if !dict && !initial {
		return false
	}
	if dot+1 >= len(s) {
		return false
	}
	if dict && !initial {
		return true
	}
	if next, ok := nextNonSpace(s, dot+1); ok && unicode.IsUpper(next) {
		return false
	}
	return true
}

func nextNonSpace(s string, i int) (rune, bool) {
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		if !unicode.IsSpace(r) {
			return r, true
		}
		i += size
	}
	return 0, false
}

func wordBefore(s string, dot int) string {
	i := dot
	for i > 0 {
		r, size := utf8.DecodeLastRuneInString(s[:i])
		if r == utf8.RuneError && size == 1 {
			break
		}
		if unicode.IsLetter(r) || r == '.' || r == '\'' || r == '’' {
			i -= size
			continue
		}
		break
	}
	return s[i:dot]
}

// isInitialism reports tokens such as "U.S" and "N.A.S.A" (single letters
// separated by dots). A lone "I" is a pronoun and is rejected by the caller.
func isInitialism(tok string) bool {
	if tok == "" {
		return false
	}
	letters := 0
	dots := 0
	for _, r := range tok {
		switch {
		case unicode.IsLetter(r):
			letters++
		case r == '.':
			if letters != 1 {
				return false
			}
			dots++
			letters = 0
		default:
			return false
		}
	}
	// "U.S" has a trailing letter group of one; "U.S." is passed without the
	// final dot, so the token still contains a dot and only single letters.
	if dots == 0 {
		return letters == 1
	}
	return letters <= 1
}

// abbreviations are compared after lowercasing and, for dotted forms, after
// the dots are removed as well ("e.g" matches "eg").
var abbreviations = map[string]struct{}{
	"mr": {}, "mrs": {}, "ms": {}, "miss": {}, "dr": {}, "prof": {}, "sr": {}, "jr": {},
	"st": {}, "vs": {}, "etc": {}, "eg": {}, "ie": {}, "fig": {}, "vol": {}, "no": {},
	"pp": {}, "al": {}, "gen": {}, "rep": {}, "sen": {}, "gov": {}, "lt": {}, "col": {},
	"sgt": {}, "cpl": {}, "capt": {}, "hon": {}, "rev": {}, "mt": {}, "ft": {},
	"inc": {}, "ltd": {}, "co": {}, "corp": {}, "dept": {}, "univ": {}, "assn": {},
	"bros": {}, "ph": {}, "phd": {}, "md": {}, "ba": {}, "ma": {},
	"approx": {}, "appt": {}, "apt": {}, "est": {}, "avg": {},
}
