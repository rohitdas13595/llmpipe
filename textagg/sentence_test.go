// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// tests/test_utils_string.py and tests/test_simple_text_aggregator.py.
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

package textagg

import (
	"strings"
	"testing"
	"unicode"
)

func TestMatchEndOfSentenceCorpus(t *testing.T) {
	ends := []string{
		"This is a sentence.",
		"This is a sentence!",
		"This is a sentence?",
		"This is a sentence;",
		"This is a sentence...",
		"This is a sentence…",
		"Meet the new Mr. and Mrs.",
		"U.S.A. and N.A.S.A.",
		"USA and NASA.",
		"My number is 123-456-7890.",
		"For information, call 411.",
		"My emails are foo@pipecat.ai and bar@pipecat.ai.",
		"My email is foo.bar@pipecat.ai.",
		"My email is spell(foo.bar@pipecat.ai).",
		"The number pi is 3.14159.",
		"Valid scientific notation 1.23e4.",
		"Valid scientific notation 0.e4.",
		"It still early, it's 3:00 a.m.",
		"It is 3:00 p.m.",
		"See you then, e.g.",
		"That is, i.e.",
		"We met Dr. Shah.",
		"Hello world.",
		"Wait for it...",
		"नमस्ते।",
		"पहला वाक्य। दूसरा",
		"你好。",
		"你好。世界",
		"何ですか？",
		"何ですか？次へ",
		"끝났습니다！",
		"مرحبا؟",
		"Done।",
		`He said "hello."`,
		"Visit https://example.com/a.b/c.",
		"See www.example.com.",
		"Price is $29.",
		"Item (one).",
		"Really?!",
		"Go now!!",
		"第一。第二",
		"終わり。",
		"はい！",
		"そうですか？",
		"Line one.\nLine two",
	}
	for _, s := range ends {
		got := MatchEndOfSentence(s)
		want := firstBoundaryWant(s)
		if got != want {
			t.Errorf("MatchEndOfSentence(%q) = %d, want %d (%q)", s, got, want, slice(s, got))
		}
	}

	none := []string{
		"",
		"   ",
		"This is not a sentence",
		"This is not a sentence,",
		"This is not a sentence, ",
		"Ok, Mr. Smith let's ",
		"Dr. Walker, I presume ",
		"Prof. Walker, I presume ",
		"zweitens, und 3",
		"Heute ist Dienstag, der 3",
		"America, or the U.S",
		"My emails are foo@pipecat.ai and bar@pipecat.ai",
		"The number pi is 3.14159",
		"Hello",
		"world",
		"The quick brown fox",
		"Hello world",
		"e.g. more words",
		"i.e. more words",
		"Visit https://example.com/a.b/c now",
		"Email foo.bar@pipecat.ai today",
		"Version 1.23e4 is fine",
		"Call Mr. Smith later",
		"St. Louis is rainy",
		"U.S.A. and more",
		"3.14 is only a number",
		"No punctuation here at all",
		"Wait",
		"你好",
		"नमस्ते",
	}
	for _, s := range none {
		if got := MatchEndOfSentence(s); got != 0 {
			t.Errorf("MatchEndOfSentence(%q) = %d, want 0", s, got)
		}
	}

	// Explicit split points where the boundary is not the end of the string.
	splits := []struct {
		in, sent string
	}{
		{"This is a sentence. This is another one", "This is a sentence."},
		{"This is a sentence! And another", "This is a sentence!"},
		{"This is a sentence? Yes", "This is a sentence?"},
		{"Hello world. Next sentence", "Hello world."},
		{"This is for Mr. and Mrs. Jones. They left.", "This is for Mr. and Mrs. Jones."},
		{"Wait... Next", "Wait..."},
		{"Wait… Next", "Wait…"},
		{"The number pi is 3.14159. Next", "The number pi is 3.14159."},
		{"foo@pipecat.ai is an email. Next", "foo@pipecat.ai is an email."},
		{"नमस्ते। आगे बढ़ो", "नमस्ते।"},
		{"你好。世界很大", "你好。"},
		{"何ですか？次の文", "何ですか？"},
		{`He said "hello." Next`, `He said "hello."`},
		{"Price is $29. Next week", "Price is $29."},
		{"Dr. Shah went home. He slept.", "Dr. Shah went home."},
		{"Really?! Yes", "Really?!"},
		{"One; two", "One;"},
		{"See https://example.com/a.b. Then go", "See https://example.com/a.b."},
		{"It is 3:00 a.m. We should sleep.", "It is 3:00 a.m."},
		{"Call 411. Now", "Call 411."},
	}
	for _, tc := range splits {
		got := MatchEndOfSentence(tc.in)
		if slice(tc.in, got) != tc.sent {
			t.Errorf("MatchEndOfSentence(%q) = %q, want %q", tc.in, slice(tc.in, got), tc.sent)
		}
	}
}

// firstBoundaryWant is the expected index for strings whose first boundary is
// either the full trimmed sentence or the first unambiguous / Latin terminator
// that the corpus treats as ending the sentence.
func firstBoundaryWant(s string) int {
	s = strings.TrimRightFunc(s, unicode.IsSpace)
	// Known early splits inside the "ends" list.
	switch s {
	case "पहला वाक्य। दूसरा":
		return len("पहला वाक्य।")
	case "你好。世界":
		return len("你好。")
	case "何ですか？次へ":
		return len("何ですか？")
	case "第一。第二":
		return len("第一。")
	case "Line one.\nLine two":
		return len("Line one.")
	default:
		return len(s)
	}
}

func slice(s string, n int) string {
	if n <= 0 || n > len(s) {
		return ""
	}
	return s[:n]
}

func TestSentenceAggregatorLookahead(t *testing.T) {
	a := &SentenceAggregator{}
	if got := a.Add("Hello world."); len(got) != 0 {
		t.Fatalf("punctuation alone emitted %v", got)
	}
	got := a.Add(" Next")
	if len(got) != 1 || got[0] != "Hello world." {
		t.Fatalf("lookahead = %v", got)
	}
	if rest := a.Flush(); rest != "Next" {
		t.Fatalf("flush = %q", rest)
	}
}

func TestSentenceAggregatorStreamingTokens(t *testing.T) {
	a := &SentenceAggregator{}
	tokens := []string{"The", " quick", " brown", " fox", " jumps", "."}
	var got []string
	for _, tok := range tokens {
		got = append(got, a.Add(tok)...)
	}
	if len(got) != 0 {
		t.Fatalf("emitted before lookahead: %v", got)
	}
	got = append(got, a.Add(" Then")...)
	if len(got) != 1 || got[0] != "The quick brown fox jumps." {
		t.Fatalf("got %v", got)
	}
}

func TestSentenceAggregatorAbbreviationsDecimals(t *testing.T) {
	a := &SentenceAggregator{}
	got := a.Add("Dr. Shah went home. He slept.")
	// The period after home needs lookahead; " He" confirms it. The final
	// period stays buffered until flush.
	if len(got) != 1 || got[0] != "Dr. Shah went home." {
		t.Fatalf("got %v, buf %q", got, a.buf)
	}
	if rest := a.Flush(); rest != "He slept." {
		t.Fatalf("flush = %q", rest)
	}

	b := &SentenceAggregator{}
	if n := b.Add("The value is 3.14159"); len(n) != 0 {
		t.Fatalf("decimal split: %v", n)
	}
	got = b.Add(". Next")
	if len(got) != 1 || got[0] != "The value is 3.14159." {
		t.Fatalf("after decimal: %v", got)
	}
}

func TestSentenceAggregatorMinChars(t *testing.T) {
	a := &SentenceAggregator{MinChars: 12}
	got := a.Add("Hi. This is longer. More")
	if len(got) != 1 || got[0] != "Hi. This is longer." {
		t.Fatalf("got %v buf %q", got, a.buf)
	}
	if got[0] == "Hi." {
		t.Fatal("short sentence emitted on its own")
	}
}

func TestSentenceAggregatorReset(t *testing.T) {
	a := &SentenceAggregator{}
	_ = a.Add("Hello world. Next")
	a.Reset()
	if rest := a.Flush(); rest != "" {
		t.Fatalf("after reset: %q", rest)
	}
	if got := a.Add("Hi"); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestSentenceAggregatorMarkdown(t *testing.T) {
	a := NewSentenceAggregator()
	got := a.Add("Hello **world**. Next sentence")
	if len(got) != 1 || got[0] != "Hello world." {
		t.Fatalf("got %v", got)
	}
	if rest := a.Flush(); rest != "Next sentence" {
		t.Fatalf("flush = %q", rest)
	}
}

func TestMarkdownFilter(t *testing.T) {
	f := MarkdownFilter{}
	cases := []struct{ in, want string }{
		{"**bold** and *italic*", "bold and italic"},
		{"Use `code` here", "Use code here"},
		{"See [docs](https://example.com) now", "See docs now"},
		{"# Title\nHello", "Title\nHello"},
		{"A &amp; B", "A & B"},
		{"![alt](img.png) hi", " hi"},
		{"```\ncode\n```", " \ncode\n "},
	}
	for _, tc := range cases {
		got := f.Strip(tc.in)
		if got != tc.want {
			t.Errorf("Strip(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := (MarkdownFilter{FilterCode: true}).Strip("before ```\nsecret\n``` after"); strings.Contains(got, "secret") {
		t.Fatalf("code kept: %q", got)
	}
}
