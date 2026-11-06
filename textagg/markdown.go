// Portions derived from Pipecat (https://github.com/pipecat-ai/pipecat),
// src/pipecat/utils/text/markdown_text_filter.py.
// Copyright (c) 2024–2026, Daily. BSD 2-Clause (see THIRD_PARTY_NOTICES.md).
// Modifications Copyright (c) 2026 Rohit Kumar Das, AGPL-3.0.

package textagg

import (
	"html"
	"regexp"
	"strings"
	"unicode/utf8"
)

// MarkdownFilter removes formatting that should not be spoken.
// FilterCode drops fenced code blocks. FilterTables drops table rows.
// Otherwise code and table text is kept and only the markers are removed.
type MarkdownFilter struct {
	FilterCode   bool
	FilterTables bool
}

var (
	reCodeFence = regexp.MustCompile("(?s)```.*?```")
	reImage     = regexp.MustCompile(`!\[[^\]]*\]\([^)]*\)`)
	reLink      = regexp.MustCompile(`\[([^\]]+)\]\([^)]*\)`)
	reHeader    = regexp.MustCompile(`(?m)^[ \t]{0,3}#{1,6}[ \t]*`)
	reInline    = regexp.MustCompile("`([^`]*)`")
	reItalic    = regexp.MustCompile(`(^|[\s])\*([^*\n]+)\*([\s]|$)`)
	reUnder     = regexp.MustCompile(`(^|[\s])_([^_\n]+)_([\s]|$)`)
	reTableSep  = regexp.MustCompile(`(?m)^[ \t]*\|?[ \t]*:?-+:?[ \t]*(\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*$`)
	reTableRow  = regexp.MustCompile(`(?m)^[ \t]*\|.+\|[ \t]*$`)
)

// Strip returns s without Markdown markers.
func (f MarkdownFilter) Strip(s string) string {
	if s == "" {
		return ""
	}
	if f.FilterCode {
		s = reCodeFence.ReplaceAllString(s, " ")
	} else {
		s = strings.ReplaceAll(s, "```", " ")
	}
	s = reImage.ReplaceAllString(s, "")
	s = reLink.ReplaceAllString(s, "$1")
	s = reHeader.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "**", "")
	s = strings.ReplaceAll(s, "__", "")
	s = reInline.ReplaceAllString(s, "$1")
	s = strings.ReplaceAll(s, "`", "")
	s = reItalic.ReplaceAllString(s, "$1$2$3")
	s = reUnder.ReplaceAllString(s, "$1$2$3")
	if f.FilterTables {
		s = reTableSep.ReplaceAllString(s, "")
		s = reTableRow.ReplaceAllString(s, "")
	} else {
		s = reTableSep.ReplaceAllString(s, "")
		s = strings.ReplaceAll(s, "|", " ")
	}
	s = html.UnescapeString(s)
	return s
}

// mdStream applies MarkdownFilter to a token stream, holding back a trailing
// marker that the next chunk may still complete.
type mdStream struct {
	f    MarkdownFilter
	hold string
}

func (m *mdStream) Push(s string) string {
	in := m.hold + s
	m.hold = ""
	emit, hold := detachPartial(in)
	m.hold = hold
	return m.f.Strip(emit)
}

func (m *mdStream) Flush() string {
	s := m.f.Strip(m.hold)
	m.hold = ""
	return s
}

func detachPartial(s string) (string, string) {
	if i := strings.LastIndex(s, "["); i >= 0 && !strings.Contains(s[i:], "]") && len(s)-i < 160 {
		return s[:i], s[i:]
	}
	i := len(s)
	for i > 0 {
		r, size := utf8.DecodeLastRuneInString(s[:i])
		if r == '*' || r == '_' || r == '`' || r == '#' || r == '[' {
			i -= size
			continue
		}
		break
	}
	if i < len(s) {
		return s[:i], s[i:]
	}
	return s, ""
}
