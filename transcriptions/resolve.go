package transcriptions

import (
	"log/slog"
	"strings"
)

// ResolveLanguage maps a Language to a provider-specific code using m, matching Pipecat's
// resolve_language: if lang is in m, return m[lang]; otherwise log a warning and return either
// the base subtag (e.g. "en" from "en-US") when useBaseCode is true, or the full tag.
func ResolveLanguage(lang Language, m map[Language]string, useBaseCode bool) string {
	if s, ok := m[lang]; ok {
		return s
	}
	ls := string(lang)
	if useBaseCode {
		base := ls
		if i := strings.IndexByte(ls, '-'); i >= 0 {
			base = ls[:i]
		}
		out := strings.ToLower(base)
		slog.Warn("language not verified; using base code", "language", string(lang), "base", out)
		return out
	}
	slog.Warn("language not verified; using full tag", "language", ls)
	return ls
}
