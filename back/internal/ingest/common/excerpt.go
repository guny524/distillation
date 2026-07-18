package common

import (
	"html"
	"regexp"
	"strings"
)

// DefaultMaxExcerptChars is the upper bound (in runes) for a Chunk.Text
// excerpt. The copyright rule (internal/artifact doc comment, research report
// sec on 저작권) requires excerpts short enough to be a rephrasing anchor, not a
// redistributable copy. Adapters may lower it via config but never store the
// full source body.
const DefaultMaxExcerptChars = 480

var (
	tagRe   = regexp.MustCompile(`(?s)<[^>]*>`)
	wsRe    = regexp.MustCompile(`\s+`)
	scriptRe = regexp.MustCompile(`(?is)<(script|style)[^>]*>.*?</(script|style)>`)
)

// StripHTML removes HTML/XML markup and unescapes entities, collapsing runs of
// whitespace to single spaces. Used on StackExchange post bodies and PMC/JATS
// content before excerpting.
func StripHTML(s string) string {
	s = scriptRe.ReplaceAllString(s, " ")
	s = tagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	s = wsRe.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// Excerpt returns a short, whitespace-normalised prefix of s bounded to max
// runes. It truncates on a word boundary when possible and appends an ellipsis
// to signal the excerpt is partial. A non-positive max falls back to
// DefaultMaxExcerptChars. This is the single enforcement point for the "no long
// verbatim text in a Chunk" rule.
func Excerpt(s string, max int) string {
	if max <= 0 {
		max = DefaultMaxExcerptChars
	}
	s = wsRe.ReplaceAllString(strings.TrimSpace(s), " ")
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	cut := string(runes[:max])
	// Prefer cutting at the last space so we don't split a word.
	if i := strings.LastIndex(cut, " "); i > max/2 {
		cut = cut[:i]
	}
	return strings.TrimSpace(cut) + "…"
}
