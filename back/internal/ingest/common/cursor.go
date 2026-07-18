package common

import (
	"strconv"
	"strings"
)

// ParseCursorInt interprets an opaque per-source cursor as a non-negative
// integer (an arXiv/PMC start offset or a StackExchange/k8s page number),
// returning def for an empty or malformed cursor so a first run (cursor "") and
// a corrupt/legacy value both start from a well-defined position. Each adapter
// picks its own base: offset sources pass def=0, page sources pass def=1.
func ParseCursorInt(cursor string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(cursor))
	if err != nil || n < 0 {
		return def
	}
	return n
}

// FormatCursorInt renders an integer cursor position back to its opaque string
// form for SaveCursor.
func FormatCursorInt(n int) string {
	return strconv.Itoa(n)
}
