package common

import (
	"regexp"
	"strings"

	"github.com/guny524/distillation/internal/artifact"
)

// benchmarkNames are distinctive eval-benchmark identifiers that must never be
// seeded into the pipeline (contamination guard, todos sec 2-3). They match
// case-insensitively on token boundaries (containsToken) so ordinary prose that
// merely contains a substring (e.g. "arc" inside "architecture") is not dropped.
// Benchmark names that are ALSO ordinary English words live in
// ambiguousBenchmarkNames, since a bare boundary match on those would misfire.
var benchmarkNames = []string{
	"medqa", "medmcqa", "pubmedqa", "gpqa", "hle", "humanity's last exam",
	"humanitys last exam", "mmlu", "mmlu-pro", "gsm8k", "bbh", "big-bench",
	"bigbench", "agieval", "truthfulqa", "hellaswag", "winogrande", "arc-challenge",
	"arc challenge", "openbookqa", "swe-bench", "swebench", "math-500", "math500",
	"aime", "livecodebench", "humaneval", "mbpp", "boolq", "piqa",
	"triviaqa", "naturalquestions", "legalbench", "lex glue", "lex-glue",
	"casehold", "mathqa", "theoremqa", "minerva math", "commonsenseqa",
}

// ambiguousBenchmarkNames are benchmark identifiers that double as ordinary
// English words: "race" (RACE), "drop" (DROP), "squad" (SQuAD), "natural
// questions" (NaturalQuestions). A bare boundary match would drop legitimate
// prose ("fix the race condition", "drop the stale connection", "answer natural
// questions from users"), so these count as contamination only when a
// benchmark-context word sits within benchmarkContextChars of the occurrence.
var ambiguousBenchmarkNames = []string{
	"race", "drop", "squad", "natural questions",
}

// benchmarkContextChars bounds how far (either side) from an ambiguous name a
// qualifying context word may sit to confirm it denotes the eval benchmark.
const benchmarkContextChars = 48

// benchmarkContextRe recognises the words that, near an ambiguous benchmark name,
// mark it as the eval benchmark rather than the plain English word.
var benchmarkContextRe = regexp.MustCompile(
	`(?i)\b(benchmark\w*|dataset|data set|eval\w*|leaderboard|accuracy|` +
		`exact[ -]match|f1|zero[ -]shot|few[ -]shot|[0-9]+[ -]shot|` +
		`test set|test-set|testset|held[ -]out|split|subset)\b`,
)

// benchmarkPhraseRe catches structural tells of a benchmark eval item even when
// the benchmark is unnamed: an explicit answer-key line, or a 4-way labelled
// multiple-choice block ((A)…(B)…(C)…(D)). These are characteristic of
// exam/benchmark questions we must not reverse-generate from.
var benchmarkPhraseRe = regexp.MustCompile(
	`(?is)(answer\s*[:=]\s*[\(\[]?[a-eA-E][\)\]]?\b` +
		`|correct answer is` +
		`|\(a\).{1,400}\(b\).{1,400}\(c\).{1,400}\(d\))`,
)

// containsBenchmarkName reports whether hay (already lowercased) names an eval
// benchmark: a distinctive name on a word boundary, or an ambiguous common-word
// name that is additionally qualified by nearby benchmark-context vocabulary.
func containsBenchmarkName(hay string) bool {
	for _, name := range benchmarkNames {
		if containsToken(hay, name) {
			return true
		}
	}
	for _, name := range ambiguousBenchmarkNames {
		if containsQualifiedToken(hay, name) {
			return true
		}
	}
	return false
}

// forEachToken invokes fn at every word-boundary occurrence of needle in hay,
// returning true as soon as fn does. hay and needle must already be lowercased.
func forEachToken(hay, needle string, fn func(at int) bool) bool {
	if needle == "" || len(needle) > len(hay) {
		return false
	}
	for idx := 0; ; {
		rel := strings.Index(hay[idx:], needle)
		if rel < 0 {
			return false
		}
		at := idx + rel
		if isBoundary(hay, at, len(needle)) && fn(at) {
			return true
		}
		idx = at + 1
		if idx+len(needle) > len(hay) {
			return false
		}
	}
}

// containsToken reports whether needle occurs in hay on word boundaries. Shared
// by the benchmark-name and license-marker (license.go) matchers.
func containsToken(hay, needle string) bool {
	return forEachToken(hay, needle, func(int) bool { return true })
}

// containsQualifiedToken is containsToken plus the requirement that a
// benchmark-context word sits within benchmarkContextChars of the match.
func containsQualifiedToken(hay, needle string) bool {
	return forEachToken(hay, needle, func(at int) bool {
		return hasBenchmarkContext(hay, at, len(needle))
	})
}

// hasBenchmarkContext reports whether a benchmark-context word appears within
// benchmarkContextChars of hay[start:start+length].
func hasBenchmarkContext(hay string, start, length int) bool {
	lo := start - benchmarkContextChars
	if lo < 0 {
		lo = 0
	}
	hi := start + length + benchmarkContextChars
	if hi > len(hay) {
		hi = len(hay)
	}
	return benchmarkContextRe.MatchString(hay[lo:hi])
}

func isBoundary(hay string, start, length int) bool {
	before := start == 0 || !isWord(hay[start-1])
	endPos := start + length
	after := endPos >= len(hay) || !isWord(hay[endPos])
	return before && after
}

func isWord(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

// IsBenchmarkContaminated reports whether any of the given text fragments looks
// like an evaluation-benchmark item (named benchmark or structural MCQ/answer-key
// tell). Adapters call this on title+body before emitting an Artifact.
func IsBenchmarkContaminated(texts ...string) bool {
	for _, t := range texts {
		low := strings.ToLower(t)
		if containsBenchmarkName(low) {
			return true
		}
		if benchmarkPhraseRe.MatchString(t) {
			return true
		}
	}
	return false
}

// FilterContaminated drops every Artifact whose title or any chunk text trips
// IsBenchmarkContaminated, returning the clean subset. This is the mandatory
// last gate each adapter's Fetch applies before returning.
func FilterContaminated(arts []artifact.Artifact) []artifact.Artifact {
	out := arts[:0:0]
	for _, a := range arts {
		texts := make([]string, 0, len(a.Chunks)+1)
		texts = append(texts, a.Title)
		for _, c := range a.Chunks {
			texts = append(texts, c.Text)
		}
		if IsBenchmarkContaminated(texts...) {
			continue
		}
		out = append(out, a)
	}
	return out
}
