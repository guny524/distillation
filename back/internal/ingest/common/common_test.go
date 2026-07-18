package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/guny524/distillation/internal/artifact"
)

func TestExcerpt_TruncatesLongText(t *testing.T) {
	long := strings.Repeat("word ", 500) // 2500 chars
	got := Excerpt(long, 100)
	assert.LessOrEqual(t, len([]rune(got)), 101, "within cap + ellipsis")
	assert.True(t, strings.HasSuffix(got, "…"))
}

func TestExcerpt_ShortTextUnchanged(t *testing.T) {
	assert.Equal(t, "hello world", Excerpt("  hello   world  ", 100))
}

func TestExcerpt_DefaultCap(t *testing.T) {
	long := strings.Repeat("x", 1000)
	got := Excerpt(long, 0)
	assert.LessOrEqual(t, len([]rune(got)), DefaultMaxExcerptChars+1)
}

func TestStripHTML(t *testing.T) {
	in := `<p>Hello <b>world</b> &amp; <a href="x">friends</a></p><script>evil()</script>`
	got := StripHTML(in)
	assert.Equal(t, "Hello world & friends", got)
	assert.NotContains(t, got, "evil")
}

func TestIsBenchmarkContaminated_NamedBenchmarks(t *testing.T) {
	for _, s := range []string{
		"This question is from MedQA.",
		"Adapted from the GPQA diamond set.",
		"Part of Humanity's Last Exam.",
		"An MMLU-style question.",
		"See SWE-bench Verified.",
	} {
		assert.True(t, IsBenchmarkContaminated(s), "should flag: %q", s)
	}
}

func TestIsBenchmarkContaminated_StructuralMCQ(t *testing.T) {
	mcq := "What is X? (A) foo (B) bar (C) baz (D) qux"
	assert.True(t, IsBenchmarkContaminated(mcq))
	assert.True(t, IsBenchmarkContaminated("The correct answer is B."))
	assert.True(t, IsBenchmarkContaminated("Answer: C"))
}

func TestIsBenchmarkContaminated_CleanTextPasses(t *testing.T) {
	for _, s := range []string{
		"How do I design a Kubernetes controller reconcile loop?",
		"Prove that the sum of two continuous functions is continuous.",
		"The architecture uses an arc-welding robot.", // 'arc' substring must not trip
		"We benchmarked our GPU kernels for throughput.",
	} {
		assert.False(t, IsBenchmarkContaminated(s), "should pass: %q", s)
	}
}

func TestFilterContaminated(t *testing.T) {
	arts := []artifact.Artifact{
		{DocID: "clean", Title: "How to tune a reconcile loop", Chunks: []artifact.Chunk{{Text: "no benchmark here"}}},
		{DocID: "dirty-title", Title: "A GPQA benchmark item", Chunks: []artifact.Chunk{{Text: "clean body"}}},
		{DocID: "dirty-chunk", Title: "clean title", Chunks: []artifact.Chunk{{Text: "From MedQA test set."}}},
	}
	got := FilterContaminated(arts)
	assert.Len(t, got, 1)
	assert.Equal(t, "clean", got[0].DocID)
}

func TestLicensePermitsCommercial(t *testing.T) {
	permit := []string{
		"CC0", "CC0 1.0", "Public Domain", "CC-BY 4.0", "CC BY 4.0",
		"CC-BY-SA 4.0", "Apache-2.0", "MIT", "BSD-3-Clause",
		"Creative Commons Attribution 4.0 International",
	}
	for _, l := range permit {
		assert.True(t, LicensePermitsCommercial(l), "should permit: %q", l)
	}
	deny := []string{
		"", "CC BY-NC 4.0", "CC-BY-NC-ND 4.0", "CC BY-ND 4.0",
		"noncommercial", "Some Unknown Proprietary License",
	}
	for _, l := range deny {
		assert.False(t, LicensePermitsCommercial(l), "should deny: %q", l)
	}
}
