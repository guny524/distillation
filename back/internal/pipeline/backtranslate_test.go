package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPipeline(t *testing.T, llm LLM, cfg Config) *Pipeline {
	t.Helper()
	p, err := New(cfg, llm)
	require.NoError(t, err)
	return p
}

// TestBacktranslate_Golden: an artifact yields a classified question with
// provenance whose why_relevant is filled and language is detected; the
// question is re-expressed (not a verbatim copy of the excerpt).
func TestBacktranslate_Golden(t *testing.T) {
	llm := newFakeLLM().script("generator", ok(backtranslateJSON))
	p := newTestPipeline(t, llm, DefaultConfig())

	art := sampleArtifact()
	q, err := p.backtranslate(context.Background(), art)
	require.NoError(t, err)

	assert.Equal(t, "software-engineering", q.Domain)
	assert.Equal(t, "medium", q.Difficulty)
	assert.Equal(t, "code", q.TaskShape)
	assert.Equal(t, []string{"reasoning", "problem-solving"}, q.CapabilityTags)
	assert.NotEmpty(t, q.UserRequest)
	assert.NotEmpty(t, q.Context)
	assert.NotEmpty(t, q.SuccessCriteria)
	assert.NotEmpty(t, q.ReferenceAnswerSketch)
	assert.Equal(t, "en", q.Lang)

	// Provenance mapped from the artifact + rephrased why_relevant filled.
	require.Len(t, q.ArtifactRefs, 1)
	ref := q.ArtifactRefs[0]
	assert.Equal(t, "arxiv", ref.SourceType)
	assert.Equal(t, "2401.00001", ref.DocID)
	assert.Equal(t, "arXiv-nonexclusive", ref.License)
	assert.Equal(t, "sec-3", ref.Locator)
	assert.NotEmpty(t, ref.WhyRelevant)

	// Re-expression: the question does not copy the excerpt verbatim.
	assert.NotContains(t, q.UserRequest, "VERBATIM_SOURCE_SENTENCE_ABOUT_CLIPPING")

	// The generator prompt carried the excerpt as context.
	require.Equal(t, 1, llm.callsFor("generator"))
	assert.Contains(t, llm.calls[0].msgs[1].Content, "VERBATIM_SOURCE_SENTENCE_ABOUT_CLIPPING")
}

// TestBacktranslate_KoLanguageDetection: a Korean question is tagged lane ko.
func TestBacktranslate_KoLanguageDetection(t *testing.T) {
	llm := newFakeLLM().script("generator", ok(backtranslateKoJSON))
	p := newTestPipeline(t, llm, DefaultConfig())

	q, err := p.backtranslate(context.Background(), sampleArtifact())
	require.NoError(t, err)
	assert.Equal(t, "ko", q.Lang)
}

// TestBacktranslate_InvalidDomainRejected: an out-of-enum classification fails.
func TestBacktranslate_InvalidDomainRejected(t *testing.T) {
	bad := strings.Replace(backtranslateJSON, "software-engineering", "astrology", 1)
	llm := newFakeLLM().script("generator", ok(bad))
	p := newTestPipeline(t, llm, DefaultConfig())

	_, err := p.backtranslate(context.Background(), sampleArtifact())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid domain")
}

// TestBacktranslate_MissingWhyRelevant: an empty re-expression rationale fails
// (the copyright-safe justification is required).
func TestBacktranslate_MissingWhyRelevant(t *testing.T) {
	bad := strings.Replace(backtranslateJSON,
		`"why_relevant": "The source discusses stabilizing optimization, which grounds the question."`,
		`"why_relevant": ""`, 1)
	llm := newFakeLLM().script("generator", ok(bad))
	p := newTestPipeline(t, llm, DefaultConfig())

	_, err := p.backtranslate(context.Background(), sampleArtifact())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "why_relevant")
}
