package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStudentFilter_Pass: judge pass + full self-consistency agreement drops
// the question (student already solves it, zero learning value).
func TestStudentFilter_Pass(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SelfConsistencyK = 3
	llm := newFakeLLM().
		script("student", ok(studentAnswerA), ok(studentAnswerA), ok(studentAnswerA)).
		script("judge", ok(judgePassJSON))
	p := newTestPipeline(t, llm, cfg)

	out, err := p.studentFilter(context.Background(), baseQuestion())
	require.NoError(t, err)
	assert.False(t, out.ToTeacher)
	assert.Equal(t, "pass", out.Verdict.Verdict)
	assert.Equal(t, "both", out.Verdict.Method)
	require.NotNil(t, out.Verdict.SelfConsistencyDisagreement)
	assert.InDelta(t, 0.0, *out.Verdict.SelfConsistencyDisagreement, 1e-9)
	assert.Equal(t, 3, llm.callsFor("student"))
	assert.Equal(t, 1, llm.callsFor("judge"))
}

// TestStudentFilter_JudgeFail: a judge fail routes to the teacher regardless of
// self-consistency.
func TestStudentFilter_JudgeFail(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SelfConsistencyK = 3
	llm := newFakeLLM().
		script("student", ok(studentAnswerA), ok(studentAnswerA), ok(studentAnswerA)).
		script("judge", ok(judgeFailJSON))
	p := newTestPipeline(t, llm, cfg)

	out, err := p.studentFilter(context.Background(), baseQuestion())
	require.NoError(t, err)
	assert.True(t, out.ToTeacher)
	assert.Equal(t, "fail", out.Verdict.Verdict)
}

// TestStudentFilter_JudgeUncertain: a judge uncertain routes to the teacher.
func TestStudentFilter_JudgeUncertain(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SelfConsistencyK = 1
	llm := newFakeLLM().
		script("student", ok(studentAnswerA)).
		script("judge", ok(judgeUncertainJSON))
	p := newTestPipeline(t, llm, cfg)

	out, err := p.studentFilter(context.Background(), baseQuestion())
	require.NoError(t, err)
	assert.True(t, out.ToTeacher)
	assert.Equal(t, "uncertain", out.Verdict.Verdict)
	// K=1: no self-consistency signal recorded.
	assert.Nil(t, out.Verdict.SelfConsistencyDisagreement)
}

// TestStudentFilter_SelfConsistencyUncertain: judge pass but high disagreement
// across student samples still routes to the teacher.
func TestStudentFilter_SelfConsistencyUncertain(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SelfConsistencyK = 2
	cfg.SelfConsistencyThreshold = 0.5
	llm := newFakeLLM().
		script("student", ok(studentAnswerA), ok(studentAnswerB)). // disagreement 0.5
		script("judge", ok(judgePassJSON))
	p := newTestPipeline(t, llm, cfg)

	out, err := p.studentFilter(context.Background(), baseQuestion())
	require.NoError(t, err)
	assert.True(t, out.ToTeacher)
	assert.Equal(t, "uncertain", out.Verdict.Verdict)
	require.NotNil(t, out.Verdict.SelfConsistencyDisagreement)
	assert.InDelta(t, 0.5, *out.Verdict.SelfConsistencyDisagreement, 1e-9)
}

// TestStudentFilter_SelfConsistencyNormalizesFormatting: two student samples that
// differ ONLY in formatting (casing, hyphenation, punctuation, whitespace) count
// as agreement, so a solved question is not spuriously routed to the teacher.
// Under plain trim+lower keying these would key differently, yielding
// disagreement 0.5 >= threshold and an uncertain verdict.
func TestStudentFilter_SelfConsistencyNormalizesFormatting(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SelfConsistencyK = 2
	cfg.SelfConsistencyThreshold = 0.5
	llm := newFakeLLM().
		script("student", ok(studentAnswerA), ok(studentAnswerAFmt)).
		script("judge", ok(judgePassJSON))
	p := newTestPipeline(t, llm, cfg)

	out, err := p.studentFilter(context.Background(), baseQuestion())
	require.NoError(t, err)
	assert.False(t, out.ToTeacher)
	assert.Equal(t, "pass", out.Verdict.Verdict)
	require.NotNil(t, out.Verdict.SelfConsistencyDisagreement)
	assert.InDelta(t, 0.0, *out.Verdict.SelfConsistencyDisagreement, 1e-9)
}

// TestNormalizeAnswer locks the canonical form: casing, punctuation/symbols, and
// whitespace runs collapse to a single-spaced letter/digit sequence, while
// genuinely different answers stay distinct (and non-Latin scripts survive).
func TestNormalizeAnswer(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"plain", "42", "42"},
		{"trailing period", "42.", "42"},
		{"surrounding ws + bang", "  42! ", "42"},
		{"case + hyphen + comma", "Use gradient-clipping, by NORM!", "use gradient clipping by norm"},
		{"quotes and doubled spaces", `"The  answer" is  yes`, "the answer is yes"},
		{"korean keeps letters", "노름 클리핑.", "노름 클리핑"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, normalizeAnswer(c.in))
		})
	}
	// Formatting-only variants share a key; a different answer does not.
	assert.Equal(t, normalizeAnswer("Use gradient clipping by norm."), normalizeAnswer("  use gradient-clipping, by NORM!  "))
	assert.NotEqual(t, normalizeAnswer("42"), normalizeAnswer("43"))
}

// TestMutateThenPresolve_JudgeUsesMutatedSketch: end-to-end alignment of findings
// 1+2 — after a mutation rewrites the question and its reference sketch, the
// presolve judge is prompted with the MUTATED sketch, never the original.
func TestMutateThenPresolve_JudgeUsesMutatedSketch(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SelfConsistencyK = 1
	llm := newFakeLLM().
		script("generator", ok(mutateJSON)).
		script("student", ok(studentAnswerA)).
		script("judge", ok(judgePassJSON))
	p := newTestPipeline(t, llm, cfg)

	in := baseQuestion() // original sketch: "Clip gradient norm."
	q, err := p.mutate(context.Background(), in)
	require.NoError(t, err)
	require.NotEqual(t, in.ReferenceAnswerSketch, q.ReferenceAnswerSketch)

	_, err = p.studentFilter(context.Background(), q)
	require.NoError(t, err)

	var judgePromptText string
	for _, c := range llm.calls {
		if c.role == "judge" {
			judgePromptText = c.msgs[1].Content
		}
	}
	require.NotEmpty(t, judgePromptText)
	assert.Contains(t, judgePromptText, q.ReferenceAnswerSketch)
	assert.NotContains(t, judgePromptText, in.ReferenceAnswerSketch)
}
