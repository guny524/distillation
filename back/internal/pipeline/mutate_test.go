package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseQuestion() Question {
	return Question{
		Domain:                "software-engineering",
		Difficulty:            "medium",
		TaskShape:             "code",
		CapabilityTags:        []string{"reasoning"},
		UserRequest:           "Bound exploding gradients and implement a guard.",
		Context:               "A training loop that diverges.",
		SuccessCriteria:       []string{"names a clipping strategy"},
		ReferenceAnswerSketch: "Clip gradient norm.",
		Lang:                  "en",
	}
}

// TestMutate_AppliesAndFiltersOperators: the mutate stage rewrites the question,
// raises difficulty, and records only enabled+valid operators (dropping any
// operator the model names that is not in the allow-list).
func TestMutate_AppliesAndFiltersOperators(t *testing.T) {
	llm := newFakeLLM().script("generator", ok(mutateJSON))
	p := newTestPipeline(t, llm, DefaultConfig())

	q, err := p.mutate(context.Background(), baseQuestion())
	require.NoError(t, err)

	assert.Contains(t, q.UserRequest, "conflicting constraints")
	assert.Equal(t, "hard", q.Difficulty)
	// "not_a_real_operator" is dropped; only the enabled operator survives.
	assert.Equal(t, []string{"conflicting_constraint"}, q.DifficultyMutations)
	assert.Equal(t, 1, llm.callsFor("generator"))
}

// TestMutate_RefreshesReferenceSketch: a mutation that rewrites the question also
// rewrites the reference answer sketch in the same step, so the presolve judge
// never grades the mutated question against the pre-mutation answer. The
// existing sketch is offered to the operator as input.
func TestMutate_RefreshesReferenceSketch(t *testing.T) {
	llm := newFakeLLM().script("generator", ok(mutateJSON))
	p := newTestPipeline(t, llm, DefaultConfig())

	in := baseQuestion() // reference sketch: "Clip gradient norm."
	q, err := p.mutate(context.Background(), in)
	require.NoError(t, err)

	assert.NotEqual(t, in.ReferenceAnswerSketch, q.ReferenceAnswerSketch)
	assert.Contains(t, q.ReferenceAnswerSketch, "throughput/stability")

	prompt := llm.calls[0].msgs[1].Content
	assert.Contains(t, prompt, "Existing reference answer sketch")
	assert.Contains(t, prompt, in.ReferenceAnswerSketch)
	assert.Contains(t, prompt, "reference_answer_sketch:")
}

// TestMutate_KeepsSketchWhenModelOmitsIt: if the mutation response omits a new
// sketch, the prior one is kept (not blanked) so downstream non-empty-sketch
// validation still holds; the pipeline never fabricates a sketch.
func TestMutate_KeepsSketchWhenModelOmitsIt(t *testing.T) {
	const noSketch = `{
	  "user_request": "Bound exploding gradients, and implement a guard.",
	  "difficulty": "hard",
	  "success_criteria": ["names a clipping strategy"],
	  "applied_mutations": ["conflicting_constraint"]
	}`
	llm := newFakeLLM().script("generator", ok(noSketch))
	p := newTestPipeline(t, llm, DefaultConfig())

	in := baseQuestion()
	q, err := p.mutate(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, in.ReferenceAnswerSketch, q.ReferenceAnswerSketch)
}

// TestMutate_NoOperatorsIsNoOp: an empty mutation allow-list skips the LLM call
// and leaves the question unchanged.
func TestMutate_NoOperatorsIsNoOp(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mutations = nil
	llm := newFakeLLM()
	p := newTestPipeline(t, llm, cfg)

	in := baseQuestion()
	q, err := p.mutate(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, in.UserRequest, q.UserRequest)
	assert.Empty(t, q.DifficultyMutations)
	assert.Equal(t, 0, llm.callsFor("generator"))
}

// TestMutate_OnlyEnabledOperatorsOffered: only configured operators appear in
// the prompt (an operator removed from config is never offered).
func TestMutate_OnlyEnabledOperatorsOffered(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mutations = []string{"fault_injection"}
	llm := newFakeLLM().script("generator", ok(mutateJSON))
	p := newTestPipeline(t, llm, cfg)

	_, err := p.mutate(context.Background(), baseQuestion())
	require.NoError(t, err)
	prompt := llm.calls[0].msgs[1].Content
	assert.Contains(t, prompt, "fault_injection")
	assert.NotContains(t, prompt, "partial_info_removal")
	assert.NotContains(t, prompt, "conflicting_constraint")
}
