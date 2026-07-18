package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/queue"
)

// TestQuestion_BacktranslatesAndCompletes: a comprehended item is reverse-
// generated into a question (backtranslate + mutate) and advanced.
func TestQuestion_BacktranslatesAndCompletes(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageQuestion)
	fi.digest = "Title: gradient clipping\nSource: http://x"
	llm := scriptQuestionRoles(newFakeLLM())
	w := NewQuestionWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.QuestionedAt, "questioned_at stamped")
	require.NotNil(t, fi.question, "question payload written")
	assert.Equal(t, "software-engineering", fi.question.Domain)
	// mutate ran (difficulty escalated to hard, only the valid operator kept).
	assert.Equal(t, "hard", fi.question.Difficulty)
	assert.Equal(t, []string{"conflicting_constraint"}, fi.question.DifficultyMutations)
	require.Len(t, fi.question.ArtifactRefs, 1)
	assert.Equal(t, 1, s.completes[queue.StageQuestion])
}

// TestQuestion_LLMErrorFails: a generator failure fails just that item.
func TestQuestion_LLMErrorFails(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageQuestion)
	fi.digest = "digest"
	llm := newFakeLLM().script("generator", fakeResp{err: assertErr}) // backtranslate errors
	w := NewQuestionWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "one item drained (claimed and moved to a terminal state)")
	require.NotNil(t, fi.it.FailedAt)
	assert.Equal(t, "question", fi.it.FailStage)
	assert.Nil(t, fi.it.QuestionedAt)
}
