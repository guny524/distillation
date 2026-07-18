package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/queue"
)

// TestPresolve_RoutesToTeacher: a question the student cannot solve (judge fail)
// records the verdict and completes (presolved_at stamped).
func TestPresolve_RoutesToTeacher(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StagePresolve)
	q := validQuestion()
	fi.question = &q
	llm := scriptPresolveToTeacher(newFakeLLM())
	w := NewPresolveWorker(s, testPipeline(t, llm), true, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.PresolvedAt, "presolved_at stamped")
	assert.Nil(t, fi.it.SupersededAt)
	require.NotNil(t, fi.verdict)
	assert.Equal(t, "fail", fi.verdict.Verdict)
	assert.Equal(t, 1, s.completes[queue.StagePresolve])
	assert.Equal(t, 0, s.supersedes)
}

// TestPresolve_SolvedSuperseded: a question the student already solves (judge
// pass + agreement) is soft-deleted (superseded), NOT completed -- the key
// presolve gate behavior (todos sec 2-5-6).
func TestPresolve_SolvedSuperseded(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StagePresolve)
	q := validQuestion()
	fi.question = &q
	llm := scriptPresolveSolved(newFakeLLM())
	w := NewPresolveWorker(s, testPipeline(t, llm), true, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.SupersededAt, "student solves it -> soft-deleted")
	assert.Nil(t, fi.it.PresolvedAt, "not advanced to the teacher")
	require.NotNil(t, fi.verdict, "verdict recorded for the audit trail")
	assert.Equal(t, "pass", fi.verdict.Verdict)
	assert.Equal(t, 1, s.supersedes)
	assert.Equal(t, 0, s.completes[queue.StagePresolve])
}

// TestPresolve_DisabledPassThrough: with presolve disabled the item is completed
// without any student/judge call (todos: "presolve 비활성 config면 그냥 통과"), but a
// method=disabled verdict IS recorded -- the answer stage reads presolve_verdict
// unconditionally, so a NULL there would terminally fail every item downstream.
func TestPresolve_DisabledPassThrough(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StagePresolve)
	q := validQuestion()
	fi.question = &q
	llm := newFakeLLM() // nothing scripted: any LLM call would error
	w := NewPresolveWorker(s, testPipeline(t, llm), false, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.PresolvedAt, "presolved_at stamped (gate cleared)")
	require.NotNil(t, fi.verdict, "a disabled-presolve verdict is recorded for the answer stage")
	assert.Equal(t, "uncertain", fi.verdict.Verdict)
	assert.Equal(t, "disabled", fi.verdict.Method)
	assert.True(t, model.ValidStudentFilterMethods[fi.verdict.Method], "synthetic method stays schema-valid")
	assert.Equal(t, 0, llm.calls["student"], "no student call when disabled")
	assert.Equal(t, 0, llm.calls["judge"], "no judge call when disabled")
	assert.Equal(t, 1, s.completes[queue.StagePresolve])
}
