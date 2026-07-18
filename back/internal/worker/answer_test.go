package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/teacher"
)

// scriptAnswerRoles scripts the translator (one call) and teacher (two lanes)
// for one item's lane generation. The teacher responses carry raw reasoning so
// the reasoning-aware capture (cot_raw/has_raw_cot) is exercised.
func scriptAnswerRoles(llm *fakeLLM) *fakeLLM {
	return llm.
		script("translator", ok(translateJSON)).
		script("teacher",
			fakeResp{content: teacherTrajectoryJSON, reasoning: "raw chain of thought A"},
			fakeResp{content: teacherTrajectoryJSON, reasoning: "raw chain of thought B"})
}

// answerItem seeds one presolved item ready for the answer stage.
func answerItem(s *fakeStore, id int64, doc string) {
	fi := s.add(id, "arxiv", doc, queue.StageAnswer)
	q := validQuestion()
	fi.question = &q
	v := verdictToTeacher()
	fi.verdict = &v
}

// TestAnswer_GateClosedNoClaim: a closed quota gate (teacher/translator subscription
// spent, or /quota unreachable) means the worker never claims -- the explicit
// stateless-gate contract (todos sec 6-3-4: "quota 소진이면 claim 안 함").
func TestAnswer_GateClosedNoClaim(t *testing.T) {
	s := newFakeStore()
	answerItem(s, 1, "2501.1")
	fi := s.find(1)
	gate := closedGate()
	llm := scriptAnswerRoles(newFakeLLM())
	w := NewAnswerWorker(s, testPipeline(t, llm), gate, "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, res.Generated)
	assert.Nil(t, fi.it.AnsweredAt, "item untouched when the gate is closed")
	assert.Nil(t, fi.it.LockOwnerID, "never claimed")
	assert.Equal(t, 0, s.completes[queue.StageAnswer])
	assert.Equal(t, 1, gate.allowSeen, "the gate was consulted once, then the pass stopped")
}

// TestAnswer_GeneratesWhenGateOpen: an open gate lets the worker claim, generate the
// ko/en trajectory (with cot/cot_raw captured), store it, and complete.
func TestAnswer_GeneratesWhenGateOpen(t *testing.T) {
	s := newFakeStore()
	answerItem(s, 1, "2501.1")
	fi := s.find(1)
	llm := scriptAnswerRoles(newFakeLLM())
	w := NewAnswerWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Generated)
	assert.Equal(t, 2, res.TeacherCalls, "one teacher call per lane")
	require.NotNil(t, fi.it.AnsweredAt, "answered_at stamped")
	require.Len(t, fi.answer, 2, "ko + en lane records stored")

	// Reasoning capture (todos sec 2-5-5): cot mirrors the in-band reasoning and
	// cot_raw carries the provider reasoning_content.
	for _, rec := range fi.answer {
		assert.NotEmpty(t, rec.Cot, "cot populated from in-band reasoning")
		require.NotNil(t, rec.HasRawCoT)
		assert.True(t, *rec.HasRawCoT, "reasoning-aware teacher -> has_raw_cot true")
		require.NotNil(t, rec.CotRaw)
		assert.Contains(t, *rec.CotRaw, "raw chain of thought")
	}
	assert.Equal(t, 1, s.completes[queue.StageAnswer])
}

// TestAnswer_StopsWhenGateCloses: the gate is re-checked before EVERY claim, so a
// gate that opens once then closes processes exactly one item and stops -- the
// stateless replacement for the old per-pass budget cap (todos sec 6-3-4:
// "게이트 통과? -> claim -> 처리 루프", overshoot bounded by in-flight items).
func TestAnswer_StopsWhenGateCloses(t *testing.T) {
	s := newFakeStore()
	for id := int64(1); id <= 3; id++ {
		answerItem(s, id, "doc")
	}
	gate := &fakeGate{allow: []bool{true, false}} // open for the first claim, then closed
	llm := scriptAnswerRoles(newFakeLLM())
	w := NewAnswerWorker(s, testPipeline(t, llm), gate, "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Generated, "gate closed after one item caps generation at one")
	assert.Equal(t, 1, s.completes[queue.StageAnswer])
	assert.Equal(t, 2, gate.allowSeen, "the gate was consulted before each claim (open, then closed)")
}

// TestAnswer_PartialFailureReschedules: when the second lane's teacher call
// succeeds (HTTP) but its response fails to parse, the item is rescheduled for a
// bounded re-request -- one stochastic malformed reply must not discard the item
// (and with it the first lane's teacher spend) forever. The teacher calls
// already made are still counted (the quota was spent -- todos #1c).
func TestAnswer_PartialFailureReschedules(t *testing.T) {
	s := newFakeStore()
	answerItem(s, 1, "2501.1")
	fi := s.find(1)
	// translator ok; ko lane teacher ok; en lane teacher returns unparseable JSON.
	llm := newFakeLLM().
		script("translator", ok(translateJSON)).
		script("teacher", ok(teacherTrajectoryJSON), ok("not valid json"))
	w := NewAnswerWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, res.Generated, "no record produced this attempt (second lane unparseable)")
	assert.Equal(t, 2, res.TeacherCalls, "both teacher calls were made")
	assert.Equal(t, 1, res.Spent, "the item spent quota and is counted")
	assert.Nil(t, fi.it.FailedAt, "a stochastic malformed reply is retried, not terminal")
	assert.Equal(t, 1, s.retries, "item rescheduled with backoff")
}

// TestAnswer_RateLimitedStopsPassAndArmsCooldown (todos sec 6-3-4): when a teacher
// call returns 429, the worker stops claiming more items this pass (even with items
// to spare), arms the gate's process-local cooldown, and the 429'd item is
// rescheduled (transient retry), not failed.
func TestAnswer_RateLimitedStopsPassAndArmsCooldown(t *testing.T) {
	s := newFakeStore()
	for id := int64(1); id <= 3; id++ {
		answerItem(s, id, "doc")
	}
	gate := openGate()
	// translator succeeds, then the first teacher lane call is rate limited (429).
	llm := newFakeLLM().
		script("translator", ok(translateJSON)).
		script("teacher", fakeResp{err: teacher.ErrRateLimited})
	w := NewAnswerWorker(s, testPipeline(t, llm), gate, "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.True(t, res.RateLimited, "the 429 is recorded on the pass result")
	assert.Equal(t, 1, res.Spent, "the pass stops after the first rate-limited item, not draining all 3")
	assert.Equal(t, 0, s.completes[queue.StageAnswer], "the rate-limited item is not completed")
	assert.Equal(t, 1, s.retries, "the 429'd item is rescheduled (transient retry), not failed")
	assert.Equal(t, 0, s.fails[queue.StageAnswer], "a 429 is transient, never a terminal fail")
	assert.Equal(t, 1, gate.rateLimited, "the gate cooldown was armed for the next pass")
}

// TestAnswer_MissingQuestionFails: an item missing its upstream question payload is
// failed, not silently answered.
func TestAnswer_MissingQuestionFails(t *testing.T) {
	s := newFakeStore()
	s.add(1, "arxiv", "2501.1", queue.StageAnswer) // no question payload set
	fi := s.find(1)
	llm := scriptAnswerRoles(newFakeLLM())
	w := NewAnswerWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, res.Generated)
	require.NotNil(t, fi.it.FailedAt)
	assert.Equal(t, "answer", fi.it.FailStage)
}
