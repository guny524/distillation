package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/model"
)

func laneConfig() Config {
	cfg := DefaultConfig()
	cfg.TeacherModel = "gpt-5.4"
	cfg.TeacherProvider = "codex"
	return cfg
}

// TestLane_PairsBothLanes: an English question is translated to Korean, both
// lanes get a teacher trajectory, and the two records share a pair_id with
// distinct per-lane task ids and stamped provenance/metadata.
func TestLane_PairsBothLanes(t *testing.T) {
	llm := newFakeLLM().
		script("translator", ok(translateJSON)).
		script("teacher", ok(teacherTrajectoryJSON), ok(teacherTrajectoryJSON))
	p := newTestPipeline(t, llm, laneConfig())

	q := baseQuestion() // en
	q.ArtifactRefs = []model.ArtifactRef{{
		SourceType: "arxiv", DocID: "2401.00001", License: "arXiv-nonexclusive",
		WhyRelevant: "grounds the question",
	}}
	q.DifficultyMutations = []string{"conflicting_constraint"}
	verdict := model.StudentFilterVerdict{Verdict: "fail", Method: "both", Reason: "wrong"}

	res, err := p.lane(context.Background(), q, verdict, "pair-1")
	require.NoError(t, err)
	require.Len(t, res.Records, 2)
	assert.Equal(t, 2, res.TeacherCalls)

	byLane := map[string]model.DistillationPair{}
	for _, r := range res.Records {
		byLane[r.SourceLane] = r
	}
	require.Contains(t, byLane, "ko")
	require.Contains(t, byLane, "en")

	ko, en := byLane["ko"], byLane["en"]
	assert.Equal(t, "pair-1", ko.PairID)
	assert.Equal(t, "pair-1", en.PairID)
	assert.Equal(t, "pair-1-ko", ko.TaskID)
	assert.Equal(t, "pair-1-en", en.TaskID)

	// Korean lane carries the translated question; English keeps the original.
	assert.Contains(t, ko.UserRequest, "그래디언트")
	assert.Contains(t, en.UserRequest, "exploding gradients")

	// Both lanes carry the same provenance/verdict/metadata.
	for _, r := range res.Records {
		require.Len(t, r.ArtifactRefs, 1)
		assert.Equal(t, []string{"conflicting_constraint"}, r.DifficultyMutations)
		require.NotNil(t, r.StudentFilterVerdict)
		assert.Equal(t, "fail", r.StudentFilterVerdict.Verdict)
		assert.Equal(t, "gpt-5.4", r.TeacherModel)
		assert.Equal(t, "codex", r.TeacherProvider)
		assert.NotEmpty(t, r.Plan)
		assert.NotEmpty(t, r.FinalAnswer)
	}
	assert.Equal(t, 1, llm.callsFor("translator"))
	assert.Equal(t, 2, llm.callsFor("teacher"))
}

// TestLane_KoOnlyInputTranslatesToEn: a Korean question produces the English
// lane via the translator, still yielding a ko/en pair.
func TestLane_KoOnlyInputTranslatesToEn(t *testing.T) {
	enTranslation := `{"user_request": "Bound exploding gradients and implement a guard.",
	  "context": "A diverging training loop.", "success_criteria": ["clip"],
	  "reference_answer_sketch": "norm clip"}`
	llm := newFakeLLM().
		script("translator", ok(enTranslation)).
		script("teacher", ok(teacherTrajectoryJSON), ok(teacherTrajectoryJSON))
	p := newTestPipeline(t, llm, laneConfig())

	q := baseQuestion()
	q.UserRequest = "학습 중 폭주하는 그래디언트를 제한하라."
	q.Lang = "ko"

	res, err := p.lane(context.Background(), q, model.StudentFilterVerdict{Verdict: "uncertain", Method: "both", Reason: "x"}, "pair-2")
	require.NoError(t, err)
	require.Len(t, res.Records, 2)

	byLane := map[string]model.DistillationPair{}
	for _, r := range res.Records {
		byLane[r.SourceLane] = r
	}
	assert.Contains(t, byLane["ko"].UserRequest, "그래디언트")
	assert.Contains(t, byLane["en"].UserRequest, "exploding gradients")
}

// TestLane_TeacherEmptyTrajectoryRejected: a teacher response missing required
// arrays fails (never produces a schema-violating record).
func TestLane_TeacherEmptyTrajectoryRejected(t *testing.T) {
	badTrajectory := `{"plan": [], "reasoning_summary": "x", "final_answer": "y", "self_check": ["a"], "quality_notes": ["b"]}`
	llm := newFakeLLM().
		script("translator", ok(translateJSON)).
		script("teacher", ok(badTrajectory))
	p := newTestPipeline(t, llm, laneConfig())

	_, err := p.lane(context.Background(), baseQuestion(), model.StudentFilterVerdict{Verdict: "fail", Method: "both", Reason: "x"}, "pair-3")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty plan")
}
