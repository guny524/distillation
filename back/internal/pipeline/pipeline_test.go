package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullPipelineLLM scripts every stage of a happy-path run that routes to the
// teacher (judge fail) and passes verification on both lanes.
func fullPipelineLLM() *fakeLLM {
	return newFakeLLM().
		script("generator", ok(backtranslateJSON), ok(mutateJSON)).
		script("student", ok(studentAnswerA), ok(studentAnswerA), ok(studentAnswerA)).
		script("judge", ok(judgeFailJSON)).
		script("translator", ok(translateJSON)).
		script("teacher", ok(teacherTrajectoryJSON), ok(teacherTrajectoryJSON)).
		script("verifier", ok(verifyPassJSON), ok(verifyPassJSON))
}

// TestRun_ProducesKoEnPair: an artifact flows through every stage and yields the
// ko/en record pair with provenance, mutations, verdict, and verification.
func TestRun_ProducesKoEnPair(t *testing.T) {
	p := newTestPipeline(t, fullPipelineLLM(), laneConfig())

	res, err := p.Run(context.Background(), sampleArtifact(), "distill-20260713-000000-00")
	require.NoError(t, err)
	assert.False(t, res.Skipped)
	require.Len(t, res.Records, 2)
	assert.Equal(t, 2, res.TeacherCalls)

	byLane := map[string]bool{}
	for _, r := range res.Records {
		byLane[r.SourceLane] = true
		assert.Equal(t, "distill-20260713-000000-00", r.PairID)
		require.Len(t, r.ArtifactRefs, 1)
		assert.NotEmpty(t, r.ArtifactRefs[0].WhyRelevant)
		assert.Equal(t, []string{"conflicting_constraint"}, r.DifficultyMutations)
		require.NotNil(t, r.StudentFilterVerdict)
		assert.Equal(t, "fail", r.StudentFilterVerdict.Verdict)
		require.NotNil(t, r.Verification)
		assert.Equal(t, "pass", r.Verification.Result)
	}
	assert.True(t, byLane["ko"])
	assert.True(t, byLane["en"])
}

// TestRun_StudentPassSkips: when the student pre-filter passes, the pipeline
// drops the question before any teacher/verifier call.
func TestRun_StudentPassSkips(t *testing.T) {
	llm := newFakeLLM().
		script("generator", ok(backtranslateJSON), ok(mutateJSON)).
		script("student", ok(studentAnswerA), ok(studentAnswerA), ok(studentAnswerA)).
		script("judge", ok(judgePassJSON))
	p := newTestPipeline(t, llm, laneConfig())

	res, err := p.Run(context.Background(), sampleArtifact(), "pair-skip")
	require.NoError(t, err)
	assert.True(t, res.Skipped)
	assert.Empty(t, res.Records)
	assert.Equal(t, 0, res.TeacherCalls)
	assert.Equal(t, 0, llm.callsFor("teacher"))
	assert.Equal(t, 0, llm.callsFor("translator"))
	assert.Equal(t, 0, llm.callsFor("verifier"))
}

// TestRun_VerifierDropsFailingLane: a lane whose trajectory fails verification
// is excluded; the passing lane is kept.
func TestRun_VerifierDropsFailingLane(t *testing.T) {
	llm := newFakeLLM().
		script("generator", ok(backtranslateJSON), ok(mutateJSON)).
		script("student", ok(studentAnswerA), ok(studentAnswerA), ok(studentAnswerA)).
		script("judge", ok(judgeFailJSON)).
		script("translator", ok(translateJSON)).
		script("teacher", ok(teacherTrajectoryJSON), ok(teacherTrajectoryJSON)).
		script("verifier", ok(verifyPassJSON), ok(verifyFailJSON))
	p := newTestPipeline(t, llm, laneConfig())

	res, err := p.Run(context.Background(), sampleArtifact(), "pair-mixed")
	require.NoError(t, err)
	require.Len(t, res.Records, 1, "one lane failed verification and was dropped")
}

// TestRun_ToParamsValid: a produced record converts to valid DB insert params,
// exercising the schema/enum validation reuse (RecordToParams + ValidateEnums)
// with the M3 dependentRequired fields (source_lane/pair_id/student_filter_verdict).
func TestRun_ToParamsValid(t *testing.T) {
	p := newTestPipeline(t, fullPipelineLLM(), laneConfig())
	res, err := p.Run(context.Background(), sampleArtifact(), "pair-params")
	require.NoError(t, err)
	require.NotEmpty(t, res.Records)

	params, err := ToParams(res.Records[0])
	require.NoError(t, err)

	assert.NotEmpty(t, params["task_id"])
	assert.Contains(t, []any{"ko", "en"}, params["source_lane"])
	assert.Equal(t, "pair-params", params["pair_id"])
	// JSONB fields keep native JSON shapes for pgx.
	require.IsType(t, []any{}, params["artifact_refs"])
	require.IsType(t, map[string]any{}, params["student_filter_verdict"])
	require.IsType(t, map[string]any{}, params["verification"])
	// Null optional columns are dropped, not passed as malformed values.
	assert.Nil(t, params["references_"])
	assert.Nil(t, params["artifacts"])
}

// TestToParams_RejectsInvalidEnum: ToParams surfaces enum violations (defense in
// depth on top of the stage-level classification checks).
func TestToParams_RejectsInvalidEnum(t *testing.T) {
	rec := verifiableRecord()
	rec.TaskID = "t-1"
	rec.Domain = "not-a-domain"
	rec.Difficulty = "medium"
	rec.CapabilityTags = []string{"reasoning"}
	rec.Context = "c"
	rec.Plan = []string{"p"}
	rec.ReasoningSummary = "r"
	rec.QualityNotes = []string{"q"}
	_, err := ToParams(rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "domain")
}
