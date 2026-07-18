package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/pipeline"
)

// TestValidateQuestion_AcceptsValid confirms a fully-populated question (the shared
// fixture the presolve/answer workers read) passes the read-boundary validator.
func TestValidateQuestion_AcceptsValid(t *testing.T) {
	require.NoError(t, validateQuestion(1, validQuestion()))
}

// TestValidateQuestion_RejectsMissingReferenceSketch (D8): the answer stage's
// teacher-trajectory prompt (pipeline/lane.go teacherPrompt) consumes
// reference_answer_sketch, so an empty sketch is a contract violation caught at the
// read boundary -- before any teacher quota is spent, which is exactly what this
// pipeline exists to conserve.
func TestValidateQuestion_RejectsMissingReferenceSketch(t *testing.T) {
	q := validQuestion()
	q.ReferenceAnswerSketch = "   "
	err := validateQuestion(1, q)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reference_answer_sketch")
	assert.Contains(t, err.Error(), "contract violation")
}

// TestValidateQuestion_RejectsMissingCapabilityTags (D8): capability_tags is a
// required taxonomy array; an empty one is rejected before quota is spent.
func TestValidateQuestion_RejectsMissingCapabilityTags(t *testing.T) {
	q := validQuestion()
	q.CapabilityTags = nil
	err := validateQuestion(1, q)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "capability_tags")
	assert.Contains(t, err.Error(), "contract violation")
}

// TestValidateQuestion_RejectsInvalidEnums (D8): out-of-vocabulary taxonomy values
// are rejected here (terminal) via model.ValidateEnums, not at the loader INSERT
// during flush -- which is after the teacher quota has already been spent.
func TestValidateQuestion_RejectsInvalidEnums(t *testing.T) {
	cases := map[string]func(*pipeline.Question){
		"domain":         func(q *pipeline.Question) { q.Domain = "astrology" },
		"difficulty":     func(q *pipeline.Question) { q.Difficulty = "trivial" },
		"task_shape":     func(q *pipeline.Question) { q.TaskShape = "interpretive-dance" },
		"capability_tag": func(q *pipeline.Question) { q.CapabilityTags = []string{"telepathy"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			q := validQuestion()
			mutate(&q)
			err := validateQuestion(1, q)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "contract violation")
		})
	}
}

// TestValidatePresolveVerdict: enum values outside the schema vocabulary and an
// empty reason are rejected at the read boundary, so the answer stage never
// stamps corrupt filter provenance into records.
func TestValidatePresolveVerdict(t *testing.T) {
	valid := model.StudentFilterVerdict{Verdict: "uncertain", Method: "both", Reason: "close call"}
	require.NoError(t, validatePresolveVerdict(1, valid))
	disabled := model.StudentFilterVerdict{Verdict: "uncertain", Method: "disabled", Reason: "presolve off"}
	require.NoError(t, validatePresolveVerdict(1, disabled), "the disabled-presolve verdict is vocabulary-valid")

	cases := map[string]model.StudentFilterVerdict{
		"bad verdict":  {Verdict: "maybe", Method: "both", Reason: "r"},
		"bad method":   {Verdict: "fail", Method: "vibes", Reason: "r"},
		"empty reason": {Verdict: "fail", Method: "both", Reason: "  "},
		"zero value":   {},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			err := validatePresolveVerdict(1, v)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "contract violation")
		})
	}
}

// TestValidateVerifications: out-of-enum method/result values are rejected at the
// read boundary so flush projects only vocabulary-valid verification provenance.
func TestValidateVerifications(t *testing.T) {
	require.NoError(t, validateVerifications(1, []model.Verification{
		{Method: "exec", Result: "pass"}, {Method: "none", Result: "na"},
	}))
	require.Error(t, validateVerifications(1, []model.Verification{{Method: "guesswork", Result: "pass"}}))
	require.Error(t, validateVerifications(1, []model.Verification{{Method: "exec", Result: "perhaps"}}))
}

// TestValidateAnswerRecords_TrajectoryContract: a stored record missing the
// trajectory fields (reasoning summary, plan/self_check/quality_notes) or with an
// invalid source_lane is rejected before the verifier spends more quota on it.
func TestValidateAnswerRecords_TrajectoryContract(t *testing.T) {
	valid := model.DistillationPair{
		UserRequest: "q", FinalAnswer: "a", ReasoningSummary: "because",
		Plan: []string{"p"}, SelfCheck: []string{"s"}, QualityNotes: []string{"n"},
		SourceLane: "en",
	}
	require.NoError(t, validateAnswerRecords(1, []model.DistillationPair{valid}))

	noSummary := valid
	noSummary.ReasoningSummary = ""
	err := validateAnswerRecords(1, []model.DistillationPair{noSummary})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reasoning_summary")

	noPlan := valid
	noPlan.Plan = nil
	require.Error(t, validateAnswerRecords(1, []model.DistillationPair{noPlan}))

	badLane := valid
	badLane.SourceLane = "jp"
	err = validateAnswerRecords(1, []model.DistillationPair{badLane})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source_lane")
}
