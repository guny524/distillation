package worker

import (
	"fmt"
	"strings"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/pipeline"
)

// This file holds the stage-payload read-boundary validators (todos #3). A NULL
// column is already rejected by the store's getJSON/getText, but a JSON `null`,
// an empty object `{}`, or a structurally-valid-but-empty payload decodes to a
// zero value and would otherwise pass silently -- e.g. an empty digest sent to
// the question LLM, or an answer array with no records. These validators run at
// the point a worker reads an upstream payload and reject such payloads.
//
// A validation failure is a CONTRACT error, deliberately NOT wrapped as a
// transient failure: the classifier (worker.go) then fails the item terminally
// rather than retrying, because re-reading the same empty payload will never
// succeed (the fault is upstream, not a temporary endpoint blip).

// validateDigest rejects an empty/whitespace comprehension digest.
func validateDigest(id int64, digest string) error {
	if strings.TrimSpace(digest) == "" {
		return fmt.Errorf("worker: comprehension_digest for item %d is empty (contract violation)", id)
	}
	return nil
}

// validateQuestion rejects a question missing any field the downstream stages
// (presolve student filter, answer teacher trajectory) require: the request/
// context text, the taxonomy classification, success criteria, capability tags,
// and the reference answer sketch. It then validates the taxonomy ENUM values so
// an out-of-vocabulary classification is caught at the read boundary (terminal),
// not at the loader INSERT during flush -- which is AFTER the teacher quota was
// already spent, wasting the exact resource this pipeline exists to conserve.
//
// This mirrors the generation-time contract (pipeline.validateClassification +
// backtranslate) at the READ boundary, so a corrupted or partially-written
// `question` JSONB payload can never drive a teacher call.
func validateQuestion(id int64, q pipeline.Question) error {
	missing := make([]string, 0, 8)
	if strings.TrimSpace(q.UserRequest) == "" {
		missing = append(missing, "user_request")
	}
	if strings.TrimSpace(q.Context) == "" {
		missing = append(missing, "context")
	}
	if strings.TrimSpace(q.Domain) == "" {
		missing = append(missing, "domain")
	}
	if strings.TrimSpace(q.Difficulty) == "" {
		missing = append(missing, "difficulty")
	}
	if strings.TrimSpace(q.TaskShape) == "" {
		missing = append(missing, "task_shape")
	}
	if len(q.SuccessCriteria) == 0 {
		missing = append(missing, "success_criteria")
	}
	if len(q.CapabilityTags) == 0 {
		missing = append(missing, "capability_tags")
	}
	// reference_answer_sketch is consumed by the answer stage's teacher-trajectory
	// prompt (pipeline/lane.go teacherPrompt) and the presolve student filter, so an
	// empty sketch silently degrades both. Reject it before any quota is spent.
	if strings.TrimSpace(q.ReferenceAnswerSketch) == "" {
		missing = append(missing, "reference_answer_sketch")
	}
	if len(missing) > 0 {
		return fmt.Errorf("worker: question for item %d missing required field(s) %s (contract violation)",
			id, strings.Join(missing, ", "))
	}
	// All required fields present: validate the enum VALUES via the shared model
	// validator (the same check the loader applies), so an invalid taxonomy value
	// fails the item terminally here rather than after quota is spent.
	if err := model.ValidateEnums(map[string]any{
		"domain":          q.Domain,
		"difficulty":      q.Difficulty,
		"task_shape":      q.TaskShape,
		"capability_tags": q.CapabilityTags,
	}); err != nil {
		return fmt.Errorf("worker: question for item %d has %v (contract violation)", id, err)
	}
	return nil
}

// validateAnswerRecords rejects an empty answer array and any record missing the
// trajectory contract the schema requires (request/answer text, reasoning
// summary, plan/self_check/quality_notes, a valid source_lane). This guards the
// verify and flush stages against a null/partial answer_payload -- the same
// non-empty rules pipeline.validateTrajectory enforced at generation time,
// re-checked at the READ boundary so a corrupted payload fails here, not at the
// flush INSERT after the verifier quota was also spent.
func validateAnswerRecords(id int64, recs []model.DistillationPair) error {
	if len(recs) == 0 {
		return fmt.Errorf("worker: answer_payload for item %d has no lane records (contract violation)", id)
	}
	for i, rec := range recs {
		missing := make([]string, 0, 4)
		if strings.TrimSpace(rec.UserRequest) == "" {
			missing = append(missing, "user_request")
		}
		if strings.TrimSpace(rec.FinalAnswer) == "" {
			missing = append(missing, "final_answer")
		}
		if strings.TrimSpace(rec.ReasoningSummary) == "" {
			missing = append(missing, "reasoning_summary")
		}
		if len(rec.Plan) == 0 || len(rec.SelfCheck) == 0 || len(rec.QualityNotes) == 0 {
			missing = append(missing, "plan/self_check/quality_notes")
		}
		if len(missing) > 0 {
			return fmt.Errorf("worker: answer_payload for item %d lane %d missing %s (contract violation)",
				id, i, strings.Join(missing, ", "))
		}
		if !model.ValidSourceLanes[rec.SourceLane] {
			return fmt.Errorf("worker: answer_payload for item %d lane %d has invalid source_lane %q (contract violation)", id, i, rec.SourceLane)
		}
	}
	return nil
}

// validatePresolveVerdict rejects a stored verdict whose enum values fall outside
// the schema vocabulary (or whose reason is empty), so the answer stage never
// stamps corrupt filter provenance into records. Mirrors schema
// $defs/studentFilterVerdict at the read boundary.
func validatePresolveVerdict(id int64, v model.StudentFilterVerdict) error {
	if !model.ValidStudentVerdicts[v.Verdict] {
		return fmt.Errorf("worker: presolve_verdict for item %d has invalid verdict %q (contract violation)", id, v.Verdict)
	}
	if !model.ValidStudentFilterMethods[v.Method] {
		return fmt.Errorf("worker: presolve_verdict for item %d has invalid method %q (contract violation)", id, v.Method)
	}
	if strings.TrimSpace(v.Reason) == "" {
		return fmt.Errorf("worker: presolve_verdict for item %d has an empty reason (contract violation)", id)
	}
	return nil
}

// validateVerifications rejects a stored verification list with out-of-enum
// method/result values (schema $defs/verification) so flush projects only
// vocabulary-valid verification provenance.
func validateVerifications(id int64, vs []model.Verification) error {
	for i, v := range vs {
		if !model.ValidVerificationMethods[v.Method] {
			return fmt.Errorf("worker: verification for item %d entry %d has invalid method %q (contract violation)", id, i, v.Method)
		}
		if !model.ValidVerificationResults[v.Result] {
			return fmt.Errorf("worker: verification for item %d entry %d has invalid result %q (contract violation)", id, i, v.Result)
		}
	}
	return nil
}
