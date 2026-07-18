package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullRecord returns a valid JSON record (map[string]any) containing all 15 fields.
// It mirrors one teacher-generated Q&A object (or one JSONL line fed to `distill load`).
func fullRecord() map[string]any {
	return map[string]any{
		"task_id":           "test-uuid-001",
		"domain":            "software-engineering",
		"difficulty":        "medium",
		"task_shape":        "code",
		"capability_tags":   []any{"reasoning", "generation"},
		"user_request":      "Implement a function that ...",
		"context":           "Given the following constraints ...",
		"success_criteria":  []any{"Passes all tests", "O(n) complexity"},
		"plan":              []any{"Step 1: Parse input", "Step 2: Process"},
		"reasoning_summary": "The approach uses dynamic programming ...",
		"final_answer":      "func solve() { ... }",
		"self_check":        []any{"Verified edge cases"},
		"quality_notes":     []any{"Could improve variable naming"},
		"references":        []any{"https://example.com/doc1"},
		"artifacts":         []any{"snippet.go"},
	}
}

func TestRecordToParams_RequiredFields(t *testing.T) {
	record := fullRecord()

	params, err := RecordToParams(record)

	require.NoError(t, err)

	// Verify all 13 required fields are mapped correctly.
	assert.Equal(t, "test-uuid-001", params["task_id"])
	assert.Equal(t, "software-engineering", params["domain"])
	assert.Equal(t, "medium", params["difficulty"])
	assert.Equal(t, "code", params["task_shape"])
	assert.Equal(t, []string{"reasoning", "generation"}, params["capability_tags"])
	assert.Equal(t, "Implement a function that ...", params["user_request"])
	assert.Equal(t, "Given the following constraints ...", params["context"])
	assert.Equal(t, []string{"Passes all tests", "O(n) complexity"}, params["success_criteria"])
	assert.Equal(t, []string{"Step 1: Parse input", "Step 2: Process"}, params["plan"])
	assert.Equal(t, "The approach uses dynamic programming ...", params["reasoning_summary"])
	assert.Equal(t, "func solve() { ... }", params["final_answer"])
	assert.Equal(t, []string{"Verified edge cases"}, params["self_check"])
	assert.Equal(t, []string{"Could improve variable naming"}, params["quality_notes"])
}

func TestRecordToParams_ReferencesMapping(t *testing.T) {
	// JSON key "references" must map to DB parameter key "references_"
	// because the DB column is references_ (PostgreSQL reserved word avoidance).
	record := fullRecord()

	params, err := RecordToParams(record)

	require.NoError(t, err)

	// "references" key in JSON -> "references_" key in params
	refs, ok := params["references_"]
	assert.True(t, ok, "params must contain key 'references_'")
	assert.Equal(t, []string{"https://example.com/doc1"}, refs)

	// "references" key must NOT exist in params (only "references_")
	_, hasPlain := params["references"]
	assert.False(t, hasPlain, "params must not contain key 'references' (only 'references_')")
}

func TestRecordToParams_OptionalNil(t *testing.T) {
	// When optional fields (references, artifacts) are absent from the record,
	// the corresponding params values must be nil.
	record := fullRecord()
	delete(record, "references")
	delete(record, "artifacts")

	params, err := RecordToParams(record)

	require.NoError(t, err)

	assert.Nil(t, params["references_"], "absent references should produce nil")
	assert.Nil(t, params["artifacts"], "absent artifacts should produce nil")
}

func TestRecordToParams_WrongStringType(t *testing.T) {
	// A required TEXT field with a non-string value must return a type error.
	record := fullRecord()
	record["task_id"] = 12345 // int instead of string

	_, err := RecordToParams(record)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "task_id")
	assert.Contains(t, err.Error(), "string")
}

func TestRecordToParams_WrongArrayType(t *testing.T) {
	// A required TEXT[] field with a non-array value must return a type error.
	record := fullRecord()
	record["capability_tags"] = "not-an-array" // string instead of []any

	_, err := RecordToParams(record)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "capability_tags")
}

func TestRecordToParams_ArrayWithNonStringElement(t *testing.T) {
	// An array element that is not a string must return an error.
	record := fullRecord()
	record["capability_tags"] = []any{"valid", 999} // int element

	_, err := RecordToParams(record)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "capability_tags")
}

func TestRecordToParams_ReferencesWrongType(t *testing.T) {
	// Optional "references" field with wrong type must return error.
	record := fullRecord()
	record["references"] = "not-an-array"

	_, err := RecordToParams(record)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "references")
}

func TestRecordToParams_ArtifactsWrongType(t *testing.T) {
	// Optional "artifacts" field with wrong type must return error.
	record := fullRecord()
	record["artifacts"] = "not-an-array"

	_, err := RecordToParams(record)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "artifacts")
}

func TestRecordToParams_MissingFieldError(t *testing.T) {
	// Each required field, when missing, must cause an error.
	requiredKeys := []string{
		"task_id", "domain", "difficulty", "task_shape",
		"capability_tags", "user_request", "context",
		"success_criteria", "plan", "reasoning_summary",
		"final_answer", "self_check", "quality_notes",
	}

	for _, key := range requiredKeys {
		t.Run(key, func(t *testing.T) {
			record := fullRecord()
			delete(record, key)

			_, err := RecordToParams(record)

			require.Error(t, err, "missing required field %q should produce error", key)
			assert.Contains(t, err.Error(), key, "error message should mention the missing field")
		})
	}
}

func TestValidateEnums_ValidRecord(t *testing.T) {
	params := map[string]any{
		"domain":          "software-engineering",
		"difficulty":      "hard",
		"task_shape":      "code",
		"capability_tags": []string{"reasoning", "generation"},
	}
	err := ValidateEnums(params)
	assert.NoError(t, err)
}

func TestValidateEnums_InvalidDomain(t *testing.T) {
	params := map[string]any{"domain": "invalid-domain"}
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid domain")
}

func TestValidateEnums_InvalidDifficulty(t *testing.T) {
	params := map[string]any{"difficulty": "extreme"}
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid difficulty")
}

func TestValidateEnums_InvalidTaskShape(t *testing.T) {
	params := map[string]any{"task_shape": "unknown-shape"}
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid task_shape")
}

func TestValidateEnums_InvalidCapabilityTag(t *testing.T) {
	params := map[string]any{"capability_tags": []string{"reasoning", "telepathy"}}
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid capability_tag")
}

// --- M3 artifact-pipeline field tests ---

// m3Record returns a full artifact-based record (base fields + all M3 fields),
// mirroring one JSON object produced by the M3 reverse-generation pipeline.
func m3Record() map[string]any {
	rec := fullRecord()
	rec["source_lane"] = "ko"
	rec["pair_id"] = "pair-42"
	rec["artifact_refs"] = []any{
		map[string]any{
			"source_type":  "k8s",
			"doc_id":       "KEP-1234",
			"url":          "https://kep",
			"license":      "CC-BY-4.0",
			"locator":      "sec 3",
			"why_relevant": "describes lock ordering",
		},
	}
	rec["student_filter_verdict"] = map[string]any{
		"verdict": "uncertain", "method": "both", "reason": "disputed",
	}
	rec["verification"] = map[string]any{
		"method": "test", "result": "pass", "detail": "race test green",
	}
	rec["difficulty_mutations"] = []any{"fault_injection", "conflicting_constraint"}
	rec["teacher_model"] = "gpt-5.4"
	rec["teacher_provider"] = "openai"
	return rec
}

func TestRecordToParams_M3Fields(t *testing.T) {
	params, err := RecordToParams(m3Record())

	require.NoError(t, err)
	assert.Equal(t, "ko", params["source_lane"])
	assert.Equal(t, "pair-42", params["pair_id"])
	assert.Equal(t, "gpt-5.4", params["teacher_model"])
	assert.Equal(t, "openai", params["teacher_provider"])
	assert.Equal(t, []string{"fault_injection", "conflicting_constraint"}, params["difficulty_mutations"])
	// Nested fields keep their native JSON value for pgx JSONB encoding.
	assert.IsType(t, []any{}, params["artifact_refs"])
	assert.IsType(t, map[string]any{}, params["student_filter_verdict"])
	assert.IsType(t, map[string]any{}, params["verification"])
}

func TestRecordToParams_M3Absent_Nil(t *testing.T) {
	// A legacy taxonomy record (no M3 fields) maps all M3 params to nil.
	params, err := RecordToParams(fullRecord())

	require.NoError(t, err)
	for _, key := range []string{
		"source_lane", "pair_id", "artifact_refs", "student_filter_verdict",
		"verification", "difficulty_mutations", "teacher_model", "teacher_provider",
	} {
		assert.Nil(t, params[key], "absent %q should map to nil", key)
	}
}

// --- reasoning-capture field tests (todos sec 2-5-5) ---

func TestRecordToParams_ReasoningFields(t *testing.T) {
	rec := m3Record()
	rec["cot"] = "step-by-step in-band reasoning"
	rec["cot_raw"] = "raw provider reasoning_content"
	rec["has_raw_cot"] = true

	params, err := RecordToParams(rec)
	require.NoError(t, err)
	assert.Equal(t, "step-by-step in-band reasoning", params["cot"])
	assert.Equal(t, "raw provider reasoning_content", params["cot_raw"])
	assert.Equal(t, true, params["has_raw_cot"])
}

func TestRecordToParams_ReasoningAbsent_Nil(t *testing.T) {
	// A record without the reasoning fields maps them to nil (SQL NULL).
	params, err := RecordToParams(fullRecord())
	require.NoError(t, err)
	for _, key := range []string{"cot", "cot_raw", "has_raw_cot"} {
		assert.Nil(t, params[key], "absent %q should map to nil", key)
	}
}

func TestRecordToParams_CotRawExplicitNull(t *testing.T) {
	// cot_raw is nullable: an explicit JSON null (nil in the decoded map) maps
	// to nil, matching a summary-only subscription provider.
	rec := m3Record()
	rec["cot"] = "in-band only"
	rec["cot_raw"] = nil
	rec["has_raw_cot"] = false

	params, err := RecordToParams(rec)
	require.NoError(t, err)
	assert.Equal(t, "in-band only", params["cot"])
	assert.Nil(t, params["cot_raw"])
	assert.Equal(t, false, params["has_raw_cot"])
}

func TestRecordToParams_HasRawCoTWrongType(t *testing.T) {
	rec := m3Record()
	rec["has_raw_cot"] = "yes" // must be a boolean
	_, err := RecordToParams(rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has_raw_cot")
}

func TestRecordToParams_CotWrongType(t *testing.T) {
	rec := m3Record()
	rec["cot"] = 42 // must be a string
	_, err := RecordToParams(rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cot")
}

func TestRecordToParams_SourceLaneWrongType(t *testing.T) {
	rec := m3Record()
	rec["source_lane"] = 123
	_, err := RecordToParams(rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source_lane")
}

func TestRecordToParams_ArtifactRefsWrongType(t *testing.T) {
	rec := m3Record()
	rec["artifact_refs"] = "not-an-array"
	_, err := RecordToParams(rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "artifact_refs")
}

func TestRecordToParams_VerificationWrongType(t *testing.T) {
	rec := m3Record()
	rec["verification"] = "not-an-object"
	_, err := RecordToParams(rec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verification")
}

func TestValidateEnums_M3Valid(t *testing.T) {
	params, err := RecordToParams(m3Record())
	require.NoError(t, err)
	assert.NoError(t, ValidateEnums(params))
}

func TestValidateEnums_LegacyRecordNoM3(t *testing.T) {
	// A legacy record (no M3 fields) must still pass ValidateEnums.
	params, err := RecordToParams(fullRecord())
	require.NoError(t, err)
	assert.NoError(t, ValidateEnums(params))
}

func TestValidateEnums_InvalidSourceLane(t *testing.T) {
	params, _ := RecordToParams(m3Record())
	params["source_lane"] = "jp"
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid source_lane")
}

func TestValidateEnums_InvalidArtifactSourceType(t *testing.T) {
	rec := m3Record()
	rec["artifact_refs"] = []any{map[string]any{
		"source_type": "wikipedia", "doc_id": "d1", "license": "CC", "why_relevant": "r",
	}}
	params, err := RecordToParams(rec)
	require.NoError(t, err)
	err = ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source_type")
}

func TestValidateEnums_MissingArtifactRefLicense(t *testing.T) {
	rec := m3Record()
	rec["artifact_refs"] = []any{map[string]any{
		"source_type": "arxiv", "doc_id": "d1", "why_relevant": "r", // license missing
	}}
	params, err := RecordToParams(rec)
	require.NoError(t, err)
	err = ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "license")
}

func TestValidateEnums_InvalidStudentVerdict(t *testing.T) {
	rec := m3Record()
	rec["student_filter_verdict"] = map[string]any{
		"verdict": "maybe", "method": "both", "reason": "x",
	}
	params, _ := RecordToParams(rec)
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verdict")
}

func TestValidateEnums_InvalidStudentMethod(t *testing.T) {
	rec := m3Record()
	rec["student_filter_verdict"] = map[string]any{
		"verdict": "fail", "method": "vibes", "reason": "x",
	}
	params, _ := RecordToParams(rec)
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "method")
}

func TestValidateEnums_InvalidVerificationMethod(t *testing.T) {
	rec := m3Record()
	rec["verification"] = map[string]any{"method": "telepathy", "result": "pass"}
	params, _ := RecordToParams(rec)
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verification.method")
}

func TestValidateEnums_InvalidVerificationResult(t *testing.T) {
	rec := m3Record()
	rec["verification"] = map[string]any{"method": "test", "result": "maybe"}
	params, _ := RecordToParams(rec)
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verification.result")
}

func TestValidateEnums_InvalidDifficultyMutation(t *testing.T) {
	rec := m3Record()
	rec["difficulty_mutations"] = []any{"fault_injection", "teleport"}
	params, _ := RecordToParams(rec)
	err := ValidateEnums(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid difficulty_mutation")
}
