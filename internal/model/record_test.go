package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullRecord returns a valid JSON record (map[string]any) containing all 15 fields.
// It mirrors a single line from result.jsonl that codex produces.
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
