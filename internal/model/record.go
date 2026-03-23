// Package model defines data types shared across the distillation pipeline.
package model

import (
	"fmt"
	"time"
)

// DistillationPair represents a single Q&A pair stored in the distillation_pairs table.
// Field tags follow the 3-stage mapping:
//
//	JSON input:  "references"   (json tag)
//	DB column:   "references_"  (PostgreSQL reserved-word avoidance)
//	Parquet out: "references"   (restored original name)
type DistillationPair struct {
	TaskID           string    `json:"task_id"`
	Domain           string    `json:"domain"`
	Difficulty       string    `json:"difficulty"`
	TaskShape        string    `json:"task_shape"`
	CapabilityTags   []string  `json:"capability_tags"`
	UserRequest      string    `json:"user_request"`
	Context          string    `json:"context"`
	SuccessCriteria  []string  `json:"success_criteria"`
	Plan             []string  `json:"plan"`
	ReasoningSummary string    `json:"reasoning_summary"`
	FinalAnswer      string    `json:"final_answer"`
	SelfCheck        []string  `json:"self_check"`
	QualityNotes     []string  `json:"quality_notes"`
	References       []string  `json:"references"`  // nullable in DB (references_ column)
	Artifacts        []string  `json:"artifacts"`    // nullable in DB
	CreatedAt        *time.Time `json:"created_at"`  // populated by DB DEFAULT NOW()
}

// requiredStringFields lists the required TEXT column keys in the JSON record.
var requiredStringFields = []string{
	"task_id", "domain", "difficulty", "task_shape",
	"user_request", "context", "reasoning_summary",
	"final_answer",
}

// requiredArrayFields lists the required TEXT[] column keys in the JSON record.
var requiredArrayFields = []string{
	"capability_tags", "success_criteria", "plan",
	"self_check", "quality_notes",
}

// Enum values from schemas/distillation.schema.json and config/taxonomy.yaml.
// Used by ValidateEnums to reject invalid taxonomy values before DB insert.
var (
	ValidDomains = map[string]bool{
		"software-engineering": true, "data-science": true, "mathematics": true,
		"natural-science": true, "finance": true, "business": true,
		"legal-compliance": true, "education": true, "creative-writing": true,
		"technical-writing": true, "linguistics": true, "philosophy-ethics": true,
		"general-knowledge": true,
	}
	ValidDifficulties = map[string]bool{
		"easy": true, "medium": true, "hard": true,
	}
	ValidTaskShapes = map[string]bool{
		"short-text": true, "long-text": true, "code": true,
		"structured-data": true, "analysis-report": true, "step-by-step": true,
	}
	ValidCapabilities = map[string]bool{
		"reasoning": true, "knowledge-recall": true, "generation": true,
		"transformation": true, "evaluation": true, "planning": true,
		"problem-solving": true, "instruction-following": true,
	}
)

// ValidateEnums checks that domain, difficulty, task_shape, and capability_tags
// values are valid taxonomy enum values. Call after RecordToParams succeeds.
func ValidateEnums(params map[string]any) error {
	if domain, ok := params["domain"].(string); ok {
		if !ValidDomains[domain] {
			return fmt.Errorf("invalid domain %q", domain)
		}
	}
	if diff, ok := params["difficulty"].(string); ok {
		if !ValidDifficulties[diff] {
			return fmt.Errorf("invalid difficulty %q", diff)
		}
	}
	if shape, ok := params["task_shape"].(string); ok {
		if !ValidTaskShapes[shape] {
			return fmt.Errorf("invalid task_shape %q", shape)
		}
	}
	if tags, ok := params["capability_tags"].([]string); ok {
		for _, tag := range tags {
			if !ValidCapabilities[tag] {
				return fmt.Errorf("invalid capability_tag %q", tag)
			}
		}
	}
	return nil
}

// RecordToParams converts a JSON record (map[string]any, one JSONL line) into a
// parameter map suitable for DB INSERT. This is the Go equivalent of Python's
// record_to_params() in load_to_db.py.
//
// Key mapping: JSON "references" -> params "references_" (DB column name).
// Optional fields (references, artifacts): nil when absent.
// Required fields: returns error when missing.
func RecordToParams(record map[string]any) (map[string]any, error) {
	params := make(map[string]any, 15)

	// Required TEXT fields.
	for _, key := range requiredStringFields {
		val, ok := record[key]
		if !ok {
			return nil, fmt.Errorf("missing required field %q", key)
		}
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("field %q must be a string, got %T", key, val)
		}
		params[key] = s
	}

	// Required TEXT[] fields.
	for _, key := range requiredArrayFields {
		val, ok := record[key]
		if !ok {
			return nil, fmt.Errorf("missing required field %q", key)
		}
		arr, err := toStringSlice(key, val)
		if err != nil {
			return nil, err
		}
		params[key] = arr
	}

	// Optional TEXT[] fields.
	// "references" in JSON -> "references_" in DB params.
	if val, ok := record["references"]; ok {
		arr, err := toStringSlice("references", val)
		if err != nil {
			return nil, err
		}
		params["references_"] = arr
	} else {
		params["references_"] = nil
	}

	if val, ok := record["artifacts"]; ok {
		arr, err := toStringSlice("artifacts", val)
		if err != nil {
			return nil, err
		}
		params["artifacts"] = arr
	} else {
		params["artifacts"] = nil
	}

	return params, nil
}

// toStringSlice converts a JSON-decoded []any (array of interface{}) to []string.
// JSON decoding with encoding/json produces []any for arrays.
func toStringSlice(fieldName string, val any) ([]string, error) {
	raw, ok := val.([]any)
	if !ok {
		return nil, fmt.Errorf("field %q must be an array, got %T", fieldName, val)
	}
	result := make([]string, len(raw))
	for i, elem := range raw {
		s, ok := elem.(string)
		if !ok {
			return nil, fmt.Errorf("field %q[%d] must be a string, got %T", fieldName, i, elem)
		}
		result[i] = s
	}
	return result, nil
}
