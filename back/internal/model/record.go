// Package model defines data types shared across the distillation pipeline.
package model

import (
	"fmt"
	"strings"
	"time"
)

// DistillationPair represents a single Q&A pair stored in the distillation_pairs table.
// Field tags follow the 3-stage mapping:
//
//	JSON input:  "references"   (json tag)
//	DB column:   "references_"  (PostgreSQL reserved-word avoidance)
//	Parquet out: "references"   (restored original name)
//
// The M3 fields (SourceLane .. TeacherProvider) are populated by the artifact
// reverse-generation pipeline; the legacy taxonomy runner leaves them zero.
// They are optional at the DB/Go level (nullable columns) for backward
// compatibility; the schema contract marks them required for artifact-based
// records via dependentRequired (schemas/distillation.schema.json).
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
	References       []string  `json:"references"` // nullable in DB (references_ column)
	Artifacts        []string  `json:"artifacts"`  // nullable in DB

	// M3 artifact-pipeline fields (all nullable in DB).
	SourceLane           string                `json:"source_lane,omitempty"`
	PairID               string                `json:"pair_id,omitempty"`
	ArtifactRefs         []ArtifactRef         `json:"artifact_refs,omitempty"`
	StudentFilterVerdict *StudentFilterVerdict `json:"student_filter_verdict,omitempty"`
	Verification         *Verification         `json:"verification,omitempty"`
	DifficultyMutations  []string              `json:"difficulty_mutations,omitempty"`
	TeacherModel         string                `json:"teacher_model,omitempty"`
	TeacherProvider      string                `json:"teacher_provider,omitempty"`

	// Reasoning capture (todos sec 2-5-5), all nullable in DB. Cot is the
	// in-band chain-of-thought forced by the prompt (all sources) and supersedes
	// ReasoningSummary (both coexist during migration). CotRaw is the provider's
	// raw message.reasoning_content (open-weight teachers only); HasRawCoT flags
	// whether CotRaw is populated.
	Cot       string  `json:"cot,omitempty"`
	CotRaw    *string `json:"cot_raw,omitempty"`
	HasRawCoT *bool   `json:"has_raw_cot,omitempty"`

	CreatedAt *time.Time `json:"created_at"` // populated by DB DEFAULT NOW()
}

// ArtifactRef is one reverse-generation source reference: a citation ID plus
// re-expression rationale, never a verbatim copy of the source (copyright).
type ArtifactRef struct {
	SourceType  string `json:"source_type"`
	DocID       string `json:"doc_id"`
	URL         string `json:"url,omitempty"`
	License     string `json:"license"`
	Locator     string `json:"locator,omitempty"`
	WhyRelevant string `json:"why_relevant"`
}

// StudentFilterVerdict records why a pair was routed to the teacher.
type StudentFilterVerdict struct {
	Verdict                     string   `json:"verdict"`
	Method                      string   `json:"method"`
	JudgeScore                  *float64 `json:"judge_score,omitempty"`
	SelfConsistencyDisagreement *float64 `json:"self_consistency_disagreement,omitempty"`
	Reason                      string   `json:"reason"`
}

// Verification records the teacher-trajectory verification result.
type Verification struct {
	Method string `json:"method"`
	Result string `json:"result"`
	Detail string `json:"detail,omitempty"`
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

	// M3 artifact-pipeline enums (schemas/distillation.schema.json $defs).
	// ValidArtifactSourceTypes: "k8s" covers Kubernetes docs/KEP + cloud-native
	// OSS GitHub issues/PRs (todos sec 2-3 ingestion checkbox).
	ValidSourceLanes = map[string]bool{
		"ko": true, "en": true,
	}
	ValidArtifactSourceTypes = map[string]bool{
		"arxiv": true, "stackexchange": true, "k8s": true, "pmc": true,
	}
	ValidStudentVerdicts = map[string]bool{
		"pass": true, "fail": true, "uncertain": true,
	}
	// "disabled" records that presolve was configured off for this item: no
	// student/judge call was made, so student capability is unknown (verdict
	// "uncertain") -- distinguishable in the dataset from a measured uncertain.
	ValidStudentFilterMethods = map[string]bool{
		"judge_ref": true, "self_consistency": true, "both": true, "disabled": true,
	}
	ValidVerificationMethods = map[string]bool{
		"test": true, "rule": true, "exec": true, "none": true,
	}
	ValidVerificationResults = map[string]bool{
		"pass": true, "fail": true, "na": true,
	}
	// ValidDifficultyMutations is a controlled vocabulary seeded with the three
	// named operators; the M3 mutation step extends it as it adds operators.
	ValidDifficultyMutations = map[string]bool{
		"fault_injection": true, "partial_info_removal": true, "conflicting_constraint": true,
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
	return validateM3Enums(params)
}

// validateM3Enums validates the optional artifact-pipeline fields when present
// (non-nil in params). Absent fields are skipped, keeping legacy taxonomy
// records valid. Nested objects are validated for required subfields and enums.
func validateM3Enums(params map[string]any) error {
	if lane, ok := params["source_lane"].(string); ok {
		if !ValidSourceLanes[lane] {
			return fmt.Errorf("invalid source_lane %q", lane)
		}
	}
	if muts, ok := params["difficulty_mutations"].([]string); ok {
		for _, m := range muts {
			if !ValidDifficultyMutations[m] {
				return fmt.Errorf("invalid difficulty_mutation %q", m)
			}
		}
	}
	if refs, ok := params["artifact_refs"].([]any); ok {
		if err := validateArtifactRefs(refs); err != nil {
			return err
		}
	}
	if v, ok := params["student_filter_verdict"].(map[string]any); ok {
		if err := validateStudentFilterVerdict(v); err != nil {
			return err
		}
	}
	if v, ok := params["verification"].(map[string]any); ok {
		if err := validateVerification(v); err != nil {
			return err
		}
	}
	return nil
}

// validateArtifactRefs checks each ref has the required non-empty string
// subfields and a valid source_type enum.
func validateArtifactRefs(refs []any) error {
	for i, elem := range refs {
		ref, ok := elem.(map[string]any)
		if !ok {
			return fmt.Errorf("artifact_refs[%d] must be an object, got %T", i, elem)
		}
		st, err := requiredString(ref, fmt.Sprintf("artifact_refs[%d].source_type", i))
		if err != nil {
			return err
		}
		if !ValidArtifactSourceTypes[st] {
			return fmt.Errorf("invalid artifact_refs[%d].source_type %q", i, st)
		}
		for _, key := range []string{"doc_id", "license", "why_relevant"} {
			if _, err := requiredString(ref, fmt.Sprintf("artifact_refs[%d].%s", i, key)); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateStudentFilterVerdict checks required subfields and verdict/method enums.
func validateStudentFilterVerdict(v map[string]any) error {
	verdict, err := requiredString(v, "student_filter_verdict.verdict")
	if err != nil {
		return err
	}
	if !ValidStudentVerdicts[verdict] {
		return fmt.Errorf("invalid student_filter_verdict.verdict %q", verdict)
	}
	method, err := requiredString(v, "student_filter_verdict.method")
	if err != nil {
		return err
	}
	if !ValidStudentFilterMethods[method] {
		return fmt.Errorf("invalid student_filter_verdict.method %q", method)
	}
	_, err = requiredString(v, "student_filter_verdict.reason")
	return err
}

// validateVerification checks required subfields and method/result enums.
func validateVerification(v map[string]any) error {
	method, err := requiredString(v, "verification.method")
	if err != nil {
		return err
	}
	if !ValidVerificationMethods[method] {
		return fmt.Errorf("invalid verification.method %q", method)
	}
	result, err := requiredString(v, "verification.result")
	if err != nil {
		return err
	}
	if !ValidVerificationResults[result] {
		return fmt.Errorf("invalid verification.result %q", result)
	}
	return nil
}

// requiredString reads a required non-empty string subfield from a nested object.
// The dotted path is used verbatim in error messages for precise diagnostics.
func requiredString(obj map[string]any, path string) (string, error) {
	key := path[strings.LastIndexByte(path, '.')+1:]
	val, ok := obj[key]
	if !ok {
		return "", fmt.Errorf("missing required field %q", path)
	}
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("field %q must be a string, got %T", path, val)
	}
	if s == "" {
		return "", fmt.Errorf("field %q must be non-empty", path)
	}
	return s, nil
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

	if err := m3ParamsFromRecord(record, params); err != nil {
		return nil, err
	}

	if err := reasoningParamsFromRecord(record, params); err != nil {
		return nil, err
	}

	return params, nil
}

// reasoningParamsFromRecord extracts the optional reasoning-capture fields
// (todos sec 2-5-5) into params. cot/cot_raw are nullable TEXT, has_raw_cot is a
// nullable BOOLEAN; absent or JSON-null values become nil (SQL NULL). Only the
// scalar type is shape-checked here (no enum).
func reasoningParamsFromRecord(record, params map[string]any) error {
	for _, key := range []string{"cot", "cot_raw"} {
		if val, ok := record[key]; ok && val != nil {
			s, ok := val.(string)
			if !ok {
				return fmt.Errorf("field %q must be a string, got %T", key, val)
			}
			params[key] = s
		} else {
			params[key] = nil
		}
	}

	if val, ok := record["has_raw_cot"]; ok && val != nil {
		b, ok := val.(bool)
		if !ok {
			return fmt.Errorf("field %q must be a boolean, got %T", "has_raw_cot", val)
		}
		params["has_raw_cot"] = b
	} else {
		params["has_raw_cot"] = nil
	}
	// The pair is a contract: has_raw_cot=true asserts the raw reasoning was
	// captured, so cot_raw must actually be present (and vice versa a record
	// carrying cot_raw must say so). Enforced here so an external JSONL load
	// cannot insert a contradictory pair the worker path never produces.
	hasRaw, _ := params["has_raw_cot"].(bool)
	_, rawPresent := params["cot_raw"].(string)
	if hasRaw && !rawPresent {
		return fmt.Errorf("has_raw_cot is true but cot_raw is missing/null")
	}
	if rawPresent && !hasRaw {
		return fmt.Errorf("cot_raw is set but has_raw_cot is not true")
	}
	return nil
}

// m3ParamsFromRecord extracts the optional M3 fields into params. Scalars and
// difficulty_mutations get concrete Go types; the three nested fields keep their
// native JSON value (map/[]any) so pgx encodes them into JSONB columns directly.
// Absent fields become nil (SQL NULL). Enum/subfield validity is checked later
// by ValidateEnums; here we only shape-check the top-level type.
func m3ParamsFromRecord(record, params map[string]any) error {
	for _, key := range []string{"source_lane", "pair_id", "teacher_model", "teacher_provider"} {
		if val, ok := record[key]; ok {
			s, ok := val.(string)
			if !ok {
				return fmt.Errorf("field %q must be a string, got %T", key, val)
			}
			params[key] = s
		} else {
			params[key] = nil
		}
	}

	if val, ok := record["difficulty_mutations"]; ok {
		arr, err := toStringSlice("difficulty_mutations", val)
		if err != nil {
			return err
		}
		params["difficulty_mutations"] = arr
	} else {
		params["difficulty_mutations"] = nil
	}

	if val, ok := record["artifact_refs"]; ok {
		if _, ok := val.([]any); !ok {
			return fmt.Errorf("field %q must be an array, got %T", "artifact_refs", val)
		}
		params["artifact_refs"] = val
	} else {
		params["artifact_refs"] = nil
	}

	for _, key := range []string{"student_filter_verdict", "verification"} {
		if val, ok := record[key]; ok {
			if _, ok := val.(map[string]any); !ok {
				return fmt.Errorf("field %q must be an object, got %T", key, val)
			}
			params[key] = val
		} else {
			params[key] = nil
		}
	}

	return nil
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
