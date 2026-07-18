// Package pipeline implements the artifact-based reverse-generation pipeline:
// artifact -> backtranslate -> difficulty mutate -> student pre-filter ->
// 2-lane translation -> teacher trajectory -> verifier -> model.DistillationPair.
//
// Every LLM call is abstracted behind the LLM interface so stages are unit
// testable with a scripted fake and no network access. Each stage maps to a
// config role (generator/student/judge/translator/teacher/verifier); the
// concrete endpoint is resolved by internal/teacher (roles may share one
// endpoint). Copyright rule (research report): questions are re-expressed from
// short artifact excerpts, never verbatim copies of the source text.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/guny524/distillation/internal/teacher"
)

// LLM is the endpoint surface the pipeline stages need. *teacher.Client
// satisfies it; tests inject a scripted fake. It is the ChatCompletion subset
// of runner.TeacherAPI (the pipeline never fetches quota).
type LLM interface {
	ChatCompletion(ctx context.Context, role string, msgs []teacher.Message) (string, error)
}

// ReasoningLLM optionally augments LLM with the reasoning-aware Complete call.
// *teacher.Client satisfies it. The teacher-trajectory stage type-asserts to it
// so that, when the teacher endpoint returns message.reasoning_content (an
// open-weight model), the record captures cot_raw/has_raw_cot (todos sec 2-5-5).
// A content-only LLM (the legacy monolithic path, scripted test fakes) simply
// does not satisfy it and takes the ChatCompletion path unchanged.
type ReasoningLLM interface {
	LLM
	Complete(ctx context.Context, role string, msgs []teacher.Message) (teacher.Completion, error)
}

// ErrBadModelOutput marks a stage failure caused by the model's FRESH response
// violating the stage's output contract (unparseable JSON, out-of-enum value,
// missing required field). Unlike a stored-payload contract violation -- which
// reproduces identically on every attempt and must fail terminally -- this is
// stochastic: re-requesting usually yields a valid response, so the worker
// retries it under the same bounded backoff budget as transport errors.
var ErrBadModelOutput = errors.New("model output violates the stage contract")

// IsBadModelOutput reports whether err (anywhere in its chain) is a
// model-output contract violation eligible for a bounded re-request.
func IsBadModelOutput(err error) bool {
	return errors.Is(err, ErrBadModelOutput)
}

// errNoJSONObject means an LLM response contained no parseable JSON object.
var errNoJSONObject = fmt.Errorf("pipeline: no JSON object found in LLM response: %w", ErrBadModelOutput)

// chat is a small helper that sends a system+user message pair to a role and
// returns the raw content.
func chat(ctx context.Context, llm LLM, role, system, user string) (string, error) {
	return llm.ChatCompletion(ctx, role, []teacher.Message{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	})
}

// extractJSON pulls one JSON object out of a model response that may be wrapped
// in code fences or surrounded by prose. It mirrors runner.ExtractJSONObject
// (duplicated to avoid a runner<->pipeline import cycle): try the whole trimmed
// string first, then decode from each '{' with json.Decoder, which tolerates
// trailing text after the object.
func extractJSON(s string) (map[string]any, error) {
	trimmed := strings.TrimSpace(s)
	var m map[string]any
	if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
		return m, nil
	}
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] != '{' {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(trimmed[i:]))
		var candidate map[string]any
		if err := dec.Decode(&candidate); err == nil {
			return candidate, nil
		}
	}
	return nil, errNoJSONObject
}

// jstr reads an optional string field from a decoded JSON object.
func jstr(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// jstrs reads a []string field from a decoded JSON object, dropping non-string
// or empty elements.
func jstrs(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		if s, ok := e.(string); ok {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// jfloat reads an optional numeric field; ok reports presence as a JSON number.
func jfloat(m map[string]any, key string) (float64, bool) {
	if v, ok := m[key].(float64); ok {
		return v, true
	}
	return 0, false
}
