package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/guny524/distillation/internal/model"
)

// mutateSystem frames Evol-Instruct as a difficulty-mutation operator, not an
// author: it deepens the reasoning graph of an EXISTING question rather than
// inventing a new one (research report: "Evol을 출제자가 아니라 변형으로만").
// It must also rewrite the reference answer sketch, because a mutation can
// change what a correct answer is (a fault to catch, a removed premise, a
// conflicting constraint to resolve); leaving the original sketch would make the
// presolve judge grade the mutated question against the wrong answer.
const mutateSystem = "You are a difficulty-mutation operator. You are given an existing " +
	"expert question, its reference answer sketch, and a set of allowed mutation " +
	"operators. Apply one or more operators to make the question require deeper " +
	"reasoning WITHOUT changing its domain or making it unanswerable. A mutation " +
	"can change what a correct answer is, so you MUST also rewrite the reference " +
	"answer sketch to match the mutated question, not the original. Keep it " +
	"grounded and self-contained. Respond with exactly one JSON object and nothing else."

// mutationDescriptions documents each operator so the generator applies the
// intended transformation. Keys must stay within model.ValidDifficultyMutations.
var mutationDescriptions = map[string]string{
	"fault_injection":        "inject a subtle fault/edge case the solver must detect and handle",
	"partial_info_removal":   "remove a piece of given information so the solver must infer or state assumptions",
	"conflicting_constraint": "add a constraint that conflicts with an existing one so the solver must resolve the tension",
}

// mutate applies the enabled difficulty operators to a question via the
// generator role. When no operator is enabled it is a no-op. The returned
// Question carries the (possibly rewritten) user_request and the list of
// operators actually applied (intersected with the enabled + valid set).
func (p *Pipeline) mutate(ctx context.Context, q Question) (Question, error) {
	enabled := p.enabledMutations()
	if len(enabled) == 0 {
		return q, nil
	}
	content, err := chat(ctx, p.llm, p.cfg.Roles.Generator, mutateSystem, mutatePrompt(q, enabled))
	if err != nil {
		return q, fmt.Errorf("mutate: %w", err)
	}
	obj, err := extractJSON(content)
	if err != nil {
		return q, fmt.Errorf("mutate: %w", err)
	}

	if ur := jstr(obj, "user_request"); ur != "" {
		q.UserRequest = ur
		q.Lang = detectLang(ur)
	}
	if diff := jstr(obj, "difficulty"); model.ValidDifficulties[diff] {
		q.Difficulty = diff
	}
	if crit := jstrs(obj, "success_criteria"); len(crit) > 0 {
		q.SuccessCriteria = crit
	}
	// Refresh the reference answer sketch in the SAME step that rewrites the
	// question so the two never diverge: the mutation call emits the mutated
	// question and its matching sketch together, so grading (studentfilter's
	// judge) compares the student answer to the mutated question's correct
	// answer, not the pre-mutation sketch. When the model omits it, the
	// prior sketch is kept rather than blanked (an empty sketch fails downstream
	// validation); the prompt asks for it on every mutation.
	if sketch := jstr(obj, "reference_answer_sketch"); sketch != "" {
		q.ReferenceAnswerSketch = sketch
	}

	applied := jstrs(obj, "applied_mutations")
	q.DifficultyMutations = filterMutations(applied, enabled)
	return q, nil
}

// enabledMutations returns the configured operators restricted to the valid
// vocabulary, preserving config order.
func (p *Pipeline) enabledMutations() []string {
	var out []string
	for _, m := range p.cfg.Mutations {
		if model.ValidDifficultyMutations[m] {
			out = append(out, m)
		}
	}
	return out
}

// filterMutations keeps only operators the model claims it applied that are
// also in the enabled set, de-duplicated and order-preserving.
func filterMutations(applied, enabled []string) []string {
	allow := make(map[string]bool, len(enabled))
	for _, m := range enabled {
		allow[m] = true
	}
	seen := make(map[string]bool, len(applied))
	var out []string
	for _, m := range applied {
		if allow[m] && !seen[m] {
			seen[m] = true
			out = append(out, m)
		}
	}
	return out
}

// mutatePrompt renders the base question plus the allowed operators.
func mutatePrompt(q Question, enabled []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Existing question:\n%s\n\n", q.UserRequest)
	if q.Context != "" {
		fmt.Fprintf(&b, "Context:\n%s\n\n", q.Context)
	}
	if q.ReferenceAnswerSketch != "" {
		fmt.Fprintf(&b, "Existing reference answer sketch:\n%s\n\n", q.ReferenceAnswerSketch)
	}
	b.WriteString("Allowed mutation operators:\n")
	for _, m := range enabled {
		fmt.Fprintf(&b, "  - %s: %s\n", m, mutationDescriptions[m])
	}
	b.WriteString("\nProduce a JSON object with these fields:\n")
	b.WriteString("  user_request: the mutated question (deeper reasoning, same domain, still answerable)\n")
	b.WriteString("  difficulty: updated difficulty (easy, medium, hard)\n")
	b.WriteString("  success_criteria: updated array of concrete checkable conditions\n")
	b.WriteString("  reference_answer_sketch: the reference answer sketch rewritten so it is correct for the MUTATED question, so the student pre-filter grades against the mutated question's answer and not the original\n")
	b.WriteString("  applied_mutations: array naming which of the allowed operators you applied\n")
	return b.String()
}
