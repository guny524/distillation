package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/model"
)

// backtranslateSystem pins the reverse-generation contract: derive an
// expert-level question grounded in the artifact, re-expressed (never copied),
// and emit exactly one JSON object.
const backtranslateSystem = "You are an expert exam author performing instruction backtranslation. " +
	"Given short excerpts from an expert source document, reverse-generate ONE " +
	"expert-level question whose answer requires reasoning that the source " +
	"grounds. Re-express in your own words: never copy sentences verbatim from " +
	"the excerpts (copyright). The question must stand alone without the source. " +
	"Respond with exactly one JSON object and nothing else."

// backtranslate derives a Question from an artifact via the generator role.
// It fills provenance (artifact_refs with a rephrased why_relevant) and the
// 4-axis taxonomy classification, and detects the question language.
func (p *Pipeline) backtranslate(ctx context.Context, art artifact.Artifact) (Question, error) {
	prompt := backtranslatePrompt(art)
	content, err := chat(ctx, p.llm, p.cfg.Roles.Generator, backtranslateSystem, prompt)
	if err != nil {
		return Question{}, fmt.Errorf("backtranslate: %w", err)
	}
	obj, err := extractJSON(content)
	if err != nil {
		return Question{}, fmt.Errorf("backtranslate: %w", err)
	}

	q := Question{
		Domain:                jstr(obj, "domain"),
		Difficulty:            jstr(obj, "difficulty"),
		TaskShape:             jstr(obj, "task_shape"),
		CapabilityTags:        jstrs(obj, "capability_tags"),
		UserRequest:           jstr(obj, "user_request"),
		Context:               jstr(obj, "context"),
		SuccessCriteria:       jstrs(obj, "success_criteria"),
		ReferenceAnswerSketch: jstr(obj, "reference_answer_sketch"),
	}
	if err := validateClassification(q); err != nil {
		return Question{}, fmt.Errorf("backtranslate: %w: %w", err, ErrBadModelOutput)
	}
	if q.UserRequest == "" || q.Context == "" || q.ReferenceAnswerSketch == "" {
		return Question{}, fmt.Errorf("backtranslate: empty user_request/context/reference_answer_sketch: %w", ErrBadModelOutput)
	}
	if len(q.SuccessCriteria) == 0 {
		return Question{}, fmt.Errorf("backtranslate: empty success_criteria: %w", ErrBadModelOutput)
	}

	ref := art.ToRef()
	ref.WhyRelevant = jstr(obj, "why_relevant")
	if ref.WhyRelevant == "" {
		return Question{}, fmt.Errorf("backtranslate: empty why_relevant (re-expression rationale required): %w", ErrBadModelOutput)
	}
	q.ArtifactRefs = []model.ArtifactRef(nil)
	q.ArtifactRefs = append(q.ArtifactRefs, model.ArtifactRef(ref))
	q.Lang = detectLang(q.UserRequest)
	return q, nil
}

// backtranslatePrompt renders the artifact metadata and short excerpts as
// generation context. Excerpts are anchors for rephrasing, not for reproduction.
func backtranslatePrompt(art artifact.Artifact) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Source type: %s\n", art.SourceType)
	if art.Title != "" {
		fmt.Fprintf(&b, "Title: %s\n", art.Title)
	}
	if art.Locator != "" {
		fmt.Fprintf(&b, "Locator: %s\n", art.Locator)
	}
	b.WriteString("\nExcerpts (short anchors — do NOT copy verbatim):\n")
	for i, c := range art.Chunks {
		fmt.Fprintf(&b, "  [%d] %s\n", i+1, c.Text)
	}
	b.WriteString("\nProduce a JSON object with these fields:\n")
	b.WriteString("  domain: one of software-engineering, data-science, mathematics, natural-science, finance, business, legal-compliance, education, creative-writing, technical-writing, linguistics, philosophy-ethics, general-knowledge\n")
	b.WriteString("  difficulty: one of easy, medium, hard\n")
	b.WriteString("  task_shape: one of short-text, long-text, code, structured-data, analysis-report, step-by-step\n")
	b.WriteString("  capability_tags: array of one or more of reasoning, knowledge-recall, generation, transformation, evaluation, planning, problem-solving, instruction-following\n")
	b.WriteString("  user_request: the reverse-generated expert question (re-expressed, self-contained)\n")
	b.WriteString("  context: background/constraints a solver needs\n")
	b.WriteString("  success_criteria: array of concrete checkable conditions a correct answer must satisfy\n")
	b.WriteString("  reference_answer_sketch: a short outline of the expected answer (used by the student pre-filter, not stored)\n")
	b.WriteString("  why_relevant: why this source grounds the question, in your own words (NOT a quote)\n")
	return b.String()
}

// validateClassification rejects taxonomy values that fall outside the schema
// enums before they can propagate into a record.
func validateClassification(q Question) error {
	if !model.ValidDomains[q.Domain] {
		return fmt.Errorf("invalid domain %q", q.Domain)
	}
	if !model.ValidDifficulties[q.Difficulty] {
		return fmt.Errorf("invalid difficulty %q", q.Difficulty)
	}
	if !model.ValidTaskShapes[q.TaskShape] {
		return fmt.Errorf("invalid task_shape %q", q.TaskShape)
	}
	if len(q.CapabilityTags) == 0 {
		return fmt.Errorf("empty capability_tags")
	}
	for _, t := range q.CapabilityTags {
		if !model.ValidCapabilities[t] {
			return fmt.Errorf("invalid capability_tag %q", t)
		}
	}
	return nil
}
