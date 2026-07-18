package pipeline

import (
	"unicode"

	"github.com/guny524/distillation/internal/model"
)

// Question is the pipeline's intermediate representation between stages: the
// reverse-generated task plus its provenance and difficulty metadata, before
// the teacher writes the per-lane trajectory. The monolithic pipeline projects
// it straight into model.DistillationPair records; in the queue decomposition
// (todos sec 2-5-3) the question worker persists it into distillation_items'
// `question` JSONB column so the presolve and answer workers can read it back,
// hence the explicit snake_case json tags for a stable, inspectable payload.
type Question struct {
	Domain                string              `json:"domain"`
	Difficulty            string              `json:"difficulty"`
	TaskShape             string              `json:"task_shape"`
	CapabilityTags        []string            `json:"capability_tags"`
	UserRequest           string              `json:"user_request"`
	Context               string              `json:"context"`
	SuccessCriteria       []string            `json:"success_criteria"`
	ReferenceAnswerSketch string              `json:"reference_answer_sketch"`
	ArtifactRefs          []model.ArtifactRef `json:"artifact_refs"`
	// Lang is the detected language of UserRequest ("ko" or "en").
	Lang string `json:"lang"`
	// DifficultyMutations lists the operators the mutate stage applied.
	DifficultyMutations []string `json:"difficulty_mutations"`
}

// detectLang classifies text as "ko" when it contains any Hangul rune, else
// "en". This is deterministic and does not trust an LLM's self-reported
// language (anti-hallucination: verify from the artifact itself).
func detectLang(s string) string {
	for _, r := range s {
		if unicode.Is(unicode.Hangul, r) {
			return "ko"
		}
	}
	return "en"
}

// otherLang returns the opposite lane of a ko/en language.
func otherLang(lang string) string {
	if lang == "ko" {
		return "en"
	}
	return "ko"
}
