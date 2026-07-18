package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/teacher"
)

// translatorSystem asks a trusted frontier translator to render the question
// into the missing lane language, preserving technical meaning.
const translatorSystem = "You are a professional technical translator. Translate the given " +
	"question, context, success criteria, and reference answer sketch into the " +
	"target language, preserving all technical meaning and terminology. " +
	"Respond with exactly one JSON object and nothing else."

// teacherSystem asks the teacher to produce a structured, verifiable trajectory
// (not raw hidden CoT) in the lane's language (research report: structured
// rationale + evidence + verification trace as the learning signal).
const teacherSystem = "You are a domain expert producing a distillation trajectory. Solve the " +
	"question and expose your problem-solving as explicit, externally presentable " +
	"artifacts: a plan, a reasoning summary, the final answer, self-check items, " +
	"and quality notes. Write ALL fields in the requested language. Do not reveal " +
	"hidden chain-of-thought verbatim; structure it. Respond with exactly one JSON " +
	"object and nothing else."

// laneResult carries the produced records plus the number of teacher-role calls
// made (the pacing cost basis, which counts only the paced teacher endpoint).
type laneResult struct {
	Records      []model.DistillationPair
	TeacherCalls int
}

// lane builds the ko/en pair (translating the missing side) and generates a
// teacher trajectory in each lane's language. Both records share pairID and the
// same student-filter verdict/provenance; task_id is pairID-<lane>.
func (p *Pipeline) lane(ctx context.Context, q Question, verdict model.StudentFilterVerdict, pairID string) (laneResult, error) {
	origLang := q.Lang
	if origLang != "ko" && origLang != "en" {
		origLang = detectLang(q.UserRequest)
	}
	other := otherLang(origLang)

	translated, err := p.translate(ctx, q, other)
	if err != nil {
		return laneResult{}, err
	}

	byLane := map[string]Question{origLang: q, other: translated}
	var res laneResult
	// Deterministic order: ko then en.
	for _, lane := range []string{"ko", "en"} {
		lq, ok := byLane[lane]
		if !ok {
			continue
		}
		rec, err := p.teacherTrajectory(ctx, lq, lane)
		res.TeacherCalls++
		if err != nil {
			// The teacher call was already made -- it spent (or attempted to spend)
			// quota -- so return res, not laneResult{}, preserving TeacherCalls even
			// on a parse/validation failure. Discarding it (the old behavior) settled
			// the pass at 0 calls and leaked the real consumption (todos #1c).
			return res, err
		}
		rec.PairID = pairID
		rec.TaskID = pairID + "-" + lane
		rec.SourceLane = lane
		rec.ArtifactRefs = q.ArtifactRefs
		rec.DifficultyMutations = q.DifficultyMutations
		v := verdict
		rec.StudentFilterVerdict = &v
		rec.TeacherModel = p.cfg.TeacherModel
		rec.TeacherProvider = p.cfg.TeacherProvider
		res.Records = append(res.Records, rec)
	}
	return res, nil
}

// translate renders the question into targetLang via the translator role.
func (p *Pipeline) translate(ctx context.Context, q Question, targetLang string) (Question, error) {
	content, err := chat(ctx, p.llm, p.cfg.Roles.Translator, translatorSystem, translatePrompt(q, targetLang))
	if err != nil {
		return Question{}, fmt.Errorf("lane translate: %w", err)
	}
	obj, err := extractJSON(content)
	if err != nil {
		return Question{}, fmt.Errorf("lane translate: %w", err)
	}
	t := q // copy classification/provenance
	t.UserRequest = jstr(obj, "user_request")
	t.Context = jstr(obj, "context")
	if crit := jstrs(obj, "success_criteria"); len(crit) > 0 {
		t.SuccessCriteria = crit
	}
	if sketch := jstr(obj, "reference_answer_sketch"); sketch != "" {
		t.ReferenceAnswerSketch = sketch
	}
	if t.UserRequest == "" || t.Context == "" {
		return Question{}, fmt.Errorf("lane translate: empty translated user_request/context: %w", ErrBadModelOutput)
	}
	t.Lang = targetLang
	return t, nil
}

// teacherTrajectory generates the structured trajectory for one lane and
// assembles a model.DistillationPair (base + trajectory fields only; the caller
// stamps lane/provenance/metadata).
func (p *Pipeline) teacherTrajectory(ctx context.Context, q Question, lang string) (model.DistillationPair, error) {
	content, comp, err := p.teacherComplete(ctx, teacherPrompt(q, lang))
	if err != nil {
		return model.DistillationPair{}, fmt.Errorf("lane teacher(%s): %w", lang, err)
	}
	obj, err := extractJSON(content)
	if err != nil {
		return model.DistillationPair{}, fmt.Errorf("lane teacher(%s): %w", lang, err)
	}
	rec := model.DistillationPair{
		Domain:           q.Domain,
		Difficulty:       q.Difficulty,
		TaskShape:        q.TaskShape,
		CapabilityTags:   q.CapabilityTags,
		UserRequest:      q.UserRequest,
		Context:          q.Context,
		SuccessCriteria:  q.SuccessCriteria,
		Plan:             jstrs(obj, "plan"),
		ReasoningSummary: jstr(obj, "reasoning_summary"),
		FinalAnswer:      jstr(obj, "final_answer"),
		SelfCheck:        jstrs(obj, "self_check"),
		QualityNotes:     jstrs(obj, "quality_notes"),
	}
	// Reasoning capture (todos sec 2-5-5): cot is the in-band structured
	// reasoning forced by the prompt (always present, every source), so it mirrors
	// reasoning_summary. cot_raw/has_raw_cot come from the provider's
	// message.reasoning_content, populated only when the teacher endpoint is
	// reasoning-aware (open-weight); applyRawReasoning is a no-op on the
	// content-only path.
	rec.Cot = rec.ReasoningSummary
	applyRawReasoning(&rec, comp)
	if err := validateTrajectory(rec); err != nil {
		return model.DistillationPair{}, fmt.Errorf("lane teacher(%s): %w", lang, err)
	}
	return rec, nil
}

// teacherComplete sends the teacher system+user prompt, preferring the
// reasoning-aware Complete path (so message.reasoning_content is captured) when
// the LLM implements ReasoningLLM, and falling back to content-only
// ChatCompletion otherwise. The returned Completion is the zero value on the
// fallback path (no provider reasoning metadata available).
func (p *Pipeline) teacherComplete(ctx context.Context, user string) (string, teacher.Completion, error) {
	msgs := []teacher.Message{
		{Role: "system", Content: teacherSystem},
		{Role: "user", Content: user},
	}
	if rl, ok := p.llm.(ReasoningLLM); ok {
		comp, err := rl.Complete(ctx, p.cfg.Roles.Teacher, msgs)
		return comp.Content, comp, err
	}
	content, err := p.llm.ChatCompletion(ctx, p.cfg.Roles.Teacher, msgs)
	return content, teacher.Completion{}, err
}

// applyRawReasoning records cot_raw/has_raw_cot from a provider completion. On
// the content-only fallback path (zero-value Completion) it leaves both unset,
// so a subscription trajectory carries cot only (no raw), which is exactly the
// todos sec 2-5-5 contract.
func applyRawReasoning(rec *model.DistillationPair, comp teacher.Completion) {
	if comp.Content == "" && comp.ReasoningContent == "" {
		return
	}
	has := comp.HasRawCoT()
	rec.HasRawCoT = &has
	if has {
		raw := comp.ReasoningContent
		rec.CotRaw = &raw
	}
}

// validateTrajectory enforces the schema's non-empty requirements on the
// teacher-produced fields so a malformed response never reaches INSERT.
func validateTrajectory(rec model.DistillationPair) error {
	if rec.ReasoningSummary == "" || rec.FinalAnswer == "" {
		return fmt.Errorf("empty reasoning_summary/final_answer: %w", ErrBadModelOutput)
	}
	if len(rec.Plan) == 0 || len(rec.SelfCheck) == 0 || len(rec.QualityNotes) == 0 {
		return fmt.Errorf("empty plan/self_check/quality_notes: %w", ErrBadModelOutput)
	}
	return nil
}

func translatePrompt(q Question, targetLang string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Target language: %s\n\n", langName(targetLang))
	fmt.Fprintf(&b, "user_request:\n%s\n\n", q.UserRequest)
	fmt.Fprintf(&b, "context:\n%s\n\n", q.Context)
	fmt.Fprintf(&b, "success_criteria:\n%s\n\n", strings.Join(q.SuccessCriteria, "\n"))
	fmt.Fprintf(&b, "reference_answer_sketch:\n%s\n\n", q.ReferenceAnswerSketch)
	b.WriteString("Produce a JSON object with the translated fields:\n")
	b.WriteString("  user_request, context, success_criteria (array), reference_answer_sketch\n")
	return b.String()
}

func teacherPrompt(q Question, lang string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Answer in %s.\n\n", langName(lang))
	fmt.Fprintf(&b, "Question:\n%s\n\n", q.UserRequest)
	if q.Context != "" {
		fmt.Fprintf(&b, "Context:\n%s\n\n", q.Context)
	}
	if len(q.SuccessCriteria) > 0 {
		fmt.Fprintf(&b, "Success criteria:\n%s\n\n", strings.Join(q.SuccessCriteria, "\n"))
	}
	b.WriteString("Produce a JSON object with these fields:\n")
	b.WriteString("  plan: array of steps you follow\n")
	b.WriteString("  reasoning_summary: a structured summary of your reasoning (not raw hidden CoT)\n")
	b.WriteString("  final_answer: the complete final answer\n")
	b.WriteString("  self_check: array of verification items you checked\n")
	b.WriteString("  quality_notes: array of notes on answer quality and what to verify\n")
	return b.String()
}

// langName maps a lane code to a prompt-friendly language name.
func langName(lang string) string {
	if lang == "ko" {
		return "Korean"
	}
	return "English"
}
