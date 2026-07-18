package pipeline

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/guny524/distillation/internal/model"
)

// studentSystem asks the student proxy to actually solve the question so its
// answer can be compared to the reference sketch and across samples.
const studentSystem = "You are a capable but non-expert solver. Answer the question as best " +
	"you can. Respond with exactly one JSON object and nothing else."

// judgeSystem asks the judge to compare the student answer against the
// reference answer sketch and return a pass/fail/uncertain verdict.
const judgeSystem = "You are a strict grader. Compare a student's answer to a reference answer " +
	"sketch for the same question. Decide whether the student already solves the " +
	"question correctly. Respond with exactly one JSON object and nothing else."

// filterOutcome is the student pre-filter result.
type filterOutcome struct {
	Verdict model.StudentFilterVerdict
	// ToTeacher is true when the question has learning value (student did not
	// clearly solve it), i.e. verdict is fail or uncertain.
	ToTeacher bool
}

// studentFilter runs the two-signal pre-filter (research report + todos sec
// 2-3): a judge comparison against the reference answer sketch AND
// self-consistency disagreement across k student samples. Either signal firing
// (fail or uncertain) routes the question to the teacher; only a clean pass on
// both is dropped as zero-learning-value.
func (p *Pipeline) studentFilter(ctx context.Context, q Question) (filterOutcome, error) {
	k := p.cfg.SelfConsistencyK
	if k < 1 {
		k = 1
	}
	answers := make([]string, 0, k)
	for i := 0; i < k; i++ {
		content, err := chat(ctx, p.llm, p.cfg.Roles.Student, studentSystem, studentPrompt(q))
		if err != nil {
			return filterOutcome{}, fmt.Errorf("student filter: student sample %d: %w", i+1, err)
		}
		obj, err := extractJSON(content)
		if err != nil {
			return filterOutcome{}, fmt.Errorf("student filter: student sample %d: %w", i+1, err)
		}
		answers = append(answers, jstr(obj, "answer"))
	}
	disagreement := disagreementRate(answers)

	jContent, err := chat(ctx, p.llm, p.cfg.Roles.Judge, judgeSystem, judgePrompt(q, answers[0]))
	if err != nil {
		return filterOutcome{}, fmt.Errorf("student filter: judge: %w", err)
	}
	jObj, err := extractJSON(jContent)
	if err != nil {
		return filterOutcome{}, fmt.Errorf("student filter: judge: %w", err)
	}
	judgeVerdict := strings.ToLower(jstr(jObj, "verdict"))
	if !model.ValidStudentVerdicts[judgeVerdict] {
		return filterOutcome{}, fmt.Errorf("student filter: invalid judge verdict %q: %w", judgeVerdict, ErrBadModelOutput)
	}
	score, hasScore := jfloat(jObj, "score")

	verdict, reason := combineVerdict(judgeVerdict, disagreement, p.cfg.SelfConsistencyThreshold)

	sfv := model.StudentFilterVerdict{
		Verdict: verdict,
		Method:  "both",
		Reason:  reason,
	}
	if hasScore {
		sfv.JudgeScore = &score
	}
	if k > 1 {
		d := disagreement
		sfv.SelfConsistencyDisagreement = &d
	}
	return filterOutcome{Verdict: sfv, ToTeacher: verdict != "pass"}, nil
}

// combineVerdict merges the judge verdict with the self-consistency signal.
// Precedence: an explicit judge fail dominates; otherwise a judge-uncertain or
// a disagreement at/above threshold yields uncertain; only judge-pass with low
// disagreement passes.
func combineVerdict(judgeVerdict string, disagreement, threshold float64) (verdict, reason string) {
	switch judgeVerdict {
	case "fail":
		return "fail", "judge scored student answer wrong vs reference sketch"
	case "uncertain":
		return "uncertain", "judge could not confirm the student answer"
	}
	// judge == pass
	if disagreement >= threshold {
		return "uncertain", fmt.Sprintf("self-consistency disagreement %.2f >= %.2f despite judge pass", disagreement, threshold)
	}
	return "pass", "student solves it: judge pass and self-consistency agreement"
}

// disagreementRate returns 1 - (largest identical-answer cluster / k). Answers
// are semantically normalized (see normalizeAnswer) before clustering so that
// formatting-only differences ("42", "42.", "The answer is 42") do not inflate
// disagreement and spuriously route a solved question to the teacher. A
// single sample has zero disagreement by definition.
func disagreementRate(answers []string) float64 {
	n := len(answers)
	if n <= 1 {
		return 0
	}
	counts := make(map[string]int, n)
	max := 0
	for _, a := range answers {
		key := normalizeAnswer(a)
		counts[key]++
		if counts[key] > max {
			max = counts[key]
		}
	}
	return 1 - float64(max)/float64(n)
}

// normalizeAnswer canonicalizes a free-text answer for self-consistency
// clustering: it lower-cases, maps every punctuation/symbol/whitespace run to a
// single space, and trims. This collapses formatting noise (trailing periods,
// quotes, casing, hyphen-vs-space, doubled spaces) that exact-string comparison
// would otherwise count as disagreement, while keeping the letters and digits
// that carry the answer's meaning. It is deterministic and LLM-free by design:
// self-consistency must stay independent of the judge signal (the two-signal
// pre-filter), so equivalence here is lexical-after-normalization, not a model
// call.
func normalizeAnswer(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := true // leading-space suppression
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			b.WriteRune(r)
			prevSpace = false
			continue
		}
		// Punctuation, symbols, and whitespace all collapse to one separator.
		if !prevSpace {
			b.WriteByte(' ')
			prevSpace = true
		}
	}
	return strings.TrimSpace(b.String())
}

func studentPrompt(q Question) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Question:\n%s\n\n", q.UserRequest)
	if q.Context != "" {
		fmt.Fprintf(&b, "Context:\n%s\n\n", q.Context)
	}
	b.WriteString("Produce a JSON object with field:\n  answer: your best answer to the question\n")
	return b.String()
}

func judgePrompt(q Question, studentAnswer string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Question:\n%s\n\n", q.UserRequest)
	fmt.Fprintf(&b, "Reference answer sketch:\n%s\n\n", q.ReferenceAnswerSketch)
	fmt.Fprintf(&b, "Student answer:\n%s\n\n", studentAnswer)
	b.WriteString("Produce a JSON object with fields:\n")
	b.WriteString("  verdict: one of pass (student is correct), fail (student is wrong), uncertain (cannot tell)\n")
	b.WriteString("  score: 0..1 similarity of the student answer to the reference sketch\n")
	return b.String()
}
