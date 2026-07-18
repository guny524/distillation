package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/guny524/distillation/internal/model"
)

// verifierSystem asks the verifier to check the teacher trajectory against the
// success criteria and decide a verification method + result. When the task is
// not mechanically verifiable it returns method=none/result=na rather than
// guessing pass.
const verifierSystem = "You are a verification agent. Given a question, its success criteria, and " +
	"a candidate trajectory (plan, reasoning, final answer, self-check), decide " +
	"how the trajectory can be verified and whether it passes. Prefer a concrete " +
	"method (test/rule/exec) when the task allows it; use none when it is not " +
	"mechanically verifiable. Respond with exactly one JSON object and nothing else."

// verify decides the verification record for a teacher trajectory.
//
// When the code executor is enabled AND the final answer contains a fenced code
// block in a supported language, the code is actually run in isolation and the
// verdict comes from the real exit status (exit 0 => pass, else fail; a timeout
// is a fail). This replaces LLM self-report with a mechanical check for the
// code/SWE domain — the strength the research report highlights.
//
// Otherwise (executor disabled, no code fence, or an unsupported language) it
// falls back to the LLM verifier deciding rule/none. A "fail" result means the
// record is dropped; pass/na are kept (na = not mechanically verifiable, still
// usable).
func (p *Pipeline) verify(ctx context.Context, rec model.DistillationPair) (model.Verification, error) {
	if p.exec != nil && p.exec.Enabled() {
		if lang, code, ok := parseCodeFence(rec.FinalAnswer); ok && p.exec.Supports(lang) {
			// Executable code present and execution enabled: run it. The LLM is
			// never consulted here, so it cannot lazily return none/na to skip a
			// snippet that could have been mechanically verified.
			return p.verifyByExecution(ctx, lang, code)
		}
	}
	return p.verifyByLLM(ctx, rec)
}

// verifyByExecution runs the snippet and maps its outcome to a Verification.
func (p *Pipeline) verifyByExecution(ctx context.Context, lang, code string) (model.Verification, error) {
	res, err := p.exec.Run(ctx, ExecRequest{Language: lang, Code: code})
	if err != nil {
		return model.Verification{}, fmt.Errorf("verify: execute %s: %w", lang, err)
	}
	method := execMethod(lang, code)
	v := model.Verification{Method: method}
	switch {
	case res.TimedOut:
		v.Result = "fail"
		v.Detail = fmt.Sprintf("%s execution timed out after %s", lang, res.Duration.Round(time.Millisecond))
	case res.ExitCode == 0:
		v.Result = "pass"
		v.Detail = fmt.Sprintf("%s exited 0 in %s", lang, res.Duration.Round(time.Millisecond))
	default:
		v.Result = "fail"
		v.Detail = fmt.Sprintf("%s exited %d: %s", lang, res.ExitCode, execFailSnippet(res))
	}
	return v, nil
}

// execFailSnippet returns a short, single-line reason for a failed run: the
// tail of stderr (or stdout) to aid debugging without bloating the record.
func execFailSnippet(res ExecResult) string {
	msg := strings.TrimSpace(res.Stderr)
	if msg == "" {
		msg = strings.TrimSpace(res.Stdout)
	}
	msg = strings.ReplaceAll(msg, "\n", " ")
	const max = 200
	if len(msg) > max {
		msg = msg[:max] + "…"
	}
	if msg == "" {
		return "no output"
	}
	return msg
}

// verifyByLLM is the LLM-driven fallback: the verifier role judges rule/none.
func (p *Pipeline) verifyByLLM(ctx context.Context, rec model.DistillationPair) (model.Verification, error) {
	content, err := chat(ctx, p.llm, p.cfg.Roles.Verifier, verifierSystem, verifyPrompt(rec))
	if err != nil {
		return model.Verification{}, fmt.Errorf("verify: %w", err)
	}
	obj, err := extractJSON(content)
	if err != nil {
		return model.Verification{}, fmt.Errorf("verify: %w", err)
	}
	method := strings.ToLower(jstr(obj, "method"))
	result := strings.ToLower(jstr(obj, "result"))
	if !model.ValidVerificationMethods[method] {
		return model.Verification{}, fmt.Errorf("verify: invalid method %q: %w", method, ErrBadModelOutput)
	}
	// A non-verifiable method has no meaningful pass/fail; normalize to na.
	if method == "none" {
		result = "na"
	}
	if !model.ValidVerificationResults[result] {
		return model.Verification{}, fmt.Errorf("verify: invalid result %q: %w", result, ErrBadModelOutput)
	}
	return model.Verification{
		Method: method,
		Result: result,
		Detail: jstr(obj, "detail"),
	}, nil
}

// kept reports whether a verified record should be stored. Only an explicit
// fail is dropped.
func kept(v model.Verification) bool {
	return v.Result != "fail"
}

func verifyPrompt(rec model.DistillationPair) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Question:\n%s\n\n", rec.UserRequest)
	fmt.Fprintf(&b, "Task shape: %s\n\n", rec.TaskShape)
	if len(rec.SuccessCriteria) > 0 {
		fmt.Fprintf(&b, "Success criteria:\n%s\n\n", strings.Join(rec.SuccessCriteria, "\n"))
	}
	fmt.Fprintf(&b, "Final answer:\n%s\n\n", rec.FinalAnswer)
	if len(rec.SelfCheck) > 0 {
		fmt.Fprintf(&b, "Self-check:\n%s\n\n", strings.Join(rec.SelfCheck, "\n"))
	}
	b.WriteString("Produce a JSON object with these fields:\n")
	b.WriteString("  method: one of test, rule, exec, none\n")
	b.WriteString("  result: one of pass, fail, na (use na only with method none)\n")
	b.WriteString("  detail: short justification (test name, rule id, or why not verifiable)\n")
	return b.String()
}
