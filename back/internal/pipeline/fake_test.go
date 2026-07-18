package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/teacher"
)

// fakeResp scripts one ChatCompletion result.
type fakeResp struct {
	content string
	err     error
}

// fakeCall records one ChatCompletion invocation.
type fakeCall struct {
	role string
	msgs []teacher.Message
}

// fakeLLM is a role-keyed scripted LLM: each role has an ordered queue of
// responses consumed in call order. Unscripted calls error (surfacing missing
// scripting in a test rather than silently passing).
type fakeLLM struct {
	responses map[string][]fakeResp
	idx       map[string]int
	calls     []fakeCall
}

func newFakeLLM() *fakeLLM {
	return &fakeLLM{responses: map[string][]fakeResp{}, idx: map[string]int{}}
}

// script appends responses for a role in call order.
func (f *fakeLLM) script(role string, resps ...fakeResp) *fakeLLM {
	f.responses[role] = append(f.responses[role], resps...)
	return f
}

func (f *fakeLLM) ChatCompletion(_ context.Context, role string, msgs []teacher.Message) (string, error) {
	f.calls = append(f.calls, fakeCall{role: role, msgs: msgs})
	i := f.idx[role]
	f.idx[role]++
	q := f.responses[role]
	if i >= len(q) {
		return "", fmt.Errorf("fakeLLM: unscripted call %d for role %q", i, role)
	}
	return q[i].content, q[i].err
}

// callsFor returns the number of ChatCompletion calls made to a role.
func (f *fakeLLM) callsFor(role string) int {
	n := 0
	for _, c := range f.calls {
		if c.role == role {
			n++
		}
	}
	return n
}

// ok wraps content as a successful response.
func ok(content string) fakeResp { return fakeResp{content: content} }

// fakeExecutor is a scripted Executor: it records calls and returns a canned
// result, so verify tests never spawn a real process.
type fakeExecutor struct {
	enabled   bool
	supported map[string]bool
	result    ExecResult
	runErr    error
	calls     []ExecRequest
}

func (f *fakeExecutor) Enabled() bool { return f.enabled }

func (f *fakeExecutor) Supports(language string) bool {
	if f.supported == nil {
		return true
	}
	return f.supported[language]
}

func (f *fakeExecutor) Run(_ context.Context, req ExecRequest) (ExecResult, error) {
	f.calls = append(f.calls, req)
	return f.result, f.runErr
}

// withExec installs a fake executor on a pipeline and returns both.
func withExec(p *Pipeline, exec *fakeExecutor) *Pipeline {
	p.exec = exec
	return p
}

// pyRecord is a record whose final answer carries a Python code fence, so the
// executor path is exercised when enabled.
func pyRecord() model.DistillationPair {
	r := verifiableRecord()
	r.FinalAnswer = "Here is the fix:\n\n```python\nprint('ok')\n```\n"
	return r
}

// sampleArtifact is a small artifact whose excerpt text is distinct from the
// scripted question, so tests can assert re-expression (no verbatim copy).
func sampleArtifact() artifact.Artifact {
	return artifact.Artifact{
		SourceType: artifact.SourceArxiv,
		DocID:      "2401.00001",
		URL:        "https://arxiv.org/abs/2401.00001",
		License:    "arXiv-nonexclusive",
		Title:      "On gradient clipping in deep nets",
		Locator:    "sec-3",
		Chunks: []artifact.Chunk{
			{Text: "VERBATIM_SOURCE_SENTENCE_ABOUT_CLIPPING", Locator: "p-12"},
		},
		RetrievedAt: time.Now(),
	}
}

// --- scripted JSON responses -------------------------------------------------

// backtranslateJSON is an English-question backtranslation result.
const backtranslateJSON = `{
  "domain": "software-engineering",
  "difficulty": "medium",
  "task_shape": "code",
  "capability_tags": ["reasoning", "problem-solving"],
  "user_request": "Explain how to bound exploding gradients during training and implement a guard.",
  "context": "You maintain a training loop that occasionally diverges.",
  "success_criteria": ["names a clipping strategy", "handles the divergence case"],
  "reference_answer_sketch": "Clip gradient norm to a threshold before the optimizer step.",
  "why_relevant": "The source discusses stabilizing optimization, which grounds the question."
}`

// backtranslateKoJSON is a Korean-question backtranslation result.
const backtranslateKoJSON = `{
  "domain": "software-engineering",
  "difficulty": "medium",
  "task_shape": "code",
  "capability_tags": ["reasoning"],
  "user_request": "학습 중 폭주하는 그래디언트를 제한하는 방법을 설명하고 가드를 구현하라.",
  "context": "가끔 발산하는 학습 루프를 유지보수하고 있다.",
  "success_criteria": ["클리핑 전략을 언급", "발산 케이스 처리"],
  "reference_answer_sketch": "옵티마이저 스텝 전에 그래디언트 노름을 임계값으로 클리핑한다.",
  "why_relevant": "원문은 최적화 안정화를 다루며 질문의 근거가 된다."
}`

const mutateJSON = `{
  "user_request": "Bound exploding gradients when two conflicting constraints (max throughput vs numerical stability) apply, and implement a guard.",
  "difficulty": "hard",
  "success_criteria": ["names a clipping strategy", "resolves the throughput/stability tension"],
  "reference_answer_sketch": "Clip the gradient norm, and resolve the throughput/stability tension by tuning the clip threshold rather than disabling clipping.",
  "applied_mutations": ["conflicting_constraint", "not_a_real_operator"]
}`

// studentAnswerA/B are two different student answers (for disagreement).
const studentAnswerA = `{"answer": "Use gradient clipping by norm."}`
const studentAnswerB = `{"answer": "Just lower the learning rate a lot."}`

// studentAnswerAFmt is studentAnswerA differing ONLY in formatting (casing,
// hyphenation, punctuation, surrounding whitespace); normalizeAnswer must
// cluster it with studentAnswerA so self-consistency sees no spurious
// disagreement.
const studentAnswerAFmt = `{"answer": "  use gradient-clipping, by NORM!  "}`

const judgePassJSON = `{"verdict": "pass", "score": 0.92}`
const judgeFailJSON = `{"verdict": "fail", "score": 0.1}`
const judgeUncertainJSON = `{"verdict": "uncertain", "score": 0.5}`

// translateJSON renders the question into the other lane (Korean here).
const translateJSON = `{
  "user_request": "학습 중 폭주하는 그래디언트를 제한하고 가드를 구현하라.",
  "context": "가끔 발산하는 학습 루프.",
  "success_criteria": ["클리핑 전략 언급", "발산 처리"],
  "reference_answer_sketch": "노름 클리핑."
}`

// teacherTrajectoryJSON is a valid structured trajectory.
const teacherTrajectoryJSON = `{
  "plan": ["detect divergence", "clip gradient norm", "verify stability"],
  "reasoning_summary": "Clipping the global norm bounds the update magnitude.",
  "final_answer": "Clip the gradient norm to a fixed threshold before optimizer.step().",
  "self_check": ["threshold is positive", "clip happens before step"],
  "quality_notes": ["teaches a robust stabilization pattern"]
}`

const verifyPassJSON = `{"method": "rule", "result": "pass", "detail": "criteria satisfied"}`
const verifyFailJSON = `{"method": "rule", "result": "fail", "detail": "no clipping strategy"}`
const verifyNoneJSON = `{"method": "none", "result": "na", "detail": "not mechanically verifiable"}`
