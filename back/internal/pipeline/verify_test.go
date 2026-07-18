package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/model"
)

func verifiableRecord() model.DistillationPair {
	return model.DistillationPair{
		UserRequest:     "Bound exploding gradients.",
		TaskShape:       "code",
		SuccessCriteria: []string{"names a clipping strategy"},
		FinalAnswer:     "Clip the gradient norm.",
		SelfCheck:       []string{"threshold positive"},
	}
}

// TestVerify_Pass: a passing verifier verdict is kept.
func TestVerify_Pass(t *testing.T) {
	llm := newFakeLLM().script("verifier", ok(verifyPassJSON))
	p := newTestPipeline(t, llm, DefaultConfig())

	v, err := p.verify(context.Background(), verifiableRecord())
	require.NoError(t, err)
	assert.Equal(t, "rule", v.Method)
	assert.Equal(t, "pass", v.Result)
	assert.True(t, kept(v))
}

// TestVerify_FailDropped: a failing verdict marks the record for exclusion.
func TestVerify_FailDropped(t *testing.T) {
	llm := newFakeLLM().script("verifier", ok(verifyFailJSON))
	p := newTestPipeline(t, llm, DefaultConfig())

	v, err := p.verify(context.Background(), verifiableRecord())
	require.NoError(t, err)
	assert.Equal(t, "fail", v.Result)
	assert.False(t, kept(v))
}

// TestVerify_NoneKeptAsNa: a non-verifiable task is method=none/result=na and
// is still kept (na is not a failure).
func TestVerify_NoneKeptAsNa(t *testing.T) {
	llm := newFakeLLM().script("verifier", ok(verifyNoneJSON))
	p := newTestPipeline(t, llm, DefaultConfig())

	v, err := p.verify(context.Background(), verifiableRecord())
	require.NoError(t, err)
	assert.Equal(t, "none", v.Method)
	assert.Equal(t, "na", v.Result)
	assert.True(t, kept(v))
}

// TestVerify_NoneNormalizesResult: method none with a stray pass result is
// normalized to na (a non-verifiable method cannot pass).
func TestVerify_NoneNormalizesResult(t *testing.T) {
	llm := newFakeLLM().script("verifier", ok(`{"method": "none", "result": "pass"}`))
	p := newTestPipeline(t, llm, DefaultConfig())

	v, err := p.verify(context.Background(), verifiableRecord())
	require.NoError(t, err)
	assert.Equal(t, "na", v.Result)
}

// TestVerify_InvalidMethodRejected: an out-of-enum method fails.
func TestVerify_InvalidMethodRejected(t *testing.T) {
	llm := newFakeLLM().script("verifier", ok(`{"method": "vibes", "result": "pass"}`))
	p := newTestPipeline(t, llm, DefaultConfig())

	_, err := p.verify(context.Background(), verifiableRecord())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid method")
}

// TestVerify_ExecPass: enabled executor + code fence + exit 0 => pass by real
// execution; the LLM verifier is never consulted.
func TestVerify_ExecPass(t *testing.T) {
	llm := newFakeLLM() // no verifier scripted: it must not be called
	p := withExec(newTestPipeline(t, llm, DefaultConfig()),
		&fakeExecutor{enabled: true, result: ExecResult{ExitCode: 0}})

	v, err := p.verify(context.Background(), pyRecord())
	require.NoError(t, err)
	assert.Equal(t, "exec", v.Method)
	assert.Equal(t, "pass", v.Result)
	assert.True(t, kept(v))
	assert.Equal(t, 0, llm.callsFor("verifier"))
}

// TestVerify_ExecFailDropped: a non-zero exit is a mechanical fail (dropped),
// overriding any optimistic LLM self-report.
func TestVerify_ExecFailDropped(t *testing.T) {
	exec := &fakeExecutor{enabled: true, result: ExecResult{ExitCode: 1, Stderr: "AssertionError"}}
	p := withExec(newTestPipeline(t, newFakeLLM(), DefaultConfig()), exec)

	v, err := p.verify(context.Background(), pyRecord())
	require.NoError(t, err)
	assert.Equal(t, "fail", v.Result)
	assert.False(t, kept(v))
	assert.Contains(t, v.Detail, "AssertionError")
	require.Len(t, exec.calls, 1)
	assert.Equal(t, "python", exec.calls[0].Language)
}

// TestVerify_ExecTimeoutFails: a timed-out run is a fail.
func TestVerify_ExecTimeoutFails(t *testing.T) {
	exec := &fakeExecutor{enabled: true, result: ExecResult{ExitCode: -1, TimedOut: true}}
	p := withExec(newTestPipeline(t, newFakeLLM(), DefaultConfig()), exec)

	v, err := p.verify(context.Background(), pyRecord())
	require.NoError(t, err)
	assert.Equal(t, "fail", v.Result)
	assert.Contains(t, v.Detail, "timed out")
}

// TestVerify_ExecTestMethodForAsserts: python code with asserts is labelled a
// test (it self-checks its own output).
func TestVerify_ExecTestMethodForAsserts(t *testing.T) {
	rec := verifiableRecord()
	rec.FinalAnswer = "```python\nassert 1 + 1 == 2\n```"
	p := withExec(newTestPipeline(t, newFakeLLM(), DefaultConfig()),
		&fakeExecutor{enabled: true, result: ExecResult{ExitCode: 0}})

	v, err := p.verify(context.Background(), rec)
	require.NoError(t, err)
	assert.Equal(t, "test", v.Method)
	assert.Equal(t, "pass", v.Result)
}

// TestVerify_NonExecutableDomainFallsBack: with the executor enabled but a prose
// (no code fence) answer, the LLM verifier decides — the executor is not called.
func TestVerify_NonExecutableDomainFallsBack(t *testing.T) {
	exec := &fakeExecutor{enabled: true}
	llm := newFakeLLM().script("verifier", ok(verifyNoneJSON))
	p := withExec(newTestPipeline(t, llm, DefaultConfig()), exec)

	rec := verifiableRecord() // FinalAnswer has no code fence
	v, err := p.verify(context.Background(), rec)
	require.NoError(t, err)
	assert.Equal(t, "none", v.Method)
	assert.Equal(t, "na", v.Result)
	assert.Empty(t, exec.calls, "prose answer must not reach the executor")
	assert.Equal(t, 1, llm.callsFor("verifier"))
}

// TestVerify_UnsupportedLanguageFallsBack: a fenced block in an unsupported
// language cannot be executed, so the LLM verifier decides.
func TestVerify_UnsupportedLanguageFallsBack(t *testing.T) {
	exec := &fakeExecutor{enabled: true, supported: map[string]bool{"python": true}}
	llm := newFakeLLM().script("verifier", ok(verifyPassJSON))
	p := withExec(newTestPipeline(t, llm, DefaultConfig()), exec)

	rec := verifiableRecord()
	rec.FinalAnswer = "```ruby\nputs 'hi'\n```"
	v, err := p.verify(context.Background(), rec)
	require.NoError(t, err)
	assert.Equal(t, "rule", v.Method)
	assert.Empty(t, exec.calls)
	assert.Equal(t, 1, llm.callsFor("verifier"))
}

// TestVerify_DisabledExecutorFallsBack: with execution disabled, even a clear
// code fence goes to the LLM verifier (safe default preserved).
func TestVerify_DisabledExecutorFallsBack(t *testing.T) {
	exec := &fakeExecutor{enabled: false}
	llm := newFakeLLM().script("verifier", ok(verifyPassJSON))
	p := withExec(newTestPipeline(t, llm, DefaultConfig()), exec)

	v, err := p.verify(context.Background(), pyRecord())
	require.NoError(t, err)
	assert.Equal(t, "rule", v.Method)
	assert.Equal(t, "pass", v.Result)
	assert.Empty(t, exec.calls, "disabled executor must not run")
	assert.Equal(t, 1, llm.callsFor("verifier"))
}

// TestVerify_CodePresentForcesExecutionNoLazyNone: when execution is enabled and
// executable code is present, verify runs it and never gives the LLM a chance to
// lazily return none/na. Even though the LLM is scripted to say none, the exec
// verdict wins.
func TestVerify_CodePresentForcesExecutionNoLazyNone(t *testing.T) {
	exec := &fakeExecutor{enabled: true, result: ExecResult{ExitCode: 0}}
	llm := newFakeLLM().script("verifier", ok(verifyNoneJSON)) // would be "none/na"
	p := withExec(newTestPipeline(t, llm, DefaultConfig()), exec)

	v, err := p.verify(context.Background(), pyRecord())
	require.NoError(t, err)
	assert.Equal(t, "exec", v.Method)
	assert.Equal(t, "pass", v.Result)
	assert.NotEqual(t, "na", v.Result, "must not lazily pass as na")
	require.Len(t, exec.calls, 1)
	assert.Equal(t, 0, llm.callsFor("verifier"), "LLM verifier must be bypassed")
}
