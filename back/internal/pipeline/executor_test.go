package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shellExec builds a real SubprocessExecutor wired to a POSIX-sh runner, so the
// isolation machinery (temp dir, timeout, output cap, exit code) is exercised
// without depending on python/go being installed. Keeps the suite fast.
func shellExec(timeoutSec, maxOut int) *SubprocessExecutor {
	return newExecutor(ExecutorConfig{
		Enabled:        true,
		TimeoutSeconds: timeoutSec,
		MaxOutputBytes: maxOut,
		Languages: map[string]LanguageRunner{
			"shell": {Filename: "prog.sh", Command: []string{"sh", "prog.sh"}},
		},
	})
}

// TestSubprocessExecutor_Pass: a clean exit is ExitCode 0, not timed out.
func TestSubprocessExecutor_Pass(t *testing.T) {
	e := shellExec(5, 4096)
	res, err := e.Run(context.Background(), ExecRequest{Language: "shell", Code: "echo hello\nexit 0\n"})
	require.NoError(t, err)
	assert.Equal(t, 0, res.ExitCode)
	assert.False(t, res.TimedOut)
	assert.Contains(t, res.Stdout, "hello")
}

// TestSubprocessExecutor_NonZeroExit: a failing program reports its exit code
// and captured stderr, not an error.
func TestSubprocessExecutor_NonZeroExit(t *testing.T) {
	e := shellExec(5, 4096)
	res, err := e.Run(context.Background(), ExecRequest{Language: "shell", Code: "echo boom 1>&2\nexit 3\n"})
	require.NoError(t, err)
	assert.Equal(t, 3, res.ExitCode)
	assert.Contains(t, res.Stderr, "boom")
}

// TestSubprocessExecutor_Timeout: a runaway program is killed at the wall-clock
// deadline and reported as timed out.
func TestSubprocessExecutor_Timeout(t *testing.T) {
	e := shellExec(1, 4096)
	res, err := e.Run(context.Background(), ExecRequest{Language: "shell", Code: "sleep 30\n"})
	require.NoError(t, err)
	assert.True(t, res.TimedOut, "expected the sleep to be killed at the deadline")
	assert.NotEqual(t, 0, res.ExitCode)
}

// TestSubprocessExecutor_OutputCap: output beyond max_output_bytes is truncated
// and flagged, bounding memory.
func TestSubprocessExecutor_OutputCap(t *testing.T) {
	e := shellExec(5, 16)
	res, err := e.Run(context.Background(), ExecRequest{
		Language: "shell",
		// yes|head would need a pipe; a bounded loop keeps it deterministic.
		Code: "i=0; while [ $i -lt 1000 ]; do printf 'XXXXXXXXXX'; i=$((i+1)); done\n",
	})
	require.NoError(t, err)
	assert.True(t, res.Truncated)
	assert.LessOrEqual(t, len(res.Stdout), 16)
}

// TestSubprocessExecutor_UnsupportedLanguage: Run errors for a language with no
// configured runner (an infrastructure error, distinct from a program failure).
func TestSubprocessExecutor_UnsupportedLanguage(t *testing.T) {
	e := shellExec(5, 4096)
	_, err := e.Run(context.Background(), ExecRequest{Language: "python", Code: "print(1)"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported language")
}

// TestExecutor_EnabledAndSupports: config gates reflect through the interface.
func TestExecutor_EnabledAndSupports(t *testing.T) {
	e := shellExec(5, 4096)
	assert.True(t, e.Enabled())
	assert.True(t, e.Supports("shell"))
	assert.False(t, e.Supports("python"))

	off := newExecutor(DefaultExecutorConfig())
	assert.False(t, off.Enabled())
	assert.True(t, off.Supports("python"), "default runners are configured even while disabled")
	assert.True(t, off.Supports("go"))
}

// TestParseCodeFence covers language detection and selection of the first
// supported fenced block.
func TestParseCodeFence(t *testing.T) {
	cases := []struct {
		name       string
		answer     string
		wantOK     bool
		wantLang   string
		wantSubstr string
	}{
		{"python", "text\n```python\nprint(1)\n```", true, "python", "print(1)"},
		{"py alias", "```py\nx=1\n```", true, "python", "x=1"},
		{"golang alias", "```golang\npackage main\n```", true, "go", "package main"},
		{"info string extra", "```python title=a.py\nprint(2)\n```", true, "python", "print(2)"},
		{"no fence", "just prose, no code", false, "", ""},
		{"unsupported", "```ruby\nputs 1\n```", false, "", ""},
		{"empty body skipped, next used", "```python\n\n```\n```go\npackage main\n```", true, "go", "package main"},
		{"first supported wins", "```ruby\nx\n```\n```python\ngo_here=1\n```", true, "python", "go_here=1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lang, code, ok := parseCodeFence(c.answer)
			assert.Equal(t, c.wantOK, ok)
			if c.wantOK {
				assert.Equal(t, c.wantLang, lang)
				assert.Contains(t, code, c.wantSubstr)
			}
		})
	}
}

// TestExecMethod: python self-checks are "test"; everything else is "exec".
func TestExecMethod(t *testing.T) {
	assert.Equal(t, "test", execMethod("python", "assert x == 1"))
	assert.Equal(t, "test", execMethod("python", "import unittest"))
	assert.Equal(t, "exec", execMethod("python", "print('hi')"))
	assert.Equal(t, "exec", execMethod("go", "func main(){}"))
}
