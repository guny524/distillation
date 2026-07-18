package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ExecRequest is one code-execution request: a source snippet plus the
// canonical language it was written in (already resolved to a supported name).
type ExecRequest struct {
	// Language is a canonical language key ("python", "go", ...) that must match
	// a configured LanguageRunner.
	Language string
	// Code is the raw source extracted from a fenced code block.
	Code string
}

// ExecResult is the outcome of running a snippet in isolation. A non-zero exit
// is a normal result, not an error; only infrastructure failures (temp dir,
// process spawn) surface as an error from Executor.Run.
type ExecResult struct {
	// ExitCode is the process exit status; -1 when the process was killed
	// (e.g. on timeout) before it could report one.
	ExitCode int
	// Stdout/Stderr are the captured streams, each truncated to the configured
	// max_output_bytes cap (Truncated reports whether truncation happened).
	Stdout    string
	Stderr    string
	Truncated bool
	// TimedOut is true when the wall-clock deadline killed the process.
	TimedOut bool
	// Duration is the wall-clock run time.
	Duration time.Duration
}

// Executor runs LLM-generated code in isolation and reports the outcome. verify
// depends on this interface (not the concrete SubprocessExecutor) so unit tests
// inject a fake with no real subprocess.
//
// Security: implementations execute untrusted, model-generated code. They MUST
// isolate it (temp dir, wall-clock timeout, output cap, minimal env) and are
// intended to run inside a locked-down Linux container. See SubprocessExecutor
// for the concrete guarantees and their limits.
type Executor interface {
	// Enabled reports whether real execution is turned on. When false, verify
	// falls back to LLM-rule verification (the safe default).
	Enabled() bool
	// Supports reports whether a canonical language has a configured runner.
	Supports(language string) bool
	// Run executes the request and returns its result. It returns an error only
	// for infrastructure failures; a failed program is a normal ExecResult with
	// a non-zero ExitCode.
	Run(ctx context.Context, req ExecRequest) (ExecResult, error)
}

// SubprocessExecutor runs code as a child process under several isolation
// controls. It is deliberately conservative and disabled unless explicitly
// enabled in config.
//
// Isolation guarantees (best-effort, defense-in-depth):
//   - Filesystem: each run gets a fresh os.MkdirTemp directory, used as the
//     process cwd and HOME, and RemoveAll'd afterward. The child cannot see the
//     caller's working tree by relative path.
//   - Time: exec.CommandContext + a per-run context.WithTimeout enforce a hard
//     wall-clock deadline; on expiry the whole process GROUP is SIGKILL'd
//     (Setpgid), so forked grandchildren die too rather than orphaning.
//   - Output: stdout/stderr are captured through a capped writer, so a runaway
//     print loop cannot exhaust memory (bounded by max_output_bytes each).
//   - Environment: the child gets a minimal allowlisted env (PATH + a sandboxed
//     HOME/TMPDIR + language cache dirs). Caller secrets (API keys, tokens) in
//     the parent environment are NOT inherited.
//
// Known limits (honest, not a real sandbox):
//   - Network is NOT blocked by this code alone. Portable, unprivileged network
//     namespacing is not available from pure Go. To actually cut the network,
//     set sandbox_command to a wrapper that does (e.g. ["unshare","-rn","--"],
//     ["firejail","--net=none","--"], or an nsjail invocation) AND/OR run the
//     whole pipeline pod under a deny-all-egress NetworkPolicy. Without one of
//     these, executed code can reach the network.
//   - No CPU/memory/pids rlimits are imposed here; rely on the container's
//     cgroup limits (and the timeout + output cap) to bound resource use.
//   - Filesystem writes outside the temp dir are possible if the container
//     grants them; run with a read-only root filesystem where feasible.
type SubprocessExecutor struct {
	cfg ExecutorConfig
}

// newExecutor builds the concrete executor from config. It always returns a
// usable value; when cfg.Enabled is false, Enabled() reports false and verify
// never calls Run.
func newExecutor(cfg ExecutorConfig) *SubprocessExecutor {
	return &SubprocessExecutor{cfg: cfg}
}

// Enabled reports the config opt-in flag.
func (e *SubprocessExecutor) Enabled() bool { return e.cfg.Enabled }

// Supports reports whether the canonical language has a configured runner.
func (e *SubprocessExecutor) Supports(language string) bool {
	_, ok := e.cfg.Languages[language]
	return ok
}

// Run executes req.Code in an isolated temp dir with a wall-clock timeout and
// capped output. It returns an error only for setup/spawn failures.
func (e *SubprocessExecutor) Run(ctx context.Context, req ExecRequest) (ExecResult, error) {
	runner, ok := e.cfg.Languages[req.Language]
	if !ok {
		return ExecResult{}, fmt.Errorf("executor: unsupported language %q", req.Language)
	}
	if len(runner.Command) == 0 {
		return ExecResult{}, fmt.Errorf("executor: language %q has no command", req.Language)
	}

	dir, err := os.MkdirTemp("", "distill-exec-*")
	if err != nil {
		return ExecResult{}, fmt.Errorf("executor: temp dir: %w", err)
	}
	defer os.RemoveAll(dir)

	srcPath := filepath.Join(dir, runner.Filename)
	if err := os.WriteFile(srcPath, []byte(req.Code), 0o600); err != nil {
		return ExecResult{}, fmt.Errorf("executor: write source: %w", err)
	}

	timeout := time.Duration(e.cfg.TimeoutSeconds) * time.Second
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Assemble argv: optional sandbox wrapper (e.g. unshare -rn --) then the
	// language command. The sandbox_command is where real isolation (network
	// namespace) is plugged in by operators.
	argv := append(append([]string{}, e.cfg.SandboxCommand...), runner.Command...)
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = e.childEnv(dir)

	outBuf := &capWriter{max: e.cfg.MaxOutputBytes}
	errBuf := &capWriter{max: e.cfg.MaxOutputBytes}
	cmd.Stdout = outBuf
	cmd.Stderr = errBuf

	// Put the child in its own process group and kill the whole group on
	// timeout/cancel, so a shell that forks children does not orphan them.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			// Negative pid targets the process group.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	// After Cancel, give the group a brief grace, then let Wait return even if
	// a stuck child holds the output pipes open.
	cmd.WaitDelay = 2 * time.Second

	start := time.Now()
	runErr := cmd.Run()
	dur := time.Since(start)

	res := ExecResult{
		Stdout:    outBuf.String(),
		Stderr:    errBuf.String(),
		Truncated: outBuf.truncated || errBuf.truncated,
		Duration:  dur,
		ExitCode:  exitCode(cmd, runErr),
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		res.TimedOut = true
	}
	return res, nil
}

// childEnv returns a minimal allowlisted environment for the child. It keeps
// PATH (so interpreters resolve) but points HOME/TMPDIR and language caches at
// the sandbox dir, and drops every other parent variable (including secrets).
func (e *SubprocessExecutor) childEnv(dir string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + dir,
		"TMPDIR=" + dir,
		// Language build/tooling caches, kept inside the sandbox so a real
		// runner (go/python) writes there instead of the caller's home.
		"GOCACHE=" + filepath.Join(dir, "gocache"),
		"GOPATH=" + filepath.Join(dir, "gopath"),
		"GOFLAGS=-mod=mod",
		"PYTHONDONTWRITEBYTECODE=1",
	}
	return env
}

// exitCode extracts the process exit code, distinguishing a clean run (0), a
// program-reported failure (>0), and a killed/never-started process (-1).
func exitCode(cmd *exec.Cmd, runErr error) int {
	if cmd.ProcessState != nil {
		return cmd.ProcessState.ExitCode()
	}
	if runErr == nil {
		return 0
	}
	return -1
}

// capWriter is an io.Writer that stores at most max bytes and discards the rest,
// recording whether truncation occurred. It bounds memory for runaway output.
type capWriter struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	max       int
	truncated bool
}

func (w *capWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.max <= 0 || w.buf.Len() >= w.max {
		if len(p) > 0 {
			w.truncated = true
		}
		return len(p), nil // accept-and-discard so the child never blocks
	}
	remaining := w.max - w.buf.Len()
	if len(p) > remaining {
		w.buf.Write(p[:remaining])
		w.truncated = true
		return len(p), nil
	}
	w.buf.Write(p)
	return len(p), nil
}

func (w *capWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// --- code-fence parsing ------------------------------------------------------

// fencePattern matches a Markdown fenced code block, capturing the info string
// (language tag) and the body. It is non-greedy so the first complete block
// wins.
var fencePattern = regexp.MustCompile("(?s)```([^\n`]*)\n(.*?)\n?```")

// langAliases maps code-fence language tags to canonical runner keys. Only
// languages with an entry here are considered executable.
var langAliases = map[string]string{
	"python": "python", "py": "python", "python3": "python",
	"go": "go", "golang": "go",
}

// parseCodeFence returns the first fenced code block whose language tag maps to
// a canonical supported language. ok is false when no such block exists (the
// answer is prose, or fences use an unsupported/blank language).
func parseCodeFence(answer string) (lang, code string, ok bool) {
	for _, m := range fencePattern.FindAllStringSubmatch(answer, -1) {
		tag := strings.ToLower(strings.TrimSpace(m[1]))
		// The info string may carry extra words (e.g. "python title=..."); use
		// the first token as the language.
		if i := strings.IndexAny(tag, " \t"); i >= 0 {
			tag = tag[:i]
		}
		canon, known := langAliases[tag]
		if !known {
			continue
		}
		body := m[2]
		if strings.TrimSpace(body) == "" {
			continue
		}
		return canon, body, true
	}
	return "", "", false
}

// execMethod labels how a snippet was verified. Python code carrying assert /
// unittest / pytest markers self-checks its own output, so running it is a
// "test"; everything else is plain "exec". Both are valid verification methods.
func execMethod(lang, code string) string {
	if lang == "python" {
		if strings.Contains(code, "assert") ||
			strings.Contains(code, "unittest") ||
			strings.Contains(code, "pytest") {
			return "test"
		}
	}
	return "exec"
}
