package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/teacher"
)

// ComprehendWorker turns a queued source pointer into a comprehension digest --
// the material the question worker backtranslates from (todos sec 2-5-3/2-5-4).
//
// The digest is produced by an injected Comprehender so the stage work is
// swappable and unit-testable:
//   - OpencodeComprehender (production): shells out to the opencode agentic CLI,
//     which fetches the source link, reads it with auto-compaction, and emits a
//     faithful digest grounded in the real source content (todos sec 2-5-4).
//   - ProvenanceComprehender (fallback/dev): stitches the queue row's preserved
//     excerpt + provenance into the digest without any network fetch, so the
//     stage stays runnable where opencode is not installed / disabled.
//
// The queue transition contract is unchanged from every other worker: Claim ->
// digest -> SetComprehension(digest, owner) -> Complete(owner), every write
// fenced on the lock owner the item was claimed with (todos sec 2-5-2).
type ComprehendWorker struct {
	store        Store
	comprehender Comprehender
	gate         Gate
	workerID     string
	// retry governs transient-error rescheduling vs terminal failure.
	retry RetryPolicy
	// Log receives progress lines (stderr in production, nil/discard in tests).
	Log io.Writer
}

// NewComprehendWorker wires the worker with its queue store, the digest producer
// (opencode or provenance fallback), the quota gate (checks the opencode provider
// when opencode is enabled -- todos sec 6-3-4; an empty gate when the provenance
// fallback is used, which makes no network call), and the lock owner id.
func NewComprehendWorker(store Store, comprehender Comprehender, gate Gate, workerID string) *ComprehendWorker {
	return &ComprehendWorker{store: store, comprehender: comprehender, gate: gate, workerID: workerID, retry: DefaultRetryPolicy()}
}

// RunOnce drains every item pending at the comprehend stage under the opencode
// provider quota gate. Each item is claimed, digested, and completed; a digest
// failure is classified (transient -> retried with backoff, terminal -> failed)
// exactly like every other stage.
func (w *ComprehendWorker) RunOnce(ctx context.Context) (int, error) {
	return gatedDrain(ctx, w.store, w.gate, queue.StageComprehend, w.workerID, func(it queue.Item) (bool, error) {
		own := ownerOf(it)
		digest, err := w.comprehender.Comprehend(ctx, it)
		if err != nil {
			// A stage-work failure: handleStageError retries it (transient, e.g. an
			// opencode exec/endpoint failure wrapped with teacher.ErrTransient) or
			// terminally fails it (a contract error, e.g. empty provenance).
			return isRateLimited(err), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageComprehend, err)
		}
		if err := w.store.SetComprehension(ctx, it.ID, digest, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StageComprehend, it, err)
		}
		if err := w.store.Complete(ctx, it.ID, queue.StageComprehend, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StageComprehend, it, err)
		}
		logf(w.Log, "[comprehend] item %d (%s/%s) digested (%d chars)", it.ID, it.SourceType, it.DocID, len(digest))
		return false, nil
	})
}

// Comprehender produces the comprehension digest for one claimed item. It is the
// seam between the queue-transition machinery (ComprehendWorker) and the actual
// comprehension strategy (agentic opencode fetch vs provenance stitching), so the
// worker is unit-testable without a real opencode binary. A returned error is
// classified by handleStageError: wrap it with teacher.ErrTransient for a
// retryable failure (exec/endpoint hiccup), return a plain error for a terminal
// contract failure (nothing to comprehend).
type Comprehender interface {
	Comprehend(ctx context.Context, it queue.Item) (string, error)
}

// ProvenanceComprehender is the no-network fallback: it stitches the item's
// preserved source excerpt and provenance into an anchor for backtranslation.
// Grounding on the ingest excerpt (todos #5), not title/URL alone, is what keeps
// the reverse-generated question tied to real source content when opencode is
// disabled or unavailable. Empty provenance is a terminal contract error (the
// item cannot be comprehended), so it is failed, never silently completed.
type ProvenanceComprehender struct{}

// Comprehend returns the provenance-stitched digest, or a terminal error when the
// item carries no excerpt/title/url/locator to anchor on.
func (ProvenanceComprehender) Comprehend(_ context.Context, it queue.Item) (string, error) {
	digest := buildDigest(it)
	if digest == "" {
		return "", fmt.Errorf("no comprehensible provenance (empty excerpt/title/url/locator)")
	}
	return digest, nil
}

// Compile-time: ProvenanceComprehender satisfies Comprehender.
var _ Comprehender = ProvenanceComprehender{}

// buildDigest stitches the item's preserved source excerpt and provenance into an
// anchor for backtranslation (the ProvenanceComprehender payload).
func buildDigest(it queue.Item) string {
	var parts []string
	if it.SourceExcerpt != "" {
		parts = append(parts, "Excerpt:\n"+it.SourceExcerpt)
	}
	if it.Title != "" {
		parts = append(parts, "Title: "+it.Title)
	}
	if it.URL != "" {
		parts = append(parts, "Source: "+it.URL)
	}
	if it.Locator != "" {
		parts = append(parts, "Locator: "+it.Locator)
	}
	return strings.Join(parts, "\n")
}

// --- opencode agentic comprehender ------------------------------------------

// digestOpenTag / digestCloseTag delimit the digest in the opencode agent's
// stdout. The agent (config/opencode.json) is instructed to emit ONLY the digest
// between these tags; the worker extracts that span, so incidental formatting the
// CLI prints around the answer never leaks into the stored digest.
const (
	digestOpenTag  = "<comprehension_digest>"
	digestCloseTag = "</comprehension_digest>"
)

// OpencodeConfig is the resolved opencode invocation config the comprehend worker
// needs (built from runner.Config so nothing is hardcoded -- todos sec 2-5-4:
// "provider/baseURL=subgate, agent 정의 는 configmap"). BaseURL/provider/apiKey
// live in the opencode config file (ConfigPath, mounted by the deploy overlay);
// this struct only carries what the CLI invocation itself needs.
type OpencodeConfig struct {
	// BinPath is the opencode executable; "" / "opencode" resolves on PATH.
	BinPath string
	// ConfigPath points opencode at its config file (provider=subgate baseURL +
	// the comprehend agent). Exported to the subprocess as OPENCODE_CONFIG; ""
	// lets opencode discover a config from the working dir / global location.
	ConfigPath string
	// Model is the "provider/model" passed to `opencode run --model` (e.g.
	// "subgate/gpt-5.4"). "" lets the opencode config's default model stand.
	Model string
	// Agent is the agent name passed to `opencode run --agent` (e.g.
	// "comprehend"). "" uses opencode's default primary agent.
	Agent string
	// Timeout bounds one opencode invocation; <=0 means no worker-side deadline
	// (the parent context still applies).
	Timeout time.Duration
}

// commandRunner abstracts running the opencode subprocess so tests inject a fake
// without a real binary. It returns the child's stdout (the parsed surface) and
// an error on spawn failure / non-zero exit / context deadline; stderr is folded
// into that error for diagnostics.
type commandRunner interface {
	Run(ctx context.Context, name string, args, env []string) (stdout []byte, err error)
}

// execRunner is the production commandRunner over os/exec.
type execRunner struct{}

// Run executes name with args and env, capturing stdout and stderr separately so
// only the model's answer (stdout) is parsed while stderr enriches an error.
func (execRunner) Run(ctx context.Context, name string, args, env []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("%v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// OpencodeComprehender drives the opencode agentic CLI to fetch a source link,
// read it with auto-compaction, and emit a comprehension digest (todos sec
// 2-5-4). It points opencode at subgate (its config's provider baseURL), so the
// read spends the subscription quota -- the intended cost, and question
// generation needs no raw CoT. Any exec/endpoint failure is surfaced as transient
// so the queue retries the item rather than terminally failing it: opencode is
// always present in the deployed image, so a missing binary / crashed run /
// endpoint hiccup is a temporary condition, and after MaxRetries the item fails
// loudly instead of silently degrading to a title-only digest.
type OpencodeComprehender struct {
	cfg    OpencodeConfig
	runner commandRunner
	log    io.Writer
}

// NewOpencodeComprehender builds the production opencode comprehender over
// os/exec. log (may be nil) receives per-item invocation lines.
func NewOpencodeComprehender(cfg OpencodeConfig, log io.Writer) *OpencodeComprehender {
	return &OpencodeComprehender{cfg: cfg, runner: execRunner{}, log: log}
}

// Compile-time: OpencodeComprehender satisfies Comprehender.
var _ Comprehender = (*OpencodeComprehender)(nil)

// Comprehend runs opencode over the item's source pointer and returns the parsed
// digest. Spawn/exec/timeout failures and an empty result are wrapped with
// teacher.ErrTransient so handleStageError reschedules the item (todos #4).
func (o *OpencodeComprehender) Comprehend(ctx context.Context, it queue.Item) (string, error) {
	if o.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.cfg.Timeout)
		defer cancel()
	}
	bin := o.cfg.BinPath
	if bin == "" {
		bin = "opencode"
	}
	msg := buildComprehendPrompt(it)
	logf(o.log, "[comprehend] item %d (%s/%s) -> opencode fetch %s", it.ID, it.SourceType, it.DocID, it.URL)
	stdout, err := o.runner.Run(ctx, bin, o.args(msg), o.env())
	if err != nil {
		// A subprocess has no structured error channel, so a provider 429 only
		// surfaces as text in opencode's stderr (folded into err by the runner).
		// Classify it as ErrRateLimited -- still transient/retried, but ALSO the
		// signal gatedDrain uses to arm the gate cooldown and stop the pass;
		// wrapping it as plain ErrTransient would drop that signal and keep
		// spawning opencode runs straight into the same 429.
		if isRateLimitText(err.Error()) {
			return "", fmt.Errorf("%w: opencode run: %v", teacher.ErrRateLimited, err)
		}
		// Missing binary / non-zero exit / context deadline: transient so the
		// queue retries instead of terminally failing the item.
		return "", fmt.Errorf("%w: opencode run: %v", teacher.ErrTransient, err)
	}
	digest := extractDigest(string(stdout))
	if digest == "" {
		return "", fmt.Errorf("%w: opencode returned an empty digest", teacher.ErrTransient)
	}
	return digest, nil
}

// rateLimitTextRe matches the ways a provider 429 shows up in CLI error text: a
// standalone 429 status token (word-bounded, so byte counts like 4290 do not
// match), "rate limit"/"rate-limit"/"rate_limit", or "too many requests".
var rateLimitTextRe = regexp.MustCompile(`(?i)\b429\b|rate.?limit|too many requests`)

// isRateLimitText reports whether CLI error text indicates a provider 429.
func isRateLimitText(s string) bool {
	return rateLimitTextRe.MatchString(s)
}

// args builds the `opencode run` argv: non-interactive prompt, explicit
// agent/model when configured, and --auto so no permission prompt can block a
// headless run (the comprehend agent denies write/edit/bash anyway).
func (o *OpencodeComprehender) args(msg string) []string {
	args := []string{"run"}
	if o.cfg.Agent != "" {
		args = append(args, "--agent", o.cfg.Agent)
	}
	if o.cfg.Model != "" {
		args = append(args, "--model", o.cfg.Model)
	}
	args = append(args, "--auto", msg)
	return args
}

// env is the subprocess environment: the parent's (PATH, HOME, subgate api-key
// var) plus OPENCODE_CONFIG when a config path is configured, so opencode loads
// the mounted provider+agent definition regardless of its working directory.
func (o *OpencodeComprehender) env() []string {
	env := os.Environ()
	if o.cfg.ConfigPath != "" {
		env = append(env, "OPENCODE_CONFIG="+o.cfg.ConfigPath)
	}
	return env
}

// buildComprehendPrompt is the per-item run message. It carries the source
// pointer + preserved excerpt; the standing behaviour (fetch, read, auto-compact,
// emit only the tagged digest, do not invent) lives in the agent's system prompt
// (config/opencode.json), so this stays a compact instance payload.
func buildComprehendPrompt(it queue.Item) string {
	var b strings.Builder
	b.WriteString("Comprehend this source document for a distillation pipeline.\n")
	if it.URL != "" {
		b.WriteString("Source URL: " + it.URL + "\n")
	}
	if it.Title != "" {
		b.WriteString("Title: " + it.Title + "\n")
	}
	if it.Locator != "" {
		b.WriteString("Locator: " + it.Locator + "\n")
	}
	if it.License != "" {
		b.WriteString("License: " + it.License + "\n")
	}
	if it.SourceExcerpt != "" {
		b.WriteString("Excerpt (may be partial):\n" + it.SourceExcerpt + "\n")
	}
	b.WriteString("\nFetch the source URL and read it fully (rely on auto-compaction for long content). " +
		"Then write a faithful comprehension digest that a downstream worker will backtranslate into a question: " +
		"capture the core problem, claims, method, and context grounded in the actual source content, not the title/URL alone. " +
		"Output ONLY the digest wrapped exactly between " + digestOpenTag + " and " + digestCloseTag + ".")
	return b.String()
}

// extractDigest pulls the digest span from opencode's stdout: the text between
// the digest tags when present, otherwise the whole trimmed output as a
// best-effort fallback (the agent is instructed to emit the tags, but a stray
// formatting miss should still yield a usable digest rather than a hard failure).
func extractDigest(stdout string) string {
	if i := strings.Index(stdout, digestOpenTag); i >= 0 {
		rest := stdout[i+len(digestOpenTag):]
		if j := strings.Index(rest, digestCloseTag); j >= 0 {
			return strings.TrimSpace(rest[:j])
		}
	}
	return strings.TrimSpace(stdout)
}
