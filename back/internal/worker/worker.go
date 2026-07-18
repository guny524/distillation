package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/teacher"
)

// RetryPolicy governs queue-level retries (todos #4). A stage error that is
// retryable -- a transport transient (teacher.IsTransient: network/429/5xx/
// timeout) or a model-output contract violation (pipeline.IsBadModelOutput) --
// and has not exhausted MaxRetries reschedules the item with an exponential
// backoff (BaseBackoff doubling, capped at MaxBackoff); every other error, and
// retry exhaustion, fails the item terminally.
type RetryPolicy struct {
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// DefaultRetryPolicy is the sane default every worker starts with: five
// reschedules with a 1-minute base backoff doubling to a 30-minute cap.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxRetries: 5, BaseBackoff: time.Minute, MaxBackoff: 30 * time.Minute}
}

// backoffSeconds is the delay before the (retryCount+1)-th attempt: BaseBackoff
// doubled retryCount times, capped at MaxBackoff.
func (rp RetryPolicy) backoffSeconds(retryCount int) float64 {
	shift := retryCount
	if shift < 0 {
		shift = 0
	}
	if shift > 20 { // guard against time.Duration overflow on absurd counts
		shift = 20
	}
	d := rp.BaseBackoff << uint(shift)
	if d <= 0 || d > rp.MaxBackoff {
		d = rp.MaxBackoff
	}
	return d.Seconds()
}

// ResolveWorkerID derives a PROCESS-UNIQUE lock owner id to stamp on every claim
// (todos sec 2-5-2). It composes a human-readable base -- POD_NAME (a k8s replica
// has a unique pod name) or the hostname -- with a random suffix that is ALWAYS
// appended: `<base>-<rand8>`.
//
// The suffix is what makes the id process-unique, and that is the correctness
// point: two processes on the SAME host share a hostname (and, in a restart edge,
// could even reuse a POD_NAME), so a base-only owner lets a crashed worker's late
// write collide with a new worker that happens to share the base -- an ABA that
// silently defeats the lock fence (the stale write matches the current owner by
// coincidence). A random suffix per process makes that collision astronomically
// unlikely.
//
// Callers resolve the id ONCE at startup and reuse it for the worker's lifetime,
// so a single worker keeps one stable owner across claim..complete (the terminal
// write is also fenced on the owner read back from the claimed item, i.e. exactly
// the id that claimed). When the random source fails (never expected), the PID --
// unique among a host's live processes -- is a safe fallback that still yields a
// process-distinct, non-empty owner.
func ResolveWorkerID() string {
	base := os.Getenv("POD_NAME")
	if base == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			base = h
		} else {
			base = "worker"
		}
	}
	return base + "-" + workerIDSuffix()
}

// workerIDSuffix returns a random 8-byte hex string, or a PID-derived token when
// the random source is unavailable (a live PID is unique among a host's processes,
// so it still yields a process-distinct owner and is never empty).
func workerIDSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return "pid" + strconv.Itoa(os.Getpid())
}

// itemArtifact reconstructs the artifact.Artifact from a claimed item's
// provenance plus the comprehension digest. The queue row stores only the source
// pointer (no chunks -- comprehend re-fetches the source, todos sec 2-5-4), so
// the digest produced by the comprehend stage becomes the single excerpt chunk
// the question stage backtranslates from.
func itemArtifact(it queue.Item, digest string) artifact.Artifact {
	art := artifact.Artifact{
		SourceType: artifact.SourceType(it.SourceType),
		DocID:      it.DocID,
		URL:        it.URL,
		License:    it.License,
		Title:      it.Title,
		Locator:    it.Locator,
	}
	if it.RetrievedAt != nil {
		art.RetrievedAt = *it.RetrievedAt
	}
	if digest != "" {
		art.Chunks = []artifact.Chunk{{Text: digest, Locator: it.Locator}}
	}
	return art
}

// logf writes a progress line to w (io.Discard when w is nil), so workers stay
// silent in tests but log to stderr in production.
func logf(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, format+"\n", args...)
}

// drain claims and processes pending items at stage until nothing is claimable,
// returning the number processed. process owns one claimed item end to end,
// including its terminal transition (Complete/Fail/Supersede/Retry); it returns
// an error only for an infrastructure failure that should abort the pass (a
// per-item stage failure is handled inside process -- retried or terminally
// failed -- and returns nil, so one poison item never stalls the whole queue; a
// lost lease also returns nil, since another worker now owns the item). A Claim
// error aborts. A live worker's lease is protected from stale reclaim by
// queue.DefaultStaleLock being configured above a stage's worst-case processing
// time (todos sec 2-5-2 (a)); fencing is the correctness backstop if it is not.
func drain(ctx context.Context, s Store, stage queue.Stage, workerID string, process func(queue.Item) error) (int, error) {
	n := 0
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		item, ok, err := s.Claim(ctx, stage, workerID)
		if err != nil {
			return n, fmt.Errorf("claim %s: %w", stage, err)
		}
		if !ok {
			return n, nil
		}
		if err := process(item); err != nil {
			return n, err
		}
		n++
	}
}

// gatedDrain is drain plus a per-claim quota gate (todos sec 6-3-4): before EACH
// claim it asks the gate whether the step's subgate roles are within budget, and a
// closed gate (exhausted / unreachable /quota / active cooldown) ends the pass
// without claiming. process owns one claimed item end to end (its Complete/Fail/
// Supersede/Retry) and reports whether it observed a 429 (rateLimited); a 429 arms
// the gate's process-local cooldown and ends the pass, since the item was already
// rescheduled inside process and further claims would just 429 again. A Claim or
// infrastructure error aborts. Re-checking the gate every claim bounds overshoot
// past exhaustion to the items already in flight.
func gatedDrain(ctx context.Context, s Store, gate Gate, stage queue.Stage, workerID string, process func(queue.Item) (rateLimited bool, err error)) (int, error) {
	n := 0
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if !gate.Allow(ctx) {
			return n, nil // quota gate closed: skip the step this pass
		}
		item, ok, err := s.Claim(ctx, stage, workerID)
		if err != nil {
			return n, fmt.Errorf("claim %s: %w", stage, err)
		}
		if !ok {
			return n, nil
		}
		rateLimited, perr := process(item)
		if perr != nil {
			return n, perr
		}
		n++
		if rateLimited {
			gate.NoteRateLimited()
			return n, nil
		}
	}
}

// isRateLimited reports whether a stage error was a teacher 429 -- the signal that
// arms the gate cooldown and stops a gated pass (todos sec 6-3-4).
func isRateLimited(err error) bool {
	return errors.Is(err, teacher.ErrRateLimited)
}

// QuotaChecksFor builds a step's gate checks from the role endpoint map and the
// role names the step uses (todos sec 6-3-4 step->role table). It emits one check
// per role that carries a quota_url (a subgate subscription); a role WITHOUT a
// quota_url (a real API / local model, e.g. the student) yields no check and is
// used with no quota read. Order follows names; an unknown role name is skipped
// (role presence is validated by runner.Config).
func QuotaChecksFor(roles map[string]teacher.RoleConfig, names ...string) []QuotaCheck {
	var checks []QuotaCheck
	for _, n := range names {
		rc, ok := roles[n]
		if !ok || rc.QuotaURL == "" {
			continue
		}
		checks = append(checks, QuotaCheck{Role: n, Provider: rc.QuotaProvider, URL: rc.QuotaURL})
	}
	return checks
}

// ownerOf returns the lock owner an item was claimed with (the fencing token for
// every subsequent write). It is non-empty on any freshly claimed item.
func ownerOf(it queue.Item) string {
	if it.LockOwnerID != nil {
		return *it.LockOwnerID
	}
	return ""
}

// handleStageError classifies a per-item STAGE-work failure (an LLM/parse/
// contract error from doing the stage) and records it, fenced on the item's lock
// owner. Two error classes are rescheduled with backoff (Retry) while retries
// remain: transport transients (teacher.IsTransient: network/429/5xx/timeout)
// and model-output contract violations (pipeline.IsBadModelOutput: a fresh LLM
// response that failed to parse/validate -- stochastic, so a bounded re-request
// usually succeeds; a stored-payload violation is NOT tagged and stays terminal,
// since re-running reproduces it identically). Every other error, and retry
// exhaustion, fails the item terminally (Fail). It returns nil when the
// transition succeeds OR the lease was lost (another worker owns the item now);
// it returns a non-nil error only when the queue itself is unwritable (drain
// aborts).
func handleStageError(ctx context.Context, s Store, rp RetryPolicy, w io.Writer, it queue.Item, stage queue.Stage, cause error) error {
	own := ownerOf(it)
	if (teacher.IsTransient(cause) || pipeline.IsBadModelOutput(cause)) && it.RetryCount < rp.MaxRetries {
		backoff := rp.backoffSeconds(it.RetryCount)
		logf(w, "[%s] item %d (%s/%s) retryable error (attempt %d/%d), retry in %.0fs: %v",
			stage, it.ID, it.SourceType, it.DocID, it.RetryCount+1, rp.MaxRetries, backoff, cause)
		return skipIfLeaseLost(w, stage, it, s.Retry(ctx, it.ID, backoff, cause.Error(), own))
	}
	logf(w, "[%s] item %d (%s/%s) failed (terminal): %v", stage, it.ID, it.SourceType, it.DocID, cause)
	return skipIfLeaseLost(w, stage, it, s.Fail(ctx, it.ID, stage, cause.Error(), own))
}

// skipIfLeaseLost maps a store-write result to a drain outcome: a lost lease is
// swallowed (logged, nil) so draining continues with the next item WITHOUT the
// caller failing an item it no longer owns; any other error is returned as-is
// (an infrastructure failure that aborts the pass; the item stays locked and is
// reclaimed as stale, so it is retried, not lost). A nil error passes through.
func skipIfLeaseLost(w io.Writer, stage queue.Stage, it queue.Item, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, queue.ErrLeaseLost) {
		logf(w, "[%s] item %d lease lost; another worker owns it, skipping", stage, it.ID)
		return nil
	}
	return err
}
