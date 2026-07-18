package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/teacher"
)

// fakeCommandRunner is an in-memory commandRunner: it records the invocation and
// returns scripted stdout/err, so the opencode comprehender is tested with no
// real binary.
type fakeCommandRunner struct {
	stdout  []byte
	err     error
	calls   int
	gotName string
	gotArgs []string
	gotEnv  []string
}

func (f *fakeCommandRunner) Run(_ context.Context, name string, args, env []string) ([]byte, error) {
	f.calls++
	f.gotName, f.gotArgs, f.gotEnv = name, args, env
	return f.stdout, f.err
}

// --- provenance fallback path (opencode disabled / unavailable) -------------

// TestComprehend_Fallback_DigestsAndCompletes: with the provenance comprehender
// (opencode disabled), a queued item is digested from its preserved excerpt +
// provenance and advanced to comprehended.
func TestComprehend_Fallback_DigestsAndCompletes(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageComprehend)
	w := NewComprehendWorker(s, ProvenanceComprehender{}, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.NotNil(t, fi.it.ComprehendedAt, "comprehended_at stamped")
	assert.NotEmpty(t, fi.digest, "digest written")
	assert.Contains(t, fi.digest, "T-2501.1", "provenance digest carries the title")
	assert.Equal(t, 1, s.completes[queue.StageComprehend])
}

// TestComprehend_Fallback_EmptyProvenanceFails: an item with no excerpt/title/
// url/locator cannot be digested and is failed terminally (not silently
// completed, not retried).
func TestComprehend_Fallback_EmptyProvenanceFails(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageComprehend)
	fi.it.Title, fi.it.URL, fi.it.Locator, fi.it.SourceExcerpt = "", "", "", ""
	w := NewComprehendWorker(s, ProvenanceComprehender{}, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "one item drained (claimed and moved to a terminal state)")
	assert.NotNil(t, fi.it.FailedAt, "item failed")
	assert.Equal(t, "comprehend", fi.it.FailStage)
	assert.Nil(t, fi.it.ComprehendedAt)
	assert.Equal(t, 0, s.retries, "a contract error is terminal, never retried")
}

// TestComprehend_OnlyClaimsPendingStage: an item already past comprehend is not
// re-claimed.
func TestComprehend_OnlyClaimsPendingStage(t *testing.T) {
	s := newFakeStore()
	s.add(1, "arxiv", "past", queue.StageQuestion) // already comprehended
	w := NewComprehendWorker(s, ProvenanceComprehender{}, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n, "nothing pending at comprehend")
}

// --- opencode agentic path --------------------------------------------------

// TestComprehend_Opencode_Success: opencode returns a tagged digest; the worker
// extracts it, stores it, completes the item, and invokes opencode with the
// configured agent/model/config and a prompt carrying the source pointer.
func TestComprehend_Opencode_Success(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageComprehend)
	runner := &fakeCommandRunner{stdout: []byte(
		"tool: webfetch http://x/2501.1\n" +
			digestOpenTag + "\nCore idea: bound exploding gradients by clipping the norm.\n" + digestCloseTag +
			"\n(done)")}
	oc := &OpencodeComprehender{
		cfg: OpencodeConfig{
			BinPath: "opencode", Model: "subgate/gpt-5.4", Agent: "comprehend",
			ConfigPath: "/etc/opencode/opencode.json",
		},
		runner: runner,
	}
	w := NewComprehendWorker(s, oc, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.NotNil(t, fi.it.ComprehendedAt, "comprehended_at stamped")
	assert.Equal(t, "Core idea: bound exploding gradients by clipping the norm.", fi.digest,
		"digest is the extracted, trimmed tag span (surrounding CLI noise dropped)")
	assert.Equal(t, 1, s.completes[queue.StageComprehend])

	// The subprocess was invoked with the configured binary/agent/model + --auto,
	// the prompt (carrying the source URL) last, and OPENCODE_CONFIG in the env.
	require.Equal(t, 1, runner.calls)
	assert.Equal(t, "opencode", runner.gotName)
	assert.Equal(t, []string{"run", "--agent", "comprehend", "--model", "subgate/gpt-5.4", "--auto"},
		runner.gotArgs[:len(runner.gotArgs)-1], "argv before the prompt")
	prompt := runner.gotArgs[len(runner.gotArgs)-1]
	assert.Contains(t, prompt, "http://x/2501.1", "prompt carries the source URL to fetch")
	assert.Contains(t, prompt, digestOpenTag, "prompt instructs the tagged-digest contract")
	assert.Contains(t, runner.gotEnv, "OPENCODE_CONFIG=/etc/opencode/opencode.json")
}

// TestComprehend_Opencode_ExecFailure_Retries: an opencode spawn/exec failure
// (e.g. binary missing) is transient, so the item is rescheduled with backoff,
// not terminally failed.
func TestComprehend_Opencode_ExecFailure_Retries(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageComprehend)
	runner := &fakeCommandRunner{err: errors.New(`exec: "opencode": executable file not found in $PATH`)}
	oc := &OpencodeComprehender{cfg: OpencodeConfig{Agent: "comprehend"}, runner: runner}
	w := NewComprehendWorker(s, oc, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err, "a per-item transient failure never aborts the drain")
	assert.Equal(t, 1, n)
	assert.Nil(t, fi.it.ComprehendedAt, "not completed")
	assert.Nil(t, fi.it.FailedAt, "not terminally failed -- it is retried")
	assert.Equal(t, 1, s.retries, "transient exec failure reschedules the item")
	assert.NotNil(t, fi.it.NextAttemptAt, "backoff scheduled")
}

// TestComprehend_Opencode_EmptyOutput_Retries: opencode exits cleanly but returns
// no digest; that is treated as a transient endpoint hiccup and retried.
func TestComprehend_Opencode_EmptyOutput_Retries(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageComprehend)
	runner := &fakeCommandRunner{stdout: []byte("   \n  ")}
	oc := &OpencodeComprehender{cfg: OpencodeConfig{Agent: "comprehend"}, runner: runner}
	w := NewComprehendWorker(s, oc, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Nil(t, fi.it.ComprehendedAt)
	assert.Nil(t, fi.it.FailedAt)
	assert.Equal(t, 1, s.retries, "empty digest is retried, not terminally failed")
}

// TestComprehend_Opencode_TransientClassification: the opencode failure wraps
// teacher.ErrTransient so the shared retry classifier reschedules it.
func TestComprehend_Opencode_TransientClassification(t *testing.T) {
	oc := &OpencodeComprehender{cfg: OpencodeConfig{}, runner: &fakeCommandRunner{err: errors.New("boom")}}
	_, err := oc.Comprehend(context.Background(), queue.Item{URL: "http://x/1"})
	require.Error(t, err)
	assert.True(t, teacher.IsTransient(err), "opencode exec failures must classify as transient")
	assert.False(t, errors.Is(err, teacher.ErrRateLimited), "a plain failure is not a 429")
}

// TestComprehend_Opencode_RateLimitClassification: a provider 429 surfacing in
// opencode's error text classifies as ErrRateLimited (not merely transient), so
// the gate cooldown signal is preserved through the subprocess boundary.
func TestComprehend_Opencode_RateLimitClassification(t *testing.T) {
	for _, msg := range []string{
		"exit status 1: provider request failed: HTTP 429 Too Many Requests",
		"exit status 1: rate limit exceeded, retry later",
		"exit status 1: subgate: rate-limited",
	} {
		oc := &OpencodeComprehender{cfg: OpencodeConfig{}, runner: &fakeCommandRunner{err: errors.New(msg)}}
		_, err := oc.Comprehend(context.Background(), queue.Item{URL: "http://x/1"})
		require.Error(t, err)
		assert.True(t, errors.Is(err, teacher.ErrRateLimited), "%q must classify as rate limited", msg)
	}
	// Digit runs containing 429 must NOT false-positive.
	oc := &OpencodeComprehender{cfg: OpencodeConfig{}, runner: &fakeCommandRunner{err: errors.New("read 4290 bytes then EOF")}}
	_, err := oc.Comprehend(context.Background(), queue.Item{URL: "http://x/1"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, teacher.ErrRateLimited), "4290 is not a 429 status")
}

// TestComprehend_Opencode_RateLimitArmsCooldown: a 429 from opencode reaches the
// gate (NoteRateLimited) and ends the pass, so the worker stops spawning opencode
// runs straight into the same rate limit; the item itself is rescheduled.
func TestComprehend_Opencode_RateLimitArmsCooldown(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageComprehend)
	s.add(2, "arxiv", "2501.2", queue.StageComprehend)
	gate := openGate()
	runner := &fakeCommandRunner{err: errors.New("exit status 1: HTTP 429 Too Many Requests")}
	oc := &OpencodeComprehender{cfg: OpencodeConfig{Agent: "comprehend"}, runner: runner}
	w := NewComprehendWorker(s, oc, gate, "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the pass stops after the rate-limited item; item 2 is not attempted")
	assert.Equal(t, 1, gate.rateLimited, "gate cooldown armed")
	assert.Nil(t, fi.it.FailedAt)
	assert.Equal(t, 1, s.retries, "the 429'd item is rescheduled, not failed")
}

// TestExtractDigest covers the tag-span extraction and its fallbacks.
func TestExtractDigest(t *testing.T) {
	assert.Equal(t, "hi", extractDigest("noise "+digestOpenTag+"\n hi \n"+digestCloseTag+" trailing"),
		"tagged span is extracted and trimmed")
	assert.Equal(t, "plain answer", extractDigest("  plain answer  "),
		"no tags -> whole trimmed output (best-effort fallback)")
	assert.Equal(t, "", extractDigest("   \n  "), "blank output -> empty (caller treats as failure)")
	assert.Equal(t, digestOpenTag+"partial", extractDigest(digestOpenTag+"partial"),
		"unterminated open tag -> whole trimmed output")
}

// TestBuildComprehendPrompt: the per-item prompt carries the source pointer,
// preserved excerpt, and the tagged-digest output contract.
func TestBuildComprehendPrompt(t *testing.T) {
	it := queue.Item{URL: "http://x/2501.1", Title: "Grad clipping", Locator: "sec-2",
		License: "CC-BY", SourceExcerpt: "clip the gradient norm"}
	p := buildComprehendPrompt(it)
	for _, want := range []string{"http://x/2501.1", "Grad clipping", "sec-2", "CC-BY",
		"clip the gradient norm", digestOpenTag, digestCloseTag, "auto-compaction"} {
		assert.True(t, strings.Contains(p, want), "prompt must contain %q", want)
	}
}
