package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/teacher"
)

// TestFencing_StaleReclaimedWorkerCannotComplete: a worker whose lease was
// reclaimed as stale by another worker gets ErrLeaseLost from its late Complete
// (and every other fenced write), so it can never overwrite the new owner's
// result -- the keystone of todos #6.
func TestFencing_StaleReclaimedWorkerCannotComplete(t *testing.T) {
	s := newFakeStore()
	s.add(1, "arxiv", "2501.1", queue.StageComprehend)

	item, ok, err := s.Claim(context.Background(), queue.StageComprehend, "pod-a")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "pod-a", ownerOf(item))

	// Another worker reclaims the stale lease.
	s.reclaimBy(item.ID, "pod-b")

	// The original owner's fenced writes are all refused.
	assert.True(t, errors.Is(s.Complete(context.Background(), item.ID, queue.StageComprehend, "pod-a"), queue.ErrLeaseLost))
	assert.True(t, errors.Is(s.SetComprehension(context.Background(), item.ID, "late", "pod-a"), queue.ErrLeaseLost))
	assert.True(t, errors.Is(s.Fail(context.Background(), item.ID, queue.StageComprehend, "x", "pod-a"), queue.ErrLeaseLost))
	assert.True(t, errors.Is(s.Supersede(context.Background(), item.ID, "pod-a"), queue.ErrLeaseLost))

	// The new owner still holds the lease and can complete.
	require.NoError(t, s.Complete(context.Background(), item.ID, queue.StageComprehend, "pod-b"))
	assert.Equal(t, 1, s.completes[queue.StageComprehend])
}

// TestFencing_LeaseLostMidStageSkipsItem: when a payload write loses the lease
// mid-stage, the worker skips the item (does not fail it, does not abort the
// drain) -- another worker owns it now. The item is neither completed nor failed
// by the reclaimed worker.
func TestFencing_LeaseLostMidStageSkipsItem(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageComprehend)
	// A store that loses the lease exactly when the worker writes the digest.
	ls := &leaseLosingStore{fakeStore: s, loseOn: "SetComprehension"}
	w := NewComprehendWorker(ls, ProvenanceComprehender{}, openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err, "lease loss is not an infra error; drain continues")
	assert.Equal(t, 1, n, "the item was drained (skipped)")
	assert.Nil(t, fi.it.ComprehendedAt, "not completed by the reclaimed worker")
	assert.Nil(t, fi.it.FailedAt, "not failed either -- another worker owns it")
	assert.Equal(t, 0, s.fails[queue.StageComprehend])
}

// leaseLosingStore wraps fakeStore to inject an ErrLeaseLost on a chosen write,
// simulating a reclaim that lands mid-stage.
type leaseLosingStore struct {
	*fakeStore
	loseOn string
}

func (s *leaseLosingStore) SetComprehension(ctx context.Context, id int64, digest, owner string) error {
	if s.loseOn == "SetComprehension" {
		return fmt.Errorf("worker: set comprehension_digest item %d: %w", id, queue.ErrLeaseLost)
	}
	return s.fakeStore.SetComprehension(ctx, id, digest, owner)
}

var _ Store = (*leaseLosingStore)(nil)

// --- transient-retry vs terminal-fail classification (todos #4) --------------

// transientErr wraps teacher.ErrTransient so the classifier treats it as a
// retryable endpoint failure (network/429/5xx/timeout).
func transientErr() error {
	return fmt.Errorf("lane teacher(en): %w: dial tcp: connection refused", teacher.ErrTransient)
}

// TestRetry_TransientErrorReschedules: a transient stage error reschedules the
// item (retry_count++, next_attempt_at set, lock released) instead of failing it
// terminally -- the item is NOT marked failed and stays eligible after backoff.
func TestRetry_TransientErrorReschedules(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageQuestion)
	fi.digest = "a grounding digest"
	// backtranslate hits a transient endpoint failure.
	llm := newFakeLLM().script("generator", fakeResp{err: transientErr()})
	w := NewQuestionWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Nil(t, fi.it.FailedAt, "transient error must NOT terminally fail the item")
	assert.Equal(t, 0, s.fails[queue.StageQuestion])
	assert.Equal(t, 1, s.retries, "item rescheduled for retry")
	assert.Equal(t, 1, fi.it.RetryCount)
	require.NotNil(t, fi.it.NextAttemptAt, "next_attempt_at set to a future backoff")
	assert.True(t, fi.it.NextAttemptAt.After(s.now))
}

// TestRetry_ExhaustionFailsTerminally: once retry_count reaches MaxRetries a
// further transient error fails the item terminally (no infinite retry).
func TestRetry_ExhaustionFailsTerminally(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageQuestion)
	fi.digest = "a grounding digest"
	fi.it.RetryCount = 5 // already at DefaultRetryPolicy().MaxRetries
	llm := newFakeLLM().script("generator", fakeResp{err: transientErr()})
	w := NewQuestionWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.FailedAt, "retry exhausted -> terminal fail")
	assert.Equal(t, "question", fi.it.FailStage)
	assert.Equal(t, 0, s.retries, "no further reschedule after exhaustion")
}

// TestRetry_BadModelOutputReschedules: a FRESH model response that violates the
// stage's output contract (here: no JSON object at all) is stochastic -- a
// re-request usually parses -- so it is rescheduled under the same bounded
// backoff budget as a transport transient, NOT terminally failed on the first
// malformed reply.
func TestRetry_BadModelOutputReschedules(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageQuestion)
	fi.digest = "a grounding digest"
	llm := newFakeLLM().script("generator", ok("not json at all"))
	w := NewQuestionWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Nil(t, fi.it.FailedAt, "one malformed reply must NOT terminally fail the item")
	assert.Equal(t, 1, s.retries, "item rescheduled for a bounded re-request")
	assert.Equal(t, 1, fi.it.RetryCount)
	require.NotNil(t, fi.it.NextAttemptAt)
}

// TestRetry_BadModelOutputExhaustionFailsTerminally: the re-request budget is
// bounded -- a model that keeps returning malformed output exhausts MaxRetries
// and the item fails terminally (no infinite re-request loop).
func TestRetry_BadModelOutputExhaustionFailsTerminally(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageQuestion)
	fi.digest = "a grounding digest"
	fi.it.RetryCount = 5 // already at DefaultRetryPolicy().MaxRetries
	llm := newFakeLLM().script("generator", ok("not json at all"))
	w := NewQuestionWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.FailedAt, "re-request budget exhausted -> terminal fail")
	assert.Equal(t, "question", fi.it.FailStage)
	assert.Equal(t, 0, s.retries)
}

// --- read-boundary payload validation (todos #3) -----------------------------

// TestPayloadValidation_EmptyDigestRejected: the question worker refuses an empty
// comprehension digest (contract violation), failing the item terminally rather
// than calling the LLM with nothing.
func TestPayloadValidation_EmptyDigestRejected(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageQuestion)
	fi.digest = "   " // whitespace-only: an empty digest
	w := NewQuestionWorker(s, testPipeline(t, newFakeLLM()), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.FailedAt, "empty digest -> terminal contract failure")
	assert.Equal(t, "question", fi.it.FailStage)
	assert.Equal(t, 0, s.retries)
}

// TestPayloadValidation_IncompleteQuestionRejected: the answer worker refuses a
// question missing required fields (here final answer's user_request/context),
// so it never spends teacher quota on an empty question.
func TestPayloadValidation_IncompleteQuestionRejected(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageAnswer)
	incomplete := validQuestion()
	incomplete.UserRequest = "" // drop a required field
	fi.question = &incomplete
	v := verdictToTeacher()
	fi.verdict = &v
	w := NewAnswerWorker(s, testPipeline(t, scriptAnswerRoles(newFakeLLM())), openGate(), "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, res.Generated)
	assert.Equal(t, 0, res.TeacherCalls, "no teacher quota spent on an invalid question")
	require.NotNil(t, fi.it.FailedAt)
	assert.Equal(t, "answer", fi.it.FailStage)
}
