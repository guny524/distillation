package worker

import (
	"context"
	"io"

	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
)

// QuestionWorker reverse-generates a question from a comprehended item (todos
// sec 2-5-3). It reuses the pipeline's backtranslate + difficulty-mutate stages
// (the inline monolithic calls, moved onto the queue), reconstructing the
// artifact from the item's provenance and the comprehension digest.
type QuestionWorker struct {
	store    Store
	pipe     *pipeline.Pipeline
	gate     Gate
	workerID string
	// retry governs transient-error rescheduling vs terminal failure.
	retry RetryPolicy
	// Log receives progress lines (stderr in production, nil/discard in tests).
	Log io.Writer
}

// NewQuestionWorker wires the worker with its queue store, the shared pipeline, the
// quota gate (checks the generator role -- todos sec 6-3-4), and the lock owner id.
func NewQuestionWorker(store Store, pipe *pipeline.Pipeline, gate Gate, workerID string) *QuestionWorker {
	return &QuestionWorker{store: store, pipe: pipe, gate: gate, workerID: workerID, retry: DefaultRetryPolicy()}
}

// RunOnce drains every item pending at the question stage under the generator quota
// gate: read the digest, backtranslate + mutate into a Question, persist it. A
// transient generation failure reschedules the item; a contract/parse failure fails
// it terminally; a 429 arms the cooldown and stops the pass.
func (w *QuestionWorker) RunOnce(ctx context.Context) (int, error) {
	return gatedDrain(ctx, w.store, w.gate, queue.StageQuestion, w.workerID, func(it queue.Item) (bool, error) {
		own := ownerOf(it)
		digest, err := w.store.GetComprehension(ctx, it.ID)
		if err != nil {
			return isRateLimited(err), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageQuestion, err)
		}
		art := itemArtifact(it, digest)

		q, err := w.pipe.Backtranslate(ctx, art)
		if err != nil {
			return isRateLimited(err), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageQuestion, err)
		}
		q, err = w.pipe.Mutate(ctx, q)
		if err != nil {
			return isRateLimited(err), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageQuestion, err)
		}

		if err := w.store.SetQuestion(ctx, it.ID, q, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StageQuestion, it, err)
		}
		if err := w.store.Complete(ctx, it.ID, queue.StageQuestion, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StageQuestion, it, err)
		}
		logf(w.Log, "[question] item %d (%s/%s) questioned (lang=%s)", it.ID, it.SourceType, it.DocID, q.Lang)
		return false, nil
	})
}
