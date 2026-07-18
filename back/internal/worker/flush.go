package worker

import (
	"context"
	"fmt"
	"io"

	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
)

// BufferFlusher moves the distillation_pairs buffer to the append-only export
// snapshot. *flush.Flusher satisfies it; tests inject a fake.
type BufferFlusher interface {
	MaybeFlush(ctx context.Context) (flushed bool, rows int, err error)
}

// FlushWorker is the periodic export stage (todos sec 2-5-3/2-5-6). It runs the
// flush GATE via the queue (Claim(StageFlush) selects exactly items where
// presolved_at AND verified_at AND superseded_at IS NULL AND projected_at IS
// NULL), projects each item's surviving lanes into the distillation_pairs buffer
// AND marks it projected atomically (Store.ProjectAndComplete, one transaction),
// then hands the buffer to the existing DB-buffer -> export volume flush cycle. The item's
// terminal mark is projected_at (in the buffer); whether its rows reached a
// parquet batch is owned by the buffer -> export cycle and not tracked per item,
// and that cycle only fires at/over threshold, so buffered-but-not-yet-exported
// rows are expected, not a leak. Once projected, an item is immutable
// (append-only, no retro supersede).
type FlushWorker struct {
	store    Store
	flusher  BufferFlusher
	workerID string
	// retry governs transient-error rescheduling vs terminal failure.
	retry RetryPolicy
	// Log receives progress lines (stderr in production, nil/discard in tests).
	Log io.Writer
}

// NewFlushWorker wires the worker. The store projects items into
// distillation_pairs atomically; flusher runs the buffer -> parquet cycle after.
func NewFlushWorker(store Store, flusher BufferFlusher, workerID string) *FlushWorker {
	return &FlushWorker{store: store, flusher: flusher, workerID: workerID, retry: DefaultRetryPolicy()}
}

// FlushResult summarizes one flush pass.
type FlushResult struct {
	ItemsProjected int  // queue items drained into the buffer
	RecordsWritten int  // lane records inserted into distillation_pairs
	Flushed        bool // whether the buffer -> export volume cycle ran
	RowsFlushed    int  // rows exported to parquet by that cycle
}

// RunOnce drains the flush-gated items into the buffer, then flushes the buffer
// to the export volume. The two steps are separate concerns: draining fills the
// buffer (marking items projected_at); the buffer -> parquet cycle owns its own
// export-volume-lock atomicity and only fires at/over its threshold.
func (w *FlushWorker) RunOnce(ctx context.Context) (FlushResult, error) {
	var res FlushResult
	projected, err := drain(ctx, w.store, queue.StageFlush, w.workerID, func(it queue.Item) error {
		written, perr := w.projectItem(ctx, it)
		res.RecordsWritten += written
		return perr
	})
	res.ItemsProjected = projected
	if err != nil {
		return res, err
	}

	flushed, rows, ferr := w.flusher.MaybeFlush(ctx)
	if ferr != nil {
		// The projected rows are durable in the DB buffer (no data loss), but the
		// export DID fail -- surface it so the run exits non-zero and the operator
		// sees a failing CronJob instead of a green run with a silently growing
		// buffer (full volume, unmounted export dir).
		return res, fmt.Errorf("buffer flush (items already projected durably): %w", ferr)
	}
	res.Flushed, res.RowsFlushed = flushed, rows
	logf(w.Log, "[flush] done: items=%d records=%d flushed=%v rows=%d",
		res.ItemsProjected, res.RecordsWritten, res.Flushed, res.RowsFlushed)
	return res, nil
}

// projectItem inserts an item's surviving lanes into distillation_pairs AND marks
// the item projected, atomically (Store.ProjectAndComplete, one transaction fenced
// on owner -- todos #2): a partial projection can never coexist with a "done"
// mark, and a reclaimed worker's projection rolls back entirely. Survivors = lanes
// whose verification is not "fail" (the same rule the monolithic pipeline
// applies). A malformed payload is a terminal contract error for this item.
func (w *FlushWorker) projectItem(ctx context.Context, it queue.Item) (int, error) {
	own := ownerOf(it)
	records, err := w.store.GetAnswerPayload(ctx, it.ID)
	if err != nil {
		return 0, handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageFlush, err)
	}
	verifications, err := w.store.GetVerification(ctx, it.ID)
	if err != nil {
		return 0, handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageFlush, err)
	}
	if len(verifications) != len(records) {
		return 0, handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageFlush,
			fmt.Errorf("verification count %d != answer record count %d", len(verifications), len(records)))
	}

	rows := make([]map[string]any, 0, len(records))
	for i := range records {
		v := verifications[i]
		if !pipeline.Kept(v) {
			continue
		}
		rec := records[i]
		rec.Verification = &v
		params, perr := pipeline.ToParams(rec)
		if perr != nil {
			return 0, handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageFlush,
				fmt.Errorf("project %s: %w", rec.TaskID, perr))
		}
		rows = append(rows, params)
	}

	inserted, err := w.store.ProjectAndComplete(ctx, it.ID, own, rows)
	if err != nil {
		return 0, skipIfLeaseLost(w.Log, queue.StageFlush, it, err)
	}
	logf(w.Log, "[flush] item %d projected (%d/%d lane(s) kept)", it.ID, inserted, len(records))
	return inserted, nil
}
