package worker

import (
	"context"
	"io"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
)

// VerifyWorker verifies the answered trajectory of each item (todos sec 2-5-3:
// an always-on worker). It runs the pipeline verify stage (real code execution
// when enabled, else rule/none) on every lane record. Verifications are stored
// aligned by index with the answer payload so the flush worker keeps only the
// passing lanes. If NO lane survives, the item is superseded (todos: "verify
// fail이면 superseded_at"); if at least one survives, the item completes and the
// flush stage projects the survivors.
type VerifyWorker struct {
	store    Store
	pipe     *pipeline.Pipeline
	gate     Gate
	workerID string
	// retry governs transient-error rescheduling vs terminal failure.
	retry RetryPolicy
	// Log receives progress lines (stderr in production, nil/discard in tests).
	Log io.Writer
}

// NewVerifyWorker wires the worker with its queue store, pipeline, the quota gate
// (checks the verifier role -- todos sec 6-3-4), and the lock owner id.
func NewVerifyWorker(store Store, pipe *pipeline.Pipeline, gate Gate, workerID string) *VerifyWorker {
	return &VerifyWorker{store: store, pipe: pipe, gate: gate, workerID: workerID, retry: DefaultRetryPolicy()}
}

// RunOnce drains every item pending at the verify stage under the verifier quota
// gate. The answer payload read is contract-validated (non-empty lane records), so
// a missing/empty payload is a terminal contract error; a transient verification
// failure reschedules; a 429 arms the cooldown and stops the pass.
func (w *VerifyWorker) RunOnce(ctx context.Context) (int, error) {
	return gatedDrain(ctx, w.store, w.gate, queue.StageVerify, w.workerID, func(it queue.Item) (bool, error) {
		own := ownerOf(it)
		records, err := w.store.GetAnswerPayload(ctx, it.ID)
		if err != nil {
			return isRateLimited(err), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageVerify, err)
		}

		verifications := make([]model.Verification, 0, len(records))
		anyKept := false
		for i := range records {
			v, verr := w.pipe.VerifyRecord(ctx, records[i])
			if verr != nil {
				return isRateLimited(verr), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageVerify, verr)
			}
			verifications = append(verifications, v)
			if pipeline.Kept(v) {
				anyKept = true
			}
		}
		if err := w.store.SetVerification(ctx, it.ID, verifications, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StageVerify, it, err)
		}

		if !anyKept {
			// Every lane failed verification -> discard the item (todos sec 2-5-6:
			// soft-delete before flush; a superseded item never reaches parquet).
			if err := w.store.Supersede(ctx, it.ID, own); err != nil {
				return false, skipIfLeaseLost(w.Log, queue.StageVerify, it, err)
			}
			logf(w.Log, "[verify] item %d superseded (all %d lane(s) failed)", it.ID, len(records))
			return false, nil
		}

		if err := w.store.Complete(ctx, it.ID, queue.StageVerify, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StageVerify, it, err)
		}
		logf(w.Log, "[verify] item %d verified (%d lane(s), at least one kept)", it.ID, len(records))
		return false, nil
	})
}
