package worker

import (
	"context"
	"io"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
)

// PresolveWorker is the mandatory pre-flush gate (todos sec 2-5-6): a local
// student tries to solve the question, and only questions it CANNOT already
// solve advance toward the paced teacher. Questions the student already solves
// carry zero learning value and are soft-deleted (superseded).
//
// presolved_at is required by the flush gate, so this stage must stamp it even
// when presolve is disabled -- then it is a trivial pass-through (Complete only),
// keeping items flowing while spending no student calls.
type PresolveWorker struct {
	store    Store
	pipe     *pipeline.Pipeline
	enabled  bool
	gate     Gate
	workerID string
	// retry governs transient-error rescheduling vs terminal failure.
	retry RetryPolicy
	// Log receives progress lines (stderr in production, nil/discard in tests).
	Log io.Writer
}

// NewPresolveWorker wires the worker. enabled=false makes every item a pass-through
// (Complete without running the student filter). The quota gate checks the judge
// role only -- the student is a LOCAL model with no quota (todos sec 6-3-4:
// "presolve -> judge (student는 local이라 확인 없음)").
func NewPresolveWorker(store Store, pipe *pipeline.Pipeline, enabled bool, gate Gate, workerID string) *PresolveWorker {
	return &PresolveWorker{store: store, pipe: pipe, enabled: enabled, gate: gate, workerID: workerID, retry: DefaultRetryPolicy()}
}

// RunOnce drains every item pending at the presolve stage under the judge quota
// gate.
func (w *PresolveWorker) RunOnce(ctx context.Context) (int, error) {
	return gatedDrain(ctx, w.store, w.gate, queue.StagePresolve, w.workerID, func(it queue.Item) (bool, error) {
		own := ownerOf(it)
		// Disabled: pass-through so presolved_at is set and the item still reaches
		// flush (todos: "presolve 비활성 config면 그냥 통과(Complete)만"). A verdict is
		// STILL recorded -- the answer stage reads presolve_verdict unconditionally
		// (a NULL there is a contract violation that would terminally fail every
		// item), and the dataset should say the student was never measured rather
		// than carry no filter provenance. method=disabled / verdict=uncertain is
		// exactly that statement (schema $defs/studentFilterVerdict).
		if !w.enabled {
			verdict := model.StudentFilterVerdict{
				Verdict: "uncertain",
				Method:  "disabled",
				Reason:  "presolve disabled by config; student capability not measured",
			}
			if err := w.store.SetPresolveVerdict(ctx, it.ID, verdict, own); err != nil {
				return false, skipIfLeaseLost(w.Log, queue.StagePresolve, it, err)
			}
			if err := w.store.Complete(ctx, it.ID, queue.StagePresolve, own); err != nil {
				return false, skipIfLeaseLost(w.Log, queue.StagePresolve, it, err)
			}
			logf(w.Log, "[presolve] item %d passed through (presolve disabled)", it.ID)
			return false, nil
		}

		q, err := w.store.GetQuestion(ctx, it.ID)
		if err != nil {
			return isRateLimited(err), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StagePresolve, err)
		}

		verdict, toTeacher, err := w.pipe.Presolve(ctx, q)
		if err != nil {
			return isRateLimited(err), handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StagePresolve, err)
		}
		// Record the verdict either way: for a superseded item it is the audit
		// trail of why it was dropped.
		if err := w.store.SetPresolveVerdict(ctx, it.ID, verdict, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StagePresolve, it, err)
		}

		if !toTeacher {
			// Student already solves it -> soft-delete (never reaches the teacher).
			if err := w.store.Supersede(ctx, it.ID, own); err != nil {
				return false, skipIfLeaseLost(w.Log, queue.StagePresolve, it, err)
			}
			logf(w.Log, "[presolve] item %d superseded (student solves it: %s)", it.ID, verdict.Verdict)
			return false, nil
		}

		if err := w.store.Complete(ctx, it.ID, queue.StagePresolve, own); err != nil {
			return false, skipIfLeaseLost(w.Log, queue.StagePresolve, it, err)
		}
		logf(w.Log, "[presolve] item %d advanced to teacher (verdict=%s)", it.ID, verdict.Verdict)
		return false, nil
	})
}
