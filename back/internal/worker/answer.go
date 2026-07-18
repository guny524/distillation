package worker

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/teacher"
)

// AnswerWorker generates the paced teacher trajectory for presolved items (todos
// sec 2-5-3: an always-on worker). It is the ONLY stage that spends frontier
// teacher quota, so before EVERY claim it consults the quota Gate (todos sec
// 6-3-4): the gate reads the teacher+translator subscription /quota facts and,
// when either is spent (or /quota is unreachable, or a 429 cooldown is active),
// closes so the worker claims nothing more this pass. Each claimed item yields the
// ko/en trajectory (with cot/cot_raw/has_raw_cot) via the pipeline lane stage.
type AnswerWorker struct {
	store    Store
	pipe     *pipeline.Pipeline
	gate     Gate
	workerID string
	// retry governs transient-error rescheduling vs terminal failure.
	retry RetryPolicy
	// Log receives progress lines (stderr in production, nil/discard in tests).
	Log io.Writer
}

// NewAnswerWorker wires the worker with its queue store, pipeline, quota gate, and
// lock owner id.
func NewAnswerWorker(store Store, pipe *pipeline.Pipeline, gate Gate, workerID string) *AnswerWorker {
	return &AnswerWorker{store: store, pipe: pipe, gate: gate, workerID: workerID, retry: DefaultRetryPolicy()}
}

// AnswerResult summarizes one answer pass.
type AnswerResult struct {
	Generated    int  // items whose trajectory was produced and stored
	Spent        int  // items that made >=1 teacher call
	TeacherCalls int  // raw teacher-role calls made (audit/logging)
	RateLimited  bool // a teacher 429 was observed this pass (armed the cooldown)
}

// RunOnce loops "gate open? -> claim -> answer" (todos sec 6-3-4). It re-checks the
// quota gate before EVERY claim, so the moment /quota shows the teacher/translator
// subscription spent -- or a 429 arms the cooldown -- the worker stops claiming;
// overshoot past exhaustion is bounded by the items already in flight. A gate closed
// at the top claims nothing at all (todos: "quota 소진이면 claim 안 함"). A 429
// mid-pass reschedules that item (transient retry, inside answerOne), arms the
// process-local cooldown, and stops the pass -- further claims would just 429 again.
func (w *AnswerWorker) RunOnce(ctx context.Context) (AnswerResult, error) {
	var res AnswerResult
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if !w.gate.Allow(ctx) {
			// Quota gate closed for this window: end the pass silently (a --loop
			// worker polls every few seconds; per-pass logging here would spam).
			// The gate state is observable via subgate GET /quota.
			return res, nil
		}
		item, ok, err := w.store.Claim(ctx, queue.StageAnswer, w.workerID)
		if err != nil {
			return res, fmt.Errorf("claim answer: %w", err)
		}
		if !ok {
			break // no more presolved items pending
		}

		calls, produced, rateLimited, oneErr := w.answerOne(ctx, item)
		res.TeacherCalls += calls
		if calls > 0 { // this item spent (or attempted to spend) quota
			res.Spent++
		}
		if produced {
			res.Generated++
		}
		if rateLimited {
			// The teacher/translator endpoint 429'd. answerOne already rescheduled the
			// item (transient retry); arm the process-local cooldown so subsequent
			// passes skip while it is in effect, and stop this pass -- the only
			// backpressure a cli transport exposes (todos sec 6-3-4).
			res.RateLimited = true
			w.gate.NoteRateLimited()
			logf(w.Log, "[answer] rate limited; armed cooldown, stopping pass")
			return res, nil
		}
		if oneErr != nil {
			return res, oneErr
		}
	}
	// Only summarize a pass that actually did work; an idle drain (queue empty,
	// nothing claimed) stays silent instead of spamming generated=0 every poll.
	// Per-item content is logged in answerOne when a lane is produced.
	if res.Generated > 0 || res.Spent > 0 {
		logf(w.Log, "[answer] done: generated=%d spent=%d calls=%d", res.Generated, res.Spent, res.TeacherCalls)
	}
	return res, nil
}

// answerOne produces and stores one item's trajectory. calls is the number of
// teacher calls made (returned even on failure for the pass summary). A per-item
// stage failure is retried or terminally failed in place (err=nil); err is non-nil
// only for an infrastructure failure (a non-lease-lost DB error) so RunOnce aborts.
// rateLimited reports an observed 429 so RunOnce arms the cooldown and stops.
func (w *AnswerWorker) answerOne(ctx context.Context, it queue.Item) (calls int, produced bool, rateLimited bool, err error) {
	own := ownerOf(it)
	q, err := w.store.GetQuestion(ctx, it.ID)
	if err != nil {
		return 0, false, false, handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageAnswer, err)
	}
	verdict, err := w.store.GetPresolveVerdict(ctx, it.ID)
	if err != nil {
		return 0, false, false, handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageAnswer, err)
	}

	pairID := fmt.Sprintf("distill-item-%d", it.ID)
	records, calls, err := w.pipe.LaneRecords(ctx, q, verdict, pairID)
	if err != nil {
		// calls may be >0: a teacher call already spent quota before the parse
		// failure. The stage error is retried (transient) or terminally failed. A 429
		// (teacher.ErrRateLimited, transient) is additionally surfaced via rateLimited
		// so RunOnce arms the cooldown and stops the pass (todos sec 6-3-4).
		rl := errors.Is(err, teacher.ErrRateLimited)
		return calls, false, rl, handleStageError(ctx, w.store, w.retry, w.Log, it, queue.StageAnswer, err)
	}
	if err := w.store.SetAnswerPayload(ctx, it.ID, records, own); err != nil {
		return calls, false, false, skipIfLeaseLost(w.Log, queue.StageAnswer, it, err)
	}
	if err := w.store.Complete(ctx, it.ID, queue.StageAnswer, own); err != nil {
		return calls, false, false, skipIfLeaseLost(w.Log, queue.StageAnswer, it, err)
	}
	logf(w.Log, "[answer] item %d (%s/%s) answered (%d lane record(s), %d call(s))",
		it.ID, it.SourceType, it.DocID, len(records), calls)
	return calls, true, false, nil
}
