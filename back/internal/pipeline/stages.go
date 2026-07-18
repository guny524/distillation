package pipeline

import (
	"context"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/model"
)

// The queue stage workers (internal/worker, todos sec 2-5-3) drive the pipeline
// one stage at a time — Claim -> stage -> Complete/Fail/Supersede — instead of
// the monolithic Run. These exported methods are the per-stage entry points;
// the stage logic itself is unchanged (each just delegates to the existing
// private backtranslate/mutate/studentFilter/lane/verify implementation), so the
// decomposition reuses the pipeline rather than re-implementing it.

// Backtranslate reverse-generates a Question from an artifact (question worker).
func (p *Pipeline) Backtranslate(ctx context.Context, art artifact.Artifact) (Question, error) {
	return p.backtranslate(ctx, art)
}

// Mutate applies the enabled difficulty-mutation operators to a question
// (question worker). It is a no-op when no operator is enabled.
func (p *Pipeline) Mutate(ctx context.Context, q Question) (Question, error) {
	return p.mutate(ctx, q)
}

// Presolve runs the student pre-filter (presolve worker). toTeacher is true when
// the question has learning value (route it to the teacher); false means the
// student already solves it, and the presolve worker soft-deletes the item.
func (p *Pipeline) Presolve(ctx context.Context, q Question) (verdict model.StudentFilterVerdict, toTeacher bool, err error) {
	out, err := p.studentFilter(ctx, q)
	return out.Verdict, out.ToTeacher, err
}

// LaneRecords generates the ko/en teacher-trajectory records for a question
// (answer worker). Each record carries the trajectory plus cot/cot_raw/
// has_raw_cot when the teacher endpoint is reasoning-aware; calls is the paced
// teacher-call count (the pacing cost basis).
func (p *Pipeline) LaneRecords(ctx context.Context, q Question, verdict model.StudentFilterVerdict, pairID string) (records []model.DistillationPair, calls int, err error) {
	lr, err := p.lane(ctx, q, verdict, pairID)
	return lr.Records, lr.TeacherCalls, err
}

// VerifyRecord verifies one lane record (verify worker). See verify for the
// executor-vs-LLM routing; a "fail" result means the record must be dropped.
func (p *Pipeline) VerifyRecord(ctx context.Context, rec model.DistillationPair) (model.Verification, error) {
	return p.verify(ctx, rec)
}

// Kept reports whether a verified record survives (only an explicit fail drops
// it; pass/na are kept). It is the exported view of the internal kept predicate
// so the flush worker's projection applies the same rule.
func Kept(v model.Verification) bool { return kept(v) }
