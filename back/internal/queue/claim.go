package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Why these statements are NOT sqlc-generated (unlike Enqueue/GetCursor/SaveCursor
// in enqueue.go and the loader queries): claim and complete assemble the stage's
// gate/done column names (stageMeta.prev / stageMeta.doneCol) into the SQL at
// runtime via fmt.Sprintf. sqlc generates ONE fixed statement per query from a
// static string; it cannot express "the WHERE/SET column depends on which of the
// six stages is running". The column names come from the fixed internal stages
// table (never caller input), so the interpolation is injection-safe and every
// runtime VALUE stays parameterized. fail/retry/supersede are static and could in
// principle be sqlc-generated, but they are kept here with claim/complete as one
// owner-fenced transition family (shared ErrLeaseLost/fencedWrite contract) rather
// than split across two mechanisms.
//
// claimSQL builds the atomic claim statement for a stage. It is a single
// UPDATE whose target is chosen by a FOR UPDATE SKIP LOCKED subquery, which is
// the canonical PostgreSQL job-queue pattern and satisfies todos sec 2-5-2's
// "briefly lock the narrowed, unlocked candidate then set owner/at, commit
// immediately (short transaction)": a single statement auto-commits, so it IS
// the shortest possible transaction. FOR UPDATE SKIP LOCKED makes parallel
// workers pick different rows (no overlap); the stale predicate lets a worker
// reclaim a crashed worker's lock.
//
// Claimable = every prev column IS NOT NULL (previous stages done) AND doneCol
// IS NULL (this stage pending) AND not superseded AND not failed AND
// (unlocked OR lock is stale). Column names come from stageMeta (fixed table),
// so interpolation is injection-safe; $1 workerID and $2 stale-seconds are
// parameterized.
func claimSQL(m stageMeta) string {
	preds := make([]string, 0, len(m.prev)+3)
	for _, c := range m.prev {
		preds = append(preds, c+" IS NOT NULL")
	}
	preds = append(preds, m.doneCol+" IS NULL", "superseded_at IS NULL", "failed_at IS NULL")
	// Transient-retry gate: a rescheduled item (next_attempt_at in the future) is
	// invisible until its backoff elapses; never-retried items (NULL) are eligible.
	preds = append(preds, "(next_attempt_at IS NULL OR next_attempt_at <= now())")
	where := strings.Join(preds, " AND ")

	return fmt.Sprintf(`
UPDATE distillation_items
SET lock_owner_id = $1, lock_at = now()
WHERE id = (
    SELECT id FROM distillation_items
    WHERE %s
      AND (lock_owner_id IS NULL OR lock_at < now() - make_interval(secs => $2))
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING %s`, where, strings.Join(itemColumns, ", "))
}

// Claim atomically grabs the next item pending at stage for workerID and stamps
// the lock. Returns ok=false (no error) when nothing is claimable. The returned
// Item carries the freshly-stamped lock_owner_id/lock_at.
//
// The long stage work (comprehend/answer/...) runs OUTSIDE this call, in the
// caller, per todos sec 2-5-2. When done the caller calls Complete (or Fail).
func (s *Store) Claim(ctx context.Context, stage Stage, workerID string) (Item, bool, error) {
	m, err := meta(stage)
	if err != nil {
		return Item{}, false, err
	}
	row := s.q.QueryRow(ctx, claimSQL(m), workerID, s.stale.Seconds())
	item, err := scanItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Item{}, false, nil
	}
	if err != nil {
		return Item{}, false, fmt.Errorf("queue: claim %s: %w", stage, err)
	}
	return item, true, nil
}

// completeSQL sets the stage's timestamp to now(), releases the lock, and clears
// ALL retry bookkeeping. The owner fence ($2) makes it a no-op for a worker whose
// lease was reclaimed (see Complete).
//
// retry_count and last_error are reset to zero here, not just next_attempt_at:
// retry_count is a single item-global column shared by every stage, so a transient
// retry spent at (say) the comprehend stage would otherwise carry its count into
// the answer stage and let worker.RetryPolicy.MaxRetries fail a later stage early
// on its FIRST real transient error. Resetting on each stage's Complete makes the
// retry budget per-stage (each stage starts with a full budget), which is the
// intended semantics of a per-stage transient allowance.
func completeSQL(m stageMeta) string {
	return fmt.Sprintf(
		`UPDATE distillation_items
SET %s = now(), lock_owner_id = NULL, lock_at = NULL,
    next_attempt_at = NULL, retry_count = 0, last_error = ''
WHERE id = $1 AND lock_owner_id = $2`,
		m.doneCol,
	)
}

// Complete marks stage done for itemID (its timestamp = now()) and releases the
// lock (todos sec 2-5-2). It is FENCED on owner: the UPDATE only touches the row
// while lock_owner_id still equals the owner that claimed it. A worker whose
// lease was reclaimed as stale therefore affects 0 rows and gets ErrLeaseLost,
// so its late completion can never overwrite the new owner's result.
func (s *Store) Complete(ctx context.Context, itemID int64, stage Stage, owner string) error {
	m, err := meta(stage)
	if err != nil {
		return err
	}
	return s.fencedWrite(ctx, completeSQL(m), fmt.Sprintf("complete %s", stage), itemID, owner)
}

// failSQL records the failure stage/reason and releases the lock. fail_stage /
// fail_reason are parameterized ($2, $3); the owner fence ($4) refuses a
// reclaimed worker's write.
const failSQL = `UPDATE distillation_items
SET failed_at = now(), fail_stage = $2, fail_reason = $3, lock_owner_id = NULL, lock_at = NULL
WHERE id = $1 AND lock_owner_id = $4`

// Fail terminally marks itemID failed at stage with reason and releases the lock.
// failed_at takes the item out of every claim candidate set (todos sec 2-5-1). It
// is FENCED on owner (ErrLeaseLost on 0 rows) so a reclaimed worker never fails
// an item the new owner may still be succeeding at. Fail is for NON-transient
// (contract/validation) errors and for retry exhaustion; a transient error uses
// Retry instead.
func (s *Store) Fail(ctx context.Context, itemID int64, stage Stage, reason, owner string) error {
	if _, err := meta(stage); err != nil {
		return err
	}
	tag, err := s.q.Exec(ctx, failSQL, itemID, string(stage), reason, owner)
	if err != nil {
		return fmt.Errorf("queue: fail %s item %d: %w", stage, itemID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("queue: fail %s item %d: %w", stage, itemID, ErrLeaseLost)
	}
	return nil
}

// retrySQL reschedules itemID for a later attempt: bump retry_count, set
// next_attempt_at to a backoff in the future, record last_error, and release the
// lock WITHOUT setting failed_at. Owner-fenced ($5).
const retrySQL = `UPDATE distillation_items
SET retry_count = retry_count + 1,
    next_attempt_at = now() + make_interval(secs => $2),
    last_error = $3,
    lock_owner_id = NULL, lock_at = NULL
WHERE id = $1 AND lock_owner_id = $4`

// Retry records a transient (network/429/5xx/timeout) failure by rescheduling the
// item for another attempt after backoff seconds, instead of terminally failing
// it. The lock is released and next_attempt_at is set in the future, so the item
// is invisible to claims until the backoff elapses and then becomes claimable
// again. Owner-fenced: a reclaimed worker's Retry is a no-op (ErrLeaseLost).
func (s *Store) Retry(ctx context.Context, itemID int64, backoffSeconds float64, lastErr, owner string) error {
	tag, err := s.q.Exec(ctx, retrySQL, itemID, backoffSeconds, lastErr, owner)
	if err != nil {
		return fmt.Errorf("queue: retry item %d: %w", itemID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("queue: retry item %d: %w", itemID, ErrLeaseLost)
	}
	return nil
}

// supersedeSQL soft-deletes a pre-projection item and releases the lock. The
// projected_at IS NULL guard enforces "soft-delete only before the item's lanes
// reach the append-only buffer" (todos sec 2-5-6): once projected, the lanes are
// on their way to the immutable parquet snapshot and must never be retro-dropped.
// Owner-fenced ($2).
const supersedeSQL = `UPDATE distillation_items
SET superseded_at = now(), lock_owner_id = NULL, lock_at = NULL
WHERE id = $1 AND lock_owner_id = $2 AND projected_at IS NULL`

// Supersede soft-deletes itemID (superseded_at = now()) and releases the lock.
// It is the terminal transition for an item that must not be flushed: the
// presolve worker uses it when the student already solves the question (zero
// learning value), and the verify worker uses it when every lane fails
// verification. superseded_at IS NULL is part of every claim/flush predicate, so
// a superseded item is never re-claimed and never projected. It is owner-fenced
// and refuses an already-projected item (0 rows -> ErrLeaseLost): a reclaimed
// worker cannot supersede, and the append-only buffer is protected.
func (s *Store) Supersede(ctx context.Context, itemID int64, owner string) error {
	return s.fencedWrite(ctx, supersedeSQL, "supersede", itemID, owner)
}

// fencedWrite runs an owner-fenced single-row UPDATE (WHERE id=$1 AND
// lock_owner_id=$2) and maps a 0-row result to ErrLeaseLost. Used by the
// transitions whose SQL is exactly (itemID, owner).
func (s *Store) fencedWrite(ctx context.Context, sql, op string, itemID int64, owner string) error {
	tag, err := s.q.Exec(ctx, sql, itemID, owner)
	if err != nil {
		return fmt.Errorf("queue: %s item %d: %w", op, itemID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("queue: %s item %d: %w", op, itemID, ErrLeaseLost)
	}
	return nil
}
