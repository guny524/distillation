// Package queue implements the DB-backed state machine that is the foundation
// for the distillation pipeline's stage workers (todos sec 2-5).
//
// The design (todos sec 2-5-1): one row in distillation_items is one work item;
// its state is which nullable timestamp columns are filled, in order:
//
//	queued_at -> comprehended_at -> questioned_at -> presolved_at ->
//	answered_at -> verified_at -> projected_at
//
// A worker CLAIMS an item for a stage when the previous stage's timestamp is
// non-null and this stage's timestamp is still null, using a single reusable
// lock (lock_owner_id, lock_at) taken with FOR UPDATE SKIP LOCKED so parallel
// workers never grab the same row (todos sec 2-5-2). A stale lock (old lock_at)
// is reclaimable by another worker after a crash.
//
// This package deliberately does NOT decompose the stage workers themselves
// (comprehend/question/presolve/answer/verify/flush) — that is the next step.
// It provides only the queue table, the claim/complete/fail transitions, source
// ingest idempotency (UNIQUE(source_type, doc_id) + per-source cursor), and a
// Migrate entry point (whose DDL now lives in internal/db). The stage payloads (comprehension text, generated question,
// answer trajectory) are owned by the worker step: this layer carries the work
// item's identity, provenance, state, and lock only.
//
// Pure logic (the claim/complete/fail SQL builders and the stage->column
// mapping) is separated from the DB surface (db.Querier) so it is unit-testable
// with a mock querier and no PostgreSQL, mirroring internal/pacing.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/guny524/distillation/internal/db"
)

// DefaultStaleLock is how long a lock_at may sit before another worker may
// reclaim the item (crash recovery). Used when NewStore is given stale <= 0, and
// overridable per deployment (config, see cmd/distill worker wiring).
//
// It MUST exceed a stage's worst-case processing time (todos sec 2-5-2 (a)):
// otherwise a live worker doing legitimately slow work is wrongly reclaimed as
// stale, and two workers redo the same item (wasted frontier quota). The bound
// is roughly max_teacher_calls_per_stage x request_timeout_seconds x
// (1 + max_retries). With the shipped config (300s timeout, 3 retries -> 20 min
// per fully-timed-out call) the slowest stage (an answer pass: translate + two
// teacher lanes ~ 3 calls; or a question pass: backtranslate + mutate) can, in
// the pathological all-timeout case, run ~60 min, so the default is set well
// above that. Fencing (lock_owner_id on every write) is the correctness backstop
// if the threshold is ever misconfigured too low: a reclaimed worker's late
// write is refused (ErrLeaseLost), so a too-short lease wastes work but never
// corrupts data. A per-item lock_at heartbeat (which would let this stay tight)
// is deferred: it needs a dedicated DB connection, as pgx.Conn is not safe for
// concurrent use by a background goroutine and the worker's single connection.
const DefaultStaleLock = 90 * time.Minute

// ErrLeaseLost is returned by a fenced write (Complete/Fail/Supersede/Retry/
// Heartbeat and the payload writes) when the row's lock_owner_id no longer
// matches the caller's owner: the lease was reclaimed as stale by another worker,
// which is now the authority for this item. The caller MUST stop processing the
// item and must not write anything further for it (no duplicate/overwriting
// write), but this is NOT an infrastructure failure -- the queue is healthy and
// draining should continue with the next item.
var ErrLeaseLost = errors.New("queue: lease lost (item reclaimed by another worker)")

// Stage names one step of the state machine. Each stage maps (via stages) to
// the timestamp column it fills and the previous column(s) that gate it.
type Stage string

const (
	StageComprehend Stage = "comprehend"
	StageQuestion   Stage = "question"
	StagePresolve   Stage = "presolve"
	StageAnswer     Stage = "answer"
	StageVerify     Stage = "verify"
	StageFlush      Stage = "flush"
)

// stageMeta gates a stage: every prev column must be IS NOT NULL (previous
// stages done) and doneCol must be IS NULL (this stage not done). Column names
// come from this fixed internal table, NEVER from caller input, so building SQL
// by interpolating them is injection-safe; all runtime VALUES are parameterized.
type stageMeta struct {
	prev    []string // all must be IS NOT NULL for the item to be claimable
	doneCol string   // set to now() by Complete; must be IS NULL to claim
}

// stages is the single source of truth for stage -> timestamp column mapping
// (todos sec 2-5-1). The linear order is queued -> comprehended -> questioned
// -> presolved -> answered -> verified -> projected. StageFlush additionally
// requires presolved_at (the mandatory presolve gate, todos sec 2-5-6), which
// the linear order already implies but the flush gate states explicitly.
var stages = map[Stage]stageMeta{
	StageComprehend: {prev: []string{"queued_at"}, doneCol: "comprehended_at"},
	StageQuestion:   {prev: []string{"comprehended_at"}, doneCol: "questioned_at"},
	StagePresolve:   {prev: []string{"questioned_at"}, doneCol: "presolved_at"},
	StageAnswer:     {prev: []string{"presolved_at"}, doneCol: "answered_at"},
	StageVerify:     {prev: []string{"answered_at"}, doneCol: "verified_at"},
	// The flush stage's "done" column is projected_at: the item's surviving
	// lanes are inserted into the distillation_pairs buffer and that is the
	// item's TERMINAL mark (todos sec 2-5-6). Whether those buffer rows have
	// reached a parquet batch is owned entirely by the buffer -> export cycle
	// (internal/flush) and is not tracked per item. Gating on projected_at IS
	// NULL is what stops an item being re-projected (duplicate rows) after a
	// restart.
	StageFlush: {prev: []string{"presolved_at", "verified_at"}, doneCol: "projected_at"},
}

// meta looks up a stage, returning a clear error for an unknown one.
func meta(stage Stage) (stageMeta, error) {
	m, ok := stages[stage]
	if !ok {
		return stageMeta{}, fmt.Errorf("queue: unknown stage %q", stage)
	}
	return m, nil
}

// itemColumns is the SELECT/RETURNING column order. scanItem depends on it, so
// keep the two in lockstep.
var itemColumns = []string{
	"id", "source_type", "doc_id", "url", "license", "title", "locator", "retrieved_at",
	"queued_at", "comprehended_at", "questioned_at", "presolved_at", "answered_at",
	"verified_at", "superseded_at", "failed_at", "fail_stage", "fail_reason",
	"lock_owner_id", "lock_at",
	"source_excerpt", "projected_at", "retry_count", "next_attempt_at", "last_error",
}

// Item is one row of distillation_items. Nullable timestamp/lock columns are
// pointer-typed; a non-nil pointer means the stage/lock is set.
type Item struct {
	ID          int64
	SourceType  string
	DocID       string
	URL         string
	License     string
	Title       string
	Locator     string
	RetrievedAt *time.Time

	QueuedAt       *time.Time
	ComprehendedAt *time.Time
	QuestionedAt   *time.Time
	PresolvedAt    *time.Time
	AnsweredAt     *time.Time
	VerifiedAt     *time.Time
	SupersededAt   *time.Time
	FailedAt       *time.Time
	FailStage      string
	FailReason     string

	LockOwnerID *string
	LockAt      *time.Time

	// SourceExcerpt is the short ingest excerpt (artifact.Chunk text) preserved
	// from Enqueue so comprehend grounds the digest in real source content, not
	// title/URL alone (todos sec 2-5-4 / 2-5-5).
	SourceExcerpt string
	// ProjectedAt is set when the item's surviving lanes are inserted into the
	// distillation_pairs buffer -- the item's terminal mark. Whether those rows
	// reached a parquet batch is owned by the buffer -> export cycle
	// (internal/flush) and not tracked per item.
	ProjectedAt *time.Time
	// RetryCount / NextAttemptAt / LastError implement transient-error retry: a
	// network/429/5xx/timeout failure reschedules the item (lock released,
	// next_attempt_at in the future) instead of terminally failing it. A claim
	// only considers items whose next_attempt_at has elapsed.
	RetryCount    int
	NextAttemptAt *time.Time
	LastError     string
}

// scanItem scans a row whose columns are itemColumns, in order.
func scanItem(row pgx.Row) (Item, error) {
	var it Item
	err := row.Scan(
		&it.ID, &it.SourceType, &it.DocID, &it.URL, &it.License, &it.Title, &it.Locator, &it.RetrievedAt,
		&it.QueuedAt, &it.ComprehendedAt, &it.QuestionedAt, &it.PresolvedAt, &it.AnsweredAt,
		&it.VerifiedAt, &it.SupersededAt, &it.FailedAt, &it.FailStage, &it.FailReason,
		&it.LockOwnerID, &it.LockAt,
		&it.SourceExcerpt, &it.ProjectedAt, &it.RetryCount, &it.NextAttemptAt, &it.LastError,
	)
	return it, err
}

// Store runs queue operations against one db.Querier (a *pgx.Conn in
// production). stale is the lock reclaim threshold; workerID is passed per call
// by the worker (todos sec 2-5-2: POD_NAME/hostname/UUID).
type Store struct {
	q     db.Querier
	stale time.Duration
}

// NewStore wraps a db.Querier. A non-positive stale falls back to
// DefaultStaleLock so a misconfigured worker never reclaims locks instantly.
func NewStore(q db.Querier, stale time.Duration) *Store {
	if stale <= 0 {
		stale = DefaultStaleLock
	}
	return &Store{q: q, stale: stale}
}

// Migrate applies the queue DDL idempotently (distillation_items +
// distillation_source_cursors + partial indexes, then the fencing/retry/
// projected-at extension). Thin delegate to db.MigrateQueue: the migration SQL
// and its apply order now live in internal/db (single physical source shared
// with sqlc), so this wrapper only preserves the queue package's public Migrate
// signature for its callers (queue_test; workers migrate via db.Migrate).
func Migrate(ctx context.Context, q db.Querier) error {
	return db.MigrateQueue(ctx, q)
}
