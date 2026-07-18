// Package worker decomposes the monolithic distill run into DB-queue stage
// workers (todos sec 2-5). Each worker follows one loop -- Claim a pending item
// for its stage, do the stage work, then Complete / Fail / Supersede -- over the
// queue state machine (internal/queue). The stage logic itself is reused from
// internal/pipeline (backtranslate/mutate/presolve/lane/verify): the workers
// only move those calls out of the inline monolithic Run and onto the queue.
//
// Layering: internal/queue owns the item identity/state/lock; this package owns
// the per-stage payload columns (added by MigratePayloads) and the orchestration
// of pipeline / pacing / flush around the queue. The Store interface unifies the
// queue transitions and the payload I/O behind one seam so every worker is unit
// testable with an in-memory fake and no PostgreSQL.
package worker

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/loader"
	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
)

// Store is the persistence surface the stage workers need: the queue state
// machine (Claim/Complete/Fail/Supersede/Retry/Heartbeat, from internal/queue)
// plus the per-stage payload columns. *PGStore implements it over a *queue.Store
// and a db.Querier; tests use an in-memory fake. Splitting nothing off keeps
// every worker's dependency a single injected value.
//
// Every WRITE that a worker performs after claiming an item -- the terminal
// transitions AND the payload writes -- takes the owner it claimed with and is
// fenced on lock_owner_id: a worker whose lease was reclaimed as stale gets
// queue.ErrLeaseLost and must stop, so a late write can never overwrite the new
// owner's result (todos sec 2-5-2 fencing). Reads are unfenced (a stale read is
// harmless), but they validate the payload contract so a worker never proceeds
// on an empty/incomplete upstream payload.
type Store interface {
	// Queue state machine (delegated to *queue.Store).
	Claim(ctx context.Context, stage queue.Stage, workerID string) (queue.Item, bool, error)
	Complete(ctx context.Context, itemID int64, stage queue.Stage, owner string) error
	Fail(ctx context.Context, itemID int64, stage queue.Stage, reason, owner string) error
	Supersede(ctx context.Context, itemID int64, owner string) error
	Retry(ctx context.Context, itemID int64, backoffSeconds float64, lastErr, owner string) error

	// ProjectAndComplete inserts the item's projected lane rows into the
	// distillation_pairs buffer AND stamps projected_at, atomically in one
	// transaction fenced on owner (todos #2): a partial projection can never
	// coexist with a "done" mark, and a reclaimed worker's projection rolls back
	// entirely (queue.ErrLeaseLost). Returns the number of rows actually inserted.
	ProjectAndComplete(ctx context.Context, itemID int64, owner string, rows []map[string]any) (inserted int, err error)

	// Per-stage payload I/O (this package's columns). Writes are owner-fenced.
	SetComprehension(ctx context.Context, id int64, digest, owner string) error
	GetComprehension(ctx context.Context, id int64) (string, error)
	SetQuestion(ctx context.Context, id int64, q pipeline.Question, owner string) error
	GetQuestion(ctx context.Context, id int64) (pipeline.Question, error)
	SetPresolveVerdict(ctx context.Context, id int64, v model.StudentFilterVerdict, owner string) error
	GetPresolveVerdict(ctx context.Context, id int64) (model.StudentFilterVerdict, error)
	SetAnswerPayload(ctx context.Context, id int64, recs []model.DistillationPair, owner string) error
	GetAnswerPayload(ctx context.Context, id int64) ([]model.DistillationPair, error)
	SetVerification(ctx context.Context, id int64, vs []model.Verification, owner string) error
	GetVerification(ctx context.Context, id int64) ([]model.Verification, error)
}

// PGStore is the production Store: the queue transitions come from the embedded
// *queue.Store, the payload columns from direct db.Querier statements on the
// same connection.
type PGStore struct {
	*queue.Store
	q db.Querier
}

// NewPGStore wraps a queue.Store and the db.Querier backing it.
func NewPGStore(qs *queue.Store, q db.Querier) *PGStore { return &PGStore{Store: qs, q: q} }

// Compile-time check: PGStore satisfies Store.
var _ Store = (*PGStore)(nil)

// setText writes a nullable TEXT payload column for one item, fenced on owner:
// a reclaimed worker's write affects 0 rows and gets queue.ErrLeaseLost.
func (s *PGStore) setText(ctx context.Context, id int64, col, val, owner string) error {
	sql := "UPDATE distillation_items SET " + col + " = $2 WHERE id = $1 AND lock_owner_id = $3"
	tag, err := s.q.Exec(ctx, sql, id, val, owner)
	if err != nil {
		return fmt.Errorf("worker: set %s item %d: %w", col, id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("worker: set %s item %d: %w", col, id, queue.ErrLeaseLost)
	}
	return nil
}

// setJSON marshals v and writes it into a nullable JSONB payload column, fenced
// on owner. The column name is a fixed internal identifier (never caller input),
// so interpolating it is injection-safe; the value is parameterized and cast to
// jsonb so pgx sends it as JSON, not bytea.
func (s *PGStore) setJSON(ctx context.Context, id int64, col string, v any, owner string) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("worker: marshal %s item %d: %w", col, id, err)
	}
	sql := "UPDATE distillation_items SET " + col + " = $2::jsonb WHERE id = $1 AND lock_owner_id = $3"
	tag, err := s.q.Exec(ctx, sql, id, string(b), owner)
	if err != nil {
		return fmt.Errorf("worker: set %s item %d: %w", col, id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("worker: set %s item %d: %w", col, id, queue.ErrLeaseLost)
	}
	return nil
}

// txBeginner is the subset of *pgx.Conn that ProjectAndComplete needs to open a
// transaction. The production PGStore is built over a *pgx.Conn, which satisfies
// it; a non-transactional querier (unexpected in production) is rejected.
type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ProjectAndComplete inserts every projected lane row into the distillation_pairs
// buffer and stamps projected_at on the item, atomically in ONE transaction
// fenced on owner (todos #2). Either all rows are inserted AND the item is marked
// projected, or nothing changes. The mark is owner-fenced: if the lease was lost
// (0 rows on the UPDATE) the whole transaction rolls back and ErrLeaseLost is
// returned, so a reclaimed worker never double-projects. Row inserts are
// ON CONFLICT(task_id) DO NOTHING (loader.InsertRecord), so a genuine retry of a
// rolled-back projection is still idempotent.
func (s *PGStore) ProjectAndComplete(ctx context.Context, itemID int64, owner string, rows []map[string]any) (int, error) {
	b, ok := s.q.(txBeginner)
	if !ok {
		return 0, fmt.Errorf("worker: ProjectAndComplete requires a transactional querier (got %T)", s.q)
	}
	tx, err := b.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("worker: begin project tx item %d: %w", itemID, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after a successful Commit

	inserted := 0
	for _, params := range rows {
		ins, ierr := loader.InsertRecord(ctx, tx, params)
		if ierr != nil {
			return 0, fmt.Errorf("worker: project insert item %d: %w", itemID, ierr)
		}
		if ins {
			inserted++
		}
	}
	tag, err := tx.Exec(ctx,
		`UPDATE distillation_items
SET projected_at = now(), lock_owner_id = NULL, lock_at = NULL, next_attempt_at = NULL
WHERE id = $1 AND lock_owner_id = $2`, itemID, owner)
	if err != nil {
		return 0, fmt.Errorf("worker: mark projected item %d: %w", itemID, err)
	}
	if tag.RowsAffected() == 0 {
		return 0, fmt.Errorf("worker: mark projected item %d: %w", itemID, queue.ErrLeaseLost)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("worker: commit project item %d: %w", itemID, err)
	}
	return inserted, nil
}

// getJSON reads a JSONB/TEXT payload column and unmarshals it into dst. A NULL
// (never written) column is reported as an error so a worker never silently
// proceeds on missing upstream payload.
func (s *PGStore) getJSON(ctx context.Context, id int64, col string, dst any) error {
	sql := "SELECT " + col + " FROM distillation_items WHERE id = $1"
	var raw []byte
	if err := s.q.QueryRow(ctx, sql, id).Scan(&raw); err != nil {
		return fmt.Errorf("worker: get %s item %d: %w", col, id, err)
	}
	if len(raw) == 0 {
		return fmt.Errorf("worker: %s for item %d is empty (upstream stage did not write it)", col, id)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("worker: unmarshal %s item %d: %w", col, id, err)
	}
	return nil
}

func (s *PGStore) SetComprehension(ctx context.Context, id int64, digest, owner string) error {
	return s.setText(ctx, id, "comprehension_digest", digest, owner)
}

func (s *PGStore) GetComprehension(ctx context.Context, id int64) (string, error) {
	var raw []byte
	if err := s.q.QueryRow(ctx, "SELECT comprehension_digest FROM distillation_items WHERE id = $1", id).Scan(&raw); err != nil {
		return "", fmt.Errorf("worker: get comprehension_digest item %d: %w", id, err)
	}
	digest := string(raw)
	if err := validateDigest(id, digest); err != nil {
		return "", err
	}
	return digest, nil
}

func (s *PGStore) SetQuestion(ctx context.Context, id int64, q pipeline.Question, owner string) error {
	return s.setJSON(ctx, id, "question", q, owner)
}

func (s *PGStore) GetQuestion(ctx context.Context, id int64) (pipeline.Question, error) {
	var q pipeline.Question
	if err := s.getJSON(ctx, id, "question", &q); err != nil {
		return pipeline.Question{}, err
	}
	if err := validateQuestion(id, q); err != nil {
		return pipeline.Question{}, err
	}
	return q, nil
}

func (s *PGStore) SetPresolveVerdict(ctx context.Context, id int64, v model.StudentFilterVerdict, owner string) error {
	return s.setJSON(ctx, id, "presolve_verdict", v, owner)
}

func (s *PGStore) GetPresolveVerdict(ctx context.Context, id int64) (model.StudentFilterVerdict, error) {
	var v model.StudentFilterVerdict
	if err := s.getJSON(ctx, id, "presolve_verdict", &v); err != nil {
		return model.StudentFilterVerdict{}, err
	}
	if err := validatePresolveVerdict(id, v); err != nil {
		return model.StudentFilterVerdict{}, err
	}
	return v, nil
}

func (s *PGStore) SetAnswerPayload(ctx context.Context, id int64, recs []model.DistillationPair, owner string) error {
	return s.setJSON(ctx, id, "answer_payload", recs, owner)
}

func (s *PGStore) GetAnswerPayload(ctx context.Context, id int64) ([]model.DistillationPair, error) {
	var recs []model.DistillationPair
	if err := s.getJSON(ctx, id, "answer_payload", &recs); err != nil {
		return nil, err
	}
	if err := validateAnswerRecords(id, recs); err != nil {
		return nil, err
	}
	return recs, nil
}

func (s *PGStore) SetVerification(ctx context.Context, id int64, vs []model.Verification, owner string) error {
	return s.setJSON(ctx, id, "verification", vs, owner)
}

func (s *PGStore) GetVerification(ctx context.Context, id int64) ([]model.Verification, error) {
	var vs []model.Verification
	if err := s.getJSON(ctx, id, "verification", &vs); err != nil {
		return nil, err
	}
	if err := validateVerifications(id, vs); err != nil {
		return nil, err
	}
	return vs, nil
}
