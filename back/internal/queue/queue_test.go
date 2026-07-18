package queue

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db/dbtest"
)

// TestMigrate_ExecutesDDL verifies the queue migration creates both tables, the
// full set of state-timestamp columns, the idempotency constraint, the lock
// columns, and a pending partial index per stage.
func TestMigrate_ExecutesDDL(t *testing.T) {
	var gotSQL string
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			gotSQL += sql + "\n" // Migrate applies 0001 then 0002 (two Execs)
			return pgconn.NewCommandTag("CREATE TABLE"), nil
		},
	}
	require.NoError(t, Migrate(context.Background(), q))

	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS distillation_items",
		"CREATE TABLE IF NOT EXISTS distillation_source_cursors",
		"UNIQUE (source_type, doc_id)",
		"lock_owner_id",
		"lock_at",
	} {
		assert.Contains(t, gotSQL, want, "migration must contain %q", want)
	}

	// Every state timestamp column must exist (todos sec 2-5-1).
	for _, col := range []string{
		"queued_at", "comprehended_at", "questioned_at", "presolved_at",
		"answered_at", "verified_at", "projected_at", "superseded_at",
		"failed_at", "fail_stage", "fail_reason",
	} {
		assert.Contains(t, gotSQL, col, "migration must define %s", col)
	}

	// One pending partial index per claimable stage.
	for _, idx := range []string{
		"idx_items_comprehend_pending", "idx_items_question_pending",
		"idx_items_presolve_pending", "idx_items_answer_pending",
		"idx_items_verify_pending", "idx_items_flush_pending",
	} {
		assert.Contains(t, gotSQL, idx, "migration must define %s", idx)
	}
}

// TestMigrate_Error surfaces an Exec error.
func TestMigrate_Error(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, assertErr("boom")
		},
	}
	err := Migrate(context.Background(), q)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply schema")
}

// TestStages_MappingComplete guards that every named Stage has a metadata entry
// and that the done column is unique per stage (no two stages fill the same
// timestamp).
func TestStages_MappingComplete(t *testing.T) {
	all := []Stage{
		StageComprehend, StageQuestion, StagePresolve,
		StageAnswer, StageVerify, StageFlush,
	}
	seen := map[string]bool{}
	for _, st := range all {
		m, err := meta(st)
		require.NoError(t, err, "stage %s must have metadata", st)
		require.NotEmpty(t, m.prev, "stage %s must have a prev gate", st)
		require.NotEmpty(t, m.doneCol, "stage %s must have a done column", st)
		assert.False(t, seen[m.doneCol], "done column %s reused", m.doneCol)
		seen[m.doneCol] = true
	}
	assert.Len(t, seen, len(all))

	// itemColumns and scanItem must stay in lockstep.
	assert.Len(t, itemColumns, 25)
	assert.Equal(t, "id", itemColumns[0])
	assert.Equal(t, "last_error", itemColumns[len(itemColumns)-1])
}

// assertErr is a tiny error type so the migration-error test needs no extra deps.
type assertErr string

func (e assertErr) Error() string { return string(e) }
