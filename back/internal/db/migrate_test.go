package db_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/db/dbtest"
)

// captureExec returns a MockQuerier whose ExecFn records every SQL string it is
// handed, in call order, plus the slice it appends to.
func captureExec() (*dbtest.MockQuerier, *[]string) {
	var got []string
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			got = append(got, sql)
			return pgconn.NewCommandTag("CREATE TABLE"), nil
		},
	}
	return q, &got
}

// TestMigrate_AppliesUnifiedSchema verifies the single entry point applies the
// one schema.sql (one Exec) carrying the queue tables, the source cursors, and
// the distillation_pairs buffer, with the queue DDL preceding the buffer.
func TestMigrate_AppliesUnifiedSchema(t *testing.T) {
	q, got := captureExec()
	require.NoError(t, db.Migrate(context.Background(), q))

	require.Len(t, *got, 1, "the unified schema is applied in a single Exec")
	sql := (*got)[0]
	assert.Contains(t, sql, "CREATE TABLE IF NOT EXISTS distillation_items")
	assert.Contains(t, sql, "CREATE TABLE IF NOT EXISTS distillation_source_cursors")
	assert.Contains(t, sql, "CREATE TABLE IF NOT EXISTS distillation_pairs")
	// The 0002-era columns are folded into the CREATE (no ALTER history).
	assert.Contains(t, sql, "projected_at")
	assert.Contains(t, sql, "retry_count")
	assert.Contains(t, sql, "source_excerpt")
	// The per-stage payload columns live in the unified schema too (no separate
	// worker payload migration).
	for _, col := range []string{"comprehension_digest", "question", "presolve_verdict", "answer_payload", "verification"} {
		assert.Contains(t, sql, col)
	}
	// The flush index keys on projected_at (the item's terminal mark).
	assert.Contains(t, sql, "idx_items_flush_pending")
	assert.Contains(t, sql, "projected_at IS NULL")

	itemsAt := strings.Index(sql, "CREATE TABLE IF NOT EXISTS distillation_items")
	pairsAt := strings.Index(sql, "CREATE TABLE IF NOT EXISTS distillation_pairs")
	assert.Less(t, itemsAt, pairsAt, "queue DDL must precede the pairs buffer DDL")
}

// TestMigrate_WrapsError stamps an "apply schema" error on failure.
func TestMigrate_WrapsError(t *testing.T) {
	boom := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.CommandTag{}, fmt.Errorf("boom")
		},
	}
	err := db.Migrate(context.Background(), boom)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply schema")
}

// TestMigrateQueueAndPairs_DelegateToSchema verifies the compatibility wrappers
// apply the same unified schema (queue.Migrate / loader.CreateTable call sites).
func TestMigrateQueueAndPairs_DelegateToSchema(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(context.Context, db.Querier) error
	}{
		{"MigrateQueue", db.MigrateQueue},
		{"MigratePairs", db.MigratePairs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, got := captureExec()
			require.NoError(t, tc.fn(context.Background(), q))
			require.Len(t, *got, 1)
			assert.Contains(t, (*got)[0], "CREATE TABLE IF NOT EXISTS distillation_items")
			assert.Contains(t, (*got)[0], "CREATE TABLE IF NOT EXISTS distillation_pairs")
		})
	}
}
