package db

import (
	"context"
	_ "embed"
	"fmt"
)

// schema.sql is the single physical source of the pipeline's queue + buffer
// DDL: db.Migrate applies it via go:embed AND sqlc reads the very same file as
// its schema input (internal/db/sqlc.yaml), so the DDL the workers apply can
// never drift from the DDL sqlc validates the typed queries against. It is one
// final-form file (CREATE ... IF NOT EXISTS), not a versioned migration set --
// deploys are drop+redeploy, so schema evolution edits this file directly.
//
// Scope note: the pacing tables (internal/pacing.Migrate) and the stage payload
// columns (internal/worker) are applied by their own owners and are
// intentionally not embedded here -- this package owns the queue state machine
// and the distillation_pairs buffer.
//
//go:embed schema.sql
var schemaSQL string

// Migrate applies the queue state machine + distillation_pairs buffer schema.
// Idempotent (CREATE ... IF NOT EXISTS); safe on every worker startup and under
// concurrent worker boots. Worker openDB calls this single entry point.
func Migrate(ctx context.Context, q Querier) error {
	if _, err := q.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// MigrateQueue and MigratePairs are kept for the queue.Migrate / loader.CreateTable
// call sites. The schema is unified in one file, so each applies the whole thing
// (idempotent); there is no longer a separate per-domain DDL to run.
func MigrateQueue(ctx context.Context, q Querier) error { return Migrate(ctx, q) }
func MigratePairs(ctx context.Context, q Querier) error { return Migrate(ctx, q) }
