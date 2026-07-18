package pacing

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/db/dbtest"
)

func TestPGStore_TryAdvisoryLock(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, args ...any) pgx.Row {
			gotSQL, gotArgs = sql, args
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}
	s := NewPGStore(q)

	acquired, err := s.TryAdvisoryLock(context.Background(), "codex", "distill-teacher")
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Contains(t, gotSQL, "pg_try_advisory_lock")
	assert.Contains(t, gotSQL, "hashtextextended")
	require.Len(t, gotArgs, 1)
	assert.Equal(t, "quota_pacing:codex:distill-teacher", gotArgs[0])
}

func TestPGStore_TryAdvisoryLockBusy(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = false
				return nil
			}}
		},
	}
	acquired, err := NewPGStore(q).TryAdvisoryLock(context.Background(), "codex", "s")
	require.NoError(t, err)
	assert.False(t, acquired)
}

func TestPGStore_GetEMANoRows(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(_ ...any) error { return pgx.ErrNoRows }}
		},
	}
	ema, ok, err := NewPGStore(q).GetEMA(context.Background(), EMAKey{"codex", "s", WindowPrimary})
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, "codex", ema.Provider)
}

func TestPGStore_GetEMAFound(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*float64)) = 1.5 // cost_pct_per_item
				*(dest[1].(*float64)) = 0.2 // alpha
				*(dest[2].(*int64)) = 9     // sample_count
				*(dest[3].(*string)) = AttributionSourceTag
				*(dest[4].(*int)) = 0     // pending_zero_generated
				*(dest[5].(*float64)) = 0 // pending_zero_delta
				*(dest[6].(*time.Time)) = time.Now()
				return nil
			}}
		},
	}
	ema, ok, err := NewPGStore(q).GetEMA(context.Background(), EMAKey{"codex", "s", WindowPrimary})
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 1.5, ema.CostPctPerItem)
	assert.Equal(t, int64(9), ema.SampleCount)
}

func TestPGStore_LastRateLimitedAtNone(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(**time.Time)) = nil // MAX(started_at) over no rows -> NULL
				return nil
			}}
		},
	}
	_, ok, err := NewPGStore(q).LastRateLimitedAt(context.Background(), "codex", "s")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestPGStore_InsertRunLogReturnsID(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			assert.Contains(t, sql, "INSERT INTO quota_pacing_run_logs")
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 42
				return nil
			}}
		},
	}
	id, err := NewPGStore(q).InsertRunLog(context.Background(), RunLog{
		Provider: "codex", SourceTag: "s", Status: StatusRunning,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)
}

func TestPGStore_RecoverStaleRunning(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			assert.Contains(t, sql, "quota_pacing_run_logs")
			return pgconn.NewCommandTag("UPDATE 3"), nil
		},
	}
	n, err := NewPGStore(q).RecoverStaleRunning(context.Background(), "codex", "s", time.Now())
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestMigrate_ExecutesDDL(t *testing.T) {
	var gotSQL string
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			gotSQL = sql
			return pgconn.NewCommandTag("CREATE TABLE"), nil
		},
	}
	require.NoError(t, Migrate(context.Background(), q))
	for _, table := range []string{
		"quota_pacing_run_logs", "quota_cost_ema",
		"quota_observations", "quota_hourly_budget_consumption",
	} {
		assert.True(t, strings.Contains(gotSQL, table), "DDL should create %s", table)
	}
}

// Compile-time: PGStore is usable as both reader and full store.
var (
	_ StateReader = (*PGStore)(nil)
	_ db.Querier  = (*dbtest.MockQuerier)(nil)
)
