package pacing

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/guny524/distillation/internal/db"
)

//go:embed migrations/0001_quota_pacing.sql
var migration0001 string

// PGStore is the pgx-backed StateStore. It runs on the CronJob's single
// connection (advisory locks are session-level, so all calls must share one
// db.Querier backed by the same *pgx.Conn).
type PGStore struct {
	q db.Querier
}

// NewPGStore wraps a db.Querier (a single *pgx.Conn in production).
func NewPGStore(q db.Querier) *PGStore { return &PGStore{q: q} }

// Compile-time check: PGStore satisfies StateStore.
var _ StateStore = (*PGStore)(nil)

// Migrate applies the pacing table DDL idempotently.
func Migrate(ctx context.Context, q db.Querier) error {
	if _, err := q.Exec(ctx, migration0001); err != nil {
		return fmt.Errorf("apply quota pacing migration: %w", err)
	}
	return nil
}

// advisoryKey is the string hashed into the bigint advisory-lock id (spec sec 5).
func advisoryKey(provider, sourceTag string) string {
	return "quota_pacing:" + provider + ":" + sourceTag
}

// TryAdvisoryLock attempts a non-blocking session-level advisory lock.
func (s *PGStore) TryAdvisoryLock(ctx context.Context, provider, sourceTag string) (bool, error) {
	var acquired bool
	err := s.q.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtextextended($1, 0))`,
		advisoryKey(provider, sourceTag),
	).Scan(&acquired)
	if err != nil {
		return false, fmt.Errorf("try advisory lock: %w", err)
	}
	return acquired, nil
}

// AdvisoryUnlock releases the advisory lock.
func (s *PGStore) AdvisoryUnlock(ctx context.Context, provider, sourceTag string) error {
	var released bool
	err := s.q.QueryRow(ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1, 0))`,
		advisoryKey(provider, sourceTag),
	).Scan(&released)
	if err != nil {
		return fmt.Errorf("advisory unlock: %w", err)
	}
	return nil
}

// GetEMA reads the cost EMA row for key.
func (s *PGStore) GetEMA(ctx context.Context, key EMAKey) (CostEMA, bool, error) {
	e := CostEMA{Provider: key.Provider, SourceTag: key.SourceTag, WindowKind: key.WindowKind}
	err := s.q.QueryRow(ctx, `
SELECT cost_pct_per_item, alpha, sample_count, attribution_mode,
       pending_zero_generated, pending_zero_delta, updated_at
FROM quota_cost_ema
WHERE provider = $1 AND source_tag = $2 AND window_kind = $3`,
		key.Provider, key.SourceTag, key.WindowKind,
	).Scan(&e.CostPctPerItem, &e.Alpha, &e.SampleCount, &e.AttributionMode,
		&e.PendingZeroGenerated, &e.PendingZeroDelta, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, false, nil
	}
	if err != nil {
		return e, false, fmt.Errorf("get ema: %w", err)
	}
	return e, true, nil
}

// UpsertEMA inserts or replaces the EMA row by PK.
func (s *PGStore) UpsertEMA(ctx context.Context, ema CostEMA) error {
	_, err := s.q.Exec(ctx, `
INSERT INTO quota_cost_ema (
    provider, source_tag, window_kind, cost_pct_per_item, alpha, sample_count,
    attribution_mode, pending_zero_generated, pending_zero_delta, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
ON CONFLICT (provider, source_tag, window_kind) DO UPDATE SET
    cost_pct_per_item      = EXCLUDED.cost_pct_per_item,
    alpha                  = EXCLUDED.alpha,
    sample_count           = EXCLUDED.sample_count,
    attribution_mode       = EXCLUDED.attribution_mode,
    pending_zero_generated = EXCLUDED.pending_zero_generated,
    pending_zero_delta     = EXCLUDED.pending_zero_delta,
    updated_at             = NOW()`,
		ema.Provider, ema.SourceTag, ema.WindowKind, ema.CostPctPerItem, ema.Alpha,
		ema.SampleCount, ema.AttributionMode, ema.PendingZeroGenerated, ema.PendingZeroDelta)
	if err != nil {
		return fmt.Errorf("upsert ema: %w", err)
	}
	return nil
}

// GetHourlyBudget reads the hourly budget row for key.
func (s *PGStore) GetHourlyBudget(ctx context.Context, key HourlyKey) (HourlyBudget, bool, error) {
	hb := HourlyBudget{
		Provider: key.Provider, SourceTag: key.SourceTag,
		WindowKind: key.WindowKind, BudgetHour: key.BudgetHour,
	}
	err := s.q.QueryRow(ctx, `
SELECT allowance_pct, planned_pct, consumed_pct, generated_count
FROM quota_hourly_budget_consumption
WHERE provider = $1 AND source_tag = $2 AND budget_hour = $3 AND window_kind = $4`,
		key.Provider, key.SourceTag, key.BudgetHour, key.WindowKind,
	).Scan(&hb.AllowancePct, &hb.PlannedPct, &hb.ConsumedPct, &hb.GeneratedCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return hb, false, nil
	}
	if err != nil {
		return hb, false, fmt.Errorf("get hourly budget: %w", err)
	}
	return hb, true, nil
}

// AddHourlyConsumption records allowance/planned and accumulates consumed_pct /
// generated_count for the hourly budget row (insert or increment by PK).
func (s *PGStore) AddHourlyConsumption(ctx context.Context, hb HourlyBudget) error {
	_, err := s.q.Exec(ctx, `
INSERT INTO quota_hourly_budget_consumption (
    provider, source_tag, budget_hour, window_kind,
    allowance_pct, planned_pct, consumed_pct, generated_count
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (provider, source_tag, budget_hour, window_kind) DO UPDATE SET
    allowance_pct   = EXCLUDED.allowance_pct,
    planned_pct     = quota_hourly_budget_consumption.planned_pct + EXCLUDED.planned_pct,
    consumed_pct    = quota_hourly_budget_consumption.consumed_pct + EXCLUDED.consumed_pct,
    generated_count = quota_hourly_budget_consumption.generated_count + EXCLUDED.generated_count`,
		hb.Provider, hb.SourceTag, hb.BudgetHour, hb.WindowKind,
		hb.AllowancePct, hb.PlannedPct, hb.ConsumedPct, hb.GeneratedCount)
	if err != nil {
		return fmt.Errorf("add hourly consumption: %w", err)
	}
	return nil
}

// CountGeneratedSince sums generated_count of run logs at/after since for the key.
func (s *PGStore) CountGeneratedSince(ctx context.Context, provider, sourceTag string, since time.Time) (int, error) {
	var total int
	err := s.q.QueryRow(ctx, `
SELECT COALESCE(SUM(generated_count), 0)
FROM quota_pacing_run_logs
WHERE provider = $1 AND source_tag = $2 AND started_at >= $3`,
		provider, sourceTag, since,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("count generated since: %w", err)
	}
	return total, nil
}

// LastRateLimitedAt returns the most recent rate_limited run start.
func (s *PGStore) LastRateLimitedAt(ctx context.Context, provider, sourceTag string) (time.Time, bool, error) {
	var t *time.Time
	err := s.q.QueryRow(ctx, `
SELECT MAX(started_at)
FROM quota_pacing_run_logs
WHERE provider = $1 AND source_tag = $2 AND status = $3`,
		provider, sourceTag, StatusRateLimited,
	).Scan(&t)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("last rate limited at: %w", err)
	}
	if t == nil {
		return time.Time{}, false, nil
	}
	return *t, true, nil
}

// InsertRunLog inserts a run log and returns its id.
func (s *PGStore) InsertRunLog(ctx context.Context, rl RunLog) (int64, error) {
	var id int64
	err := s.q.QueryRow(ctx, `
INSERT INTO quota_pacing_run_logs (
    provider, source_tag, status, mode, started_at, decided_at, completed_at,
    planned_count, generated_count, primary_used_before, primary_used_after,
    secondary_used_before, secondary_used_after, allowance_pct, error_msg
) VALUES ($1, $2, $3, $4, COALESCE($5, NOW()), $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
RETURNING id`,
		rl.Provider, rl.SourceTag, rl.Status, rl.Mode, nullTime(rl.StartedAt),
		rl.DecidedAt, rl.CompletedAt, rl.PlannedCount, rl.GeneratedCount,
		rl.PrimaryUsedBefore, rl.PrimaryUsedAfter, rl.SecondaryUsedBefore,
		rl.SecondaryUsedAfter, rl.AllowancePct, rl.ErrorMsg,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert run log: %w", err)
	}
	return id, nil
}

// UpdateRunLog updates mutable fields of an existing run log by ID.
func (s *PGStore) UpdateRunLog(ctx context.Context, rl RunLog) error {
	_, err := s.q.Exec(ctx, `
UPDATE quota_pacing_run_logs SET
    status                = $2,
    mode                  = $3,
    decided_at            = $4,
    completed_at          = $5,
    planned_count         = $6,
    generated_count       = $7,
    primary_used_before   = $8,
    primary_used_after    = $9,
    secondary_used_before = $10,
    secondary_used_after  = $11,
    allowance_pct         = $12,
    error_msg             = $13
WHERE id = $1`,
		rl.ID, rl.Status, rl.Mode, rl.DecidedAt, rl.CompletedAt, rl.PlannedCount,
		rl.GeneratedCount, rl.PrimaryUsedBefore, rl.PrimaryUsedAfter,
		rl.SecondaryUsedBefore, rl.SecondaryUsedAfter, rl.AllowancePct, rl.ErrorMsg)
	if err != nil {
		return fmt.Errorf("update run log: %w", err)
	}
	return nil
}

// InsertObservation appends an observation row.
func (s *PGStore) InsertObservation(ctx context.Context, obs Observation) error {
	_, err := s.q.Exec(ctx, `
INSERT INTO quota_observations (
    run_log_id, provider, source_tag, window_kind, used_before, used_after,
    resets_at_before, resets_at_after, delta, generated_count,
    is_outlier, outlier_reason, used_for_ema, observed_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, COALESCE($14, NOW()))`,
		obs.RunLogID, obs.Provider, obs.SourceTag, obs.WindowKind, obs.UsedBefore,
		obs.UsedAfter, obs.ResetsAtBefore, obs.ResetsAtAfter, obs.Delta,
		obs.GeneratedCount, obs.IsOutlier, obs.OutlierReason, obs.UsedForEMA,
		nullTime(obs.ObservedAt))
	if err != nil {
		return fmt.Errorf("insert observation: %w", err)
	}
	return nil
}

// RecoverStaleRunning marks running rows started before olderThan as
// crashed_recovered and returns how many were recovered (spec sec 5).
func (s *PGStore) RecoverStaleRunning(ctx context.Context, provider, sourceTag string, olderThan time.Time) (int, error) {
	tag, err := s.q.Exec(ctx, `
UPDATE quota_pacing_run_logs
SET status = $4, completed_at = NOW()
WHERE provider = $1 AND source_tag = $2 AND status = $5 AND started_at < $3`,
		provider, sourceTag, olderThan, StatusCrashedRecovered, StatusRunning)
	if err != nil {
		return 0, fmt.Errorf("recover stale running: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// nullTime returns nil for a zero time so COALESCE(..., NOW()) fills the default.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
