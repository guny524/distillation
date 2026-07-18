// Package pacing implements quota-aware generation pacing for the distillation
// pipeline. Each CronJob run reads subgate's GET /quota snapshot and decides how
// many teacher generations (k) to launch so weekly quota is spent evenly up to a
// deadline while respecting the short (primary) rate-limit window.
//
// The package is split into pure decision logic (Decide, UpdateEMA) that depends
// only on a StateReader/StateStore interface, and a concrete pgx-backed store
// (PGStore). This separation keeps the pacing math unit-testable with a fake
// store, with no PostgreSQL required.
//
// Spec: todos/design/quota-pacing-spec.md
package pacing

import (
	"context"
	"time"
)

// Window kinds identify which rate-limit window an EMA / observation / hourly
// budget row belongs to.
const (
	WindowPrimary   = "primary"   // short rolling window (codex: 5h)
	WindowSecondary = "secondary" // long window (codex: weekly)
)

// Quota mode classification (spec sec 1).
const (
	ModeDirect     = "direct"      // primary/secondary used_percent both known
	ModeCLIUnknown = "cli_unknown" // rate-limit windows not observable
)

// Run log status values (spec sec 2, quota_pacing_run_logs.status).
const (
	StatusRunning                     = "running"
	StatusDecided                     = "decided"
	StatusCompleted                   = "completed"
	StatusSkippedLockBusy             = "skipped_lock_busy"
	StatusQuotaMissing                = "quota_missing"
	StatusQuotaFetchFailed            = "quota_fetch_failed"
	StatusResetBoundarySkip           = "reset_boundary_skip"
	StatusResetBlackout               = "reset_blackout"
	StatusNoQuotaHeadroom             = "no_quota_headroom"
	StatusHourlyBudgetExhausted       = "hourly_budget_exhausted"
	StatusUnknownQuotaCooldown        = "unknown_quota_cooldown"
	StatusUnknownVirtualBudgetExhaust = "unknown_virtual_budget_exhausted"
	StatusRateLimited                 = "rate_limited"
	StatusCrashedRecovered            = "crashed_recovered"
)

// Outlier reasons (spec sec 2/3, quota_observations.outlier_reason).
const (
	OutlierNegativeDelta      = "negative_delta"
	OutlierRollover           = "rollover"
	OutlierTooLargeDelta      = "too_large_delta"
	OutlierUserMixedDelta     = "user_mixed_delta"
	OutlierZeroResolutionPend = "zero_resolution_pending"
	OutlierCrashRecoveryMixed = "crash_recovery_mixed"
	OutlierQuotaUnknown       = "quota_unknown"
	OutlierCostOutOfBounds    = "cost_out_of_bounds"
)

// Attribution modes (spec sec 2, quota_cost_ema.attribution_mode).
const (
	AttributionSourceTag      = "source_tag"
	AttributionWindowDelta    = "window_delta"
	AttributionConservativeFB = "conservative_fallback"
	AttributionUnknownVirtual = "unknown_virtual"
)

// EMAKey identifies a cost-EMA row (quota_cost_ema PK).
type EMAKey struct {
	Provider   string
	SourceTag  string
	WindowKind string
}

// HourlyKey identifies an hourly budget row (quota_hourly_budget_consumption PK).
type HourlyKey struct {
	Provider   string
	SourceTag  string
	WindowKind string
	BudgetHour time.Time
}

// CostEMA is the learned per-item %-of-window cost for one (provider, source_tag,
// window_kind), plus pending accumulation for sub-resolution (delta==0) samples.
type CostEMA struct {
	Provider             string
	SourceTag            string
	WindowKind           string
	CostPctPerItem       float64
	Alpha                float64
	SampleCount          int64
	AttributionMode      string
	PendingZeroGenerated int
	PendingZeroDelta     float64
	UpdatedAt            time.Time
}

// HourlyBudget is the per-hour planned/consumed allowance for one key.
type HourlyBudget struct {
	Provider       string
	SourceTag      string
	WindowKind     string
	BudgetHour     time.Time
	AllowancePct   float64
	PlannedPct     float64
	ConsumedPct    float64
	GeneratedCount int
}

// RunLog is one pacing decision/run record (quota_pacing_run_logs).
type RunLog struct {
	ID                  int64
	Provider            string
	SourceTag           string
	Status              string
	Mode                string
	StartedAt           time.Time
	DecidedAt           *time.Time
	CompletedAt         *time.Time
	PlannedCount        int
	GeneratedCount      int
	PrimaryUsedBefore   *float64
	PrimaryUsedAfter    *float64
	SecondaryUsedBefore *float64
	SecondaryUsedAfter  *float64
	AllowancePct        *float64
	ErrorMsg            string
}

// Observation is one before/after quota observation tied to a run (quota_observations).
type Observation struct {
	ID             int64
	RunLogID       int64
	Provider       string
	SourceTag      string
	WindowKind     string
	UsedBefore     float64
	UsedAfter      float64
	ResetsAtBefore *time.Time
	ResetsAtAfter  *time.Time
	Delta          float64
	GeneratedCount int
	IsOutlier      bool
	OutlierReason  string
	UsedForEMA     bool
	ObservedAt     time.Time
}

// StateReader is the read-only DB surface the pure Decide logic depends on.
// Tests inject a fake; production uses PGStore.
type StateReader interface {
	// GetEMA returns the cost EMA for key; ok=false when no row exists yet.
	GetEMA(ctx context.Context, key EMAKey) (ema CostEMA, ok bool, err error)
	// GetHourlyBudget returns the hourly budget row for key; ok=false when absent.
	GetHourlyBudget(ctx context.Context, key HourlyKey) (hb HourlyBudget, ok bool, err error)
	// CountGeneratedSince sums generated_count of run logs at/after since for the key.
	CountGeneratedSince(ctx context.Context, provider, sourceTag string, since time.Time) (int, error)
	// LastRateLimitedAt returns the most recent rate_limited run start; ok=false when none.
	LastRateLimitedAt(ctx context.Context, provider, sourceTag string) (t time.Time, ok bool, err error)
}

// StateStore is the full DB surface (reads + writes + advisory lock) used by
// the orchestration layer (internal/runner).
type StateStore interface {
	StateReader
	// TryAdvisoryLock attempts pg_try_advisory_lock for (provider, source_tag).
	// Returns acquired=false without blocking when another session holds it.
	TryAdvisoryLock(ctx context.Context, provider, sourceTag string) (acquired bool, err error)
	// AdvisoryUnlock releases the session-level advisory lock.
	AdvisoryUnlock(ctx context.Context, provider, sourceTag string) error
	// InsertRunLog inserts a run log and returns its id.
	InsertRunLog(ctx context.Context, rl RunLog) (int64, error)
	// UpdateRunLog updates mutable fields of an existing run log (by ID).
	UpdateRunLog(ctx context.Context, rl RunLog) error
	// UpsertEMA writes the EMA row (insert or replace by PK).
	UpsertEMA(ctx context.Context, ema CostEMA) error
	// InsertObservation appends an observation row.
	InsertObservation(ctx context.Context, obs Observation) error
	// AddHourlyConsumption accumulates consumed_pct/generated_count and records
	// allowance/planned for the hourly budget row (insert or increment by PK).
	AddHourlyConsumption(ctx context.Context, hb HourlyBudget) error
	// RecoverStaleRunning marks running rows older than the deadline as
	// crashed_recovered and returns how many were recovered.
	RecoverStaleRunning(ctx context.Context, provider, sourceTag string, olderThan time.Time) (int, error)
}

// budgetHour truncates now to the top of its UTC hour, the budget_hour key.
func budgetHour(now time.Time) time.Time {
	return now.UTC().Truncate(time.Hour)
}
