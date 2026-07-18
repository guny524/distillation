package pacing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testProvider = "codex"
	testSource   = "distill-teacher"
)

func TestDecide_NormalDirect_CapsAtMaxItems(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	// secondary reset 8h out -> inside 24h accel window -> horizon compressed.
	snap := directSnap(testProvider, 10, 20, now.Add(4*time.Hour), now.Add(8*time.Hour), now)
	cfg := DefaultConfig()

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, cfg, newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, ModeDirect, d.Mode)
	assert.Equal(t, StatusDecided, d.Status)
	// allowance = 80 / max(1, 8*0.5=4) = 20, clamped to max_per_hour_pct=10.
	assert.InDelta(t, 10.0, d.HourlyAllowancePct, 1e-9)
	// kWeekly = floor(10/0.5)=20, kPrimary huge, clamped to max_items_per_run=5.
	assert.Equal(t, 5, d.K)
	assert.False(t, d.Probe)
}

func TestDecide_ZeroWeeklyRemaining(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	snap := directSnap(testProvider, 10, 100, now.Add(4*time.Hour), now.Add(50*time.Hour), now)

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusNoQuotaHeadroom, d.Status)
	assert.Equal(t, 0.0, d.WeeklyRemainingPct)
}

func TestDecide_ZeroPrimaryHeadroom(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	// primary at cap -> headroom = 95 - 95 - 1 < 0.
	snap := directSnap(testProvider, 95, 20, now.Add(4*time.Hour), now.Add(50*time.Hour), now)

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusNoQuotaHeadroom, d.Status)
	assert.Equal(t, 0.0, d.PrimaryHeadroomPct)
}

func TestDecide_ResetBoundarySkip(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	// primary already at reset (not after now).
	snap := directSnap(testProvider, 10, 20, now, now.Add(50*time.Hour), now)

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusResetBoundarySkip, d.Status)
}

func TestDecide_ResetBlackout(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	// secondary reset 200s out < reset_blackout_seconds=300.
	snap := directSnap(testProvider, 10, 20, now.Add(4*time.Hour), now.Add(200*time.Second), now)

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusResetBlackout, d.Status)
}

func TestDecide_UserSpikeReducesK(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	primaryReset := now.Add(4 * time.Hour)
	secondaryReset := now.Add(8 * time.Hour) // accel window
	cfg := DefaultConfig()

	low, err := Decide(context.Background(), directSnap(testProvider, 10, 20, primaryReset, secondaryReset, now),
		testProvider, testSource, now, cfg, newFakeReader())
	require.NoError(t, err)

	high, err := Decide(context.Background(), directSnap(testProvider, 10, 99, primaryReset, secondaryReset, now),
		testProvider, testSource, now, cfg, newFakeReader())
	require.NoError(t, err)

	// Heavy user consumption (secondary 99%) shrinks remaining -> fewer items.
	assert.Greater(t, low.K, high.K)
}

func TestDecide_NilSnapshotFetchFailed(t *testing.T) {
	now := time.Now().UTC()
	d, err := Decide(context.Background(), nil, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)
	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusQuotaFetchFailed, d.Status)
}

func TestDecide_ProviderMissing(t *testing.T) {
	now := time.Now().UTC()
	snap := directSnap("other", 10, 20, now.Add(4*time.Hour), now.Add(50*time.Hour), now)
	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)
	assert.Equal(t, StatusQuotaMissing, d.Status)
}

func TestDecide_StaleSnapshot(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	// observed 10 min ago, stale_quota_max_age_seconds=300.
	snap := directSnap(testProvider, 10, 20, now.Add(4*time.Hour), now.Add(50*time.Hour), now.Add(-10*time.Minute))
	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)
	assert.Equal(t, StatusQuotaFetchFailed, d.Status)
}

func TestDecide_CLIUnknownFresh(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	snap := cliUnknownSnap(testProvider, now)

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, ModeCLIUnknown, d.Mode)
	assert.Equal(t, StatusDecided, d.Status)
	assert.Equal(t, 1, d.K) // cli_unknown_max_per_run_items
}

func TestDecide_CLIUnknownCooldown(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	snap := cliUnknownSnap(testProvider, now)
	r := newFakeReader()
	rl := now.Add(-30 * time.Minute) // within 1h cooldown
	r.lastRateLimit = &rl

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), r)
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusUnknownQuotaCooldown, d.Status)
}

func TestDecide_CLIUnknownWeeklyExhausted(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	snap := cliUnknownSnap(testProvider, now)
	r := newFakeReader()
	// hour-start call returns 0, week call (since ~7d ago) returns 50 (== weekly cap).
	r.countFn = func(since time.Time) int {
		if now.Sub(since) > 24*time.Hour {
			return 50
		}
		return 0
	}

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), r)
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusUnknownVirtualBudgetExhaust, d.Status)
}

func TestDecide_Probe(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	// Non-accel horizon, secondary 99% -> tiny allowance -> kWeekly rounds to 0,
	// but weekly_remaining(1) >= 0.2 and primary_headroom >= 2 -> single probe.
	snap := directSnap(testProvider, 10, 99, now.Add(4*time.Hour), now.Add(100*time.Hour), now)

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, 1, d.K)
	assert.True(t, d.Probe)
	assert.Equal(t, StatusDecided, d.Status)
}

func TestDecide_ProbeSuppressedWhenBudgetBelowThreshold(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	// weekly_remaining = 0.05 < probe_min_weekly_budget_pct=0.2 -> no probe.
	snap := directSnap(testProvider, 10, 99.95, now.Add(4*time.Hour), now.Add(100*time.Hour), now)

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), newFakeReader())
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.False(t, d.Probe)
	assert.Equal(t, StatusNoQuotaHeadroom, d.Status)
}

func TestDecide_HourlyBudgetAlreadyConsumed(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	snap := directSnap(testProvider, 10, 20, now.Add(4*time.Hour), now.Add(8*time.Hour), now)
	r := newFakeReader()
	// This hour already spent the whole allowance.
	r.hourly[HourlyKey{Provider: testProvider, SourceTag: testSource, WindowKind: WindowSecondary, BudgetHour: budgetHour(now)}] =
		HourlyBudget{ConsumedPct: 10.0}

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), r)
	require.NoError(t, err)

	assert.Equal(t, 0, d.K)
	assert.Equal(t, StatusHourlyBudgetExhausted, d.Status)
}

func TestDecide_UsesLearnedEMACost(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	snap := directSnap(testProvider, 10, 20, now.Add(4*time.Hour), now.Add(8*time.Hour), now)
	r := newFakeReader()
	// Learned secondary cost 2.0%/item (vs fallback 0.5) -> fewer items.
	r.emas[EMAKey{testProvider, testSource, WindowSecondary}] = CostEMA{CostPctPerItem: 2.0, SampleCount: 10}

	d, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), r)
	require.NoError(t, err)

	assert.InDelta(t, 2.0, d.CostSecondaryPct, 1e-9)
	// allowance 10 / cost 2.0 = 5 -> k=5 (still capped).
	assert.Equal(t, 5, d.K)
}

func TestDecide_ReaderErrorPropagates(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	snap := directSnap(testProvider, 10, 20, now.Add(4*time.Hour), now.Add(8*time.Hour), now)
	r := newFakeReader()
	r.forceErr = errors.New("db down")

	_, err := Decide(context.Background(), snap, testProvider, testSource, now, DefaultConfig(), r)
	require.Error(t, err)
}
