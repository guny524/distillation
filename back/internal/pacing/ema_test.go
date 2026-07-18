package pacing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	resetA = time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	resetB = time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
)

func TestUpdateEMA_FirstSampleInitializes(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 10, UsedAfter: 10.5, // window delta 0.5 (<= 1% user-mixed floor)
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 5,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.True(t, res.UsedForEMA)
	assert.False(t, res.IsOutlier)
	assert.InDelta(t, 0.1, res.EMA.CostPctPerItem, 1e-9) // 0.5/5
	assert.Equal(t, int64(1), res.EMA.SampleCount)
	assert.Equal(t, AttributionWindowDelta, res.EMA.AttributionMode)
}

func TestUpdateEMA_AttributionUsesSourceDelta(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowPrimary,
		UsedBefore: 10, UsedAfter: 30, // large window delta (would be user_mixed)
		ResetsAtBefore: resetA, ResetsAtAfter: resetA,
		GeneratedCount: 4,
		SourceDelta:    2.0, HasAttribution: true,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.True(t, res.UsedForEMA)
	assert.InDelta(t, 0.5, res.EMA.CostPctPerItem, 1e-9) // 2.0/4, source delta wins
	assert.Equal(t, AttributionSourceTag, res.EMA.AttributionMode)
}

func TestUpdateEMA_Blends(t *testing.T) {
	cfg := DefaultConfig()
	prev := CostEMA{CostPctPerItem: 1.0, SampleCount: 5}
	in := ObservationInput{
		WindowKind: WindowPrimary,
		UsedBefore: 0, UsedAfter: 0,
		ResetsAtBefore: resetA, ResetsAtAfter: resetA,
		GeneratedCount: 1,
		SourceDelta:    2.0, HasAttribution: true,
	}
	res := UpdateEMA(in, prev, cfg)

	// 0.2*2.0 + 0.8*1.0 = 1.2
	assert.InDelta(t, 1.2, res.EMA.CostPctPerItem, 1e-9)
	assert.Equal(t, int64(6), res.EMA.SampleCount)
}

func TestUpdateEMA_NegativeDelta(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 50, UsedAfter: 40,
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 3,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.True(t, res.IsOutlier)
	assert.Equal(t, OutlierNegativeDelta, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_NegativeDeltaWithResetIsRollover(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 90, UsedAfter: 5,
		ResetsAtBefore: resetA, ResetsAtAfter: resetB, // reset advanced
		GeneratedCount: 3,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.Equal(t, OutlierRollover, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_RolloverPositiveDelta(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 10, UsedAfter: 12,
		ResetsAtBefore: resetA, ResetsAtAfter: resetB,
		GeneratedCount: 2,
		SourceDelta:    2, HasAttribution: true,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.Equal(t, OutlierRollover, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_TooLargeDelta(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 0, UsedAfter: 0,
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 1,
		SourceDelta:    25, HasAttribution: true, // > max_reasonable_delta_pct 20
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.Equal(t, OutlierTooLargeDelta, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_UserMixedDelta(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 10, UsedAfter: 15, // window delta 5 > min_user_mixed_delta_pct 1, no attribution
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 2,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.Equal(t, OutlierUserMixedDelta, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_ZeroResolutionPendingThenBatch(t *testing.T) {
	cfg := DefaultConfig()

	// First: sub-resolution delta (< min_observable 0.01) accumulates pending.
	in1 := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 10, UsedAfter: 10.005,
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 2,
	}
	res1 := UpdateEMA(in1, CostEMA{}, cfg)
	assert.Equal(t, OutlierZeroResolutionPend, res1.OutlierReason)
	assert.False(t, res1.UsedForEMA)
	assert.Equal(t, 2, res1.EMA.PendingZeroGenerated)
	assert.InDelta(t, 0.005, res1.EMA.PendingZeroDelta, 1e-9)
	assert.Equal(t, int64(0), res1.EMA.SampleCount)

	// Then: observable delta triggers one batched average update over pending+now.
	in2 := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 0, UsedAfter: 0,
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 4,
		SourceDelta:    0.5, HasAttribution: true,
	}
	res2 := UpdateEMA(in2, res1.EMA, cfg)
	assert.True(t, res2.UsedForEMA)
	// (0.5 + 0.005) / (4 + 2) = 0.505/6
	assert.InDelta(t, 0.505/6.0, res2.EMA.CostPctPerItem, 1e-9)
	assert.Equal(t, 0, res2.EMA.PendingZeroGenerated)
	assert.InDelta(t, 0.0, res2.EMA.PendingZeroDelta, 1e-12)
}

func TestUpdateEMA_CrashRecoveryMixed(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 10, UsedAfter: 11,
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 2,
		SourceDelta:    1, HasAttribution: true,
		CrashRecovery: true,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.Equal(t, OutlierCrashRecoveryMixed, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_QuotaUnknown(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{WindowKind: WindowSecondary, QuotaUnknown: true, GeneratedCount: 1}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.Equal(t, OutlierQuotaUnknown, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_CostOutOfBounds(t *testing.T) {
	cfg := DefaultConfig()
	// delta 0.02 (>= min_observable) over 100 items -> cost 0.0002 < min_cost 0.01.
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 0, UsedAfter: 0,
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 100,
		SourceDelta:    0.02, HasAttribution: true,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.Equal(t, OutlierCostOutOfBounds, res.OutlierReason)
	assert.False(t, res.UsedForEMA)
}

func TestUpdateEMA_ZeroGeneratedNotUsed(t *testing.T) {
	cfg := DefaultConfig()
	in := ObservationInput{
		WindowKind: WindowSecondary,
		UsedBefore: 10, UsedAfter: 10.5,
		ResetsAtBefore: resetB, ResetsAtAfter: resetB,
		GeneratedCount: 0,
	}
	res := UpdateEMA(in, CostEMA{}, cfg)

	assert.False(t, res.UsedForEMA)
	assert.False(t, res.IsOutlier)
}
