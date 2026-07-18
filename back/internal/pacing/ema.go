package pacing

import "time"

// ObservationInput is one before/after quota observation fed to UpdateEMA.
type ObservationInput struct {
	WindowKind     string
	UsedBefore     float64
	UsedAfter      float64
	ResetsAtBefore time.Time
	ResetsAtAfter  time.Time
	GeneratedCount int

	// SourceDelta is the source-tag-attributed primary-window delta from
	// pipeline[].used_percent_delta. When HasAttribution it is preferred over the
	// raw window delta, which may include interactive user consumption.
	SourceDelta    float64
	HasAttribution bool

	// CrashRecovery marks a delta that spans a crashed/recovered run; excluded
	// from EMA (mixed accounting) but usable as a conservative hourly-budget hit.
	CrashRecovery bool
	// QuotaUnknown marks a cli-unknown observation; /quota-based EMA is never
	// updated for these (unknown_virtual fallback only).
	QuotaUnknown bool
}

// EMAResult is the outcome of one observation: the (possibly unchanged) EMA row,
// the effective delta, and outlier classification (spec sec 3).
type EMAResult struct {
	EMA           CostEMA
	Delta         float64
	ObservedCost  float64
	IsOutlier     bool
	OutlierReason string
	UsedForEMA    bool
}

func (r EMAResult) outlier(reason string) EMAResult {
	r.IsOutlier = true
	r.OutlierReason = reason
	r.UsedForEMA = false
	return r
}

// UpdateEMA classifies an observation and returns the next EMA state.
//
// Normal update requires: generated>0, resets_at unchanged, after>=before,
// 0<delta<=max_reasonable, cost in [min,max]. All other cases are outliers with
// a reason and leave the EMA cost untouched — except delta==0 (sub-resolution),
// which accumulates into pending_zero for a later batched update (spec sec 3).
func UpdateEMA(in ObservationInput, ema CostEMA, cfg Config) EMAResult {
	windowDelta := in.UsedAfter - in.UsedBefore
	effectiveDelta := windowDelta
	if in.HasAttribution {
		effectiveDelta = in.SourceDelta
	}
	res := EMAResult{EMA: ema, Delta: effectiveDelta}

	// cli-unknown: /quota EMA is never updated.
	if in.QuotaUnknown {
		return res.outlier(OutlierQuotaUnknown)
	}
	// Crash-recovery spans are mixed; excluded from EMA.
	if in.CrashRecovery {
		return res.outlier(OutlierCrashRecoveryMixed)
	}

	resetChanged := !in.ResetsAtAfter.Equal(in.ResetsAtBefore) &&
		!in.ResetsAtBefore.IsZero() && !in.ResetsAtAfter.IsZero()

	// Negative delta: rollover if the window reset advanced, else a bad sample.
	if effectiveDelta < 0 {
		if in.ResetsAtAfter.After(in.ResetsAtBefore) {
			return res.outlier(OutlierRollover)
		}
		return res.outlier(OutlierNegativeDelta)
	}
	// Window rolled over mid-run: exclude, only the reset boundary is meaningful.
	if resetChanged {
		return res.outlier(OutlierRollover)
	}
	// No items generated: nothing to attribute cost to; not an error, just unused.
	if in.GeneratedCount <= 0 {
		res.UsedForEMA = false
		return res
	}
	// Sub-resolution: delta below the smallest observable change. Do not update;
	// accumulate generated/delta so a later observable delta yields one batched
	// average update.
	if effectiveDelta < cfg.MinObservableDeltaPct {
		newEMA := ema
		newEMA.PendingZeroGenerated += in.GeneratedCount
		newEMA.PendingZeroDelta += effectiveDelta
		r := res.outlier(OutlierZeroResolutionPend)
		r.EMA = newEMA
		return r
	}
	// Implausibly large delta: suspect user mixing or delayed accounting.
	if effectiveDelta > cfg.MaxReasonableDeltaPct {
		return res.outlier(OutlierTooLargeDelta)
	}
	// Without source attribution, a large window delta likely includes user
	// interactive consumption; exclude from EMA (the decision still reacts via
	// total used_percent, so heavy user use shrinks k automatically).
	if !in.HasAttribution {
		threshold := ema.CostPctPerItem * float64(in.GeneratedCount) * cfg.UserMixedDeltaMultiplier
		if (ema.SampleCount > 0 && windowDelta > threshold) || windowDelta > cfg.MinUserMixedDeltaPct {
			return res.outlier(OutlierUserMixedDelta)
		}
	}

	// Batched average over this sample plus any accumulated pending_zero.
	totalDelta := effectiveDelta + ema.PendingZeroDelta
	totalGen := in.GeneratedCount + ema.PendingZeroGenerated
	observedCost := totalDelta / float64(totalGen)
	if observedCost < cfg.MinCostPct || observedCost > cfg.MaxCostPct {
		return res.outlier(OutlierCostOutOfBounds)
	}

	newEMA := ema
	if ema.SampleCount == 0 {
		newEMA.CostPctPerItem = observedCost
	} else {
		a := cfg.EMAAlpha
		newEMA.CostPctPerItem = a*observedCost + (1-a)*ema.CostPctPerItem
	}
	newEMA.Alpha = cfg.EMAAlpha
	newEMA.SampleCount = ema.SampleCount + 1
	newEMA.PendingZeroGenerated = 0
	newEMA.PendingZeroDelta = 0
	if in.HasAttribution {
		newEMA.AttributionMode = AttributionSourceTag
	} else {
		newEMA.AttributionMode = AttributionWindowDelta
	}

	res.EMA = newEMA
	res.ObservedCost = observedCost
	res.UsedForEMA = true
	return res
}
