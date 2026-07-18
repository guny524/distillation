package pacing

import (
	"context"
	"math"
	"time"
)

// Decision is the output of Decide: how many items to generate this run (K) plus
// the status and the intermediate quantities used to reach it (for run logging).
type Decision struct {
	K      int
	Status string
	Mode   string

	PrimaryUsedPct              float64
	SecondaryUsedPct            float64
	WeeklyRemainingPct          float64
	PrimaryHeadroomPct          float64
	HourlyAllowancePct          float64
	RemainingHourlyAllowancePct float64
	CostPrimaryPct              float64
	CostSecondaryPct            float64
	Probe                       bool
}

// Decide computes how many teacher generations to launch this run for
// (provider, sourceTag), given a quota snapshot already fetched under the
// advisory lock. The caller must acquire the lock first: on lock contention it
// records k=0 / StatusSkippedLockBusy without ever calling Decide (spec sec 1).
//
// The returned k is an upper bound for the run; the orchestration layer
// (internal/runner) decrements the hourly/virtual budgets as it actually
// generates.
func Decide(
	ctx context.Context,
	snap *QuotaSnapshot,
	provider, sourceTag string,
	now time.Time,
	cfg Config,
	r StateReader,
) (Decision, error) {
	// Whole-snapshot fetch failure.
	if snap == nil {
		return Decision{Status: StatusQuotaFetchFailed}, nil
	}
	// Staleness guard: a snapshot older than the max age is treated as a fetch
	// failure rather than acted on (spec sec 4: allow_stale_quota=false).
	if !cfg.AllowStaleQuota && cfg.StaleQuotaMaxAgeSeconds > 0 && !snap.ObservedAt.IsZero() {
		maxAge := time.Duration(cfg.StaleQuotaMaxAgeSeconds) * time.Second
		if now.Sub(snap.ObservedAt) > maxAge {
			return Decision{Status: StatusQuotaFetchFailed}, nil
		}
	}
	ps, ok := snap.Provider(provider)
	if !ok {
		return Decision{Status: StatusQuotaMissing}, nil
	}
	if ps.Mode() == ModeCLIUnknown {
		return decideCLIUnknown(ctx, provider, sourceTag, now, cfg, r)
	}
	return decideDirect(ctx, ps, provider, sourceTag, now, cfg, r)
}

// decideDirect handles the direct-transport branch where both rate-limit windows
// carry real used_percent (spec sec 1).
func decideDirect(
	ctx context.Context,
	ps ProviderSnapshot,
	provider, sourceTag string,
	now time.Time,
	cfg Config,
	r StateReader,
) (Decision, error) {
	primaryUsed := *ps.Primary.UsedPercent
	secondaryUsed := *ps.Secondary.UsedPercent
	d := Decision{
		Mode:             ModeDirect,
		PrimaryUsedPct:   primaryUsed,
		SecondaryUsedPct: secondaryUsed,
	}

	// Reset boundary: a window already at/after its reset is inconsistent with a
	// live decision; skip this run.
	if !ps.Primary.ResetsAt.After(now) || !ps.Secondary.ResetsAt.After(now) {
		d.Status = StatusResetBoundarySkip
		return d, nil
	}
	// Reset blackout: too close to a reset, delta accounting is unreliable.
	minReset := ps.Secondary.ResetsAt
	if ps.Primary.ResetsAt.Before(minReset) {
		minReset = ps.Primary.ResetsAt
	}
	if minReset.Sub(now).Seconds() < float64(cfg.ResetBlackoutSeconds) {
		d.Status = StatusResetBlackout
		return d, nil
	}

	weeklyRemaining := math.Max(0, cfg.WeeklyCapPct-secondaryUsed)
	primaryHeadroom := math.Max(0, cfg.PrimaryCapPct-primaryUsed-cfg.PrimarySafetyMarginPct)
	d.WeeklyRemainingPct = weeklyRemaining
	d.PrimaryHeadroomPct = primaryHeadroom
	if weeklyRemaining <= 0 || primaryHeadroom <= 0 {
		d.Status = StatusNoQuotaHeadroom
		return d, nil
	}

	// Deadline-aware horizon: inside the acceleration window, compress the
	// horizon to burn faster before the weekly reset (still primary-capped).
	hoursLeft := ps.Secondary.ResetsAt.Sub(now).Hours()
	horizon := hoursLeft
	if hoursLeft <= cfg.AccelWindowHours {
		horizon = hoursLeft * cfg.AccelHorizonRatio
	}
	horizon = math.Max(cfg.MinHorizonHours, horizon)

	allowance := clampFloat(weeklyRemaining/horizon, 0, cfg.MaxPerHourPct)
	d.HourlyAllowancePct = allowance

	// Subtract what this hour already spent (leaky bucket).
	consumed := 0.0
	hb, ok, err := r.GetHourlyBudget(ctx, HourlyKey{
		Provider: provider, SourceTag: sourceTag,
		WindowKind: WindowSecondary, BudgetHour: budgetHour(now),
	})
	if err != nil {
		return d, err
	}
	if ok {
		consumed = hb.ConsumedPct
	}
	remainingHourly := math.Max(0, allowance-consumed)
	d.RemainingHourlyAllowancePct = remainingHourly

	costPrimary, err := costFromEMA(ctx, r, EMAKey{provider, sourceTag, WindowPrimary}, cfg.FallbackPrimaryCost, cfg)
	if err != nil {
		return d, err
	}
	costSecondary, err := costFromEMA(ctx, r, EMAKey{provider, sourceTag, WindowSecondary}, cfg.FallbackSecondaryCost, cfg)
	if err != nil {
		return d, err
	}
	d.CostPrimaryPct = costPrimary
	d.CostSecondaryPct = costSecondary

	kWeekly := int(math.Floor(remainingHourly / costSecondary))
	kPrimary := int(math.Floor(primaryHeadroom / costPrimary))
	k := clampInt(minInt(kWeekly, kPrimary), 0, cfg.MaxItemsPerRun)

	switch {
	case remainingHourly <= 0:
		d.Status = StatusHourlyBudgetExhausted
	case k <= 0:
		d.Status = StatusNoQuotaHeadroom
	default:
		d.Status = StatusDecided
	}

	// Probe: EMA can be over-conservative early on. When both budgets are healthy
	// but k rounded to 0, allow a single probe generation to gather a sample.
	if k <= 0 && cfg.AllowOneProbeWhenBudgetPositive && remainingHourly > 0 &&
		weeklyRemaining >= cfg.ProbeMinWeeklyBudgetPct &&
		primaryHeadroom >= cfg.ProbeMinPrimaryHeadroomPct {
		k = 1
		d.Probe = true
		d.Status = StatusDecided
	}

	d.K = k
	return d, nil
}

// decideCLIUnknown handles the cli-unknown branch: no /quota-based catch-up, only
// fixed virtual item budgets, and a cooldown after a rate-limit error (spec sec 1).
func decideCLIUnknown(
	ctx context.Context,
	provider, sourceTag string,
	now time.Time,
	cfg Config,
	r StateReader,
) (Decision, error) {
	d := Decision{Mode: ModeCLIUnknown}

	if t, ok, err := r.LastRateLimitedAt(ctx, provider, sourceTag); err != nil {
		return d, err
	} else if ok && now.Sub(t) < cfg.CLIUnknownRateLimitCooldown.Std() {
		d.Status = StatusUnknownQuotaCooldown
		return d, nil
	}

	perHour, err := r.CountGeneratedSince(ctx, provider, sourceTag, budgetHour(now))
	if err != nil {
		return d, err
	}
	perWeek, err := r.CountGeneratedSince(ctx, provider, sourceTag, now.Add(-7*24*time.Hour))
	if err != nil {
		return d, err
	}

	k := cfg.CLIUnknownMaxPerRunItems
	k = minInt(k, cfg.CLIUnknownMaxPerHourItems-perHour)
	k = minInt(k, cfg.CLIUnknownMaxPerWeekItems-perWeek)
	if k < 0 {
		k = 0
	}
	if k <= 0 {
		d.Status = StatusUnknownVirtualBudgetExhaust
	} else {
		d.Status = StatusDecided
	}
	d.K = k
	return d, nil
}

// costFromEMA returns the clamped per-item cost for key: the learned EMA when a
// usable sample exists, else the fallback. Always > 0 so callers can divide.
func costFromEMA(ctx context.Context, r StateReader, key EMAKey, fallback float64, cfg Config) (float64, error) {
	cost := fallback
	e, ok, err := r.GetEMA(ctx, key)
	if err != nil {
		return 0, err
	}
	if ok && e.SampleCount > 0 && e.CostPctPerItem > 0 {
		cost = e.CostPctPerItem
	}
	cost = clampFloat(cost, cfg.MinCostPct, cfg.MaxCostPct)
	if cost <= 0 {
		cost = cfg.MaxCostPct // guard against a zero/negative clamp floor
	}
	return cost, nil
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
