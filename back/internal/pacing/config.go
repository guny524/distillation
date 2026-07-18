package pacing

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a Go duration string ("1h",
// "30m") in YAML, since time.Duration has no native YAML decoding.
type Duration time.Duration

// UnmarshalYAML parses a duration string via time.ParseDuration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Std returns the underlying time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config holds every pacing parameter (spec sec 4). Field defaults come from
// DefaultConfig; LoadConfig overlays a YAML file over those defaults so unset
// keys keep their recommended value.
type Config struct {
	// Weekly (secondary) headroom.
	WeeklyCapPct float64 `yaml:"weekly_cap_pct"`
	// SecondaryWindowHours is the full length of the secondary (weekly) rate-limit
	// window. The worker StatusGate uses it to build the straight-line pacing
	// target (GET /quota reports resets_at but not the window length). The
	// DEPRECATED monolithic runner does not read it.
	SecondaryWindowHours float64 `yaml:"secondary_window_hours"`
	// Primary (5h) guardrail.
	PrimaryCapPct          float64 `yaml:"primary_cap_pct"`
	PrimarySafetyMarginPct float64 `yaml:"primary_safety_margin_pct"`
	// Deadline-aware horizon shaping.
	MinHorizonHours      float64 `yaml:"min_horizon_hours"`
	MaxPerHourPct        float64 `yaml:"max_per_hour_pct"`
	AccelWindowHours     float64 `yaml:"accel_window_hours"`
	AccelHorizonRatio    float64 `yaml:"accel_horizon_ratio"`
	ResetBlackoutSeconds int     `yaml:"reset_blackout_seconds"`
	// EMA cost learning.
	EMAAlpha                 float64 `yaml:"ema_alpha"`
	FallbackPrimaryCost      float64 `yaml:"fallback_primary_cost"`
	FallbackSecondaryCost    float64 `yaml:"fallback_secondary_cost"`
	MaxReasonableDeltaPct    float64 `yaml:"max_reasonable_delta_pct"`
	UserMixedDeltaMultiplier float64 `yaml:"user_mixed_delta_multiplier"`
	MinUserMixedDeltaPct     float64 `yaml:"min_user_mixed_delta_pct"`
	MinObservableDeltaPct    float64 `yaml:"min_observable_delta_pct"`
	// Cost clamp bounds (spec sec 1: EMA/fallback clamped to [min_cost, max_cost]).
	MinCostPct float64 `yaml:"min_cost_pct"`
	MaxCostPct float64 `yaml:"max_cost_pct"`
	// Per-run item ceiling + probe.
	MaxItemsPerRun                  int     `yaml:"max_items_per_run"`
	AllowOneProbeWhenBudgetPositive bool    `yaml:"allow_one_probe_when_budget_positive"`
	ProbeMinWeeklyBudgetPct         float64 `yaml:"probe_min_weekly_budget_pct"`
	ProbeMinPrimaryHeadroomPct      float64 `yaml:"probe_min_primary_headroom_pct"`
	// cli-unknown virtual budget.
	CLIUnknownMaxPerRunItems    int      `yaml:"cli_unknown_max_per_run_items"`
	CLIUnknownMaxPerHourItems   int      `yaml:"cli_unknown_max_per_hour_items"`
	CLIUnknownMaxPerWeekItems   int      `yaml:"cli_unknown_max_per_week_items"`
	CLIUnknownRateLimitCooldown Duration `yaml:"cli_unknown_rate_limit_cooldown"`
	// Staleness guard.
	AllowStaleQuota         bool `yaml:"allow_stale_quota"`
	StaleQuotaMaxAgeSeconds int  `yaml:"stale_quota_max_age_seconds"`
}

// DefaultConfig returns the recommended defaults (spec sec 4).
func DefaultConfig() Config {
	return Config{
		WeeklyCapPct:           100.0,
		SecondaryWindowHours:   168.0, // 7-day weekly window
		PrimaryCapPct:          95.0,
		PrimarySafetyMarginPct: 1.0,

		MinHorizonHours:      1.0,
		MaxPerHourPct:        10.0,
		AccelWindowHours:     24.0,
		AccelHorizonRatio:    0.5,
		ResetBlackoutSeconds: 300,

		EMAAlpha:                 0.2,
		FallbackPrimaryCost:      1.0,
		FallbackSecondaryCost:    0.5,
		MaxReasonableDeltaPct:    20.0,
		UserMixedDeltaMultiplier: 3.0,
		MinUserMixedDeltaPct:     1.0,
		MinObservableDeltaPct:    0.01,

		MinCostPct: 0.01,
		MaxCostPct: 20.0,

		MaxItemsPerRun:                  5,
		AllowOneProbeWhenBudgetPositive: true,
		ProbeMinWeeklyBudgetPct:         0.2,
		ProbeMinPrimaryHeadroomPct:      2.0,

		CLIUnknownMaxPerRunItems:    1,
		CLIUnknownMaxPerHourItems:   1,
		CLIUnknownMaxPerWeekItems:   50,
		CLIUnknownRateLimitCooldown: Duration(time.Hour),

		AllowStaleQuota:         false,
		StaleQuotaMaxAgeSeconds: 300,
	}
}

// LoadConfig reads a YAML file over DefaultConfig. Keys absent from the file
// keep their default. A missing path returns the defaults with no error.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read pacing config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("parse pacing config %s: %w", path, err)
	}
	return cfg, nil
}
