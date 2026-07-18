package pacing

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, 100.0, cfg.WeeklyCapPct)
	assert.Equal(t, 95.0, cfg.PrimaryCapPct)
	assert.Equal(t, 1.0, cfg.PrimarySafetyMarginPct)
	assert.Equal(t, 0.2, cfg.EMAAlpha)
	assert.Equal(t, 5, cfg.MaxItemsPerRun)
	assert.True(t, cfg.AllowOneProbeWhenBudgetPositive)
	assert.Equal(t, 50, cfg.CLIUnknownMaxPerWeekItems)
	assert.Equal(t, time.Hour, cfg.CLIUnknownRateLimitCooldown.Std())
	assert.False(t, cfg.AllowStaleQuota)
	assert.Equal(t, 300, cfg.StaleQuotaMaxAgeSeconds)
}

func TestLoadConfig_MissingFileReturnsDefaults(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	require.NoError(t, err)
	assert.Equal(t, DefaultConfig(), cfg)
}

func TestLoadConfig_OverlaysPartialYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pacing.yaml")
	require.NoError(t, os.WriteFile(path, []byte(
		"max_items_per_run: 12\n"+
			"weekly_cap_pct: 90.0\n"+
			"cli_unknown_rate_limit_cooldown: 30m\n"), 0o600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	// overridden
	assert.Equal(t, 12, cfg.MaxItemsPerRun)
	assert.Equal(t, 90.0, cfg.WeeklyCapPct)
	assert.Equal(t, 30*time.Minute, cfg.CLIUnknownRateLimitCooldown.Std())
	// untouched keys keep defaults
	assert.Equal(t, 0.2, cfg.EMAAlpha)
	assert.Equal(t, 95.0, cfg.PrimaryCapPct)
}

func TestLoadConfig_BadDurationErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pacing.yaml")
	require.NoError(t, os.WriteFile(path, []byte("cli_unknown_rate_limit_cooldown: notaduration\n"), 0o600))

	_, err := LoadConfig(path)
	require.Error(t, err)
}
