package pacing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const directBody = `{
  "providers": [
    {
      "provider": "codex",
      "primary": {"used_percent": 12.5, "resets_at": "2026-07-11T15:00:00Z"},
      "secondary": {"used_percent": 40.0, "resets_at": "2026-07-18T00:00:00Z"}
    }
  ],
  "pipeline": [
    {"source": "distill-teacher", "used_percent_delta": 3.5, "requests": 7}
  ],
  "observed_at": "2026-07-11T12:00:00Z"
}`

func TestParseSnapshot_Direct(t *testing.T) {
	s, err := ParseSnapshot([]byte(directBody))
	require.NoError(t, err)

	p, ok := s.Provider("codex")
	require.True(t, ok)
	assert.Equal(t, ModeDirect, p.Mode())
	require.NotNil(t, p.Primary.UsedPercent)
	assert.Equal(t, 12.5, *p.Primary.UsedPercent)
	assert.Equal(t, 40.0, *p.Secondary.UsedPercent)

	delta, ok := s.SourceDelta("distill-teacher")
	require.True(t, ok)
	assert.Equal(t, 3.5, delta)

	_, ok = s.Provider("missing")
	assert.False(t, ok)
	_, ok = s.SourceDelta("missing")
	assert.False(t, ok)
}

func TestParseSnapshot_CLIUnknownNullUsedPercent(t *testing.T) {
	body := `{
      "providers": [
        {
          "provider": "claude",
          "primary": {"used_percent": null, "resets_at": "0001-01-01T00:00:00Z"},
          "secondary": {"used_percent": null, "resets_at": "0001-01-01T00:00:00Z"}
        }
      ],
      "pipeline": [],
      "observed_at": "2026-07-11T12:00:00Z"
    }`

	s, err := ParseSnapshot([]byte(body))
	require.NoError(t, err)

	p, ok := s.Provider("claude")
	require.True(t, ok)
	assert.Nil(t, p.Primary.UsedPercent)
	assert.False(t, p.Primary.Known())
	assert.Equal(t, ModeCLIUnknown, p.Mode())
}

func TestParseSnapshot_Invalid(t *testing.T) {
	_, err := ParseSnapshot([]byte("not json"))
	require.Error(t, err)
}
