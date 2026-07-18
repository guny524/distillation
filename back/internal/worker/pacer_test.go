package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/guny524/distillation/internal/pacing"
)

// gateNow is the fixed clock for the gate tests.
var gateNow = time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)

// quotaURL is the single subgate /quota URL every check shares in these tests.
const quotaURL = "http://subgate/quota"

// --- fake URL-based QuotaFetcher --------------------------------------------

// fakeFetcher returns a canned /quota body (or per-URL bodies) and counts calls so
// a test can assert fetch dedup.
type fakeFetcher struct {
	body  []byte
	byURL map[string][]byte
	err   error
	calls int
}

func (f *fakeFetcher) FetchQuotaURL(_ context.Context, url string) ([]byte, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if b, ok := f.byURL[url]; ok {
		return b, nil
	}
	return f.body, nil
}

// directBody builds a healthy direct-transport /quota body (both windows known).
// The secondary window's resets_at is set so elapsedRatio (over the default 168h
// week) is `secondaryElapsed`; the primary resets_at is irrelevant to the cap check.
func directBody(primaryUsed, secondaryUsed, secondaryElapsed float64) []byte {
	period := 168 * time.Hour
	remaining := time.Duration((1 - secondaryElapsed) * float64(period))
	snap := pacing.QuotaSnapshot{
		Providers: []pacing.ProviderSnapshot{{
			Provider:  "codex",
			Primary:   pacing.Window{UsedPercent: &primaryUsed, ResetsAt: gateNow.Add(3 * time.Hour)},
			Secondary: pacing.Window{UsedPercent: &secondaryUsed, ResetsAt: gateNow.Add(remaining)},
		}},
	}
	b, _ := json.Marshal(snap)
	return b
}

// cliUnknownBody builds a cli-unknown /quota body (null used_percent -> the
// transport cannot observe rate-limit state).
func cliUnknownBody() []byte {
	snap := pacing.QuotaSnapshot{Providers: []pacing.ProviderSnapshot{{Provider: "codex"}}}
	b, _ := json.Marshal(snap)
	return b
}

// gateWith builds a StatusGate over one codex check at quotaURL, with the fixed
// clock and default pacing config (weekly_cap 100, primary_cap 95, margin 1,
// window 168h, cooldown 1h).
func gateWith(fetcher QuotaFetcher) *StatusGate {
	g := NewStatusGate(fetcher, []QuotaCheck{{Role: "teacher", Provider: "codex", URL: quotaURL}}, pacing.DefaultConfig())
	g.now = func() time.Time { return gateNow }
	return g
}

// TestStatusGate_SecondaryBelowTargetAllows: at 50% into the week the straight-line
// target is 50%; a secondary used% below it opens the gate (todos sec 6-3-4).
func TestStatusGate_SecondaryBelowTargetAllows(t *testing.T) {
	g := gateWith(&fakeFetcher{body: directBody(10, 49, 0.5)})
	assert.True(t, g.Allow(context.Background()), "used 49% < target 50% -> pass")
}

// TestStatusGate_SecondaryOverTargetSkips: secondary used% past the straight-line
// target closes the gate (ahead of the weekly pace).
func TestStatusGate_SecondaryOverTargetSkips(t *testing.T) {
	g := gateWith(&fakeFetcher{body: directBody(10, 51, 0.5)})
	assert.False(t, g.Allow(context.Background()), "used 51% > target 50% -> skip")
}

// TestStatusGate_SecondaryBoundarySkips: exactly at the target skips (the gate uses
// used% >= target).
func TestStatusGate_SecondaryBoundarySkips(t *testing.T) {
	g := gateWith(&fakeFetcher{body: directBody(10, 50, 0.5)})
	assert.False(t, g.Allow(context.Background()), "used 50% == target 50% -> skip (>=)")
}

// TestStatusGate_PrimaryCeiling: the primary (5h) window is a hard ceiling at
// cap-margin (95-1=94), independent of any time distribution. Below it opens; at/
// above it closes, even with the secondary window wide open.
func TestStatusGate_PrimaryCeiling(t *testing.T) {
	// secondaryElapsed 0.5 -> target 50; secondaryUsed 0 keeps secondary open so the
	// PRIMARY decision is isolated.
	allow := gateWith(&fakeFetcher{body: directBody(93, 0, 0.5)})
	assert.True(t, allow.Allow(context.Background()), "primary 93% < 94% ceiling -> pass")

	skip := gateWith(&fakeFetcher{body: directBody(94, 0, 0.5)})
	assert.False(t, skip.Allow(context.Background()), "primary 94% == ceiling -> skip")

	over := gateWith(&fakeFetcher{body: directBody(96, 0, 0.5)})
	assert.False(t, over.Allow(context.Background()), "primary 96% > ceiling -> skip")
}

// TestStatusGate_CLIUnknownAllows: a cli-unknown provider exposes no rate-limit
// facts, so the gate opens (error-driven backpressure governs) -- todos sec 6-3-4.
func TestStatusGate_CLIUnknownAllows(t *testing.T) {
	g := gateWith(&fakeFetcher{body: cliUnknownBody()})
	assert.True(t, g.Allow(context.Background()), "cli-unknown -> gate opens (429-driven)")
}

// TestStatusGate_NoChecksAllowsWithoutFetch: a step whose roles all lack a quota_url
// has no checks, so the gate opens with NO fetch (real-API / local, error-driven).
func TestStatusGate_NoChecksAllowsWithoutFetch(t *testing.T) {
	f := &fakeFetcher{body: directBody(99, 99, 1)}
	g := NewStatusGate(f, nil, pacing.DefaultConfig())
	g.now = func() time.Time { return gateNow }
	assert.True(t, g.Allow(context.Background()), "no checks -> always allow")
	assert.Equal(t, 0, f.calls, "no quota_url -> the gate never reads /quota")
}

// TestStatusGate_FetchFailureSkips: an unreachable /quota is a conservative skip --
// an unknown quota state means don't spend (todos sec 6-3-4).
func TestStatusGate_FetchFailureSkips(t *testing.T) {
	g := gateWith(&fakeFetcher{err: errors.New("connection refused")})
	assert.False(t, g.Allow(context.Background()), "fetch failure -> conservative skip")
}

// TestStatusGate_ParseFailureSkips: a /quota body that does not parse is likewise a
// conservative skip.
func TestStatusGate_ParseFailureSkips(t *testing.T) {
	g := gateWith(&fakeFetcher{body: []byte("not json")})
	assert.False(t, g.Allow(context.Background()), "parse failure -> conservative skip")
}

// TestStatusGate_ProviderAbsentSkips: a snapshot missing the checked provider cannot
// confirm headroom, so the gate skips (same rationale as a fetch failure).
func TestStatusGate_ProviderAbsentSkips(t *testing.T) {
	snap := pacing.QuotaSnapshot{Providers: []pacing.ProviderSnapshot{{Provider: "other"}}}
	b, _ := json.Marshal(snap)
	g := gateWith(&fakeFetcher{body: b})
	assert.False(t, g.Allow(context.Background()), "checked provider absent -> skip")
}

// TestStatusGate_RateLimitCooldownSkipsUnknown (todos sec 6-3-4): a cli-unknown gate
// opens until an observed 429 arms the PROCESS-LOCAL cooldown, after which it skips
// for the cooldown window, then re-opens once the window elapses. Nothing is
// persisted.
func TestStatusGate_RateLimitCooldownSkipsUnknown(t *testing.T) {
	now := gateNow
	g := gateWith(&fakeFetcher{body: cliUnknownBody()})
	g.now = func() time.Time { return now }

	assert.True(t, g.Allow(context.Background()), "before any 429 the cli gate is open")

	g.NoteRateLimited() // observed a 429
	assert.False(t, g.Allow(context.Background()), "within the cooldown the gate skips")

	now = gateNow.Add(2 * time.Hour) // past the 1h default cooldown
	assert.True(t, g.Allow(context.Background()), "after the cooldown elapses the gate re-opens")
}

// TestStatusGate_CooldownIgnoredForDirect: the cooldown gates only cli-unknown
// providers. A direct provider with healthy facts stays open even after a 429 armed
// its cooldown -- the facts govern, so a spurious cooldown never wrongly skips it.
func TestStatusGate_CooldownIgnoredForDirect(t *testing.T) {
	g := gateWith(&fakeFetcher{body: directBody(10, 10, 0.5)})
	g.NoteRateLimited()
	assert.True(t, g.Allow(context.Background()), "direct provider ignores cooldown; facts govern")
}

// TestStatusGate_DedupSharedURL: a step whose roles share one quota_url (answer ->
// teacher + translator, both subgate) fetches /quota ONCE per Allow (todos sec
// 6-3-4: "같은 quota_url이 중복이면 1회만 fetch").
func TestStatusGate_DedupSharedURL(t *testing.T) {
	f := &fakeFetcher{body: directBody(10, 10, 0.5)}
	g := NewStatusGate(f, []QuotaCheck{
		{Role: "teacher", Provider: "codex", URL: quotaURL},
		{Role: "translator", Provider: "codex", URL: quotaURL},
	}, pacing.DefaultConfig())
	g.now = func() time.Time { return gateNow }

	assert.True(t, g.Allow(context.Background()))
	assert.Equal(t, 1, f.calls, "two checks sharing one quota_url fetch it once")
}

// TestStatusGate_AnyExhaustedRoleSkips: when a step's roles map to DIFFERENT quota
// URLs and one provider is exhausted, the whole step skips (it needs all roles).
func TestStatusGate_AnyExhaustedRoleSkips(t *testing.T) {
	f := &fakeFetcher{byURL: map[string][]byte{
		"http://a/quota": directBody(10, 10, 0.5),  // healthy
		"http://b/quota": directBody(96, 10, 0.5),  // primary over the ceiling
	}}
	g := NewStatusGate(f, []QuotaCheck{
		{Role: "teacher", Provider: "codex", URL: "http://a/quota"},
		{Role: "translator", Provider: "codex", URL: "http://b/quota"},
	}, pacing.DefaultConfig())
	g.now = func() time.Time { return gateNow }
	assert.False(t, g.Allow(context.Background()), "one exhausted role closes the whole step")
}

// TestElapsedRatio clamps to [0,1] and reads a degenerate period as fully elapsed.
func TestElapsedRatio(t *testing.T) {
	period := 168 * time.Hour
	assert.InDelta(t, 0.5, elapsedRatio(gateNow, gateNow.Add(84*time.Hour), period), 1e-9)
	assert.Equal(t, 0.0, elapsedRatio(gateNow, gateNow.Add(200*time.Hour), period), "reset beyond a full period -> clamp 0")
	assert.Equal(t, 1.0, elapsedRatio(gateNow, gateNow.Add(-time.Hour), period), "reset in the past -> clamp 1")
	assert.Equal(t, 1.0, elapsedRatio(gateNow, gateNow.Add(time.Hour), 0), "degenerate period -> fully elapsed")
}

// Compile-time: the fake fetcher satisfies the gate's fetch surface.
var _ QuotaFetcher = (*fakeFetcher)(nil)
