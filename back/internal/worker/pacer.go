package worker

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/guny524/distillation/internal/pacing"
)

// Gate is the per-step quota gate the always-on workers consult before spending a
// subscription role's quota (todos sec 6-3-4, the stateless redesign). Allow reads
// the /quota FACTS for the step's subgate roles and reports whether the step may
// claim + process an item right now; false means the subscription is spent (or the
// facts are unreadable, or a 429 cooldown is active) so the step skips this pass.
// NoteRateLimited arms a PROCESS-LOCAL cooldown after an observed 429 -- the only
// backpressure a cli transport (which exposes no rate-limit facts) has.
//
// The gate holds NO distillation-side quota state: no reservation ledger, no EMA,
// no advisory lock, nothing written to the DB. The single source of truth for
// remaining quota is subgate's GET /quota. The cli-unknown cooldown is the sole
// in-memory state and is never persisted (todos sec 6-3-4: "in-memory 쿨다운,
// DB 기록 금지").
type Gate interface {
	Allow(ctx context.Context) bool
	NoteRateLimited()
}

// QuotaCheck is one (role, provider, quota_url) a step's gate must clear before
// spending that role's subscription. A step gates on one check per subgate role it
// uses (comprehend->opencode provider, question->generator, presolve->judge,
// answer->teacher+translator, verify->verifier). A role WITHOUT a quota_url is a
// real API / local model and yields NO check -- it is used without a quota read,
// error-driven only (todos sec 6-3-4: "quota_url이 없으면 확인 없이 쓰고 에러-driven").
type QuotaCheck struct {
	// Role is the pipeline role this check guards (for wiring visibility + logs).
	Role string
	// Provider is the entry to find in the /quota snapshot's providers[].
	Provider string
	// URL is the quota_url to GET; checks sharing a URL fetch it once per Allow.
	URL string
}

// QuotaFetcher GETs a quota URL and returns the raw body (parsed by the gate with
// pacing.ParseSnapshot). *teacher.Client implements it via FetchQuotaURL.
type QuotaFetcher interface {
	FetchQuotaURL(ctx context.Context, url string) ([]byte, error)
}

// StatusGate is the production Gate: a STATELESS quota gate over subgate's GET
// /quota facts (todos sec 6-3-4). Before a step claims an item it reads the facts
// for every subgate role the step uses and decides, from those facts alone, whether
// the subscription is spent -- no reservation, no EMA, no advisory lock, no DB.
//
// Judgment per provider (facts only):
//   - direct (both windows known): the secondary (weekly) window is paced on a
//     straight line -- skip once used% reaches the cap scaled by how far into the
//     window we are; the primary (5h) window is a hard ceiling -- skip once used% is
//     within the safety margin of the cap. Either trips -> skip.
//   - cli-unknown (used% null, cli transport): no fact to check, so the gate opens
//     and 429-driven backpressure governs. An observed 429 (NoteRateLimited) arms a
//     process-local cooldown during which the gate skips.
//   - fetch/parse failure, or the provider absent from /quota: conservative skip
//     (an unknown quota state means do not spend; if subgate is down, /v1 would fail
//     anyway).
//
// The only in-memory state is the cli-unknown cooldown (a process-local until
// timestamp per provider), never persisted.
type StatusGate struct {
	fetcher QuotaFetcher
	checks  []QuotaCheck
	cfg     pacing.Config
	now     func() time.Time
	// Log receives gate skip/fetch warnings (stderr in production, nil in tests).
	Log io.Writer

	mu            sync.Mutex
	cooldownUntil map[string]time.Time // provider -> cli-unknown cooldown end
}

// NewStatusGate wires the gate with the /quota fetcher, the step's checks, and the
// pacing config. Cap / safety margin / weekly-window length / cli cooldown are all
// REUSED from the existing pacing config (todos sec 6-3-4: "상한/마진 값은 기존
// pacing config 재사용", "쿨다운 길이는 기존 cli 쿨다운 값 재사용").
func NewStatusGate(fetcher QuotaFetcher, checks []QuotaCheck, cfg pacing.Config) *StatusGate {
	return &StatusGate{
		fetcher:       fetcher,
		checks:        checks,
		cfg:           cfg,
		now:           time.Now,
		cooldownUntil: make(map[string]time.Time),
	}
}

// Compile-time check: StatusGate satisfies Gate.
var _ Gate = (*StatusGate)(nil)

// Allow reads /quota for every distinct check URL and reports whether EVERY subgate
// role the step uses is within budget. A step with no checks (every role is a real
// API / local model) always proceeds. Any exhausted provider, unreachable /quota,
// or active cooldown closes the gate (skip this pass). Checks sharing a quota_url
// fetch it once (todos sec 6-3-4: "같은 quota_url이 중복이면 1회만 fetch").
func (g *StatusGate) Allow(ctx context.Context) bool {
	if len(g.checks) == 0 {
		return true // no subgate role to check -> error-driven only
	}
	now := g.now()
	snaps := make(map[string]*pacing.QuotaSnapshot, len(g.checks))
	for _, chk := range g.checks {
		snap, fetched := snaps[chk.URL]
		if !fetched {
			snap = g.fetch(ctx, chk.URL) // nil on fetch/parse failure
			snaps[chk.URL] = snap
		}
		if snap == nil {
			// Unknown quota state -> conservatively skip (todos sec 6-3-4: "quota
			// 상태를 모르면 구독 안 씀. subgate가 죽었으면 /v1도 어차피 실패").
			logf(g.Log, "[gate] role %q: /quota unreadable, skipping step", chk.Role)
			return false
		}
		if g.exhausted(*snap, chk.Provider, now) {
			logf(g.Log, "[gate] role %q (provider %q): subscription exhausted, skipping step", chk.Role, chk.Provider)
			return false
		}
	}
	return true
}

// NoteRateLimited arms a process-local cooldown for every provider the step checks,
// after an observed 429 (teacher.ErrRateLimited). During the cooldown Allow skips
// cli-unknown providers -- the only backpressure a cli transport has. The cooldown
// is consulted ONLY for cli-unknown providers (see exhausted), so arming it for a
// direct provider, whose facts govern, is harmless. The cooldown length reuses the
// pacing config's CLIUnknownRateLimitCooldown (default 1h).
func (g *StatusGate) NoteRateLimited() {
	until := g.now().Add(g.cfg.CLIUnknownRateLimitCooldown.Std())
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, chk := range g.checks {
		g.cooldownUntil[chk.Provider] = until
	}
}

// exhausted decides, from the /quota facts alone, whether provider's subscription
// is spent. A provider absent from the snapshot cannot be confirmed to have
// headroom, so it is treated as exhausted (conservative, same as a fetch failure).
func (g *StatusGate) exhausted(snap pacing.QuotaSnapshot, provider string, now time.Time) bool {
	ps, ok := snap.Provider(provider)
	if !ok {
		logf(g.Log, "[gate] provider %q absent from /quota, treating as exhausted", provider)
		return true
	}
	if ps.Mode() == pacing.ModeCLIUnknown {
		// cli transport: no rate-limit facts to read, so the gate opens unless a
		// recent 429 armed the cooldown (error-driven backpressure).
		return g.inCooldown(provider, now)
	}
	return g.directExhausted(ps, now)
}

// directExhausted applies the two stateless formulas to a direct-transport provider
// (both windows known):
//
//   - primary (5h) HARD CEILING: skip once used% is within the safety margin of the
//     cap. No time distribution -- the short window is a guardrail, not a budget to
//     pace out (todos sec 6-3-4: "primary(5h)는 배분 없이 상한만").
//
//   - secondary (weekly) STRAIGHT-LINE PACE: skip once used% has reached the cap
//     scaled by how far into the window we are (elapsed fraction from resets_at +
//     the window length). This is the stateless, long-run EQUIVALENT of "spread the
//     remaining weekly budget over the remaining time" (todos sec 2-4): sitting
//     exactly on the line means the implied hourly allowance
//     (cap-used)/(hours-left) stays constant at cap/period, so gating "used >= line"
//     is the same pace decision, recomputed from facts each call with zero state.
func (g *StatusGate) directExhausted(ps pacing.ProviderSnapshot, now time.Time) bool {
	if ps.Primary.Known() && *ps.Primary.UsedPercent >= g.cfg.PrimaryCapPct-g.cfg.PrimarySafetyMarginPct {
		return true
	}
	if ps.Secondary.Known() {
		target := g.cfg.WeeklyCapPct * elapsedRatio(now, ps.Secondary.ResetsAt, g.secondaryPeriod())
		if *ps.Secondary.UsedPercent >= target {
			return true
		}
	}
	return false
}

// secondaryPeriod is the full length of the weekly (secondary) window, used to turn
// resets_at into an elapsed fraction. Reuses the existing pacing config field
// (default 168h = 7d); GET /quota reports resets_at but not the window length.
func (g *StatusGate) secondaryPeriod() time.Duration {
	return time.Duration(g.cfg.SecondaryWindowHours * float64(time.Hour))
}

// inCooldown reports whether provider is inside its process-local 429 cooldown.
func (g *StatusGate) inCooldown(provider string, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	until, ok := g.cooldownUntil[provider]
	return ok && now.Before(until)
}

// fetch GETs and parses /quota; any failure returns nil, which Allow maps to a
// conservative skip.
func (g *StatusGate) fetch(ctx context.Context, url string) *pacing.QuotaSnapshot {
	b, err := g.fetcher.FetchQuotaURL(ctx, url)
	if err != nil {
		logf(g.Log, "[gate] quota fetch (%s) failed: %v", url, err)
		return nil
	}
	snap, err := pacing.ParseSnapshot(b)
	if err != nil {
		logf(g.Log, "[gate] quota parse (%s) failed: %v", url, err)
		return nil
	}
	return &snap
}

// elapsedRatio is how far into a window of length period we are, measured back from
// its end (resets_at): 0 at the window start, 1 at reset. Clamped to [0,1] against
// clock skew / a stale snapshot. A degenerate period (<=0) reads as fully elapsed
// (the most conservative reading), whose target 0 makes the straight-line gate skip
// -- self-healing once a real resets_at/period is observed. At the exact reset
// instant (ratio 0, used 0) the gate momentarily skips a just-reset window; the
// next poll (ratio > 0) opens it, so the block is self-healing and measure-zero.
func elapsedRatio(now, resetsAt time.Time, period time.Duration) float64 {
	if period <= 0 {
		return 1
	}
	r := 1 - resetsAt.Sub(now).Seconds()/period.Seconds()
	if r < 0 {
		return 0
	}
	if r > 1 {
		return 1
	}
	return r
}
