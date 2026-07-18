package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db/dbtest"
	"github.com/guny524/distillation/internal/pacing"
	"github.com/guny524/distillation/internal/teacher"
)

// writeTemp writes content to a temp file and returns its path.
func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

// testConfig builds a runnable config with temp prompt/taxonomy files.
func testConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Teacher.Roles = map[string]teacher.RoleConfig{
		"teacher": {
			BaseURL: "http://unused", Model: "test-model",
			SourceTag: "distillation", QuotaURL: "http://unused/quota",
		},
	}
	cfg.PromptTemplatePath = writeTemp(t, "prompt.md",
		"TAX:{{.TaxonomyYAML}}\nCOV:{{.CoverageJSON}}\nID:{{.TaskID}}")
	cfg.TaxonomyPath = writeTemp(t, "taxonomy.yaml", "domain:\n  values: {}")
	// These tests exercise the legacy taxonomy generation path.
	cfg.Pipeline.Mode = ModeTaxonomy
	return cfg
}

// emptyCoverageQuerier mocks db.Querier so coverage.TableExists returns false
// (empty coverage) and INSERTs succeed. insertCount tracks inserted rows.
func emptyCoverageQuerier(insertCount *int) *dbtest.MockQuerier {
	return &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = false
				return nil
			}}
		},
		ExecFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			if strings.HasPrefix(strings.TrimSpace(sql), "INSERT") {
				*insertCount++
				return pgconn.NewCommandTag("INSERT 0 1"), nil
			}
			return pgconn.NewCommandTag(""), nil
		},
	}
}

// newTestRunner wires a Runner with fakes and a fixed clock.
func newTestRunner(t *testing.T, cfg Config, store *fakeStore, ft *fakeTeacher, insertCount *int, now time.Time) *Runner {
	t.Helper()
	r, err := New(cfg, store, emptyCoverageQuerier(insertCount), ft)
	require.NoError(t, err)
	r.Now = func() time.Time { return now }
	r.Log = io.Discard
	return r
}

// TestRun_LockBusy: a busy advisory lock records skipped_lock_busy and never
// touches quota or the teacher.
func TestRun_LockBusy(t *testing.T) {
	now := time.Now()
	store := newFakeStore()
	store.lockAcquired = false
	ft := &fakeTeacher{}
	var inserts int
	r := newTestRunner(t, testConfig(t), store, ft, &inserts, now)

	require.NoError(t, r.Run(context.Background()))

	require.Len(t, store.insertedRunLogs, 1)
	assert.Equal(t, pacing.StatusSkippedLockBusy, store.insertedRunLogs[0].Status)
	assert.Equal(t, 0, ft.fetchCount, "quota must not be fetched without the lock")
	assert.Equal(t, 0, ft.chatCalls)
	assert.False(t, store.unlocked, "must not unlock a lock it never acquired")
	assert.Equal(t, 0, inserts)
}

// TestRun_QuotaFetchFailed: quota fetch failure yields k=0 with
// quota_fetch_failed and no teacher calls.
func TestRun_QuotaFetchFailed(t *testing.T) {
	now := time.Now()
	store := newFakeStore()
	ft := &fakeTeacher{quotaErr: errors.New("connection refused")}
	var inserts int
	r := newTestRunner(t, testConfig(t), store, ft, &inserts, now)

	require.NoError(t, r.Run(context.Background()))

	require.Len(t, store.insertedRunLogs, 1)
	assert.Equal(t, pacing.StatusQuotaFetchFailed, store.insertedRunLogs[0].Status)
	assert.Equal(t, 0, store.insertedRunLogs[0].PlannedCount)
	assert.Equal(t, 0, ft.chatCalls)
	assert.True(t, store.unlocked)
	assert.Equal(t, 0, inserts)
}

// TestRun_HappyPath: with fallback costs and healthy budgets, k=2 items are
// generated, inserted, and both windows' EMA/observations are recorded.
//
// Numbers: weekly_remaining=90, horizon=72h -> allowance=1.25%/h,
// k_weekly=floor(1.25/0.5)=2, k_primary=floor((95-10-1)/1.0)=84 -> k=2.
func TestRun_HappyPath(t *testing.T) {
	now := time.Now()
	provider := "codex"
	primaryReset := now.Add(3 * time.Hour)
	secondaryReset := now.Add(72 * time.Hour)

	before := directSnapshot(provider, 10, 10, primaryReset, secondaryReset, now, nil)
	after := directSnapshot(provider, 12.4, 11.0, primaryReset, secondaryReset, now,
		[]pacing.SourceUsage{{Source: "distillation", UsedPercentDelta: 2.4, Requests: 2}})

	store := newFakeStore()
	ft := &fakeTeacher{
		quotaBefore: snapJSON(t, before),
		quotaAfter:  snapJSON(t, after),
		outcomes: []chatOutcome{
			{content: validRecordJSON},
			{content: validRecordJSON},
		},
	}
	var inserts int
	r := newTestRunner(t, testConfig(t), store, ft, &inserts, now)

	require.NoError(t, r.Run(context.Background()))

	// Teacher was called twice with the teacher role and a rendered prompt.
	assert.Equal(t, 2, ft.chatCalls)
	assert.Equal(t, []string{"teacher", "teacher"}, ft.gotRoles)
	require.Len(t, ft.gotMsgs[0], 2)
	assert.Contains(t, ft.gotMsgs[0][1].Content, "TAX:domain:")
	assert.Contains(t, ft.gotMsgs[0][1].Content, `COV:{`)
	assert.Contains(t, ft.gotMsgs[0][1].Content, "ID:distill-")
	assert.Equal(t, 2, inserts)

	// Run log: running -> completed with generated=2 and before/after pcts.
	require.Len(t, store.insertedRunLogs, 1)
	assert.Equal(t, pacing.StatusRunning, store.insertedRunLogs[0].Status)
	assert.Equal(t, 2, store.insertedRunLogs[0].PlannedCount)
	require.Len(t, store.updatedRunLogs, 1)
	final := store.updatedRunLogs[0]
	assert.Equal(t, pacing.StatusCompleted, final.Status)
	assert.Equal(t, 2, final.GeneratedCount)
	require.NotNil(t, final.PrimaryUsedBefore)
	assert.InDelta(t, 10.0, *final.PrimaryUsedBefore, 1e-9)
	require.NotNil(t, final.SecondaryUsedAfter)
	assert.InDelta(t, 11.0, *final.SecondaryUsedAfter, 1e-9)

	// Observations: primary (source-attributed) + secondary (window delta).
	require.Len(t, store.observations, 2)
	prim, sec := store.observations[0], store.observations[1]
	assert.Equal(t, pacing.WindowPrimary, prim.WindowKind)
	assert.InDelta(t, 2.4, prim.Delta, 1e-9, "primary delta must use source attribution")
	assert.True(t, prim.UsedForEMA)
	assert.Equal(t, pacing.WindowSecondary, sec.WindowKind)
	assert.InDelta(t, 1.0, sec.Delta, 1e-9)
	assert.True(t, sec.UsedForEMA)

	// EMA learned: primary 2.4/2=1.2 (source_tag), secondary 1.0/2=0.5.
	primEMA := store.emas[pacing.EMAKey{Provider: provider, SourceTag: "distillation", WindowKind: pacing.WindowPrimary}]
	assert.InDelta(t, 1.2, primEMA.CostPctPerItem, 1e-9)
	assert.Equal(t, pacing.AttributionSourceTag, primEMA.AttributionMode)
	secEMA := store.emas[pacing.EMAKey{Provider: provider, SourceTag: "distillation", WindowKind: pacing.WindowSecondary}]
	assert.InDelta(t, 0.5, secEMA.CostPctPerItem, 1e-9)
	assert.Equal(t, pacing.AttributionWindowDelta, secEMA.AttributionMode)

	// Hourly budget: observed secondary delta charged against the allowance.
	require.Len(t, store.hourlyAdds, 1)
	hb := store.hourlyAdds[0]
	assert.Equal(t, pacing.WindowSecondary, hb.WindowKind)
	assert.InDelta(t, 1.25, hb.AllowancePct, 1e-9)
	assert.InDelta(t, 1.0, hb.ConsumedPct, 1e-9)
	assert.Equal(t, 2, hb.GeneratedCount)
	assert.Equal(t, budgetHour(now), hb.BudgetHour)

	assert.True(t, store.unlocked)
}

// TestRun_SchemaRetry: an invalid first response is re-requested once; the
// retry succeeds and both calls count toward the quota cost basis.
func TestRun_SchemaRetry(t *testing.T) {
	now := time.Now()
	provider := "codex"
	primaryReset := now.Add(3 * time.Hour)
	secondaryReset := now.Add(72 * time.Hour)

	before := directSnapshot(provider, 10, 10, primaryReset, secondaryReset, now, nil)
	after := directSnapshot(provider, 11, 10.5, primaryReset, secondaryReset, now, nil)

	cfg := testConfig(t)
	cfg.Pacing.MaxItemsPerRun = 1

	store := newFakeStore()
	ft := &fakeTeacher{
		quotaBefore: snapJSON(t, before),
		quotaAfter:  snapJSON(t, after),
		outcomes: []chatOutcome{
			{content: "sorry, I cannot produce JSON right now"},
			{content: "```json\n" + validRecordJSON + "\n```"},
		},
	}
	var inserts int
	r := newTestRunner(t, cfg, store, ft, &inserts, now)

	require.NoError(t, r.Run(context.Background()))

	assert.Equal(t, 2, ft.chatCalls, "invalid response must trigger exactly one re-request")
	assert.Equal(t, 1, inserts)
	require.Len(t, store.updatedRunLogs, 1)
	assert.Equal(t, pacing.StatusCompleted, store.updatedRunLogs[0].Status)
	assert.Equal(t, 1, store.updatedRunLogs[0].GeneratedCount)
	// Cost basis counts both teacher calls.
	require.Len(t, store.observations, 2)
	assert.Equal(t, 2, store.observations[0].GeneratedCount)
}

// TestRun_SchemaInvalidExhausted: when retries never yield a valid object the
// item is skipped and the run still completes.
func TestRun_SchemaInvalidExhausted(t *testing.T) {
	now := time.Now()
	provider := "codex"
	before := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)
	after := directSnapshot(provider, 10.5, 10.2, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)

	cfg := testConfig(t)
	cfg.Pacing.MaxItemsPerRun = 1

	store := newFakeStore()
	ft := &fakeTeacher{
		quotaBefore: snapJSON(t, before),
		quotaAfter:  snapJSON(t, after),
		outcomes: []chatOutcome{
			{content: "no json here"},
			{content: "still no json"},
		},
	}
	var inserts int
	r := newTestRunner(t, cfg, store, ft, &inserts, now)

	require.NoError(t, r.Run(context.Background()))

	assert.Equal(t, 2, ft.chatCalls)
	assert.Equal(t, 0, inserts)
	require.Len(t, store.updatedRunLogs, 1)
	assert.Equal(t, pacing.StatusCompleted, store.updatedRunLogs[0].Status)
	assert.Equal(t, 0, store.updatedRunLogs[0].GeneratedCount)
}

// TestRun_RateLimited: an ErrRateLimited teacher failure marks the run
// rate_limited (feeding the pacing cooldown) and returns nil.
func TestRun_RateLimited(t *testing.T) {
	now := time.Now()
	provider := "codex"
	before := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)
	after := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)

	store := newFakeStore()
	ft := &fakeTeacher{
		quotaBefore: snapJSON(t, before),
		quotaAfter:  snapJSON(t, after),
		outcomes: []chatOutcome{
			{err: fmt.Errorf("teacher: %w", teacher.ErrRateLimited)},
		},
	}
	var inserts int
	r := newTestRunner(t, testConfig(t), store, ft, &inserts, now)

	require.NoError(t, r.Run(context.Background()))

	assert.Equal(t, 1, ft.chatCalls)
	assert.Equal(t, 0, inserts)
	require.Len(t, store.updatedRunLogs, 1)
	assert.Equal(t, pacing.StatusRateLimited, store.updatedRunLogs[0].Status)
	assert.Equal(t, 0, store.updatedRunLogs[0].GeneratedCount)
	assert.True(t, store.unlocked)
}

// TestRun_TransportError: a non-rate-limit teacher failure aborts the loop,
// records the error message, and propagates the error (visible pod failure).
func TestRun_TransportError(t *testing.T) {
	now := time.Now()
	provider := "codex"
	before := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)
	after := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)

	store := newFakeStore()
	ft := &fakeTeacher{
		quotaBefore: snapJSON(t, before),
		quotaAfter:  snapJSON(t, after),
		outcomes: []chatOutcome{
			{err: errors.New("teacher: POST http://unused: dial tcp: no route to host")},
		},
	}
	var inserts int
	r := newTestRunner(t, testConfig(t), store, ft, &inserts, now)

	err := r.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no route to host")
	require.Len(t, store.updatedRunLogs, 1)
	assert.Equal(t, pacing.StatusCompleted, store.updatedRunLogs[0].Status)
	assert.Contains(t, store.updatedRunLogs[0].ErrorMsg, "no route to host")
	assert.True(t, store.unlocked)
}

// TestRun_TaskIDsUniquePerItem: k=2 items in the same run get distinct task ids.
func TestRun_TaskIDsUniquePerItem(t *testing.T) {
	now := time.Now()
	provider := "codex"
	before := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)
	after := directSnapshot(provider, 11, 11, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)

	store := newFakeStore()
	ft := &fakeTeacher{
		quotaBefore: snapJSON(t, before),
		quotaAfter:  snapJSON(t, after),
		outcomes:    []chatOutcome{{content: validRecordJSON}, {content: validRecordJSON}},
	}
	var inserts int
	r := newTestRunner(t, testConfig(t), store, ft, &inserts, now)

	require.NoError(t, r.Run(context.Background()))
	require.Equal(t, 2, ft.chatCalls)

	id0 := taskIDFromPrompt(t, ft.gotMsgs[0][1].Content)
	id1 := taskIDFromPrompt(t, ft.gotMsgs[1][1].Content)
	assert.NotEqual(t, id0, id1)
	assert.True(t, strings.HasPrefix(id0, "distill-"))
}

func taskIDFromPrompt(t *testing.T, prompt string) string {
	t.Helper()
	idx := strings.LastIndex(prompt, "ID:")
	require.GreaterOrEqual(t, idx, 0)
	return strings.TrimSpace(prompt[idx+len("ID:"):])
}
