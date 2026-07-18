package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/db/dbtest"
)

// Compile-time: MockQuerier is a valid db.Querier for the Store.
var _ db.Querier = (*dbtest.MockQuerier)(nil)

// stagePrevCur is the expected (prev-columns, done-column) mapping. It pins the
// state machine so a wrong edge (claiming the wrong stage) fails the test.
var stagePrevCur = map[Stage]struct {
	prev []string
	done string
}{
	StageComprehend: {[]string{"queued_at"}, "comprehended_at"},
	StageQuestion:   {[]string{"comprehended_at"}, "questioned_at"},
	StagePresolve:   {[]string{"questioned_at"}, "presolved_at"},
	StageAnswer:     {[]string{"presolved_at"}, "answered_at"},
	StageVerify:     {[]string{"answered_at"}, "verified_at"},
	StageFlush:      {[]string{"presolved_at", "verified_at"}, "projected_at"},
}

// TestClaimSQL_PrevNonNullCurNull verifies each stage claims exactly rows whose
// previous stage timestamp is non-null AND its own timestamp is null, and never
// claims superseded/failed rows (todos sec 2-5-1).
func TestClaimSQL_PrevNonNullCurNull(t *testing.T) {
	for stage, want := range stagePrevCur {
		m, err := meta(stage)
		require.NoError(t, err, "stage %s", stage)
		sql := claimSQL(m)

		for _, prev := range want.prev {
			assert.Contains(t, sql, prev+" IS NOT NULL", "stage %s must require %s non-null", stage, prev)
		}
		assert.Contains(t, sql, want.done+" IS NULL", "stage %s must require %s null", stage, want.done)
		assert.Contains(t, sql, "superseded_at IS NULL", "stage %s must skip superseded", stage)
		assert.Contains(t, sql, "failed_at IS NULL", "stage %s must skip failed", stage)
		assert.Contains(t, sql, "FOR UPDATE SKIP LOCKED", "stage %s must use SKIP LOCKED", stage)
		assert.Contains(t, sql, "SET lock_owner_id = $1, lock_at = now()", "stage %s must stamp the lock", stage)
	}
}

// TestClaimSQL_StaleReclaim verifies the lock predicate lets an unlocked or
// stale-locked row be claimed (crash recovery, todos sec 2-5-2).
func TestClaimSQL_StaleReclaim(t *testing.T) {
	m, err := meta(StageAnswer)
	require.NoError(t, err)
	sql := claimSQL(m)
	assert.Contains(t, sql, "lock_owner_id IS NULL OR lock_at < now() - make_interval(secs => $2)")
}

// TestClaimSQL_RetryGate verifies the claim predicate excludes an item whose
// transient-retry backoff has not yet elapsed (todos #4): only items with a NULL
// or past next_attempt_at are claimable.
func TestClaimSQL_RetryGate(t *testing.T) {
	for stage := range stagePrevCur {
		m, err := meta(stage)
		require.NoError(t, err, "stage %s", stage)
		sql := claimSQL(m)
		assert.Contains(t, sql, "(next_attempt_at IS NULL OR next_attempt_at <= now())",
			"stage %s must skip items still in transient-retry backoff", stage)
	}
}

// TestClaim_ReturnsItem verifies a claimed row is scanned and the stale seconds
// are passed as $2.
func TestClaim_ReturnsItem(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, args ...any) pgx.Row {
			gotSQL, gotArgs = sql, args
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 7          // id
				*(dest[1].(*string)) = "arxiv"   // source_type
				*(dest[2].(*string)) = "2501.99" // doc_id
				owner := "pod-a"
				*(dest[18].(**string)) = &owner // lock_owner_id (itemColumns order)
				return nil
			}}
		},
	}
	s := NewStore(q, 10*time.Minute)

	item, ok, err := s.Claim(context.Background(), StageAnswer, "pod-a")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(7), item.ID)
	assert.Equal(t, "arxiv", item.SourceType)
	assert.Equal(t, "2501.99", item.DocID)
	require.NotNil(t, item.LockOwnerID)
	assert.Equal(t, "pod-a", *item.LockOwnerID)

	assert.Contains(t, gotSQL, "presolved_at IS NOT NULL")
	assert.Contains(t, gotSQL, "answered_at IS NULL")
	require.Len(t, gotArgs, 2)
	assert.Equal(t, "pod-a", gotArgs[0])
	assert.Equal(t, 600.0, gotArgs[1]) // 10m in seconds, passed to make_interval
}

// TestClaim_NoCandidate verifies an empty candidate set (pgx.ErrNoRows from the
// RETURNING-less UPDATE) is ok=false with no error.
func TestClaim_NoCandidate(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(_ ...any) error { return pgx.ErrNoRows }}
		},
	}
	_, ok, err := NewStore(q, time.Minute).Claim(context.Background(), StageComprehend, "w")
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestClaim_UnknownStage rejects an unknown stage before touching the DB.
func TestClaim_UnknownStage(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			t.Fatal("must not query the DB for an unknown stage")
			return nil
		},
	}
	_, _, err := NewStore(q, time.Minute).Claim(context.Background(), Stage("bogus"), "w")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown stage "bogus"`)
}

// TestComplete_SetsTimestampClearsLock verifies Complete stamps the stage's
// timestamp, releases the lock, and is fenced on the owner ($2).
func TestComplete_SetsTimestampClearsLock(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL, gotArgs = sql, args
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	require.NoError(t, NewStore(q, time.Minute).Complete(context.Background(), 42, StageVerify, "pod-a"))
	assert.Contains(t, gotSQL, "verified_at = now()")
	assert.Contains(t, gotSQL, "lock_owner_id = NULL")
	assert.Contains(t, gotSQL, "lock_at = NULL")
	assert.Contains(t, gotSQL, "WHERE id = $1 AND lock_owner_id = $2", "Complete is owner-fenced")
	require.Len(t, gotArgs, 2)
	assert.Equal(t, int64(42), gotArgs[0])
	assert.Equal(t, "pod-a", gotArgs[1])
}

// TestComplete_ResetsRetryBudget (D1) verifies Complete clears the item-global
// retry bookkeeping (retry_count -> 0, last_error -> '') alongside next_attempt_at,
// so each stage starts with a full transient-retry budget. retry_count is a single
// shared column, so without this a retry spent at one stage would carry its count
// into the next and let RetryPolicy.MaxRetries fail a later stage early on its
// first real transient error.
func TestComplete_ResetsRetryBudget(t *testing.T) {
	var gotSQL string
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			gotSQL = sql
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	require.NoError(t, NewStore(q, time.Minute).Complete(context.Background(), 42, StageComprehend, "pod-a"))
	assert.Contains(t, gotSQL, "retry_count = 0", "Complete resets the retry count so the next stage gets a full budget")
	assert.Contains(t, gotSQL, "last_error = ''", "Complete clears the last transient error")
	assert.Contains(t, gotSQL, "next_attempt_at = NULL")
	assert.Contains(t, gotSQL, "WHERE id = $1 AND lock_owner_id = $2", "still owner-fenced")
}

// TestComplete_UnknownStage rejects an unknown stage.
func TestComplete_UnknownStage(t *testing.T) {
	err := NewStore(&dbtest.MockQuerier{}, time.Minute).Complete(context.Background(), 1, Stage("nope"), "w")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown stage "nope"`)
}

// TestComplete_LeaseLost surfaces a 0-row fenced update as ErrLeaseLost: a
// worker whose lease was reclaimed as stale cannot complete late (todos #6).
func TestComplete_LeaseLost(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		},
	}
	err := NewStore(q, time.Minute).Complete(context.Background(), 99, StageAnswer, "stale-owner")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLeaseLost), "0 rows on a fenced complete -> lease lost")
}

// TestFail_RecordsReasonClearsLock verifies Fail writes failed_at/fail_stage/
// fail_reason, releases the lock, and is fenced on owner ($4).
func TestFail_RecordsReasonClearsLock(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL, gotArgs = sql, args
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	require.NoError(t, NewStore(q, time.Minute).Fail(context.Background(), 5, StagePresolve, "student endpoint down", "pod-a"))
	assert.Contains(t, gotSQL, "failed_at = now()")
	assert.Contains(t, gotSQL, "fail_stage = $2")
	assert.Contains(t, gotSQL, "fail_reason = $3")
	assert.Contains(t, gotSQL, "lock_owner_id = NULL")
	assert.Contains(t, gotSQL, "lock_owner_id = $4", "Fail is owner-fenced")
	require.Len(t, gotArgs, 4)
	assert.Equal(t, int64(5), gotArgs[0])
	assert.Equal(t, "presolve", gotArgs[1])
	assert.Equal(t, "student endpoint down", gotArgs[2])
	assert.Equal(t, "pod-a", gotArgs[3])
}

// TestFail_LeaseLost surfaces a 0-row fenced update as ErrLeaseLost.
func TestFail_LeaseLost(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		},
	}
	err := NewStore(q, time.Minute).Fail(context.Background(), 7, StageAnswer, "x", "stale")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLeaseLost))
}

// TestRetry_ReschedulesReleasesLock verifies Retry bumps retry_count, sets a
// future next_attempt_at from the backoff, records last_error, releases the lock
// WITHOUT setting failed_at, and is fenced on owner ($4) (todos #4).
func TestRetry_ReschedulesReleasesLock(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL, gotArgs = sql, args
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	require.NoError(t, NewStore(q, time.Minute).Retry(context.Background(), 5, 120.0, "429 rate limited", "pod-a"))
	assert.Contains(t, gotSQL, "retry_count = retry_count + 1")
	assert.Contains(t, gotSQL, "next_attempt_at = now() + make_interval(secs => $2)")
	assert.Contains(t, gotSQL, "last_error = $3")
	assert.NotContains(t, gotSQL, "failed_at", "retry must NOT terminally fail the item")
	assert.Contains(t, gotSQL, "lock_owner_id = $4", "Retry is owner-fenced")
	require.Len(t, gotArgs, 4)
	assert.Equal(t, int64(5), gotArgs[0])
	assert.Equal(t, 120.0, gotArgs[1])
	assert.Equal(t, "429 rate limited", gotArgs[2])
	assert.Equal(t, "pod-a", gotArgs[3])
}

// TestRetry_LeaseLost surfaces a 0-row fenced update as ErrLeaseLost.
func TestRetry_LeaseLost(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		},
	}
	err := NewStore(q, time.Minute).Retry(context.Background(), 7, 60, "x", "stale")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLeaseLost))
}

// TestSupersede_SoftDeletesPreProjection verifies Supersede sets superseded_at,
// releases the lock, and guards projected_at IS NULL + owner so a projected
// (buffer-bound, immutable) row is never retro-superseded (todos sec 2-5-6/#2).
func TestSupersede_SoftDeletesPreProjection(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL, gotArgs = sql, args
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	require.NoError(t, NewStore(q, time.Minute).Supersede(context.Background(), 8, "pod-a"))
	assert.Contains(t, gotSQL, "superseded_at = now()")
	assert.Contains(t, gotSQL, "projected_at IS NULL")
	assert.Contains(t, gotSQL, "lock_owner_id = NULL")
	assert.Contains(t, gotSQL, "lock_owner_id = $2", "Supersede is owner-fenced")
	require.Len(t, gotArgs, 2)
	assert.Equal(t, int64(8), gotArgs[0])
	assert.Equal(t, "pod-a", gotArgs[1])
}

// TestSupersede_LeaseLostOrProjected surfaces a 0-row update (already projected,
// reclaimed, or missing) as ErrLeaseLost: the append-only buffer is protected.
func TestSupersede_LeaseLostOrProjected(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		},
	}
	err := NewStore(q, time.Minute).Supersede(context.Background(), 8, "pod-a")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLeaseLost))
}

// TestNewStore_DefaultStale falls back to DefaultStaleLock for a non-positive
// stale so a misconfigured worker never reclaims locks instantly.
func TestNewStore_DefaultStale(t *testing.T) {
	var gotArgs []any
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, args ...any) pgx.Row {
			gotArgs = args
			return &dbtest.MockRow{ScanFn: func(_ ...any) error { return pgx.ErrNoRows }}
		},
	}
	_, _, err := NewStore(q, 0).Claim(context.Background(), StageComprehend, "w")
	require.NoError(t, err)
	require.Len(t, gotArgs, 2)
	assert.Equal(t, DefaultStaleLock.Seconds(), gotArgs[1])
}

// TestClaimSQL_Injectionsafe is a guard that the interpolated column names are
// only ever the fixed internal identifiers, never runtime values.
func TestClaimSQL_ColumnsFromFixedTable(t *testing.T) {
	for stage := range stagePrevCur {
		m, err := meta(stage)
		require.NoError(t, err)
		sql := claimSQL(m)
		// Every interpolated column must be a known identifier and values must be
		// parameterized ($1, $2 only).
		assert.Equal(t, 2, strings.Count(sql, "$"), "stage %s must parameterize exactly workerID and stale", stage)
	}
}
