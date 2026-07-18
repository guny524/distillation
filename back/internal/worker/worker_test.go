package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db/dbtest"
	"github.com/guny524/distillation/internal/queue"
)

// TestResolveWorkerID_PodName uses POD_NAME (the k8s replica identity) as the
// base but ALWAYS appends a random suffix, so the id is process-unique.
func TestResolveWorkerID_PodName(t *testing.T) {
	t.Setenv("POD_NAME", "distill-answer-7c9f-abcde")
	id := ResolveWorkerID()
	assert.True(t, strings.HasPrefix(id, "distill-answer-7c9f-abcde-"),
		"base is POD_NAME, then a suffix: got %q", id)
	assert.Greater(t, len(id), len("distill-answer-7c9f-abcde-"), "a non-empty suffix is appended")
}

// TestResolveWorkerID_Fallback returns a non-empty id (hostname base + suffix)
// when POD_NAME is unset.
func TestResolveWorkerID_Fallback(t *testing.T) {
	t.Setenv("POD_NAME", "")
	assert.NotEmpty(t, ResolveWorkerID())
}

// TestResolveWorkerID_SameBaseDistinct (D3): two workers that resolve their id
// from the SAME base (same POD_NAME / hostname, as two processes on one host do)
// still get DISTINCT owner ids, because a random per-process suffix is always
// appended. Without it, both would share the base and a stale worker's late write
// could ABA-match the new owner and defeat the lock fence.
func TestResolveWorkerID_SameBaseDistinct(t *testing.T) {
	t.Setenv("POD_NAME", "distill-answer-shared-host")
	a := ResolveWorkerID()
	b := ResolveWorkerID()
	assert.True(t, strings.HasPrefix(a, "distill-answer-shared-host-"))
	assert.True(t, strings.HasPrefix(b, "distill-answer-shared-host-"))
	assert.NotEqual(t, a, b, "same base must still yield distinct process-unique owners")
}

// TestItemArtifact_UsesDigestAsChunk reconstructs the artifact from provenance
// plus the comprehension digest (the single excerpt chunk backtranslate reads).
func TestItemArtifact_UsesDigestAsChunk(t *testing.T) {
	rt := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	it := queue.Item{
		SourceType: "arxiv", DocID: "2501.1", URL: "http://x", License: "CC-BY",
		Title: "T", Locator: "sec-1", RetrievedAt: &rt,
	}
	art := itemArtifact(it, "the digest")
	assert.Equal(t, "arxiv", string(art.SourceType))
	assert.Equal(t, "2501.1", art.DocID)
	require.Len(t, art.Chunks, 1)
	assert.Equal(t, "the digest", art.Chunks[0].Text)
	assert.Equal(t, rt, art.RetrievedAt)
}

// TestItemArtifact_NoDigestNoChunk keeps Chunks empty when there is no digest.
func TestItemArtifact_NoDigestNoChunk(t *testing.T) {
	art := itemArtifact(queue.Item{SourceType: "pmc", DocID: "PMC1"}, "")
	assert.Empty(t, art.Chunks)
}

// TestDrain_StopsWhenEmpty drains until nothing is claimable, processing each
// item exactly once.
func TestDrain_StopsWhenEmpty(t *testing.T) {
	s := newFakeStore()
	s.add(1, "arxiv", "a", queue.StageComprehend)
	s.add(2, "arxiv", "b", queue.StageComprehend)
	seen := map[int64]int{}
	n, err := drain(context.Background(), s, queue.StageComprehend, "w", func(it queue.Item) error {
		seen[it.ID]++
		return s.Complete(context.Background(), it.ID, queue.StageComprehend, ownerOf(it))
	})
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, map[int64]int{1: 1, 2: 1}, seen, "each item processed exactly once")
}

// TestDrain_ContextCancelled stops promptly on a cancelled context.
func TestDrain_ContextCancelled(t *testing.T) {
	s := newFakeStore()
	s.add(1, "arxiv", "a", queue.StageComprehend)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := drain(ctx, s, queue.StageComprehend, "w", func(queue.Item) error { return nil })
	assert.Error(t, err)
	assert.Equal(t, 0, n)
}

// TestPGStore_JSONWriteCastsJsonb pins that JSONB payload writes use a ::jsonb
// cast (so pgx sends JSON text, not bytea) and a parameterized value.
func TestPGStore_JSONWriteCastsJsonb(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL, gotArgs = sql, args
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	st := NewPGStore(queue.NewStore(q, time.Minute), q)
	require.NoError(t, st.SetVerification(context.Background(), 5, verifs("pass"), "pod-x"))
	assert.Contains(t, gotSQL, "verification = $2::jsonb")
	assert.Contains(t, gotSQL, "lock_owner_id = $3", "payload write is owner-fenced")
	require.Len(t, gotArgs, 3)
	assert.Equal(t, int64(5), gotArgs[0])
	assert.True(t, strings.HasPrefix(gotArgs[1].(string), "["), "verification serialized as a JSON array")
	assert.Equal(t, "pod-x", gotArgs[2])
}
