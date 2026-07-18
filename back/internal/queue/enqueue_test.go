package queue

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/db/dbtest"
)

func sampleArtifact() artifact.Artifact {
	return artifact.Artifact{
		SourceType:  artifact.SourceArxiv,
		DocID:       "2501.12345",
		URL:         "https://arxiv.org/abs/2501.12345",
		License:     "arXiv-nonexclusive",
		Title:       "A Paper",
		Locator:     "abstract",
		Chunks:      []artifact.Chunk{{Text: "a short grounding excerpt"}},
		RetrievedAt: time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC),
	}
}

// TestEnqueue_Inserts verifies a new artifact is inserted at queued_at with the
// idempotency ON CONFLICT clause and the provenance args in order.
func TestEnqueue_Inserts(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL, gotArgs = sql, args
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}
	inserted, err := NewStore(q, time.Minute).Enqueue(context.Background(), sampleArtifact())
	require.NoError(t, err)
	assert.True(t, inserted)

	assert.Contains(t, gotSQL, "INSERT INTO distillation_items")
	assert.Contains(t, gotSQL, "ON CONFLICT (source_type, doc_id) DO NOTHING")
	assert.Contains(t, gotSQL, "queued_at")
	assert.Contains(t, gotSQL, "source_excerpt", "the ingest excerpt is preserved (todos #5)")
	require.Len(t, gotArgs, 8)
	assert.Equal(t, "arxiv", gotArgs[0])
	assert.Equal(t, "2501.12345", gotArgs[1])
	assert.Equal(t, "https://arxiv.org/abs/2501.12345", gotArgs[2])
	assert.Equal(t, "arXiv-nonexclusive", gotArgs[3])
	assert.Equal(t, "A Paper", gotArgs[4])
	assert.Equal(t, "abstract", gotArgs[5])
	// retrieved_at passed as *time.Time (non-nil since RetrievedAt is set).
	ts, ok := gotArgs[6].(*time.Time)
	require.True(t, ok)
	require.NotNil(t, ts)
	// source_excerpt preserved from the artifact's short chunk (todos #5).
	assert.Equal(t, "a short grounding excerpt", gotArgs[7])
}

// TestEnqueue_ConflictNoOp verifies a UNIQUE(source_type, doc_id) collision is a
// no-op (inserted=false), the ingest idempotency guarantee (todos sec 2-5-7).
func TestEnqueue_ConflictNoOp(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 0"), nil // ON CONFLICT skipped
		},
	}
	inserted, err := NewStore(q, time.Minute).Enqueue(context.Background(), sampleArtifact())
	require.NoError(t, err)
	assert.False(t, inserted)
}

// TestEnqueue_NilRetrievedAt passes SQL NULL for a zero RetrievedAt.
func TestEnqueue_NilRetrievedAt(t *testing.T) {
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
			gotArgs = args
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}
	art := sampleArtifact()
	art.RetrievedAt = time.Time{}
	_, err := NewStore(q, time.Minute).Enqueue(context.Background(), art)
	require.NoError(t, err)
	require.Len(t, gotArgs, 8)
	ts, ok := gotArgs[6].(*time.Time)
	require.True(t, ok)
	assert.Nil(t, ts, "zero RetrievedAt must be SQL NULL")
}

// TestEnqueue_RequiresIdentity rejects an artifact missing source_type or doc_id.
func TestEnqueue_RequiresIdentity(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			t.Fatal("must not insert an artifact without identity")
			return pgconn.CommandTag{}, nil
		},
	}
	s := NewStore(q, time.Minute)

	_, err := s.Enqueue(context.Background(), artifact.Artifact{DocID: "d"})
	require.Error(t, err)

	_, err = s.Enqueue(context.Background(), artifact.Artifact{SourceType: artifact.SourceArxiv})
	require.Error(t, err)
}

// TestGetCursor_Found returns the saved position.
func TestGetCursor_Found(t *testing.T) {
	var gotArgs []any
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, args ...any) pgx.Row {
			gotArgs = args
			assert.Contains(t, sql, "FROM distillation_source_cursors")
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*string)) = "600"
				return nil
			}}
		},
	}
	pos, err := NewStore(q, time.Minute).GetCursor(context.Background(), "arxiv")
	require.NoError(t, err)
	assert.Equal(t, "600", pos)
	require.Len(t, gotArgs, 1)
	assert.Equal(t, "arxiv", gotArgs[0])
}

// TestGetCursor_None returns "" when the source has never been ingested.
func TestGetCursor_None(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(_ ...any) error { return pgx.ErrNoRows }}
		},
	}
	pos, err := NewStore(q, time.Minute).GetCursor(context.Background(), "k8s")
	require.NoError(t, err)
	assert.Equal(t, "", pos)
}

// TestSaveCursor_Upserts verifies the cursor upsert SQL and args.
func TestSaveCursor_Upserts(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL, gotArgs = sql, args
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}
	require.NoError(t, NewStore(q, time.Minute).SaveCursor(context.Background(), "stackexchange", "post-9001"))
	assert.Contains(t, gotSQL, "INSERT INTO distillation_source_cursors")
	assert.Contains(t, gotSQL, "ON CONFLICT (source_type) DO UPDATE SET position = EXCLUDED.position")
	require.Len(t, gotArgs, 2)
	assert.Equal(t, "stackexchange", gotArgs[0])
	assert.Equal(t, "post-9001", gotArgs[1])
}

// TestSaveCursor_RequiresSource rejects an empty source_type.
func TestSaveCursor_RequiresSource(t *testing.T) {
	err := NewStore(&dbtest.MockQuerier{}, time.Minute).SaveCursor(context.Background(), "", "x")
	require.Error(t, err)
}
