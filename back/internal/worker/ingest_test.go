package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/artifact"
)

func art(st artifact.SourceType, doc string) artifact.Artifact {
	return artifact.Artifact{SourceType: st, DocID: doc, License: "CC-BY", Title: "T-" + doc}
}

// TestIngest_EnqueuesAndSavesCursor: every fetched artifact is enqueued once,
// the saved per-source cursor is handed to Fetch, and the adapter-returned next
// cursor is persisted verbatim for the following run.
func TestIngest_EnqueuesAndSavesCursor(t *testing.T) {
	q := newFakeIngestQueue()
	q.cursors["arxiv"] = "2"
	src := &fakeSource{st: artifact.SourceArxiv, next: "4", arts: []artifact.Artifact{
		art(artifact.SourceArxiv, "2501.1"), art(artifact.SourceArxiv, "2501.2"),
	}}
	w := NewIngestWorker(q, []artifact.Source{src}, 32)

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, res.Enqueued)
	assert.Equal(t, 0, res.Duplicates)
	assert.Equal(t, 0, res.Failed)
	assert.Len(t, q.enqueued, 2)
	assert.Equal(t, []string{"2"}, src.gotCursors, "the stored cursor is passed into Fetch")
	assert.Equal(t, "4", q.cursors["arxiv"], "the adapter's next cursor is persisted verbatim")
}

// TestIngest_CursorUnchangedSkipsSave: an adapter that reached the end returns
// the cursor it was given; the worker must not rewrite the same watermark.
func TestIngest_CursorUnchangedSkipsSave(t *testing.T) {
	q := newFakeIngestQueue()
	q.cursors["arxiv"] = "7"
	src := &fakeSource{st: artifact.SourceArxiv, next: "7"}
	w := NewIngestWorker(q, []artifact.Source{src}, 32)

	_, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "7", q.cursors["arxiv"])
	assert.Zero(t, q.saveCalls, "unchanged cursor is not re-written")
}

// TestIngest_Idempotent: re-running over the same source enqueues nothing new
// (the ON CONFLICT dedup, todos sec 2-5-7).
func TestIngest_Idempotent(t *testing.T) {
	q := newFakeIngestQueue()
	src := &fakeSource{st: artifact.SourceArxiv, arts: []artifact.Artifact{art(artifact.SourceArxiv, "2501.1")}}
	w := NewIngestWorker(q, []artifact.Source{src}, 32)

	_, err := w.RunOnce(context.Background())
	require.NoError(t, err)

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, res.Enqueued)
	assert.Equal(t, 1, res.Duplicates, "second pass sees the doc as a duplicate")
	assert.Len(t, q.enqueued, 1)
}

// TestIngest_PerSourceErrorIsolated: one source's fetch failure does not abort
// ingestion of the others.
func TestIngest_PerSourceErrorIsolated(t *testing.T) {
	q := newFakeIngestQueue()
	bad := &fakeSource{st: artifact.SourceK8s, fetchErr: errors.New("github 503")}
	good := &fakeSource{st: artifact.SourcePMC, arts: []artifact.Artifact{art(artifact.SourcePMC, "PMC1")}}
	w := NewIngestWorker(q, []artifact.Source{bad, good}, 32)

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Failed, "the broken source is counted, not fatal")
	assert.Equal(t, 1, res.Enqueued, "the healthy source still ingests")
}
