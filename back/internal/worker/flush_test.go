package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/queue"
)

func verifs(results ...string) []model.Verification {
	out := make([]model.Verification, len(results))
	for i, r := range results {
		out[i] = model.Verification{Method: "rule", Result: r, Detail: "d"}
	}
	return out
}

// TestFlush_ProjectsKeptLanesAndFlushesBuffer: a flush-gated item projects only
// its non-failing lanes into distillation_pairs, marks the item projected, and
// the buffer -> export volume cycle runs.
func TestFlush_ProjectsKeptLanesAndFlushesBuffer(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageFlush)
	fi.answer = laneRecords()
	fi.verifs = verifs("pass", "fail") // only the ko lane survives

	flusher := &fakeFlusher{flushed: true, rows: 1}
	w := NewFlushWorker(s, flusher, "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.ItemsProjected)
	assert.Equal(t, 1, res.RecordsWritten, "only the passing lane is projected")
	require.NotNil(t, fi.it.ProjectedAt, "projected_at stamped (the item's terminal mark)")
	require.Len(t, s.projectedRows, 1)
	assert.Equal(t, "distill-item-1-ko", s.projectedRows[0]["task_id"])
	// The projected record carries its verification (pass) for the buffer.
	require.IsType(t, map[string]any{}, s.projectedRows[0]["verification"])
	assert.Equal(t, 1, flusher.called, "buffer -> export volume cycle invoked")
	assert.True(t, res.Flushed)
	assert.Equal(t, 1, s.completes[queue.StageFlush])
}

// TestFlush_SkipsSupersededAndProjected: a superseded item and an
// already-projected item are outside the flush gate and never re-projected (no
// parquet duplicate on restart -- todos #2).
func TestFlush_SkipsSupersededAndProjected(t *testing.T) {
	s := newFakeStore()
	sup := s.add(1, "arxiv", "sup", queue.StageFlush)
	t0 := s.now
	sup.it.SupersededAt = &t0 // soft-deleted before flush
	done := s.add(2, "arxiv", "done", queue.StageFlush)
	done.it.ProjectedAt = &t0 // already projected into the buffer

	flusher := &fakeFlusher{}
	w := NewFlushWorker(s, flusher, "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, res.ItemsProjected, "neither item is in the flush gate")
	assert.Empty(t, s.projectedRows)
}

// TestFlush_LengthMismatchFails: an item whose verification count does not match
// its answer records is failed (never projects a misaligned record).
func TestFlush_LengthMismatchFails(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageFlush)
	fi.answer = laneRecords()  // 2 lanes
	fi.verifs = verifs("pass") // 1 verification -> mismatch

	w := NewFlushWorker(s, &fakeFlusher{}, "pod-a")

	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, res.RecordsWritten)
	require.NotNil(t, fi.it.FailedAt)
	assert.Equal(t, "flush", fi.it.FailStage)
	assert.Empty(t, s.projectedRows)
}

// TestFlush_LeaseLostProjectsNothing: a worker whose lease was reclaimed mid-pass
// projects NOTHING (atomic rollback) and the item is not marked projected -- the
// keystone of todos #2/#6 (no partial export, no double-project).
func TestFlush_LeaseLostProjectsNothing(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageFlush)
	fi.answer = laneRecords()
	fi.verifs = verifs("pass", "pass")

	// Directly exercise the fenced atomic projection with a stale owner.
	item, ok, err := s.Claim(context.Background(), queue.StageFlush, "pod-a")
	require.NoError(t, err)
	require.True(t, ok)
	s.reclaimBy(item.ID, "pod-b") // another worker reclaimed the lease

	n, perr := s.ProjectAndComplete(context.Background(), item.ID, "pod-a", []map[string]any{{"task_id": "x"}})
	require.Error(t, perr)
	assert.True(t, errors.Is(perr, queue.ErrLeaseLost))
	assert.Equal(t, 0, n, "a lost lease inserts nothing")
	assert.Empty(t, s.projectedRows, "atomic: no partial projection")
	assert.Nil(t, fi.it.ProjectedAt, "item not marked projected by the reclaimed worker")
}
