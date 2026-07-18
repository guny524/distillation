package worker

import (
	"context"
	"io"

	"github.com/guny524/distillation/internal/artifact"
)

// IngestQueue is the queue surface the ingest worker needs (implemented by
// *queue.Store): idempotent enqueue plus the per-source cursor. It is separate
// from Store because ingest never claims or writes stage payloads.
type IngestQueue interface {
	Enqueue(ctx context.Context, art artifact.Artifact) (inserted bool, err error)
	GetCursor(ctx context.Context, sourceType string) (string, error)
	SaveCursor(ctx context.Context, sourceType, position string) error
}

// IngestWorker discovers source documents and registers them as queue items
// (todos sec 2-5-3: the periodic ingest job). It is a one-shot batch (CronJob /
// ofelia @every): fetch each enabled source, Enqueue every artifact (the
// ON CONFLICT(source_type, doc_id) upsert makes re-runs idempotent, todos sec
// 2-5-7), and record a per-source cursor watermark.
type IngestWorker struct {
	store   IngestQueue
	sources []artifact.Source
	limit   int
	// Log receives progress lines (stderr in production, nil/discard in tests).
	Log io.Writer
}

// NewIngestWorker wires the worker. sources is the enabled adapter list
// (ingest.Registry.BuildEnabled); limit bounds each source's contribution per
// run so a cron tick never pulls an unbounded dump slice.
func NewIngestWorker(store IngestQueue, sources []artifact.Source, limit int) *IngestWorker {
	return &IngestWorker{store: store, sources: sources, limit: limit}
}

// IngestResult summarizes one ingest pass.
type IngestResult struct {
	Enqueued   int // new items registered
	Duplicates int // artifacts already present (ON CONFLICT no-op)
	Failed     int // sources that errored (isolated, not fatal)
}

// RunOnce fetches every enabled source once and enqueues the artifacts. A single
// source's fetch/enqueue failure is isolated (logged, counted, skipped) so one
// broken dump does not abort ingestion of the others -- mirroring
// Registry.FetchAll's per-source error isolation.
//
// The per-source cursor drives true incremental collection: the saved position
// is passed into Fetch (arXiv start offset, PMC retstart, StackExchange/k8s page
// number) so each run pages forward, and the next cursor the adapter returns is
// persisted for the following run. Enqueue's ON CONFLICT still guards against the
// rare overlap, but the cursor -- not dedup -- is what advances the corpus.
func (w *IngestWorker) RunOnce(ctx context.Context) (IngestResult, error) {
	var res IngestResult
	for _, src := range w.sources {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		st := string(src.SourceType())
		prevCursor, err := w.store.GetCursor(ctx, st)
		if err != nil {
			logf(w.Log, "[ingest] %s: get cursor failed: %v", st, err)
			res.Failed++
			continue
		}

		arts, nextCursor, err := src.Fetch(ctx, w.limit, prevCursor)
		if err != nil {
			logf(w.Log, "[ingest] %s: fetch failed: %v", st, err)
			res.Failed++
			continue
		}

		for _, art := range arts {
			inserted, err := w.store.Enqueue(ctx, art)
			if err != nil {
				logf(w.Log, "[ingest] %s: enqueue %s failed: %v", st, art.DocID, err)
				res.Failed++
				continue
			}
			if inserted {
				res.Enqueued++
			} else {
				res.Duplicates++
			}
		}

		// Persist the source's own next-page watermark so the next run resumes
		// where this one stopped. An adapter that reached the end returns the
		// unchanged cursor, so skip the redundant write then.
		if nextCursor != "" && nextCursor != prevCursor {
			if err := w.store.SaveCursor(ctx, st, nextCursor); err != nil {
				logf(w.Log, "[ingest] %s: save cursor failed: %v", st, err)
			}
		}
		logf(w.Log, "[ingest] %s: fetched %d artifact(s)", st, len(arts))
	}
	logf(w.Log, "[ingest] done: enqueued=%d duplicates=%d failed=%d", res.Enqueued, res.Duplicates, res.Failed)
	return res, nil
}
