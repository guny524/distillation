package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/db/sqlcgen"
)

// Enqueue registers one ingested artifact as a queue item (sqlcgen.Enqueue: an
// INSERT ... ON CONFLICT (source_type, doc_id) DO NOTHING, the ingest idempotency
// guard, todos sec 2-5-7). Returns inserted=true when a new row was created,
// false when it was a duplicate no-op. The artifact's short excerpt chunks are
// preserved in source_excerpt so comprehend grounds its digest in real source
// content rather than title/URL alone (todos sec 2-5-4/2-5-5); the eventual
// opencode worker replaces this with a full fetch+compact, but until then the
// ingest excerpt is the grounding anchor for backtranslation.
func (s *Store) Enqueue(ctx context.Context, art artifact.Artifact) (inserted bool, err error) {
	if art.SourceType == "" || art.DocID == "" {
		return false, fmt.Errorf("queue: enqueue requires source_type and doc_id (got %q/%q)", art.SourceType, art.DocID)
	}
	rows, err := sqlcgen.New(s.q).Enqueue(ctx, sqlcgen.EnqueueParams{
		SourceType:    string(art.SourceType),
		DocID:         art.DocID,
		Url:           art.URL,
		License:       art.License,
		Title:         art.Title,
		Locator:       art.Locator,
		RetrievedAt:   nullTime(art.RetrievedAt),
		SourceExcerpt: joinChunks(art.Chunks),
	})
	if err != nil {
		return false, fmt.Errorf("queue: enqueue %s/%s: %w", art.SourceType, art.DocID, err)
	}
	return rows > 0, nil
}

// joinChunks concatenates an artifact's short excerpt chunks into a single
// newline-separated excerpt for source_excerpt. Empty chunks are skipped.
func joinChunks(chunks []artifact.Chunk) string {
	parts := make([]string, 0, len(chunks))
	for _, c := range chunks {
		if t := strings.TrimSpace(c.Text); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, "\n")
}

// GetCursor returns the last saved ingest position for sourceType (sqlcgen.
// GetCursor), or "" when the source has never been ingested (pgx.ErrNoRows).
// Sources use this to resume instead of re-collecting from the front (todos
// sec 2-5-7).
func (s *Store) GetCursor(ctx context.Context, sourceType string) (string, error) {
	pos, err := sqlcgen.New(s.q).GetCursor(ctx, sourceType)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("queue: get cursor %s: %w", sourceType, err)
	}
	return pos, nil
}

// SaveCursor records the ingest watermark for sourceType (sqlcgen.SaveCursor, an
// upsert). position is opaque; each source defines its own encoding (arXiv start
// offset, k8s since-timestamp or page token, StackExchange last post id).
func (s *Store) SaveCursor(ctx context.Context, sourceType, position string) error {
	if sourceType == "" {
		return fmt.Errorf("queue: save cursor requires source_type")
	}
	if err := sqlcgen.New(s.q).SaveCursor(ctx, sqlcgen.SaveCursorParams{
		SourceType: sourceType,
		Position:   position,
	}); err != nil {
		return fmt.Errorf("queue: save cursor %s: %w", sourceType, err)
	}
	return nil
}

// nullTime returns nil for a zero time so retrieved_at stays SQL NULL.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
