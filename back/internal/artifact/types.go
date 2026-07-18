// Package artifact defines the intermediate representation shared between
// ingestion (source adapters that fetch/parse expert material) and the
// generation pipeline (backtranslation -> filter -> 2-lane -> verify).
//
// This type is the CONTRACT between internal/ingest and internal/pipeline.
// Ingestion produces []Artifact; the pipeline consumes them and, via
// backtranslation, derives questions whose provenance is recorded as
// model.Record.ArtifactRefs. Copyright rule (research report): never store or
// redistribute long verbatim source text — keep reference IDs + offsets and
// rephrase. Chunk.Text is a short excerpt used only as generation context,
// not for redistribution.
package artifact

import (
	"context"
	"time"
)

// SourceType enumerates the four ingestion sources. Must stay in sync with the
// artifact_refs[].source_type enum in schemas/distillation.schema.json and
// model.artifactSourceTypes.
type SourceType string

const (
	SourceArxiv         SourceType = "arxiv"
	SourceStackExchange SourceType = "stackexchange"
	SourceK8s           SourceType = "k8s"           // k8s docs/KEP + cloud-native OSS GitHub issue/PR
	SourcePMC           SourceType = "pmc"           // PubMed Central OA subset
)

// Artifact is one unit of source material to backtranslate a question from.
// It carries provenance (for artifact_refs) and short excerpts (for generation
// context), never a full-text copy.
type Artifact struct {
	SourceType  SourceType `json:"source_type"`
	DocID       string     `json:"doc_id"`        // stable per-source id (arXiv id, SE post id, repo#issue, PMC id)
	URL         string     `json:"url,omitempty"` // canonical URL if any
	License     string     `json:"license"`       // SPDX-ish or source license label; required for traceability
	Title       string     `json:"title,omitempty"`
	Chunks      []Chunk    `json:"chunks"`             // short excerpts, NOT full text
	Locator     string     `json:"locator,omitempty"`  // section/line/offset pointer into the source
	RetrievedAt time.Time  `json:"retrieved_at"`
}

// Chunk is a short, individually-locatable excerpt. Keep Text brief (an anchor
// for rephrasing), never a long verbatim passage.
type Chunk struct {
	Text    string `json:"text"`
	Locator string `json:"locator,omitempty"` // offset/paragraph/line within the artifact
}

// Ref is the provenance record derived from an Artifact, shaped to match one
// element of model.Record.ArtifactRefs (schema $defs/artifactRef). WhyRelevant
// is filled by the generation stage (rephrased justification), so ToRef leaves
// it empty for the caller to populate.
type Ref struct {
	SourceType  string `json:"source_type"`
	DocID       string `json:"doc_id"`
	URL         string `json:"url,omitempty"`
	License     string `json:"license"`
	Locator     string `json:"locator,omitempty"`
	WhyRelevant string `json:"why_relevant,omitempty"`
}

// ToRef projects provenance into an artifact_refs element. WhyRelevant is left
// for the generation stage to fill with a rephrased relevance note.
func (a Artifact) ToRef() Ref {
	return Ref{
		SourceType: string(a.SourceType),
		DocID:      a.DocID,
		URL:        a.URL,
		License:    a.License,
		Locator:    a.Locator,
	}
}

// Source is the interface every ingestion adapter implements. Fetch returns up
// to limit artifacts starting from the opaque cursor position (empty = first
// page) and the next cursor to resume from, so successive ingest runs page
// forward through the source instead of re-scanning the front and relying on the
// enqueue ON CONFLICT to dedup. The cursor encoding is source-defined (arXiv
// start offset, PMC retstart, StackExchange/k8s page number); the worker persists
// it verbatim via SaveCursor and hands it back on the next run. Adapters must
// apply the benchmark-contamination filter (no MedQA/GPQA-style eval seeds)
// before returning. Implementations live in internal/ingest/<source>.
type Source interface {
	SourceType() SourceType
	Fetch(ctx context.Context, limit int, cursor string) (arts []Artifact, next string, err error)
}
