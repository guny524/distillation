package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/model"
)

// Pipeline assembles the reverse-generation stages over a shared LLM client.
type Pipeline struct {
	cfg  Config
	llm  LLM
	exec Executor
	// Log receives per-stage progress lines; defaults to io.Discard.
	Log io.Writer
}

// New builds a Pipeline. The config is normalized (defaults filled) so a
// partially-specified config still resolves every role. The code executor is
// constructed from cfg.Executor and is inert unless explicitly enabled.
func New(cfg Config, llm LLM) (*Pipeline, error) {
	if llm == nil {
		return nil, fmt.Errorf("pipeline: nil LLM")
	}
	norm := cfg.normalized()
	return &Pipeline{cfg: norm, llm: llm, exec: newExecutor(norm.Executor), Log: io.Discard}, nil
}

// Result is one artifact's pipeline output.
type Result struct {
	// Records are the kept ko/en records (0..2). Empty when the student
	// pre-filter dropped the question or every lane failed verification.
	Records []model.DistillationPair
	// TeacherCalls counts paced teacher-role requests (the pacing cost basis).
	TeacherCalls int
	// Skipped is true when the student pre-filter dropped the question
	// (zero-learning-value: student already solves it).
	Skipped bool
}

func (p *Pipeline) logf(format string, args ...any) {
	if p.Log != nil {
		fmt.Fprintf(p.Log, format+"\n", args...)
	}
}

// Run executes the full pipeline for one artifact, producing the ko/en record
// pair. pairID links the two lane records (task_id becomes pairID-<lane>).
//
// Stages: backtranslate -> mutate -> student pre-filter (drop if solved) ->
// 2-lane translate + teacher trajectory -> verify (drop fails).
func (p *Pipeline) Run(ctx context.Context, art artifact.Artifact, pairID string) (Result, error) {
	q, err := p.backtranslate(ctx, art)
	if err != nil {
		return Result{}, err
	}
	q, err = p.mutate(ctx, q)
	if err != nil {
		return Result{}, err
	}

	outcome, err := p.studentFilter(ctx, q)
	if err != nil {
		return Result{}, err
	}
	if !outcome.ToTeacher {
		p.logf("[pipeline] %s dropped by student pre-filter (verdict=%s)", pairID, outcome.Verdict.Verdict)
		return Result{Skipped: true}, nil
	}

	lr, err := p.lane(ctx, q, outcome.Verdict, pairID)
	if err != nil {
		return Result{TeacherCalls: lr.TeacherCalls}, err
	}

	res := Result{TeacherCalls: lr.TeacherCalls}
	for _, rec := range lr.Records {
		v, err := p.verify(ctx, rec)
		if err != nil {
			return res, err
		}
		rec.Verification = &v
		if !kept(v) {
			p.logf("[pipeline] %s lane=%s dropped by verifier (%s/%s)", pairID, rec.SourceLane, v.Method, v.Result)
			continue
		}
		res.Records = append(res.Records, rec)
	}
	return res, nil
}

// ToParams projects a produced record into DB insert params, reusing the same
// validation the JSONL loader applies (model.RecordToParams + ValidateEnums).
// It round-trips through JSON (respecting the record's json tags), then drops
// null-valued optional keys (references/artifacts/created_at) so RecordToParams
// treats them as absent rather than a malformed non-array.
func ToParams(rec model.DistillationPair) (map[string]any, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("pipeline: marshal record: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("pipeline: unmarshal record: %w", err)
	}
	for k, v := range m {
		if v == nil {
			delete(m, k)
		}
	}
	params, err := model.RecordToParams(m)
	if err != nil {
		return nil, err
	}
	if err := model.ValidateEnums(params); err != nil {
		return nil, err
	}
	return params, nil
}

// ArtifactSource yields artifacts for the runner to feed the pipeline. Next
// returns (artifact, true, nil) for the next artifact, (_, false, nil) when
// exhausted. Ingestion adapters (internal/ingest) implement it; SliceSource is
// the in-memory implementation used by tests and fixtures.
type ArtifactSource interface {
	Next(ctx context.Context) (artifact.Artifact, bool, error)
}

// SliceSource is an in-memory ArtifactSource over a fixed slice.
type SliceSource struct {
	items []artifact.Artifact
	i     int
}

// NewSliceSource wraps a slice of artifacts as an ArtifactSource.
func NewSliceSource(items []artifact.Artifact) *SliceSource {
	return &SliceSource{items: items}
}

// Next returns the next artifact or ok=false when the slice is exhausted.
func (s *SliceSource) Next(_ context.Context) (artifact.Artifact, bool, error) {
	if s.i >= len(s.items) {
		return artifact.Artifact{}, false, nil
	}
	a := s.items[s.i]
	s.i++
	return a, true, nil
}
