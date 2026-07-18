package runner

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/pacing"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/teacher"
)

// artifactTestConfig builds an artifact-mode config with every pipeline role
// present (all pointing at the same fake endpoint) and k=1.
func artifactTestConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	role := teacher.RoleConfig{BaseURL: "http://unused", Model: "test-model", SourceTag: "distillation", QuotaURL: "http://unused/quota"}
	cfg.Teacher.Roles = map[string]teacher.RoleConfig{
		"teacher":    role,
		"generator":  {BaseURL: "http://unused", Model: "test-model", SourceTag: "distillation-generator"},
		"student":    {BaseURL: "http://unused", Model: "test-model", SourceTag: "distillation-student"},
		"judge":      {BaseURL: "http://unused", Model: "test-model", SourceTag: "distillation-judge"},
		"translator": {BaseURL: "http://unused", Model: "test-model", SourceTag: "distillation-translator"},
		"verifier":   {BaseURL: "http://unused", Model: "test-model", SourceTag: "distillation-verifier"},
	}
	cfg.PromptTemplatePath = writeTemp(t, "prompt.md", "ID:{{.TaskID}}")
	cfg.TaxonomyPath = writeTemp(t, "taxonomy.yaml", "domain:\n  values: {}")
	cfg.Pipeline.Mode = ModeArtifact
	cfg.Pacing.MaxItemsPerRun = 1
	require.NoError(t, cfg.Validate())
	return cfg
}

func sampleArtifact() artifact.Artifact {
	return artifact.Artifact{
		SourceType: artifact.SourceArxiv,
		DocID:      "2401.00001",
		License:    "arXiv-nonexclusive",
		Title:      "test",
		Locator:    "sec-1",
		Chunks:     []artifact.Chunk{{Text: "excerpt anchor"}},
	}
}

// pipeline stage JSON responses (duplicated minimally here; the pipeline package
// owns the exhaustive stage tests).
const (
	btJSON   = `{"domain":"software-engineering","difficulty":"medium","task_shape":"code","capability_tags":["reasoning"],"user_request":"Bound exploding gradients.","context":"diverging loop","success_criteria":["clip"],"reference_answer_sketch":"norm clip","why_relevant":"grounds it"}`
	mutJSON  = `{"user_request":"Bound exploding gradients under conflicting constraints.","difficulty":"hard","success_criteria":["clip"],"applied_mutations":["conflicting_constraint"]}`
	stuJSON  = `{"answer":"clip by norm"}`
	judJSON  = `{"verdict":"fail","score":0.1}`
	transJSON = `{"user_request":"학습 그래디언트를 제한하라.","context":"발산 루프","success_criteria":["클립"],"reference_answer_sketch":"노름 클립"}`
	teachJSON = `{"plan":["clip"],"reasoning_summary":"bound norm","final_answer":"clip norm","self_check":["positive"],"quality_notes":["robust"]}`
	verJSON   = `{"method":"rule","result":"pass","detail":"ok"}`
)

// TestRun_ArtifactMode: an artifact-mode run pulls one artifact, runs the full
// pipeline, and inserts the ko/en pair; the pacing cost basis counts only the
// two teacher-role calls.
func TestRun_ArtifactMode(t *testing.T) {
	now := time.Now()
	provider := "codex"
	before := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)
	after := directSnapshot(provider, 12, 11, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)

	// Chat outcomes consumed in pipeline call order (default SelfConsistencyK=3).
	ft := &fakeTeacher{
		quotaBefore: snapJSON(t, before),
		quotaAfter:  snapJSON(t, after),
		outcomes: []chatOutcome{
			{content: btJSON},                                   // backtranslate (generator)
			{content: mutJSON},                                  // mutate (generator)
			{content: stuJSON}, {content: stuJSON}, {content: stuJSON}, // student x3
			{content: judJSON},                                  // judge
			{content: transJSON},                                // translate
			{content: teachJSON}, {content: teachJSON},          // teacher x2 (ko, en)
			{content: verJSON}, {content: verJSON},              // verify x2
		},
	}

	store := newFakeStore()
	var inserts int
	cfg := artifactTestConfig(t)
	r, err := New(cfg, store, emptyCoverageQuerier(&inserts), ft)
	require.NoError(t, err)
	r.Now = func() time.Time { return now }
	r.Log = io.Discard
	r.Artifacts = pipeline.NewSliceSource([]artifact.Artifact{sampleArtifact()})

	require.NoError(t, r.Run(context.Background()))

	assert.Equal(t, 2, inserts, "ko + en records inserted")
	require.Len(t, store.updatedRunLogs, 1)
	final := store.updatedRunLogs[0]
	assert.Equal(t, pacing.StatusCompleted, final.Status)
	assert.Equal(t, 1, final.GeneratedCount)
	// Pacing cost basis: only the two teacher-role calls, not every stage call.
	require.Len(t, store.observations, 2)
	assert.Equal(t, 2, store.observations[0].GeneratedCount)
	assert.True(t, store.unlocked)
}

// TestRun_ArtifactMode_NoSourceIsNoOp: artifact mode with no wired source
// completes cleanly with zero generated (no pod failure).
func TestRun_ArtifactMode_NoSourceIsNoOp(t *testing.T) {
	now := time.Now()
	provider := "codex"
	before := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)
	after := directSnapshot(provider, 10, 10, now.Add(3*time.Hour), now.Add(72*time.Hour), now, nil)

	ft := &fakeTeacher{quotaBefore: snapJSON(t, before), quotaAfter: snapJSON(t, after)}
	store := newFakeStore()
	var inserts int
	r, err := New(artifactTestConfig(t), store, emptyCoverageQuerier(&inserts), ft)
	require.NoError(t, err)
	r.Now = func() time.Time { return now }
	r.Log = io.Discard
	// r.Artifacts left nil.

	require.NoError(t, r.Run(context.Background()))
	assert.Equal(t, 0, inserts)
	assert.Equal(t, 0, ft.chatCalls)
	require.Len(t, store.updatedRunLogs, 1)
	assert.Equal(t, pacing.StatusCompleted, store.updatedRunLogs[0].Status)
	assert.Equal(t, 0, store.updatedRunLogs[0].GeneratedCount)
	assert.True(t, store.unlocked)
}
