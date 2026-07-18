package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/queue"
)

// laneRecords is a two-lane answer payload (ko/en) with code-free final answers,
// so verify takes the LLM path (no executor).
func laneRecords() []model.DistillationPair {
	base := model.DistillationPair{
		Domain: "software-engineering", Difficulty: "medium", TaskShape: "code",
		CapabilityTags: []string{"reasoning"}, UserRequest: "q", Context: "c",
		SuccessCriteria: []string{"crit"}, Plan: []string{"p"},
		ReasoningSummary: "r", FinalAnswer: "clip the gradient norm", SelfCheck: []string{"s"},
		QualityNotes: []string{"n"},
	}
	ko, en := base, base
	ko.SourceLane, ko.TaskID = "ko", "distill-item-1-ko"
	en.SourceLane, en.TaskID = "en", "distill-item-1-en"
	return []model.DistillationPair{ko, en}
}

// TestVerify_AnyPassCompletes: at least one passing lane completes the item and
// stores the aligned per-lane verifications.
func TestVerify_AnyPassCompletes(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageVerify)
	fi.answer = laneRecords()
	// One lane passes, one fails -> item kept (survivor flushed later).
	llm := newFakeLLM().script("verifier", ok(verifyPassJSON), ok(verifyFailJSON))
	w := NewVerifyWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.VerifiedAt, "verified_at stamped")
	assert.Nil(t, fi.it.SupersededAt)
	require.Len(t, fi.verifs, 2, "one verification per lane")
	assert.Equal(t, "pass", fi.verifs[0].Result)
	assert.Equal(t, "fail", fi.verifs[1].Result)
	assert.Equal(t, 1, s.completes[queue.StageVerify])
}

// TestVerify_AllFailSuperseded: when every lane fails verification the item is
// superseded, not completed (todos: "verify fail이면 superseded_at").
func TestVerify_AllFailSuperseded(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageVerify)
	fi.answer = laneRecords()
	llm := newFakeLLM().script("verifier", ok(verifyFailJSON), ok(verifyFailJSON))
	w := NewVerifyWorker(s, testPipeline(t, llm), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NotNil(t, fi.it.SupersededAt, "all lanes failed -> discarded")
	assert.Nil(t, fi.it.VerifiedAt, "not completed")
	assert.Equal(t, 1, s.supersedes)
	assert.Equal(t, 0, s.completes[queue.StageVerify])
}

// TestVerify_EmptyPayloadFails: a verify item with no answer records is failed.
func TestVerify_EmptyPayloadFails(t *testing.T) {
	s := newFakeStore()
	fi := s.add(1, "arxiv", "2501.1", queue.StageVerify)
	fi.answer = []model.DistillationPair{}
	w := NewVerifyWorker(s, testPipeline(t, newFakeLLM()), openGate(), "pod-a")

	n, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "one item drained (claimed and moved to a terminal state)")
	require.NotNil(t, fi.it.FailedAt)
	assert.Equal(t, "verify", fi.it.FailStage)
}
