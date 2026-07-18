package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/teacher"
)

// assertErr is a generic sentinel for scripting stage failures in tests.
var assertErr = errors.New("scripted stage failure")

// fakeItem is one in-memory queue row: its state timestamps + lock live on the
// embedded queue.Item, its stage payloads alongside.
type fakeItem struct {
	it       queue.Item
	digest   string
	question *pipeline.Question
	verdict  *model.StudentFilterVerdict
	answer   []model.DistillationPair
	verifs   []model.Verification
}

// fakeStore is an in-memory worker.Store: it models the queue state machine
// (claimable = prev stage done AND this stage pending AND not superseded/failed/
// locked) plus the stage payload columns, so every worker is testable with no
// PostgreSQL. Deterministic Claim order = insertion order. Every write is
// owner-fenced exactly like the real store: a write whose owner no longer matches
// the row's lock_owner_id returns queue.ErrLeaseLost.
type fakeStore struct {
	items []*fakeItem
	now   time.Time
	// counters for assertions.
	completes     map[queue.Stage]int
	fails         map[queue.Stage]int
	supersedes    int
	retries       int
	lastBackoff   float64
	projectedRows []map[string]any // rows passed to ProjectAndComplete
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		now:       time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
		completes: map[queue.Stage]int{},
		fails:     map[queue.Stage]int{},
	}
}

// fenced reports the lease-loss error when owner no longer holds the item's lock
// (a stale reclaim reassigned it), mirroring the real store's owner fence.
func (s *fakeStore) fenced(fi *fakeItem, owner string) error {
	if fi.it.LockOwnerID == nil || *fi.it.LockOwnerID != owner {
		return queue.ErrLeaseLost
	}
	return nil
}

// reclaimBy simulates a stale reclaim: another worker takes the lock, so the
// original owner's subsequent fenced writes fail with ErrLeaseLost.
func (s *fakeStore) reclaimBy(id int64, newOwner string) {
	fi := s.find(id)
	o := newOwner
	fi.it.LockOwnerID = &o
}

// add appends an item at the given furthest-reached stage by back-filling every
// prior timestamp, so it is immediately claimable at stage.
func (s *fakeStore) add(id int64, source, doc string, pendingAt queue.Stage) *fakeItem {
	fi := &fakeItem{it: queue.Item{ID: id, SourceType: source, DocID: doc, Title: "T-" + doc, URL: "http://x/" + doc, License: "CC-BY", Locator: "sec-1"}}
	ts := s.now
	stamp := func(p **time.Time) { t := ts; *p = &t }
	// The linear order; stop stamping right before pendingAt.
	order := []struct {
		stage queue.Stage
		col   **time.Time
	}{
		{"", &fi.it.QueuedAt},
		{queue.StageComprehend, &fi.it.ComprehendedAt},
		{queue.StageQuestion, &fi.it.QuestionedAt},
		{queue.StagePresolve, &fi.it.PresolvedAt},
		{queue.StageAnswer, &fi.it.AnsweredAt},
		{queue.StageVerify, &fi.it.VerifiedAt},
	}
	for _, o := range order {
		if o.stage == pendingAt {
			break
		}
		stamp(o.col)
	}
	s.items = append(s.items, fi)
	return fi
}

func (s *fakeStore) find(id int64) *fakeItem {
	for _, fi := range s.items {
		if fi.it.ID == id {
			return fi
		}
	}
	return nil
}

func claimable(it queue.Item, stage queue.Stage) bool {
	if it.SupersededAt != nil || it.FailedAt != nil || it.LockOwnerID != nil {
		return false
	}
	switch stage {
	case queue.StageComprehend:
		return it.QueuedAt != nil && it.ComprehendedAt == nil
	case queue.StageQuestion:
		return it.ComprehendedAt != nil && it.QuestionedAt == nil
	case queue.StagePresolve:
		return it.QuestionedAt != nil && it.PresolvedAt == nil
	case queue.StageAnswer:
		return it.PresolvedAt != nil && it.AnsweredAt == nil
	case queue.StageVerify:
		return it.AnsweredAt != nil && it.VerifiedAt == nil
	case queue.StageFlush:
		// The flush stage's done column is projected_at (todos #2).
		return it.PresolvedAt != nil && it.VerifiedAt != nil && it.ProjectedAt == nil
	}
	return false
}

func (s *fakeStore) Claim(_ context.Context, stage queue.Stage, workerID string) (queue.Item, bool, error) {
	for _, fi := range s.items {
		if !claimable(fi.it, stage) {
			continue
		}
		// Transient-retry gate: skip an item still in backoff (todos #4), mirroring
		// the claim predicate (next_attempt_at IS NULL OR next_attempt_at <= now()).
		if fi.it.NextAttemptAt != nil && fi.it.NextAttemptAt.After(s.now) {
			continue
		}
		owner := workerID
		fi.it.LockOwnerID = &owner
		fi.it.LockAt = &s.now
		return fi.it, true, nil
	}
	return queue.Item{}, false, nil
}

func (s *fakeStore) Complete(_ context.Context, id int64, stage queue.Stage, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: complete: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	t := s.now
	switch stage {
	case queue.StageComprehend:
		fi.it.ComprehendedAt = &t
	case queue.StageQuestion:
		fi.it.QuestionedAt = &t
	case queue.StagePresolve:
		fi.it.PresolvedAt = &t
	case queue.StageAnswer:
		fi.it.AnsweredAt = &t
	case queue.StageVerify:
		fi.it.VerifiedAt = &t
	case queue.StageFlush:
		// The flush stage's done column is projected_at (see queue.stages).
		fi.it.ProjectedAt = &t
	default:
		return fmt.Errorf("fake: complete: unknown stage %q", stage)
	}
	fi.it.LockOwnerID, fi.it.LockAt = nil, nil
	fi.it.NextAttemptAt = nil
	s.completes[stage]++
	return nil
}

func (s *fakeStore) Fail(_ context.Context, id int64, stage queue.Stage, reason, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: fail: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	t := s.now
	fi.it.FailedAt = &t
	fi.it.FailStage = string(stage)
	fi.it.FailReason = reason
	fi.it.LockOwnerID, fi.it.LockAt = nil, nil
	s.fails[stage]++
	return nil
}

func (s *fakeStore) Retry(_ context.Context, id int64, backoffSeconds float64, lastErr, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: retry: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	fi.it.RetryCount++
	next := s.now.Add(time.Duration(backoffSeconds) * time.Second)
	fi.it.NextAttemptAt = &next
	fi.it.LastError = lastErr
	fi.it.LockOwnerID, fi.it.LockAt = nil, nil
	s.retries++
	s.lastBackoff = backoffSeconds
	return nil
}

func (s *fakeStore) Supersede(_ context.Context, id int64, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: supersede: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	if fi.it.ProjectedAt != nil {
		return queue.ErrLeaseLost // already projected: the append-only buffer is protected
	}
	t := s.now
	fi.it.SupersededAt = &t
	fi.it.LockOwnerID, fi.it.LockAt = nil, nil
	s.supersedes++
	return nil
}

func (s *fakeStore) ProjectAndComplete(_ context.Context, id int64, owner string, rows []map[string]any) (int, error) {
	fi := s.find(id)
	if fi == nil {
		return 0, fmt.Errorf("fake: project: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return 0, err // atomic: a lost lease projects nothing
	}
	s.projectedRows = append(s.projectedRows, rows...)
	t := s.now
	fi.it.ProjectedAt = &t
	fi.it.LockOwnerID, fi.it.LockAt = nil, nil
	fi.it.NextAttemptAt = nil
	s.completes[queue.StageFlush]++
	return len(rows), nil
}

func (s *fakeStore) SetComprehension(_ context.Context, id int64, digest, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: set comprehension: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	fi.digest = digest
	return nil
}

func (s *fakeStore) GetComprehension(_ context.Context, id int64) (string, error) {
	fi := s.find(id)
	if fi == nil {
		return "", fmt.Errorf("fake: get comprehension: item %d not found", id)
	}
	if err := validateDigest(id, fi.digest); err != nil {
		return "", err
	}
	return fi.digest, nil
}

func (s *fakeStore) SetQuestion(_ context.Context, id int64, q pipeline.Question, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: set question: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	qq := q
	fi.question = &qq
	return nil
}

func (s *fakeStore) GetQuestion(_ context.Context, id int64) (pipeline.Question, error) {
	fi := s.find(id)
	if fi == nil || fi.question == nil {
		return pipeline.Question{}, fmt.Errorf("fake: question for item %d not set", id)
	}
	if err := validateQuestion(id, *fi.question); err != nil {
		return pipeline.Question{}, err
	}
	return *fi.question, nil
}

func (s *fakeStore) SetPresolveVerdict(_ context.Context, id int64, v model.StudentFilterVerdict, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: set presolve verdict: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	vv := v
	fi.verdict = &vv
	return nil
}

func (s *fakeStore) GetPresolveVerdict(_ context.Context, id int64) (model.StudentFilterVerdict, error) {
	fi := s.find(id)
	if fi == nil || fi.verdict == nil {
		return model.StudentFilterVerdict{}, fmt.Errorf("fake: presolve verdict for item %d not set", id)
	}
	if err := validatePresolveVerdict(id, *fi.verdict); err != nil {
		return model.StudentFilterVerdict{}, err
	}
	return *fi.verdict, nil
}

func (s *fakeStore) SetAnswerPayload(_ context.Context, id int64, recs []model.DistillationPair, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: set answer payload: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	fi.answer = recs
	return nil
}

func (s *fakeStore) GetAnswerPayload(_ context.Context, id int64) ([]model.DistillationPair, error) {
	fi := s.find(id)
	if fi == nil || fi.answer == nil {
		return nil, fmt.Errorf("fake: answer payload for item %d not set", id)
	}
	if err := validateAnswerRecords(id, fi.answer); err != nil {
		return nil, err
	}
	return fi.answer, nil
}

func (s *fakeStore) SetVerification(_ context.Context, id int64, vs []model.Verification, owner string) error {
	fi := s.find(id)
	if fi == nil {
		return fmt.Errorf("fake: set verification: item %d not found", id)
	}
	if err := s.fenced(fi, owner); err != nil {
		return err
	}
	fi.verifs = vs
	return nil
}

func (s *fakeStore) GetVerification(_ context.Context, id int64) ([]model.Verification, error) {
	fi := s.find(id)
	if fi == nil || fi.verifs == nil {
		return nil, fmt.Errorf("fake: verification for item %d not set", id)
	}
	if err := validateVerifications(id, fi.verifs); err != nil {
		return nil, err
	}
	return fi.verifs, nil
}

// Compile-time: fakeStore satisfies Store.
var _ Store = (*fakeStore)(nil)

// --- fake reasoning-aware LLM -----------------------------------------------

// fakeResp scripts one role response: content for ChatCompletion, plus optional
// raw reasoning for the reasoning-aware Complete path.
type fakeResp struct {
	content   string
	reasoning string
	err       error
}

// fakeLLM is a role-keyed scripted LLM that satisfies pipeline.ReasoningLLM:
// ChatCompletion returns the content, Complete returns the full Completion
// (content + reasoning_content). Each role has an ordered response queue.
type fakeLLM struct {
	responses map[string][]fakeResp
	idx       map[string]int
	calls     map[string]int
}

func newFakeLLM() *fakeLLM {
	return &fakeLLM{responses: map[string][]fakeResp{}, idx: map[string]int{}, calls: map[string]int{}}
}

func (f *fakeLLM) script(role string, resps ...fakeResp) *fakeLLM {
	f.responses[role] = append(f.responses[role], resps...)
	return f
}

func (f *fakeLLM) next(role string) (fakeResp, error) {
	f.calls[role]++
	i := f.idx[role]
	f.idx[role]++
	q := f.responses[role]
	if i >= len(q) {
		return fakeResp{}, fmt.Errorf("fakeLLM: unscripted call %d for role %q", i, role)
	}
	return q[i], q[i].err
}

func (f *fakeLLM) ChatCompletion(_ context.Context, role string, _ []teacher.Message) (string, error) {
	r, err := f.next(role)
	return r.content, err
}

func (f *fakeLLM) Complete(_ context.Context, role string, _ []teacher.Message) (teacher.Completion, error) {
	r, err := f.next(role)
	if err != nil {
		return teacher.Completion{}, err
	}
	return teacher.Completion{Content: r.content, ReasoningContent: r.reasoning}, nil
}

func ok(content string) fakeResp { return fakeResp{content: content} }

// Compile-time: fakeLLM is reasoning-aware.
var _ pipeline.ReasoningLLM = (*fakeLLM)(nil)

// testPipeline builds a pipeline over the fake LLM with the artifact-mode
// defaults and stamped teacher model/provider.
func testPipeline(t interface{ Fatalf(string, ...any) }, llm pipeline.LLM) *pipeline.Pipeline {
	cfg := pipeline.DefaultConfig()
	cfg.TeacherModel = "gpt-5.4"
	cfg.TeacherProvider = "codex"
	p, err := pipeline.New(cfg, llm)
	if err != nil {
		t.Fatalf("build pipeline: %v", err)
	}
	return p
}

// --- fake gate / ingest / flush ---------------------------------------------

// fakeGate scripts the per-step quota gate (worker.Gate). allow is the Allow
// result sequence; an empty sequence means always-open, and once the sequence is
// consumed its last value repeats. It counts Allow calls and NoteRateLimited calls
// so a test can assert the gate was re-checked each pass and armed on a 429.
type fakeGate struct {
	allow       []bool
	i           int
	allowSeen   int
	rateLimited int
}

// openGate is always open; closedGate is always closed. A custom sequence (e.g.
// {true, false}) opens then closes to test the per-claim re-check.
func openGate() *fakeGate   { return &fakeGate{} }
func closedGate() *fakeGate { return &fakeGate{allow: []bool{false}} }

func (g *fakeGate) Allow(context.Context) bool {
	g.allowSeen++
	if len(g.allow) == 0 {
		return true
	}
	if g.i >= len(g.allow) {
		return g.allow[len(g.allow)-1]
	}
	v := g.allow[g.i]
	g.i++
	return v
}

func (g *fakeGate) NoteRateLimited() { g.rateLimited++ }

// Compile-time: fakeGate satisfies Gate.
var _ Gate = (*fakeGate)(nil)

// fakeIngestQueue records enqueues and cursors in memory, deduping by
// source_type/doc_id (the ON CONFLICT behavior).
type fakeIngestQueue struct {
	seen       map[string]bool
	enqueued   []artifact.Artifact
	cursors    map[string]string
	saveCalls  int
	enqueueErr error
}

func newFakeIngestQueue() *fakeIngestQueue {
	return &fakeIngestQueue{seen: map[string]bool{}, cursors: map[string]string{}}
}

func (q *fakeIngestQueue) Enqueue(_ context.Context, art artifact.Artifact) (bool, error) {
	if q.enqueueErr != nil {
		return false, q.enqueueErr
	}
	key := string(art.SourceType) + "/" + art.DocID
	if q.seen[key] {
		return false, nil
	}
	q.seen[key] = true
	q.enqueued = append(q.enqueued, art)
	return true, nil
}

func (q *fakeIngestQueue) GetCursor(_ context.Context, st string) (string, error) {
	return q.cursors[st], nil
}

func (q *fakeIngestQueue) SaveCursor(_ context.Context, st, pos string) error {
	q.saveCalls++
	q.cursors[st] = pos
	return nil
}

// fakeSource is an artifact.Source over a fixed slice. It records the cursor
// each Fetch received and returns next as the resume cursor.
type fakeSource struct {
	st         artifact.SourceType
	arts       []artifact.Artifact
	next       string
	fetchErr   error
	gotCursors []string
}

func (s *fakeSource) SourceType() artifact.SourceType { return s.st }
func (s *fakeSource) Fetch(_ context.Context, limit int, cursor string) ([]artifact.Artifact, string, error) {
	s.gotCursors = append(s.gotCursors, cursor)
	if s.fetchErr != nil {
		return nil, cursor, s.fetchErr
	}
	arts := s.arts
	if limit >= 0 && limit < len(arts) {
		arts = arts[:limit]
	}
	next := s.next
	if next == "" {
		next = cursor
	}
	return arts, next, nil
}

// fakeFlusher records whether the buffer flush ran.
type fakeFlusher struct {
	called  int
	flushed bool
	rows    int
	err     error
}

func (f *fakeFlusher) MaybeFlush(context.Context) (bool, int, error) {
	f.called++
	return f.flushed, f.rows, f.err
}

// validQuestion is a resolved English question the presolve/answer workers read
// from the `question` payload column (all pipeline-required fields present).
func validQuestion() pipeline.Question {
	return pipeline.Question{
		Domain: "software-engineering", Difficulty: "medium", TaskShape: "code",
		CapabilityTags:        []string{"reasoning"},
		UserRequest:           "Bound exploding gradients and implement a guard.",
		Context:               "A diverging training loop.",
		SuccessCriteria:       []string{"names a clipping strategy"},
		ReferenceAnswerSketch: "Clip the gradient norm to a threshold.",
		ArtifactRefs: []model.ArtifactRef{{
			SourceType: "arxiv", DocID: "2401.00001", License: "CC-BY", WhyRelevant: "grounds the question",
		}},
		Lang: "en",
	}
}

// verdictToTeacher is a presolve verdict that routed the question to the teacher.
func verdictToTeacher() model.StudentFilterVerdict {
	return model.StudentFilterVerdict{Verdict: "fail", Method: "both", Reason: "student wrong vs reference"}
}
