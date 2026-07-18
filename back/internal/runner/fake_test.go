package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/pacing"
	"github.com/guny524/distillation/internal/teacher"
)

// fakeStore is an in-memory pacing.StateStore that records every write.
type fakeStore struct {
	lockAcquired bool
	lockErr      error
	unlocked     bool
	recovered    int

	emas   map[pacing.EMAKey]pacing.CostEMA
	hourly map[pacing.HourlyKey]pacing.HourlyBudget

	insertedRunLogs []pacing.RunLog
	updatedRunLogs  []pacing.RunLog
	observations    []pacing.Observation
	upsertedEMAs    []pacing.CostEMA
	hourlyAdds      []pacing.HourlyBudget

	nextRunID int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		lockAcquired: true,
		emas:         map[pacing.EMAKey]pacing.CostEMA{},
		hourly:       map[pacing.HourlyKey]pacing.HourlyBudget{},
		nextRunID:    100,
	}
}

var _ pacing.StateStore = (*fakeStore)(nil)

func (f *fakeStore) GetEMA(_ context.Context, key pacing.EMAKey) (pacing.CostEMA, bool, error) {
	e, ok := f.emas[key]
	if !ok {
		// Mirror PGStore: key fields prefilled on miss.
		e = pacing.CostEMA{Provider: key.Provider, SourceTag: key.SourceTag, WindowKind: key.WindowKind}
	}
	return e, ok, nil
}

func (f *fakeStore) GetHourlyBudget(_ context.Context, key pacing.HourlyKey) (pacing.HourlyBudget, bool, error) {
	hb, ok := f.hourly[key]
	return hb, ok, nil
}

func (f *fakeStore) CountGeneratedSince(_ context.Context, _, _ string, _ time.Time) (int, error) {
	return 0, nil
}

func (f *fakeStore) LastRateLimitedAt(_ context.Context, _, _ string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (f *fakeStore) TryAdvisoryLock(_ context.Context, _, _ string) (bool, error) {
	return f.lockAcquired, f.lockErr
}

func (f *fakeStore) AdvisoryUnlock(_ context.Context, _, _ string) error {
	f.unlocked = true
	return nil
}

func (f *fakeStore) InsertRunLog(_ context.Context, rl pacing.RunLog) (int64, error) {
	f.nextRunID++
	rl.ID = f.nextRunID
	f.insertedRunLogs = append(f.insertedRunLogs, rl)
	return f.nextRunID, nil
}

func (f *fakeStore) UpdateRunLog(_ context.Context, rl pacing.RunLog) error {
	f.updatedRunLogs = append(f.updatedRunLogs, rl)
	return nil
}

func (f *fakeStore) UpsertEMA(_ context.Context, ema pacing.CostEMA) error {
	f.upsertedEMAs = append(f.upsertedEMAs, ema)
	f.emas[pacing.EMAKey{Provider: ema.Provider, SourceTag: ema.SourceTag, WindowKind: ema.WindowKind}] = ema
	return nil
}

func (f *fakeStore) InsertObservation(_ context.Context, obs pacing.Observation) error {
	f.observations = append(f.observations, obs)
	return nil
}

func (f *fakeStore) AddHourlyConsumption(_ context.Context, hb pacing.HourlyBudget) error {
	f.hourlyAdds = append(f.hourlyAdds, hb)
	return nil
}

func (f *fakeStore) RecoverStaleRunning(_ context.Context, _, _ string, _ time.Time) (int, error) {
	return f.recovered, nil
}

// chatOutcome scripts one ChatCompletion result.
type chatOutcome struct {
	content string
	err     error
}

// fakeTeacher scripts quota fetches (first call = before, later = after) and
// sequential chat completions.
type fakeTeacher struct {
	quotaBefore []byte
	quotaAfter  []byte
	quotaErr    error
	fetchCount  int

	outcomes  []chatOutcome
	chatCalls int
	gotRoles  []string
	gotMsgs   [][]teacher.Message
}

var _ TeacherAPI = (*fakeTeacher)(nil)

func (f *fakeTeacher) ChatCompletion(_ context.Context, role string, msgs []teacher.Message) (string, error) {
	f.gotRoles = append(f.gotRoles, role)
	f.gotMsgs = append(f.gotMsgs, msgs)
	i := f.chatCalls
	f.chatCalls++
	if i >= len(f.outcomes) {
		return "", fmt.Errorf("fakeTeacher: unscripted call %d", i)
	}
	return f.outcomes[i].content, f.outcomes[i].err
}

func (f *fakeTeacher) FetchQuota(_ context.Context, _ string) ([]byte, error) {
	f.fetchCount++
	if f.quotaErr != nil {
		return nil, f.quotaErr
	}
	if f.fetchCount == 1 {
		return f.quotaBefore, nil
	}
	return f.quotaAfter, nil
}

// snapJSON marshals a snapshot to the /quota wire format.
func snapJSON(t *testing.T, s pacing.QuotaSnapshot) []byte {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return b
}

func fptr(v float64) *float64 { return &v }

// directSnapshot builds a single-provider direct-mode snapshot.
func directSnapshot(provider string, primaryUsed, secondaryUsed float64, primaryReset, secondaryReset, observedAt time.Time, pipeline []pacing.SourceUsage) pacing.QuotaSnapshot {
	return pacing.QuotaSnapshot{
		Providers: []pacing.ProviderSnapshot{{
			Provider:  provider,
			Primary:   pacing.Window{UsedPercent: fptr(primaryUsed), ResetsAt: primaryReset},
			Secondary: pacing.Window{UsedPercent: fptr(secondaryUsed), ResetsAt: secondaryReset},
		}},
		Pipeline:   pipeline,
		ObservedAt: observedAt,
	}
}

// validRecordJSON is a schema-valid teacher response (task_id gets overridden
// by the runner regardless of its value here).
const validRecordJSON = `{
  "task_id": "model-made-id",
  "domain": "software-engineering",
  "difficulty": "easy",
  "task_shape": "code",
  "capability_tags": ["reasoning"],
  "user_request": "q",
  "context": "c",
  "success_criteria": ["s"],
  "plan": ["p"],
  "reasoning_summary": "r",
  "final_answer": "a",
  "self_check": ["sc"],
  "quality_notes": ["qn"]
}`
