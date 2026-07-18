package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"text/template"
	"time"

	"github.com/guny524/distillation/internal/coverage"
	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/loader"
	"github.com/guny524/distillation/internal/model"
	"github.com/guny524/distillation/internal/pacing"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/teacher"
)

// ErrSchemaInvalid marks a generation whose teacher response never passed JSON
// extraction + schema validation within schema_retries. The item is skipped;
// the run continues with the next item.
var ErrSchemaInvalid = errors.New("teacher response failed schema validation")

// errNoMoreArtifacts signals that the artifact source is exhausted (or not
// wired). The Run loop treats it as a clean stop, not a pod failure.
var errNoMoreArtifacts = errors.New("no more artifacts")

// TeacherAPI is the endpoint surface the runner needs; *teacher.Client
// implements it, tests inject a fake.
type TeacherAPI interface {
	ChatCompletion(ctx context.Context, role string, msgs []teacher.Message) (string, error)
	FetchQuota(ctx context.Context, role string) ([]byte, error)
}

// Runner executes one pacing-controlled generation cycle.
type Runner struct {
	cfg      Config
	store    pacing.StateStore
	db       db.Querier
	teacher  TeacherAPI
	tmpl     *template.Template
	taxonomy string
	pipeline *pipeline.Pipeline

	// Artifacts is the source consumed in artifact mode (one artifact per
	// generated item). Wired by the caller once ingestion is available; nil
	// makes an artifact-mode run a clean no-op.
	Artifacts pipeline.ArtifactSource
	// Now supplies the run's reference time; tests override it.
	Now func() time.Time
	// Log receives progress lines (stderr in production).
	Log io.Writer
}

// New validates the config, loads the prompt template and taxonomy from disk,
// and wires the runner.
func New(cfg Config, store pacing.StateStore, q db.Querier, t TeacherAPI) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	tmplSrc, err := os.ReadFile(cfg.PromptTemplatePath)
	if err != nil {
		return nil, fmt.Errorf("read prompt template %s: %w", cfg.PromptTemplatePath, err)
	}
	tmpl, err := parsePromptTemplate(string(tmplSrc))
	if err != nil {
		return nil, err
	}
	taxonomy, err := os.ReadFile(cfg.TaxonomyPath)
	if err != nil {
		return nil, fmt.Errorf("read taxonomy %s: %w", cfg.TaxonomyPath, err)
	}
	r := &Runner{
		cfg:      cfg,
		store:    store,
		db:       q,
		teacher:  t,
		tmpl:     tmpl,
		taxonomy: string(taxonomy),
		Now:      time.Now,
		Log:      os.Stderr,
	}
	if cfg.Pipeline.Mode == ModeArtifact {
		// The teacher client (TeacherAPI) also satisfies pipeline.LLM: every
		// pipeline role is routed through the same role-based endpoint client.
		p, perr := pipeline.New(cfg.toPipelineConfig(), t)
		if perr != nil {
			return nil, fmt.Errorf("build pipeline: %w", perr)
		}
		r.pipeline = p
		if w := cfg.FrontierQuotaWarning(); w != "" {
			fmt.Fprintf(r.Log, "[run] WARNING: %s\n", w)
		}
	}
	return r, nil
}

// sourceTag returns the teacher role's source tag (validated non-empty).
func (r *Runner) sourceTag() string {
	return r.cfg.Teacher.Roles[r.cfg.TeacherRole].SourceTag
}

func (r *Runner) logf(format string, args ...any) {
	fmt.Fprintf(r.Log, format+"\n", args...)
}

// Run performs one full cycle: stale-run recovery -> advisory lock -> quota
// fetch -> pacing decision -> k generations -> quota re-observation -> EMA
// update -> run log completion. A busy lock or an exhausted budget is a normal
// no-op run (nil error).
func (r *Runner) Run(ctx context.Context) error {
	now := r.Now()
	provider := r.cfg.Provider
	tag := r.sourceTag()

	staleBefore := now.Add(-time.Duration(r.cfg.StaleRunAfterMinutes) * time.Minute)
	if n, err := r.store.RecoverStaleRunning(ctx, provider, tag, staleBefore); err != nil {
		return fmt.Errorf("recover stale running: %w", err)
	} else if n > 0 {
		r.logf("[run] recovered %d stale running run(s)", n)
	}

	acquired, err := r.store.TryAdvisoryLock(ctx, provider, tag)
	if err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	if !acquired {
		r.logf("[run] advisory lock busy, skipping this run")
		_, err := r.store.InsertRunLog(ctx, pacing.RunLog{
			Provider: provider, SourceTag: tag,
			Status: pacing.StatusSkippedLockBusy, StartedAt: now, CompletedAt: &now,
		})
		return err
	}
	defer func() {
		if uerr := r.store.AdvisoryUnlock(ctx, provider, tag); uerr != nil {
			r.logf("[run] advisory unlock: %v", uerr)
		}
	}()

	// Quota is fetched only after the lock is held (spec sec 1).
	before := r.fetchQuota(ctx)
	decision, err := pacing.Decide(ctx, before, provider, tag, now, r.cfg.Pacing, r.store)
	if err != nil {
		return fmt.Errorf("pacing decide: %w", err)
	}
	r.logf("[run] decision: k=%d status=%s mode=%s allowance=%.4f%% cost(primary=%.4f%% secondary=%.4f%%) probe=%v",
		decision.K, decision.Status, decision.Mode, decision.HourlyAllowancePct,
		decision.CostPrimaryPct, decision.CostSecondaryPct, decision.Probe)

	rl := pacing.RunLog{
		Provider: provider, SourceTag: tag,
		Mode: decision.Mode, StartedAt: now,
		PlannedCount: decision.K,
	}
	fillUsedBefore(&rl, before, provider)
	if decision.Status == pacing.StatusDecided {
		allowance := decision.HourlyAllowancePct
		rl.AllowancePct = &allowance
	}

	if decision.K <= 0 {
		rl.Status = decision.Status
		rl.DecidedAt = &now
		rl.CompletedAt = &now
		_, err := r.store.InsertRunLog(ctx, rl)
		return err
	}

	rl.Status = pacing.StatusRunning
	rl.DecidedAt = &now
	runID, err := r.store.InsertRunLog(ctx, rl)
	if err != nil {
		return fmt.Errorf("insert run log: %w", err)
	}
	rl.ID = runID

	// calls counts teacher requests (the quota cost basis, including schema
	// retries); generated counts rows actually inserted.
	generated, calls := 0, 0
	rateLimited := false
	exhausted := false
	var loopErr error
	for i := 0; i < decision.K; i++ {
		n, genErr := r.generateOne(ctx, i, now)
		calls += n
		switch {
		case genErr == nil:
			generated++
		case errors.Is(genErr, errNoMoreArtifacts):
			r.logf("[run] artifact source exhausted after %d item(s)", generated)
			exhausted = true
		case errors.Is(genErr, teacher.ErrRateLimited):
			r.logf("[run] rate limited after %d/%d item(s): %v", generated, decision.K, genErr)
			rateLimited = true
		case errors.Is(genErr, ErrSchemaInvalid):
			r.logf("[run] item %d/%d skipped: %v", i+1, decision.K, genErr)
			continue
		default:
			loopErr = genErr
		}
		if rateLimited || loopErr != nil || exhausted {
			break
		}
	}

	after := r.fetchQuota(ctx)
	done := r.Now()

	consumed, observed := r.recordObservations(ctx, runID, before, after, calls, done)
	if decision.Mode == pacing.ModeDirect {
		if !observed {
			// Conservative fallback: charge the learned/fallback cost per call.
			consumed = decision.CostSecondaryPct * float64(calls)
		}
		hb := pacing.HourlyBudget{
			Provider: provider, SourceTag: tag,
			WindowKind: pacing.WindowSecondary, BudgetHour: budgetHour(now),
			AllowancePct:   decision.HourlyAllowancePct,
			PlannedPct:     decision.CostSecondaryPct * float64(decision.K),
			ConsumedPct:    consumed,
			GeneratedCount: generated,
		}
		if err := r.store.AddHourlyConsumption(ctx, hb); err != nil {
			r.logf("[run] add hourly consumption: %v", err)
		}
	}

	rl.Status = pacing.StatusCompleted
	if rateLimited {
		rl.Status = pacing.StatusRateLimited
	}
	rl.CompletedAt = &done
	rl.GeneratedCount = generated
	fillUsedAfter(&rl, after, provider)
	if loopErr != nil {
		rl.ErrorMsg = loopErr.Error()
	}
	if err := r.store.UpdateRunLog(ctx, rl); err != nil {
		r.logf("[run] update run log: %v", err)
	}

	r.logf("[run] finished: status=%s generated=%d/%d calls=%d", rl.Status, generated, decision.K, calls)
	return loopErr
}

// fetchQuota GETs and parses /quota; any failure returns nil, which
// pacing.Decide maps to quota_fetch_failed (k=0).
func (r *Runner) fetchQuota(ctx context.Context) *pacing.QuotaSnapshot {
	b, err := r.teacher.FetchQuota(ctx, r.cfg.TeacherRole)
	if err != nil {
		r.logf("[run] quota fetch failed: %v", err)
		return nil
	}
	snap, err := pacing.ParseSnapshot(b)
	if err != nil {
		r.logf("[run] quota parse failed: %v", err)
		return nil
	}
	return &snap
}

// generateOne produces and stores one item, dispatching on the configured
// mode. Returns how many paced teacher calls it made (the pacing cost basis).
func (r *Runner) generateOne(ctx context.Context, seq int, startedAt time.Time) (int, error) {
	if r.cfg.Pipeline.Mode == ModeArtifact {
		return r.generateOneArtifact(ctx, seq, startedAt)
	}
	return r.generateOneTaxonomy(ctx, seq, startedAt)
}

// generateOneArtifact runs the M3 artifact pipeline for one artifact: pull an
// artifact -> reverse-generate + mutate + student pre-filter + 2-lane + verify
// -> INSERT the kept ko/en records. Returns the paced teacher-call count.
// A nil/exhausted source returns errNoMoreArtifacts, a clean loop stop.
func (r *Runner) generateOneArtifact(ctx context.Context, seq int, startedAt time.Time) (int, error) {
	if r.pipeline == nil || r.Artifacts == nil {
		return 0, errNoMoreArtifacts
	}
	art, ok, err := r.Artifacts.Next(ctx)
	if err != nil {
		return 0, fmt.Errorf("artifact source: %w", err)
	}
	if !ok {
		return 0, errNoMoreArtifacts
	}

	pairID := fmt.Sprintf("distill-%s-%02d", startedAt.UTC().Format("20060102-150405"), seq)
	res, err := r.pipeline.Run(ctx, art, pairID)
	if err != nil {
		return res.TeacherCalls, err
	}
	for _, rec := range res.Records {
		params, perr := pipeline.ToParams(rec)
		if perr != nil {
			return res.TeacherCalls, fmt.Errorf("%w (task %s): %v", ErrSchemaInvalid, rec.TaskID, perr)
		}
		inserted, ierr := loader.InsertRecord(ctx, r.db, params)
		if ierr != nil {
			return res.TeacherCalls, fmt.Errorf("insert %s: %w", rec.TaskID, ierr)
		}
		if !inserted {
			r.logf("[run] task %s duplicate task_id, skipped by ON CONFLICT", rec.TaskID)
		}
	}
	if res.Skipped {
		r.logf("[run] pair %s dropped by student pre-filter", pairID)
	}
	return res.TeacherCalls, nil
}

// generateOneTaxonomy produces and stores one Q&A pair the legacy way: coverage
// -> prompt -> teacher call -> JSON extraction -> schema validation (re-request
// once on failure) -> INSERT. Returns how many teacher calls it made.
func (r *Runner) generateOneTaxonomy(ctx context.Context, seq int, startedAt time.Time) (int, error) {
	covJSON, err := r.coverageJSON(ctx)
	if err != nil {
		return 0, fmt.Errorf("coverage: %w", err)
	}
	taskID := fmt.Sprintf("distill-%s-%02d", startedAt.UTC().Format("20060102-150405"), seq)
	prompt, err := renderPrompt(r.tmpl, promptData{
		TaskID: taskID, TaxonomyYAML: r.taxonomy, CoverageJSON: covJSON,
	})
	if err != nil {
		return 0, err
	}
	msgs := []teacher.Message{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: prompt},
	}

	attempts := r.cfg.SchemaRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	calls := 0
	var lastErr error
	for a := 0; a < attempts; a++ {
		content, err := r.teacher.ChatCompletion(ctx, r.cfg.TeacherRole, msgs)
		if err != nil {
			return calls, err
		}
		calls++
		params, verr := parseAndValidate(content, taskID)
		if verr != nil {
			lastErr = verr
			r.logf("[run] task %s attempt %d/%d invalid response: %v", taskID, a+1, attempts, verr)
			continue
		}
		inserted, ierr := loader.InsertRecord(ctx, r.db, params)
		if ierr != nil {
			return calls, fmt.Errorf("insert %s: %w", taskID, ierr)
		}
		if !inserted {
			r.logf("[run] task %s duplicate task_id, skipped by ON CONFLICT", taskID)
		}
		return calls, nil
	}
	return calls, fmt.Errorf("%w (task %s): %v", ErrSchemaInvalid, taskID, lastErr)
}

// parseAndValidate extracts the JSON object, pins the Go-generated task_id
// (uniqueness is owned here, not by the model), and runs the same field/enum
// validation the loader applies to JSONL input.
func parseAndValidate(content, taskID string) (map[string]any, error) {
	record, err := ExtractJSONObject(content)
	if err != nil {
		return nil, err
	}
	record["task_id"] = taskID
	params, err := model.RecordToParams(record)
	if err != nil {
		return nil, err
	}
	if err := model.ValidateEnums(params); err != nil {
		return nil, err
	}
	return params, nil
}

// coverageJSON builds the current 4-axis coverage summary for the prompt.
func (r *Runner) coverageJSON(ctx context.Context) (string, error) {
	exists, err := coverage.TableExists(ctx, r.db)
	if err != nil {
		return "", err
	}
	var cov *coverage.Coverage
	if exists {
		cov, err = coverage.BuildCoverage(ctx, r.db)
		if err != nil {
			return "", err
		}
	} else {
		cov = coverage.BuildEmptyCoverage()
	}
	b, err := json.MarshalIndent(cov, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal coverage: %w", err)
	}
	return string(b), nil
}

// recordObservations writes before/after observations for both windows and
// applies UpdateEMA. Returns the usable secondary-window delta (the hourly
// budget consumption) when one was observed.
func (r *Runner) recordObservations(ctx context.Context, runID int64, before, after *pacing.QuotaSnapshot, calls int, observedAt time.Time) (secondaryConsumed float64, ok bool) {
	if before == nil || after == nil {
		return 0, false
	}
	psB, okB := before.Provider(r.cfg.Provider)
	psA, okA := after.Provider(r.cfg.Provider)
	if !okB || !okA {
		return 0, false
	}

	inP := observationInput(pacing.WindowPrimary, psB.Primary, psA.Primary, calls)
	if d, found := sourceDeltaDiff(before, after, r.sourceTag()); found {
		inP.HasAttribution = true
		inP.SourceDelta = d
	}
	r.applyObservation(ctx, runID, inP, observedAt)

	inS := observationInput(pacing.WindowSecondary, psB.Secondary, psA.Secondary, calls)
	resS, applied := r.applyObservation(ctx, runID, inS, observedAt)
	if applied && !resS.IsOutlier && resS.Delta >= 0 {
		return resS.Delta, true
	}
	return 0, false
}

// observationInput maps a window pair to an ObservationInput; unknown windows
// (cli transport) become QuotaUnknown so UpdateEMA never learns from them.
func observationInput(kind string, wb, wa pacing.Window, calls int) pacing.ObservationInput {
	in := pacing.ObservationInput{WindowKind: kind, GeneratedCount: calls}
	if wb.Known() && wa.Known() {
		in.UsedBefore = *wb.UsedPercent
		in.UsedAfter = *wa.UsedPercent
		in.ResetsAtBefore = wb.ResetsAt
		in.ResetsAtAfter = wa.ResetsAt
	} else {
		in.QuotaUnknown = true
	}
	return in
}

// sourceDeltaDiff computes this run's source-tag-attributed primary delta from
// the cumulative pipeline[].used_percent_delta values of the two snapshots.
// A missing before-row means no tagged request existed yet, so the after value
// is entirely this run's.
func sourceDeltaDiff(before, after *pacing.QuotaSnapshot, tag string) (float64, bool) {
	a, okA := after.SourceDelta(tag)
	if !okA {
		return 0, false
	}
	b, okB := before.SourceDelta(tag)
	if !okB {
		return a, true
	}
	return a - b, true
}

// applyObservation runs UpdateEMA for one window, persists the (possibly
// pending-accumulating) EMA row, and appends the observation. State write
// failures are logged, not fatal: the generated data is already stored.
func (r *Runner) applyObservation(ctx context.Context, runID int64, in pacing.ObservationInput, observedAt time.Time) (pacing.EMAResult, bool) {
	key := pacing.EMAKey{Provider: r.cfg.Provider, SourceTag: r.sourceTag(), WindowKind: in.WindowKind}
	ema, _, err := r.store.GetEMA(ctx, key)
	if err != nil {
		r.logf("[run] get ema (%s): %v", in.WindowKind, err)
		return pacing.EMAResult{}, false
	}
	res := pacing.UpdateEMA(in, ema, r.cfg.Pacing)
	res.EMA.Provider = key.Provider
	res.EMA.SourceTag = key.SourceTag
	res.EMA.WindowKind = key.WindowKind
	if err := r.store.UpsertEMA(ctx, res.EMA); err != nil {
		r.logf("[run] upsert ema (%s): %v", in.WindowKind, err)
	}

	obs := pacing.Observation{
		RunLogID: runID, Provider: key.Provider, SourceTag: key.SourceTag,
		WindowKind: in.WindowKind,
		UsedBefore: in.UsedBefore, UsedAfter: in.UsedAfter,
		Delta: res.Delta, GeneratedCount: in.GeneratedCount,
		IsOutlier: res.IsOutlier, OutlierReason: res.OutlierReason,
		UsedForEMA: res.UsedForEMA, ObservedAt: observedAt,
	}
	if !in.ResetsAtBefore.IsZero() {
		tb := in.ResetsAtBefore
		obs.ResetsAtBefore = &tb
	}
	if !in.ResetsAtAfter.IsZero() {
		ta := in.ResetsAtAfter
		obs.ResetsAtAfter = &ta
	}
	if err := r.store.InsertObservation(ctx, obs); err != nil {
		r.logf("[run] insert observation (%s): %v", in.WindowKind, err)
	}
	return res, true
}

// fillUsedBefore copies the provider's used_percent values into the run log
// (direct mode only; unknown windows stay NULL).
func fillUsedBefore(rl *pacing.RunLog, snap *pacing.QuotaSnapshot, provider string) {
	ps, ok := providerOf(snap, provider)
	if !ok {
		return
	}
	if ps.Primary.Known() {
		v := *ps.Primary.UsedPercent
		rl.PrimaryUsedBefore = &v
	}
	if ps.Secondary.Known() {
		v := *ps.Secondary.UsedPercent
		rl.SecondaryUsedBefore = &v
	}
}

// fillUsedAfter is fillUsedBefore's post-run counterpart.
func fillUsedAfter(rl *pacing.RunLog, snap *pacing.QuotaSnapshot, provider string) {
	ps, ok := providerOf(snap, provider)
	if !ok {
		return
	}
	if ps.Primary.Known() {
		v := *ps.Primary.UsedPercent
		rl.PrimaryUsedAfter = &v
	}
	if ps.Secondary.Known() {
		v := *ps.Secondary.UsedPercent
		rl.SecondaryUsedAfter = &v
	}
}

func providerOf(snap *pacing.QuotaSnapshot, provider string) (pacing.ProviderSnapshot, bool) {
	if snap == nil {
		return pacing.ProviderSnapshot{}, false
	}
	return snap.Provider(provider)
}

// budgetHour truncates t to the top of its UTC hour (the budget_hour PK).
func budgetHour(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}
