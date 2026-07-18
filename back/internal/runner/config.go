// Package runner orchestrates one CronJob run of the distillation pipeline:
// advisory lock -> GET /quota -> pacing.Decide -> k generations (coverage ->
// prompt -> teacher call -> schema validation -> DB insert) -> /quota
// re-observation -> pacing.UpdateEMA -> run log completion.
package runner

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/guny524/distillation/internal/flush"
	"github.com/guny524/distillation/internal/pacing"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/teacher"
)

// Pipeline generation modes.
const (
	// ModeArtifact is the default M3 mode: reverse-generate questions from
	// ingested artifacts, student-pre-filter, 2-lane, verify.
	ModeArtifact = "artifact"
	// ModeTaxonomy is the legacy fallback: the teacher invents a task from the
	// 4-axis taxonomy.
	ModeTaxonomy = "taxonomy"
)

// DefaultConfigPath is where `distill run` looks for its config. Relative to
// the working directory (/workspace in the container).
const DefaultConfigPath = "config/settings.yaml"

// Config is the full `distill run` configuration: role endpoints, pacing
// parameters, and prompt assembly inputs. Loaded from one YAML file; unset
// keys keep DefaultConfig values.
type Config struct {
	// Provider is the quota provider name matched against /quota providers[]
	// and used in pacing state keys (e.g. "codex").
	Provider string `yaml:"provider"`
	// TeacherRole names the role (in Teacher.Roles) that answers generation
	// prompts and whose quota_url drives pacing.
	TeacherRole string `yaml:"teacher_role"`
	// Teacher holds the role endpoint map + HTTP transport settings (the
	// roles: and http: top-level YAML keys).
	Teacher teacher.Config `yaml:",inline"`
	// PromptTemplatePath is the Go text/template rendered per generation.
	PromptTemplatePath string `yaml:"prompt_template"`
	// TaxonomyPath is the 4-axis taxonomy YAML embedded into the prompt.
	TaxonomyPath string `yaml:"taxonomy_path"`
	// SchemaRetries is how many re-requests are made when a teacher response
	// fails JSON extraction / schema validation.
	SchemaRetries int `yaml:"schema_retries"`
	// StaleRunAfterMinutes marks `running` run logs older than this as
	// crashed_recovered at startup. Must exceed the CronJob
	// activeDeadlineSeconds plus grace.
	StaleRunAfterMinutes int `yaml:"stale_run_after_minutes"`
	// Pacing holds the quota pacing parameters (see internal/pacing).
	Pacing pacing.Config `yaml:"pacing"`
	// Pipeline selects and parameterizes the question-generation pipeline
	// (artifact reverse-generation vs legacy taxonomy creation).
	Pipeline PipelineConfig `yaml:"pipeline"`
	// Flush configures the DB-buffer -> export-volume flush -> DB-clear cycle. When
	// enabled, a run that leaves distillation_pairs at/over threshold_rows
	// merge-exports the buffer to a compressed Parquet batch on the export volume and
	// deletes the flushed rows (see internal/flush).
	Flush flush.Config `yaml:"flush"`
	// Worker holds the DB-queue stage-worker knobs (todos sec 2-5).
	Worker WorkerConfig `yaml:"worker"`
	// Comprehend configures the comprehend stage worker's opencode agentic
	// harness (todos sec 2-5-4).
	Comprehend ComprehendConfig `yaml:"comprehend"`
}

// ComprehendConfig configures the comprehend stage worker (todos sec 2-5-4): the
// opencode agentic harness that fetches a source link, reads it with
// auto-compaction, and produces the comprehension digest the question worker
// backtranslates from.
type ComprehendConfig struct {
	// Opencode is the opencode CLI integration. Disabled by default so the stage
	// runs anywhere (falling back to the provenance-only digest); the deployed
	// image installs opencode and the config enables it.
	Opencode OpencodeConfig `yaml:"opencode"`
}

// OpencodeConfig parameterizes the `opencode run` invocation the comprehend
// worker shells out to. The provider (subgate baseURL / api key) and the agent
// definition live in the opencode config FILE (config_path, mounted by the
// deploy overlay), not here; these keys are only what the CLI call itself needs,
// so nothing about the endpoint is hardcoded in the binary (todos sec 2-5-4).
type OpencodeConfig struct {
	// Enabled turns on the opencode subprocess. false -> the worker uses the
	// provenance fallback digest (buildDigest), so a machine without opencode
	// (local dev, CI) still runs the stage.
	Enabled bool `yaml:"enabled"`
	// BinPath is the opencode executable; "opencode" resolves on PATH.
	BinPath string `yaml:"bin_path"`
	// ConfigPath is exported as OPENCODE_CONFIG so opencode loads the mounted
	// provider(subgate)+agent config regardless of its working directory.
	ConfigPath string `yaml:"config_path"`
	// Model is the "provider/model" passed to `opencode run --model`.
	Model string `yaml:"model"`
	// Agent is the agent name passed to `opencode run --agent`.
	Agent string `yaml:"agent"`
	// TimeoutSeconds bounds one opencode invocation (agentic fetch+read of a
	// large source can be slow). <=0 means no worker-side deadline.
	TimeoutSeconds int `yaml:"timeout_seconds"`
	// QuotaURL points the comprehend stage's quota gate at subgate's GET /quota
	// for the opencode provider (todos sec 6-3-4: "comprehend -> opencode가 쓰는
	// provider"). opencode reads the source THROUGH subgate, so the same /quota
	// facts gate it. Empty (or Enabled=false) disables the comprehend gate --
	// the provenance fallback makes no network call, so it spends no quota.
	QuotaURL string `yaml:"quota_url"`
	// QuotaProvider names the opencode provider in that /quota response (the key
	// the gate matches). Set alongside quota_url.
	QuotaProvider string `yaml:"quota_provider"`
}

// Timeout is the per-invocation deadline as a duration (0 when unset).
func (o OpencodeConfig) Timeout() time.Duration {
	if o.TimeoutSeconds <= 0 {
		return 0
	}
	return time.Duration(o.TimeoutSeconds) * time.Second
}

// WorkerConfig holds DB-queue stage-worker knobs (todos sec 2-5).
type WorkerConfig struct {
	// StaleLockMinutes is how long a claimed item's lock may sit before another
	// worker reclaims it as stale (crash recovery). It MUST exceed a stage's
	// worst-case processing time -- roughly max_teacher_calls_per_stage x
	// request_timeout_seconds x (1 + max_retries) -- or a live worker doing slow
	// work is wrongly reclaimed and its item is redone (wasted quota). Fencing
	// keeps that safe (no corruption), but the lease should still be generous. A
	// value <= 0 uses queue.DefaultStaleLock.
	StaleLockMinutes int `yaml:"stale_lock_minutes"`
}

// StaleLock returns the configured stale-lock duration, or 0 (which
// queue.NewStore maps to its default) when unset.
func (w WorkerConfig) StaleLock() time.Duration {
	if w.StaleLockMinutes <= 0 {
		return 0
	}
	return time.Duration(w.StaleLockMinutes) * time.Minute
}

// PipelineConfig selects the generation mode and parameterizes the M3 artifact
// reverse-generation pipeline. Mode defaults to artifact; taxonomy is the
// legacy fallback that leaves the M3 fields empty.
type PipelineConfig struct {
	// Mode is "artifact" (default) or "taxonomy".
	Mode string `yaml:"mode"`
	// SelfConsistencyK is the student self-consistency sample count.
	SelfConsistencyK int `yaml:"self_consistency_k"`
	// SelfConsistencyThreshold is the disagreement rate that flags uncertain.
	SelfConsistencyThreshold float64 `yaml:"self_consistency_threshold"`
	// Mutations is the enabled difficulty-mutation operator allow-list.
	Mutations []string `yaml:"mutations"`
	// Roles maps each pipeline stage to an endpoint role name (must exist in
	// the roles: map for artifact mode).
	Roles pipeline.Roles `yaml:"roles"`
	// Executor configures real code-execution verification. Disabled by
	// default (zero value); enable in settings.yaml under pipeline.executor
	// only inside a sandboxed container (see pipeline.ExecutorConfig).
	Executor pipeline.ExecutorConfig `yaml:"executor"`
}

// ToPipelineConfig exposes toPipelineConfig for the queue stage workers
// (internal/worker), which build their own pipeline from the same run config the
// monolithic runner uses (roles, mutations, executor, teacher model/provider).
func (c Config) ToPipelineConfig() pipeline.Config { return c.toPipelineConfig() }

// toPipelineConfig builds the pipeline.Config, stamping the teacher model
// (from the pipeline teacher role's endpoint) and provider (the pacing
// provider) as the credit-mechanism source of truth.
func (c Config) toPipelineConfig() pipeline.Config {
	roles := c.Pipeline.Roles.WithDefaults()
	return pipeline.Config{
		Roles:                    roles,
		SelfConsistencyK:         c.Pipeline.SelfConsistencyK,
		SelfConsistencyThreshold: c.Pipeline.SelfConsistencyThreshold,
		Mutations:                c.Pipeline.Mutations,
		Executor:                 c.Pipeline.Executor,
		TeacherModel:             c.Teacher.Roles[roles.Teacher].Model,
		TeacherProvider:          c.Provider,
	}
}

// DefaultConfig returns the recommended defaults for everything except the
// role endpoint map, which has no sensible default and must come from YAML.
func DefaultConfig() Config {
	return Config{
		Provider:           "codex",
		TeacherRole:        "teacher",
		PromptTemplatePath: "prompts/distillation_api.md",
		TaxonomyPath:       "config/taxonomy.yaml",
		SchemaRetries:      1,
		// CronJob activeDeadlineSeconds is 1800 (30m); 45m adds grace.
		StaleRunAfterMinutes: 45,
		Teacher: teacher.Config{
			HTTP: teacher.HTTPConfig{
				RequestTimeoutSeconds: 300,
				MaxRetries:            3,
				RetryBaseDelayMS:      2000,
			},
		},
		Pacing: pacing.DefaultConfig(),
		Pipeline: PipelineConfig{
			Mode:                     ModeArtifact,
			SelfConsistencyK:         3,
			SelfConsistencyThreshold: 0.5,
			Mutations: []string{
				"fault_injection", "partial_info_removal", "conflicting_constraint",
			},
			Roles: pipeline.Roles{}.WithDefaults(),
		},
		Flush: flush.DefaultConfig(),
		// Default stale lock (90m) matches queue.DefaultStaleLock: comfortably
		// above the slowest stage's pathological all-timeout processing time.
		Worker: WorkerConfig{StaleLockMinutes: 90},
		// Comprehend opencode integration: opt-in (enabled via settings.yaml in
		// the image that installs opencode); the CLI knobs default to the image's
		// baked opencode config file and the subgate-backed model.
		Comprehend: ComprehendConfig{
			Opencode: OpencodeConfig{
				Enabled:        false,
				BinPath:        "opencode",
				ConfigPath:     "config/opencode.json",
				Model:          "subgate/gpt-5.4",
				Agent:          "comprehend",
				TimeoutSeconds: 600,
			},
		},
	}
}

// LoadConfig reads a YAML file over DefaultConfig. The file is required: a run
// without role endpoints cannot do anything useful.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read run config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("parse run config %s: %w", path, err)
	}
	cfg.applyEndpointEnvOverrides(os.Getenv)
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("run config %s: %w", path, err)
	}
	return cfg, nil
}

// applyEndpointEnvOverrides repoints a role's endpoint from an env var so a
// deployment can change it without rebuilding the image or editing the baked-in
// settings.yaml: for each role, SUBGATE_ENDPOINT_<UPPER(role)> (e.g.
// SUBGATE_ENDPOINT_STUDENT) overrides that role's base_url when set non-empty.
// Only short per-environment scalars go through env; structured config
// (taxonomy, prompt, mutation list) stays in the baked settings.yaml. getenv is
// injected for testability.
func (c *Config) applyEndpointEnvOverrides(getenv func(string) string) {
	for name, rc := range c.Teacher.Roles {
		if v := getenv("SUBGATE_ENDPOINT_" + strings.ToUpper(name)); v != "" {
			rc.BaseURL = v
			c.Teacher.Roles[name] = rc
		}
	}
}

// Validate checks cross-field invariants the runner depends on.
func (c Config) Validate() error {
	if c.Provider == "" {
		return fmt.Errorf("provider must not be empty")
	}
	rc, ok := c.Teacher.Roles[c.TeacherRole]
	if !ok {
		return fmt.Errorf("teacher_role %q not found in roles", c.TeacherRole)
	}
	if rc.SourceTag == "" {
		return fmt.Errorf("role %q needs source_tag (pacing state key + X-Subgate-Source)", c.TeacherRole)
	}
	if err := c.validatePipeline(); err != nil {
		return err
	}
	if err := c.Flush.Validate(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// FrontierQuotaWarning returns a non-empty warning when, in artifact mode, any
// cheap pre-filter/support role (generator/student/judge/translator/verifier)
// shares the teacher role's endpoint. That defeats the pipeline's purpose: the
// student pre-filter exists to spend the paced frontier quota ONLY on teacher
// trajectories, running the filter on cheap local models (cluster.md). Sharing
// the teacher endpoint burns frontier quota on the filter itself. Pacing's
// account-wide headroom still prevents rate-limit overrun, but per-item EMA
// under-counts and quota is wasted. Returns "" when correctly separated or not
// in artifact mode.
func (c Config) FrontierQuotaWarning() string {
	if c.Pipeline.Mode != ModeArtifact {
		return ""
	}
	roles := c.Pipeline.Roles.WithDefaults()
	teacherURL := c.Teacher.Roles[roles.Teacher].BaseURL
	if teacherURL == "" {
		return ""
	}
	shared := make([]string, 0, 5)
	for _, r := range []struct{ stage, name string }{
		{"generator", roles.Generator}, {"student", roles.Student},
		{"judge", roles.Judge}, {"translator", roles.Translator},
		{"verifier", roles.Verifier},
	} {
		if c.Teacher.Roles[r.name].BaseURL == teacherURL {
			shared = append(shared, r.stage)
		}
	}
	if len(shared) == 0 {
		return ""
	}
	return fmt.Sprintf("pre-filter roles [%s] share the teacher endpoint %q; "+
		"frontier quota will be spent on the student pre-filter/support calls, "+
		"not just teacher trajectories. Point these roles at local LLMs (cluster.md) "+
		"to reserve frontier quota for the teacher.", strings.Join(shared, ", "), teacherURL)
}

// validatePipeline checks the generation mode and, for artifact mode, that
// every pipeline stage role resolves to a configured endpoint.
func (c Config) validatePipeline() error {
	switch c.Pipeline.Mode {
	case "", ModeTaxonomy:
		return nil
	case ModeArtifact:
		roles := c.Pipeline.Roles.WithDefaults()
		for stage, name := range map[string]string{
			"generator": roles.Generator, "student": roles.Student,
			"judge": roles.Judge, "translator": roles.Translator,
			"teacher": roles.Teacher, "verifier": roles.Verifier,
		} {
			if _, ok := c.Teacher.Roles[name]; !ok {
				return fmt.Errorf("pipeline mode artifact: %s role %q not found in roles", stage, name)
			}
		}
		return nil
	default:
		return fmt.Errorf("pipeline.mode %q must be %q or %q", c.Pipeline.Mode, ModeArtifact, ModeTaxonomy)
	}
}
