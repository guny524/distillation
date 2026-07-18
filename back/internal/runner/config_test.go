package runner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/teacher"
)

const sampleConfigYAML = `
provider: codex
teacher_role: teacher
roles:
  teacher:
    base_url: http://subgate:8080
    model: gpt-5.4
    quota_url: http://subgate:8080/quota
    source_tag: distillation
  judge:
    base_url: http://local-llm:8000
    model: qwen3-32b
    source_tag: distillation-judge
http:
  request_timeout_seconds: 120
  max_retries: 5
prompt_template: prompts/distillation_api.md
pacing:
  max_items_per_run: 3
pipeline:
  mode: taxonomy
  self_consistency_k: 5
`

// TestLoadConfig_OverlayAndDefaults verifies YAML keys overlay DefaultConfig
// while unset keys keep their defaults (pacing + http partially set).
func TestLoadConfig_OverlayAndDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.yaml")
	require.NoError(t, os.WriteFile(path, []byte(sampleConfigYAML), 0o644))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	assert.Equal(t, "codex", cfg.Provider)
	assert.Equal(t, "teacher", cfg.TeacherRole)

	// Inline roles/http parsed from top-level keys.
	require.Contains(t, cfg.Teacher.Roles, "teacher")
	require.Contains(t, cfg.Teacher.Roles, "judge")
	assert.Equal(t, "http://subgate:8080", cfg.Teacher.Roles["teacher"].BaseURL)
	assert.Equal(t, "http://subgate:8080/quota", cfg.Teacher.Roles["teacher"].QuotaURL)
	assert.Equal(t, "distillation", cfg.Teacher.Roles["teacher"].SourceTag)
	assert.Equal(t, "qwen3-32b", cfg.Teacher.Roles["judge"].Model)

	// Overridden HTTP keys.
	assert.Equal(t, 120, cfg.Teacher.HTTP.RequestTimeoutSeconds)
	assert.Equal(t, 5, cfg.Teacher.HTTP.MaxRetries)
	// Unset HTTP key keeps its default.
	assert.Equal(t, DefaultConfig().Teacher.HTTP.RetryBaseDelayMS, cfg.Teacher.HTTP.RetryBaseDelayMS)

	// Overridden pacing key; unset pacing keys keep spec defaults.
	assert.Equal(t, 3, cfg.Pacing.MaxItemsPerRun)
	assert.InDelta(t, 95.0, cfg.Pacing.PrimaryCapPct, 1e-9)
	assert.InDelta(t, 0.2, cfg.Pacing.EMAAlpha, 1e-9)

	// Unset top-level keys keep defaults.
	assert.Equal(t, DefaultConfig().TaxonomyPath, cfg.TaxonomyPath)
	assert.Equal(t, DefaultConfig().SchemaRetries, cfg.SchemaRetries)

	// Pipeline section overlays; unset pipeline keys keep defaults.
	assert.Equal(t, ModeTaxonomy, cfg.Pipeline.Mode)
	assert.Equal(t, 5, cfg.Pipeline.SelfConsistencyK)
	assert.Equal(t, DefaultConfig().Pipeline.Mutations, cfg.Pipeline.Mutations)
}

// TestLoadConfig_ShippedSettingsYAML gates the config actually shipped in the
// image: config/settings.yaml must parse and validate.
func TestLoadConfig_ShippedSettingsYAML(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("..", "..", "config", "settings.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "codex", cfg.Provider)
	rc := cfg.Teacher.Roles[cfg.TeacherRole]
	assert.NotEmpty(t, rc.BaseURL)
	assert.NotEmpty(t, rc.Model)
	assert.NotEmpty(t, rc.QuotaURL, "pacing-driving role needs quota_url or every run decides k=0")
	assert.NotEmpty(t, rc.SourceTag)

	// The shipped image installs opencode (Dockerfile) and bakes config/
	// opencode.json, so the agentic comprehend integration ships enabled.
	oc := cfg.Comprehend.Opencode
	assert.True(t, oc.Enabled, "shipped settings enable the opencode comprehend integration")
	assert.Equal(t, "comprehend", oc.Agent)
	assert.NotEmpty(t, oc.Model, "opencode needs a provider/model")
	assert.NotEmpty(t, oc.ConfigPath, "opencode needs its provider+agent config path (OPENCODE_CONFIG)")
	assert.Positive(t, oc.Timeout(), "an agentic fetch needs a per-invocation deadline")
}

// TestComprehendConfig_DefaultsAndTimeout: DefaultConfig ships the opencode knobs
// opt-in (so a machine without the binary falls back), and Timeout() converts.
func TestComprehendConfig_DefaultsAndTimeout(t *testing.T) {
	oc := DefaultConfig().Comprehend.Opencode
	assert.False(t, oc.Enabled, "opencode is opt-in by default (fallback without the binary)")
	assert.Equal(t, "opencode", oc.BinPath)
	assert.Equal(t, "comprehend", oc.Agent)
	assert.Equal(t, "subgate/gpt-5.4", oc.Model)
	assert.Equal(t, 600*time.Second, oc.Timeout())
	assert.Equal(t, time.Duration(0), OpencodeConfig{TimeoutSeconds: 0}.Timeout(), "unset -> no deadline")
}

// TestLoadConfig_ComprehendOpencode: the comprehend.opencode block overlays
// DefaultConfig; unset keys keep their defaults.
func TestLoadConfig_ComprehendOpencode(t *testing.T) {
	yaml := sampleConfigYAML + `
comprehend:
  opencode:
    enabled: true
    model: subgate/gpt-5.4
    agent: comprehend
    config_path: /etc/opencode/opencode.json
    timeout_seconds: 120
`
	path := filepath.Join(t.TempDir(), "settings.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	oc := cfg.Comprehend.Opencode
	assert.True(t, oc.Enabled)
	assert.Equal(t, "subgate/gpt-5.4", oc.Model)
	assert.Equal(t, "/etc/opencode/opencode.json", oc.ConfigPath)
	assert.Equal(t, 120*time.Second, oc.Timeout())
	assert.Equal(t, DefaultConfig().Comprehend.Opencode.BinPath, oc.BinPath, "unset bin_path keeps default")
}

// TestLoadConfig_MissingFile: the run config is required.
func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
}

// TestConfig_Validate covers the cross-field invariants.
func TestConfig_Validate(t *testing.T) {
	cfg := testConfig(t)
	require.NoError(t, cfg.Validate())

	bad := cfg
	bad.Provider = ""
	assert.ErrorContains(t, bad.Validate(), "provider")

	bad = cfg
	bad.TeacherRole = "ghost"
	assert.ErrorContains(t, bad.Validate(), "not found in roles")
}

// TestConfig_Validate_MissingSourceTag: the pacing key requires a source tag.
func TestConfig_Validate_MissingSourceTag(t *testing.T) {
	cfg := testConfig(t)
	rc := cfg.Teacher.Roles["teacher"]
	rc.SourceTag = ""
	cfg.Teacher.Roles["teacher"] = rc
	assert.ErrorContains(t, cfg.Validate(), "source_tag")
}

// TestFrontierQuotaWarning: artifact mode fires when a pre-filter role shares
// the teacher endpoint, stays silent when separated or in taxonomy mode.
func TestFrontierQuotaWarning(t *testing.T) {
	base := DefaultConfig()
	base.Pipeline.Mode = ModeArtifact
	base.Pipeline.Roles = pipeline.Roles{}.WithDefaults()

	// Taxonomy mode: never warns.
	tax := base
	tax.Pipeline.Mode = ModeTaxonomy
	assert.Empty(t, tax.FrontierQuotaWarning())

	// All roles share the teacher endpoint -> warns, names the shared stages.
	shared := base
	shared.Teacher.Roles = map[string]teacher.RoleConfig{}
	for _, r := range []string{"teacher", "generator", "student", "judge", "translator", "verifier"} {
		shared.Teacher.Roles[r] = teacher.RoleConfig{BaseURL: "http://svc-subgate:8080", SourceTag: r}
	}
	w := shared.FrontierQuotaWarning()
	assert.Contains(t, w, "student")
	assert.Contains(t, w, "generator")
	assert.Contains(t, w, "http://svc-subgate:8080")

	// Pre-filter roles on a separate local endpoint -> silent.
	sep := base
	sep.Teacher.Roles = map[string]teacher.RoleConfig{
		"teacher": {BaseURL: "http://svc-subgate:8080", SourceTag: "teacher"},
	}
	for _, r := range []string{"generator", "student", "judge", "translator", "verifier"} {
		sep.Teacher.Roles[r] = teacher.RoleConfig{BaseURL: "http://local-vllm:8000", SourceTag: r}
	}
	assert.Empty(t, sep.FrontierQuotaWarning())
}

// TestApplyEndpointEnvOverrides: SUBGATE_ENDPOINT_<ROLE> repoints a role's
// base_url; unset roles, empty values, and other fields are untouched.
func TestApplyEndpointEnvOverrides(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Teacher.Roles = map[string]teacher.RoleConfig{
		"teacher": {BaseURL: "http://subgate:8080", SourceTag: "teacher"},
		"student": {BaseURL: "http://subgate:8080", SourceTag: "student", Model: "m"},
		"judge":   {BaseURL: "http://subgate:8080", SourceTag: "judge"},
	}
	env := map[string]string{
		"SUBGATE_ENDPOINT_STUDENT": "http://local-vllm:8000",
		"SUBGATE_ENDPOINT_JUDGE":   "", // empty -> keep baked value
		"SUBGATE_ENDPOINT_GHOST":   "http://ignored",
	}
	cfg.applyEndpointEnvOverrides(func(k string) string { return env[k] })

	assert.Equal(t, "http://local-vllm:8000", cfg.Teacher.Roles["student"].BaseURL, "student repointed")
	assert.Equal(t, "m", cfg.Teacher.Roles["student"].Model, "other fields untouched")
	assert.Equal(t, "http://subgate:8080", cfg.Teacher.Roles["judge"].BaseURL, "empty env keeps baked value")
	assert.Equal(t, "http://subgate:8080", cfg.Teacher.Roles["teacher"].BaseURL, "unset role untouched")
	assert.NotContains(t, cfg.Teacher.Roles, "ghost", "unknown role not created")
}

// TestRenderPrompt verifies template fields are substituted.
func TestRenderPrompt(t *testing.T) {
	tmpl, err := parsePromptTemplate("id={{.TaskID}} tax={{.TaxonomyYAML}} cov={{.CoverageJSON}}")
	require.NoError(t, err)
	out, err := renderPrompt(tmpl, promptData{TaskID: "t-1", TaxonomyYAML: "y", CoverageJSON: "{}"})
	require.NoError(t, err)
	assert.Equal(t, "id=t-1 tax=y cov={}", out)
}
