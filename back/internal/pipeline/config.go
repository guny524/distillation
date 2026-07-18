package pipeline

// Roles names the endpoint role each stage calls. Multiple stages may share a
// role, and roles may share an endpoint (resolved by the teacher client). The
// pipeline never hardcodes a model; it only names roles, keeping endpoint
// selection in config (todos sec 1-1: "역할별 endpoint를 config로 정의").
type Roles struct {
	// Generator answers backtranslation and difficulty-mutation prompts.
	Generator string `yaml:"generator"`
	// Student answers the question k times for the self-consistency pre-filter.
	Student string `yaml:"student"`
	// Judge scores the student answer against the reference answer sketch.
	Judge string `yaml:"judge"`
	// Translator builds the missing ko/en side of the 2-lane pair.
	Translator string `yaml:"translator"`
	// Teacher generates the structured trajectory per lane (the paced role).
	Teacher string `yaml:"teacher"`
	// Verifier checks the teacher trajectory (rule/exec/test/none).
	Verifier string `yaml:"verifier"`
}

// WithDefaults fills unset role names with their conventional defaults so a
// minimal config still resolves every stage.
func (r Roles) WithDefaults() Roles {
	if r.Generator == "" {
		r.Generator = "generator"
	}
	if r.Student == "" {
		r.Student = "student"
	}
	if r.Judge == "" {
		r.Judge = "judge"
	}
	if r.Translator == "" {
		r.Translator = "translator"
	}
	if r.Teacher == "" {
		r.Teacher = "teacher"
	}
	if r.Verifier == "" {
		r.Verifier = "verifier"
	}
	return r
}

// Config parameterizes the pipeline. TeacherModel/TeacherProvider are authored
// by the runner from its own config (the credit-mechanism source of truth) and
// stamped onto every record, exactly like task_id.
type Config struct {
	// Roles maps each stage to an endpoint role name.
	Roles Roles `yaml:"roles"`
	// SelfConsistencyK is how many times the student answers each question for
	// the disagreement signal. <=1 disables the self-consistency component.
	SelfConsistencyK int `yaml:"self_consistency_k"`
	// SelfConsistencyThreshold is the disagreement rate (0..1) at or above which
	// the student pre-filter flags the question uncertain (routes to teacher).
	SelfConsistencyThreshold float64 `yaml:"self_consistency_threshold"`
	// Mutations is the enabled difficulty-mutation operator allow-list. Only
	// operators in this set (and in model.ValidDifficultyMutations) are applied.
	Mutations []string `yaml:"mutations"`
	// Executor configures real code-execution verification. Disabled by
	// default; when off, verify falls back to LLM-rule verification.
	Executor ExecutorConfig `yaml:"executor"`
	// TeacherModel/TeacherProvider are recorded on every produced record.
	TeacherModel    string `yaml:"-"`
	TeacherProvider string `yaml:"-"`
}

// ExecutorConfig parameterizes the verifier's code executor. It is DISABLED by
// default (Enabled false): the pipeline executes untrusted, model-generated
// code, so real execution must be an explicit, deliberate opt-in on a host that
// is locked down (isolated container, deny-all egress). See SubprocessExecutor
// for the isolation guarantees and their limits.
type ExecutorConfig struct {
	// Enabled turns real execution on. When false the verifier uses LLM-rule
	// verification only (current behavior preserved).
	Enabled bool `yaml:"enabled"`
	// TimeoutSeconds is the hard wall-clock deadline per snippet.
	TimeoutSeconds int `yaml:"timeout_seconds"`
	// MaxOutputBytes caps captured stdout and stderr each (memory bound against
	// runaway output).
	MaxOutputBytes int `yaml:"max_output_bytes"`
	// SandboxCommand is an optional wrapper argv prefixed before the language
	// command. This is where real network/namespace isolation is plugged in,
	// e.g. ["unshare","-rn","--"] or ["firejail","--net=none","--"]. Empty by
	// default: without it, executed code is NOT network-isolated by this code
	// (rely on the container's NetworkPolicy).
	SandboxCommand []string `yaml:"sandbox_command"`
	// Languages maps a canonical language key ("python","go") to its runner.
	Languages map[string]LanguageRunner `yaml:"languages"`
}

// LanguageRunner describes how to run one language: the source filename written
// into the sandbox temp dir and the argv executed there (cwd = temp dir).
type LanguageRunner struct {
	// Filename is the source file name inside the sandbox dir.
	Filename string `yaml:"filename"`
	// Command is the argv run with the sandbox dir as cwd. It should reference
	// Filename (e.g. ["python3","solution.py"] or ["go","run","main.go"]).
	Command []string `yaml:"command"`
}

// DefaultExecutorConfig returns the disabled-by-default executor settings with
// Python and Go runners pre-populated, so flipping enabled: true is enough to
// verify those languages.
func DefaultExecutorConfig() ExecutorConfig {
	return ExecutorConfig{
		Enabled:        false,
		TimeoutSeconds: 20,
		MaxOutputBytes: 64 * 1024,
		Languages: map[string]LanguageRunner{
			"python": {Filename: "solution.py", Command: []string{"python3", "solution.py"}},
			"go":     {Filename: "main.go", Command: []string{"go", "run", "main.go"}},
		},
	}
}

// DefaultConfig returns the recommended pipeline defaults. Role names use the
// conventional per-stage names; two named roles may still resolve to one shared
// endpoint via the teacher role map.
func DefaultConfig() Config {
	return Config{
		Roles:                    Roles{}.WithDefaults(),
		SelfConsistencyK:         3,
		SelfConsistencyThreshold: 0.5,
		Mutations: []string{
			"fault_injection", "partial_info_removal", "conflicting_constraint",
		},
		Executor: DefaultExecutorConfig(),
	}
}

// normalized returns a copy with defaults applied to any unset field, so New
// tolerates a partially-populated config.
func (c Config) normalized() Config {
	c.Roles = c.Roles.WithDefaults()
	if c.SelfConsistencyK == 0 {
		c.SelfConsistencyK = 3
	}
	if c.SelfConsistencyThreshold == 0 {
		c.SelfConsistencyThreshold = 0.5
	}
	c.Executor = c.Executor.normalized()
	return c
}

// normalized fills unset executor fields with their defaults so a config that
// only sets enabled: true still has a sane timeout, output cap, and the built-in
// Python/Go runners.
func (e ExecutorConfig) normalized() ExecutorConfig {
	def := DefaultExecutorConfig()
	if e.TimeoutSeconds <= 0 {
		e.TimeoutSeconds = def.TimeoutSeconds
	}
	if e.MaxOutputBytes <= 0 {
		e.MaxOutputBytes = def.MaxOutputBytes
	}
	if len(e.Languages) == 0 {
		e.Languages = def.Languages
	}
	return e
}
