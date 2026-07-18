// Package teacher provides role-based OpenAI-compatible endpoint clients for
// the distillation pipeline. Each role (teacher/generator/student/judge/
// translator) maps to an endpoint config; two roles may share one endpoint.
// The concrete endpoint is subgate (subscription LLM gateway), but any
// OpenAI-compatible /v1/chat/completions server works.
package teacher

// RoleConfig is one role's endpoint definition.
type RoleConfig struct {
	// BaseURL is the endpoint root; /v1/chat/completions is appended.
	BaseURL string `yaml:"base_url"`
	// Model is the model name sent in the chat completion request.
	Model string `yaml:"model"`
	// APIKeyEnv optionally names an environment variable holding a bearer
	// token. Empty means no Authorization header (subgate holds the OAuth
	// credentials itself).
	APIKeyEnv string `yaml:"api_key_env"`
	// QuotaURL optionally points at subgate's GET /quota for this role's
	// provider. Set on every subgate-backed role: the per-step quota gate reads
	// it before spending this role's subscription (todos sec 6-3-4). Empty means
	// a real API / local model, which is used without a quota check (error-driven
	// only).
	QuotaURL string `yaml:"quota_url"`
	// QuotaProvider names this role's provider in subgate's GET /quota providers[]
	// -- the key the gate matches to find this role's primary(5h)/secondary(weekly)
	// windows. Set alongside quota_url on every subgate role; empty for real-API /
	// local roles (todos sec 6-3-4).
	QuotaProvider string `yaml:"quota_provider"`
	// SourceTag is sent as the X-Subgate-Source header so subgate attributes
	// pipeline usage separately from interactive user consumption.
	SourceTag string `yaml:"source_tag"`
}

// HTTPConfig holds transport-level settings shared by all roles.
type HTTPConfig struct {
	// RequestTimeoutSeconds bounds a single HTTP request (0 = no timeout).
	RequestTimeoutSeconds int `yaml:"request_timeout_seconds"`
	// MaxRetries is the number of retries after the first attempt for
	// retryable failures (429, 5xx, network errors).
	MaxRetries int `yaml:"max_retries"`
	// RetryBaseDelayMS is the first backoff delay; it doubles per retry.
	RetryBaseDelayMS int `yaml:"retry_base_delay_ms"`
}

// Config is the role map plus transport settings.
type Config struct {
	Roles map[string]RoleConfig `yaml:"roles"`
	HTTP  HTTPConfig            `yaml:"http"`
}
