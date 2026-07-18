package teacher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// ErrRateLimited marks a request that exhausted retries on HTTP 429. Callers
// (the run orchestration) match it with errors.Is to record a rate_limited run
// log, which feeds the pacing cooldown.
var ErrRateLimited = errors.New("rate limited (HTTP 429)")

// ErrTransient marks a request that failed on a retryable class (HTTP 429, 5xx,
// or a network/timeout error) AND exhausted the client's own backoff retries.
// It wraps the final cause so callers can distinguish a temporary endpoint
// problem (worth a queue-level retry with a longer backoff) from a permanent
// contract/validation failure. A non-retryable failure (4xx other than 429,
// request-build errors) is NEVER wrapped with it.
var ErrTransient = errors.New("transient failure (retries exhausted)")

// IsTransient reports whether err is a transient endpoint failure (see
// ErrTransient). A 429 is always transient (it also wraps ErrRateLimited).
func IsTransient(err error) bool {
	return errors.Is(err, ErrTransient) || errors.Is(err, ErrRateLimited)
}

// chatCompletionsPath is the OpenAI-compatible endpoint path appended to
// each role's base_url.
const chatCompletionsPath = "/v1/chat/completions"

// maxResponseBytes caps a response body read (matches loader's 10MB line cap).
const maxResponseBytes = 10 << 20

// Message is one chat message in an OpenAI-compatible request.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Completion is the reasoning-aware result of one chat completion (todos sec
// 2-5-5 / 5-3). Content is the in-band answer (always present). ReasoningContent
// is the provider's raw chain-of-thought from message.reasoning_content, which
// only open-weight models expose (DeepSeek-R1/Qwen3/Kimi); it is "" for
// subscription providers that return an in-band summary only. There is no
// provider branching: the prompt always forces an in-band cot, and this struct
// additionally carries reasoning_content when the endpoint happens to return it.
type Completion struct {
	Content          string
	ReasoningContent string
	Provider         ProviderMetadata
}

// ProviderMetadata is response-level provenance echoed by OpenAI-compatible
// endpoints, recorded to attribute a trajectory to its teacher.
type ProviderMetadata struct {
	Model        string // echoed response.model
	FinishReason string // choices[0].finish_reason ("stop", "length", ...)
}

// HasRawCoT reports whether the provider returned a raw reasoning_content
// (maps to the schema's has_raw_cot flag).
func (c Completion) HasRawCoT() bool { return c.ReasoningContent != "" }

// Client issues role-routed requests to OpenAI-compatible endpoints.
type Client struct {
	cfg   Config
	httpc *http.Client
	// sleep is time.Sleep-compatible; tests can shorten backoff via config
	// (retry_base_delay_ms) so no injection is needed, but keeping it a field
	// documents the seam.
	sleep func(time.Duration)
}

// NewClient validates the config and returns a client. Every role must have a
// base_url and model; quota_url/api_key_env/source_tag are optional per role.
func NewClient(cfg Config) (*Client, error) {
	if len(cfg.Roles) == 0 {
		return nil, fmt.Errorf("teacher: no roles configured")
	}
	for name, rc := range cfg.Roles {
		if rc.BaseURL == "" {
			return nil, fmt.Errorf("teacher: role %q missing base_url", name)
		}
		if rc.Model == "" {
			return nil, fmt.Errorf("teacher: role %q missing model", name)
		}
	}
	return &Client{cfg: cfg, httpc: &http.Client{}, sleep: time.Sleep}, nil
}

// Role returns the config for a role name.
func (c *Client) Role(name string) (RoleConfig, error) {
	rc, ok := c.cfg.Roles[name]
	if !ok {
		return RoleConfig{}, fmt.Errorf("teacher: unknown role %q", name)
	}
	return rc, nil
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type chatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Content string `json:"content"`
			// reasoning_content is the raw chain-of-thought some OpenAI-compatible
			// open-weight endpoints return; absent (-> "") for summary-only
			// subscription providers (todos sec 2-5-5).
			ReasoningContent string `json:"reasoning_content"`
		} `json:"message"`
	} `json:"choices"`
}

// Complete sends a non-streaming chat completion request to the role's endpoint
// and returns the first choice as a reasoning-aware Completion. ReasoningContent
// is populated from message.reasoning_content when present, "" otherwise.
func (c *Client) Complete(ctx context.Context, role string, msgs []Message) (Completion, error) {
	rc, err := c.Role(role)
	if err != nil {
		return Completion{}, err
	}
	payload, err := json.Marshal(chatRequest{Model: rc.Model, Messages: msgs, Stream: false})
	if err != nil {
		return Completion{}, fmt.Errorf("teacher: marshal chat request: %w", err)
	}
	url := strings.TrimRight(rc.BaseURL, "/") + chatCompletionsPath
	body, err := c.doWithRetry(ctx, http.MethodPost, url, rc, payload)
	if err != nil {
		return Completion{}, err
	}
	var resp chatResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return Completion{}, fmt.Errorf("teacher: parse chat response: %w", err)
	}
	if len(resp.Choices) == 0 {
		return Completion{}, fmt.Errorf("teacher: chat response has no choices")
	}
	choice := resp.Choices[0]
	return Completion{
		Content:          choice.Message.Content,
		ReasoningContent: choice.Message.ReasoningContent,
		Provider:         ProviderMetadata{Model: resp.Model, FinishReason: choice.FinishReason},
	}, nil
}

// ChatCompletion returns just the in-band content of one completion. It is the
// content-only view of Complete, kept so the existing monolithic runner/pipeline
// call sites compile unchanged while the reasoning-aware workers adopt Complete.
func (c *Client) ChatCompletion(ctx context.Context, role string, msgs []Message) (string, error) {
	comp, err := c.Complete(ctx, role, msgs)
	if err != nil {
		return "", err
	}
	return comp.Content, nil
}

// FetchQuota GETs the role's quota_url and returns the raw body. The caller
// parses it with pacing.ParseSnapshot; this package stays quota-format-agnostic.
func (c *Client) FetchQuota(ctx context.Context, role string) ([]byte, error) {
	rc, err := c.Role(role)
	if err != nil {
		return nil, err
	}
	if rc.QuotaURL == "" {
		return nil, fmt.Errorf("teacher: role %q has no quota_url", role)
	}
	return c.doWithRetry(ctx, http.MethodGet, rc.QuotaURL, rc, nil)
}

// FetchQuotaURL GETs an arbitrary quota URL and returns the raw body. It is the
// URL-based counterpart of FetchQuota, for the per-step quota gate (todos sec
// 6-3-4): the gate dedups fetches by quota_url across a step's roles, and one of
// its providers (the comprehend stage's opencode provider) is not a teacher role
// at all. No role headers are sent -- GET /quota is a status read, not an
// X-Subgate-Source-attributed /v1 call.
func (c *Client) FetchQuotaURL(ctx context.Context, url string) ([]byte, error) {
	if url == "" {
		return nil, fmt.Errorf("teacher: empty quota url")
	}
	return c.doWithRetry(ctx, http.MethodGet, url, RoleConfig{}, nil)
}

// doWithRetry runs doOnce with exponential backoff on retryable failures
// (429, 5xx, network errors). Non-retryable failures return immediately.
func (c *Client) doWithRetry(ctx context.Context, method, url string, rc RoleConfig, payload []byte) ([]byte, error) {
	attempts := c.cfg.HTTP.MaxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	base := time.Duration(c.cfg.HTTP.RetryBaseDelayMS) * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := base << (attempt - 1)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			c.sleep(delay)
		}
		body, retryable, err := c.doOnce(ctx, method, url, rc, payload)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
	}
	// Every attempt failed on a retryable class: surface it as transient so the
	// queue worker reschedules the item instead of terminally failing it.
	return nil, fmt.Errorf("%w: %w", ErrTransient, lastErr)
}

// doOnce performs one HTTP round trip. retryable reports whether the failure
// class is worth retrying.
func (c *Client) doOnce(ctx context.Context, method, url string, rc RoleConfig, payload []byte) (body []byte, retryable bool, err error) {
	reqCtx := ctx
	if c.cfg.HTTP.RequestTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, time.Duration(c.cfg.HTTP.RequestTimeoutSeconds)*time.Second)
		defer cancel()
	}

	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, url, reader)
	if err != nil {
		return nil, false, fmt.Errorf("teacher: build request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if rc.SourceTag != "" {
		req.Header.Set("X-Subgate-Source", rc.SourceTag)
	}
	if rc.APIKeyEnv != "" {
		if key := os.Getenv(rc.APIKeyEnv); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
	}

	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("teacher: %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, true, fmt.Errorf("teacher: read response body: %w", err)
	}

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, true, fmt.Errorf("teacher: %s %s: %w: %s", method, url, ErrRateLimited, bodySnippet(b))
	case resp.StatusCode == http.StatusRequestTimeout:
		// 408 Request Timeout is a TRANSIENT server-side signal that the request
		// took too long to arrive; retrying (a fresh connection) can succeed. It
		// would otherwise fall into the >=300 non-retryable arm below and fail the
		// item permanently. Scoped to 408 only: other 4xx (409/425/...) stay
		// non-retryable, matching their permanent/precondition semantics.
		return nil, true, fmt.Errorf("teacher: %s %s: request timeout %d: %s", method, url, resp.StatusCode, bodySnippet(b))
	case resp.StatusCode >= 500:
		return nil, true, fmt.Errorf("teacher: %s %s: server error %d: %s", method, url, resp.StatusCode, bodySnippet(b))
	case resp.StatusCode >= 300:
		return nil, false, fmt.Errorf("teacher: %s %s: unexpected status %d: %s", method, url, resp.StatusCode, bodySnippet(b))
	}
	return b, false, nil
}

// bodySnippet truncates an error body for log-friendly error messages.
func bodySnippet(b []byte) string {
	const max = 200
	s := strings.TrimSpace(string(b))
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}
