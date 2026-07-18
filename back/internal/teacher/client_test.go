package teacher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chatOK writes a minimal OpenAI-compatible chat completion response.
func chatOK(w http.ResponseWriter, content string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"choices": []map[string]any{
			{"message": map[string]any{"role": "assistant", "content": content}},
		},
	})
}

// testConfig builds a single-role config pointing at a test server.
func testConfig(role, baseURL string) Config {
	return Config{
		Roles: map[string]RoleConfig{
			role: {BaseURL: baseURL, Model: "test-model", SourceTag: "distillation"},
		},
		HTTP: HTTPConfig{RequestTimeoutSeconds: 5, MaxRetries: 2, RetryBaseDelayMS: 1},
	}
}

// TestNewClient_Validation rejects configs missing roles, base_url, or model.
func TestNewClient_Validation(t *testing.T) {
	_, err := NewClient(Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no roles")

	_, err = NewClient(Config{Roles: map[string]RoleConfig{"teacher": {Model: "m"}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing base_url")

	_, err = NewClient(Config{Roles: map[string]RoleConfig{"teacher": {BaseURL: "http://x"}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing model")
}

// TestChatCompletion_RoleRouting verifies two roles route to their own
// endpoints with their own models, and that a shared endpoint is allowed.
func TestChatCompletion_RoleRouting(t *testing.T) {
	var teacherModel, judgeModel atomic.Value

	teacherSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/chat/completions", r.URL.Path)
		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		teacherModel.Store(req["model"].(string))
		chatOK(w, "teacher-answer")
	}))
	defer teacherSrv.Close()

	judgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		judgeModel.Store(req["model"].(string))
		chatOK(w, "judge-answer")
	}))
	defer judgeSrv.Close()

	cfg := Config{
		Roles: map[string]RoleConfig{
			"teacher": {BaseURL: teacherSrv.URL, Model: "gpt-5.4"},
			"judge":   {BaseURL: judgeSrv.URL, Model: "judge-model"},
			// judge2 shares the judge endpoint: allowed by design.
			"judge2": {BaseURL: judgeSrv.URL, Model: "judge-model"},
		},
		HTTP: HTTPConfig{MaxRetries: 0, RetryBaseDelayMS: 1},
	}
	c, err := NewClient(cfg)
	require.NoError(t, err)

	out, err := c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "teacher-answer", out)
	assert.Equal(t, "gpt-5.4", teacherModel.Load())

	out, err = c.ChatCompletion(context.Background(), "judge", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "judge-answer", out)
	assert.Equal(t, "judge-model", judgeModel.Load())

	_, err = c.ChatCompletion(context.Background(), "nope", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown role "nope"`)
}

// TestChatCompletion_Headers verifies X-Subgate-Source and Authorization
// (from api_key_env) are sent.
func TestChatCompletion_Headers(t *testing.T) {
	t.Setenv("TEST_SUBGATE_KEY", "sekrit")

	var gotSource, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSource = r.Header.Get("X-Subgate-Source")
		gotAuth = r.Header.Get("Authorization")
		chatOK(w, "ok")
	}))
	defer srv.Close()

	cfg := testConfig("teacher", srv.URL)
	rc := cfg.Roles["teacher"]
	rc.APIKeyEnv = "TEST_SUBGATE_KEY"
	cfg.Roles["teacher"] = rc

	c, err := NewClient(cfg)
	require.NoError(t, err)
	_, err = c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "distillation", gotSource)
	assert.Equal(t, "Bearer sekrit", gotAuth)
}

// TestChatCompletion_RetryOn429ThenSuccess verifies backoff-retry recovers
// from a transient 429.
func TestChatCompletion_RetryOn429ThenSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		chatOK(w, "recovered")
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	out, err := c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "recovered", out)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

// TestChatCompletion_RateLimitExhausted verifies a persistent 429 surfaces as
// ErrRateLimited after all retries.
func TestChatCompletion_RateLimitExhausted(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	_, err = c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRateLimited), "want ErrRateLimited, got: %v", err)
	// MaxRetries=2 -> 3 attempts total.
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

// TestChatCompletion_RetryOn5xx verifies 5xx responses are retried.
func TestChatCompletion_RetryOn5xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) <= 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		chatOK(w, "up-again")
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	out, err := c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "up-again", out)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

// TestChatCompletion_RetryOn408 (D2) verifies a 408 Request Timeout is treated as
// transient and retried, then recovers -- it must NOT fall into the non-retryable
// >=300 arm that would fail the item permanently.
func TestChatCompletion_RetryOn408(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusRequestTimeout) // 408
			return
		}
		chatOK(w, "recovered-after-408")
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	out, err := c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "recovered-after-408", out)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls), "408 was retried, not failed permanently")
}

// TestChatCompletion_408ExhaustedIsTransient (D2) verifies a persistent 408
// surfaces as ErrTransient (a retryable-class exhaustion the queue reschedules),
// never a permanent "unexpected status" failure -- 408 is timing, not contract.
func TestChatCompletion_408ExhaustedIsTransient(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusRequestTimeout)
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	_, err = c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.Error(t, err)
	assert.True(t, IsTransient(err), "408 exhaustion must be transient (queue retries), got: %v", err)
	assert.NotContains(t, err.Error(), "unexpected status", "408 must not be classified non-retryable")
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls), "MaxRetries=2 -> 3 attempts")
}

// TestChatCompletion_NoRetryOn4xx verifies non-429 4xx fails immediately.
func TestChatCompletion_NoRetryOn4xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	_, err = c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status 400")
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

// TestChatCompletion_EmptyChoices verifies an empty choices array is an error.
func TestChatCompletion_EmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{}})
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	_, err = c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no choices")
}

// TestFetchQuota verifies the quota GET path and the missing-quota_url error.
func TestFetchQuota(t *testing.T) {
	quotaBody := `{"providers":[{"provider":"codex","primary":{"used_percent":10,"resets_at":"2026-07-12T10:00:00Z"},"secondary":{"used_percent":20,"resets_at":"2026-07-15T00:00:00Z"}}],"observed_at":"2026-07-12T05:00:00Z"}`
	var gotSource string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/quota", r.URL.Path)
		gotSource = r.Header.Get("X-Subgate-Source")
		_, _ = w.Write([]byte(quotaBody))
	}))
	defer srv.Close()

	cfg := testConfig("teacher", srv.URL)
	rc := cfg.Roles["teacher"]
	rc.QuotaURL = srv.URL + "/quota"
	cfg.Roles["teacher"] = rc

	c, err := NewClient(cfg)
	require.NoError(t, err)
	b, err := c.FetchQuota(context.Background(), "teacher")
	require.NoError(t, err)
	assert.JSONEq(t, quotaBody, string(b))
	assert.Equal(t, "distillation", gotSource)

	// Role without quota_url.
	c2, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	_, err = c2.FetchQuota(context.Background(), "teacher")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no quota_url")
}

// TestFetchQuotaURL verifies the URL-based quota GET used by the per-step gate: it
// GETs an arbitrary URL, sends NO X-Subgate-Source header (a status read, not an
// attributed /v1 call), and rejects an empty URL.
func TestFetchQuotaURL(t *testing.T) {
	quotaBody := `{"providers":[{"provider":"codex","primary":{"used_percent":10,"resets_at":"2026-07-12T10:00:00Z"},"secondary":{"used_percent":20,"resets_at":"2026-07-15T00:00:00Z"}}]}`
	var gotSource, gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotSource = r.Header.Get("X-Subgate-Source")
		_, _ = w.Write([]byte(quotaBody))
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)

	b, err := c.FetchQuotaURL(context.Background(), srv.URL+"/quota")
	require.NoError(t, err)
	assert.JSONEq(t, quotaBody, string(b))
	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Equal(t, "/quota", gotPath)
	assert.Empty(t, gotSource, "GET /quota is a status read; no source-tag header is sent")

	_, err = c.FetchQuotaURL(context.Background(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty quota url")
}

// TestComplete_ParsesReasoningContent verifies an open-weight response with
// message.reasoning_content is surfaced as Completion.ReasoningContent, along
// with the provider metadata (model/finish_reason). (todos sec 2-5-5)
func TestComplete_ParsesReasoningContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "deepseek-r1",
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message": map[string]any{
					"role":              "assistant",
					"content":           "42",
					"reasoning_content": "first I recalled the answer to everything",
				},
			}},
		})
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	comp, err := c.Complete(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "42", comp.Content)
	assert.Equal(t, "first I recalled the answer to everything", comp.ReasoningContent)
	assert.True(t, comp.HasRawCoT())
	assert.Equal(t, "deepseek-r1", comp.Provider.Model)
	assert.Equal(t, "stop", comp.Provider.FinishReason)
}

// TestComplete_NoReasoningContent verifies a subscription/summary-only response
// (no reasoning_content) falls back to an empty ReasoningContent, HasRawCoT
// false, while Content still parses.
func TestComplete_NoReasoningContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		chatOK(w, "in-band answer only")
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	comp, err := c.Complete(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "in-band answer only", comp.Content)
	assert.Equal(t, "", comp.ReasoningContent)
	assert.False(t, comp.HasRawCoT())
}

// TestChatCompletion_ContentOnlyWrapsComplete verifies the legacy string method
// returns Complete's Content (the monolithic call sites keep working).
func TestChatCompletion_ContentOnlyWrapsComplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message": map[string]any{"content": "answer", "reasoning_content": "hidden cot"},
			}},
		})
	}))
	defer srv.Close()

	c, err := NewClient(testConfig("teacher", srv.URL))
	require.NoError(t, err)
	out, err := c.ChatCompletion(context.Background(), "teacher", []Message{{Role: "user", Content: "q"}})
	require.NoError(t, err)
	assert.Equal(t, "answer", out) // reasoning_content is dropped by the content-only view
}
