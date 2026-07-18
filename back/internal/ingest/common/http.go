// Package common holds utilities shared by every ingestion source adapter:
// the HTTP client seam (so network fetches can be mocked in tests), the
// benchmark-contamination filter, license judgement, and the short-excerpt
// helper that enforces the copyright rule (Chunk.Text is a rephrasing anchor,
// never a long verbatim passage). It is a leaf package: it imports
// internal/artifact but never any adapter package, so no import cycle forms.
package common

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxResponseBytes caps a fetched body read (matches loader/teacher 10MB cap)
// so a malformed or hostile response cannot exhaust memory.
const maxResponseBytes = 64 << 20 // 64MB (dump-scale API pages)

// HTTPDoer is the seam every network-backed adapter depends on instead of a
// concrete *http.Client. Production wiring passes DefaultHTTPClient; tests pass
// a stub that returns fixture bytes, so the real fetch path is exercised
// without touching the network (network is a user-run integration concern).
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// DefaultHTTPClient returns an *http.Client with a sane request timeout for
// ingestion crawls.
func DefaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 60 * time.Second}
}

// GetBytes issues a GET with the given headers and returns the response body.
// Non-2xx responses are an error. The body read is capped at maxResponseBytes.
func GetBytes(ctx context.Context, doer HTTPDoer, url string, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request %s: %w", url, err)
	}
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := doer.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return body, nil
}

// Gunzip inflates b when it carries the gzip magic header (0x1f 0x8b) and
// returns it unchanged otherwise. The StackExchange API gzip-encodes every
// response regardless of Accept-Encoding; an adapter that sets
// Accept-Encoding: gzip itself (so Go's transport does NOT transparently
// inflate — the transport only auto-inflates responses to a gzip request it
// added on its own) routes the body through this to decode it. The magic-byte
// guard makes the call a safe no-op when the body is already plain text, so a
// caller need not know whether the transport happened to inflate it. The
// inflated read is capped at maxResponseBytes to bound a decompression bomb.
func Gunzip(b []byte) ([]byte, error) {
	if len(b) < 2 || b[0] != 0x1f || b[1] != 0x8b {
		return b, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("gunzip: %w", err)
	}
	defer zr.Close()
	out, err := io.ReadAll(io.LimitReader(zr, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("gunzip: read: %w", err)
	}
	return out, nil
}
