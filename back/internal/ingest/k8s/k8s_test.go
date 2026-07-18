package k8s

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/artifact"
)

// recordingDoer returns fixture bytes and captures the request for header/URL
// assertions, standing in for the GitHub API.
type recordingDoer struct {
	body    []byte
	lastReq *http.Request
}

func (d *recordingDoer) Do(req *http.Request) (*http.Response, error) {
	d.lastReq = req
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(d.body)),
		Header:     make(http.Header),
	}, nil
}

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/issues.json")
	require.NoError(t, err)
	return b
}

func repo() Repo {
	return Repo{Owner: "kubernetes-sigs", Name: "controller-runtime", License: "Apache-2.0"}
}

func TestParse_DocIDLicenseAndKind(t *testing.T) {
	s := New(Config{}, nil)
	arts, err := s.parse(fixture(t), repo())
	require.NoError(t, err)
	require.Len(t, arts, 3)

	assert.Equal(t, artifact.SourceK8s, arts[0].SourceType)
	assert.Equal(t, "kubernetes-sigs/controller-runtime#12345", arts[0].DocID)
	assert.Equal(t, "Apache-2.0", arts[0].License)
	assert.Equal(t, "issue-body", arts[0].Locator)

	// second item is a PR (pull_request present)
	assert.Equal(t, "pull-request-body", arts[1].Locator)
	assert.Equal(t, "kubernetes-sigs/controller-runtime#6789", arts[1].DocID)
}

func TestParse_ExcerptCapped(t *testing.T) {
	const cap = 200
	s := New(Config{MaxExcerptChars: cap}, nil)
	arts, err := s.parse(fixture(t), repo())
	require.NoError(t, err)
	txt := []rune(arts[0].Chunks[0].Text)
	assert.LessOrEqual(t, len(txt), cap+1)
	assert.Contains(t, arts[0].Chunks[0].Text, "…")
}

func TestFetch_InjectedClient_HeadersAndFilter(t *testing.T) {
	t.Setenv("GH_TOKEN_TEST", "secret-token")
	doer := &recordingDoer{body: fixture(t)}
	s := New(Config{
		Repos:    []Repo{repo()},
		TokenEnv: "GH_TOKEN_TEST",
	}, doer)

	arts, next, err := s.Fetch(context.Background(), 0, "")
	require.NoError(t, err)

	// contamination (MMLU / Answer: B) filtered out -> 2 of 3 remain
	assert.Len(t, arts, 2)
	for _, a := range arts {
		assert.NotContains(t, a.Title, "MMLU")
	}

	require.NotNil(t, doer.lastReq)
	assert.Equal(t, "Bearer secret-token", doer.lastReq.Header.Get("Authorization"))
	assert.Contains(t, doer.lastReq.URL.String(), "/repos/kubernetes-sigs/controller-runtime/issues")
	assert.Contains(t, doer.lastReq.URL.String(), "state=all")
	assert.Contains(t, doer.lastReq.URL.String(), "page=1", "empty cursor starts at page 1")
	assert.Equal(t, "2", next)
}

// TestFetch_CursorAdvancesPage: the saved cursor selects the GitHub page and the
// next cursor points one page deeper, so successive runs walk into each repo's
// history instead of re-reading page 1.
func TestFetch_CursorAdvancesPage(t *testing.T) {
	doer := &recordingDoer{body: fixture(t)}
	s := New(Config{Repos: []Repo{repo()}}, doer)
	_, next, err := s.Fetch(context.Background(), 0, "3")
	require.NoError(t, err)
	require.NotNil(t, doer.lastReq)
	assert.Contains(t, doer.lastReq.URL.String(), "page=3")
	assert.Equal(t, "4", next)
}

func TestFetch_NoClientErrors(t *testing.T) {
	s := New(Config{Repos: []Repo{repo()}}, nil)
	_, _, err := s.Fetch(context.Background(), 0, "")
	require.Error(t, err)
}

func TestFetch_LimitRespected(t *testing.T) {
	s := New(Config{Repos: []Repo{repo()}}, &recordingDoer{body: fixture(t)})
	arts, _, err := s.Fetch(context.Background(), 1, "")
	require.NoError(t, err)
	assert.Len(t, arts, 1)
}
