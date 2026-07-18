package arxiv

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

// bodyDoer returns fixed bytes for any request, standing in for the network so
// the real HTTP fetch path is exercised without touching the network. It keeps
// the last requested URL for pagination assertions.
type bodyDoer struct {
	body    []byte
	lastURL string
}

func (d *bodyDoer) Do(req *http.Request) (*http.Response, error) {
	d.lastURL = req.URL.String()
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(d.body)),
		Header:     make(http.Header),
	}, nil
}

func loadFixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/feed.xml")
	require.NoError(t, err)
	return b
}

func TestParse_CategoryFilter(t *testing.T) {
	s := New(Config{Categories: []string{"cs", "math", "q-bio", "physics"}}, nil)
	arts, err := s.parse(loadFixture(t))
	require.NoError(t, err)

	// econ.GN entry dropped; cs.LG, math.AG, physics.ed-ph kept (physics one is
	// contamination but parse() does not filter — Fetch does).
	ids := map[string]bool{}
	for _, a := range arts {
		ids[a.DocID] = true
		assert.Equal(t, artifact.SourceArxiv, a.SourceType)
		assert.NotEmpty(t, a.License)
		assert.Equal(t, "abstract", a.Locator)
	}
	assert.True(t, ids["2401.07013"], "cs.LG kept, version stripped")
	assert.True(t, ids["2402.00001"], "math.AG kept")
	assert.False(t, ids["2403.00002"], "econ.GN dropped by category filter")
}

func TestParse_VersionStrippedAndURL(t *testing.T) {
	s := New(Config{Categories: []string{"cs"}}, nil)
	arts, err := s.parse(loadFixture(t))
	require.NoError(t, err)
	require.NotEmpty(t, arts)
	a := arts[0]
	assert.Equal(t, "2401.07013", a.DocID)
	assert.Equal(t, "https://arxiv.org/abs/2401.07013", a.URL)
}

func TestParse_ExcerptLengthCapped(t *testing.T) {
	const cap = 200
	s := New(Config{Categories: []string{"cs"}, MaxExcerptChars: cap}, nil)
	arts, err := s.parse(loadFixture(t))
	require.NoError(t, err)
	require.NotEmpty(t, arts)
	// The long abstract must be truncated: no full-verbatim copy in the chunk.
	txt := []rune(arts[0].Chunks[0].Text)
	assert.LessOrEqual(t, len(txt), cap+1, "excerpt (+ellipsis) within cap")
	assert.Contains(t, arts[0].Chunks[0].Text, "…", "long abstract truncated with ellipsis")
}

func TestFetch_FromDump_FiltersContamination(t *testing.T) {
	s := New(Config{
		Categories: []string{"cs", "math", "physics", "q-bio"},
		DumpPath:   "testdata/feed.xml",
	}, nil)
	arts, _, err := s.Fetch(context.Background(), 0, "")
	require.NoError(t, err)
	for _, a := range arts {
		assert.NotContains(t, a.Title, "GPQA", "benchmark entry filtered out")
		assert.NotEqual(t, "2404.00003", a.DocID)
	}
}

func TestFetch_FromAPI_UsesInjectedClient(t *testing.T) {
	doer := &bodyDoer{body: loadFixture(t)}
	s := New(Config{Categories: []string{"cs"}, Query: "cat:cs.LG"}, doer)
	arts, next, err := s.Fetch(context.Background(), 1, "")
	require.NoError(t, err)
	require.Len(t, arts, 1)
	assert.Equal(t, "2401.07013", arts[0].DocID)
	assert.Equal(t, "1", next, "next cursor = offset + page size")
	assert.Contains(t, doer.lastURL, "start=0", "first run queries offset 0")
}

// TestFetch_FromAPI_CursorAdvancesOffset: a second run resumes from the saved
// cursor, querying the API at that start offset and reporting the offset past
// the page it just read — successive ingest runs walk forward, never re-reading
// the front page.
func TestFetch_FromAPI_CursorAdvancesOffset(t *testing.T) {
	doer := &bodyDoer{body: loadFixture(t)}
	s := New(Config{Categories: []string{"cs"}, Query: "cat:cs.LG"}, doer)
	_, next, err := s.Fetch(context.Background(), 2, "2")
	require.NoError(t, err)
	assert.Equal(t, "4", next)
	assert.Contains(t, doer.lastURL, "start=2", "resumes from the saved offset")
}

func TestFetch_LimitRespected(t *testing.T) {
	s := New(Config{Categories: []string{"cs", "math", "physics", "q-bio"}, DumpPath: "testdata/feed.xml"}, nil)
	arts, _, err := s.Fetch(context.Background(), 1, "")
	require.NoError(t, err)
	assert.Len(t, arts, 1)
}

// TestFetch_FromDump_CursorStopsAtEnd: the dump path advances the cursor only by
// what it actually yields, so paging a static file terminates instead of running
// past the end forever.
func TestFetch_FromDump_CursorStopsAtEnd(t *testing.T) {
	s := New(Config{Categories: []string{"cs", "math", "physics", "q-bio"}, DumpPath: "testdata/feed.xml"}, nil)
	arts, next, err := s.Fetch(context.Background(), 100, "")
	require.NoError(t, err)
	require.NotEmpty(t, arts)
	// Resume from the reported end: nothing further, cursor stays put.
	again, next2, err := s.Fetch(context.Background(), 100, next)
	require.NoError(t, err)
	assert.Empty(t, again)
	assert.Equal(t, next, next2)
}

func TestGroupMatches(t *testing.T) {
	assert.True(t, groupMatches("cs.LG", "cs"))
	assert.True(t, groupMatches("math.AG", "math"))
	assert.True(t, groupMatches("math-ph", "math"))
	assert.True(t, groupMatches("q-bio.NC", "q-bio"))
	assert.True(t, groupMatches("hep-th", "physics"))
	assert.True(t, groupMatches("physics.optics", "physics"))
	assert.False(t, groupMatches("econ.GN", "cs"))
	assert.False(t, groupMatches("cs.LG", "math"))
}
