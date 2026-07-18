package stackexchange

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/common"
)

func questionsJSON(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/questions.json")
	require.NoError(t, err)
	return b
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(b)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// gzipQuestionsServer serves the questions fixture gzip-encoded, mimicking the
// StackExchange API (which always gzips its JSON). It records the last request
// URL so tests can assert the derived query.
func gzipQuestionsServer(t *testing.T, body []byte) (*httptest.Server, *string) {
	t.Helper()
	var lastURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastURL = r.URL.String()
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(gzipBytes(t, body))
	}))
	t.Cleanup(srv.Close)
	return srv, &lastURL
}

func site() Site {
	return Site{Name: "mathoverflow", URLBase: "https://mathoverflow.net/q/"}
}

func TestParse_KeepsQuestions(t *testing.T) {
	s := New(Config{MinScore: -100}, nil) // keep all incl. negative score
	arts, err := s.parse(questionsJSON(t), site())
	require.NoError(t, err)
	require.Len(t, arts, 3) // contamination not applied in parse

	for _, a := range arts {
		assert.Equal(t, artifact.SourceStackExchange, a.SourceType)
		assert.Equal(t, DefaultLicense, a.License)
	}
	assert.Equal(t, "mathoverflow#101", arts[0].DocID)
	assert.Equal(t, "https://mathoverflow.net/q/101", arts[0].URL)
}

func TestParse_ExcerptCappedAndHTMLStripped(t *testing.T) {
	const cap = 150
	s := New(Config{MaxExcerptChars: cap}, nil)
	arts, err := s.parse(questionsJSON(t), site())
	require.NoError(t, err)
	require.NotEmpty(t, arts)

	body := arts[0].Chunks[0].Text
	assert.LessOrEqual(t, len([]rune(body)), cap+1, "excerpt within cap")
	assert.NotContains(t, body, "<p>", "HTML stripped")
	assert.NotContains(t, body, "<b>", "HTML stripped")
	assert.Contains(t, body, "…", "long body truncated")
}

// TestParse_URLBaseFallback: an item without a link falls back to url_base+id.
func TestParse_URLBaseFallback(t *testing.T) {
	raw := []byte(`{"items":[{"question_id":55,"score":3,"title":"t","body":"<p>b</p>"}]}`)
	s := New(Config{}, nil)
	arts, err := s.parse(raw, site())
	require.NoError(t, err)
	require.Len(t, arts, 1)
	assert.Equal(t, "https://mathoverflow.net/q/55", arts[0].URL)
}

func TestFetch_GzipMinScoreAndContamination(t *testing.T) {
	srv, lastURL := gzipQuestionsServer(t, questionsJSON(t))
	s := New(Config{
		APIBaseURL: srv.URL,
		Sites:      []Site{site()},
		MinScore:   0, // drops the -3 question (104)
	}, common.DefaultHTTPClient())

	arts, next, err := s.Fetch(context.Background(), 0, "")
	require.NoError(t, err)

	ids := map[string]bool{}
	for _, a := range arts {
		ids[a.DocID] = true
	}
	assert.True(t, ids["mathoverflow#101"], "gzip response decoded and parsed")
	assert.False(t, ids["mathoverflow#103"], "GPQA/MCQ contamination filtered")
	assert.False(t, ids["mathoverflow#104"], "min_score dropped")

	// The request carried the expected query: derived site slug + votes sort +
	// withbody filter (so the question body is present in the response).
	require.NotNil(t, lastURL)
	u, err := url.Parse(*lastURL)
	require.NoError(t, err)
	assert.Equal(t, "/questions", u.Path)
	assert.Equal(t, "mathoverflow", u.Query().Get("site"))
	assert.Equal(t, "withbody", u.Query().Get("filter"))
	assert.Equal(t, "votes", u.Query().Get("sort"))
	assert.Equal(t, "1", u.Query().Get("page"), "empty cursor starts at page 1")
	assert.Equal(t, "2", next)
}

// TestFetch_CursorAdvancesPage: the saved cursor selects the API page and the
// next cursor points one page deeper, so successive runs walk down each site's
// vote ranking instead of re-reading the top page.
func TestFetch_CursorAdvancesPage(t *testing.T) {
	srv, lastURL := gzipQuestionsServer(t, questionsJSON(t))
	s := New(Config{APIBaseURL: srv.URL, Sites: []Site{site()}, MinScore: -100}, common.DefaultHTTPClient())

	_, next, err := s.Fetch(context.Background(), 0, "5")
	require.NoError(t, err)
	u, err := url.Parse(*lastURL)
	require.NoError(t, err)
	assert.Equal(t, "5", u.Query().Get("page"))
	assert.Equal(t, "6", next)
}

// TestLicenseFor_ByCreationDate: a post is bound by the CC BY-SA version in
// force at its creation date, not blanket-labeled with the current version.
func TestLicenseFor_ByCreationDate(t *testing.T) {
	s := New(Config{}, nil)
	assert.Equal(t, "CC BY-SA 2.5", s.licenseFor(time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC).Unix()))
	assert.Equal(t, "CC BY-SA 3.0", s.licenseFor(time.Date(2015, 6, 1, 0, 0, 0, 0, time.UTC).Unix()))
	assert.Equal(t, "CC BY-SA 4.0", s.licenseFor(time.Date(2020, 9, 13, 0, 0, 0, 0, time.UTC).Unix()))
	assert.Equal(t, DefaultLicense, s.licenseFor(0), "missing creation date falls back to configured default")
}

// TestFetch_APISiteDerivation: "math.stackexchange" -> API site slug "math".
func TestFetch_APISiteDerivation(t *testing.T) {
	srv, lastURL := gzipQuestionsServer(t, questionsJSON(t))
	s := New(Config{
		APIBaseURL: srv.URL,
		Sites:      []Site{{Name: "math.stackexchange"}},
	}, common.DefaultHTTPClient())

	_, _, err := s.Fetch(context.Background(), 0, "")
	require.NoError(t, err)

	u, err := url.Parse(*lastURL)
	require.NoError(t, err)
	assert.Equal(t, "math", u.Query().Get("site"))
}

func TestFetch_LimitRespected(t *testing.T) {
	srv, _ := gzipQuestionsServer(t, questionsJSON(t))
	s := New(Config{APIBaseURL: srv.URL, Sites: []Site{site()}}, common.DefaultHTTPClient())
	arts, _, err := s.Fetch(context.Background(), 1, "")
	require.NoError(t, err)
	assert.Len(t, arts, 1)
}

func TestFetch_NoClientErrors(t *testing.T) {
	s := New(Config{Sites: []Site{site()}}, nil)
	_, _, err := s.Fetch(context.Background(), 0, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no HTTP client")
}
