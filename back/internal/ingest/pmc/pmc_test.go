package pmc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/common"
)

func readFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}

// eutilsServer mocks NCBI E-utilities: esearch.fcgi returns the id list,
// efetch.fcgi returns the <pmc-articleset> JATS. Records the last esearch and
// efetch queries for pagination/id assertions.
func eutilsServer(t *testing.T) (*httptest.Server, *string, *string) {
	t.Helper()
	var lastEsearchQuery, lastEfetchQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/esearch.fcgi", func(w http.ResponseWriter, r *http.Request) {
		lastEsearchQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(readFile(t, "esearch.json"))
	})
	mux.HandleFunc("/efetch.fcgi", func(w http.ResponseWriter, r *http.Request) {
		lastEfetchQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write(readFile(t, "efetch_articleset.xml"))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &lastEsearchQuery, &lastEfetchQuery
}

// --- parseArticle: single <article> blob path (retained fixtures) ---

func TestParseArticle_CommercialLicenseKept(t *testing.T) {
	s := New(Config{}, nil)
	a, ok, err := s.parseArticle(readFile(t, "by.nxml"), time.Now())
	require.NoError(t, err)
	require.True(t, ok, "CC BY article must be kept")

	assert.Equal(t, artifact.SourcePMC, a.SourceType)
	assert.Equal(t, "PMC7654321", a.DocID)
	assert.Contains(t, a.URL, "PMC7654321")
	assert.Contains(t, a.License, "creativecommons.org/licenses/by/4.0")
	assert.Equal(t, "abstract", a.Locator)
	assert.NotEmpty(t, a.Chunks[0].Text)
}

func TestParseArticle_NonCommercialFilteredByDefault(t *testing.T) {
	s := New(Config{}, nil) // OnlyCommercial defaults to true
	_, ok, err := s.parseArticle(readFile(t, "by-nc.nxml"), time.Now())
	require.NoError(t, err)
	assert.False(t, ok, "CC BY-NC article must be filtered when only_commercial_use is on")
}

func TestParseArticle_NonCommercialKeptWhenFilterDisabled(t *testing.T) {
	s := NewWithCommercialFilter(Config{}, nil, false)
	_, ok, err := s.parseArticle(readFile(t, "by-nc.nxml"), time.Now())
	require.NoError(t, err)
	assert.True(t, ok, "with filter disabled, NC article is parsed")
}

func TestParseArticle_ExcerptCapped(t *testing.T) {
	const cap = 180
	s := New(Config{MaxExcerptChars: cap}, nil)
	a, ok, err := s.parseArticle(readFile(t, "by.nxml"), time.Now())
	require.NoError(t, err)
	require.True(t, ok)
	txt := []rune(a.Chunks[0].Text)
	assert.LessOrEqual(t, len(txt), cap+1, "no long verbatim abstract in chunk")
	assert.Contains(t, a.Chunks[0].Text, "…")
}

// --- parseArticleSet: efetch <pmc-articleset> path ---

func TestParseArticleSet_FiltersLicenseKeepsPMCID(t *testing.T) {
	s := New(Config{}, nil)
	arts, err := s.parseArticleSet(readFile(t, "efetch_articleset.xml"), time.Now())
	require.NoError(t, err)

	ids := map[string]bool{}
	for _, a := range arts {
		assert.Equal(t, artifact.SourcePMC, a.SourceType)
		ids[a.DocID] = true
	}
	assert.True(t, ids["PMC7654321"], "CC BY kept")
	assert.False(t, ids["PMC7654322"], "CC BY-NC filtered by license")
	// Contamination is applied in Fetch, not parseArticleSet, so 7654323 is here.
	assert.True(t, ids["PMC7654323"], "benchmark article still parsed at set level")
	assert.True(t, ids["PMC7654324"], "second CC BY kept")
}

// --- Fetch: esearch -> efetch, with license + contamination gates ---

func TestFetch_EsearchEfetchFiltersLicenseAndContamination(t *testing.T) {
	srv, lastEsearch, lastEfetch := eutilsServer(t)
	s := New(Config{APIBaseURL: srv.URL}, common.DefaultHTTPClient())

	arts, next, err := s.Fetch(context.Background(), 0, "")
	require.NoError(t, err)

	ids := map[string]bool{}
	for _, a := range arts {
		ids[a.DocID] = true
	}
	assert.True(t, ids["PMC7654321"], "CC BY kept")
	assert.True(t, ids["PMC7654324"], "second CC BY kept")
	assert.False(t, ids["PMC7654322"], "CC BY-NC filtered by license")
	assert.False(t, ids["PMC7654323"], "PubMedQA benchmark filtered by contamination")

	// esearch started at the front, efetch received the ids esearch returned.
	require.NotNil(t, lastEsearch)
	assert.Contains(t, *lastEsearch, "retstart=0")
	require.NotNil(t, lastEfetch)
	assert.Contains(t, *lastEfetch, "id=7654321")
	// The cursor advances past every id scanned (4), not only the 2 that survived
	// the license/contamination filters, so a fully-filtered page still advances.
	assert.Equal(t, "4", next)
}

// TestFetch_CursorAdvancesRetstart: the saved cursor is the esearch retstart, so
// successive runs scan disjoint id ranges instead of re-reading the front.
func TestFetch_CursorAdvancesRetstart(t *testing.T) {
	srv, lastEsearch, _ := eutilsServer(t)
	s := New(Config{APIBaseURL: srv.URL}, common.DefaultHTTPClient())

	_, next, err := s.Fetch(context.Background(), 0, "40")
	require.NoError(t, err)
	require.NotNil(t, lastEsearch)
	assert.Contains(t, *lastEsearch, "retstart=40")
	assert.Equal(t, "44", next, "40 + the 4 ids the fixture esearch returned")
}

func TestFetch_LimitRespected(t *testing.T) {
	srv, _, _ := eutilsServer(t)
	s := New(Config{APIBaseURL: srv.URL}, common.DefaultHTTPClient())
	arts, _, err := s.Fetch(context.Background(), 1, "")
	require.NoError(t, err)
	assert.Len(t, arts, 1) // two commercial articles survive filtering; limit trims to 1
}

func TestFetch_NoClientErrors(t *testing.T) {
	s := New(Config{}, nil)
	_, _, err := s.Fetch(context.Background(), 0, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no HTTP client")
}

func TestNormalizePMCID(t *testing.T) {
	assert.Equal(t, "PMC123", normalizePMCID("123"))
	assert.Equal(t, "PMC123", normalizePMCID("PMC123"))
	assert.Equal(t, "PMC123", normalizePMCID("pmc123"))
	assert.Equal(t, "", normalizePMCID(""))
}
