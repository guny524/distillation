package ingest

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/common"
)

func TestLoadConfig_ShippedSettingsYAML(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("..", "..", "config", "settings.yaml"))
	require.NoError(t, err)
	// The shipped config enables all four sources (arxiv works API-only;
	// stackexchange/pmc need mounted dumps, k8s wants GITHUB_TOKEN). Category/repo
	// config still parses.
	assert.True(t, cfg.Arxiv.Enabled)
	assert.True(t, cfg.StackExchange.Enabled)
	assert.True(t, cfg.K8s.Enabled)
	assert.True(t, cfg.PMC.Enabled)
	assert.Contains(t, cfg.Arxiv.Categories, "cs")
	assert.Equal(t, "CC BY-SA 4.0", cfg.StackExchange.License)
	assert.NotEmpty(t, cfg.K8s.Repos)
	require.NotNil(t, cfg.PMC.OnlyCommercial)
	assert.True(t, *cfg.PMC.OnlyCommercial)
}

func TestBuildEnabled_OnlyEnabledSources(t *testing.T) {
	cfg := Config{}
	cfg.Arxiv.Enabled = true
	cfg.PMC.Enabled = true
	// stackexchange, k8s left disabled

	reg := NewRegistry(nil)
	sources := reg.BuildEnabled(cfg)
	require.Len(t, sources, 2)

	types := []artifact.SourceType{}
	for _, s := range sources {
		types = append(types, s.SourceType())
	}
	assert.Contains(t, types, artifact.SourceArxiv)
	assert.Contains(t, types, artifact.SourcePMC)
	assert.NotContains(t, types, artifact.SourceK8s)
}

func TestBuildEnabled_NoneEnabled(t *testing.T) {
	reg := NewRegistry(nil)
	assert.Empty(t, reg.BuildEnabled(Config{}))
}

func TestFetchAll_AggregatesAcrossSources(t *testing.T) {
	// PMC now fetches over HTTP (esearch -> efetch), so mock NCBI E-utilities with
	// the pmc adapter's own fixtures. arXiv still supports an offline dump_path, so
	// this exercises aggregation across an offline and an API-backed source through
	// the one injected HTTP client.
	mux := http.NewServeMux()
	mux.HandleFunc("/esearch.fcgi", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("pmc", "testdata", "esearch.json"))
	})
	mux.HandleFunc("/efetch.fcgi", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join("pmc", "testdata", "efetch_articleset.xml"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := Config{}
	cfg.Arxiv.Enabled = true
	cfg.Arxiv.DumpPath = filepath.Join("arxiv", "testdata", "feed.xml")
	cfg.Arxiv.Categories = []string{"cs", "math", "physics", "q-bio"}
	cfg.PMC.Enabled = true
	cfg.PMC.APIBaseURL = srv.URL

	reg := NewRegistry(common.DefaultHTTPClient())
	arts, err := reg.FetchAll(cfg, 0)
	require.NoError(t, err)

	byType := map[artifact.SourceType]int{}
	for _, a := range arts {
		byType[a.SourceType]++
		assert.NotEmpty(t, a.DocID)
		assert.NotEmpty(t, a.License)
	}
	assert.Greater(t, byType[artifact.SourceArxiv], 0)
	assert.Greater(t, byType[artifact.SourcePMC], 0)
}
