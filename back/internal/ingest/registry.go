// Package ingest wires the four artifact ingestion source adapters (arxiv,
// stackexchange, k8s, pmc) behind a config-driven Registry. It lives in the
// parent package rather than internal/ingest/common because building concrete
// adapters requires importing them, and common must stay a leaf (every adapter
// imports common, so common importing an adapter would be a cycle). The Registry
// reads the `ingest:` section of config/settings.yaml and returns the
// artifact.Source list for the enabled sources.
package ingest

import (
	"context"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/arxiv"
	"github.com/guny524/distillation/internal/ingest/common"
	"github.com/guny524/distillation/internal/ingest/k8s"
	"github.com/guny524/distillation/internal/ingest/pmc"
	"github.com/guny524/distillation/internal/ingest/stackexchange"
)

// Config is the `ingest:` section of settings.yaml: one sub-config per source,
// each with its own `enabled` flag.
type Config struct {
	// LimitPerSource caps how many artifacts each enabled source contributes per
	// ingest run. The ingest cadence is a deployment concern (ofelia @every N /
	// k8s CronJob schedule), NOT code; this is only the per-run batch size. 0
	// means "use the built-in default".
	LimitPerSource int                  `yaml:"limit_per_source"`
	Arxiv          arxiv.Config         `yaml:"arxiv"`
	StackExchange  stackexchange.Config `yaml:"stackexchange"`
	K8s            k8s.Config           `yaml:"k8s"`
	PMC            pmc.Config           `yaml:"pmc"`
}

// settingsFile is the on-disk shape: only the ingest key is read here so the
// same file can also hold the runner/pacing config without conflict.
type settingsFile struct {
	Ingest Config `yaml:"ingest"`
}

// LoadConfig reads the ingest section from a settings YAML file. Unknown keys
// (runner/pacing config) are ignored.
func LoadConfig(path string) (Config, error) {
	var sf settingsFile
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read ingest config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, &sf); err != nil {
		return Config{}, fmt.Errorf("parse ingest config %s: %w", path, err)
	}
	return sf.Ingest, nil
}

// Registry builds the enabled artifact.Source adapters from a Config. The HTTP
// client is injected so tests (and the future `distill ingest` command) control
// the network seam; pass common.DefaultHTTPClient() in production.
type Registry struct {
	httpc common.HTTPDoer
}

// NewRegistry returns a Registry using the given HTTP client for the
// network-backed sources. All four adapters fetch over HTTP now (arxiv API,
// stackexchange API, k8s GitHub API, pmc NCBI E-utilities), so a non-nil client
// is required unless every enabled source can run offline (only arxiv with a
// local dump_path).
func NewRegistry(httpc common.HTTPDoer) *Registry {
	return &Registry{httpc: httpc}
}

// BuildEnabled returns one artifact.Source per source whose `enabled: true` is
// set in cfg. The order is deterministic (arxiv, stackexchange, k8s, pmc).
func (r *Registry) BuildEnabled(cfg Config) []artifact.Source {
	var sources []artifact.Source
	if cfg.Arxiv.Enabled {
		sources = append(sources, arxiv.New(cfg.Arxiv, r.httpc))
	}
	if cfg.StackExchange.Enabled {
		sources = append(sources, stackexchange.New(cfg.StackExchange, r.httpc))
	}
	if cfg.K8s.Enabled {
		sources = append(sources, k8s.New(cfg.K8s, r.httpc))
	}
	if cfg.PMC.Enabled {
		sources = append(sources, pmc.New(cfg.PMC, r.httpc))
	}
	return sources
}

// FetchAll fetches from every enabled source, tagging errors with the source
// type and continuing past a single source's failure so one broken dump does
// not abort the whole run. limitPerSource bounds each source's contribution.
// This is the stateless one-shot aggregate path (the `distill run` artifact-mode
// batch): it always starts each source from the front (empty cursor) and drops
// the returned next cursor. Incremental, cursor-resuming ingestion is the
// IngestWorker's job (internal/worker), which persists the cursor between runs.
func (r *Registry) FetchAll(cfg Config, limitPerSource int) ([]artifact.Artifact, error) {
	sources := r.BuildEnabled(cfg)
	var all []artifact.Artifact
	var errs []error
	for _, s := range sources {
		arts, _, err := s.Fetch(context.Background(), limitPerSource, "")
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.SourceType(), err))
			continue
		}
		all = append(all, arts...)
	}
	if len(errs) > 0 {
		return all, fmt.Errorf("ingest: %d source(s) failed: %v", len(errs), errs)
	}
	return all, nil
}
