// Package k8s ingests cloud-native OSS GitHub issues/PRs (kubernetes,
// controller-runtime, etcd, …) as backtranslation source material via the
// GitHub REST API. The issue title + a short body excerpt become an Artifact;
// doc_id = "<owner>/<repo>#<number>", license = the repo's declared license
// (from config, since a repo license is uniform per repo). The GitHub token is
// read from the env var named by TokenEnv. KEP markdown docs live in the
// kubernetes/enhancements repo and are a future file-based variant (see TODO).
package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/common"
)

// DefaultAPIBaseURL is the GitHub REST API root.
const DefaultAPIBaseURL = "https://api.github.com"

// Repo names one repository and its (uniform) license label.
type Repo struct {
	Owner   string `yaml:"owner"`
	Name    string `yaml:"name"`
	License string `yaml:"license"` // e.g. "Apache-2.0"
}

// Config configures the GitHub-issue source.
type Config struct {
	Enabled         bool   `yaml:"enabled"`
	APIBaseURL      string `yaml:"api_base_url"`
	TokenEnv        string `yaml:"token_env"` // env var holding a GitHub token
	Repos           []Repo `yaml:"repos"`
	State           string `yaml:"state"`     // open | closed | all
	PerPage         int    `yaml:"per_page"`
	MaxExcerptChars int    `yaml:"max_excerpt_chars"`
}

// Source implements artifact.Source for GitHub issues/PRs.
type Source struct {
	cfg  Config
	http common.HTTPDoer
}

// New builds a GitHub-issue source.
func New(cfg Config, httpc common.HTTPDoer) *Source {
	if cfg.APIBaseURL == "" {
		cfg.APIBaseURL = DefaultAPIBaseURL
	}
	if cfg.State == "" {
		cfg.State = "all"
	}
	if cfg.PerPage == 0 {
		cfg.PerPage = 100
	}
	return &Source{cfg: cfg, http: httpc}
}

// SourceType identifies this adapter.
func (s *Source) SourceType() artifact.SourceType { return artifact.SourceK8s }

// ghIssue mirrors the GitHub issue JSON we consume. pull_request is present
// (non-nil) when the issue is actually a PR; we keep both.
type ghIssue struct {
	Number      int             `json:"number"`
	Title       string          `json:"title"`
	Body        string          `json:"body"`
	HTMLURL     string          `json:"html_url"`
	PullRequest json.RawMessage `json:"pull_request"`
}

// Fetch queries each configured repo's issues endpoint for the page identified
// by cursor (a 1-based GitHub `page`; empty = page 1) and returns up to limit
// artifacts total plus the next page number, so successive runs walk deeper into
// each repo instead of re-reading the first page. TODO(network): KEP markdown
// ingestion from kubernetes/enhancements when running against the live API.
func (s *Source) Fetch(ctx context.Context, limit int, cursor string) ([]artifact.Artifact, string, error) {
	if s.http == nil {
		return nil, "", fmt.Errorf("k8s: no HTTP client configured")
	}
	page := common.ParseCursorInt(cursor, 1)
	if page < 1 {
		page = 1
	}
	var token string
	if s.cfg.TokenEnv != "" {
		token = os.Getenv(s.cfg.TokenEnv)
	}
	var out []artifact.Artifact
	for _, repo := range s.cfg.Repos {
		if limit > 0 && len(out) >= limit {
			break
		}
		u := s.issuesURL(repo, page)
		headers := map[string]string{
			"Accept":               "application/vnd.github+json",
			"X-GitHub-Api-Version": "2022-11-28",
		}
		if token != "" {
			headers["Authorization"] = "Bearer " + token
		}
		raw, err := common.GetBytes(ctx, s.http, u, headers)
		if err != nil {
			return nil, "", fmt.Errorf("k8s: fetch %s/%s: %w", repo.Owner, repo.Name, err)
		}
		arts, err := s.parse(raw, repo)
		if err != nil {
			return nil, "", err
		}
		out = append(out, arts...)
	}
	out = common.FilterContaminated(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, common.FormatCursorInt(page + 1), nil
}

func (s *Source) issuesURL(repo Repo, page int) string {
	q := url.Values{}
	q.Set("state", s.cfg.State)
	q.Set("per_page", fmt.Sprintf("%d", s.cfg.PerPage))
	q.Set("page", strconv.Itoa(page))
	return fmt.Sprintf("%s/repos/%s/%s/issues?%s",
		s.cfg.APIBaseURL, repo.Owner, repo.Name, q.Encode())
}

// parse converts a GitHub issues JSON array into artifacts.
func (s *Source) parse(raw []byte, repo Repo) ([]artifact.Artifact, error) {
	var issues []ghIssue
	if err := json.Unmarshal(raw, &issues); err != nil {
		return nil, fmt.Errorf("k8s: parse issues %s/%s: %w", repo.Owner, repo.Name, err)
	}
	now := time.Now().UTC()
	out := make([]artifact.Artifact, 0, len(issues))
	for _, iss := range issues {
		kind := "issue"
		if len(iss.PullRequest) > 0 {
			kind = "pull-request"
		}
		docID := fmt.Sprintf("%s/%s#%d", repo.Owner, repo.Name, iss.Number)
		out = append(out, artifact.Artifact{
			SourceType:  artifact.SourceK8s,
			DocID:       docID,
			URL:         iss.HTMLURL,
			License:     repo.License,
			Title:       collapse(iss.Title),
			Locator:     kind + "-body",
			RetrievedAt: now,
			Chunks: []artifact.Chunk{
				{
					Text:    common.Excerpt(common.StripHTML(iss.Body), s.cfg.MaxExcerptChars),
					Locator: kind + "-body",
				},
			},
		})
	}
	return out, nil
}

func collapse(s string) string {
	return common.Excerpt(s, 0) // reuse whitespace normalisation; titles are short
}
