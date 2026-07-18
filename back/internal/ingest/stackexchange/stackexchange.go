// Package stackexchange ingests StackExchange questions (CC BY-SA) as
// backtranslation source material by querying the live StackExchange API
// (api.stackexchange.com/2.3/questions) instead of a local Posts.xml dump. It
// keeps only questions, stores a short excerpt of the HTML-stripped body — never
// the full post — and drops below-min_score items. doc_id = "<site>#<question_id>",
// license = CC BY-SA. The API always gzip-encodes its JSON, so responses are
// inflated via common.Gunzip.
package stackexchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/common"
)

// CC BY-SA version labels for StackExchange content. The network switched
// license twice; a post is bound by whichever version was in force at its
// creation date (see licenseFor). All three permit commercial use + derivatives.
const (
	licenseBySA25 = "CC BY-SA 2.5"
	licenseBySA30 = "CC BY-SA 3.0"
	licenseBySA40 = "CC BY-SA 4.0"
)

// DefaultLicense is the StackExchange content license for current content and the
// fallback when a question carries no creation date.
const DefaultLicense = licenseBySA40

// DefaultAPIBaseURL is the StackExchange API v2.3 root (the /questions path is
// appended). Overridable via config so tests point it at a mock server.
const DefaultAPIBaseURL = "https://api.stackexchange.com/2.3"

// maxPageSize is the StackExchange API's hard per-request page cap.
const maxPageSize = 100

// Site names one StackExchange site to crawl.
type Site struct {
	Name string `yaml:"name"` // "mathoverflow", "math.stackexchange", "physics.stackexchange"
	// APISite overrides the API `site` slug derived from Name (see apiSite). Set
	// it only for sites whose slug is not Name-with-".stackexchange"-trimmed.
	APISite string `yaml:"api_site"`
	// URLBase is the fallback question-URL prefix used only when the API response
	// omits the canonical `link` (e.g. "https://mathoverflow.net/q/").
	URLBase string `yaml:"url_base"`
}

// Config configures the StackExchange source.
type Config struct {
	Enabled         bool   `yaml:"enabled"`
	APIBaseURL      string `yaml:"api_base_url"`
	Sites           []Site `yaml:"sites"`
	License         string `yaml:"license"`
	MinScore        int    `yaml:"min_score"` // drop questions below this score
	MaxExcerptChars int    `yaml:"max_excerpt_chars"`
}

// Source implements artifact.Source for the StackExchange API.
type Source struct {
	cfg  Config
	http common.HTTPDoer
}

// New builds a StackExchange source. httpc is the network seam (pass
// common.DefaultHTTPClient in production, a mock in tests).
func New(cfg Config, httpc common.HTTPDoer) *Source {
	if cfg.APIBaseURL == "" {
		cfg.APIBaseURL = DefaultAPIBaseURL
	}
	if cfg.License == "" {
		cfg.License = DefaultLicense
	}
	return &Source{cfg: cfg, http: httpc}
}

// SourceType identifies this adapter.
func (s *Source) SourceType() artifact.SourceType { return artifact.SourceStackExchange }

// seResponse / seQuestion mirror the subset of the /2.3/questions response we
// consume. filter=withbody adds the `body` field (absent from the default filter).
type seResponse struct {
	Items          []seQuestion `json:"items"`
	QuotaRemaining int          `json:"quota_remaining"`
}

type seQuestion struct {
	QuestionID   int64    `json:"question_id"`
	Title        string   `json:"title"`
	Body         string   `json:"body"`
	Score        int      `json:"score"`
	Link         string   `json:"link"`
	Tags         []string `json:"tags"`
	CreationDate int64    `json:"creation_date"`
}

// Fetch queries each configured site's /questions endpoint (highest-voted first)
// for the page identified by cursor (a 1-based page number; empty = page 1) and
// returns up to limit questions total plus the next page number, so successive
// runs walk deeper into each site instead of re-fetching the top page. The
// min_score and benchmark-contamination filters are applied before returning.
func (s *Source) Fetch(ctx context.Context, limit int, cursor string) ([]artifact.Artifact, string, error) {
	if s.http == nil {
		return nil, "", fmt.Errorf("stackexchange: no HTTP client configured")
	}
	page := common.ParseCursorInt(cursor, 1)
	if page < 1 {
		page = 1
	}
	var out []artifact.Artifact
	for _, site := range s.cfg.Sites {
		remaining := 0
		if limit > 0 {
			remaining = limit - len(out)
			if remaining <= 0 {
				break
			}
		}
		u := s.questionsURL(site, page, remaining)
		// Force gzip so Go's transport does not transparently inflate; the
		// adapter owns decoding via common.Gunzip (the API gzips unconditionally).
		raw, err := common.GetBytes(ctx, s.http, u, map[string]string{
			"Accept-Encoding": "gzip",
		})
		if err != nil {
			return nil, "", fmt.Errorf("stackexchange: fetch %s: %w", site.Name, err)
		}
		raw, err = common.Gunzip(raw)
		if err != nil {
			return nil, "", fmt.Errorf("stackexchange: %s: %w", site.Name, err)
		}
		arts, err := s.parse(raw, site)
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

// questionsURL builds the /questions request for one site's page. remaining>0
// caps the page size to what is still needed (never above the API's maxPageSize).
func (s *Source) questionsURL(site Site, page, remaining int) string {
	pageSize := maxPageSize
	if remaining > 0 && remaining < pageSize {
		pageSize = remaining
	}
	q := url.Values{}
	q.Set("order", "desc")
	q.Set("sort", "votes")
	q.Set("site", apiSite(site))
	q.Set("page", strconv.Itoa(page))
	q.Set("pagesize", strconv.Itoa(pageSize))
	q.Set("filter", "withbody") // include question body (default filter omits it)
	return strings.TrimSuffix(s.cfg.APIBaseURL, "/") + "/questions?" + q.Encode()
}

// apiSite derives the API `site` slug from a Site: "math.stackexchange" -> "math",
// "physics.stackexchange" -> "physics", "mathoverflow" -> "mathoverflow". An
// explicit APISite override wins.
func apiSite(site Site) string {
	if v := strings.TrimSpace(site.APISite); v != "" {
		return v
	}
	return strings.TrimSuffix(site.Name, ".stackexchange")
}

// parse converts a /questions JSON response into artifacts. Contamination
// filtering is applied later in Fetch (matching the other adapters).
func (s *Source) parse(raw []byte, site Site) ([]artifact.Artifact, error) {
	var resp seResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("stackexchange: parse %s: %w", site.Name, err)
	}
	now := time.Now().UTC()
	out := make([]artifact.Artifact, 0, len(resp.Items))
	for _, q := range resp.Items {
		// Drop below-threshold questions. Default MinScore 0 discards
		// negative-score (low-quality) questions; set it lower to keep them.
		if q.Score < s.cfg.MinScore {
			continue
		}
		title := strings.TrimSpace(common.StripHTML(q.Title))
		body := common.StripHTML(q.Body)
		if title == "" && strings.TrimSpace(body) == "" {
			continue
		}
		id := strconv.FormatInt(q.QuestionID, 10)
		out = append(out, artifact.Artifact{
			SourceType:  artifact.SourceStackExchange,
			DocID:       site.Name + "#" + id,
			URL:         questionURL(q, site, id),
			License:     s.licenseFor(q.CreationDate),
			Title:       title,
			Locator:     "question-body",
			RetrievedAt: now,
			Chunks: []artifact.Chunk{
				{Text: common.Excerpt(body, s.cfg.MaxExcerptChars), Locator: "question-body"},
			},
		})
	}
	return out, nil
}

// questionURL prefers the API's canonical link, falling back to url_base+id.
func questionURL(q seQuestion, site Site, id string) string {
	if v := strings.TrimSpace(q.Link); v != "" {
		return v
	}
	if site.URLBase != "" {
		return site.URLBase + id
	}
	return ""
}

// StackExchange content-license cutoffs (UTC midnight of each switch date). The
// network launched on CC BY-SA 2.5, moved to 3.0 on 2011-04-08, and to 4.0 on
// 2018-05-02; a post is bound by whichever version was in force at its creation.
var (
	bySA30Cutoff = time.Date(2011, 4, 8, 0, 0, 0, 0, time.UTC) // 2.5 -> 3.0
	bySA40Cutoff = time.Date(2018, 5, 2, 0, 0, 0, 0, time.UTC) // 3.0 -> 4.0
)

// licenseFor returns the CC BY-SA version a question created at unixSecs is
// licensed under. A missing/zero creation date falls back to the configured
// default (the live API always supplies creation_date, so the fallback only
// covers hand-built or partial fixtures).
func (s *Source) licenseFor(unixSecs int64) string {
	if unixSecs <= 0 {
		return s.cfg.License
	}
	created := time.Unix(unixSecs, 0).UTC()
	switch {
	case created.Before(bySA30Cutoff):
		return licenseBySA25
	case created.Before(bySA40Cutoff):
		return licenseBySA30
	default:
		return licenseBySA40
	}
}
