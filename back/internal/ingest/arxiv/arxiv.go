// Package arxiv ingests arXiv metadata/abstracts as backtranslation source
// material. It parses the arXiv Atom API response (encoding/xml); the same
// parser handles a locally-cached Atom dump so bulk and API paths share code.
// Only math/physics/cs/q-bio categories are kept. Copyright: the abstract is
// stored only as a short excerpt (a rephrasing anchor), never verbatim in full;
// doc_id = arXiv id, url = abs page, license = arXiv label.
package arxiv

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/common"
)

// DefaultLicense labels arXiv's default distribution license. The Atom API does
// not expose per-paper license; accurate per-paper license (some papers are
// CC BY) lives in the bulk metadata JSON dump. See the TODO in Fetch.
const DefaultLicense = "arXiv.org perpetual non-exclusive license"

// DefaultAPIBaseURL is the arXiv export API query endpoint.
const DefaultAPIBaseURL = "http://export.arxiv.org/api/query"

// defaultMaxResults bounds an API page when the caller passes no positive limit.
const defaultMaxResults = 100

// Config configures the arXiv source. Either DumpPath (local Atom XML) or the
// API (APIBaseURL + Query) supplies the raw feed.
type Config struct {
	Enabled         bool     `yaml:"enabled"`
	Categories      []string `yaml:"categories"` // top-level groups: math, physics, cs, q-bio
	Query           string   `yaml:"query"`      // arXiv search_query, e.g. "cat:cs.LG"
	APIBaseURL      string   `yaml:"api_base_url"`
	DumpPath        string   `yaml:"dump_path"` // optional local Atom XML feed
	License         string   `yaml:"license"`
	MaxExcerptChars int      `yaml:"max_excerpt_chars"`
}

// Source implements artifact.Source for arXiv.
type Source struct {
	cfg  Config
	http common.HTTPDoer
}

// New builds an arXiv source. httpc may be nil when DumpPath is set (no network).
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
func (s *Source) SourceType() artifact.SourceType { return artifact.SourceArxiv }

// atomFeed / atomEntry mirror the subset of the arXiv Atom response we consume.
type atomFeed struct {
	XMLName xml.Name    `xml:"feed"`
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	ID         string         `xml:"id"`      // http://arxiv.org/abs/2401.07013v1
	Title      string         `xml:"title"`
	Summary    string         `xml:"summary"` // abstract
	Published  string         `xml:"published"`
	Categories []atomCategory `xml:"category"`
}

type atomCategory struct {
	Term string `xml:"term,attr"`
}

// Fetch returns up to limit artifacts for the page starting at cursor (an arXiv
// start offset; empty = offset 0) and the next offset to resume from, so each
// run advances instead of re-scanning the front. DumpPath (if set) is paged from
// disk; otherwise the arXiv API is queried via the injected HTTPDoer. Category
// and benchmark-contamination filters are applied before returning.
//
// TODO(network): per-paper license (some arXiv papers are CC BY / CC0) is only in
// the bulk metadata JSON dump, not the Atom API. Wire the JSON dump parser and
// populate Artifact.License per paper when running for real.
func (s *Source) Fetch(ctx context.Context, limit int, cursor string) ([]artifact.Artifact, string, error) {
	offset := common.ParseCursorInt(cursor, 0)
	pageSize := limit
	if pageSize <= 0 {
		pageSize = defaultMaxResults
	}
	if s.cfg.DumpPath != "" {
		return s.fetchDump(offset, pageSize)
	}
	return s.fetchAPI(ctx, offset, pageSize)
}

// fetchAPI queries the arXiv API for the page [offset, offset+pageSize) and
// reports offset+pageSize as the next cursor. A stable submittedDate/ascending
// sort makes the offset meaningful across runs (new papers append at the tail as
// the corpus grows) rather than the API's default relevance order.
func (s *Source) fetchAPI(ctx context.Context, offset, pageSize int) ([]artifact.Artifact, string, error) {
	if s.http == nil {
		return nil, "", fmt.Errorf("arxiv: no dump_path and no HTTP client configured")
	}
	q := url.Values{}
	if s.cfg.Query != "" {
		q.Set("search_query", s.cfg.Query)
	}
	q.Set("sortBy", "submittedDate")
	q.Set("sortOrder", "ascending")
	q.Set("start", strconv.Itoa(offset))
	q.Set("max_results", strconv.Itoa(pageSize))
	u := s.cfg.APIBaseURL + "?" + q.Encode()
	raw, err := common.GetBytes(ctx, s.http, u, nil)
	if err != nil {
		return nil, "", err
	}
	arts, err := s.parse(raw)
	if err != nil {
		return nil, "", err
	}
	arts = common.FilterContaminated(arts)
	if len(arts) > pageSize {
		arts = arts[:pageSize]
	}
	return arts, common.FormatCursorInt(offset + pageSize), nil
}

// fetchDump pages a local Atom dump: the whole file is parsed and filtered once,
// then sliced to [offset, offset+pageSize). The next cursor advances only by the
// number of items actually yielded so it stops cleanly at the end of a static
// file instead of running off the end.
func (s *Source) fetchDump(offset, pageSize int) ([]artifact.Artifact, string, error) {
	b, err := os.ReadFile(s.cfg.DumpPath)
	if err != nil {
		return nil, "", fmt.Errorf("arxiv: read dump %s: %w", s.cfg.DumpPath, err)
	}
	arts, err := s.parse(b)
	if err != nil {
		return nil, "", err
	}
	arts = common.FilterContaminated(arts)
	page := pageArtifacts(arts, offset, pageSize)
	return page, common.FormatCursorInt(offset + len(page)), nil
}

// pageArtifacts returns arts[offset:offset+pageSize] with both bounds clamped to
// the slice length (an out-of-range offset yields the empty page).
func pageArtifacts(arts []artifact.Artifact, offset, pageSize int) []artifact.Artifact {
	if offset < 0 || offset >= len(arts) {
		return nil
	}
	end := offset + pageSize
	if end > len(arts) {
		end = len(arts)
	}
	return arts[offset:end]
}

func (s *Source) parse(raw []byte) ([]artifact.Artifact, error) {
	var feed atomFeed
	if err := xml.Unmarshal(raw, &feed); err != nil {
		return nil, fmt.Errorf("arxiv: parse atom: %w", err)
	}
	now := time.Now().UTC()
	out := make([]artifact.Artifact, 0, len(feed.Entries))
	for _, e := range feed.Entries {
		if !s.categoryMatch(e.Categories) {
			continue
		}
		id := arxivID(e.ID)
		if id == "" {
			continue
		}
		title := strings.TrimSpace(collapse(e.Title))
		excerpt := common.Excerpt(e.Summary, s.cfg.MaxExcerptChars)
		out = append(out, artifact.Artifact{
			SourceType:  artifact.SourceArxiv,
			DocID:       id,
			URL:         "https://arxiv.org/abs/" + id,
			License:     s.cfg.License,
			Title:       title,
			Locator:     "abstract",
			RetrievedAt: now,
			Chunks: []artifact.Chunk{
				{Text: excerpt, Locator: "abstract"},
			},
		})
	}
	return out, nil
}

// categoryMatch keeps an entry if any of its categories falls in a configured
// top-level group. Empty config Categories means accept all.
func (s *Source) categoryMatch(cats []atomCategory) bool {
	if len(s.cfg.Categories) == 0 {
		return true
	}
	for _, c := range cats {
		for _, g := range s.cfg.Categories {
			if groupMatches(c.Term, g) {
				return true
			}
		}
	}
	return false
}

// physicsArchives are arXiv physics archive prefixes (physics.* plus the older
// standalone physics archives).
var physicsArchives = map[string]bool{
	"physics": true, "hep-th": true, "hep-ph": true, "hep-ex": true,
	"hep-lat": true, "gr-qc": true, "quant-ph": true, "astro-ph": true,
	"cond-mat": true, "nucl-th": true, "nucl-ex": true, "nlin": true,
}

func groupMatches(term, group string) bool {
	term = strings.ToLower(strings.TrimSpace(term))
	archive := term
	if i := strings.Index(term, "."); i >= 0 {
		archive = term[:i]
	}
	switch strings.ToLower(group) {
	case "cs":
		return archive == "cs"
	case "math":
		return archive == "math" || term == "math-ph"
	case "q-bio":
		return archive == "q-bio"
	case "physics":
		return physicsArchives[archive]
	default:
		return archive == strings.ToLower(group)
	}
}

// arxivID extracts the bare arXiv id (version stripped) from an Atom id URL.
func arxivID(atomID string) string {
	s := strings.TrimSpace(atomID)
	if i := strings.LastIndex(s, "/abs/"); i >= 0 {
		s = s[i+len("/abs/"):]
	}
	// strip trailing version marker vN
	if i := strings.LastIndex(s, "v"); i > 0 {
		allDigits := true
		for _, r := range s[i+1:] {
			if r < '0' || r > '9' {
				allDigits = false
				break
			}
		}
		if allDigits && i < len(s)-1 {
			s = s[:i]
		}
	}
	return s
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
