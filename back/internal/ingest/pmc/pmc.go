// Package pmc ingests PubMed Central Open-Access subset articles (JATS/NXML) as
// backtranslation source material by querying NCBI E-utilities live: esearch
// lists OA-subset PMC ids, efetch returns their JATS XML. Only articles whose
// license permits commercial reuse and derivatives are kept
// (common.LicensePermitsCommercial), so the resulting dataset stays
// redistributable. Copyright: only the title and a short abstract excerpt are
// stored — never the full article body. doc_id = PMCID, license = the article's
// declared license.
package pmc

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/guny524/distillation/internal/artifact"
	"github.com/guny524/distillation/internal/ingest/common"
)

// DefaultAPIBaseURL is the NCBI E-utilities root (esearch.fcgi / efetch.fcgi are
// appended). Overridable via config so tests point it at a mock server.
const DefaultAPIBaseURL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

// DefaultSearchTerm restricts esearch to the PMC Open-Access subset. The license
// filter (only_commercial_use) still runs per article on top of this.
const DefaultSearchTerm = "open access[filter]"

// defaultRetMax bounds esearch when the caller passes no positive limit.
const defaultRetMax = 100

// Config configures the PMC source. OnlyCommercial is a pointer so an absent
// YAML key defaults to ON while an explicit `false` is honored.
type Config struct {
	Enabled    bool   `yaml:"enabled"`
	APIBaseURL string `yaml:"api_base_url"`
	SearchTerm string `yaml:"search_term"` // esearch term; default DefaultSearchTerm
	// Tool and Email are the optional E-utilities courtesy identifiers (NCBI asks
	// unauthenticated callers to send them; neither is required at 3 req/s).
	Tool            string `yaml:"tool"`
	Email           string `yaml:"email"`
	OnlyCommercial  *bool  `yaml:"only_commercial_use"` // nil => default true
	MaxExcerptChars int    `yaml:"max_excerpt_chars"`
}

// Source implements artifact.Source for the PMC OA subset.
type Source struct {
	cfg            Config
	http           common.HTTPDoer
	onlyCommercial bool
}

// New builds a PMC source. Commercial-use filtering defaults to ON; set
// only_commercial_use:false in config only for a corpus already known to be safe.
// httpc is the network seam (pass common.DefaultHTTPClient in production).
func New(cfg Config, httpc common.HTTPDoer) *Source {
	only := true
	if cfg.OnlyCommercial != nil {
		only = *cfg.OnlyCommercial
	}
	return newSource(cfg, httpc, only)
}

// NewWithCommercialFilter is an explicit constructor for tests/callers that want
// to control the license filter directly.
func NewWithCommercialFilter(cfg Config, httpc common.HTTPDoer, onlyCommercial bool) *Source {
	return newSource(cfg, httpc, onlyCommercial)
}

func newSource(cfg Config, httpc common.HTTPDoer, onlyCommercial bool) *Source {
	if cfg.APIBaseURL == "" {
		cfg.APIBaseURL = DefaultAPIBaseURL
	}
	if cfg.SearchTerm == "" {
		cfg.SearchTerm = DefaultSearchTerm
	}
	return &Source{cfg: cfg, http: httpc, onlyCommercial: onlyCommercial}
}

// SourceType identifies this adapter.
func (s *Source) SourceType() artifact.SourceType { return artifact.SourcePMC }

// jatsArticle mirrors the subset of a JATS/NXML article we consume.
type jatsArticle struct {
	XMLName xml.Name  `xml:"article"`
	Front   jatsFront `xml:"front"`
}

type jatsFront struct {
	ArticleMeta jatsArticleMeta `xml:"article-meta"`
}

type jatsArticleMeta struct {
	ArticleIDs  []jatsArticleID `xml:"article-id"`
	TitleGroup  jatsTitleGroup  `xml:"title-group"`
	Permissions jatsPermissions `xml:"permissions"`
	Abstract    jatsAbstract    `xml:"abstract"`
}

type jatsArticleID struct {
	PubIDType string `xml:"pub-id-type,attr"`
	Value     string `xml:",chardata"`
}

type jatsTitleGroup struct {
	ArticleTitle string `xml:"article-title"`
}

type jatsPermissions struct {
	License jatsLicense `xml:"license"`
}

type jatsLicense struct {
	Type string `xml:"license-type,attr"`
	Href string `xml:"href,attr"` // xlink:href on <license>
	Text string `xml:",innerxml"`
}

type jatsAbstract struct {
	Paragraphs []string `xml:"p"`
	Inner      string   `xml:",innerxml"`
}

// esearchResponse mirrors the esearch retmode=json envelope.
type esearchResponse struct {
	ESearchResult struct {
		IDList []string `json:"idlist"`
	} `json:"esearchresult"`
}

// Fetch lists OA-subset PMC ids via esearch starting at cursor (an esearch
// retstart offset; empty = 0), retrieves their JATS via a single batched efetch,
// keeps license-eligible articles, and returns up to limit artifacts plus the
// next retstart so each run pages forward instead of re-scanning the front.
func (s *Source) Fetch(ctx context.Context, limit int, cursor string) ([]artifact.Artifact, string, error) {
	if s.http == nil {
		return nil, "", fmt.Errorf("pmc: no HTTP client configured")
	}
	offset := common.ParseCursorInt(cursor, 0)
	ids, err := s.searchIDs(ctx, limit, offset)
	if err != nil {
		return nil, "", err
	}
	// Advance past the ids just scanned (not only the license-eligible ones) so a
	// page wholly dropped by the license filter still moves the cursor forward. An
	// empty id list leaves the cursor unchanged (clean stop at the end).
	next := common.FormatCursorInt(offset + len(ids))
	if len(ids) == 0 {
		return nil, next, nil
	}
	raw, err := s.fetchArticles(ctx, ids)
	if err != nil {
		return nil, "", err
	}
	out, err := s.parseArticleSet(raw, time.Now().UTC())
	if err != nil {
		return nil, "", err
	}
	out = common.FilterContaminated(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, next, nil
}

// searchIDs runs esearch from retstart and returns the PMC id list.
func (s *Source) searchIDs(ctx context.Context, limit, retstart int) ([]string, error) {
	retmax := limit
	if retmax <= 0 {
		retmax = defaultRetMax
	}
	q := url.Values{}
	q.Set("db", "pmc")
	q.Set("term", s.cfg.SearchTerm)
	q.Set("retmode", "json")
	q.Set("retmax", strconv.Itoa(retmax))
	q.Set("retstart", strconv.Itoa(retstart))
	s.addCourtesy(q)
	u := s.eutilURL("esearch.fcgi", q)
	raw, err := common.GetBytes(ctx, s.http, u, nil)
	if err != nil {
		return nil, fmt.Errorf("pmc: esearch: %w", err)
	}
	var resp esearchResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("pmc: parse esearch: %w", err)
	}
	return resp.ESearchResult.IDList, nil
}

// fetchArticles runs a single efetch for all ids (comma-joined). db=pmc returns
// a <pmc-articleset> wrapping one <article> per id.
func (s *Source) fetchArticles(ctx context.Context, ids []string) ([]byte, error) {
	q := url.Values{}
	q.Set("db", "pmc")
	q.Set("id", strings.Join(ids, ","))
	s.addCourtesy(q)
	u := s.eutilURL("efetch.fcgi", q)
	raw, err := common.GetBytes(ctx, s.http, u, nil)
	if err != nil {
		return nil, fmt.Errorf("pmc: efetch: %w", err)
	}
	return raw, nil
}

func (s *Source) eutilURL(endpoint string, q url.Values) string {
	return strings.TrimSuffix(s.cfg.APIBaseURL, "/") + "/" + endpoint + "?" + q.Encode()
}

func (s *Source) addCourtesy(q url.Values) {
	if s.cfg.Tool != "" {
		q.Set("tool", s.cfg.Tool)
	}
	if s.cfg.Email != "" {
		q.Set("email", s.cfg.Email)
	}
}

// parseArticleSet decodes an efetch response — a <pmc-articleset> of <article>
// elements, or a single bare <article> — into artifacts, applying the same
// per-article license/pmcid gate as parseArticle. The decoder is lenient
// (Strict=false + HTML entity table) so a real efetch body carrying undefined
// named entities still yields the fields we read.
func (s *Source) parseArticleSet(raw []byte, now time.Time) ([]artifact.Artifact, error) {
	dec := xml.NewDecoder(bytes.NewReader(raw))
	dec.Strict = false
	dec.Entity = xml.HTMLEntity
	var out []artifact.Artifact
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("pmc: decode articleset: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "article" {
			continue
		}
		var art jatsArticle
		if err := dec.DecodeElement(&art, &se); err != nil {
			return nil, fmt.Errorf("pmc: decode article: %w", err)
		}
		if a, keep := s.articleToArtifact(art, now); keep {
			out = append(out, a)
		}
	}
	return out, nil
}

// parseArticle parses one JATS <article> blob. The bool is false (skip) when the
// article fails the license filter or has no PMCID. Retained as the single-blob
// entry point (direct callers/tests); Fetch uses parseArticleSet.
func (s *Source) parseArticle(raw []byte, now time.Time) (artifact.Artifact, bool, error) {
	var art jatsArticle
	if err := xml.Unmarshal(raw, &art); err != nil {
		return artifact.Artifact{}, false, fmt.Errorf("pmc: parse article: %w", err)
	}
	a, ok := s.articleToArtifact(art, now)
	return a, ok, nil
}

// articleToArtifact maps a decoded JATS article to an Artifact, returning
// keep=false when it has no PMCID or fails the commercial-license filter.
func (s *Source) articleToArtifact(art jatsArticle, now time.Time) (artifact.Artifact, bool) {
	meta := art.Front.ArticleMeta

	pmcid := ""
	for _, id := range meta.ArticleIDs {
		if strings.EqualFold(id.PubIDType, "pmc") || strings.EqualFold(id.PubIDType, "pmcid") {
			pmcid = normalizePMCID(strings.TrimSpace(id.Value))
			break
		}
	}
	if pmcid == "" {
		return artifact.Artifact{}, false
	}

	license := licenseLabel(meta.Permissions.License)
	if s.onlyCommercial && !common.LicensePermitsCommercial(license) {
		return artifact.Artifact{}, false
	}

	title := strings.TrimSpace(common.StripHTML(meta.TitleGroup.ArticleTitle))
	abstract := abstractText(meta.Abstract)
	excerpt := common.Excerpt(abstract, s.cfg.MaxExcerptChars)

	return artifact.Artifact{
		SourceType:  artifact.SourcePMC,
		DocID:       pmcid,
		URL:         "https://www.ncbi.nlm.nih.gov/pmc/articles/" + pmcid + "/",
		License:     license,
		Title:       title,
		Locator:     "abstract",
		RetrievedAt: now,
		Chunks: []artifact.Chunk{
			{Text: excerpt, Locator: "abstract"},
		},
	}, true
}

// licenseLabel derives a comparable license label from the JATS <license>
// element. The href (a CC deed URL like creativecommons.org/licenses/by/4.0/) is
// the most reliable signal for commercial-use judgement, so it is preferred over
// the frequently-generic license-type attribute ("open-access"); the license
// paragraph text is the last resort.
func licenseLabel(l jatsLicense) string {
	if strings.TrimSpace(l.Href) != "" {
		return strings.TrimSpace(l.Href)
	}
	if txt := common.StripHTML(l.Text); strings.TrimSpace(txt) != "" {
		return txt
	}
	return strings.TrimSpace(l.Type)
}

func abstractText(a jatsAbstract) string {
	if len(a.Paragraphs) > 0 {
		return common.StripHTML(strings.Join(a.Paragraphs, " "))
	}
	return common.StripHTML(a.Inner)
}

// normalizePMCID ensures a "PMC" prefix (some ids arrive as the bare number).
func normalizePMCID(id string) string {
	if id == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToUpper(id), "PMC") {
		return "PMC" + id[3:]
	}
	return "PMC" + id
}
