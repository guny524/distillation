package common

import "strings"

// nonCommercialMarkers and noDerivativesMarkers identify Creative-Commons
// restrictions that disqualify a source: the pipeline rephrases (a derivative)
// and produces a dataset that may be used commercially, so NC and ND licenses
// are excluded. Matched as substrings on a normalised (lowercased, hyphen/space
// unified) license label.
var nonCommercialMarkers = []string{"by-nc", "noncommercial", "non commercial"}

var noDerivativesMarkers = []string{"by-nd", "noderiv", "no derivative", "nd 4.0", "nd 3.0"}

// permissiveMarkers are recognised commercial-use-friendly licenses, including
// Creative Commons deed URLs (e.g. creativecommons.org/licenses/by/4.0/). They
// are matched on word boundaries (containsToken), NOT as bare substrings, so a
// short marker like "mit"/"isc"/"mpl" no longer falsely permits unrelated prose
// ("limitations apply", "commit", "miscellaneous", "template"). NC and ND are
// excluded earlier, so "cc-by" / "licenses/by" here is safe even though it is a
// bounded prefix of "cc-by-nc"/"cc-by-nd". Unknown labels are treated as NOT
// permissive (conservative: better to skip a source than redistribute under an
// unclear license).
var permissiveMarkers = []string{
	"cc0", "public domain", "publicdomain", "zero/1.0",
	"cc-by", "cc by", "licenses/by",
	"creative commons attribution",
	"apache", "mit", "bsd", "isc", "mpl", "unlicense",
}

// normalizeLicense lowercases and unifies separators so "CC BY-NC 4.0",
// "cc_by_nc", and "CC-BY-NC" all compare equally.
func normalizeLicense(license string) string {
	l := strings.ToLower(strings.TrimSpace(license))
	l = strings.ReplaceAll(l, "_", "-")
	// collapse internal whitespace
	l = strings.Join(strings.Fields(l), " ")
	return l
}

// LicensePermitsCommercial reports whether a license label allows commercial
// reuse and derivatives. NC (non-commercial) and ND (no-derivatives) licenses
// return false; recognised permissive licenses return true; an empty or
// unrecognised label returns false (conservative default). PMC ingestion uses
// this to keep only OA-subset articles safe for a redistributable dataset.
func LicensePermitsCommercial(license string) bool {
	l := normalizeLicense(license)
	if l == "" {
		return false
	}
	for _, m := range nonCommercialMarkers {
		if strings.Contains(l, m) {
			return false
		}
	}
	for _, m := range noDerivativesMarkers {
		if strings.Contains(l, m) {
			return false
		}
	}
	for _, m := range permissiveMarkers {
		if containsToken(l, m) {
			return true
		}
	}
	return false
}
