package pacing

import (
	"encoding/json"
	"fmt"
	"time"
)

// Window is one rate-limit window as parsed from subgate's GET /quota body.
//
// UsedPercent is a pointer so a null JSON value (cli-unknown transport, which
// cannot observe rate-limit state) is distinguishable from a real 0.0. When nil
// the window is unknown and the provider is in ModeCLIUnknown.
type Window struct {
	UsedPercent *float64  `json:"used_percent"`
	ResetsAt    time.Time `json:"resets_at"`
}

// Known reports whether this window carries a real used_percent value.
func (w Window) Known() bool { return w.UsedPercent != nil }

// ProviderSnapshot is one provider's normalized primary/secondary windows.
type ProviderSnapshot struct {
	Provider  string `json:"provider"`
	Primary   Window `json:"primary"`
	Secondary Window `json:"secondary"`
}

// Mode classifies the provider: ModeDirect when both windows are known,
// ModeCLIUnknown when either used_percent is null.
func (p ProviderSnapshot) Mode() string {
	if p.Primary.Known() && p.Secondary.Known() {
		return ModeDirect
	}
	return ModeCLIUnknown
}

// SourceUsage is per pipeline source-tag usage attribution (X-Subgate-Source).
type SourceUsage struct {
	Source           string  `json:"source"`
	UsedPercentDelta float64 `json:"used_percent_delta"`
	Requests         int64   `json:"requests"`
}

// QuotaSnapshot is the full GET /quota response body.
type QuotaSnapshot struct {
	Providers  []ProviderSnapshot `json:"providers"`
	Pipeline   []SourceUsage      `json:"pipeline"`
	ObservedAt time.Time          `json:"observed_at"`
}

// ParseSnapshot decodes a GET /quota response body.
func ParseSnapshot(b []byte) (QuotaSnapshot, error) {
	var s QuotaSnapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return QuotaSnapshot{}, fmt.Errorf("parse quota snapshot: %w", err)
	}
	return s, nil
}

// Provider returns the snapshot for name; ok=false when the provider is absent.
func (s *QuotaSnapshot) Provider(name string) (ProviderSnapshot, bool) {
	for _, p := range s.Providers {
		if p.Provider == name {
			return p, true
		}
	}
	return ProviderSnapshot{}, false
}

// SourceDelta returns the primary-window used_percent delta attributed to source;
// ok=false when the source tag has no pipeline attribution row.
func (s *QuotaSnapshot) SourceDelta(source string) (float64, bool) {
	for _, u := range s.Pipeline {
		if u.Source == source {
			return u.UsedPercentDelta, true
		}
	}
	return 0, false
}
