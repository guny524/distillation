package pacing

import (
	"os"
	"testing"
)

// TestParseSnapshot_LiveSubgateSample parses a real GET /quota body captured
// from a running subgate (2026-07-18, docker compose, codex direct provider,
// pre-first-quota-observation). This pins the cross-repo contract the worker
// StatusGate depends on: provider lookup by name, explicit-null used_percent
// (-> Window.Known()==false -> gate treats it as no-facts), null resets_at
// decoding to the zero time, and pipeline source buckets parsing.
func TestParseSnapshot_LiveSubgateSample(t *testing.T) {
	raw, err := os.ReadFile("testdata/quota_live_sample.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	snap, err := ParseSnapshot(raw)
	if err != nil {
		t.Fatalf("ParseSnapshot: %v", err)
	}

	if len(snap.Providers) != 1 || snap.Providers[0].Provider != "codex" {
		t.Fatalf("providers = %+v, want exactly [codex]", snap.Providers)
	}
	p := snap.Providers[0]
	if p.Primary.Known() || p.Secondary.Known() {
		t.Errorf("windows = %+v, want both unknown (null used_percent)", p)
	}
	if !p.Primary.ResetsAt.IsZero() || !p.Secondary.ResetsAt.IsZero() {
		t.Errorf("resets_at = %v/%v, want zero time from JSON null", p.Primary.ResetsAt, p.Secondary.ResetsAt)
	}
	if snap.ObservedAt.IsZero() {
		t.Error("observed_at not parsed")
	}

	// The tagged verify call and the untagged call both landed in pipeline
	// buckets (X-Subgate-Source attribution).
	got := map[string]int64{}
	for _, u := range snap.Pipeline {
		got[u.Source] = u.Requests
	}
	if got["verify-test"] < 1 || got["untagged"] < 1 {
		t.Errorf("pipeline buckets = %+v, want verify-test and untagged with requests >= 1", snap.Pipeline)
	}
}
