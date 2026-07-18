package pacing

import (
	"context"
	"time"
)

// fakeReader is an in-memory StateReader for the pure-logic decision tests.
type fakeReader struct {
	emas          map[EMAKey]CostEMA
	hourly        map[HourlyKey]HourlyBudget
	countFn       func(since time.Time) int
	lastRateLimit *time.Time
	forceErr      error
}

func newFakeReader() *fakeReader {
	return &fakeReader{
		emas:   map[EMAKey]CostEMA{},
		hourly: map[HourlyKey]HourlyBudget{},
	}
}

func (f *fakeReader) GetEMA(_ context.Context, key EMAKey) (CostEMA, bool, error) {
	if f.forceErr != nil {
		return CostEMA{}, false, f.forceErr
	}
	e, ok := f.emas[key]
	return e, ok, nil
}

func (f *fakeReader) GetHourlyBudget(_ context.Context, key HourlyKey) (HourlyBudget, bool, error) {
	if f.forceErr != nil {
		return HourlyBudget{}, false, f.forceErr
	}
	hb, ok := f.hourly[key]
	return hb, ok, nil
}

func (f *fakeReader) CountGeneratedSince(_ context.Context, _, _ string, since time.Time) (int, error) {
	if f.forceErr != nil {
		return 0, f.forceErr
	}
	if f.countFn != nil {
		return f.countFn(since), nil
	}
	return 0, nil
}

func (f *fakeReader) LastRateLimitedAt(_ context.Context, _, _ string) (time.Time, bool, error) {
	if f.forceErr != nil {
		return time.Time{}, false, f.forceErr
	}
	if f.lastRateLimit == nil {
		return time.Time{}, false, nil
	}
	return *f.lastRateLimit, true, nil
}

func fptr(v float64) *float64 { return &v }

// directSnap builds a single-provider direct-mode snapshot.
func directSnap(provider string, primaryUsed, secondaryUsed float64, primaryReset, secondaryReset, observedAt time.Time) *QuotaSnapshot {
	return &QuotaSnapshot{
		Providers: []ProviderSnapshot{{
			Provider:  provider,
			Primary:   Window{UsedPercent: fptr(primaryUsed), ResetsAt: primaryReset},
			Secondary: Window{UsedPercent: fptr(secondaryUsed), ResetsAt: secondaryReset},
		}},
		ObservedAt: observedAt,
	}
}

// cliUnknownSnap builds a single-provider cli-unknown snapshot (null used_percent).
func cliUnknownSnap(provider string, observedAt time.Time) *QuotaSnapshot {
	return &QuotaSnapshot{
		Providers: []ProviderSnapshot{{
			Provider:  provider,
			Primary:   Window{UsedPercent: nil},
			Secondary: Window{UsedPercent: nil},
		}},
		ObservedAt: observedAt,
	}
}
