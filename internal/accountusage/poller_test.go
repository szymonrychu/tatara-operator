package accountusage

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeFetcher struct {
	snaps []Snapshot
	errs  []error
	i     int
}

func (f *fakeFetcher) Fetch(context.Context) (Snapshot, error) {
	defer func() { f.i++ }()
	if f.i < len(f.errs) && f.errs[f.i] != nil {
		return Snapshot{}, f.errs[f.i]
	}
	if f.i < len(f.snaps) {
		return f.snaps[f.i], nil
	}
	return Snapshot{}, errors.New("exhausted")
}

func TestPollOnceSuccessSetsHealthy(t *testing.T) {
	f := &fakeFetcher{snaps: []Snapshot{{FiveHour: Window{Percent: 33}, Healthy: true}}}
	st := &Store{}
	p := &Poller{Fetcher: f, Store: st, FailureThreshold: 3, Now: time.Now}
	p.pollOnce(context.Background())
	if got := st.Get(); !got.Healthy || got.FiveHour.Percent != 33 {
		t.Fatalf("store after success: %+v", got)
	}
}

func TestConsecutiveFailuresMarkStaleAfterThreshold(t *testing.T) {
	f := &fakeFetcher{
		snaps: []Snapshot{{FiveHour: Window{Percent: 50}, Healthy: true}},
		errs:  []error{nil, errors.New("x"), errors.New("x"), errors.New("x")},
	}
	st := &Store{}
	p := &Poller{Fetcher: f, Store: st, FailureThreshold: 3, Now: time.Now}
	p.pollOnce(context.Background()) // success -> healthy, keep last-known percent
	p.pollOnce(context.Background()) // fail 1 -> still healthy (last-known)
	if !st.Get().Healthy {
		t.Fatal("single failure must not mark stale")
	}
	p.pollOnce(context.Background()) // fail 2
	p.pollOnce(context.Background()) // fail 3 -> stale
	got := st.Get()
	if got.Healthy {
		t.Fatal("threshold failures must mark stale")
	}
	if got.FiveHour.Percent != 50 {
		t.Fatal("stale snapshot must retain last-known windows")
	}
}

// TestPollOnceStampsPollerSource locks the invariant that every snapshot in the
// store names the feed that produced it: without it the poller's snapshot would
// land in tatara_account_usage_snapshot_age_seconds under an empty source label
// and nobody could tell which feed had gone quiet. The stale path keeps the
// source it already carried, since it keeps the last-known windows too.
func TestPollOnceStampsPollerSource(t *testing.T) {
	f := &fakeFetcher{
		snaps: []Snapshot{{FiveHour: Window{Percent: 33}}},
		errs:  []error{nil, errors.New("x")},
	}
	st := &Store{}
	p := &Poller{Fetcher: f, Store: st, FailureThreshold: 1, Now: time.Now}
	p.pollOnce(context.Background())
	if got := st.Get().Source; got != SourcePoller {
		t.Fatalf("Source = %q after a successful poll, want %q", got, SourcePoller)
	}
	p.pollOnce(context.Background())
	if got := st.Get().Source; got != SourcePoller {
		t.Fatalf("Source = %q after a failing poll, want %q retained", got, SourcePoller)
	}
}
