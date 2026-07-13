package scm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// rateLimitedCount reads operator_scm_ratelimited_total{provider,path,limit_type}.
func rateLimitedCount(t *testing.T, provider, path, limitType string) float64 {
	t.Helper()
	return testutil.ToFloat64(obs.SCMRateLimitedCounter(provider, path, limitType))
}

// realRetrySleep restores the real ghRetrySleep for the duration of a test that
// wants to measure elapsed time, in case another test in the package stubbed it.
func realRetrySleep(t *testing.T) {
	t.Helper()
	orig := ghRetrySleep
	ghRetrySleep = func(ctx context.Context, d time.Duration) error {
		tm := time.NewTimer(d)
		defer tm.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tm.C:
			return nil
		}
	}
	t.Cleanup(func() { ghRetrySleep = orig })
}

// The read path had ZERO rate-limit handling: no 429 branch, no Retry-After, no
// backoff, no retry. A 429 with Retry-After: 1 must now be retried once, after
// actually waiting the second the forge asked for.
func TestDoPagedGET_HonoursRetryAfter(t *testing.T) {
	realRetrySleep(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"a":7}`))
	}))
	t.Cleanup(srv.Close)

	var out struct {
		A int `json:"a"`
	}
	start := time.Now()
	_, err := doPagedGET(context.Background(), srv.URL+"/repos/rl-ra/x", "github", "/repos/rl-ra/x", nil, "Link", &out)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, 2, calls, "must retry exactly once")
	require.Equal(t, 7, out.A)
	require.GreaterOrEqual(t, elapsed, time.Second, "must honour Retry-After: 1")
}

// With no Retry-After, X-RateLimit-Reset (epoch seconds) is the wait, and it is
// only consulted when X-RateLimit-Remaining is 0.
func TestDoPagedGET_HonoursRateLimitReset(t *testing.T) {
	var sleeps []time.Duration
	orig := ghRetrySleep
	ghRetrySleep = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() { ghRetrySleep = orig })

	reset := time.Now().Add(3 * time.Second).Unix()
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	t.Cleanup(srv.Close)

	var out struct {
		A int `json:"a"`
	}
	_, err := doPagedGET(context.Background(), srv.URL+"/repos/rl-reset/x", "github", "/repos/rl-reset/x", nil, "Link", &out)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Len(t, sleeps, 1)
	require.Greater(t, sleeps[0], 2*time.Second, "wait must come from X-RateLimit-Reset, not the 1s backoff floor")
	require.LessOrEqual(t, sleeps[0], 3*time.Second)
}

// GitHub's SECONDARY limit answers 403, NOT 429, and carries no
// X-RateLimit-Remaining. Detection has to key on the response BODY's
// "secondary rate limit" marker. The read path must REUSE ghIsRateLimited (the
// write path's existing detector), not re-implement the rule: a second copy of
// it is a second place for it to be wrong.
func TestDoPagedGET_SecondaryLimitIs403AndReusesGhIsRateLimited(t *testing.T) {
	// The detector itself: a 403 with only the body marker is a rate limit.
	resp := &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}}
	require.True(t, ghIsRateLimited(resp, `{"message":"You have exceeded a secondary rate limit."}`))
	require.Equal(t, "secondary", limitType(resp, `{"message":"You have exceeded a secondary rate limit."}`))
	// And a plain 403 with no rate-limit signal at all is NOT one.
	require.False(t, ghIsRateLimited(resp, `{"message":"Resource not accessible by integration"}`))

	var sleeps []time.Duration
	orig := ghRetrySleep
	ghRetrySleep = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() { ghRetrySleep = orig })

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			// No Retry-After, no X-RateLimit-Remaining. Body marker only.
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit. Please wait a few minutes."}`))
			return
		}
		_, _ = w.Write([]byte(`{"a":2}`))
	}))
	t.Cleanup(srv.Close)

	before := rateLimitedCount(t, "github", "/repos/{owner}/{repo}", "secondary")
	var out struct {
		A int `json:"a"`
	}
	_, err := doPagedGET(context.Background(), srv.URL+"/repos/rl-sec/x", "github", "/repos/rl-sec/x", nil, "Link", &out)
	require.NoError(t, err)
	require.Equal(t, 2, calls, "a secondary-limit 403 on the READ path must be retried")
	require.Len(t, sleeps, 1)
	require.Equal(t, before+1, rateLimitedCount(t, "github", "/repos/{owner}/{repo}", "secondary"),
		"operator_scm_ratelimited_total{limit_type=secondary} must be incremented")
}

// A plain 403 (permission denied) on the read path must NOT be retried and must
// NOT be ErrRateLimited: a genuine auth failure has to surface immediately.
func TestDoPagedGET_PlainForbiddenIsNotRateLimited(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"Resource not accessible by integration"}`))
	}))
	t.Cleanup(srv.Close)

	_, err := doPagedGET(context.Background(), srv.URL+"/repos/rl-403/x", "github", "/repos/rl-403/x", nil, "Link", nil)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrRateLimited))
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusForbidden, he.Status)
	require.Equal(t, 1, calls)
}

// ErrRateLimited is TYPED: a caller can errors.Is it and REQUEUE the reconcile
// rather than failing the Task on a bare 4xx. The *HTTPError is still reachable
// underneath, so the status stays readable for the metric.
func TestDoPagedGET_ExhaustedRetriesSurfacesTypedErrRateLimited(t *testing.T) {
	orig := ghRetrySleep
	ghRetrySleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { ghRetrySleep = orig })

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	_, err := doPagedGET(context.Background(), srv.URL+"/repos/rl-typed/x", "github", "/repos/rl-typed/x", nil, "Link", nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrRateLimited), "callers requeue on errors.Is(err, ErrRateLimited)")

	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusTooManyRequests, he.Status)
	require.Equal(t, "429", ErrorStatus(err))
	require.Equal(t, ghMaxRetries+1, calls)
}

// The per-project token bucket SERIALISES. 40 concurrent requests against a
// 20 rps bucket cannot finish faster than ~1s: 20 drain the burst, the other 20
// refill at 20/s. This is the last line of defence against a sweep, a reconcile
// storm and an agent poll loop coinciding on one forge.
func TestBucket_SerialisesConcurrentRequests(t *testing.T) {
	lim := Bucket("test-serialise-project")
	require.Equal(t, rate.Limit(egressRPS), lim.Limit())
	require.Equal(t, egressBurst, lim.Burst())

	ctx := context.Background()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, lim.Wait(ctx))
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 900*time.Millisecond,
		"40 requests through a 20rps/20burst bucket must take ~1s, got %s", elapsed)
}

// The same bucket is handed to every reader and writer for a project, and
// different projects get different buckets (one noisy project cannot starve
// another).
func TestBucket_SharedPerProject(t *testing.T) {
	require.Same(t, Bucket("proj-a"), Bucket("proj-a"))
	require.NotSame(t, Bucket("proj-a"), Bucket("proj-b"))
}

// The bucket key is derived from the request path, so every egress path
// (doPagedGET, ghDo, glDo) lands in the SAME bucket for a given forge owner /
// group without threading a project name through every SCM signature.
func TestEgressKey(t *testing.T) {
	tests := []struct {
		provider, path, want string
	}{
		{"github", "/repos/szymonrychu/tatara-operator/pulls/5/reviews", "github:szymonrychu"},
		{"github", "https://api.github.com/repos/szymonrychu/tatara-cli/issues?page=2", "github:szymonrychu"},
		{"gitlab", "/projects/infra%2Ftatara-helmfile/merge_requests/3/discussions", "gitlab:infra"},
		{"gitlab", "/projects/infra/merge_requests/3", "gitlab:infra"},
		{"github", "/rate_limit", "github"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, egressKey(tt.provider, tt.path), "path %q", tt.path)
	}
}

// The metric's path label is a bounded route template: ids, shas and owner/repo
// segments collapse, so operator_scm_ratelimited_total does not grow a series
// per repo (let alone per PR).
func TestMetricPath(t *testing.T) {
	tests := []struct{ in, want string }{
		{"/repos/o/r/pulls/5/reviews", "/repos/{owner}/{repo}/pulls/{n}/reviews"},
		{"/repos/o/r/commits/9f2b1c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b/status", "/repos/{owner}/{repo}/commits/{sha}/status"},
		{"/repos/o/r/issues/12/comments", "/repos/{owner}/{repo}/issues/{n}/comments"},
		{"https://api.github.com/repos/o/r/issues?per_page=100", "/repos/{owner}/{repo}/issues"},
		{"/projects/g%2Fp/merge_requests/7/discussions", "/projects/{owner}/merge_requests/{n}/discussions"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, metricPath(tt.in), "in %q", tt.in)
	}
}
