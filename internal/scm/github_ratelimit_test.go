package scm

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stubRetrySleep replaces ghRetrySleep with a no-op that records the requested
// waits, so retry tests run instantly. Returns the slice of recorded delays.
func stubRetrySleep(t *testing.T) *[]time.Duration {
	t.Helper()
	var sleeps []time.Duration
	orig := ghRetrySleep
	ghRetrySleep = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() { ghRetrySleep = orig })
	return &sleeps
}

// A secondary-rate-limit 403 with a short Retry-After is retried in-process and
// the write eventually lands - the burst that drove #161 no longer counts as a
// hard failure when GitHub asks us to wait briefly.
func TestGhDoRetriesSecondaryRateLimitThenSucceeds(t *testing.T) {
	sleeps := stubRetrySleep(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls <= 2 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit. Please wait a few minutes."}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	err := ghDo(context.Background(), srv.URL, http.MethodDelete, "/repos/o/r/issues/7/labels/x", "tok", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 3, calls)
	require.Equal(t, []time.Duration{time.Second, time.Second}, *sleeps)
}

// 429 with no Retry-After falls back to exponential backoff and, if the limit
// never clears, surfaces the HTTPError after the bounded retry budget.
func TestGhDo429ExhaustsRetriesAndSurfaces(t *testing.T) {
	sleeps := stubRetrySleep(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	err := ghDo(context.Background(), srv.URL, http.MethodPost, "/x", "tok", map[string]string{"a": "b"}, nil)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusTooManyRequests, he.Status)
	require.Equal(t, ghMaxRetries+1, calls)
	require.Len(t, *sleeps, ghMaxRetries)
}

// A plain 403 (permission denied, no rate-limit signal) must NOT be retried:
// retrying would not help and could mask a real auth problem.
func TestGhDoPlainForbiddenIsNotRetried(t *testing.T) {
	sleeps := stubRetrySleep(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"Resource not accessible by integration"}`))
	}))
	t.Cleanup(srv.Close)

	err := ghDo(context.Background(), srv.URL, http.MethodDelete, "/x", "tok", nil, nil)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusForbidden, he.Status)
	require.Equal(t, 1, calls)
	require.Empty(t, *sleeps)
}

// 5xx is never retried here: the request may have been applied server-side, so
// re-sending a non-idempotent write (e.g. create_issue) could duplicate it.
func TestGhDoServerErrorIsNotRetried(t *testing.T) {
	sleeps := stubRetrySleep(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	err := ghDo(context.Background(), srv.URL, http.MethodPost, "/x", "tok", map[string]string{"a": "b"}, nil)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusInternalServerError, he.Status)
	require.Equal(t, 1, calls)
	require.Empty(t, *sleeps)
}

// A Retry-After longer than ghMaxBackoff fails fast (no in-process wait) so a
// worker goroutine is not blocked; the reconcile requeues instead.
func TestGhDoLongRetryAfterFailsFast(t *testing.T) {
	sleeps := stubRetrySleep(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Retry-After", "600")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"secondary rate limit"}`))
	}))
	t.Cleanup(srv.Close)

	err := ghDo(context.Background(), srv.URL, http.MethodPost, "/x", "tok", map[string]string{"a": "b"}, nil)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusForbidden, he.Status)
	require.Equal(t, 1, calls)
	require.Empty(t, *sleeps)
}

// RemoveLabel's rate-limit retry applies end-to-end: a throttled DELETE is
// retried and the sibling-label cleanup succeeds.
func TestRemoveLabelRetriesOnRateLimit(t *testing.T) {
	stubRetrySleep(t)
	var calls int
	c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"secondary rate limit"}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	require.NoError(t, c.RemoveLabel(context.Background(), "tok", "o/r#7", "tatara/approved"))
	require.Equal(t, 2, calls)
}

// The observability fix for #161: a non-404 RemoveLabel failure emits a WARN
// (visible to on-call under level=~"ERROR|WARN") carrying the HTTP status, while
// a benign 404 stays silent (no WARN, so it never inflates the burst).
func TestRemoveLabelLogsNonNotFoundAtWarn(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	cErr := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"nope"}`))
	})
	require.Error(t, cErr.RemoveLabel(context.Background(), "tok", "o/r#7", "tatara-rejected"))
	out := buf.String()
	require.Contains(t, out, "github: remove label failed")
	require.Contains(t, out, `"level":"WARN"`)
	require.Contains(t, out, `"status":"403"`)
	require.Contains(t, out, `"issue_ref":"o/r#7"`)

	buf.Reset()
	cNoop := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Label does not exist"}`))
	})
	require.NoError(t, cNoop.RemoveLabel(context.Background(), "tok", "o/r#7", "tatara/approved"))
	require.Empty(t, buf.String())
}
