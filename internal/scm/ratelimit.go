package scm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/time/rate"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// ErrRateLimited is returned when an SCM call was rejected by the forge's rate
// limiter and the bounded in-process retry budget did not clear it. It is typed
// (and wraps the underlying *HTTPError) so a caller can errors.Is it and REQUEUE
// the reconcile instead of failing the Task: a rate-limit rejection is never a
// property of the work, only of the moment.
var ErrRateLimited = errors.New("scm: rate limited")

// egressRPS is the per-project SCM egress ceiling, shared by every reader and
// writer in the operator process. It is the last line of defence against a
// sweep, a reconcile storm and an agent poll loop coinciding on one forge
// (contract C.8). Burst equals the rate: a 20-request burst drains instantly,
// the 21st waits 50ms, and 40 concurrent requests take ~1s.
const (
	egressRPS   = 20
	egressBurst = 20
)

var (
	bucketsMu sync.Mutex
	buckets   = map[string]*rate.Limiter{}
)

// Bucket returns the process-wide token bucket for a project, creating it on
// first use. Every SCM request the operator makes passes through one of these.
// The key is a project identity: for the automatic egress path it is
// "<provider>:<owner-or-group>" (egressKey), and a caller that holds a real
// Project CR name may pass it directly.
func Bucket(project string) *rate.Limiter {
	bucketsMu.Lock()
	defer bucketsMu.Unlock()
	l, ok := buckets[project]
	if !ok {
		l = rate.NewLimiter(rate.Limit(egressRPS), egressBurst)
		buckets[project] = l
	}
	return l
}

// waitEgress blocks until the project's bucket admits one request, or ctx ends.
// Called by every egress path (doPagedGET, ghDo, glDo) before the request goes
// out on the wire.
func waitEgress(ctx context.Context, provider, path string) error {
	if err := Bucket(egressKey(provider, path)).Wait(ctx); err != nil {
		return fmt.Errorf("%s: egress bucket: %w", provider, err)
	}
	return nil
}

// egressKey derives the bucket key from a request path. A tatara project maps
// 1:1 onto a forge owner (GitHub org) or top-level group (GitLab), so the first
// meaningful path segment is the project identity: /repos/{owner}/... on GitHub,
// /projects/{group%2Fproject}/... on GitLab. Anything unrecognised falls back to
// the provider itself, which is a stricter (shared) bucket, never a looser one.
func egressKey(provider, path string) string {
	p := path
	if i := strings.Index(p, "://"); i >= 0 {
		if j := strings.Index(p[i+3:], "/"); j >= 0 {
			p = p[i+3+j:]
		}
	}
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		p = p[:i]
	}
	segs := strings.Split(strings.Trim(p, "/"), "/")
	switch {
	case len(segs) >= 2 && segs[0] == "repos":
		return provider + ":" + segs[1]
	case len(segs) >= 2 && segs[0] == "projects":
		owner, _, _ := strings.Cut(unescapeSeg(segs[1]), "/")
		return provider + ":" + owner
	default:
		return provider
	}
}

// unescapeSeg decodes the %2F GitLab uses to pack group/project into one path
// segment, without pulling in url.PathUnescape's error path for a value that is
// only ever used as a map key.
func unescapeSeg(s string) string {
	return strings.NewReplacer("%2F", "/", "%2f", "/").Replace(s)
}

// metricPath collapses a request path to a bounded Prometheus label: numeric
// ids, commit shas and owner/repo segments become placeholders, so
// operator_scm_ratelimited_total{path} has a fixed cardinality no matter how
// many repos the platform grows to.
func metricPath(path string) string {
	p := path
	if i := strings.Index(p, "://"); i >= 0 {
		if j := strings.Index(p[i+3:], "/"); j >= 0 {
			p = p[i+3+j:]
		} else {
			p = "/"
		}
	}
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		p = p[:i]
	}
	segs := strings.Split(p, "/")
	for i, s := range segs {
		switch {
		case s == "":
		case isAllDigits(s):
			segs[i] = "{n}"
		case isHexSHA(s):
			segs[i] = "{sha}"
		case i > 0 && (segs[i-1] == "repos" || segs[i-1] == "projects"):
			segs[i] = "{owner}"
		case i > 1 && segs[i-2] == "repos":
			segs[i] = "{repo}"
		}
	}
	return strings.Join(segs, "/")
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isHexSHA(s string) bool {
	if len(s) < 7 || len(s) > 40 {
		return false
	}
	digits := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits++
		case r >= 'a' && r <= 'f', r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	// A pure-digit string is an id, not a sha; isAllDigits already claimed it.
	return digits != len(s)
}

// limitType classifies a rate-limit rejection for the metric. GitHub's SECONDARY
// limit (80 content-creating requests/minute) answers 403 with a
// "secondary rate limit" marker in the BODY and carries no X-RateLimit-Remaining;
// the primary limit answers 429, or 403 with X-RateLimit-Remaining: 0.
func limitType(resp *http.Response, body string) string {
	if strings.Contains(strings.ToLower(body), "secondary rate limit") {
		return "secondary"
	}
	return "primary"
}

// recordRateLimited emits operator_scm_ratelimited_total for one rejected
// request. Every rate-limit branch in the package funnels through here so the
// provider/path/limit_type labels are produced in exactly one place.
func recordRateLimited(provider, path string, resp *http.Response, body string) {
	obs.SCMRateLimited(provider, metricPath(path), limitType(resp, body))
}

// rateLimitedError wraps the forge's HTTPError in ErrRateLimited so a caller can
// errors.Is(err, ErrRateLimited) to requeue and still errors.As(err, &HTTPError)
// to read the status.
func rateLimitedError(status int, body, path string) error {
	return &rateLimitedHTTPError{HTTPError: &HTTPError{Status: status, Body: body, Path: path}}
}

type rateLimitedHTTPError struct {
	*HTTPError
}

func (e *rateLimitedHTTPError) Error() string {
	return ErrRateLimited.Error() + " (" + strconv.Itoa(e.Status) + "): " + e.HTTPError.Error()
}

func (e *rateLimitedHTTPError) Is(target error) bool { return target == ErrRateLimited }

func (e *rateLimitedHTTPError) Unwrap() error { return e.HTTPError }
