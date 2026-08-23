package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/cicontract"
)

// This binary is transport and exit codes, and both were untested. Two of the
// three things in it can fail SILENTLY, which is the failure class #640 is
// about:
//
//   - the `bump-` output key is a literal here and a second literal in
//     fanout_contract_test.go, bound to each other by nothing. Renaming it
//     leaves the build and every test green; at runtime `plan` exits 0, all four
//     `steps.plan.outputs.bump-<repo>` gates evaluate '' == 'true' -> false, all
//     four steps skip, and `fanout-ci-shared` reports SUCCESS having written no
//     pin anywhere. A silently dead fan-out is exactly what release.yml's
//     four-explicit-steps shape was chosen to prevent.
//   - `plan`'s exit-code split IS the fail-closed contract: a STALE consumer is
//     what the caller is about to fix (exit 0), an UNREADABLE one is a repo the
//     fan-out would silently skip (exit 1). Getting it backwards either strands
//     every release or reintroduces the silence.

type fakeFetcher struct {
	files map[string]string
	tags  []string
	err   error
}

func (f fakeFetcher) File(_ context.Context, repo, ref, path string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	v, ok := f.files[repo+"@"+ref+":"+path]
	if !ok {
		return nil, errors.New("404 not found")
	}
	return []byte(v), nil
}

func (f fakeFetcher) Tags(_ context.Context, _ string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.tags, nil
}

func consumerCI(pin string) string {
	return "jobs:\n  ci:\n    uses: " + cicontract.ProducerRepo + "/" + cicontract.CISharedPath + "@" + pin + "\n"
}

// fleet serves every live consumer at `pin`, and the producer serves `served`
// at that pin.
//
// A repo in ImageVerifyExempt is served with the gate OFF, because that is what
// the exemption asserts about it: served gate-ON it is an exemption-unused
// finding, and this fixture is supposed to mean "the fleet is in the state the
// contract says it should be in".
func fleet(pin, served string) fakeFetcher {
	files := map[string]string{
		cicontract.ProducerRepo + "@" + pin + ":" + cicontract.CISharedPath: served,
	}
	for _, repo := range cicontract.LiveConsumers {
		ci := consumerCI(pin)
		if _, exempt := cicontract.ImageVerifyExempt[repo]; exempt {
			ci += "    with:\n      enable-image-verify: false\n"
		}
		files[repo+"@main:"+cicontract.ConsumerWorkflowPath] = ci
	}
	return fakeFetcher{files: files}
}

// withReference writes `content` to a temp file and returns its path, standing
// in for the local .github/workflows/ci-shared.yml `plan` compares against.
func withReference(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ci-shared.yml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// capturedOutputs runs fn with $GITHUB_OUTPUT pointed at a temp file and returns
// what the step wrote, the way Actions would read it.
func capturedOutputs(t *testing.T, fn func()) map[string]string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "github_output")
	t.Setenv("GITHUB_OUTPUT", p)
	fn()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("nothing was written to $GITHUB_OUTPUT: %v", err)
	}
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			got[k] = v
		}
	}
	return got
}

// TestPlan_EmitsTheKeyReleaseYAMLGatesOn is the binding the review found
// missing. It asserts the key through cicontract.BumpOutputKey - the same
// builder release.yml's contract test uses - so the prefix cannot be changed
// here without the workflow assertion moving with it.
func TestPlan_EmitsTheKeyReleaseYAMLGatesOn(t *testing.T) {
	ref := withReference(t, "on: workflow_call\n")
	f := fleet("v3.10.0", "on: workflow_call\n")

	var code int
	got := capturedOutputs(t, func() { code = plan(context.Background(), f, ref) })

	if code != 0 {
		t.Fatalf("plan on a current fleet exited %d, want 0", code)
	}
	for _, repo := range cicontract.LiveConsumers {
		key := cicontract.BumpOutputKey(repo)
		if _, ok := got[key]; !ok {
			t.Errorf("plan wrote no %q; release.yml gates a cd-release step on "+
				"steps.plan.outputs.%s, and a missing key evaluates to '' there - so that "+
				"consumer's pin is silently never written", key, key)
		}
	}
}

func TestPlan_StaleIsNotFatalAndUnreadableIs(t *testing.T) {
	cli := cicontract.LiveConsumers[0]

	t.Run("stale consumer bumps and exits 0", func(t *testing.T) {
		ref := withReference(t, "on: workflow_call\n# the image-verify job\n")
		f := fleet("v3.10.0", "on: workflow_call\n")

		var code int
		got := capturedOutputs(t, func() { code = plan(context.Background(), f, ref) })

		if code != 0 {
			t.Fatalf("plan exited %d on a stale fleet; stale is what the fan-out is about to fix, not an error", code)
		}
		if got[cicontract.BumpOutputKey(cli)] != "true" {
			t.Errorf("%s = %q, want true", cicontract.BumpOutputKey(cli), got[cicontract.BumpOutputKey(cli)])
		}
	})

	t.Run("unreadable consumer exits 1", func(t *testing.T) {
		ref := withReference(t, "on: workflow_call\n")
		f := fleet("v3.10.0", "on: workflow_call\n")
		delete(f.files, cli+"@main:"+cicontract.ConsumerWorkflowPath)

		var code int
		capturedOutputs(t, func() { code = plan(context.Background(), f, ref) })

		if code != 1 {
			t.Errorf("plan exited %d on a consumer it could not read, want 1: a fan-out that "+
				"skips a repo it could not read is #640 with a new trigger", code)
		}
	})

	t.Run("a dark gate is neither: it must not stop the fan-out", func(t *testing.T) {
		// Not exempt, gate off, otherwise current. The scheduled audit files an
		// issue about this; failing `plan` on it would skip the ENTIRE fan-out
		// on every release for as long as it lasted, so the pin stops being
		// written - #640 restored by the check added to prevent it.
		ref := withReference(t, "on: workflow_call\n")
		f := fleet("v3.10.0", "on: workflow_call\n")
		f.files[cli+"@main:"+cicontract.ConsumerWorkflowPath] =
			consumerCI("v3.10.0") + "    with:\n      enable-image-verify: false\n"

		var code int
		capturedOutputs(t, func() { code = plan(context.Background(), f, ref) })

		if code != 0 {
			t.Errorf("plan exited %d on a gate-dark consumer, want 0", code)
		}
	})
}

func TestAudit_AnyFindingIsFatal(t *testing.T) {
	served := "on: workflow_call\n"
	f := fleet("v3.10.0", served)
	f.tags = []string{"v3.10.0"}

	if code := audit(context.Background(), f); code != 0 {
		t.Fatalf("audit exited %d on a current fleet, want 0", code)
	}

	// Newest tag now serves something the fleet is not running.
	f.tags = []string{"v3.10.0", "v3.11.0"}
	f.files[cicontract.ProducerRepo+"@v3.11.0:"+cicontract.CISharedPath] = served + "# the image-verify job\n"
	if code := audit(context.Background(), f); code != 1 {
		t.Errorf("audit exited %d on a stale fleet, want 1: unlike plan, ANY finding is fatal here", code)
	}
}

func TestAudit_AnUnresolvableReferenceIsNotACleanFleet(t *testing.T) {
	f := fakeFetcher{err: errors.New("502 bad gateway")}
	if code := audit(context.Background(), f); code != 1 {
		t.Errorf("audit exited %d when it could not resolve its own reference, want 1: "+
			"an unknown fleet is not a current fleet", code)
	}
}

// The retry budget must cover the failure it was written for. The stated reason
// for retrying at all is anonymous per-IP rate limiting from a shared ARC egress
// IP, and that window is ~60s - but the first cut was `(attempt-1)*2s` over 4
// attempts, i.e. 12s total, so it slept through none of it and rendered the 429
// as a Finding asserting the tag had been deleted. That text is what lands in
// the filed issue.
//
// A 429 also usually carries Retry-After, which is the server telling you the
// answer instead of guessing it.
func TestBackoff_CoversTheRateLimitWindowItWasWrittenFor(t *testing.T) {
	var total time.Duration
	for attempt := 1; attempt < fetchAttempts; attempt++ {
		total += backoff(attempt, "")
	}
	if total < 60*time.Second {
		t.Errorf("the retry budget is %s across %d attempts, but it exists for anonymous "+
			"per-IP rate limiting, whose window is ~60s. It sleeps through none of it and "+
			"then reports the ref as deleted", total, fetchAttempts)
	}
	if total > fetchTimeout {
		t.Errorf("the retry budget (%s) exceeds the whole run's timeout (%s), so one "+
			"unlucky fetch starves the other consumers", total, fetchTimeout)
	}
}

func TestBackoff_HonoursRetryAfter(t *testing.T) {
	if got := backoff(1, "30"); got != 30*time.Second {
		t.Errorf("backoff(1, \"30\") = %s, want 30s: the server said how long to wait", got)
	}
	// Capped, so a hostile or absurd header cannot park the job past its timeout.
	if got := backoff(1, "99999"); got > time.Minute {
		t.Errorf("backoff(1, \"99999\") = %s, want it capped at 60s", got)
	}
	// Garbage falls back to the schedule rather than to zero.
	if got := backoff(2, "soon"); got <= 0 {
		t.Errorf("backoff(2, \"soon\") = %s, want the exponential fallback", got)
	}
}
