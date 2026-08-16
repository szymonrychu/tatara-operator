package obs

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// mintBudgetSkipReason is controller.SweepSkipMintBudget. Literal here for the
// same no-reverse-import reason as sweepSkipReasons, and it is a member of
// that list, so TestSweepSkipReasonsMatchSweepConstants already pins the string
// against the constant in sweep.go.
const mintBudgetSkipReason = "mint_budget_bound"

// steadyStateSkipReasons are the skip reasons a HEALTHY project emits on every
// pass forever, and therefore the only ones TataraSweepSkipPersistent may
// exclude. upgrade_headroom_bound was the other member of this set until Task
// 8 retired the per-pass adoption headroom entirely - the sweep now ENQUEUES
// an adoptable dependency merge request under the same dedup key the webhook
// uses, so there is no lane cap left to defer against and nothing left to skip.
//
//	mint_budget_bound  maxOpenTasks (6 in prod) is full and an orphan is
//	                   deferred to the next pass - a CAP DOING ITS JOB, not a
//	                   failure.
//
// A cap that never frees is still visible - as a Task whose
// operator_task_state_age_seconds keeps climbing, which is the signal that
// actually names the stuck object - and the deferral stays on
// operator_sweep_skipped_total for a dashboard to read.
var steadyStateSkipReasons = []string{mintBudgetSkipReason}

// A NORMAL STEADY STATE MUST NOT FIRE A PERSISTENT ALERT.
//
// operator_sweep_skipped_total{reason="mint_budget_bound"} is emitted once per
// pass in which maxOpenTasks (6 in prod) is full and an orphan is deferred to
// the next pass - a CAP DOING ITS JOB, not a failure. TataraSweepSkipPersistent
// alerts on ANY reason at >= sweepSkipPassThreshold increases in its window, so
// an unexcluded steady-state reason fires it permanently and buries the ONE
// signal it was written for (reason="mr_claimed_by_other_task": a live Task
// that has stopped progressing).
//
// The reason is excluded in the ALERT rather than metered on its own counter:
// that keeps the alert meaningful for every other member of the closed reason
// set, and keeps the deferral visible on the series a dashboard already reads.
// It is deduped to ONE increment per pass (sweepBudget.countBudgetSkip), which
// fixes the metric - a backlog no longer inflates the series by its own size -
// but does NOT clear the alert: the window is 24h at threshold 6 and the sweep
// runs the 4h issueScan cron, so a project parked at its task cap emits exactly
// 6 increases in the window and fires anyway.
func TestSweepSkipPersistentAlertExcludesTheSteadyStateDeferrals(t *testing.T) {
	path := filepath.Join("..", "..", "charts", "tatara-operator", "templates", "prometheusrule.yaml")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	expr := alertExpr(t, string(src), "TataraSweepSkipPersistent")
	for _, r := range steadyStateSkipReasons {
		if !strings.Contains(expr, `reason!="`+r+`"`) {
			t.Fatalf("TataraSweepSkipPersistent expr does not exclude reason=%q:\n  %s\n"+
				"That deferral is a normal steady state, emitted every pass forever, so it "+
				"fires this alert permanently and buries reason=mr_claimed_by_other_task.", r, expr)
		}
	}
	// The alert must still SEE every other reason: an exclusion that narrowed it
	// to one reason would be the opposite mistake.
	if !strings.Contains(expr, "operator_sweep_skipped_total") {
		t.Fatalf("TataraSweepSkipPersistent no longer reads operator_sweep_skipped_total:\n  %s", expr)
	}
	for _, r := range sweepSkipReasons {
		if slices.Contains(steadyStateSkipReasons, r) {
			continue
		}
		if strings.Contains(expr, `reason!="`+r+`"`) {
			t.Errorf("TataraSweepSkipPersistent also excludes reason %q; only a CAP DOING ITS "+
				"JOB is a normal steady state", r)
		}
	}
}

// alertExpr pulls one alert's expr line out of the UNRENDERED PrometheusRule
// source. The file is a helm template and the expr lines carry template actions
// of their own (TataraSweepSkipPersistent interpolates both sweepSkipWindow and
// sweepSkipPassThreshold), so this is a text scan and not a YAML parse - and the
// string it returns is the TEMPLATE, not the rendered PromQL. Every caller must
// therefore assert on a literal fragment that survives rendering, such as a
// label matcher, and never on the value of an interpolated tunable.
func alertExpr(t *testing.T, src, alert string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^\s*- alert: ` + regexp.QuoteMeta(alert) + `\n\s*expr: (.+)$`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("no `- alert: %s` with an expr on the next line in the PrometheusRule template", alert)
	}
	return m[1]
}
