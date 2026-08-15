package obs

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// upgradeHeadroomSkipReason is controller.SweepSkipUpgradeHeadroom. Literal
// here for the same no-reverse-import reason as sweepSkipReasons, and it is a
// member of that list, so TestSweepSkipReasonsMatchSweepConstants already pins
// the string against the constant in sweep.go.
const upgradeHeadroomSkipReason = "upgrade_headroom_bound"

// A NORMAL STEADY STATE MUST NOT FIRE A PERSISTENT ALERT.
//
// operator_sweep_skipped_total{reason="upgrade_headroom_bound"} is emitted once
// per DEFERRED dependency merge request per sweep pass. A dependency engine
// opens more merge requests than maxOpenUpgrades has lanes by construction -
// that is what the cap is for - so a backlog of ten deferred bumps increments
// this ten times per pass, thousands of times a day, forever. TataraSweepSkipPersistent
// alerts on ANY reason at >= sweepSkipPassThreshold increases in its window, so
// it fires permanently and buries the ONE signal it was written for
// (reason="mr_claimed_by_other_task": a live Task that has stopped progressing).
//
// The reason is excluded in the ALERT rather than metered on its own counter:
// that keeps the alert meaningful for every other member of the closed reason
// set, and keeps the deferral visible on the series a dashboard already reads.
func TestSweepSkipPersistentAlertExcludesTheUpgradeHeadroomDeferral(t *testing.T) {
	path := filepath.Join("..", "..", "charts", "tatara-operator", "templates", "prometheusrule.yaml")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	expr := alertExpr(t, string(src), "TataraSweepSkipPersistent")
	if !strings.Contains(expr, `reason!="`+upgradeHeadroomSkipReason+`"`) {
		t.Fatalf("TataraSweepSkipPersistent expr does not exclude reason=%q:\n  %s\n"+
			"A deferred dependency merge request is a normal steady state and fires this "+
			"alert permanently, burying reason=mr_claimed_by_other_task.", upgradeHeadroomSkipReason, expr)
	}
	// The alert must still SEE every other reason: an exclusion that narrowed it
	// to one reason would be the opposite mistake.
	if !strings.Contains(expr, "operator_sweep_skipped_total") {
		t.Fatalf("TataraSweepSkipPersistent no longer reads operator_sweep_skipped_total:\n  %s", expr)
	}
	for _, r := range sweepSkipReasons {
		if r == upgradeHeadroomSkipReason {
			continue
		}
		if strings.Contains(expr, `reason!="`+r+`"`) {
			t.Errorf("TataraSweepSkipPersistent also excludes reason %q; only the headroom "+
				"deferral is a normal steady state", r)
		}
	}
}

// alertExpr pulls one alert's expr line out of the rendered-but-untemplated
// PrometheusRule source. The file is a helm template, so this is a text scan and
// not a YAML parse - the expr itself carries no template action, which is the
// only property this needs.
func alertExpr(t *testing.T, src, alert string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^\s*- alert: ` + regexp.QuoteMeta(alert) + `\n\s*expr: (.+)$`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("no `- alert: %s` with an expr on the next line in the PrometheusRule template", alert)
	}
	return m[1]
}
