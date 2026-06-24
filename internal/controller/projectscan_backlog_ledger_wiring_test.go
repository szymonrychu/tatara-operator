package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// mkSourceLedgerTask builds a non-terminal issueLifecycle Task carrying ONLY a
// role:source ledger entry (the steady state after the P2 seed-on-reconcile
// change). It has NO role:proposed entry - exactly the migration-window shape
// that the project-wide anyTaskHasLedger gate mishandled.
func mkSourceLedgerTask(name, repo string, number int) tatarav1alpha1.Task {
	var t tatarav1alpha1.Task
	t.Name = name
	t.Spec.Kind = "issueLifecycle"
	t.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Repo: repo, Number: number, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	return t
}

// TestBrainstorm_MigrationWindow_SourceLedgerPlusSCMProposals_TripsCap is the
// regression test for the critical migration-safety finding: a project whose
// Tasks carry ONLY source-role ledgers (so anyTaskHasLedger=true) while open
// proposals exist solely as SCM brainstorming issues must still trip the cap.
// Before the fix, the project-wide ledger gate forced total=0 and the cap was
// silently bypassed -> proposal flooding.
func TestBrainstorm_MigrationWindow_SourceLedgerPlusSCMProposals_TripsCap(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-mig-cap", []string{"o/m1", "o/m2"}, 2)
	// Two open brainstorming proposals live ONLY as SCM issues (no role:proposed
	// ledger entry anywhere) - the legacy/migration shape. cap=2 -> at cap.
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{
		"o/m1": {{Repo: "o/m1", Number: 1, Labels: []string{"tatara-brainstorming"}}},
		"o/m2": {{Repo: "o/m2", Number: 2, Labels: []string{"tatara-brainstorming"}}},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	// Existing Tasks carry source-role ledgers only (anyTaskHasLedger=true).
	existing := []tatarav1alpha1.Task{
		mkSourceLedgerTask("life-1", "o/m1", 1),
		mkSourceLedgerTask("life-2", "o/m2", 2),
	}
	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 2}
	r.brainstorm(context.Background(), proj, reader, repos, existing, act)

	qes := listBrainstormQEs(t, "bs-mig-cap")
	require.Empty(t, qes, "cap must trip from SCM count even when source-only ledgers exist; no brainstorm QE expected")
}

// TestHealthCheck_MigrationWindow_SourceLedgerPlusSCMProposals_TripsCap mirrors
// the brainstorm regression test for the healthCheck path.
func TestHealthCheck_MigrationWindow_SourceLedgerPlusSCMProposals_TripsCap(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "hc-mig-cap", []string{"o/h1", "o/h2"}, 2)
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{
		"o/h1": {{Repo: "o/h1", Number: 1, Labels: []string{"tatara-brainstorming"}}},
		"o/h2": {{Repo: "o/h2", Number: 2, Labels: []string{"tatara-brainstorming"}}},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{
		mkSourceLedgerTask("hlife-1", "o/h1", 1),
		mkSourceLedgerTask("hlife-2", "o/h2", 2),
	}
	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenProposals: 2}
	r.healthCheck(context.Background(), proj, reader, repos, existing, act)

	qes := listHealthCheckQEs(t, "hc-mig-cap")
	require.Empty(t, qes, "healthCheck cap must trip from SCM count even when source-only ledgers exist")
}

// TestBrainstorm_LedgerProposals_TripCap verifies the ledger path: N role:proposed
// entries at the cap block a new brainstorm even when SCM returns no proposal issues
// (the ledger is authoritative when it counts higher).
func TestBrainstorm_LedgerProposals_TripCap(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-ledger-cap", []string{"o/lc1", "o/lc2"}, 2)
	// SCM shows zero proposals; the cap must trip from the ledger alone.
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{
		"o/lc1": {},
		"o/lc2": {},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{
		mkProposedTask("o/lc1", "", "proposal A"),
		mkProposedTask("o/lc2", "", "proposal B"),
	}
	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 2}
	r.brainstorm(context.Background(), proj, reader, repos, existing, act)

	qes := listBrainstormQEs(t, "bs-ledger-cap")
	require.Empty(t, qes, "ledger proposal count must trip the cap when SCM count is zero")
}

// TestBrainstorm_UnderCap_LedgerAndSCM_Proceeds verifies that when both counts
// are under the cap a brainstorm proceeds, and the SetOpenProposals gauge is
// refreshed even though ledger entries exist (no project-wide gauge staleness).
func TestBrainstorm_UnderCap_LedgerAndSCM_Proceeds(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-under-cap", []string{"o/uc1"}, 5)
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{
		"o/uc1": {{Repo: "o/uc1", Number: 1, Labels: []string{"tatara-brainstorming"}}},
	}}
	reg := prometheus.NewRegistry()
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(reg)

	existing := []tatarav1alpha1.Task{mkSourceLedgerTask("uc-life", "o/uc1", 9)}
	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, existing, act)

	qes := listBrainstormQEs(t, "bs-under-cap")
	require.Len(t, qes, 1, "under cap -> exactly one brainstorm QE")

	// The per-repo open-proposals gauge must be refreshed (not stale) despite the
	// ledger being non-empty.
	require.Equal(t, 1.0, auditGaugeValue(t, reg, "operator_open_proposals", map[string]string{"repo": "o/uc1"}),
		"SetOpenProposals must be emitted for the repo in the ledger-present path")
}
