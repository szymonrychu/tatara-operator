package webhook

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// incidentGroupKey correlates DIFFERENT alert rules that share the configured
// correlation-label values (namespace/cluster), while keeping their dedup keys
// distinct - so a 5-alert storm for one root cause links into one tree.
func TestIncidentGroupKey(t *testing.T) {
	corr := correlationSet(nil) // namespace, cluster
	a := GrafanaAlert{CommonLabels: map[string]string{
		"alertname": "CNPGConnectionsHigh", "namespace": "tatara-memory", "cluster": "home", "pod": "pg-1"}}
	b := GrafanaAlert{CommonLabels: map[string]string{
		"alertname": "PostgresDown", "namespace": "tatara-memory", "cluster": "home", "pod": "pg-2"}}

	ka := incidentGroupKey(a, "tatara", corr)
	kb := incidentGroupKey(b, "tatara", corr)
	if ka == "" || kb == "" {
		t.Fatalf("group key empty: ka=%q kb=%q", ka, kb)
	}
	if ka != kb {
		t.Fatalf("different rules in the SAME namespace/cluster must share a group key: %q vs %q", ka, kb)
	}
	// Their DEDUP keys stay distinct (different alertname), so admission does not
	// suppress the second - correlation is link-only.
	den := denylistSet(nil)
	if incidentDedupKey(a, "tatara", den) == incidentDedupKey(b, "tatara", den) {
		t.Fatal("distinct alert rules must keep distinct dedup keys")
	}

	// A different namespace is a different group.
	c := GrafanaAlert{CommonLabels: map[string]string{
		"alertname": "PostgresDown", "namespace": "other", "cluster": "home"}}
	if incidentGroupKey(c, "tatara", corr) == ka {
		t.Fatal("a different namespace must be a different group")
	}
	// A different project is a different group even with identical labels.
	if incidentGroupKey(a, "other-project", corr) == ka {
		t.Fatal("group key must be project-scoped")
	}
}

// An alert carrying NONE of the correlation labels gets an EMPTY group key: an
// all-empty group would bucket every unlabelled alert together (a false
// correlation), so no correlation is safer.
func TestIncidentGroupKey_NoCorrelationLabelIsEmpty(t *testing.T) {
	corr := correlationSet(nil)
	a := GrafanaAlert{CommonLabels: map[string]string{"alertname": "SomethingElse", "service": "x"}}
	if got := incidentGroupKey(a, "tatara", corr); got != "" {
		t.Fatalf("group key with no correlation label present = %q, want empty", got)
	}
}

// escalationHarness seeds an open incident tracker with a given RefireCount and
// creation time, then drives one suppressed refire through the HTTP path.
func escalationHarness(t *testing.T, refireCount int, escalatedAt *metav1.Time, created time.Time,
	cfgMut func(*Config)) (client.Client, *prometheus.Registry, *tatarav1.Issue) {
	t.Helper()
	sch := runtime.NewScheme()
	_ = tatarav1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)

	proj := grafanaProject("p1")
	dedupKey := incidentDedupKey(mustParseGrafanaAlert(t, grafanaFiringA), proj.Name, denylistSet(nil))
	iss := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "iss-tatara-operator-320", Namespace: "tatara",
			Labels:            map[string]string{queue.LabelAlertRuleKey: dedupKey},
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: tatarav1.IssueSpec{RepositoryRef: "tatara-operator", Number: 320, ProjectRef: proj.Name},
	}
	fc := fake.NewClientBuilder().WithScheme(sch).
		WithObjects(proj, grafanaSecret("p1"), iss).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}, &tatarav1.Issue{}).
		Build()
	iss.Status.State = "open"
	iss.Status.RefireCount = refireCount
	iss.Status.EscalatedAt = escalatedAt
	if err := fc.Status().Update(context.Background(), iss); err != nil {
		t.Fatalf("seed issue status: %v", err)
	}

	reg := prometheus.NewRegistry()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	cfg := Config{
		Client: fc, Namespace: "tatara", Metrics: obs.NewOperatorMetrics(reg),
		Seq:                             &queue.SeqSource{Client: fc, Namespace: "tatara"},
		IncidentRefireCommentCooldown:   30 * time.Minute,
		IncidentEscalateRefireThreshold: 3,
		IncidentEscalateStaleAge:        48 * time.Hour,
		Now:                             func() time.Time { return now },
	}
	if cfgMut != nil {
		cfgMut(&cfg)
	}
	s := NewServer(cfg)
	r := chi.NewRouter()
	s.Mount(r)
	if w := postGrafana(r, "p1", "tok", grafanaFiringA); w.Code != http.StatusAccepted {
		t.Fatalf("refire POST: want 202, got %d: %s", w.Code, w.Body.String())
	}
	return fc, reg, iss
}

// A suppressed refire that crosses the refire threshold RE-ADMITS a fresh
// incident investigation (a QueuedEvent is minted despite the open tracker) and
// stamps EscalatedAt.
func TestSuppressedRefire_EscalatesAtThreshold(t *testing.T) {
	// RefireCount 2 -> after this refire it becomes 3 == threshold.
	fc, reg, iss := escalationHarness(t, 2, nil, time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC), nil)

	if n := len(listIncidentQueuedEvents(t, fc)); n != 1 {
		t.Fatalf("escalation must mint one fresh incident QueuedEvent past the open tracker, got %d", n)
	}
	var got tatarav1.Issue
	if err := fc.Get(context.Background(), client.ObjectKeyFromObject(iss), &got); err != nil {
		t.Fatalf("get issue: %v", err)
	}
	if got.Status.RefireCount != 3 {
		t.Fatalf("RefireCount = %d, want 3", got.Status.RefireCount)
	}
	if got.Status.EscalatedAt == nil {
		t.Fatal("EscalatedAt must be stamped on escalation")
	}
	assertCounter(t, reg, "operator_incident_escalated_total", "result", "minted", 1)
}

// Below the threshold (and not stale) a suppressed refire only comments - no
// escalation Task is minted.
func TestSuppressedRefire_NoEscalateBelowThreshold(t *testing.T) {
	fc, _, _ := escalationHarness(t, 0, nil, time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC),
		func(c *Config) { c.IncidentEscalateRefireThreshold = 100 })
	if n := len(listIncidentQueuedEvents(t, fc)); n != 0 {
		t.Fatalf("no escalation expected below threshold, got %d QueuedEvents", n)
	}
}

// A tracker already escalated within the stale-age window is NOT re-escalated
// even when the refire threshold is met again (at most one escalation/window).
func TestSuppressedRefire_NoReEscalateWithinWindow(t *testing.T) {
	recent := metav1.NewTime(time.Date(2026, 7, 18, 11, 30, 0, 0, time.UTC)) // 30m ago
	fc, _, _ := escalationHarness(t, 2, &recent, time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC), nil)
	if n := len(listIncidentQueuedEvents(t, fc)); n != 0 {
		t.Fatalf("must not re-escalate within the stale-age window, got %d QueuedEvents", n)
	}
}

func assertCounter(t *testing.T, reg *prometheus.Registry, name, label, value string, want float64) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var got float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					got = m.GetCounter().GetValue()
				}
			}
		}
	}
	if got != want {
		t.Fatalf("%s{%s=%s} = %g, want %g", name, label, value, got, want)
	}
}
