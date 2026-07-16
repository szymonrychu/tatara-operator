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

// Two firings of the SAME rule, differing only in volatile labels (pod,
// reason), must dedup to one tracker: the second createIncidentTask/webhook
// POST must NOT create a second QueuedEvent. This is the regression the
// #320/#328 rewrite (O1-O3) exists to fix, exercised through the actual HTTP
// admission path (not just the pure incidentDedupKey function).
// The groupKeys deliberately DIFFER (as Alertmanager's real groupKey does: it
// embeds the group-by label VALUES, e.g. pod), so a passing test proves the
// fix - the OLD alertGroupHash(groupKey) would NOT have deduped these.
const grafanaFiringA = `{"status":"firing","groupKey":"{}/{alertname=\"MemStuck\",pod=\"pg-1\"}",` +
	`"commonLabels":{"alertname":"MemStuck","namespace":"tatara-memory","pod":"pg-1","reason":"CrashLoopBackOff"},` +
	`"commonAnnotations":{},"externalURL":"http://grafana:3000","alerts":[]}`

const grafanaFiringB = `{"status":"firing","groupKey":"{}/{alertname=\"MemStuck\",pod=\"neo4j-3\"}",` +
	`"commonLabels":{"alertname":"MemStuck","namespace":"tatara-memory","pod":"neo4j-3","reason":"CreateContainerError"},` +
	`"commonAnnotations":{},"externalURL":"http://grafana:3000","alerts":[]}`

func TestCreateIncidentTask_UsesRuleWorkloadKey(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p1"), grafanaSecret("p1"))

	w1 := postGrafana(r, "p1", "tok", grafanaFiringA)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first fire: want 202, got %d: %s", w1.Code, w1.Body.String())
	}
	if n := len(listIncidentQueuedEvents(t, fc)); n != 1 {
		t.Fatalf("first fire: want 1 incident QueuedEvent, got %d", n)
	}

	w2 := postGrafana(r, "p1", "tok", grafanaFiringB)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("second fire: want 202, got %d: %s", w2.Code, w2.Body.String())
	}
	if n := len(listIncidentQueuedEvents(t, fc)); n != 1 {
		t.Fatalf("rule+workload dedup failed: want STILL 1 incident QueuedEvent after a pod/reason-only refire, got %d", n)
	}
}

// TestSuppressedRefire_CoalescesComment: given an open incident Issue CR
// (rule-key stamped) and a suppressed refire, RefireCount increments every
// time; the FIRST refire (LastRefireCommentAt nil) enqueues one
// PendingComment and sets LastRefireCommentAt (metric result=posted); an
// immediate SECOND refire (within cooldown) enqueues NO new comment but still
// increments RefireCount (metric result=coalesced).
func TestSuppressedRefire_CoalescesComment(t *testing.T) {
	sch := runtime.NewScheme()
	_ = tatarav1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)

	proj := grafanaProject("p1")
	dedupKey := incidentDedupKey(mustParseGrafanaAlert(t, grafanaFiringA), proj.Name, denylistSet(nil))
	iss := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "iss-tatara-operator-320", Namespace: "tatara",
			Labels: map[string]string{queue.LabelAlertRuleKey: dedupKey},
		},
		Spec: tatarav1.IssueSpec{RepositoryRef: "tatara-operator", Number: 320, ProjectRef: proj.Name},
	}

	fc := fake.NewClientBuilder().WithScheme(sch).
		WithObjects(proj, grafanaSecret("p1"), iss).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}, &tatarav1.Issue{}).
		Build()
	if err := fc.Status().Update(context.Background(), iss); err != nil {
		t.Fatalf("seed issue status: %v", err)
	}
	iss.Status.State = "open"
	if err := fc.Status().Update(context.Background(), iss); err != nil {
		t.Fatalf("seed issue status open: %v", err)
	}

	seq := &queue.SeqSource{Client: fc, Namespace: "tatara"}
	reg := prometheus.NewRegistry()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	s := NewServer(Config{
		Client: fc, Namespace: "tatara", Metrics: obs.NewOperatorMetrics(reg), Seq: seq,
		IncidentRefireCommentCooldown: 30 * time.Minute,
		Now:                           func() time.Time { return now },
	})
	r := chi.NewRouter()
	s.Mount(r)

	// First refire: suppressed (the Issue already tracks this rule-key), no live
	// QE/Task competing. Must post ONE comment and set LastRefireCommentAt.
	w1 := postGrafana(r, "p1", "tok", grafanaFiringA)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first refire: want 202, got %d: %s", w1.Code, w1.Body.String())
	}

	var got1 tatarav1.Issue
	if err := fc.Get(context.Background(), client.ObjectKeyFromObject(iss), &got1); err != nil {
		t.Fatalf("get issue after first refire: %v", err)
	}
	if got1.Status.RefireCount != 1 {
		t.Fatalf("RefireCount after first refire = %d, want 1", got1.Status.RefireCount)
	}
	if len(got1.Status.PendingComments) != 1 {
		t.Fatalf("PendingComments after first refire = %d, want 1", len(got1.Status.PendingComments))
	}
	if got1.Status.LastRefireCommentAt == nil {
		t.Fatal("LastRefireCommentAt not set after first refire")
	}

	// Second refire (immediate, well within the 30m cooldown): RefireCount still
	// increments, but NO second comment is enqueued.
	now = now.Add(time.Minute)
	w2 := postGrafana(r, "p1", "tok", grafanaFiringA)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("second refire: want 202, got %d: %s", w2.Code, w2.Body.String())
	}

	var got2 tatarav1.Issue
	if err := fc.Get(context.Background(), client.ObjectKeyFromObject(iss), &got2); err != nil {
		t.Fatalf("get issue after second refire: %v", err)
	}
	if got2.Status.RefireCount != 2 {
		t.Fatalf("RefireCount after second refire = %d, want 2", got2.Status.RefireCount)
	}
	if len(got2.Status.PendingComments) != 1 {
		t.Fatalf("PendingComments after second (coalesced) refire = %d, want STILL 1", len(got2.Status.PendingComments))
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var posted, coalesced float64
	for _, mf := range mfs {
		if mf.GetName() != "operator_incident_refire_comment_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() != "result" {
					continue
				}
				switch lp.GetValue() {
				case "posted":
					posted = m.GetCounter().GetValue()
				case "coalesced":
					coalesced = m.GetCounter().GetValue()
				}
			}
		}
	}
	if posted != 1 {
		t.Fatalf("posted metric = %g, want 1", posted)
	}
	if coalesced != 1 {
		t.Fatalf("coalesced metric = %g, want 1", coalesced)
	}
}

func mustParseGrafanaAlert(t *testing.T, body string) GrafanaAlert {
	t.Helper()
	a, err := parseGrafanaAlert([]byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return a
}
