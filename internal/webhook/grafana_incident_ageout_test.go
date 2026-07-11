package webhook

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestGrafana_StaleIncident_ReTriggers is liveness-hardening finding #5: a re-firing
// alert is deduped for the WHOLE lifetime of a non-terminal incident Task (dedup on
// alertGroupHash). If that incident is WEDGED non-terminal, the firing alert can
// never escalate. Past a staleness bound, a persistent re-fire must re-trigger/bump
// the wedged incident instead of being silently suppressed forever.
func TestGrafana_StaleIncident_ReTriggers(t *testing.T) {
	alert, err := parseGrafanaAlert([]byte(grafanaFiring))
	require.NoError(t, err)
	gh := alertGroupHash(alert)

	// A wedged non-terminal incident for this alert group, last active long ago.
	stale := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stale-incident", Namespace: "tatara",
			Labels: map[string]string{tatarav1.LabelActivity: "incident", tatarav1.LabelAlertGroup: gh, queue.LabelDedupKey: gh},
		},
		Spec:   tatarav1.TaskSpec{ProjectRef: "pstale", Kind: "incident", Goal: "investigate"},
		Status: tatarav1.TaskStatus{Phase: "Running"},
	}

	r, fc := grafanaRouter(t, grafanaProject("pstale"), grafanaSecret("pstale"), stale)
	old := metav1.NewTime(time.Now().Add(-24 * time.Hour))
	stale.Status.LastActivityAt = &old
	require.NoError(t, fc.Status().Update(context.Background(), stale))

	w := postGrafana(r, "pstale", "tok", grafanaFiring)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, fc.Get(context.Background(), client.ObjectKey{Namespace: "tatara", Name: "stale-incident"}, &got))
	require.Equal(t, "", got.Status.Phase, "a stale wedged incident must be re-driven (Phase reset) on a persistent re-fire")
	require.NotNil(t, got.Status.LastActivityAt)
	require.True(t, got.Status.LastActivityAt.After(old.Time), "the re-fire must bump the incident's last-activity")
}

// TestGrafana_FreshIncident_StillDeduped: a non-terminal incident that is NOT stale
// (recently active) still dedups a re-fire - the age-out only fires past the bound,
// so a live investigation is never disrupted.
func TestGrafana_FreshIncident_StillDeduped(t *testing.T) {
	alert, err := parseGrafanaAlert([]byte(grafanaFiring))
	require.NoError(t, err)
	gh := alertGroupHash(alert)

	fresh := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fresh-incident", Namespace: "tatara",
			Labels: map[string]string{tatarav1.LabelActivity: "incident", tatarav1.LabelAlertGroup: gh, queue.LabelDedupKey: gh},
		},
		Spec:   tatarav1.TaskSpec{ProjectRef: "pfresh", Kind: "incident", Goal: "investigate"},
		Status: tatarav1.TaskStatus{Phase: "Running"},
	}

	r, fc := grafanaRouter(t, grafanaProject("pfresh"), grafanaSecret("pfresh"), fresh)
	recent := metav1.NewTime(time.Now().Add(-time.Minute))
	fresh.Status.LastActivityAt = &recent
	require.NoError(t, fc.Status().Update(context.Background(), fresh))

	w := postGrafana(r, "pfresh", "tok", grafanaFiring)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, fc.Get(context.Background(), client.ObjectKey{Namespace: "tatara", Name: "fresh-incident"}, &got))
	require.Equal(t, "Running", got.Status.Phase, "a fresh (non-stale) incident must not be disrupted by a re-fire")
	require.Empty(t, listIncidentQueuedEvents(t, fc), "no new incident QE while a fresh incident is deduping")
}
