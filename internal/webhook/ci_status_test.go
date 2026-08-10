package webhook

// PR A item 2. A Kind:"ci" delivery used to fall through handle()'s switch onto
// the `default: accept(..., "ignored")` arm. These tests pin the replacement:
// the delivery is joined to the mirrored MergeRequest CRs at that head sha and
// stamps status.ciStatus + status.ciUpdatedAt on each.
//
// Nothing here has to wake the owning Task by hand: TaskReconciler's builder
// carries Owns(&MergeRequest{}) (task_controller.go), so the mirror status write
// enqueues the owner for free.

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func ciEvent(headSHA, status string) scm.WebhookEvent {
	return scm.WebhookEvent{Kind: "ci", Repo: peRepoURL, HeadSHA: headSHA, CIStatus: status}
}

func TestCIStatus_StampsMatchingHeadSHA(t *testing.T) {
	task := peTask("t-ci-1", tatarav1.StateUnderImplementation, "")
	mr := peMR(11, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "sha-head"})
	proj := peProject("tatara-bot")
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	w := httptest.NewRecorder()
	s.handleCIStatus(context.Background(), w, "github", *proj, ciEvent("sha-head", scm.CIMirrorRed))
	require.Equal(t, 202, w.Code)

	got := getPEMR(t, c, mr.Name)
	require.Equal(t, scm.CIMirrorRed, got.Status.CIStatus)
	require.NotNil(t, got.Status.CIUpdatedAt, "a live CI observation must date itself")
}

// The status is re-stamped even when it does not change, because ciUpdatedAt
// dates the OBSERVATION. A re-confirmed green is fresher than the one before it,
// and the whole point of the field is to let the bundle say so.
func TestCIStatus_RestampsUnchangedStatusToRefreshTheDate(t *testing.T) {
	task := peTask("t-ci-2", tatarav1.StateUnderImplementation, "")
	old := metav1.NewTime(time.Date(2026, 8, 10, 1, 0, 0, 0, time.UTC))
	mr := peMR(12, task, tatarav1.MergeRequestStatus{
		State: "open", HeadSHA: "sha-head", CIStatus: scm.CIMirrorGreen, CIUpdatedAt: &old,
	})
	proj := peProject("tatara-bot")
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	w := httptest.NewRecorder()
	s.handleCIStatus(context.Background(), w, "github", *proj, ciEvent("sha-head", scm.CIMirrorGreen))
	require.Equal(t, 202, w.Code)

	got := getPEMR(t, c, mr.Name)
	require.Equal(t, scm.CIMirrorGreen, got.Status.CIStatus)
	require.True(t, got.Status.CIUpdatedAt.After(old.Time), "ciUpdatedAt must advance on every observation")
}

// A CI delivery for a head this platform does not mirror is the NORMAL case:
// CI runs on every push to every branch, and most branches are nobody's Task.
// It is accepted and dropped, never an error the forge will retry.
func TestCIStatus_UnknownHeadSHA_AcceptedAndDropped(t *testing.T) {
	task := peTask("t-ci-3", tatarav1.StateUnderImplementation, "")
	mr := peMR(13, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "sha-ours"})
	proj := peProject("tatara-bot")
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	w := httptest.NewRecorder()
	s.handleCIStatus(context.Background(), w, "github", *proj, ciEvent("sha-someone-elses", scm.CIMirrorRed))
	require.Equal(t, 202, w.Code, "an unmatched CI delivery must not be an error")

	got := getPEMR(t, c, mr.Name)
	require.Empty(t, got.Status.CIStatus, "a CI event at a head we do not mirror must not touch our mirror")
	require.Nil(t, got.Status.CIUpdatedAt)
}

// A delivery from a repository this Project does not own is dropped before any
// mirror is read.
func TestCIStatus_UnknownRepository_AcceptedAndDropped(t *testing.T) {
	task := peTask("t-ci-4", tatarav1.StateUnderImplementation, "")
	mr := peMR(14, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "sha-head"})
	proj := peProject("tatara-bot")
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	ev := ciEvent("sha-head", scm.CIMirrorRed)
	ev.Repo = "https://github.com/other/repo.git"
	w := httptest.NewRecorder()
	s.handleCIStatus(context.Background(), w, "github", *proj, ev)
	require.Equal(t, 202, w.Code)

	require.Empty(t, getPEMR(t, c, mr.Name).Status.CIStatus)
}

// Two MRs can legitimately share a head sha (a PR retargeted at a second base
// branch, or a stacked pair). Every match is stamped; the join is on the sha,
// not on "the first one found".
func TestCIStatus_StampsEveryMirrorAtThatHead(t *testing.T) {
	task := peTask("t-ci-5", tatarav1.StateUnderImplementation, "")
	a := peMR(15, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "sha-head"})
	b := peMR(16, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "sha-head"})
	other := peMR(17, task, tatarav1.MergeRequestStatus{State: "open", HeadSHA: "sha-other"})
	proj := peProject("tatara-bot")
	c := peClient(t, proj, peRepo(), task, a, b, other)
	s := peServer(c, &stubSpiller{}, nil)

	w := httptest.NewRecorder()
	s.handleCIStatus(context.Background(), w, "github", *proj, ciEvent("sha-head", scm.CIMirrorGreen))
	require.Equal(t, 202, w.Code)

	require.Equal(t, scm.CIMirrorGreen, getPEMR(t, c, a.Name).Status.CIStatus)
	require.Equal(t, scm.CIMirrorGreen, getPEMR(t, c, b.Name).Status.CIStatus)
	require.Empty(t, getPEMR(t, c, other.Name).Status.CIStatus)
}

func getPEMR(t *testing.T, c client.Client, name string) *tatarav1.MergeRequest {
	t.Helper()
	var mr tatarav1.MergeRequest
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: name}, &mr); err != nil {
		t.Fatalf("get mergerequest %s: %v", name, err)
	}
	return &mr
}
