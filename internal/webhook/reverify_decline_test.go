package webhook

// The 2026-07-27 stall (gitlab helmfile#26): a maintainer's "use the starred
// options!\ngo ahead" matched the C.6 grammar and the label reached the forge,
// but loadOwnedIssues re-read the owning Issue through the CACHED client
// microseconds after the grammar's own write, lost the race, and the un-park
// declined with a bare `if !ok { return nil }` - no log, no metric, and the
// grammar's verdict was thrown away so nothing could retry it later.
//
// This file proves the three-part fix: (1) the verdict is now DURABLE
// (Task.Status.ApprovalVerdict survives a decline, so Task 4's periodic
// backstop can re-enter without re-running the grammar), (2) every reverify
// decline is named, logged, and counted, and (3) loadOwnedIssues reads the
// UNCACHED APIReader, not the cached client.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// rvProject is a maintainer-configured Project distinct from pending_events_test's
// peProject: named to match the live incident's project ("infrastructure") and
// its repo ("helmfile").
func rvProject() *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "infrastructure", Namespace: peNS},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "rv-scm",
			Scm: &tatarav1.ScmSpec{
				Provider:         "github",
				Owner:            "o",
				BotLogin:         "tatara-bot",
				MaintainerLogins: []string{"szymonrychu"},
			},
		},
	}
}

func rvRepo() *tatarav1.Repository {
	return &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "helmfile", Namespace: peNS},
		Spec:       tatarav1.RepositorySpec{ProjectRef: "infrastructure", URL: "https://github.com/o/helmfile.git", DefaultBranch: "main"},
	}
}

// newReverifyTestServer builds a *Server whose cfg.Client and cfg.APIReader
// are the SAME fake client (peClient, already carrying the IssueKeyIndex
// SyncIssueOnDemand needs) seeded with objs, wired to reader for the C3-3
// on-demand forge sync. It returns the real *obs.OperatorMetrics so the
// caller can assert on operator_unpark_declined_total via the same
// testutil.ToFloat64 idiom every sibling decline test already uses.
func newReverifyTestServer(t *testing.T, reader scm.SCMReader, objs ...client.Object) (*Server, *obs.OperatorMetrics) {
	t.Helper()
	c := peClient(t, objs...)
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	return NewServer(Config{
		Client:    c,
		APIReader: c,
		Namespace: peNS,
		Metrics:   metrics,
		Spiller:   &stubSpiller{},
		ReaderFor: func(string, string) (scm.SCMReader, error) { return reader, nil },
	}), metrics
}

// newSplitReaderTestServer builds a *Server with TWO DIFFERENT fake clients:
// cfg.Client (cached) seeded from cached, cfg.APIReader seeded from
// apiReaderObj - reproducing the exact shape of the 2026-07-27 stall, where
// the cached client had not yet observed a write the APIReader's store
// already carries.
func newSplitReaderTestServer(t *testing.T, cached, apiReaderObj client.Object) *Server {
	t.Helper()
	cachedClient := peClient(t, cached)
	apiReaderClient := peClient(t, apiReaderObj)
	return NewServer(Config{
		Client:    cachedClient,
		APIReader: apiReaderClient,
		Namespace: peNS,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
	})
}

// The 2026-07-27 stall: the grammar passed, the label landed, and the Task
// never moved. The verdict must be DURABLE so the periodic backstop can retry,
// and the decline must be LOGGED and COUNTED so nobody has to guess again.
//
// The Task owns TWO Issues:
//   - iss-helmfile-26 carries the maintainer's approving comment (synced
//     on-demand from the forge, exactly like production): the C.6 grammar
//     passes on it and it becomes the ApprovalVerdict's source.
//   - iss-helmfile-9 is open but Status=rejected: OUT of the C.6 grammar's
//     scope (approvalInScope excludes done/rejected), so it never blocks the
//     grammar pass, but IN the F.6 rule's open-issue scope (openIssues only
//     checks State==open) - so it is what makes allApproved false. This is a
//     real, narrow mismatch between the two scope definitions, and it
//     reproduces DeclineNotAllApproved even though the grammar itself passed.
func TestReverifyParked_PersistsTheVerdictAndNamesTheDecline(t *testing.T) {
	proj := rvProject()
	repo := rvRepo()
	sec := peSecret("rv-scm", "pat")

	task := &tatarav1.Task{}
	task.Namespace = peNS
	task.Name = "infrastructure-clarify-2026-07-27-gtwgp"
	task.Spec.ProjectRef = proj.Name
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now()}
	task.Status.IssueRefs = []string{"iss-helmfile-26", "iss-helmfile-9"}
	task.Status.PendingEvents = []tatarav1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 26,
		Author: "szymonrychu", Body: "use the starred options!\ngo ahead",
	}}

	iss := &tatarav1.Issue{}
	iss.Namespace = peNS
	iss.Name = "iss-helmfile-26"
	iss.Spec = tatarav1.IssueSpec{RepositoryRef: "helmfile", Number: 26, ProjectRef: proj.Name}
	iss.Status.State = "open"
	iss.Status.Status = "open" // deliberately NOT approved yet

	otherIss := &tatarav1.Issue{}
	otherIss.Namespace = peNS
	otherIss.Name = "iss-helmfile-9"
	otherIss.Spec = tatarav1.IssueSpec{RepositoryRef: "helmfile", Number: 9, ProjectRef: proj.Name}
	otherIss.Status.State = "open"
	otherIss.Status.Status = "rejected"

	rd := &fakeApprovalReader{comments: []scm.IssueComment{
		{ExternalID: "c-star", Author: "szymonrychu", Body: "use the starred options!\ngo ahead", CreatedAt: time.Now().UTC()},
	}}
	srv, metrics := newReverifyTestServer(t, rd, proj, repo, sec, task, iss, otherIss)

	srv.reverifyParked(context.Background(), proj, task, task.Status.PendingEvents[0])

	fresh := &tatarav1.Task{}
	if err := srv.cfg.Client.Get(context.Background(), objKey(peNS, task.Name), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.ApprovalVerdict == nil {
		t.Fatal("ApprovalVerdict is nil: a grammar pass that is not persisted cannot be retried by the backstop")
	}
	if fresh.Status.ApprovalVerdict.IssueRef != "iss-helmfile-26" {
		t.Errorf("verdict.IssueRef = %q, want iss-helmfile-26", fresh.Status.ApprovalVerdict.IssueRef)
	}
	if fresh.Status.ApprovalVerdict.Author != "szymonrychu" {
		t.Errorf("verdict.Author = %q, want szymonrychu", fresh.Status.ApprovalVerdict.Author)
	}
	if fresh.Status.Stage != tatarav1.StageParked || fresh.Status.StageReason != stage.ReasonIdentityUnverified {
		t.Errorf("stage = (%q,%q), want still parked(identity-unverified) - not every owned issue is approved yet",
			fresh.Status.Stage, fresh.Status.StageReason)
	}
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonIdentityUnverified, string(controller.DeclineNotAllApproved))); got != 1 {
		t.Errorf("operator_unpark_declined_total{identity-unverified,not-all-approved} = %v, want 1", got)
	}
}

// The read that lost the cache race must be the UNCACHED one.
func TestLoadOwnedIssuesUsesTheUncachedReader(t *testing.T) {
	iss := &tatarav1.Issue{}
	iss.Namespace = peNS
	iss.Name = "iss-helmfile-26"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	task := &tatarav1.Task{}
	task.Namespace = peNS
	task.Name = "t"
	task.Status.IssueRefs = []string{"iss-helmfile-26"}

	// The CACHED client is seeded with a STALE, unapproved copy; only the
	// APIReader has the approval. loadOwnedIssues must see the approval.
	stale := iss.DeepCopy()
	stale.Status.Status = "open"

	srv := newSplitReaderTestServer(t, stale, iss)

	issues, err := srv.loadOwnedIssues(context.Background(), task)
	if err != nil {
		t.Fatalf("loadOwnedIssues: %v", err)
	}
	if len(issues) != 1 || issues[0].Status.Status != "approved" {
		t.Fatalf("loadOwnedIssues read the CACHED client: got %+v, want the approved APIReader copy", issues)
	}
}
