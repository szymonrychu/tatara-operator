package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// THE DEPLOYING STAGE HAS AN ACTOR (gap G3).
//
// merge.go gates deploying -> delivered on every owned MR being merged AND
// carrying deployedAt. Nothing wrote deployedAt: the stage was a black hole and
// every Task sat there until its 2h budget parked it. StageDriver.ReconcileDeploying
// is the writer, and the MergeRequest CR is the ledger.

// mdDeployTime is after mdNewDriver's clock so a merge "happened" before the
// apply run that carried it.
var (
	mdMergedAt = metav1.NewTime(time.Date(2026, 7, 12, 11, 0, 0, 0, time.UTC))
	mdApplyAt  = time.Date(2026, 7, 12, 11, 30, 0, 0, time.UTC)
)

// mdPin renders an applied helmfile pin state carrying one release at version.
func mdPin(release, version string) string {
	return "releases:\n  - name: " + release + "\n    chart: oci://harbor/charts/" + release +
		"\n    version: " + version + "\n"
}

// mdImagePin is the IMAGE-pin form. A chart `version:` line is attributed to an
// artifact only through releaseArtifact (which names the helmfile RELEASES), so a
// component the helmfile pins by image - not by its own release - is pinned this
// way instead.
func mdImagePin(artifact, version string) string {
	return "        image: harbor.szymonrichert.pl/tatara/" + artifact + ":v" + version + "\n"
}

// mdDeployingMR is a merged, undeployed MR owned by task.
func mdDeployingMR(task *tatarav1alpha1.Task, repo string, number int) *tatarav1alpha1.MergeRequest {
	mr := mdMR(task, repo, number)
	mr.Status.State = "merged"
	mr.Status.MergedAt = &mdMergedAt
	return mr
}

// mdHelmfileRepo is the terminal CD repo, enrolled in the Project.
func mdHelmfileRepo() *tatarav1alpha1.Repository { return mdRepo(helmfileRepoName) }

func mdSuccessfulApply(sha string) scm.WorkflowRun {
	return scm.WorkflowRun{
		HeadSHA: sha, Status: "completed", Conclusion: "success",
		HTMLURL: "https://github.com/szymonrychu/tatara-helmfile/actions/runs/1", CreatedAt: mdApplyAt,
	}
}

// The apply sweep observes the merged MR's cut version applied: deployedAt and
// deployedVersion are stamped ON THE MERGEREQUEST, the Task delivers, and the
// owned Issue is closed BEFORE deliveredAt is stamped (contract C.4, Section I).
func TestDeployingStampsDeployedAtAndDelivers(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mr := mdDeployingMR(task, "tatara-operator", 7)
	iss := mdIssue(task, "tatara-operator", 41)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task, mr, iss)

	f := newFakeForge(t)
	rd := mdNewReader(f)
	rd.tags["tatara-operator"] = "v1.4.0"
	rd.runs[helmfileRepoName] = mdSuccessfulApply("apply-sha")
	rd.pin["main"] = mdPin("tatara-operator", "1.3.0")      // the artifact IS pinned
	rd.pin["apply-sha"] = mdPin("tatara-operator", "1.4.0") // and the apply carried v1.4.0
	d := mdNewDriverWithReader(t, f, c, rd)

	// THE ORDER IS THE CONTRACT: at the moment the Issue is closed, deliveredAt
	// must still be nil.
	f.closeHook = func() {
		if mdGetTask(t, c, "t1").Status.DeliveredAt != nil {
			t.Fatalf("deliveredAt was stamped BEFORE the owned Issue was closed (C.4 order)")
		}
	}

	if _, err := d.ReconcileDeploying(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}

	gm := mdGetMR(t, c, mr.Name)
	if gm.Status.DeployedAt == nil {
		t.Fatalf("deployedAt not stamped on the merged, applied MergeRequest")
	}
	if gm.Status.DeployedVersion != "v1.4.0" {
		t.Fatalf("deployedVersion = %q, want v1.4.0", gm.Status.DeployedVersion)
	}
	if len(f.closedIssues) != 1 {
		t.Fatalf("closed %d issues, want 1", len(f.closedIssues))
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageDelivered || got.Status.DeliveredAt == nil {
		t.Fatalf("stage = %q deliveredAt = %v, want delivered/set", got.Status.Stage, got.Status.DeliveredAt)
	}
}

// An apply that does NOT carry this MR's version leaves it undeployed: the Task
// stays in deploying and closes nothing.
func TestDeployingWaitsForTheApply(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mrA := mdDeployingMR(task, "tatara-operator", 7)
	mrB := mdDeployingMR(task, "tatara-cli", 9)
	iss := mdIssue(task, "tatara-operator", 41)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdRepo("tatara-cli"),
		mdHelmfileRepo(), task, mrA, mrB, iss)

	f := newFakeForge(t)
	rd := mdNewReader(f)
	rd.tags["tatara-operator"] = "v1.4.0"
	rd.tags["tatara-cli"] = "v0.9.1"
	rd.runs[helmfileRepoName] = mdSuccessfulApply("apply-sha")
	// The apply carries the operator's v1.4.0 but the cli is still pinned a version back.
	rd.pin["main"] = mdPin("tatara-operator", "1.3.0") + mdImagePin("tatara-cli", "0.9.0")
	rd.pin["apply-sha"] = mdPin("tatara-operator", "1.4.0") + mdImagePin("tatara-cli", "0.9.0")
	d := mdNewDriverWithReader(t, f, c, rd)

	res, err := d.ReconcileDeploying(context.Background(), mdProject(), task)
	if err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("an undeployed MR must keep the deploying poll alive")
	}
	if mdGetMR(t, c, mrA.Name).Status.DeployedAt == nil {
		t.Fatalf("the APPLIED MR must be stamped even while its sibling waits")
	}
	if mdGetMR(t, c, mrB.Name).Status.DeployedAt != nil {
		t.Fatalf("an MR whose pin was NOT applied must not be stamped deployed")
	}
	if len(f.closedIssues) != 0 {
		t.Fatalf("an Issue was closed while an owned MR was still undeployed")
	}
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageDeploying || got.Status.DeliveredAt != nil {
		t.Fatalf("stage = %q deliveredAt = %v, want deploying/nil", got.Status.Stage, got.Status.DeliveredAt)
	}
}

// A SUCCESSFUL apply that predates the merge proves nothing: it cannot have
// carried a commit that did not exist when it ran.
func TestDeployingIgnoresAnApplyThatPredatesTheMerge(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mr := mdDeployingMR(task, "tatara-operator", 7)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task, mr)

	f := newFakeForge(t)
	rd := mdNewReader(f)
	rd.tags["tatara-operator"] = "v1.4.0"
	stale := mdSuccessfulApply("apply-sha")
	stale.CreatedAt = mdMergedAt.Add(-time.Hour)
	rd.runs[helmfileRepoName] = stale
	rd.pin["main"] = mdPin("tatara-operator", "1.3.0")
	rd.pin["apply-sha"] = mdPin("tatara-operator", "1.4.0")
	d := mdNewDriverWithReader(t, f, c, rd)

	if _, err := d.ReconcileDeploying(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if mdGetMR(t, c, mr.Name).Status.DeployedAt != nil {
		t.Fatalf("an apply run that predates the merge must never stamp deployedAt")
	}
}

// A repo the helmfile deploys NOTHING for (docs, observability) has no cascade
// to observe. Waiting for a pin that will never move is a WEDGE: the merge IS
// the delivery.
func TestDeployingUnpinnedRepoDeliversOnMerge(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mr := mdDeployingMR(task, "tatara-documentation", 7)
	iss := mdIssue(task, "tatara-documentation", 41)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-documentation"), mdHelmfileRepo(), task, mr, iss)

	f := newFakeForge(t)
	rd := mdNewReader(f)
	rd.pin["main"] = mdPin("tatara-operator", "1.3.0") // no tatara-documentation pin anywhere
	d := mdNewDriverWithReader(t, f, c, rd)

	if _, err := d.ReconcileDeploying(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if mdGetMR(t, c, mr.Name).Status.DeployedAt == nil {
		t.Fatalf("a repo carrying no helmfile pin must deliver on merge, not wedge at deploying")
	}
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageDelivered {
		t.Fatalf("stage = %q, want delivered", got.Status.Stage)
	}
}

// mdPlainReader is an SCMReader that is NOT a DeployWatcher (the GitLab
// adapter): the cascade is unobservable from here.
type mdPlainReader struct{ scm.SCMReader }

// An unobservable cascade fails OPEN. The alternative is that every Task on a
// non-GitHub project wedges at deploying forever.
func TestDeployingWithoutADeployWatcherDeliversOnMerge(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mr := mdDeployingMR(task, "tatara-operator", 7)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task, mr)

	f := newFakeForge(t)
	d := mdNewDriverWithReader(t, f, c, &mdPlainReader{})

	if _, err := d.ReconcileDeploying(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if mdGetMR(t, c, mr.Name).Status.DeployedAt == nil {
		t.Fatalf("an unobservable cascade must fail OPEN: merge is delivery")
	}
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageDelivered {
		t.Fatalf("stage = %q, want delivered", got.Status.Stage)
	}
}

// A Task in deploying that owns an UNMERGED MR never delivers.
func TestDeployingNeverDeliversAnUnmergedMR(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mr := mdMR(task, "tatara-operator", 7) // open, not merged
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if _, err := d.ReconcileDeploying(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if mdGetMR(t, c, mr.Name).Status.DeployedAt != nil {
		t.Fatalf("an UNMERGED MR must never be stamped deployed")
	}
	if got := mdGetTask(t, c, "t1"); got.Status.Stage != tatarav1alpha1.StageDeploying {
		t.Fatalf("stage = %q, want deploying", got.Status.Stage)
	}
}

// The pin match is SEMVER, not a substring: a pin that has moved PAST our
// version still proves our version was applied (the pins only ever move
// forward), and that is what keeps a Task whose apply was superseded by the
// next one from wedging until its budget parks it.
func TestPinAtOrPastArtifactVersion(t *testing.T) {
	cases := []struct {
		name     string
		pin      string
		artifact string
		version  string
		want     bool
	}{
		{"exact", mdPin("tatara-operator", "1.4.0"), "tatara-operator", "v1.4.0", true},
		{"past", mdPin("tatara-operator", "1.4.1"), "tatara-operator", "v1.4.0", true},
		{"far past", mdPin("tatara-operator", "1.4.10"), "tatara-operator", "v1.4.1", true},
		{"behind", mdPin("tatara-operator", "1.3.9"), "tatara-operator", "v1.4.0", false},
		{"another artifact's pin", mdPin("tatara-cli", "1.4.0"), "tatara-operator", "v1.4.0", false},
		{"image pin", "        image: harbor.szymonrichert.pl/tatara/tatara-cli:v0.9.2\n", "tatara-cli", "v0.9.1", true},
		{"image pin behind", "        image: harbor.szymonrichert.pl/tatara/tatara-cli:v0.9.0\n", "tatara-cli", "v0.9.1", false},
		{"empty pin state", "", "tatara-operator", "v1.4.0", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pinAtOrPastArtifactVersion(tc.pin, tc.artifact, tc.version); got != tc.want {
				t.Fatalf("pinAtOrPastArtifactVersion(%q, %q) = %v, want %v", tc.artifact, tc.version, got, tc.want)
			}
		})
	}
}

// pinArtifactPresent decides "is this repo deployed by the helmfile AT ALL" -
// the predicate that separates a cascade worth waiting for from a wedge.
func TestPinArtifactPresent(t *testing.T) {
	pin := mdPin("tatara-operator", "1.4.0") +
		"        image: harbor.szymonrichert.pl/tatara/tatara-cli:v0.9.2\n"
	if !pinArtifactPresent(pin, "tatara-operator") {
		t.Fatalf("a chart pin in the release's own block is a presence signal")
	}
	if !pinArtifactPresent(pin, "tatara-cli") {
		t.Fatalf("an image pin carrying the artifact token is a presence signal")
	}
	if pinArtifactPresent(pin, "tatara-documentation") {
		t.Fatalf("tatara-documentation is not deployed by the helmfile")
	}
}
