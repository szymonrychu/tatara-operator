package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
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
	if got.Status.State != tatarav1alpha1.StateDone || got.Status.DeliveredAt == nil {
		t.Fatalf("stage = %q deliveredAt = %v, want delivered/set", got.Status.State, got.Status.DeliveredAt)
	}
}

// An apply that does NOT carry this MR's version leaves it undeployed: the Task
// stays in deploying and closes nothing.
func TestDeployingWaitsForTheApply(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
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
	// The fan-out HAS landed both pins on main (so the #512 fan-out bound is not
	// what is in play here); the apply carries the operator's v1.4.0 but ran
	// before the cli bump, so the cli is still applied a version back.
	rd.pin["main"] = mdPin("tatara-operator", "1.4.0") + mdImagePin("tatara-cli", "0.9.1")
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
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateDeployed || got.Status.DeliveredAt != nil {
		t.Fatalf("stage = %q deliveredAt = %v, want deploying/nil", got.Status.State, got.Status.DeliveredAt)
	}
}

// A SUCCESSFUL apply that predates the merge proves nothing: it cannot have
// carried a commit that did not exist when it ran.
func TestDeployingIgnoresAnApplyThatPredatesTheMerge(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
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
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
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
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateDone {
		t.Fatalf("stage = %q, want delivered", got.Status.State)
	}
}

// mdPlainReader is an SCMReader that is NOT a DeployWatcher (the GitLab
// adapter): the cascade is unobservable from here.
type mdPlainReader struct{ scm.SCMReader }

// An unobservable cascade fails OPEN. The alternative is that every Task on a
// non-GitHub project wedges at deploying forever.
func TestDeployingWithoutADeployWatcherDeliversOnMerge(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
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
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateDone {
		t.Fatalf("stage = %q, want delivered", got.Status.State)
	}
}

// A Task in deploying that owns an UNMERGED MR never delivers.
func TestDeployingNeverDeliversAnUnmergedMR(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
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
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateDeployed {
		t.Fatalf("stage = %q, want deploying", got.Status.State)
	}
}

// ---------------------------------------------------------------------------
// DEPLOYING MUST NARRATE ITS WAIT (issue #513).
//
// merging logs merge_waiting on every poll pass (stallMerge). deploying logged
// NOTHING, so a wedged deploy was silent for its whole 2h budget and the first
// evidence anyone got was parked(deploy-timeout), 2h07m after the fact - with no
// record of WHICH owned MR never got its deployedAt.
// ---------------------------------------------------------------------------

// kvLogEntry is one recorded log line WITH its structured fields.
// recordingSink (unpark_decline_test.go) keeps only the message; these
// assertions are about the FIELDS.
type kvLogEntry struct {
	msg string
	kv  map[string]any
}

func (e kvLogEntry) field(key string) any { return e.kv[key] }

type kvSink struct{ entries *[]kvLogEntry }

func (s kvSink) Init(logr.RuntimeInfo)        {}
func (s kvSink) Enabled(int) bool             { return true }
func (s kvSink) WithName(string) logr.LogSink { return s }

func (s kvSink) WithValues(kv ...any) logr.LogSink { return s }

func (s kvSink) Info(_ int, msg string, kv ...any) {
	*s.entries = append(*s.entries, kvLogEntry{msg: msg, kv: kvPairs(kv)})
}

func (s kvSink) Error(_ error, msg string, kv ...any) {
	*s.entries = append(*s.entries, kvLogEntry{msg: msg, kv: kvPairs(kv)})
}

func kvPairs(kv []any) map[string]any {
	out := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok {
			out[k] = kv[i+1]
		}
	}
	return out
}

func kvLoggingCtx() (context.Context, *[]kvLogEntry) {
	var entries []kvLogEntry
	return log.IntoContext(context.Background(), logr.New(kvSink{entries: &entries})), &entries
}

// oneLoggedAction returns the single entry carrying action=want.
func oneLoggedAction(t *testing.T, entries []kvLogEntry, want string) kvLogEntry {
	t.Helper()
	var found []kvLogEntry
	for _, e := range entries {
		if e.field("action") == want {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		t.Fatalf("logged action=%q %d times, want exactly 1; entries: %+v", want, len(found), entries)
	}
	return found[0]
}

// An owned MR still lacking deployedAt must be NAMED on every poll pass, AND the
// line must say WHY it is still pending: the count alone does not distinguish an
// apply that never went green (issue #513) from a pin that is merely behind.
func TestDeployingLogsWaitingWithThePendingMRs(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
	task.Status.StateEnteredAt = &mdMergedAt // 11:00; mdNewDriver's clock is 12:00
	// A deploy-timeout un-park already folded 2h into the carry. stalled_seconds
	// must ADD it, not report the bare hour since this entry - without a non-zero
	// carry here the assertion below passes identically for an UN-adjusted reading.
	task.Status.StageElapsedCarrySeconds = 7200
	mrA := mdDeployingMR(task, "tatara-operator", 7)
	mrB := mdDeployingMR(task, "tatara-cli", 9)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdRepo("tatara-cli"),
		mdHelmfileRepo(), task, mrA, mrB)

	f := newFakeForge(t)
	rd := mdNewReader(f)
	rd.tags["tatara-operator"] = "v1.4.0"
	rd.tags["tatara-cli"] = "v0.9.1"
	rd.runs[helmfileRepoName] = mdSuccessfulApply("apply-sha")
	// Both pins ARE on main (the fan-out ran); the apply carries the operator's
	// v1.4.0 and predates the cli bump, so only the cli is still undeployed.
	rd.pin["main"] = mdPin("tatara-operator", "1.4.0") + mdImagePin("tatara-cli", "0.9.1")
	rd.pin["apply-sha"] = mdPin("tatara-operator", "1.4.0") + mdImagePin("tatara-cli", "0.9.0")
	d := mdNewDriverWithReader(t, f, c, rd)

	ctx, entries := kvLoggingCtx()
	res, err := d.ReconcileDeploying(ctx, mdProject(), task)
	if err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("an undeployed MR must keep the deploying poll alive")
	}

	e := oneLoggedAction(t, *entries, "deploy_waiting")
	if got := e.field("pending"); got != 1 {
		t.Fatalf("pending = %v, want 1 (only tatara-cli is undeployed)", got)
	}
	const wantRef = "tatara-cli#9 (the applied helmfile pin is behind v0.9.1)"
	if got, ok := e.field("pending_mrs").(string); !ok || got != wantRef {
		t.Fatalf("pending_mrs = %v, want %q: a wait that does not say WHY cannot be triaged",
			e.field("pending_mrs"), wantRef)
	}
	if got := e.field("stalled_seconds"); got != 10800.0 {
		t.Fatalf("stalled_seconds = %v, want 10800 (1h since stageEnteredAt PLUS the 2h carry)", got)
	}
	if got := e.field("resource_id"); got != "t1" {
		t.Fatalf("resource_id = %v, want t1", got)
	}
}

// THIS IS ISSUE #513's OWN WAIT. The ARC runner that runs tatara-helmfile's
// apply.yaml ran out of disk, so the apply never went green and no pin ever
// moved. That branch of resolveDeployed returned a bare false and logged nothing
// at all, which is why 2h07m of deploying produced no evidence whatsoever: the
// pending count says an MR is not deployed, it never said the apply is the thing
// that is broken.
func TestDeployingLogsWaitingWhenNoApplyRunWentGreen(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
	task.Status.StateEnteredAt = &mdMergedAt
	mr := mdDeployingMR(task, "tatara-operator", 7)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task, mr)

	f := newFakeForge(t)
	rd := mdNewReader(f)
	rd.tags["tatara-operator"] = "v1.4.0"
	// The pin IS on main - the fan-out ran - so this is the APPLY being broken,
	// not the #512 fan-out stall, and the poll is the right response.
	rd.pin["main"] = mdPin("tatara-operator", "1.4.0")
	// rd.runs is deliberately EMPTY: no completed successful apply run exists.
	d := mdNewDriverWithReader(t, f, c, rd)

	ctx, entries := kvLoggingCtx()
	if _, err := d.ReconcileDeploying(ctx, mdProject(), task); err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}

	e := oneLoggedAction(t, *entries, "deploy_waiting")
	refs, _ := e.field("pending_mrs").(string)
	if !strings.HasPrefix(refs, "tatara-operator#7 (") || !strings.Contains(refs, "apply run on main yet") {
		t.Fatalf("pending_mrs = %q, want tatara-operator#7 named WITH \"no ... apply run on main yet\": "+
			"this is the branch that was silent for issue #513's whole 2h07m", refs)
	}
}

// THE EMPTY SET (C.4) is the silent wedge that ends at parked(deploy-timeout)
// with no explanation at all: it must say that it owns none.
func TestDeployingLogsWaitingWhenItOwnsNoMergeRequests(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
	task.Status.StateEnteredAt = &mdMergedAt
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task)

	d := mdNewDriver(t, newFakeForge(t), c)
	ctx, entries := kvLoggingCtx()
	res, err := d.ReconcileDeploying(ctx, mdProject(), task)
	if err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("the empty set must keep the deploying poll alive")
	}

	e := oneLoggedAction(t, *entries, "deploy_waiting")
	if got := e.field("pending"); got != 0 {
		t.Fatalf("pending = %v, want 0", got)
	}
	reason, _ := e.field("reason").(string)
	if !strings.Contains(reason, "no merge requests") {
		t.Fatalf("reason = %q, want it to say the Task owns no merge requests", reason)
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
