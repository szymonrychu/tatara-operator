package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// staleGetClient reproduces the cache-lag exposure precisely: Get always
// returns a captured, hardcoded snapshot for the watched Task regardless of
// what the live backing store holds, while every other call (notably
// Status().Update) passes straight through to the embedded live client. This
// is exactly how a cached/informer client and the real API server relate in
// production: reads may lag an in-flight write, writes never do - there is
// only ever one true store.
type staleGetClient struct {
	client.Client
	stale *tatarav1alpha1.Task
}

func (s *staleGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if task, ok := obj.(*tatarav1alpha1.Task); ok && key.Name == s.stale.Name {
		s.stale.DeepCopyInto(task)
		return nil
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

// ---------------------------------------------------------------------------
// The cache-lag regression: a human comment on an issue drives
// AppendTaskEvent's Status().Update (a write straight to the API server),
// then driveCommentUnpark calls ApplyUnpark, whose Get used to run through the
// CACHED client. Microseconds is not long enough for the informer to have
// observed AppendTaskEvent's write, so the cached Get returned the Task
// WITHOUT the pendingEvent that was just appended, hasNonBotEvent read false,
// and the un-park was silently declined - live evidence: 2 queued events, 0
// un-parks, 0 errors, over 24h.
// ---------------------------------------------------------------------------

func TestApplyUnpark_UsesLiveReadNotCachedGet(t *testing.T) {
	proj := wfProject()
	// The CACHED view: parked(awaiting-human), no pendingEvent yet - what the
	// informer still shows.
	stale := wfParkedTask("t-cachelag", "review", stage.ReasonAwaitingHuman)
	// The LIVE view: the same Task on the real API server, AFTER the human
	// comment's AppendTaskEvent write landed.
	live := stale.DeepCopy()
	live.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
	}}

	liveClient := newMirrorClient(t, proj, live)
	cached := &staleGetClient{Client: liveClient, stale: stale.DeepCopy()}

	target, decline, err := ApplyUnpark(context.Background(), cached, liveClient, proj, stale, 0, 6, false, time.Now())
	if err != nil {
		t.Fatalf("ApplyUnpark: %v", err)
	}
	if decline != DeclineNone {
		t.Fatalf("decline = %q, want DeclineNone on a successful un-park", decline)
	}
	if target != tatarav1alpha1.StageReviewing {
		t.Fatalf("ApplyUnpark declined (target=%q) despite a fresh non-bot pendingEvent visible on the live read; "+
			"the cached Get's staleness was not bypassed", target)
	}
	got := mdGetTask(t, liveClient, stale.Name)
	if got.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("persisted stage = %s, want reviewing", got.Status.Stage)
	}
}

// A cache that is NOT stale (agrees with the live store) must still decline
// when there really is no non-bot event: proves the fix does not just make
// ApplyUnpark unconditionally optimistic.
func TestApplyUnpark_BotOnlyEventStillDeclines(t *testing.T) {
	proj := wfProject()
	task := wfParkedTask("t-botonly", "review", stage.ReasonAwaitingHuman)
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "tatara-bot", Body: "parked",
	}}
	c := newMirrorClient(t, proj, task)

	target, decline, err := ApplyUnpark(context.Background(), c, c, proj, task, 0, 6, false, time.Now())
	if err != nil {
		t.Fatalf("ApplyUnpark: %v", err)
	}
	if target != "" {
		t.Fatalf("a bot-only pendingEvent must never un-park; got target=%q", target)
	}
	if decline != DeclineRule {
		t.Fatalf("decline = %q, want DeclineRule for a bot-only-event non-error refusal", decline)
	}
}

// The Stage/StageReason guard (unpark.go:80-84) must still short-circuit when
// a DIFFERENT park is in play by the time the live read lands: the caller
// believes this Task is parked(awaiting-human), but another writer already
// re-parked it under a different F.6 reason (merge-timeout) - "a different
// park is in play" per the guard's own comment. The live read must not
// resurrect a decision keyed on the caller's now-stale reason.
func TestApplyUnpark_StageReasonGuardStillShortCircuitsOnLiveRead(t *testing.T) {
	proj := wfProject()
	caller := wfParkedTask("t-guard", "implement", stage.ReasonAwaitingHuman)
	caller.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
	}}
	mr := wfMR("mr-guard", "open", caller)
	caller.Status.MRRefs = []string{mr.Name}

	// LIVE: re-parked under merge-timeout by a different writer/pass before this
	// caller's ApplyUnpark runs. merge-timeout's own re-entry rule needs no
	// comment at all, so if the guard did not fire this would re-enter merging
	// regardless of hasNonBotEvent - proving the guard, not the reason dispatch,
	// is what stops it.
	live := caller.DeepCopy()
	live.Status.StageReason = stage.ReasonMergeTimeout

	c := newMirrorClient(t, proj, live, mr)

	target, decline, err := ApplyUnpark(context.Background(), c, c, proj, caller, 0, 6, false, time.Now())
	if err != nil {
		t.Fatalf("ApplyUnpark: %v", err)
	}
	if target != "" {
		t.Fatalf("guard must refuse an unpark whose live stageReason no longer matches the caller's park; got target=%q", target)
	}
	if decline != DeclineGuard {
		t.Fatalf("decline = %q, want DeclineGuard when the live stageReason drifted", decline)
	}
	got := mdGetTask(t, c, caller.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonMergeTimeout {
		t.Fatalf("guard must leave the live object untouched; got stage=%s(%s)", got.Status.Stage, got.Status.StageReason)
	}
}

// #406: the real no-outcome escalation hole. An incident Task's pod-liveness
// respawn loop turns 0 into investigating (never implementing), so once
// PodRecreations hits the budget it parks at no-outcome with
// ParkedFromStage=investigating. Before the Fix 5a guard, driveUnparks'
// unconditional no-outcome re-entry drove it straight into implementing
// anyway; it must now stay parked, since no agent turn ever ran.
func TestDriveUnparks_IncidentNoOutcomeStaysParked(t *testing.T) {
	task := wfParkedTask("t-incident-no-outcome", "incident", stage.ReasonNoOutcome)
	task.Status.ParkedFromStage = tatarav1alpha1.StageInvestigating
	task.Status.Stats.PodRecreations = 3
	c := newMirrorClient(t, task)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
	if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("incident parked(no-outcome) from investigating re-entered %s, want parked", got.Status.Stage)
	}
	if got.Status.StageReason != stage.ReasonNoOutcome {
		t.Fatalf("stageReason = %q, want unchanged no-outcome", got.Status.StageReason)
	}
}
