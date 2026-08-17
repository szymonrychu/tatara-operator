package webhook

import (
	"context"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// WS3 webhook reactions. EVERY function here is one of: an idempotent MIRROR
// write (Status upsert through the A.7 byte guard), a PENDING-EVENT append, or a
// POKE that a leader-only reconcile consumes. NONE performs a stage Enter, an
// ownerRef drop, a pod delete, or an unpark from this HTTP goroutine - those are
// leader-only (the IssueReconciler / project-reconcile drivers act on the
// resourceVersion bump these writes cause). This is the #353 / F6-1 boundary.

// handleIssueClosed refreshes the mirror Issue's Status.State to "closed" (WS3-I3
// signal path step 1). The leader-only IssueReconciler observes the closed state
// and drives ApplyIssueClosedStop. A missing mirror CR is a no-op (nothing to
// stop). Bot-authored closes are ignored: the operator's own C.4 deploying-close
// must not look like a human stop (and deploying is excluded leader-side anyway).
func (s *Server) handleIssueClosed(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	if isBotActor(&proj, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if s.stampIssueState(ctx, &proj, repo, ev.Number, "closed") {
		s.log.InfoContext(ctx, "issues: mirrored close; leader stops the task if live",
			"action", "issue_closed_mirror", "project", proj.Name, "repository", repo.Name, "number", ev.Number)
	}
	// RESUME TRIGGER (maintainer close). Same signal as the comment trigger:
	// a maintainer disposing of an item is engagement.
	if tatarav1.IsMaintainer(&proj, repo, ev.ActorLogin) {
		if rerr := controller.StampBrainstormResume(ctx, s.cfg.Client, s.cfg.Namespace, proj.Name,
			controller.ResumeTriggerMaintainerClose); rerr != nil {
			s.log.ErrorContext(ctx, "brainstorm resume failed", "project", proj.Name,
				"number", ev.Number, "error", rerr)
		}
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// handleIssueEdited is WS3-I2. On an owned/tracked issue update it (a) refreshes
// the mirror Body/Title (safe MIRROR write - the agent's scm_read is served from
// the mirror), and (b) if the body or title actually CHANGED, appends an
// issue_edited pending event on the owning Task. It does NOT drive the unpark:
// the leader-only driveUnparks/Task reconcile consumes the fresh pending event.
//
// AN EDIT DOES RE-ENGAGE A PARKED TASK, INCLUDING identity-unverified. This
// comment used to claim the opposite ("identity-unverified needs a recorded
// approval, not an edit, so it stays parked"), and that stopped being true when
// the durable approval verdict was deleted. The actual behaviour: the bot-actor
// early return above means any surviving event is HUMAN-authored;
// stage.hasNonBotEvent tests only Author != botLogin and does NOT filter on
// Kind, so an issue_edited event re-engages exactly like a comment does; and the
// identity-unverified arm's only remaining question after that is conversing
// room. So a human editing an issue body un-parks the Task to CONVERSING.
//
// That is benign, and the reason it is benign is the part worth carrying: for
// EVERY comment-driven re-entry reason, re-engagement is a RESPONSIVENESS
// decision, not an authorization one. identity-unverified re-enters conversing
// ONLY - never implementing - and the approval decision happens later and
// elsewhere, at restapi.verifyApprovalScope, against a live pod's
// submit_outcome. No event class reaching this file can approve anything.
func (s *Server) handleIssueEdited(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	if isBotActor(&proj, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if !tatarav1.IsAllowedReporter(&proj, repo, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	iss := &tatarav1.Issue{}
	key := objKey(s.cfg.Namespace, tatarav1.IssueName(repo.Name, ev.Number))
	if err := s.cfg.Client.Get(ctx, key, iss); err != nil {
		// No mirror CR yet: nothing tracked to refresh. The sweep converges it.
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	sp := s.cfg.SpillerFor(&proj)
	changed := false
	if sp != nil {
		if ferr := objbudget.FitIssue(ctx, s.cfg.Client, sp, key,
			func(i *tatarav1.Issue) {
				// Diff and write in the SAME fitForWrite transaction: keying the
				// reaction on the actual mirror DIFF (not the action string) is what
				// gives GitHub/GitLab parity across their divergent action vocabularies.
				body := tatarav1.TruncateUTF8(ev.Body, tatarav1.IssueBodyMaxBytes)
				changed = i.Status.Body != body || i.Status.Title != ev.Title
				i.Status.Body = body
				i.Status.Title = ev.Title
			}); ferr != nil {
			// changed never flips true (the closure that sets it never ran), so the
			// !changed check below returns early and the owning Task never gets its
			// issue_edited TaskEvent - this drop silently suppresses the derived
			// unpark-worthy event, not just the mirror body/title.
			obs.MirrorWriteDroppedTotal.WithLabelValues(proj.Name, "Issue", "issue_body_title").Inc()
			s.log.WarnContext(ctx, "issues: mirror body/title refresh failed; issue_edited event also suppressed (changed stays false)",
				"error", ferr, "project", proj.Name, "issue_ref", ev.IssueRef)
		}
	} else {
		// Same consequence as the FitIssue error above: no Spiller means the mirror
		// is never touched, changed stays false, and the issue_edited TaskEvent is
		// silently suppressed along with the body/title refresh.
		obs.MirrorWriteDroppedTotal.WithLabelValues(proj.Name, "Issue", "issue_body_title").Inc()
		s.log.WarnContext(ctx, "issues: no Spiller configured; mirror body/title refresh skipped, issue_edited event also suppressed",
			"project", proj.Name, "issue_ref", ev.IssueRef)
	}

	if !changed {
		// A label-only update (or a no-op edit): mirror refreshed, NO event. This is
		// what keeps a GitLab labeled/unlabeled edit from firing a spurious unpark.
		s.accept(w, provider, ev.Kind, ev.Action, "accepted")
		return
	}

	ownerName, owned := own.ControllerOwner(iss)
	if owned {
		task := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, ownerName), task); err == nil {
			taskEv := tatarav1.TaskEvent{
				At:     metav1.Now(),
				Kind:   "issue_edited",
				Repo:   repo.Name,
				Number: ev.Number,
				Author: ev.ActorLogin,
				Body:   ev.Title, // the goal snapshot moved; the new title is the useful summary
			}
			if err := controller.AppendTaskEvent(ctx, s.cfg.Client, task, taskEv); err != nil {
				s.log.ErrorContext(ctx, "issues: append issue_edited event failed", "error", err, "task", task.Name)
			} else {
				s.log.InfoContext(ctx, "issues: mirrored edit and queued issue_edited event",
					"action", "issue_edited", "project", proj.Name, "repository", repo.Name, "number", ev.Number)
			}
		} else if !apierrors.IsNotFound(err) {
			s.log.ErrorContext(ctx, "issues: get owning task for edit failed", "error", err, "task", ownerName)
		}
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// maybeTriggerLabelMint mints a Task when a human adds the project's configured
// trigger label to an orphan issue (reactivity parity with issues.opened). It is
// a best-effort SIDE EFFECT - it never writes the HTTP response - so the caller
// still runs the I2 edit fold. Guards: the changed label must EQUAL the trigger
// label, the actor must be non-bot and an allowed reporter, the label must NOT be
// one of the operator's own approved/declined lifecycle-projection labels (else a
// projection write self-triggers a mint), and the issue must still be an orphan.
func (s *Server) maybeTriggerLabelMint(ctx context.Context, provider string, proj *tatarav1.Project, ev scm.WebhookEvent) {
	trigger := proj.Spec.TriggerLabel
	if trigger == "" || ev.ChangedLabel != trigger {
		return
	}
	if isBotActor(proj, ev.ActorLogin) {
		return
	}
	_, approved, _, declined := controller.LifecycleLabels(proj.Spec.Scm)
	if ev.ChangedLabel == approved || ev.ChangedLabel == declined {
		return // a lifecycle-projection label must never self-trigger a mint
	}
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil || repo == nil || ev.Number <= 0 {
		return
	}
	if !tatarav1.IsAllowedReporter(proj, repo, ev.ActorLogin) {
		return
	}
	if !s.commentIsOrphan(ctx, repo, ev) {
		return // already owned: the sweep/existing Task drives it
	}
	item := controller.ForgeItem{Issue: scm.Issue{
		Number: ev.Number, State: "open", Author: ev.ActorLogin,
		Title: ev.Title, Body: ev.Body, Labels: ev.Labels, URL: ev.URL}}
	_, outcome, merr := s.minter().MintForItem(ctx, proj, repo, item, true, s.cfg.SpillerFor(proj))
	if merr != nil {
		s.log.ErrorContext(ctx, "issues: trigger-label mint failed", "error", merr,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		return
	}
	if outcome == controller.MintTombstoneDeleted {
		// Best-effort backstop: there is no response to fail, and the sweep's
		// 30s tombstone requeue re-drives the mint that is still owed.
		s.log.InfoContext(ctx, "issues: trigger-label mint deleted a stale terminal task; the sweep re-drives it",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		return
	}
	if outcome == controller.MintCreated {
		if cerr := controller.ClearWebhookOriginated(ctx, s.cfg.Client, s.reader(), s.cfg.Namespace, tatarav1.IssueName(repo.Name, ev.Number)); cerr != nil {
			s.log.ErrorContext(ctx, "issues: clear webhook-originated marker failed", "error", cerr, "issue_ref", ev.IssueRef)
		}
		s.log.InfoContext(ctx, "issues: trigger label minted clarify task",
			"action", "issue_trigger_label_mint", "project", proj.Name, "repository", repo.Name, "number", ev.Number, "label", ev.ChangedLabel)
	}
}

// handleMRSynchronize is WS3-M1: a human pushed to the agent's branch mid-review.
// It refreshes ONLY the mirror head from the event (safe MIRROR write, no forge
// call) so the reviewing agent's next scm_read(kind=mr) sees the new head. NO
// review restart - correctness is guaranteed at merge time by the head-pinned
// merge (ErrHeadMoved) and the merging->reviewing head-move bounce.
func (s *Server) handleMRSynchronize(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 || ev.HeadSHA == "" {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	// A verified bot-push webhook advances the bot-head cursor immediately, so a
	// push webhook that races ahead of the implement-outcome record still reads
	// as attributable (no false external-push flip). A non-bot pusher advances
	// only the HeadSHA mirror, leaving LastBotHeadSHA stale - ReconcileOwnership
	// sees the drift and flips.
	//
	// isBotActor is a string-equality check against Scm.BotLogin. On the
	// AGENT's own merge requests it is the FAST PATH only, not the
	// authoritative signal: the identity gate lives at outcome accept
	// (/outcome's record_bot_head, restapi/outcome.go), which re-stamps
	// LastBotHeadSHA from a LIVE forge read (GetPRHead) rather than trusting the
	// webhook's reported actor. A spoofed or mismatched BotLogin/push-identity
	// there can only cause a spurious flip whose window is bounded by the next
	// accept - it self-corrects, it never compounds.
	//
	// THAT BACKSTOP IS SUBMITTED-ONLY, and the paragraph above used to imply it
	// covered every accept. record_bot_head runs after the `submitted` arm's
	// commit; a turn that ends `declined` returns well before it and re-stamps
	// nothing. On a kind=takeover Task that is now more than a stalled merge
	// request: ReconcileOwnership's parkOwnerTask UPGRADES a takeover's
	// implement-declined park to ownership-lost on the flip (#604), so a spurious
	// flip against tatara's own sha rewrites a TERMINAL rather than hitting the
	// already-parked early return. Same accepted staleness, a third place where
	// nothing corrects it. Argued in full, with the remedies that do not work and
	// the one that would, at controller/ownership.go's parkOwnerTask.
	//
	// THAT BACKSTOP DOES NOT EXIST ON AN ADOPTED MERGE REQUEST, and the comment
	// used to imply it did. record_bot_head runs only on the implement/upgrade
	// `submitted` path, and the COMMON adopted path - approved at first review -
	// never runs an upgrade turn at all, so this webhook is the ONLY thing that
	// re-anchors the baseline when the engine rebases its own branch. One
	// dropped `synchronize` delivery therefore flips the merge request to
	// external permanently, and since that flip now stamps
	// adoptedPushReasonPrefix the merge request becomes human-merged-only for
	// good.
	//
	// THAT IS ACCEPTED, DELIBERATELY, AND HERE IS THE ARGUMENT. Re-anchoring
	// from the SWEEP would need the drifted head ATTRIBUTED to the engine, and
	// there is no portable authenticated signal for that: GitLab's commit API
	// returns author_name/author_email (git metadata, which this repo refuses to
	// key decisions on - see UpgradeEngineLogins) and only GitHub exposes a
	// mapped committer login. The failure mode after the flip is FAIL-SAFE: a
	// dependency bump stops, nobody merges anything unattributed, and a human
	// merges it or the engine opens a fresh merge request on its next run.
	// Before adoption existed the same dropped delivery caused the OPPOSITE and
	// worse outcome - the operator merged the unattributed commits on the next
	// approve. Trading a stalled bump for that is the right direction, and the
	// stall is visible: the flip logs ownership_flip with adopted=true and
	// increments operator_ownership_flip_total.
	//
	// isUpgradeEngineActor, not isBotActor: a dependency-upgrade engine
	// rebasing its OWN branch is not a human taking the branch back, and parking
	// the adopted upgrade Task ownership-lost for it would stall a merge request
	// nobody touched. Everything above still holds - this is still the FAST PATH
	// and /outcome's live GetPRHead read is still the authoritative stamp - and
	// so does everything about a non-attributable pusher: a human, an unknown
	// login and an empty login all leave LastBotHeadSHA stale, and
	// ReconcileOwnership flips.
	bot := isUpgradeEngineActor(&proj, ev.ActorLogin)
	if s.stampMRHead(ctx, &proj, repo, ev.Number, ev.HeadSHA, bot) {
		s.log.InfoContext(ctx, "mr: mirrored new head on synchronize; no review restart",
			"action", "mr_synchronize_mirror", "project", proj.Name, "repository", repo.Name,
			"number", ev.Number, "head_sha", ev.HeadSHA, "bot_push", bot)
	}
	s.refreshQueuedAdoption(ctx, &proj, repo, ev)
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// queuedAdoption returns the STILL-QUEUED adoption event for (proj, repo,
// number), or nil when there is none or it has already been admitted.
//
// It is a single Get on the DETERMINISTIC name, not a list: QueuedEventName is a
// pure function of (project, dedupKey) and AdoptUpgradeDedupKey is a pure
// function of (repository CR name, number), both of which this handler already
// holds. Every merge request in the platform receives synchronize and closed
// deliveries and almost none has a queued adoption, so the miss path has to be
// one cheap read.
//
// THE READ IS UNCACHED (s.reader(), the manager's APIReader), for the same
// reason the review guards are and the dispatcher's own mintedTask is (fix
// M28): the QueuedEvent this looks for was created SECONDS earlier by
// handleMROpened, possibly on a DIFFERENT one of the three non-leader-elected
// webhook replicas. A merge/close delivery arriving before that Create has
// propagated into this replica's informer would find nothing through the cached
// client, dropQueuedAdoption would no-op silently, and the admit-time backstop
// cannot cover it - a FIRST adoption has no MergeRequest mirror CR at all, so
// admitAdoptedUpgrade's merged/closed check sees nothing either (its own doc
// comment says so). The cost of that miss is an agent pod burned reviewing an
// already-merged merge request, which is exactly what design D4 exists to
// prevent.
func (s *Server) queuedAdoption(ctx context.Context, proj *tatarav1.Project,
	repo *tatarav1.Repository, number int) *tatarav1.QueuedEvent {

	if repo == nil || number <= 0 {
		return nil
	}
	name := queue.QueuedEventName(proj.Name, queue.AdoptUpgradeDedupKey(repo.Name, number))
	var qe tatarav1.QueuedEvent
	if err := s.reader().Get(ctx, objKey(s.cfg.Namespace, name), &qe); err != nil {
		return nil
	}
	if !stillQueuedAdoption(&qe) {
		return nil
	}
	return &qe
}

// stillQueuedAdoption is queuedAdoption's own predicate, factored out so
// refreshQueuedAdoption's retry loop - which must re-Get inside the
// RetryOnConflict closure rather than reuse an outer read - can apply the
// IDENTICAL rule to its own fresh read.
//
// The empty state is a QueuedEvent whose post-Create status update was lost,
// which is still effectively Queued (isQueued's own rule). Anything else -
// Admitted, or any other non-empty state - is spent work this belongs to
// neither refresh nor drop.
func stillQueuedAdoption(qe *tatarav1.QueuedEvent) bool {
	if qe.Spec.Payload.AdoptedUpgrade == nil {
		return false
	}
	return qe.Status.State == "" || qe.Status.State == tatarav1.QueueStateQueued
}

// refreshQueuedAdoption re-snapshots a still-queued adoption from a synchronize
// delivery. Renovate FORCE-PUSHES each successive bump onto the same branch,
// keeping the same number and the same merge request, so an event that waits
// behind a full pool is pointing at a tree that no longer exists - and
// AdoptUpgradeMR clause (g) binds its refusal marker to a head SHA, so minting
// against a stale one rules on the wrong tree.
//
// GET-MUTATE-UPDATE UNDER retry.RetryOnConflict, re-Getting fresh INSIDE the
// closure THROUGH THE UNCACHED READER, mirroring fitMR/objbudget.FitIssue in
// this same file and patchQueuedEventStatus in queue_controller.go. The reader
// is what makes the retry mean anything: a re-Get through the informer cache
// hands back the SAME stale resourceVersion the losing Update was built on, so
// every retry re-submits the identical losing write and the loop just burns its
// four attempts. It is also what lets this handler see an event a sibling
// replica created moments ago at all (see queuedAdoption). Production runs 3 webhook
// replicas with no leader election on this path, so two near-simultaneous
// synchronize deliveries - literally "Renovate force-pushes each successive
// bump", the case this whole handler exists for - can land on different
// replicas and race this same write. A LOST update here is not cosmetic:
// admitAdoptedUpgrade's refusal check (AdoptUpgradeMR clause g) is keyed on the
// stored HeadSHA, so losing the newer one makes a human's refusal of it
// invisible at admission and the merge corridor never sees it - the Task has
// already minted by then. A plain single Get-then-Update, as this used to be,
// drops the newer write silently on every conflict instead of retrying it.
func (s *Server) refreshQueuedAdoption(ctx context.Context, proj *tatarav1.Project,
	repo *tatarav1.Repository, ev scm.WebhookEvent) {

	if repo == nil || ev.Number <= 0 {
		return
	}
	key := objKey(s.cfg.Namespace, queue.QueuedEventName(proj.Name, queue.AdoptUpgradeDedupKey(repo.Name, ev.Number)))
	refreshed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		refreshed = false
		var qe tatarav1.QueuedEvent
		if gerr := s.reader().Get(ctx, key, &qe); gerr != nil {
			if apierrors.IsNotFound(gerr) {
				return nil // no queued adoption at all: the normal case
			}
			return gerr
		}
		if !stillQueuedAdoption(&qe) {
			return nil // admitted, or already dropped, between deliveries
		}
		qe.Spec.Payload.AdoptedUpgrade.HeadSHA = ev.HeadSHA
		if ev.Title != "" {
			qe.Spec.Payload.AdoptedUpgrade.Title = ev.Title
		}
		if ev.Body != "" {
			// CLAMPED, exactly as the enqueue funnel clamps it
			// (controller.AdoptedUpgradeRefFromPR). This is the SECOND writer of
			// this bounded field, and an unclamped write here would 422 the Update
			// for precisely the grouped bumps whose per-dependency release notes are
			// longest - ending design D4's freshness guarantee for them silently,
			// since this handler's failure policy is best-effort.
			qe.Spec.Payload.AdoptedUpgrade.Body = controller.ClampAdoptedUpgradeBody(ev.Body)
		}
		if uerr := s.cfg.Client.Update(ctx, &qe); uerr != nil {
			if apierrors.IsNotFound(uerr) {
				// BENIGN RACE, not a failure: a concurrent dropQueuedAdoption (a
				// synchronize immediately followed by a close/merge, on either the
				// same or a different replica) deleted the event between this Get
				// and this Update. There is nothing left to refresh.
				return nil
			}
			return uerr
		}
		refreshed = true
		return nil
	})
	if err != nil {
		// BEST EFFORT, and unlike the enqueue this one really is: the event is
		// still queued, the dispatcher still adopts, and the worst case after every
		// retry is exhausted is a mint at the previous head - which the merge
		// corridor's own head pin catches for a review-path Task, though NOT for
		// the adoption-refusal race this handler exists to close (see the doc
		// comment above).
		s.log.ErrorContext(ctx, "mr: refresh of a queued adoption failed after retry; the stale snapshot still mints",
			"action", "mr_adopt_refresh_failed", "error", err,
			"project", proj.Name, "repository", repo.Name, "number", ev.Number)
		return
	}
	if refreshed {
		s.log.InfoContext(ctx, "mr: refreshed a queued dependency-upgrade adoption",
			"action", "mr_adopt_refreshed", "resource_id", key.Name, "project", proj.Name,
			"repository", repo.Name, "number", ev.Number, "head_sha", ev.HeadSHA)
	}
}

// dropQueuedAdoption deletes a still-queued adoption for a merge request that
// merged or closed while it waited. Admitting it would spend an agent pod on a
// merge request there is nothing left to review.
//
// Delete needs no RetryOnConflict: unlike Update, a plain Delete carries no
// resourceVersion precondition, so it cannot itself conflict - it either
// succeeds or, on a race with another deleter, returns NotFound, which is
// already handled below as the benign outcome it is.
func (s *Server) dropQueuedAdoption(ctx context.Context, proj *tatarav1.Project,
	repo *tatarav1.Repository, number int, reason string) {

	qe := s.queuedAdoption(ctx, proj, repo, number)
	if qe == nil {
		return
	}
	if err := s.cfg.Client.Delete(ctx, qe); err != nil && !apierrors.IsNotFound(err) {
		// A FAILED DELETE HERE IS NOT RETRIED BY THE FORGE: handleMRClosed still
		// answers 202 regardless of this error (the same best-effort policy as
		// every other mirror write in this file), so the event stays Queued with
		// no redelivery to try this again.
		//
		// admitAdoptedUpgrade (queue_controller.go) IS a real second gate, and it
		// costs nothing new: it already fetches this same MergeRequest mirror CR
		// for clause (e)'s live-owner check, and now refuses to mint when that
		// mirror's State is merged or closed, dropping the event there instead
		// under this SAME reason. But that backstop is NOT universal - fitMR
		// (this file) only UPDATES an existing MergeRequest mirror, it never
		// CREATES one, so an adoption that has never been admitted before has no
		// mirror CR yet and admitAdoptedUpgrade cannot see this merge request's
		// real state at all. For that common case, a Delete failure here has no
		// second gate before mint.
		s.log.ErrorContext(ctx, "mr: dropping a queued adoption failed; only a merge request with an existing mirror CR gets a second gate at admit",
			"action", "mr_adopt_drop_failed", "error", err,
			"project", proj.Name, "repository", repo.Name, "number", number)
		return
	}
	obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, reason).Inc()
	s.log.InfoContext(ctx, "mr: dropped a queued dependency-upgrade adoption",
		"action", "mr_adopt_dropped", "resource_id", qe.Name, "project", proj.Name,
		"repository", repo.Name, "number", number, "reason", reason)
}

// handleMRClosed refreshes the mirror MergeRequest state on an out-of-band PR/MR
// close or merge (safe MIRROR write). merging already treats State=="merged" as
// done, and the merge/review reconcile finds a closed MR and converges - no new
// stage edge from the webhook.
func (s *Server) handleMRClosed(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	state := "closed"
	if ev.Merged || ev.Action == "merged" {
		state = "merged"
	}
	if s.stampMRState(ctx, &proj, repo, ev.Number, state) {
		s.log.InfoContext(ctx, "mr: mirrored out-of-band close/merge; reconcile converges",
			"action", "mr_closed_mirror", "project", proj.Name, "repository", repo.Name, "number", ev.Number, "state", state)
	}
	// LITERAL AT THIS CALL SITE, not `state` reused: a merged bump is the happy
	// path racing the queue, a closed one is a withdrawal, and
	// TestAdoptionDropReasonsMatchTheirProducers (internal/obs) scans for a
	// literal string argument passed directly to dropQueuedAdoption - the same
	// shape it already scans the dispatcher's drop(...) and the webhook's
	// pre-enqueue refusal in. Passing the shared `state` local through instead
	// would let a future third reason, added some OTHER way than a variable
	// literally named `state`, go unseen by that scan.
	if state == "merged" {
		s.dropQueuedAdoption(ctx, &proj, repo, ev.Number, "merged")
	} else {
		s.dropQueuedAdoption(ctx, &proj, repo, ev.Number, "closed")
	}
	// RESUME TRIGGER (maintainer close). Same signal as the comment trigger:
	// a maintainer disposing of an item is engagement.
	if tatarav1.IsMaintainer(&proj, repo, ev.ActorLogin) {
		if rerr := controller.StampBrainstormResume(ctx, s.cfg.Client, s.cfg.Namespace, proj.Name,
			controller.ResumeTriggerMaintainerClose); rerr != nil {
			s.log.ErrorContext(ctx, "brainstorm resume failed", "project", proj.Name,
				"number", ev.Number, "error", rerr)
		}
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// stampIssueState upserts Issue.Status.State on the mirror CR. Returns false when
// the CR is absent (nothing to refresh) or no Spiller is configured.
func (s *Server) stampIssueState(ctx context.Context, proj *tatarav1.Project, repo *tatarav1.Repository, number int, state string) bool {
	sp := s.cfg.SpillerFor(proj)
	if sp == nil {
		return false
	}
	key := types.NamespacedName{Namespace: s.cfg.Namespace, Name: tatarav1.IssueName(repo.Name, number)}
	if err := s.cfg.Client.Get(ctx, key, &tatarav1.Issue{}); err != nil {
		return false
	}
	if err := objbudget.FitIssue(ctx, s.cfg.Client, sp, key, func(i *tatarav1.Issue) {
		i.Status.State = state
	}); err != nil {
		obs.MirrorWriteDroppedTotal.WithLabelValues(proj.Name, "Issue", "issue_state").Inc()
		s.log.ErrorContext(ctx, "issues: mirror state refresh failed", "error", err, "issue", key.Name, "state", state)
		return false
	}
	return true
}

// stampMRHead upserts MergeRequest.Status.HeadSHA on the mirror CR, and - when
// botPush is true (the pusher is the project's configured bot identity) -
// also advances Status.LastBotHeadSHA to the same sha. A non-bot push leaves
// LastBotHeadSHA untouched: that staleness is the drift ReconcileOwnership
// (OP8) detects.
func (s *Server) stampMRHead(ctx context.Context, proj *tatarav1.Project, repo *tatarav1.Repository, number int, headSHA string, botPush bool) bool {
	return s.fitMR(ctx, proj, repo, number, func(m *tatarav1.MergeRequest) {
		m.Status.HeadSHA = headSHA
		if botPush {
			m.Status.LastBotHeadSHA = headSHA
		}
	})
}

// stampMRState upserts MergeRequest.Status.State (+ MergedAt on a merge) on the
// mirror CR.
func (s *Server) stampMRState(ctx context.Context, proj *tatarav1.Project, repo *tatarav1.Repository, number int, state string) bool {
	return s.fitMR(ctx, proj, repo, number, func(m *tatarav1.MergeRequest) {
		m.Status.State = state
		if state == "merged" && m.Status.MergedAt == nil {
			now := metav1.Now()
			m.Status.MergedAt = &now
		}
	})
}

// handleCIStatus is the CI-truth intake (PR A). GitHub check_suite/check_run/
// status and GitLab's Pipeline Hook used to be decoded into Kind:"other" and
// dropped on handle()'s default arm, which is why MergeRequest.status.ciStatus
// had exactly ONE writer - the mint-time mirror sync - and an MR the agent
// opened itself was stamped "" once and never again.
//
// THE JOIN IS ON THE HEAD SHA, not on a number: a CI delivery names a commit,
// never a pull request. Every mirrored MergeRequest in the delivering repository
// sitting at that head is stamped; two MRs may legitimately share a head.
//
// THE OWNING TASK NEEDS NO POKE FROM HERE. TaskReconciler's builder carries
// Owns(&tatarav1alpha1.MergeRequest{}) (task_controller.go), so the mirror
// status write enqueues its owner directly. That is also why this stays a pure
// mirror write and performs no stage transition: the F6-1 boundary keeps every
// Enter leader-only, and this runs on an HTTP goroutine.
//
// NO MATCH IS THE NORMAL CASE. CI runs on every push to every branch, and most
// of those branches are nobody's Task. The delivery is accepted and logged at
// debug - never rejected, because a 500 here is a delivery the forge retries
// forever for a commit this platform will never care about.
func (s *Server) handleCIStatus(ctx context.Context, w http.ResponseWriter, provider string,
	proj tatarav1.Project, ev scm.WebhookEvent) {

	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, "ci", ev.Action, "error")
		return
	}
	if repo == nil || ev.HeadSHA == "" || ev.CIStatus == "" {
		s.accept(w, provider, "ci", ev.Action, "ignored")
		return
	}

	var mrs tatarav1.MergeRequestList
	if err := s.cfg.Client.List(ctx, &mrs, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.reject(w, http.StatusInternalServerError, "list mergerequests", provider, "ci", ev.Action, "error")
		return
	}
	stamped := 0
	for i := range mrs.Items {
		mr := &mrs.Items[i]
		if mr.Spec.RepositoryRef != repo.Name || mr.Status.HeadSHA != ev.HeadSHA {
			continue
		}
		if s.stampMRCI(ctx, &proj, repo, mr.Spec.Number, ev.CIStatus) {
			stamped++
		}
	}
	if stamped == 0 {
		s.log.DebugContext(ctx, "ci: no mirrored merge request sits at this head; nothing to stamp",
			"action", "ci_status_unmatched", "project", proj.Name, "repository", repo.Name,
			"head_sha", ev.HeadSHA, "ci_status", ev.CIStatus)
		s.accept(w, provider, "ci", ev.Action, "ignored")
		return
	}
	s.log.InfoContext(ctx, "ci: stamped mirrored merge requests",
		"action", "ci_status_stamp", "project", proj.Name, "repository", repo.Name,
		"head_sha", ev.HeadSHA, "ci_status", ev.CIStatus, "count", stamped)
	s.accept(w, provider, "ci", ev.Action, "accepted")
}

// stampMRCI upserts Status.CIStatus and Status.CIUpdatedAt together.
//
// CIUpdatedAt is written on EVERY observation, including one that leaves the
// status untouched: it dates the OBSERVATION, not the change. A re-confirmed
// green is a stronger claim than the same green an hour old, and the bundle
// renders the date precisely so an agent can tell the two apart.
func (s *Server) stampMRCI(ctx context.Context, proj *tatarav1.Project, repo *tatarav1.Repository,
	number int, ciStatus string) bool {

	now := metav1.NewTime(s.now())
	return s.fitMR(ctx, proj, repo, number, func(m *tatarav1.MergeRequest) {
		m.Status.CIStatus = ciStatus
		m.Status.CIUpdatedAt = &now
	})
}

func (s *Server) fitMR(ctx context.Context, proj *tatarav1.Project, repo *tatarav1.Repository, number int, mut func(*tatarav1.MergeRequest)) bool {
	sp := s.cfg.SpillerFor(proj)
	if sp == nil {
		return false
	}
	key := types.NamespacedName{Namespace: s.cfg.Namespace, Name: tatarav1.MergeRequestName(repo.Name, number)}
	if err := s.cfg.Client.Get(ctx, key, &tatarav1.MergeRequest{}); err != nil {
		return false
	}
	if err := objbudget.FitMergeRequest(ctx, s.cfg.Client, sp, key, mut); err != nil {
		// stampMRHead routes through here to advance Status.LastBotHeadSHA on a
		// bot push; a dropped write leaves LastBotHeadSHA stale, which
		// ReconcileOwnership (OP8) later reads as head drift and triggers a
		// SPURIOUS ownership stand-down.
		obs.MirrorWriteDroppedTotal.WithLabelValues(proj.Name, "MergeRequest", "mr_refresh").Inc()
		s.log.WarnContext(ctx, "mr: mirror refresh failed; a stampMRHead caller may leave LastBotHeadSHA stale and trigger a spurious ownership stand-down",
			"error", err, "mr", key.Name, "project", proj.Name)
		return false
	}
	return true
}
