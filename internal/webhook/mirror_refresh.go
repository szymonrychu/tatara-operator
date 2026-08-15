package webhook

import (
	"context"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
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
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
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
