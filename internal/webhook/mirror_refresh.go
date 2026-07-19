package webhook

import (
	"context"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
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
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// handleIssueEdited is WS3-I2. On an owned/tracked issue update it (a) refreshes
// the mirror Body/Title (safe MIRROR write - the agent's scm_read is served from
// the mirror), and (b) if the body or title actually CHANGED, appends an
// issue_edited pending event on the owning Task. It does NOT drive the unpark:
// the leader-only driveUnparks/Task reconcile consumes the fresh pending event
// (awaiting-human / backlog-sweep re-engage; identity-unverified needs an
// approval phrase, not an edit, so it stays parked - correct).
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
				changed = i.Status.Body != ev.Body || i.Status.Title != ev.Title
				i.Status.Body = ev.Body
				i.Status.Title = ev.Title
			}); ferr != nil {
			s.log.ErrorContext(ctx, "issues: mirror body/title refresh failed", "error", ferr,
				"project", proj.Name, "issue_ref", ev.IssueRef)
		}
	} else {
		s.log.ErrorContext(ctx, "issues: no Spiller configured; mirror body/title refresh skipped", "issue_ref", ev.IssueRef)
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
	if _, created, merr := s.minter().MintForItem(ctx, proj, repo, item, true, s.cfg.SpillerFor(proj)); merr != nil {
		s.log.ErrorContext(ctx, "issues: trigger-label mint failed", "error", merr,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		return
	} else if created {
		if cerr := controller.ClearWebhookOriginated(ctx, s.cfg.Client, s.cfg.Namespace, tatarav1.IssueName(repo.Name, ev.Number)); cerr != nil {
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
	bot := isBotActor(&proj, ev.ActorLogin)
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
		s.log.ErrorContext(ctx, "mr: mirror refresh failed", "error", err, "mr", key.Name)
		return false
	}
	return true
}
