package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE REVIEW POST IS TWO PHASES, AND THE IDEMPOTENCY KEY LIVES ON THE FORGE
// (contract C.5.3).
//
// PHASE 1 is the /outcome HTTP handler (internal/restapi): pure etcd, no forge
// write. It persists mr.status.pendingReview and returns.
//
// PHASE 2 is HERE, in the reconciler, and it is idempotent across a crash
// ANYWHERE. Step 3 dedups the FORGE (the round marker in the review body); step
// 5 dedups the MIRROR (a set-union on externalId). NEITHER depends on evidence
// written after the irreversible act - which is exactly what v5 did, and why a
// crash between the post and the CR write re-posted the review and every finding,
// every round.
//
// The marker that gates the skip is written LAST and means "everything for this
// round is on the forge". A marker written before the work it guards is not an
// idempotency key; it is a lie with a timestamp.

// pendingCommentMarkerPrefix is the requestId marker every deferred comment
// carries. It is what replaces the DuplicateRecentBotComment guard mr_write has
// never had.
const pendingCommentMarkerPrefix = "<!-- tatara-request id="

// editIntentMarker / closeIntentMarker are the C.2.12 encodings the REST layer
// uses to defer issue_write(edit|close) through PendingComment, whose action
// enum is only {comment,reply}. The drain reads them back out and performs the
// edit/close; it never posts the marker as a comment body.
const (
	editIntentMarker  = "<!-- tatara-edit -->"
	closeIntentMarker = "<!-- tatara-close -->"
)

// PendingCommentMarker renders the forge-side idempotency key of one deferred
// comment. A comment on the thread carrying it means "this requestId already
// landed": the post is skipped, and only the mirror is reconciled.
func PendingCommentMarker(requestID string) string {
	return pendingCommentMarkerPrefix + requestID + " -->"
}

// DrainPendingReview is C.5.3 PHASE 2, on ONE MergeRequest. A nil pendingReview
// is a no-op.
//
//  3. FORGE-SIDE DEDUP CHECK - the load-bearing step.
//  4. POST (only when step 3 found nothing).
//     4b. FETCH THE COMMENT IDS - a SECOND, SEPARATE READ. The skip path lands
//     HERE, not at step 5: a skip that jumps past the id fetch mirrors ZERO
//     comments, and "the mirror converges to exactly one copy of each comment"
//     fails against it.
//  5. MIRROR APPEND, a set-union keyed on externalId.
//  6. BELT: the findings as an operator Note, so they ride in the next pod's
//     bundle even if the mirror append lost a race.
//  7. status / reviewedSHA / reviewRounds.
//  8. pendingReview = nil, LAST.
//  9. ONLY NOW does the Task advance (the stage machine gates on it).
func (d *StageDriver) DrainPendingReview(ctx context.Context, mr *tatarav1alpha1.MergeRequest) error {
	pr := mr.Status.PendingReview
	if pr == nil {
		return nil
	}
	l := log.FromContext(ctx)

	proj, repo, err := d.projectAndRepo(ctx, mr.Namespace, mr.Spec.ProjectRef, mr.Spec.RepositoryRef)
	if err != nil {
		return err
	}
	task, err := d.owningTask(ctx, mr)
	if err != nil {
		return err
	}
	writer, token, provider, err := d.forge(ctx, proj)
	if err != nil {
		return err
	}

	round := strconv.Itoa(pr.Round)
	marker := scm.ReviewMarker(round, pr.SHA)

	// STEP 3: THE FORGE IS THE LEDGER.
	existing, err := writer.ListReviews(ctx, repo.Spec.URL, token, mr.Spec.Number)
	if err != nil {
		obs.ReviewPostTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("review: list reviews %s!%d: %w", repo.Name, mr.Spec.Number, err)
	}
	reviewID := ""
	for _, rv := range existing {
		if scm.HasReviewMarker(rv.Body, round, pr.SHA) {
			reviewID = rv.ID
			break
		}
	}

	if reviewID == "" {
		// STEP 4: the marker goes in the BODY. GitLab parses it back out to derive
		// its per-finding markers, so a caller that does not prepend it degrades
		// GitLab's crash-resume keys to "round= sha= finding=K" and they stop being
		// unique across rounds.
		findings := scmFindings(pr.Findings)
		id, perr := writer.PostReview(ctx, repo.Spec.URL, token, mr.Spec.Number, marker+"\n"+pr.Body, findings)
		RecordSCM(d.Metrics, provider, "post_review", perr)
		switch {
		case perr == nil:
			reviewID = id
			obs.ReviewPostTotal.WithLabelValues("posted").Inc()
		case errors.Is(perr, scm.ErrReviewRefused):
			// A STRUCTURAL 4xx (422/401/403) is TERMINAL. It NEVER hot-requeues:
			// writeback_review.go treats any Approve error as firstErr and requeues
			// forever, re-driving members that can never succeed.
			obs.ReviewPostTotal.WithLabelValues("refused").Inc()
			l.Error(perr, "review: the forge REFUSED the review post; parking",
				"action", "review_post_refused", "resource_id", mr.Name,
				"repo", repo.Name, "pr", mr.Spec.Number, "round", pr.Round)
			if err := d.clearPendingReview(ctx, proj, mr, pr, "refused"); err != nil {
				return err
			}
			if task == nil {
				return nil
			}
			mrs, err := ownedMergeRequests(ctx, d.Client, task)
			if err != nil {
				return err
			}
			return d.enterStage(ctx, proj, task, tatarav1alpha1.StageParked, stage.ReasonReviewPostRefused, mrs)
		default:
			obs.ReviewPostTotal.WithLabelValues("error").Inc()
			return fmt.Errorf("review: post review %s!%d: %w", repo.Name, mr.Spec.Number, perr)
		}
	} else {
		obs.ReviewPostTotal.WithLabelValues("skipped").Inc()
		l.Info("review: the round marker is already on the forge; skipping the POST, reconciling the mirror",
			"action", "review_post_skipped", "resource_id", mr.Name,
			"repo", repo.Name, "pr", mr.Spec.Number, "round", pr.Round)
	}

	// STEP 4b: the SECOND READ. GitHub's create-review response returns the review
	// object (id, body, state, commit_id) and NOT the inline comments it created.
	posted, err := writer.ListReviewComments(ctx, repo.Spec.URL, token, mr.Spec.Number, reviewID)
	if err != nil {
		obs.ReviewPostTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("review: list review comments %s!%d: %w", repo.Name, mr.Spec.Number, err)
	}

	// STEP 5: the MIRROR APPEND, a SET-UNION KEYED ON externalId. A re-run is a
	// NO-OP, not a duplicate.
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	for _, c := range posted {
		cmt := tatarav1alpha1.Comment{
			ExternalID:  c.ExternalID,
			Author:      botLogin,
			Body:        c.Body,
			CreatedAt:   metav1.NewTime(c.CreatedAt),
			IsBot:       botLogin != "",
			Path:        c.Path,
			Line:        c.Line,
			InReplyTo:   c.InReplyTo,
			ReviewRound: pr.Round,
		}
		if err := AppendCommentToMirror(ctx, d.Client, d.spiller(proj), mr, cmt); err != nil {
			return err
		}
	}

	// STEP 6: THE BELT. reviewing -> implementing fires IMMEDIATELY, and the
	// mirror sweep is HOURLY: without this note a next implement pod whose mirror
	// append lost a race has no idea what to fix, re-submits, burns
	// maxReviewRounds and dies at parked(review-loop-exhausted).
	if task != nil && len(pr.Findings) > 0 {
		if err := d.appendOperatorNote(ctx, proj, task,
			reviewBeltNote(repo.Name, mr.Spec.Number, pr)); err != nil {
			return err
		}
	}

	// STEPS 7 + 8: settle the MR, and clear pendingReview LAST.
	if err := d.clearPendingReview(ctx, proj, mr, pr, reviewVerdictFromBody(pr.Body)); err != nil {
		return err
	}
	l.Info("review: posted",
		"action", "review_posted", "resource_id", mr.Name, "repo", repo.Name,
		"pr", mr.Spec.Number, "round", pr.Round, "sha", pr.SHA,
		"findings", len(pr.Findings), "mirrored", len(posted))

	// STEP 9: ONLY NOW does the Task advance.
	if task == nil {
		return nil
	}
	return d.advanceAfterReview(ctx, proj, task)
}

// reviewVerdictFromBody reads the verdict back out of the review body. There is
// no Event field on PendingReview (fix M9) and no verdict field: the body IS the
// record, and its first line is the operator's own text.
func reviewVerdictFromBody(body string) string {
	if strings.Contains(body, "## Review: approved") {
		return "approved"
	}
	return "needs-changes"
}

// reviewBeltNote renders the operator note that carries the findings into the
// NEXT pod's bundle.
func reviewBeltNote(repo string, number int, pr *tatarav1alpha1.PendingReview) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Review requested changes on %s!%d @ %s:\n", repo, number, pr.SHA)
	for _, f := range pr.Findings {
		fmt.Fprintf(&b, "- %s:%d [%s] %s\n", f.Path, f.Line, f.Severity, f.Body)
	}
	return b.String()
}

// clearPendingReview is C.5.3 steps 7 and 8 in ONE write: the settled review
// state, and pendingReview = nil LAST. verdict "refused" clears the intent
// without stamping a verdict - the review never landed.
func (d *StageDriver) clearPendingReview(ctx context.Context, proj *tatarav1alpha1.Project,
	mr *tatarav1alpha1.MergeRequest, pr *tatarav1alpha1.PendingReview, verdict string) error {
	key := client.ObjectKeyFromObject(mr)
	if err := objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
		if verdict != "refused" {
			m.Status.Status = verdict
			m.Status.ReviewedSHA = pr.SHA
			// reviewRounds counts ACCEPTED request_changes verdicts (A.2), so an
			// approve does not consume a round. /outcome already stamped it; this is
			// the idempotent re-assert.
			if verdict == "needs-changes" && m.Status.ReviewRounds < pr.Round {
				m.Status.ReviewRounds = pr.Round
			}
		}
		m.Status.PendingReview = nil
	}); err != nil {
		return fmt.Errorf("review: settle mr %s: %w", key.Name, err)
	}
	mr.Status.PendingReview = nil
	return nil
}

// advanceAfterReview is F.3's reviewing exit, and it runs ONLY once every owned
// MR has pendingReview == nil. Task 12's /outcome makes NO stage transition and
// structurally cannot: stage.LegalFor gates reviewing -> implementing|merging on
// that very field, and the handler had just set it.
func (d *StageDriver) advanceAfterReview(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) error {
	if task.Status.Stage != tatarav1alpha1.StageReviewing {
		return nil
	}
	mrs, err := ownedMergeRequests(ctx, d.Client, task)
	if err != nil {
		return err
	}
	if len(mrs) == 0 {
		return nil
	}
	needsChanges := false
	for i := range mrs {
		if mrs[i].Status.PendingReview != nil {
			return nil // a review is still owed to the forge
		}
		if mrs[i].Status.Status == "needs-changes" && mrs[i].Status.State != "merged" {
			needsChanges = true
		}
	}

	maxRounds := 3
	if proj.Spec.Agent.MaxReviewRounds > 0 {
		maxRounds = proj.Spec.Agent.MaxReviewRounds
	}

	var edge stage.Edge
	switch {
	case task.Spec.Kind == "review":
		// A kind=review Task NEVER reaches implementing or merging - not on
		// approve, not on request_changes, not from anywhere. Fixing and merging a
		// human's PR is a HUMAN action.
		edge = stage.Edge{To: tatarav1alpha1.StageParked, Reason: stage.ReasonAwaitingHuman}
	case needsChanges:
		edge, _ = stage.RequestChanges(task, mrs, maxRounds)
	default:
		edge = stage.Edge{To: tatarav1alpha1.StageMerging}
	}
	log.FromContext(ctx).Info("review: task advancing off reviewing",
		"action", "review_advance", "resource_id", task.Name,
		"to", edge.To, "reason", edge.Reason, "kind", task.Spec.Kind)
	return d.enterStage(ctx, proj, task, edge.To, edge.Reason, mrs)
}

// DrainPendingComments is the SAME SHAPE as the review post, for
// mr_write(comment|reply) and issue_write(comment|edit|close): the requestId
// marker is the forge-side dedup, and the mirror append is a set-union on
// externalId. obj is an *Issue or a *MergeRequest.
func (d *StageDriver) DrainPendingComments(ctx context.Context, obj client.Object) error {
	var (
		pending []tatarav1alpha1.PendingComment
		repoRef string
		projRef string
		number  int
	)
	switch o := obj.(type) {
	case *tatarav1alpha1.Issue:
		pending, repoRef, projRef, number = o.Status.PendingComments, o.Spec.RepositoryRef, o.Spec.ProjectRef, o.Spec.Number
	case *tatarav1alpha1.MergeRequest:
		pending, repoRef, projRef, number = o.Status.PendingComments, o.Spec.RepositoryRef, o.Spec.ProjectRef, o.Spec.Number
	default:
		return fmt.Errorf("review: DrainPendingComments: unsupported object %T", obj)
	}
	if len(pending) == 0 {
		return nil
	}
	l := log.FromContext(ctx)

	proj, repo, err := d.projectAndRepo(ctx, obj.GetNamespace(), projRef, repoRef)
	if err != nil {
		return err
	}
	writer, token, provider, err := d.forge(ctx, proj)
	if err != nil {
		return err
	}
	slug, _, err := repoSlugFromURL(repo.Spec.URL, provider)
	if err != nil {
		return fmt.Errorf("review: repo slug for %s: %w", repo.Name, err)
	}

	drained := make([]string, 0, len(pending))
	for _, pc := range pending {
		body := pc.Body
		switch {
		case strings.HasPrefix(body, closeIntentMarker):
			// issue_write(close), deferred as a comment intent (C.2.12).
			reason := strings.TrimPrefix(body, closeIntentMarker)
			reason = strings.TrimPrefix(reason, "\n")
			closeErr := writer.CloseIssue(ctx, token, slug, number, reason)
			RecordSCM(d.Metrics, provider, "close_issue", closeErr)
			if closeErr != nil {
				return fmt.Errorf("review: close issue %s#%d: %w", slug, number, closeErr)
			}
			if iss, ok := obj.(*tatarav1alpha1.Issue); ok {
				key := client.ObjectKeyFromObject(iss)
				if err := objbudget.FitIssue(ctx, d.Client, d.spiller(proj), key, func(i *tatarav1alpha1.Issue) {
					i.Status.State = "closed"
				}); err != nil {
					return fmt.Errorf("review: stamp closed on %s: %w", key.Name, err)
				}
			}
			l.Info("review: issue closed", "action", "scm_issue_closed", "resource_id", obj.GetName(),
				"repo", repo.Name, "number", number, "request_id_key", pc.RequestID)

		case strings.HasPrefix(body, editIntentMarker):
			// issue_write(edit), deferred as a comment intent (C.2.12).
			title, newBody := parseEditIntent(body)
			req := scm.EditIssueReq{}
			if title != "" {
				req.Title = &title
			}
			if newBody != "" {
				req.Body = &newBody
			}
			editErr := writer.EditIssue(ctx, token, slug, number, req)
			RecordSCM(d.Metrics, provider, "edit_issue", editErr)
			if editErr != nil {
				return fmt.Errorf("review: edit issue %s#%d: %w", slug, number, editErr)
			}
			l.Info("review: issue edited", "action", "scm_issue_edited", "resource_id", obj.GetName(),
				"repo", repo.Name, "number", number, "request_id_key", pc.RequestID)

		default:
			if err := d.postThreadComment(ctx, proj, repo, obj, writer, token, provider, slug, number, pc); err != nil {
				return err
			}
		}
		drained = append(drained, pc.RequestID)
	}

	return d.removePendingComments(ctx, proj, obj, drained)
}

// postThreadComment posts one deferred comment/reply, SKIPPING the write when
// the thread already carries its requestId marker, and then reconciles the
// mirror by externalId. The forge returns no id from the post, so the id (like
// the review's inline comments) comes from a SECOND read.
func (d *StageDriver) postThreadComment(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, obj client.Object, writer scm.SCMWriter,
	token, provider, slug string, number int, pc tatarav1alpha1.PendingComment) error {
	marker := PendingCommentMarker(pc.RequestID)
	reader, err := d.reader(provider, token)
	if err != nil {
		return err
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return fmt.Errorf("review: owner/repo for %s: %w", repo.Name, err)
	}

	thread, err := listThreadComments(ctx, reader, obj, owner, name, number)
	if err != nil {
		return err
	}
	if !threadCarriesMarker(thread, marker) {
		commentErr := writer.Comment(ctx, token, fmt.Sprintf("%s#%d", slug, number), marker+"\n"+pc.Body)
		RecordSCM(d.Metrics, provider, "comment", commentErr)
		if commentErr != nil {
			return fmt.Errorf("review: comment on %s#%d: %w", slug, number, commentErr)
		}
		thread, err = listThreadComments(ctx, reader, obj, owner, name, number)
		if err != nil {
			return err
		}
	}

	for _, c := range thread {
		if !strings.Contains(c.Body, marker) {
			continue
		}
		cmt := mirrorCommentFrom(proj, c)
		cmt.InReplyTo = pc.InReplyTo
		if err := AppendCommentToMirror(ctx, d.Client, d.spiller(proj), obj, cmt); err != nil {
			return err
		}
	}
	log.FromContext(ctx).Info("review: pending comment drained",
		"action", "scm_comment_posted", "resource_id", obj.GetName(),
		"repo", repo.Name, "number", number, "request_id_key", pc.RequestID, "comment_action", pc.Action)
	return nil
}

func listThreadComments(ctx context.Context, reader scm.SCMReader, obj client.Object,
	owner, name string, number int) ([]scm.IssueComment, error) {
	if _, isMR := obj.(*tatarav1alpha1.MergeRequest); isMR {
		if pl, ok := reader.(scm.PRCommentLister); ok {
			out, err := pl.ListPRComments(ctx, owner, name, number)
			if err != nil {
				return nil, fmt.Errorf("review: list pr comments %s!%d: %w", name, number, err)
			}
			return out, nil
		}
	}
	out, err := reader.ListIssueComments(ctx, owner, name, number)
	if err != nil {
		return nil, fmt.Errorf("review: list comments %s#%d: %w", name, number, err)
	}
	return out, nil
}

func threadCarriesMarker(thread []scm.IssueComment, marker string) bool {
	for _, c := range thread {
		if strings.Contains(c.Body, marker) {
			return true
		}
	}
	return false
}

// parseEditIntent reads Task 12's edit encoding back out: an optional
// "title: <t>" first line, then the body.
func parseEditIntent(body string) (title, newBody string) {
	rest := strings.TrimPrefix(body, editIntentMarker)
	rest = strings.TrimPrefix(rest, "\n")
	if strings.HasPrefix(rest, "title: ") {
		line, tail, _ := strings.Cut(rest, "\n")
		return strings.TrimPrefix(line, "title: "), tail
	}
	return "", rest
}

// removePendingComments drops the drained intents. It is keyed on requestId, so
// an intent queued while the drain was in flight survives.
func (d *StageDriver) removePendingComments(ctx context.Context, proj *tatarav1alpha1.Project,
	obj client.Object, drained []string) error {
	done := make(map[string]bool, len(drained))
	for _, id := range drained {
		done[id] = true
	}
	keep := func(in []tatarav1alpha1.PendingComment) []tatarav1alpha1.PendingComment {
		out := make([]tatarav1alpha1.PendingComment, 0, len(in))
		for _, pc := range in {
			if !done[pc.RequestID] {
				out = append(out, pc)
			}
		}
		return out
	}
	key := client.ObjectKeyFromObject(obj)
	switch obj.(type) {
	case *tatarav1alpha1.Issue:
		if err := objbudget.FitIssue(ctx, d.Client, d.spiller(proj), key, func(i *tatarav1alpha1.Issue) {
			i.Status.PendingComments = keep(i.Status.PendingComments)
		}); err != nil {
			return fmt.Errorf("review: drain pending comments on %s: %w", key.Name, err)
		}
	case *tatarav1alpha1.MergeRequest:
		if err := objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
			m.Status.PendingComments = keep(m.Status.PendingComments)
		}); err != nil {
			return fmt.Errorf("review: drain pending comments on %s: %w", key.Name, err)
		}
	}
	return nil
}

// --- shared plumbing ------------------------------------------------------

func (d *StageDriver) reader(provider, token string) (scm.SCMReader, error) {
	if d.ReaderFor == nil {
		return nil, fmt.Errorf("review: no SCM reader wired")
	}
	reader, err := d.ReaderFor(provider, token)
	if err != nil {
		return nil, fmt.Errorf("review: scm reader: %w", err)
	}
	return reader, nil
}

func (d *StageDriver) projectAndRepo(ctx context.Context, ns, projRef, repoRef string) (
	*tatarav1alpha1.Project, *tatarav1alpha1.Repository, error) {
	var proj tatarav1alpha1.Project
	if err := d.Get(ctx, types.NamespacedName{Namespace: ns, Name: projRef}, &proj); err != nil {
		return nil, nil, fmt.Errorf("review: get project %s: %w", projRef, err)
	}
	var repo tatarav1alpha1.Repository
	if err := d.Get(ctx, types.NamespacedName{Namespace: ns, Name: repoRef}, &repo); err != nil {
		return nil, nil, fmt.Errorf("review: get repository %s: %w", repoRef, err)
	}
	return &proj, &repo, nil
}

// owningTask resolves the CONTROLLER-owning Task. A MergeRequest with no
// controller owner is not an error here: B.2 rule 5's repair guard owns that.
func (d *StageDriver) owningTask(ctx context.Context, obj client.Object) (*tatarav1alpha1.Task, error) {
	name, ok := own.ControllerOwner(obj)
	if !ok {
		return nil, nil
	}
	var task tatarav1alpha1.Task
	if err := d.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}, &task); err != nil {
		return nil, fmt.Errorf("review: get owning task %s: %w", name, err)
	}
	return &task, nil
}

// appendOperatorNote appends ONE operator note, idempotently: a re-run of phase
// 2 must not write the belt twice.
func (d *StageDriver) appendOperatorNote(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, body string) error {
	now := metav1.NewTime(d.now())
	key := client.ObjectKeyFromObject(task)
	if err := objbudget.FitTask(ctx, d.Client, d.spiller(proj), key, func(t *tatarav1alpha1.Task) {
		for _, n := range t.Status.Notes {
			if n.Agent == "operator" && n.Body == body {
				return
			}
		}
		t.Status.Notes = append(t.Status.Notes, tatarav1alpha1.Note{
			At: now, Agent: "operator", Kind: "note", Body: body,
		})
	}); err != nil {
		return fmt.Errorf("review: append operator note to %s: %w", key.Name, err)
	}
	return nil
}

// scmFindings maps the CR's findings onto the SCM shape.
func scmFindings(in []tatarav1alpha1.ReviewFinding) []scm.ReviewFinding {
	out := make([]scm.ReviewFinding, 0, len(in))
	for _, f := range in {
		out = append(out, scm.ReviewFinding{Path: f.Path, Line: f.Line, Body: f.Body, Severity: f.Severity})
	}
	return out
}
