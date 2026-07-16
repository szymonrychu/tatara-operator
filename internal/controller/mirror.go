package controller

import (
	"context"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// THE MIRROR (contract B.4, C.1, C.2.8/C.2.11).
//
// scm_read(kind=issues|comments|mr) is served from the Issue/MergeRequest CR
// mirror and from NOTHING ELSE. Only kind=ci hits the forge live. v2's live
// passthrough with an optional repo fanned ONE agent call into ~60 unpaced forge
// requests across 15 repos; it built this mirror and then bypassed it. Building
// the mirror and then bypassing it is exactly what this redesign exists to stop,
// so every read the agents make lands here.
//
// EVERY write in this file goes through objbudget.Fit* (contract A.7). An
// unsized write to an Issue/MergeRequest/Task is how a 413 makes an object
// permanently unwritable and pins everything it owns open forever.

const (
	// commentBodyLimit is the A.1 ingest cap: 8192 BYTES, cut on a rune
	// boundary. GitHub allows 65,536-char bodies, so 25 max-size comments is
	// 1.6 MB - over the etcd ceiling on its own - and a 64 KB comment is not
	// prompt-useful anyway.
	commentBodyLimit = 8192

	// MirrorCadenceActive is the sync interval for the Issues/MRs of an ACTIVE
	// (pod-eligible) Task.
	MirrorCadenceActive = time.Hour

	// MirrorCadenceParked is the sync interval for the Issues/MRs of EVERY
	// parked Task - not just parked(backlog-sweep) (fix M27, completed by fix
	// M11). A parked backlog Task is NOT free: its Issues are MIRRORED, and 150
	// backlog issues plus their threads is several hundred forge requests per
	// sweep, hourly, forever. A backlog issue nobody is working does not need an
	// hourly re-read: a webhook tells us the moment it changes, and the daily
	// pass is the backstop for a missed webhook.
	//
	// The DAILY cadence is what makes SyncIssueOnDemand mandatory rather than an
	// optimisation - see that function.
	MirrorCadenceParked = 24 * time.Hour
)

// Field indexes (contract A.3). Dedup - the sweep AND the QueuedEvent producer -
// is an indexed lookup on issueKey/mrKey, never a hashed Task name and NEVER a
// label selector: label VALUES reject ':' and '#', so a "natural key" on a label
// would be silently sha256-hashed straight back into the opaque digest this
// redesign exists to delete.
const (
	// IssueKeyIndex indexes Issue by "<repositoryRef>#<number>".
	IssueKeyIndex = "issueKey"
	// MRKeyIndex indexes MergeRequest by "<repositoryRef>!<number>".
	MRKeyIndex = "mrKey"
	// TaskProjectRefIndex indexes Task by spec.projectRef.
	TaskProjectRefIndex = "projectRef"
	// TaskDocumentsTasksIndex indexes Task by EACH element of
	// spec.documentsTasks (one index entry per element), so the reaper can ask
	// "which nightly documentation batch covers this delivered Task?" without
	// listing every Task.
	TaskDocumentsTasksIndex = "documentsTasks"
)

// IssueKey returns the A.3 issueKey for an Issue: "<repositoryRef>#<number>".
func IssueKey(repoRef string, number int) string {
	return fmt.Sprintf("%s#%d", repoRef, number)
}

// MRKey returns the A.3 mrKey for a MergeRequest: "<repositoryRef>!<number>".
func MRKey(repoRef string, number int) string {
	return fmt.Sprintf("%s!%d", repoRef, number)
}

// IssueKeyIndexer is the client.IndexerFunc for IssueKeyIndex.
func IssueKeyIndexer(obj client.Object) []string {
	iss, ok := obj.(*tatarav1alpha1.Issue)
	if !ok || iss.Spec.RepositoryRef == "" || iss.Spec.Number == 0 {
		return nil
	}
	return []string{IssueKey(iss.Spec.RepositoryRef, iss.Spec.Number)}
}

// MRKeyIndexer is the client.IndexerFunc for MRKeyIndex.
func MRKeyIndexer(obj client.Object) []string {
	mr, ok := obj.(*tatarav1alpha1.MergeRequest)
	if !ok || mr.Spec.RepositoryRef == "" || mr.Spec.Number == 0 {
		return nil
	}
	return []string{MRKey(mr.Spec.RepositoryRef, mr.Spec.Number)}
}

// TaskProjectRefIndexer is the client.IndexerFunc for TaskProjectRefIndex.
func TaskProjectRefIndexer(obj client.Object) []string {
	t, ok := obj.(*tatarav1alpha1.Task)
	if !ok || t.Spec.ProjectRef == "" {
		return nil
	}
	return []string{t.Spec.ProjectRef}
}

// TaskDocumentsTasksIndexer is the client.IndexerFunc for
// TaskDocumentsTasksIndex: ONE index entry per element.
func TaskDocumentsTasksIndexer(obj client.Object) []string {
	t, ok := obj.(*tatarav1alpha1.Task)
	if !ok || len(t.Spec.DocumentsTasks) == 0 {
		return nil
	}
	out := make([]string, 0, len(t.Spec.DocumentsTasks))
	for _, name := range t.Spec.DocumentsTasks {
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

// MirrorCadence returns how often the Issues/MergeRequests owned by t are
// re-read from the forge (contract B.4). EVERY parked Task syncs DAILY; every
// other Task - and an artifact with no owning Task at all - syncs HOURLY.
func MirrorCadence(t *tatarav1alpha1.Task) time.Duration {
	if t != nil && t.Status.Stage == tatarav1alpha1.StageParked {
		return MirrorCadenceParked
	}
	return MirrorCadenceActive
}

// truncateCommentBody cuts body at commentBodyLimit BYTES on a RUNE boundary
// (contract A.1, fix E3) and reports whether it cut. A byte-slice of a UTF-8
// string cuts mid-rune, and an invalid-UTF-8 string is rejected by the API
// server's JSON encoder, so the rune boundary is not cosmetic.
func truncateCommentBody(body string) (string, bool) {
	if len(body) <= commentBodyLimit {
		return body, false
	}
	cut := body[:commentBodyLimit]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut, true
}

// mirrorCommentFrom maps one forge comment onto the CR mirror's Comment,
// truncating the body and computing IsBot from Project.spec.scm.botLogin.
//
// IsBot is the STRUCTURAL bot exclusion the approval grammar (C.6 clause 3a) and
// the pendingEvents enqueue filter (E.3) rely on. An EMPTY author is never the
// bot: a deleted account must not pass an equality gate.
func mirrorCommentFrom(proj *tatarav1alpha1.Project, c scm.IssueComment) tatarav1alpha1.Comment {
	body, truncated := truncateCommentBody(c.Body)
	botLogin := ""
	if proj != nil && proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	return tatarav1alpha1.Comment{
		ExternalID: c.ExternalID,
		Author:     c.Author,
		Body:       body,
		CreatedAt:  metav1.NewTime(c.CreatedAt),
		IsBot:      botLogin != "" && c.Author != "" && c.Author == botLogin,
		Truncated:  truncated,
	}
}

// mirrorComments maps a whole forge thread, counting truncations.
func mirrorComments(kind string, proj *tatarav1alpha1.Project, in []scm.IssueComment) []tatarav1alpha1.Comment {
	out := make([]tatarav1alpha1.Comment, 0, len(in))
	for _, c := range in {
		cm := mirrorCommentFrom(proj, c)
		if cm.Truncated {
			obs.MirrorCommentTruncatedTotal.WithLabelValues(kind).Inc()
		}
		out = append(out, cm)
	}
	return out
}

// mergeComments is the SET-UNION KEYED ON externalId that every mirror write
// goes through. It is pure and idempotent (objbudget.Fit* calls the mutate
// closure once to size the write and again against a freshly re-Get'd object on
// every conflict retry), and it NEVER deletes: an inline review comment the
// operator appended in the same handler (fix C5) must survive a later sync of a
// thread listing that does not carry it.
//
// retainFrom is the fix-M18 eviction WATERMARK. Comments older than it were
// spilled to tatara-memory and are NOT re-ingested. Without this the very next
// sweep re-fetches every evicted comment (its ExternalID is no longer in the CR,
// so the dedup key is gone), the next fit re-evicts it, and the pair loops -
// writing a duplicate spill record every hour, forever.
func mergeComments(existing, incoming []tatarav1alpha1.Comment, retainFrom *metav1.Time) []tatarav1alpha1.Comment {
	out := make([]tatarav1alpha1.Comment, len(existing))
	copy(out, existing)
	byID := make(map[string]int, len(out))
	for i, c := range out {
		byID[c.ExternalID] = i
	}
	for _, c := range incoming {
		if retainFrom != nil && c.CreatedAt.Time.Before(retainFrom.Time) {
			continue
		}
		if i, ok := byID[c.ExternalID]; ok && c.ExternalID != "" {
			// Upsert: a body edited on the forge converges, and the operator's
			// own inline fields (path/line/inReplyTo/reviewRound) are preserved
			// when the incoming copy does not carry them.
			out[i] = mergeOneComment(out[i], c)
			continue
		}
		byID[c.ExternalID] = len(out)
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.Time.Before(out[j].CreatedAt.Time) })
	return out
}

// syncedCondition is the "Synced" condition stamped on an Issue/MergeRequest by
// every successful mirror write. meta.SetStatusCondition keeps
// LastTransitionTime stable when nothing changed, so re-stamping it on every
// sync is a no-op write and does not hot-loop the reconciler.
func syncedCondition() metav1.Condition {
	return metav1.Condition{
		Type:    "Synced",
		Status:  metav1.ConditionTrue,
		Reason:  "MirrorSynced",
		Message: "the mirror is converged with the forge",
	}
}

// mergeOneComment folds an incoming copy of a comment onto the mirrored one.
// The forge listing is authoritative for the body/author/timestamp; the inline
// review fields are kept when the incoming copy does not carry them (a plain
// thread listing does not return path/line/inReplyTo).
func mergeOneComment(cur, in tatarav1alpha1.Comment) tatarav1alpha1.Comment {
	out := in
	if out.Path == "" {
		out.Path = cur.Path
	}
	if out.Line == 0 {
		out.Line = cur.Line
	}
	if out.InReplyTo == "" {
		out.InReplyTo = cur.InReplyTo
	}
	if out.ReviewRound == 0 {
		out.ReviewRound = cur.ReviewRound
	}
	return out
}

// ensureIssueCR creates the Issue CR for (repo, number) when it does not exist.
// It creates it OWNERLESS: ownership is the sweep's adopt-or-create (B.4, fix
// M3-10), which appends the minting Task as the controller owner - and a zero-
// owner Issue is exactly the survivor that path adopts.
func ensureIssueCR(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int, url string) error {
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.IssueName(repo.Name, number)}
	var cur tatarav1alpha1.Issue
	err := c.Get(ctx, key, &cur)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("mirror: get issue %s: %w", key.Name, err)
	}
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name,
			Number:        number,
			URL:           url,
			ProjectRef:    proj.Name,
		},
	}
	if err := c.Create(ctx, iss); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("mirror: create issue %s: %w", key.Name, err)
	}
	return nil
}

// ensureMergeRequestCR is the MergeRequest counterpart of ensureIssueCR.
func ensureMergeRequestCR(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int, url string) error {
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, number)}
	var cur tatarav1alpha1.MergeRequest
	err := c.Get(ctx, key, &cur)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("mirror: get mergerequest %s: %w", key.Name, err)
	}
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name,
			Number:        number,
			URL:           url,
			ProjectRef:    proj.Name,
		},
	}
	if err := c.Create(ctx, mr); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("mirror: create mergerequest %s: %w", key.Name, err)
	}
	return nil
}

// SyncIssue upserts the Issue CR from a forge issue and its thread. It makes NO
// forge call: ext is the snapshot the caller already read on the paced,
// rate-limited read path (C.8).
//
// It never writes status.status: that is the platform's decision state, owned by
// the C.6 approval grammar and the operator's own lifecycle writes. The mirror
// carries SCM TRUTH (state/labels/comments) and nothing else.
func SyncIssue(ctx context.Context, c client.Client, sp objbudget.Spiller, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, ext scm.Issue) error {
	if err := ensureIssueCR(ctx, c, proj, repo, ext.Number, ext.URL); err != nil {
		obs.MirrorSyncTotal.WithLabelValues("Issue", "error").Inc()
		return err
	}
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.IssueName(repo.Name, ext.Number)}
	incoming := mirrorComments("Issue", proj, ext.Comments)
	now := metav1.Now()

	err := objbudget.FitIssue(ctx, c, sp, key, func(iss *tatarav1alpha1.Issue) {
		iss.Status.Title = ext.Title
		iss.Status.Author = ext.Author
		iss.Status.Body = ext.Body
		iss.Status.State = ext.State
		iss.Status.Labels = ext.Labels
		if !ext.CreatedAt.IsZero() {
			t := metav1.NewTime(ext.CreatedAt)
			iss.Status.CreatedAt = &t
		}
		if !ext.UpdatedAt.IsZero() {
			t := metav1.NewTime(ext.UpdatedAt)
			iss.Status.UpdatedAt = &t
		}
		iss.Status.Comments = mergeComments(iss.Status.Comments, incoming, iss.Status.CommentsRetainedFrom)
		iss.Status.LastSyncedAt = &now
		meta.SetStatusCondition(&iss.Status.Conditions, syncedCondition())
	})
	if err != nil {
		obs.MirrorSyncTotal.WithLabelValues("Issue", "error").Inc()
		return fmt.Errorf("mirror: sync issue %s: %w", key.Name, err)
	}
	obs.MirrorSyncTotal.WithLabelValues("Issue", "ok").Inc()
	log.FromContext(ctx).Info("mirror: synced issue",
		"action", "mirror_sync", "kind", "Issue", "resource_id", key.Name,
		"repo", repo.Name, "number", ext.Number, "state", ext.State, "comments", len(incoming))
	return nil
}

// SyncMergeRequest is the MergeRequest half of SyncIssue. status.headSHA is the
// MIRROR's last-synced head: a merge and an approval both re-read it LIVE (fix
// 10), because the mirror can be an hour stale and a merge pinned to a stale SHA
// is a TOCTOU hole on the repo that deploys the cluster.
func SyncMergeRequest(ctx context.Context, c client.Client, sp objbudget.Spiller, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, ext scm.MergeRequest) error {
	if err := ensureMergeRequestCR(ctx, c, proj, repo, ext.Number, ext.URL); err != nil {
		obs.MirrorSyncTotal.WithLabelValues("MergeRequest", "error").Inc()
		return err
	}
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, ext.Number)}
	incoming := mirrorComments("MergeRequest", proj, ext.Comments)
	now := metav1.Now()

	err := objbudget.FitMergeRequest(ctx, c, sp, key, func(mr *tatarav1alpha1.MergeRequest) {
		mr.Status.Title = ext.Title
		mr.Status.Author = ext.Author
		mr.Status.Body = ext.Body
		mr.Status.State = ext.State
		mr.Status.HeadBranch = ext.HeadBranch
		mr.Status.HeadSHA = ext.HeadSHA
		mr.Status.CIStatus = ext.CIStatus
		mr.Status.Mergeable = ext.Mergeable
		if !ext.CreatedAt.IsZero() {
			t := metav1.NewTime(ext.CreatedAt)
			mr.Status.CreatedAt = &t
		}
		if !ext.UpdatedAt.IsZero() {
			t := metav1.NewTime(ext.UpdatedAt)
			mr.Status.UpdatedAt = &t
		}
		if ext.MergedAt != nil {
			t := metav1.NewTime(*ext.MergedAt)
			mr.Status.MergedAt = &t
		}
		mr.Status.Comments = mergeComments(mr.Status.Comments, incoming, mr.Status.CommentsRetainedFrom)
		mr.Status.LastSyncedAt = &now
		meta.SetStatusCondition(&mr.Status.Conditions, syncedCondition())
	})
	if err != nil {
		obs.MirrorSyncTotal.WithLabelValues("MergeRequest", "error").Inc()
		return fmt.Errorf("mirror: sync mergerequest %s: %w", key.Name, err)
	}
	obs.MirrorSyncTotal.WithLabelValues("MergeRequest", "ok").Inc()
	log.FromContext(ctx).Info("mirror: synced mergerequest",
		"action", "mirror_sync", "kind", "MergeRequest", "resource_id", key.Name,
		"repo", repo.Name, "number", ext.Number, "state", ext.State, "comments", len(incoming))
	return nil
}

// AppendCommentToMirror is the C.5 write-back helper (fix C5). EVERY operator
// SCM write that produces a comment calls it IN THE SAME HANDLER.
//
// Without it: fix 14 has the OPERATOR post the review, fix C1 serves
// <merge_request><comments> FROM THE MIRROR (which only the sweep writes), and
// reviewing -> implementing fires immediately - so the next implement pod's
// turn-0 bundle is rendered from a mirror that does NOT contain the findings it
// is supposed to be fixing. The agent has no idea what to fix, re-submits, hits
// maxReviewRounds and dies at parked(review-loop-exhausted), on EVERY
// changes-requested cycle, for the first hour after every review.
//
// The append is a set-union on externalId, so a crash between the forge post and
// this call re-runs as a NO-OP, not a duplicate.
func AppendCommentToMirror(ctx context.Context, c client.Client, sp objbudget.Spiller, obj client.Object, cmt tatarav1alpha1.Comment) error {
	cmt.Body, cmt.Truncated = truncateCommentBody(cmt.Body)
	key := client.ObjectKeyFromObject(obj)

	switch obj.(type) {
	case *tatarav1alpha1.Issue:
		if cmt.Truncated {
			obs.MirrorCommentTruncatedTotal.WithLabelValues("Issue").Inc()
		}
		if err := objbudget.FitIssue(ctx, c, sp, key, func(iss *tatarav1alpha1.Issue) {
			iss.Status.Comments = mergeComments(iss.Status.Comments,
				[]tatarav1alpha1.Comment{cmt}, iss.Status.CommentsRetainedFrom)
		}); err != nil {
			return fmt.Errorf("mirror: append comment to issue %s: %w", key.Name, err)
		}
	case *tatarav1alpha1.MergeRequest:
		if cmt.Truncated {
			obs.MirrorCommentTruncatedTotal.WithLabelValues("MergeRequest").Inc()
		}
		if err := objbudget.FitMergeRequest(ctx, c, sp, key, func(mr *tatarav1alpha1.MergeRequest) {
			mr.Status.Comments = mergeComments(mr.Status.Comments,
				[]tatarav1alpha1.Comment{cmt}, mr.Status.CommentsRetainedFrom)
		}); err != nil {
			return fmt.Errorf("mirror: append comment to mergerequest %s: %w", key.Name, err)
		}
	default:
		return fmt.Errorf("mirror: AppendCommentToMirror: unsupported object %T", obj)
	}

	log.FromContext(ctx).Info("mirror: appended operator comment",
		"action", "mirror_append_comment", "resource_id", key.Name,
		"external_id", cmt.ExternalID, "review_round", cmt.ReviewRound)
	return nil
}

// syncIssueThread re-reads ONE issue thread from the forge and merges it into
// the mirror. It is the single-forge-read path shared by the Issue reconciler's
// cadence sync and by SyncIssueOnDemand.
func syncIssueThread(ctx context.Context, c client.Client, sp objbudget.Spiller, reader scm.SCMReader,
	proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, iss *tatarav1alpha1.Issue) error {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return fmt.Errorf("mirror: owner/repo for %s: %w", repo.Name, err)
	}
	comments, err := reader.ListIssueComments(ctx, owner, name, iss.Spec.Number)
	if err != nil {
		obs.MirrorSyncTotal.WithLabelValues("Issue", "error").Inc()
		return fmt.Errorf("mirror: list comments for %s: %w", IssueKey(repo.Name, iss.Spec.Number), err)
	}
	incoming := mirrorComments("Issue", proj, comments)
	now := metav1.Now()
	key := client.ObjectKeyFromObject(iss)
	if err := objbudget.FitIssue(ctx, c, sp, key, func(cur *tatarav1alpha1.Issue) {
		cur.Status.Comments = mergeComments(cur.Status.Comments, incoming, cur.Status.CommentsRetainedFrom)
		cur.Status.LastSyncedAt = &now
		meta.SetStatusCondition(&cur.Status.Conditions, syncedCondition())
	}); err != nil {
		obs.MirrorSyncTotal.WithLabelValues("Issue", "error").Inc()
		return fmt.Errorf("mirror: sync issue thread %s: %w", key.Name, err)
	}
	obs.MirrorSyncTotal.WithLabelValues("Issue", "ok").Inc()
	log.FromContext(ctx).Info("mirror: synced issue thread",
		"action", "mirror_sync_thread", "kind", "Issue", "resource_id", key.Name,
		"repo", repo.Name, "number", iss.Spec.Number, "comments", len(incoming))
	return nil
}

// syncMergeRequestThread is the MergeRequest counterpart of syncIssueThread. It
// reads the MR's own notes endpoint when the reader implements PRCommentLister:
// GitLab merge requests live in a distinct IID namespace, so reusing
// ListIssueComments for an MR reads the wrong (or no) notes.
func syncMergeRequestThread(ctx context.Context, c client.Client, sp objbudget.Spiller, reader scm.SCMReader,
	proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return fmt.Errorf("mirror: owner/repo for %s: %w", repo.Name, err)
	}
	var comments []scm.IssueComment
	if pl, ok := reader.(scm.PRCommentLister); ok {
		comments, err = pl.ListPRComments(ctx, owner, name, mr.Spec.Number)
	} else {
		comments, err = reader.ListIssueComments(ctx, owner, name, mr.Spec.Number)
	}
	if err != nil {
		obs.MirrorSyncTotal.WithLabelValues("MergeRequest", "error").Inc()
		return fmt.Errorf("mirror: list comments for %s: %w", MRKey(repo.Name, mr.Spec.Number), err)
	}
	incoming := mirrorComments("MergeRequest", proj, comments)
	now := metav1.Now()
	key := client.ObjectKeyFromObject(mr)
	if err := objbudget.FitMergeRequest(ctx, c, sp, key, func(cur *tatarav1alpha1.MergeRequest) {
		cur.Status.Comments = mergeComments(cur.Status.Comments, incoming, cur.Status.CommentsRetainedFrom)
		cur.Status.LastSyncedAt = &now
		meta.SetStatusCondition(&cur.Status.Conditions, syncedCondition())
	}); err != nil {
		obs.MirrorSyncTotal.WithLabelValues("MergeRequest", "error").Inc()
		return fmt.Errorf("mirror: sync mergerequest thread %s: %w", key.Name, err)
	}
	obs.MirrorSyncTotal.WithLabelValues("MergeRequest", "ok").Inc()
	log.FromContext(ctx).Info("mirror: synced mergerequest thread",
		"action", "mirror_sync_thread", "kind", "MergeRequest", "resource_id", key.Name,
		"repo", repo.Name, "number", mr.Spec.Number, "comments", len(incoming))
	return nil
}

// SyncIssueOnDemand re-reads ONE issue's thread NOW - one forge read, off
// cadence. It is run whenever a NON-BOT pendingEvent arrives on a parked Task
// (fix M11), and it is NOT an optimisation.
//
// The C.6 grammar re-evaluation that releases parked(identity-unverified) (F.6)
// needs the approving comment IN THE MIRROR, with its ExternalID: clause 3d
// enforces single-use evidence against it, and TaskEvent carries no externalId.
// The parked cadence is DAILY. Without this sync the grammar re-runs against a
// thread that does not contain the comment that triggered it, and silently fails
// - restoring the exact 7-day dead end the redesign removes.
//
// issueKey is the A.3 index key ("<repositoryRef>#<number>"), so the lookup is
// an indexed field lookup, never a hashed name.
func SyncIssueOnDemand(ctx context.Context, c client.Client, sp objbudget.Spiller, reader scm.SCMReader,
	proj *tatarav1alpha1.Project, issueKey string) error {
	var list tatarav1alpha1.IssueList
	if err := c.List(ctx, &list, client.InNamespace(proj.Namespace),
		client.MatchingFields{IssueKeyIndex: issueKey}); err != nil {
		return fmt.Errorf("mirror: lookup issueKey %q: %w", issueKey, err)
	}
	if len(list.Items) == 0 {
		return fmt.Errorf("mirror: no Issue CR for issueKey %q", issueKey)
	}
	iss := &list.Items[0]

	var repo tatarav1alpha1.Repository
	if err := c.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: iss.Spec.RepositoryRef}, &repo); err != nil {
		return fmt.Errorf("mirror: get repository %s: %w", iss.Spec.RepositoryRef, err)
	}
	return syncIssueThread(ctx, c, sp, reader, proj, &repo, iss)
}

// SyncMergeRequestOnDemand re-reads ONE MergeRequest NOW - its thread from the
// forge and the caller-supplied LIVE head - and converges the mirror, off
// cadence. It is the MergeRequest counterpart of SyncIssueOnDemand, run from
// /outcome when a review's reported head moved: "pull the new commits
// underneath" so the agent's next scm_read(kind=mr) sees the live head and
// re-reviews it, instead of re-submitting the stale sha into a permanent 409
// loop while the mirror waits out the hourly sweep.
//
// liveHeadSHA is the head the CALLER already read LIVE (GetPRHead): the
// SCMReader interface carries no head read, and re-reading it here would be a
// second forge call for a value the caller holds. The head is stamped in its own
// write so it converges even if the thread re-read failed - a thread error is
// returned for the caller to log, but the load-bearing head is already durable.
func SyncMergeRequestOnDemand(ctx context.Context, c client.Client, sp objbudget.Spiller, reader scm.SCMReader,
	proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, liveHeadSHA string) error {
	threadErr := syncMergeRequestThread(ctx, c, sp, reader, proj, repo, mr)
	now := metav1.Now()
	key := client.ObjectKeyFromObject(mr)
	if err := objbudget.FitMergeRequest(ctx, c, sp, key, func(cur *tatarav1alpha1.MergeRequest) {
		cur.Status.HeadSHA = liveHeadSHA
		cur.Status.LastSyncedAt = &now
		meta.SetStatusCondition(&cur.Status.Conditions, syncedCondition())
	}); err != nil {
		obs.MirrorSyncTotal.WithLabelValues("MergeRequest", "error").Inc()
		return fmt.Errorf("mirror: stamp live head on %s: %w", key.Name, err)
	}
	log.FromContext(ctx).Info("mirror: synced mergerequest to live head on demand",
		"action", "mirror_sync_ondemand", "kind", "MergeRequest", "resource_id", key.Name,
		"repo", repo.Name, "number", mr.Spec.Number, "head_sha", liveHeadSHA)
	return threadErr
}

// mirrorOwnerTask resolves the controller-owning Task of an artifact, which is
// what MirrorCadence keys on. A missing owner (an artifact the sweep has not
// minted a Task for yet, or whose Task the reaper just collected) is not an
// error: it syncs at the ACTIVE cadence until the sweep re-owns it.
func mirrorOwnerTask(ctx context.Context, c client.Client, obj client.Object) *tatarav1alpha1.Task {
	name, ok := own.ControllerOwner(obj)
	if !ok {
		return nil
	}
	var task tatarav1alpha1.Task
	if err := c.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}, &task); err != nil {
		return nil
	}
	return &task
}

// mirrorSyncDue reports whether lastSyncedAt is older than the cadence.
func mirrorSyncDue(lastSyncedAt *metav1.Time, cadence time.Duration, now time.Time) bool {
	return lastSyncedAt == nil || now.Sub(lastSyncedAt.Time) >= cadence
}

// mirrorSCMToken reads the project's SCM token. It is the same secret every
// other SCM path in the operator reads; the mirror reconcilers need their own
// accessor because they are not TaskReconciler methods.
func mirrorSCMToken(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project) (string, error) {
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Spec.ScmSecretRef}, &sec); err != nil {
		return "", fmt.Errorf("mirror: get scm secret %s: %w", proj.Spec.ScmSecretRef, err)
	}
	return string(sec.Data["token"]), nil
}
