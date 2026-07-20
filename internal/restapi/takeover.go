package restapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
)

// mrTakeoverReq is the wire contract for POST /projects/{p}/scm/mr-takeover -
// it MUST match what tatara-cli's mr_takeover_request tool already ships:
// there is no top-level "action" field, exactly these four keys.
type mrTakeoverReq struct {
	Repo              string `json:"repo"`
	Number            int    `json:"number"`
	CommentExternalID string `json:"commentExternalId"`
	Task              string `json:"task"`
}

// takeoverTaskName is the deterministic natural-key name OP5's Minter mints
// the ONE full-lifecycle takeover Task under for (proj, repo, number).
// Delegates to controller.TakeoverTaskName rather than re-deriving the
// "takeover" kind string here, so the two packages can never drift on the
// naming scheme.
func takeoverTaskName(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) string {
	return controller.TakeoverTaskName(proj, repo, number)
}

// mrTakeover is the consumer end of the OP6 (webhook fast path) / OP12 (sweep
// convergence) comment->task pipeline: a maintainer's "take over" comment has
// already reached a review Task - which, by the time this endpoint is called,
// controller-owns the MR mirror - and the review agent judged intent and
// called mr_takeover_request. This endpoint NEVER trusts that judgment: the
// referenced comment must exist in the MR CR mirror, and its recorded author
// must be a verified MAINTAINER (never the bot, never merely a listed
// reporter - takeover is a privilege grant, not intake trust), and the caller
// Task must currently controller-own the MR. Only then does it
// flip ownership external -> tatara, mint/unpark the single full-lifecycle
// takeover Task (OP5's Minter.MintOrUnparkTakeoverTask), and move the MR
// mirror's controller ownership onto it. The stand-down/takeover announcement
// is posted by the MergeRequest reconcile drain (OP11), not here.
func (s *Server) mrTakeover(w http.ResponseWriter, r *http.Request) {
	if !authorizeCaller(w, r) {
		return
	}
	ctx := r.Context()

	var req mrTakeoverReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Repo == "" || req.Number <= 0 || req.CommentExternalID == "" {
		writeError(w, http.StatusBadRequest, "repo, number, commentExternalId required")
		return
	}

	projName := chi.URLParam(r, "p")
	proj, err := s.getProjectCR(ctx, projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	repo, err := s.repoCR(ctx, projName, req.Repo)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	task, ok := s.callerTask(w, r, taskParam(r, req.Task))
	if !ok {
		return
	}

	name := tatarav1alpha1.MergeRequestName(repo.Name, req.Number)
	var mr tatarav1alpha1.MergeRequest
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: name}, &mr); err != nil {
		writeClientErr(w, err)
		return
	}

	// Controller-ownership gate: the SAME check every other MR write
	// (mrDeferred, mrOpen) uses. A caller that lost the race - an external
	// re-push, a concurrent takeover - gets a 409, never a silent no-op.
	if ctrl, ok := own.ControllerOwner(&mr); !ok || ctrl != task.Name {
		obs.RestOwnershipRefusedTotal.WithLabelValues("mr").Inc()
		writeError(w, http.StatusConflict, "task does not own this merge request")
		return
	}
	if mr.Status.State != "open" {
		writeError(w, http.StatusConflict, "merge request is not open")
		return
	}

	// Server-side authz: the comment must exist in the mirror, and its
	// recorded author must be a verified human MAINTAINER - never the bot,
	// never an unlisted login, and NOT merely a listed reporter. Takeover
	// grants the bot push+merge agency over a human's own MR: a privilege
	// grant, gated on the maintainer tier (IsMaintainer), not the weaker
	// intake-tier trust IsTrustedAuthor/IsAllowedReporter extend to listed
	// reporters. The agent's own judgment carries no weight here.
	var cmt *tatarav1alpha1.Comment
	for i := range mr.Status.Comments {
		if mr.Status.Comments[i].ExternalID == req.CommentExternalID {
			cmt = &mr.Status.Comments[i]
			break
		}
	}
	if cmt == nil {
		writeError(w, http.StatusUnprocessableEntity, "referenced comment not found in the merge request mirror")
		return
	}
	if cmt.IsBot || cmt.Author == "" || cmt.Author == botLogin(proj) ||
		!tatarav1alpha1.IsMaintainer(proj, repo, cmt.Author) {
		writeError(w, http.StatusForbidden, "author not in maintainer set: only a project maintainer may hand this merge request over")
		return
	}

	// Idempotent no-op: ownership is already tatara, so there is nothing to
	// take over (a repeat "take over" comment after a prior takeover already
	// succeeded, or a stray one on an MR tatara has authored from the start).
	// The caller already proved (the gate above) that it IS the current
	// controller owner, so its own name is the answer.
	if mr.Status.Ownership == tatarav1alpha1.OwnershipTatara {
		s.log.InfoContext(ctx, "restapi: takeover requested on an already-tatara merge request; idempotent no-op",
			append(reqLogFields(r), "action", "mr_takeover_idempotent", "resource_id", mr.Name,
				"user", cmt.Author, "task", task.Name)...)
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ok", "repo": repo.Name, "number": req.Number, "task": task.Name,
		})
		return
	}

	sp := s.spillerForOrNil(proj)

	// Checked BEFORE the demote below: a misconfigured nil minter must fail
	// before any owner-ref write happens, never after - leaving the MR
	// demoted to no controller owner (controllerless) behind a 500 would be
	// worse than refusing up front.
	if s.minter == nil {
		s.log.ErrorContext(ctx, "restapi: takeover called with no minter configured",
			append(reqLogFields(r), "mr", mr.Name)...)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// MintOrUnparkTakeoverTask's fresh-mint path binds the takeover Task as
	// the MR's controller owner through the SAME intake funnel a review mint
	// uses (Minter.ownMergeRequest), which REFUSES to steal a controller ref
	// it does not already recognize as its own Task's. The caller Task
	// (review) is still that controller here, so the mint would hard-fail
	// with "already has controller owner" unless that flag is cleared first -
	// the same demote-before-remint step flipToExternal's own hand-back uses
	// (reMintReviewOwner), just running in the opposite direction.
	if err := controller.DemoteMRController(ctx, s.c, &mr); err != nil {
		s.log.ErrorContext(ctx, "restapi: takeover demote controller failed",
			append(reqLogFields(r), "mr", mr.Name, "error", err)...)
		obs.RestTakeoverErrorTotal.WithLabelValues("demote").Inc()
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	tk, err := s.minter.MintOrUnparkTakeoverTask(ctx, proj, repo, &mr, cmt.Author, cmt.Body, sp)
	if err != nil {
		s.log.ErrorContext(ctx, "restapi: mint/unpark takeover task failed",
			append(reqLogFields(r), "mr", mr.Name, "error", err)...)
		obs.RestTakeoverErrorTotal.WithLabelValues("mint").Inc()
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Move (or re-assert) the MR mirror's controller ownership onto the
	// takeover Task. A fresh mint above may already have done this (via
	// bindMRToTask); a re-take (an existing parked(ownership-lost) Task just
	// re-entered approved) has not - MintOrUnparkTakeoverTask never touches
	// owner refs on that branch. Both own.AddPlainOwner and
	// own.HandOverController are idempotent, so doing this unconditionally is
	// safe either way, and it is what leaves the review Task behind as a
	// plain owner (HandOverController only ever demotes a controller flag; it
	// never drops the ref).
	if err := controller.MutateOwnerRefs(ctx, s.c, &mr, func(fresh *tatarav1alpha1.MergeRequest) error {
		own.AddPlainOwner(fresh, tk)
		return own.HandOverController(fresh, task, tk)
	}); err != nil {
		s.log.ErrorContext(ctx, "restapi: move merge request ownership failed",
			append(reqLogFields(r), "mr", mr.Name, "error", err)...)
		obs.RestTakeoverErrorTotal.WithLabelValues("ownerref").Inc()
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Seed LastBotHeadSHA (and the HeadSHA mirror) from the LIVE PR head, not
	// mr.Status.HeadSHA - the mirror can lag the forge (the last webhook/sweep
	// sync may predate this exact instant). A stale seed reproduces the same
	// bug the comment below already fixed once: the very next
	// ReconcileOwnership sweep would see liveHead != LastBotHeadSHA and flip
	// this fresh takeover straight back to external. Best-effort: a transient
	// read failure must never fail the takeover itself, so it falls back to
	// the mirrored head exactly as before this fetch existed - same reasoning
	// as the discardWriter fallback at /outcome's record_bot_head (outcome.go).
	liveHead := mr.Status.HeadSHA
	if writer, token, ok := s.projectSCMWriterAndToken(&discardWriter{}, r, proj); ok {
		if live, herr := writer.GetPRHead(ctx, repo.Spec.URL, token, req.Number); herr != nil {
			s.log.WarnContext(ctx, "restapi: takeover live head read failed; falling back to the mirrored head",
				append(reqLogFields(r), "mr", mr.Name, "error", herr)...)
		} else if live != "" {
			liveHead = live
		}
	} else {
		s.log.WarnContext(ctx, "restapi: takeover scm writer/token resolution failed; falling back to the mirrored head",
			append(reqLogFields(r), "mr", mr.Name)...)
	}

	now := metav1.Now()
	key := types.NamespacedName{Namespace: s.ns, Name: mr.Name}
	// LastBotHeadSHA is set to the CURRENT (live) head at the takeover point,
	// not left at its prior (possibly empty, possibly stale) value:
	// ReconcileOwnership's drift check fires whenever ownership==tatara &&
	// liveHead != LastBotHeadSHA (internal/controller/ownership.go). A
	// never-bot-pushed MR (the headline Renovate takeover case) has
	// LastBotHeadSHA=="" - left unset here, the very next reconcile would read
	// liveHead != "" and flip this MR straight back to external, parking the
	// takeover Task before its agent ever runs. A re-take after a stand-down
	// has the same bug with a STALE old bot head. HeadSHA is refreshed to the
	// same value so the mirror does not itself go stale relative to the SHA
	// this flip is keyed on. A later real human push still moves the live
	// head off this baseline and stands the MR down correctly; the takeover
	// agent's own push refreshes this same field again at outcome accept.
	if err := objbudget.FitMergeRequest(ctx, s.c, sp, key, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.Ownership = tatarav1alpha1.OwnershipTatara
		m.Status.OwnershipReason = "takeover-requested-by:" + cmt.Author
		m.Status.OwnershipChangedAt = &now
		m.Status.LastBotHeadSHA = liveHead
		m.Status.HeadSHA = liveHead
	}); err != nil {
		s.log.ErrorContext(ctx, "restapi: record ownership flip failed",
			append(reqLogFields(r), "mr", mr.Name, "error", err)...)
		obs.RestTakeoverErrorTotal.WithLabelValues("stamp").Inc()
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	obs.OwnershipFlip("to-tatara", "takeover")
	s.log.InfoContext(ctx, "restapi: ownership flipped to tatara",
		append(reqLogFields(r), "action", "ownership_flip", "resource_id", mr.Name,
			"direction", "to-tatara", "user", cmt.Author, "task", tk.Name)...)

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "repo": repo.Name, "number": req.Number, "task": tk.Name,
	})
}
