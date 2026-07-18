package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// IssueName returns the CR name for an Issue: iss-<repositoryRef>-<number>.
// repoRef is the Repository CR name (already RFC-1123), never the
// owner/repo slug.
func IssueName(repoRef string, number int) string {
	return fmt.Sprintf("iss-%s-%d", repoRef, number)
}

// IssueSpec defines the desired state of an Issue.
type IssueSpec struct {
	RepositoryRef string `json:"repositoryRef"`
	// +kubebuilder:validation:Minimum=1
	Number     int    `json:"number"`
	URL        string `json:"url"`
	ProjectRef string `json:"projectRef"`
	// ProposalBodyHash is the auto-approve INTEGRITY ANCHOR for a tatara-proposed
	// issue: ComputeProposalContentHash of the issue body as FILED, written ONCE
	// by the operator at mintIssueCR time. It lives in Spec, which the mirror
	// never writes, so nothing SCM-side can forge it. autoApproveApplies refuses
	// unless the current mirrored Status.Body still hashes to this value - so a
	// forge-side body edit (scope change, marker rewrite) cannot auto-approve
	// edited scope. Empty on non-proposal issues and on proposals filed by an
	// older build (fail-closed: no anchor => no auto-approve).
	// +optional
	ProposalBodyHash string `json:"proposalBodyHash,omitempty"`
}

// Comment is one comment mirrored from the forge onto an Issue or
// MergeRequest.
type Comment struct {
	// ExternalID is the provider's comment id as a STRING (GitHub int64,
	// GitLab note id; the two disagree on width).
	ExternalID string `json:"externalId"`
	Author     string `json:"author"`
	// Body is TRUNCATED AT INGEST to 8192 bytes (fix E3). GitHub allows 65,536-
	// char bodies: 25 max-size comments = 1.6 MB = over the etcd ceiling. A 64 KB
	// comment is not prompt-useful anyway.
	// +kubebuilder:validation:MaxLength=8192
	Body      string      `json:"body"`
	CreatedAt metav1.Time `json:"createdAt"`
	// IsBot is true when Author == Project.spec.scm.botLogin. It is the
	// STRUCTURAL bot exclusion relied on by the approval grammar (C.6) and by
	// the pendingEvents enqueue filter (E.3).
	IsBot bool `json:"isBot,omitempty"`
	// Truncated is true when the ingest cut Body at 8192 bytes. The bundle
	// renders truncated="true" on the <comment> element so the agent knows the
	// text is partial and can pull the full body from the forge if it matters.
	Truncated bool `json:"truncated,omitempty"`

	// --- Inline review-comment fields (fix H11). MANDATORY, not decorative. ---
	// v3 had none of these, while C.2.11's response shape, E.2's golden bundle,
	// and mr_write(action=reply)'s REQUIRED in_reply_to all consume them - and
	// after fix C1 they are served FROM THIS MIRROR. Without them an inline review
	// comment cannot round-trip, which means the review->fix loop cannot work at
	// all: A.2 calls inline findings "this platform's NORMAL output".

	// Path is the file an inline review comment is anchored to. Empty for a
	// plain issue/MR comment.
	// +optional
	Path string `json:"path,omitempty"`
	// Line is the line an inline review comment is anchored to. Zero when unset.
	// +optional
	Line int `json:"line,omitempty"`
	// InReplyTo is the ExternalID of the review comment this one replies to. It
	// is the value mr_write(action=reply) takes as in_reply_to.
	// +optional
	InReplyTo string `json:"inReplyTo,omitempty"`
	// ReviewRound is the review round this comment was posted in, when the
	// OPERATOR posted it (fix M8 - v5's C.5.3 used this field and never declared
	// it). Zero for every comment the operator did not author. It is the MIRROR
	// half of the idempotency story; the FORGE half is the body marker (C.5.3).
	// +optional
	ReviewRound int `json:"reviewRound,omitempty"`
}

// PendingReview is the DURABLE INTENT to post a review, persisted BEFORE any
// forge call (fix H3-5, fields added by fix M8). The MergeRequest reconciler -
// not the HTTP handler - drains it. See C.5.3.
type PendingReview struct {
	// There is NO Event field (fix M9): the event is ALWAYS "COMMENT" (C.5.1b),
	// so it is a constant in the implementation, not data on the wire.
	// +kubebuilder:validation:MaxLength=16384
	Body string `json:"body"`
	// +optional
	// +kubebuilder:validation:MaxItems=30
	Findings []ReviewFinding `json:"findings,omitempty"`
	// SHA is the head the review was made against (== MergeRequest.status.reviewedSHA).
	SHA string `json:"sha"`
	// Round is the idempotency key. It appears BOTH in the forge marker
	// (<!-- tatara-review round=N sha=... -->) and on every Comment this round
	// produces (Comment.ReviewRound), so a crash between the forge post and the
	// mirror append cannot double-post.
	Round int `json:"round"`
}

// ReviewFinding is one inline review comment. MaxLength is load-bearing: 30
// findings x an unbounded body is an A.7 byte-budget input the guard CANNOT
// evict (it is spec-adjacent intent, not an evictable comment) - fix M8.
type ReviewFinding struct {
	Path string `json:"path"`
	// +kubebuilder:validation:Minimum=1
	Line int `json:"line"`
	// +kubebuilder:validation:MaxLength=8192
	Body string `json:"body"`
	// +kubebuilder:validation:Enum=critical;high;medium;low
	Severity string `json:"severity"`
}

// PendingComment is the DURABLE INTENT to post one comment/reply (fix M8). Same
// contract as PendingReview: persisted first, posted by a reconciler, cleared on
// a verified append. RequestID is the client-supplied idempotency key.
type PendingComment struct {
	RequestID string `json:"requestId"`
	// +kubebuilder:validation:Enum=comment;reply
	Action string `json:"action"`
	// +kubebuilder:validation:MaxLength=16384
	Body string `json:"body"`
	// +optional
	InReplyTo string `json:"inReplyTo,omitempty"`
}

// ApprovalEvidence is the single-use, auditable record of the ONE maintainer
// comment the operator verified before setting Status=approved (fix 15). It is
// consumed once: a later approval must cite a NEWER comment (a replayed
// evidence id is refused).
type ApprovalEvidence struct {
	Login     string      `json:"login"`     // verified maintainer, never the bot
	CommentID string      `json:"commentId"` // the Comment.ExternalID whose TEXT matched
	CreatedAt metav1.Time `json:"createdAt"`
	Phrase    string      `json:"phrase"` // the matched approvalPhrases entry
	// Auto is the autoApproveTataraProposals path; Login is then the sentinel
	// "<tatara:auto>" and CommentID is empty.
	Auto bool `json:"auto,omitempty"`
}

// IssueStatus defines the observed state of an Issue.
type IssueStatus struct {
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Author string `json:"author,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	Body string `json:"body,omitempty"`
	// +optional
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`
	// +optional
	UpdatedAt *metav1.Time `json:"updatedAt,omitempty"`
	// State is SCM truth.
	// +kubebuilder:validation:Enum=open;closed
	// +optional
	State string `json:"state,omitempty"`
	// Status is the platform's decision state. OPERATOR-OWNED: no MCP tool and no
	// agent-reachable REST endpoint writes it.
	//
	// The enum member "brainstormed" is REMOVED (fix M23): no F.3 transition ever
	// set it, and an enum member nothing writes is a lie in the schema.
	//
	// APPROVAL IS COMMENT-ONLY (fix M23). v3 kept a "only the webhook may drive a
	// label -> status transition" guard while A.6 deleted the entire label
	// vocabulary it guarded (approvalLabel / ideaLabel / rejectedLabel) - a
	// guarded path with nothing on it. There is NO label -> status path at all
	// now. Labels are a one-way PROJECTION of this field (C.6).
	// +kubebuilder:validation:Enum=new;approved;rejected;done
	// +optional
	Status string `json:"status,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=50
	Labels []string `json:"labels,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=200
	Comments []Comment `json:"comments,omitempty"`
	// CommentCount = len(Comments) + SpilledComments. The COMMENTS printcolumn
	// needs a scalar (kubebuilder cannot count a list).
	// +optional
	CommentCount int `json:"commentCount,omitempty"`
	// SpilledComments / SpilledCommentsRefs: oldest comments evicted to
	// tatara-memory by the A.7 byte guard. SpilledCommentsRefs ACCUMULATES, one
	// track_id per spill batch (fix M19): a scalar ref silently orphaned every
	// earlier batch on the second spill.
	// +optional
	SpilledComments int `json:"spilledComments,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=50
	SpilledCommentsRefs []string `json:"spilledCommentsRefs,omitempty"`
	// Approval is the single-use evidence for Status=approved. Nil means NO
	// verified approval: the operator fails CLOSED.
	// +optional
	Approval *ApprovalEvidence `json:"approval,omitempty"`
	// CommentsRetainedFrom is the eviction WATERMARK (fix M18): the CreatedAt of
	// the oldest comment still held in .comments. The mirror sync ingests ONLY
	// comments newer than it. Without this, an evicted comment is re-fetched by
	// the very next sweep (its ExternalID is no longer in the CR, so the dedup
	// key is gone) and re-evicted by the next fitForWrite: an evict/re-fetch/
	// re-evict loop that writes a duplicate spill record every hour, forever.
	// +optional
	CommentsRetainedFrom *metav1.Time `json:"commentsRetainedFrom,omitempty"`
	// PendingComments are durable comment intents (fix M8), drained by the Issue
	// reconciler. issue_write(action=comment|edit|close) writes here and returns;
	// issue_write(action=create) is SYNCHRONOUS (C.2.12) because the agent needs
	// the issue NUMBER back.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	PendingComments []PendingComment `json:"pendingComments,omitempty"`
	// RefireCount counts suppressed refires of this incident's rule while the
	// tracker stays open (A4). Incremented on EVERY suppressed refire; the comment
	// is separately rate-limited by LastRefireCommentAt.
	// +optional
	RefireCount int `json:"refireCount,omitempty"`
	// LastRefireCommentAt is when the last coalesced refire comment was enqueued.
	// Skips enqueue while now-LastRefireCommentAt < IncidentRefireCommentCooldown.
	// +optional
	LastRefireCommentAt *metav1.Time `json:"lastRefireCommentAt,omitempty"`
	// +optional
	LastSyncedAt *metav1.Time `json:"lastSyncedAt,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=iss
// +kubebuilder:printcolumn:name="Task",type=string,JSONPath=`.metadata.ownerReferences[?(@.controller==true)].name`
// +kubebuilder:printcolumn:name="Repo",type=string,JSONPath=`.spec.repositoryRef`
// +kubebuilder:printcolumn:name="Num",type=integer,JSONPath=`.spec.number`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.status`
// +kubebuilder:printcolumn:name="Comments",type=integer,JSONPath=`.status.commentCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Issue is a mirror of a forge issue, owned by the Task that is working it.
type Issue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IssueSpec   `json:"spec,omitempty"`
	Status IssueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// IssueList contains a list of Issue.
type IssueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Issue `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &Issue{}, &IssueList{})
		return nil
	})
}
