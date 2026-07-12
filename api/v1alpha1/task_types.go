package v1alpha1

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SystemicGroup describes the systemic-improvement group a lead issue owns.
type SystemicGroup struct {
	SystemicID       string   `json:"systemicId"`
	SameRepoSiblings []int    `json:"sameRepoSiblings,omitempty"` // sibling issue numbers in THIS repo, closed by the lead PR
	CrossRepo        []string `json:"crossRepo,omitempty"`        // "owner/repo#N - title" references, context only
}

// ProposedIssueSpec is a tatara-proposed issue awaiting human approval.
type ProposedIssueSpec struct {
	RepositoryRef string `json:"repositoryRef"`
	Title         string `json:"title"`
	Body          string `json:"body"`
	// +kubebuilder:validation:Enum=bug;improvement
	Kind string `json:"kind"`
	// SystemicID correlates one of several issues opened for a single systemic
	// improvement. When set, createProposal stamps label tatara/systemic-<id>
	// and a sibling footer; the group counts as one against maxOpenProposals.
	// +optional
	SystemicID string `json:"systemicId,omitempty"`
	// Incident is true when this proposal was filed by an incident-investigation
	// agent; createProposal then adds the incident label to the tracker issue.
	// +optional
	Incident bool `json:"incident,omitempty"`
	// AlertGroup is the per-alert-group dedup identity of the incident that filed
	// this proposal: the Spec.DedupKey of the in-flight incident Task, falling
	// back to its descriptive AlertRule name. createProposal dedups future
	// incident proposals by it (matching another incident Task's DedupKey and
	// its recorded tracked issue), so a recurring alert tracks onto its existing
	// open issue instead of spawning a near-duplicate. Empty for non-incident
	// proposals.
	// +optional
	AlertGroup string `json:"alertGroup,omitempty"`
}

// Suggestion is one inline code suggestion on a PR/MR.
type Suggestion struct {
	Path string `json:"path"`
	// +kubebuilder:validation:Minimum=1
	Line int    `json:"line"`
	Body string `json:"body"`
}

// SemverAssignment is one per-MR push-CD level the review agent assigns on
// approval, so the release tag can be cut for EVERY MR in the stream - including
// human/maintainer MRs that carry no bot change_significance (the review approve
// is their ONLY stamping opportunity). Repo is the "owner/repo" slug (matches
// WorkItemRef.Repo); Number is the PR/MR number in that repo.
type SemverAssignment struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	// +kubebuilder:validation:Enum=major;minor;patch
	Level string `json:"level"`
}

// ReviewVerdict is the agent's review decision for a human-authored PR/MR.
type ReviewVerdict struct {
	// +kubebuilder:validation:Enum=approve;request_changes;comment
	Decision string `json:"decision"`
	// +optional
	Body string `json:"body,omitempty"`
	// +optional
	Suggestions []Suggestion `json:"suggestions,omitempty"`
	// Semver is the per-MR push-CD level the review agent assigns on approval so
	// the release tag can be cut for EVERY MR in the stream (human MRs otherwise
	// carry no change_significance -> cd-release refuses to tag). Applied
	// best-effort in the approve writeback; an existing semver:* label on a member
	// MR is respected (a deliberate human semver is authoritative).
	// +optional
	Semver []SemverAssignment `json:"semver,omitempty"`
}

// PROutcome is the agent's outcome for a tatara-authored PR/MR.
type PROutcome struct {
	// +kubebuilder:validation:Enum=merge;close
	Action string `json:"action"`
	// +optional
	Reason string `json:"reason,omitempty"`
}

// IssueOutcome is the agent's outcome for an issue-triage task.
type IssueOutcome struct {
	// +kubebuilder:validation:Enum=implement;close;discuss
	Action string `json:"action"`
	// +optional
	Comment string `json:"comment,omitempty"` // required when Action==close or discuss
	// +optional
	Plan string `json:"plan,omitempty"` // short description of what will be implemented; posted as an implementation-start message when Action==implement
	// Locked declares, when Action==implement, that the clarify agent found NO
	// open questions and every decision is settled - the issue is ready for
	// full-scope implementation the moment a maintainer approves it. Wired
	// through to Status.ImplementationLocked on handoff (item Request C/d:
	// "implementation locked" + approval fan-out). Ignored when Action != implement.
	// +optional
	Locked bool `json:"locked,omitempty"`
}

// ImplementOutcome is the agent's declared outcome for an implement task when
// it opens no PR (e.g. a deliberate refusal). Mirrors IssueOutcome.
type ImplementOutcome struct {
	// +kubebuilder:validation:Enum=declined;already_done
	Action string `json:"action"`
	Reason string `json:"reason"` // required; why no implementation
}

// BrainstormOutcome is the agent's declared outcome for a brainstorm task when
// it files no proposal (a deliberate early-exit). Mirrors ImplementOutcome.
type BrainstormOutcome struct {
	// +kubebuilder:validation:Enum=none
	Action string `json:"action"`
	Reason string `json:"reason"` // required; why nothing was proposed
}

// ChangeSummary holds the scope report submitted by the agent at the end of an
// Implement run via the change_summary MCP tool.
type ChangeSummary struct {
	// +optional
	PRTitle string `json:"prTitle,omitempty"`
	// +optional
	PRBody string `json:"prBody,omitempty"`
	// +optional
	DeliveredScope string `json:"deliveredScope,omitempty"`
	// RemainingScope, when non-empty, means the implementation is INCOMPLETE:
	// the operator hard-fails the Task (Phase=Failed, reason=
	// IncompleteImplementation) rather than opening a follow-up issue.
	// Agents must implement the full scope in one PR or call
	// decline_implementation instead. Item Request C (full-scope-or-
	// decline); no follow-up issues are ever filed by the operator.
	// +optional
	RemainingScope string `json:"remainingScope,omitempty"`
	// +optional
	MostProblematic string `json:"mostProblematic,omitempty"` // most problematic part of the change; from the cli most_problematic field
	// Significance is the agent's declared change significance, the lever the
	// push-CD cascade uses to cut the next semver tag (major resets minor+patch,
	// minor resets patch, patch increments). REQUIRED on the change_summary MCP
	// tool and re-validated at the REST /change-summary endpoint (D2): a compliant
	// agent that summarizes its change cannot omit it. Enforcement lives at those
	// two layers, NOT in writeback - writeBackOpenChange still opens the PR when
	// this is empty (a change with no change_summary at all keeps the legacy
	// close+Done path, pushCDEligible=false), but applySemverAutoMerge stamps the
	// semver:<level> label and enables native auto-merge only when it is present.
	// An empty value therefore opens an unlabeled, non-cascading PR (logged WARN
	// at writeback so the legacy path stays visible).
	// +kubebuilder:validation:Enum=major;minor;patch
	// +optional
	Significance string `json:"significance,omitempty"`
}

// TaskSource records the SCM work-item that originated a webhook-born Task.
type TaskSource struct {
	// +kubebuilder:validation:Enum=github;gitlab
	Provider string `json:"provider"`
	IssueRef string `json:"issueRef"`
	// +optional
	URL string `json:"url,omitempty"`
	// +optional
	AuthorLogin string `json:"authorLogin,omitempty"`
	// +optional
	IsPR bool `json:"isPR,omitempty"`
	// +optional
	Number int `json:"number,omitempty"`
	// HeadSHA is the PR/MR head commit SHA captured at enqueue. It seeds the
	// review Task's role:reviewed ledger entry so same-head re-review dedup works
	// on the very next scan cycle, without waiting for the cron backstop to fill
	// it. Empty for issues.
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`
	// Title is the originating issue/PR/MR title, captured at enqueue. Feeds the
	// branch slug (TaskBranch) and the no-agent PR-title fallback.
	// +optional
	Title string `json:"title,omitempty"`
	// DedupNumber is the linked-issue number for bot-PR tasks. When a bot MR
	// carries "Closes #N" in its body, this field holds N so the dedup logic can
	// match the task against the issue slot (not the PR number). Zero means the
	// task targets the item identified by Number (the PR/issue number itself).
	// +optional
	DedupNumber int `json:"dedupNumber,omitempty"`
}

// The 7-kind redesign makes every agent kind project-scoped EXCEPT
// documentation. Three of the seven (implement, review, clarify) are
// project-scoped umbrellas that nonetheless still carry a repo ref for stored /
// legacy CRs and (implement) open PRs, so they are deliberately UNCONSTRAINED
// here: the validator accepts either an empty or a non-empty RepositoryRef for
// them, and IsProjectScopedKind returns false so the writeback project-scoped
// fence never short-circuits implement's PR path.

// repoScopedKinds are task kinds that require a non-empty RepositoryRef.
// documentation is the ONE repo-scoped agent kind; the rest are the retired
// legacy kinds, kept here so a stored repo-scoped legacy Task still validates.
var repoScopedKinds = map[string]bool{
	"documentation":  true,
	"selfImprove":    true,
	"triageIssue":    true,
	"issueLifecycle": true,
}

// projectScopedKinds are task kinds that must have an empty RepositoryRef and
// never open a PR/MR (IsProjectScopedKind true). implement/review/clarify are
// project-scoped umbrellas but are NOT in this map (they are unconstrained; see
// the note above).
var projectScopedKinds = map[string]bool{
	"brainstorm":  true,
	"healthCheck": true,
	"incident":    true,
	"refine":      true,
}

// unconstrainedKinds are the umbrella agent kinds that validate with either an
// empty or a non-empty RepositoryRef. They are known kinds (IsKnownKind true)
// but neither repo- nor project-scoped for validation purposes.
var unconstrainedKinds = map[string]bool{
	"implement": true,
	"review":    true,
	"clarify":   true,
}

// IsProjectScopedKind reports whether a task kind is project-scoped (operates on
// the whole Project, carries an empty RepositoryRef, and never opens a PR/MR).
// implement/review/clarify are umbrella kinds but return false here so the
// writeback fence does not stop implement from opening PRs.
func IsProjectScopedKind(kind string) bool {
	return projectScopedKinds[kind]
}

// IsKnownKind reports whether kind is a valid Task kind (any of the scoped,
// project-scoped, or unconstrained sets). Used by the QueuedEvent validator.
func IsKnownKind(kind string) bool {
	return repoScopedKinds[kind] || projectScopedKinds[kind] || unconstrainedKinds[kind]
}

// ValidateTaskSpec validates the RepositoryRef contract for a TaskSpec:
//   - repo-scoped kinds require a non-empty RepositoryRef.
//   - project-scoped kinds require an empty RepositoryRef.
//
// Returns nil when valid. The CRD schema cannot express this kind-conditional
// rule (a field required for some kinds and forbidden for others), so the
// TaskReconciler calls this as a reconcile guard and terminates Tasks that
// violate the contract.
func ValidateTaskSpec(spec TaskSpec) error {
	kind := spec.Kind
	if kind == "" {
		kind = "implement" // matches +kubebuilder:default="implement"
	}
	if repoScopedKinds[kind] && spec.RepositoryRef == "" {
		return fmt.Errorf("task kind %q requires a non-empty repositoryRef", kind)
	}
	if projectScopedKinds[kind] && spec.RepositoryRef != "" {
		return fmt.Errorf("task kind %q must have an empty repositoryRef (project-scoped); got %q", kind, spec.RepositoryRef)
	}
	return nil
}

// infraIncidentKeywords are lower-case substrings that mark a Grafana alert as
// targeting the project's core memory/storage infrastructure: the memory stack
// (LightRAG retrieval surface, Postgres/CNPG, Neo4j) or its backing storage
// (CephFS PVCs, WAL, quorum). Matched case-insensitively against a Task's
// AlertRule (the alertname label). Kept intentionally narrow so only genuine
// infra alerts qualify for the admission-gate exemption below.
var infraIncidentKeywords = []string{
	"memory",
	"lightrag",
	"postgres",
	"cnpg",
	"neo4j",
	"pvc",
	"cephfs",
	"quorum",
	"wal",
}

// AlertTargetsCoreInfra reports whether a Grafana alert-rule name implicates the
// project's core memory/storage infrastructure, by case-insensitive substring
// match against infraIncidentKeywords.
func AlertTargetsCoreInfra(alertRule string) bool {
	lower := strings.ToLower(alertRule)
	for _, kw := range infraIncidentKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// InfraIncidentExempt reports whether a Task is an incident-kind Task whose alert
// targets the core memory/storage infrastructure, and is therefore exempt from
// the project memory-readiness admission gate.
//
// Rationale (tatara-operator#236): when the memory stack is down, every Task is
// gated on Memory.Phase == Ready. Applying that gate to the very incident Task
// created to investigate the memory outage is a deadlock: the self-heal agent
// can never run, never opens a tracker issue, and cannot escalate. Incident
// agents investigate live via Grafana (k8s/CNPG/storage) and do not need the
// memory graph, so it is safe to let an infra-incident run with memory down.
// Only incident Tasks qualify; all normal work keeps the gate.
func InfraIncidentExempt(spec TaskSpec) bool {
	return spec.Kind == "incident" && AlertTargetsCoreInfra(spec.AlertRule)
}

// TaskSpec defines the desired state of a Task.
type TaskSpec struct {
	ProjectRef string `json:"projectRef"`
	// +optional
	RepositoryRef string `json:"repositoryRef,omitempty"`
	Goal          string `json:"goal"`
	// +optional
	Source *TaskSource `json:"source,omitempty"`
	// +optional
	MaxTurns int `json:"maxTurns,omitempty"`
	// Kind selects the agent behavior. The 7-kind model is
	// brainstorm;incident;clarify;implement;review;documentation;refine. The
	// strings selfImprove;triageIssue;healthCheck;issueLifecycle are RETIRED as
	// agent kinds - inert, retained in the enum only so already-persisted terminal
	// CRs still deserialize and read; no code path creates them anymore.
	// +kubebuilder:validation:Enum=implement;review;selfImprove;triageIssue;brainstorm;issueLifecycle;incident;healthCheck;refine;documentation;clarify
	// +kubebuilder:default="implement"
	// +optional
	Kind string `json:"kind,omitempty"`
	// ApprovalRequired is reserved for future use; no production code path reads
	// this field for any gating decision. Approval is driven by the SCM
	// conversation flow. Do not set this field expecting behavior - it has none.
	// +optional
	ApprovalRequired bool `json:"approvalRequired,omitempty"`
	// +optional
	ProposedIssue *ProposedIssueSpec `json:"proposedIssue,omitempty"`
	// ReposInScope is the optional declarative list of Project Repository CR
	// names this Task is expected to change. When set, the implement prompt tells
	// the agent the issue spans these repos and writeback posts a WARNING comment
	// for any in-scope repo whose branch produced no commits, instead of skipping
	// it silently. Absent/empty = single-repo behavior (primary repo only), so
	// existing Tasks are unaffected.
	// +optional
	ReposInScope []string `json:"reposInScope,omitempty"`
	// SystemicGroup, when set, marks this Task as the lead for a brainstorm
	// systemic group: it resolves SameRepoSiblings in one combined PR and is
	// aware of CrossRepo siblings (reference only).
	// +optional
	SystemicGroup *SystemicGroup `json:"systemicGroup,omitempty"`
	// AlertRule names the Grafana alert rule that produced an incident Task
	// (commonLabels.alertname, falling back to groupKey). Descriptive only.
	// +optional
	AlertRule string `json:"alertRule,omitempty"`
	// DedupKey is the dedup identity for an incident Task: the alert-group hash
	// (sha256(groupKey)[:16]) that ties re-fires of the same alert to the same
	// tracked issue. Replaces the former tatara.dev/alert-group Task label and
	// tatara/alert-group-<hash> issue label - dedup lookups List incident Tasks
	// and filter by this field in Go instead of a label selector. Empty for
	// non-incident Tasks.
	// +optional
	DedupKey string `json:"dedupKey,omitempty"`
}

// Task Phase string literals. Phases are bare strings on Status.Phase (there is
// no single central enum); these consts name the ones the push-CD cascade and
// terminal/active predicates key on so callers stop hand-typing them.
const (
	PhasePlanning  = "Planning"
	PhaseRunning   = "Running"
	PhaseSucceeded = "Succeeded"
	PhaseFailed    = "Failed"
	// PhaseDeploying is the pod-less post-merge phase: the implement PR has
	// auto-merged and the operator (not an agent pod) drives the deploy cascade
	// to tatara-helmfile-applied. It is non-terminal (TaskTerminal is false) and
	// MUST be excluded from per-repo lane occupancy: no pod runs, so counting it
	// against the lane re-creates the lane-starvation trap
	// (operator-laneoccupancy-starves-recovery-2026-06-15). It re-acquires a lane
	// only to spawn a fix agent.
	PhaseDeploying = "Deploying"
	// DeployStateDeploying is the issueLifecycle counterpart of PhaseDeploying:
	// the durable per-issue Task carries it in Status.DeployState while the
	// operator drives the post-merge deploy cascade. It is set together with
	// Status.Phase=PhaseDeploying. It is NOT a terminal lifecycle state (TaskTerminal
	// stays false) so conversation-GC / reaper / lane logic treat it as live.
	DeployStateDeploying = "Deploying"
)

// TaskTerminal reports whether t has reached a terminal state, accounting for
// the dual Phase / DeployState design: issueLifecycle tasks leave Phase
// empty for their whole life and signal completion via DeployState. Any
// predicate that must treat finished lifecycle tasks as terminal MUST call
// this helper instead of testing Phase alone.
//
// PhaseDeploying is deliberately NOT terminal: a Task in Deploying is alive but
// pod-less (the operator polls the cascade), so conversation-GC / reaper / lane
// logic must treat it as live-but-podless, not finished.
func TaskTerminal(t *Task) bool {
	if t.Status.Phase == PhaseSucceeded || t.Status.Phase == PhaseFailed {
		return true
	}
	ls := t.Status.DeployState
	return ls == "Done" || ls == "Stopped" || ls == "Parked"
}

// TaskDeploying reports whether t is in the pod-less Deploying phase. Lane
// occupancy, reaper, and conversation-GC use this to treat it as a live work
// item that holds no execution lane (no agent pod runs during Deploying).
func TaskDeploying(t *Task) bool {
	return t.Status.Phase == PhaseDeploying
}

// IsRecoverableGiveup reports whether a Parked reason represents an
// implementation that gave up and may be re-rolled (vs a deliberate decline).
// merge-timeout (parkUmbrellaMergeTimeout's mergeParkReason) is symmetric with
// deploy-timeout: both are auto-recoverable stalls, not a human decline, so both
// must be aged-out by recoverOrphans and spared by the reaper the same way.
func IsRecoverableGiveup(reason string) bool {
	switch reason {
	case "implement-failed", "maxIterations", "refused-no-explanation", "deadline", "deploy-timeout", "merge-timeout":
		return true
	default:
		return false
	}
}

// TaskStatus defines the observed state of a Task.
type TaskStatus struct {
	// +kubebuilder:validation:Enum=Planning;Running;Succeeded;Failed;Deploying
	// NOTE: Pending and AwaitingApproval are intentionally absent: no code path
	// ever writes them (approval is now driven by the SCM conversation flow and
	// projected onto labels, not a Phase transition). They are removed here to
	// keep the CRD enum honest and prevent confusion with DeployState.
	// Deploying is the pod-less post-merge deploy-supervision phase (PhaseDeploying).
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	TurnsCompleted int `json:"turnsCompleted,omitempty"`
	// +optional
	PrURL string `json:"prURL,omitempty"`
	// +optional
	ResultSummary string `json:"resultSummary,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	DiscoveredIssues []string `json:"discoveredIssues,omitempty"`
	// +optional
	ReviewVerdict *ReviewVerdict `json:"reviewVerdict,omitempty"`
	// +optional
	PROutcome *PROutcome `json:"prOutcome,omitempty"`
	// +optional
	IssueOutcome *IssueOutcome `json:"issueOutcome,omitempty"`
	// +optional
	ImplementOutcome *ImplementOutcome `json:"implementOutcome,omitempty"`
	// +optional
	BrainstormOutcome *BrainstormOutcome `json:"brainstormOutcome,omitempty"`
	// +optional
	ChangeSummary *ChangeSummary `json:"changeSummary,omitempty"`
	// FollowupIssueURL is DEPRECATED and vestigial: the operator no longer
	// opens follow-up issues (item Request C, full-scope-or-decline - a
	// non-empty ChangeSummary.RemainingScope now hard-fails the Task
	// instead). Retained on the CRD for backward compatibility with
	// existing Tasks/readers only; nothing writes to it anymore.
	// +optional
	FollowupIssueURL string `json:"followupIssueURL,omitempty"`
	// +optional
	GateEnteredAt *metav1.Time `json:"gateEnteredAt,omitempty"`

	// Lifecycle fields (issueLifecycle kind only; empty on all other kinds).

	// Deploying is the pod-less post-merge deploy-supervision lifecycle state: the
	// issueLifecycle Task's PR auto-merged + main CI (incl. the release tag-cut +
	// propagation) went green, and the operator now drives the push-CD cascade to a
	// tatara-helmfile apply. It is paired with Status.Phase=Deploying so lane
	// occupancy excludes it (no agent pod runs) and TaskTerminal keeps it live.
	// The Go field is DeployState (agent-invisible, deploy-supervisor-only) but the
	// JSON/CRD key stays lifecycleState so stored CRs still deserialize and the
	// agent-visible task_list field is unchanged. The enum keeps the front-half
	// values (Triage/Conversation/Implement/MRCI) for the drain of in-flight
	// issueLifecycle Tasks.
	// +kubebuilder:validation:Enum=Triage;Conversation;Implement;MRCI;Merge;MainCI;Deploying;Done;Stopped;Parked
	// +optional
	DeployState string `json:"lifecycleState,omitempty"`
	// +optional
	LastActivityAt *metav1.Time `json:"lastActivityAt,omitempty"`
	// +optional
	DeadlineAt *metav1.Time `json:"deadlineAt,omitempty"`
	// +optional
	HeadBranch string `json:"headBranch,omitempty"`
	// +optional
	PRNumber int `json:"prNumber,omitempty"`
	// +optional
	MergeCommitSHA string `json:"mergeCommitSHA,omitempty"`
	// MergedHeadSHA is the source-branch head commit SHA of the most recently
	// merged PR/MR. Recorded on a successful Merge and deliberately preserved
	// across clearMergedChangeState so the next MRCI cycle can detect a re-opened
	// PR that re-proposes the already-merged commits with no new fix (the
	// deterministic task branch is reused; if a post-merge re-implement does not
	// advance it, writeBackOpenChange opens a duplicate of the merged change).
	// +optional
	MergedHeadSHA string `json:"mergedHeadSHA,omitempty"`
	// ResolvedModel is the MODEL env resolved for this Task's agent pod at spawn
	// (modelForKind: per-kind override else project-wide). Stamped once at
	// pod-creation; read by the token/terminal metrics so $ is priced by the
	// model that actually ran. +optional
	ResolvedModel string `json:"resolvedModel,omitempty"`
	// +optional
	CumulativeTokens int64 `json:"cumulativeTokens,omitempty"`
	// +optional
	LastTurnInputTokens int64 `json:"lastTurnInputTokens,omitempty"`
	// CumulativeInput is the running total of uncached input tokens
	// (turnUsage.InputTokens) across all turns of this Task. +optional
	CumulativeInput int64 `json:"cumulativeInput,omitempty"`
	// CumulativeOutput is the running total of output tokens across all turns
	// of this Task. +optional
	CumulativeOutput int64 `json:"cumulativeOutput,omitempty"`
	// CumulativeCacheRead is the running total of cache-read input tokens
	// across all turns of this Task. +optional
	CumulativeCacheRead int64 `json:"cumulativeCacheRead,omitempty"`
	// CumulativeCacheCreation is the running total of cache-creation input
	// tokens across all turns of this Task. +optional
	CumulativeCacheCreation int64 `json:"cumulativeCacheCreation,omitempty"`
	// +optional
	LifecycleIterations int `json:"lifecycleIterations,omitempty"`
	// +optional
	Handover string `json:"handover,omitempty"`
	// ConversationObjectKey is the S3 object key under which the wrapper stores
	// and restores this Task's full Claude conversation transcript (issue #114).
	// Stable across lifecycle phases. Empty until conversation persistence is
	// configured and the first run has reported it (or a forked key is set for a
	// brainstorm-derived issue).
	// +optional
	ConversationObjectKey string `json:"conversationObjectKey,omitempty"`
	// SessionID is the Claude session id of the persisted conversation. The
	// operator passes it back to the next pod (as CONVERSATION_SESSION_ID) so a
	// fresh pod resumes via `claude --resume <id>` instead of starting empty.
	// +optional
	SessionID string `json:"sessionID,omitempty"`
	// ImplementContext is an optional re-entry prompt injected at the start of
	// the next Implement agent turn (e.g. CI failure details, conflict notice).
	// Cleared after the turn is submitted so a later fresh entry is clean.
	// +optional
	ImplementContext string `json:"implementContext,omitempty"`
	// ImplementEmptyRetries counts consecutive Implement runs that finished
	// with zero commits (no PR opened). Bounded retry guard: after the cap the
	// task is commented + parked with reason "implement-empty" instead of
	// silently parked as a benign no-change. Reset to 0 when a run opens a PR.
	// +optional
	ImplementEmptyRetries int `json:"implementEmptyRetries,omitempty"`
	// ImplementGiveUps counts implementation attempts that gave up for this
	// issue's durable lifecycle Task (transition Implement->Parked with a
	// recoverable reason). Bounds the auto-reroll backstop. +optional
	ImplementGiveUps int `json:"implementGiveUps,omitempty"`
	// WritebackSkip4xxAttempts counts consecutive writeback sweeps that opened
	// no PR because every project repo returned a permanent 4xx from OpenChange
	// (issue #166: the un-triageable 4xx-skip loop). Bounded loop-breaker: once
	// it reaches writebackSkip4xxCap the writeback gate stops re-sweeping the SCM
	// and records a terminal WritebackFailed condition instead of churning the
	// SCM API every reconcile. Reset to 0 when a PR opens.
	// +optional
	WritebackSkip4xxAttempts int `json:"writebackSkip4xxAttempts,omitempty"`
	// PendingComments are free-form comments queued by the agent via the
	// comment MCP tool, posted to the task's linked issue on the next
	// reconcile and then cleared. Does not change the lifecycle state.
	// +optional
	PendingComments []string `json:"pendingComments,omitempty"`
	// PendingInterjections are comment bodies queued by the webhook when a new
	// issue/MR comment arrives while an agent turn is in flight. The reconciler
	// delivers each to the live wrapper session (as mid-session user input) and
	// then clears them. Does not change the lifecycle state.
	// +optional
	PendingInterjections []string `json:"pendingInterjections,omitempty"`
	// WorkItems is the work-item ledger: every SCM artifact (issues, PRs,
	// proposals) this Task spans. Carried as the single source of truth for
	// dedup, stall recovery, and prompt-building. Seeded lazily from Spec.Source
	// on first reconcile; maintained by the operator as the agent drives actions
	// via MCP tools.
	// +optional
	WorkItems []WorkItemRef `json:"workItems,omitempty"`
	// ParkReason is the reason string passed to the last Parked transition.
	// Cleared when the Task transitions out of Parked. Carried for context and
	// observability; does NOT gate re-activation.
	// +optional
	ParkReason string `json:"parkReason,omitempty"`

	// ApprovedByMaintainer records the identity-verified fact that a HUMAN
	// MAINTAINER explicitly approved this issue for implementation, by applying
	// the approved label to it. It is set by the webhook ONLY when it observes an
	// issues.labeled{approved} event whose ACTOR is a MaintainerLogins member
	// (never the bot: a bot/agent that sets the label itself cannot self-approve).
	// It is the ONLY signal that releases a front-half Task (clarify / the
	// issueLifecycle bridge) into the autonomous implement->review->merge->deploy
	// chain: every path that would advance a front-half issue to Implement gates
	// on it (finishFrontHalf, the Conversation label readback, the trigger-label
	// jump). Empty means NO verified maintainer approval - the operator fails
	// CLOSED (parks to Conversation) rather than trusting raw label presence,
	// which an agent with SCM write could forge. Holds the approving maintainer's
	// login for audit.
	// +optional
	ApprovedByMaintainer string `json:"approvedByMaintainer,omitempty"`
	// AutoApproved is true when ApprovedByMaintainer was set by the auto-approve
	// release path (item 4a) rather than a real maintainer - the sentinel value
	// "<tatara:auto:<kind>>" is also written to ApprovedByMaintainer for audit,
	// but this bool is the fast structural check (avoids string-parsing the
	// sentinel at every consumer).
	// +optional
	AutoApproved bool `json:"autoApproved,omitempty"`

	// ImplementationLocked records that this Task's clarify conversation
	// reached a state with NO open questions and every decision locked (set
	// via issue_outcome{action=implement, locked=true} on handoff to
	// implement). It is the signal systemic-group approval fan-out
	// (filterSystemicGroupByApproval) checks for a sibling that lacks its
	// own direct maintainer approval: an approved lead's group extends to
	// every OTHER member that is implementation-locked. Item Request C/d.
	// +optional
	ImplementationLocked bool `json:"implementationLocked,omitempty"`

	// Deploy-supervision fields (PhaseDeploying only; empty otherwise). The
	// implement Task does not go terminal at PR-merge: it enters Deploying and
	// the operator drives the push-CD cascade to a tatara-helmfile apply, then
	// resolves Done + closes the originating issue.

	// DeployDeadline is the wall-clock deadline for the deploy cascade
	// (now + Project deployBudgetSeconds, single-hop override applied per
	// artifact). On exceed, the Task parks recoverable with reason deploy-timeout.
	// +optional
	DeployDeadline *metav1.Time `json:"deployDeadline,omitempty"`
	// CascadeStage tracks how far this Task's artifact has propagated toward the
	// terminal tatara-helmfile apply.
	// +kubebuilder:validation:Enum=tagged;parent-pr-open;parent-merged;helmfile-applied
	// +optional
	CascadeStage string `json:"cascadeStage,omitempty"`
	// DeployedVersion is the semver (vX.Y.Z) this Task's artifact published and is
	// driving toward the cluster.
	// +optional
	DeployedVersion string `json:"deployedVersion,omitempty"`
	// DeployArtifact is the deploy-ledger artifact identity (repo@vX.Y.Z) this
	// Task records, the key the apply-outcome sweep matches against applied pins.
	// +optional
	DeployArtifact string `json:"deployArtifact,omitempty"`
	// MergeWaitDeadline bounds a discrete-implement umbrella Task's wait for its
	// member PRs to be reviewed + merged (the pre-Deploying window). When members
	// stay unmerged past this wall clock, superviseMergedPRs parks the stream
	// recoverable with an issue comment naming the stuck member(s) (item 3). It is
	// distinct from DeployDeadline, which bounds the post-merge cascade.
	// +optional
	MergeWaitDeadline *metav1.Time `json:"mergeWaitDeadline,omitempty"`
	// ReviewResolveDeadline bounds an umbrella review's wall-clock wait for an
	// unresolvable member repo URL to become resolvable (un-enrolled member repo,
	// or a projectRepoURLBySlug List error). Stamped on the first unresolvable
	// encounter; once it elapses, writeBackReview parks the review recoverable with
	// an issue comment naming the stuck member instead of error-looping forever
	// (liveness finding #4).
	// +optional
	ReviewResolveDeadline *metav1.Time `json:"reviewResolveDeadline,omitempty"`

	// ShortDescription is the first line of Spec.Goal, truncated to ~60 chars,
	// set on reconcile so `kubectl get task` is scannable without describe.
	// +optional
	ShortDescription string `json:"shortDescription,omitempty"`

	// Subtasks is the durable rollup of every subtask (incl. the synthetic
	// order-0 planning entry), maintained as subtasks progress. See SubtaskRef.
	// +optional
	Subtasks []SubtaskRef `json:"subtasks,omitempty"`

	// IssueLinks is every issue this Task touched: DiscoveredIssues,
	// FollowupIssueURL, and issue-kind WorkItems, deduped. Full URLs where the
	// source field carries one (DiscoveredIssues/FollowupIssueURL); "repo#N"
	// refs where only a WorkItemRef exists (item 9).
	// +optional
	IssueLinks []string `json:"issueLinks,omitempty"`
	// PRLinks is every PR/MR this Task touched: PrURL and PR-kind WorkItems,
	// deduped. Same mixed URL/"repo#N" format as IssueLinks.
	// +optional
	PRLinks []string `json:"prLinks,omitempty"`
}

// SubtaskRef is a durable point-in-time snapshot of one subtask, rolled onto
// TaskStatus.Subtasks as subtasks progress. Lets kubectl and a re-entering
// agent read the full reasoning trail (including the synthetic order-0
// "Planning" entry capturing turn 0's FinalText) without listing Subtask
// objects directly (item 8).
type SubtaskRef struct {
	// Name is the Subtask object's name, empty for the synthetic order-0
	// planning entry (turn 0 has no backing Subtask object).
	// +optional
	Name  string `json:"name,omitempty"`
	Order int    `json:"order"`
	Title string `json:"title"`
	// +kubebuilder:validation:Enum=Pending;Running;Done;Failed
	Phase string `json:"phase"`
	// +optional
	Result string `json:"result,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Lifecycle",type=string,JSONPath=`.status.lifecycleState`
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.projectRef`,priority=1
// +kubebuilder:printcolumn:name="Turns",type=integer,JSONPath=`.status.turnsCompleted`
// +kubebuilder:printcolumn:name="Description",type=string,JSONPath=`.status.shortDescription`

// Task is one agent session driving a Repository toward a goal.
type Task struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskSpec   `json:"spec,omitempty"`
	Status TaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TaskList contains a list of Task.
type TaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Task `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &Task{}, &TaskList{})
		return nil
	})
}
