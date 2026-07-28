package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TaskSource records the SCM work-item that originated a Task. It is the SEED
// IDENTITY the triaging stage mints the Issue CR from (F.2) and the base of the
// deterministic task branch (agent.TaskBranch). It is NOT a dedup ledger: the
// five dedup mechanisms folded into the (repo, number) natural key at the
// cutover, and the sixth (the incident alert-group hash) lives on Spec.DedupKey.
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
	// HeadSHA is the PR/MR head commit SHA captured at mint. Empty for issues.
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`
	// Title is the originating issue/PR/MR title, captured at mint. Feeds the
	// branch slug (TaskBranch) and the no-agent PR-title fallback.
	// +optional
	Title string `json:"title,omitempty"`
}

// repoScopedKinds are task kinds that require a non-empty RepositoryRef.
// documentation is the ONE repo-scoped kind.
var repoScopedKinds = map[string]bool{
	"documentation": true,
}

// projectScopedKinds are task kinds that must have an empty RepositoryRef and
// never open a PR/MR (IsProjectScopedKind true).
var projectScopedKinds = map[string]bool{
	"brainstorm": true,
	"incident":   true,
	"refine":     true,
}

// unconstrainedKinds are the umbrella origin kinds that validate with either an
// empty or a non-empty RepositoryRef: the sweep mints them with no repo, while
// a proposal-born clarify carries its proposal's repo.
var unconstrainedKinds = map[string]bool{
	"review":   true,
	"clarify":  true,
	"takeover": true,
}

// IsProjectScopedKind reports whether a task kind is project-scoped (operates on
// the whole Project, carries an empty RepositoryRef, and never opens a PR/MR).
func IsProjectScopedKind(kind string) bool {
	return projectScopedKinds[kind]
}

// IsKnownKind reports whether kind is a valid Task ORIGIN kind (any of the
// scoped, project-scoped, or unconstrained sets). Used by the QueuedEvent
// validator. It is NOT the agent-kind vocabulary (that is Status.AgentKind,
// driven by the F.2 stage table).
func IsKnownKind(kind string) bool {
	return repoScopedKinds[kind] || projectScopedKinds[kind] || unconstrainedKinds[kind]
}

// ValidateTaskSpec validates the RepositoryRef contract for a TaskSpec:
//   - repo-scoped kinds require a non-empty RepositoryRef.
//   - project-scoped kinds require an empty RepositoryRef.
//
// Returns nil when valid. The CRD schema cannot express this kind-conditional
// rule (a field required for some kinds and forbidden for others), so the
// TaskReconciler calls this as a reconcile guard and fails Tasks that violate it.
func ValidateTaskSpec(spec TaskSpec) error {
	kind := spec.Kind
	if kind == "" {
		return nil
	}
	if repoScopedKinds[kind] && spec.RepositoryRef == "" {
		return fmt.Errorf("task kind %q requires a non-empty repositoryRef", kind)
	}
	if projectScopedKinds[kind] && spec.RepositoryRef != "" {
		return fmt.Errorf("task kind %q must have an empty repositoryRef (project-scoped); got %q", kind, spec.RepositoryRef)
	}
	return nil
}

// TaskSpec defines the desired state of a Task.
type TaskSpec struct {
	ProjectRef string `json:"projectRef"`
	// RepositoryRef is the PRIMARY repo, set ONLY on documentation Tasks (and on
	// a proposal-born clarify, which carries the repo its proposal was filed in).
	// +optional
	RepositoryRef string `json:"repositoryRef,omitempty"`
	// Goal is NON-EVICTABLE: the A.7 byte guard can spill comments and notes, but
	// it can never shrink the goal. It therefore needs a hard cap of its own
	// (fix L31) or it eats the budget the guard is defending.
	// The same cap applies to QueuedTaskBlueprint.Goal (B.7).
	// +kubebuilder:validation:MaxLength=16384
	Goal string `json:"goal"`
	// Source is the originating SCM work item. It is the seed identity triaging
	// mints the Issue CR from and the base of the deterministic task branch.
	// Absent on brainstorm/refine Tasks and on alert-born incidents.
	// +optional
	Source *TaskSource `json:"source,omitempty"`
	// Kind is the ORIGIN. Immutable, baked into the name. NOT the running agent
	// kind (that is Status.AgentKind, driven by the F.2 stage table).
	// +kubebuilder:validation:Enum=brainstorm;incident;clarify;refine;review;documentation;takeover
	// +optional
	Kind string `json:"kind,omitempty"`
	// DedupKey is the dedup identity for an incident Task: the alert-group hash
	// (sha256(groupKey)[:16]) that ties re-fires of the same alert to the same
	// tracked issue. It is the ONE dedup mechanism that does NOT fold into the
	// (repo, number) natural key: a firing alert arrives from Grafana with no
	// Issue and no MR to key on. Empty for non-incident Tasks.
	// +optional
	DedupKey string `json:"dedupKey,omitempty"`
	// GroupKey is the CORRELATION identity for an incident Task: a coarser hash
	// (project + the configured correlation labels, e.g. namespace/cluster) than
	// DedupKey. Different alert RULES that fire for one shared root cause carry
	// the same GroupKey but DISTINCT DedupKeys, so admission does NOT suppress
	// them (each is a real, distinct alert) yet file_issue auto-links the new
	// tracker as a sub-issue under the oldest open sibling tracker sharing this
	// GroupKey - collapsing a 5-alert storm into one linked tree instead of five
	// unrelated issues. Empty for non-incident Tasks and when no correlation
	// label was present on the alert.
	// +optional
	GroupKey string `json:"groupKey,omitempty"`
	// MergeOrder is the sequential, dependency-ordered list of Repository CR
	// names whose MRs merge in this order. REQUIRED (and validated to cover every
	// owned MR's repo) whenever the Task owns MRs in MORE THAN ONE repo.
	// THERE IS NO LEXICAL DEFAULT (fix 11): lexical order is
	// agent-skills < cli < claude-code-wrapper < operator, i.e. it merges cli
	// BEFORE operator - precisely the DisallowUnknownFields fleet outage this
	// redesign exists to prevent.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	MergeOrder []string `json:"mergeOrder,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=50
	AlertRules []string `json:"alertRules,omitempty"`
	// DocumentsTasks are the delivered Tasks this NIGHTLY DOCUMENTATION BATCH
	// covers (fix F2 - USER DECISION). Documentation is ONE batch Task per
	// project per night covering everything delivered in the last 24h, NOT one
	// Task per delivery: per-delivery was a 3-5x work amplifier (doc Task -> doc
	// MR -> review pod -> merge -> a tatara-documentation release, for every
	// one-line patch fix) against 3 agent slots.
	// +optional
	// +kubebuilder:validation:MaxItems=100
	DocumentsTasks []string `json:"documentsTasks,omitempty"`
	// MaxTurnsPerTask is the LIFETIME turn backstop across every pod of this
	// Task. Zero = Project.spec.agent.maxTurnsPerTask (default 300).
	// +optional
	MaxTurnsPerTask int `json:"maxTurnsPerTask,omitempty"`
	// InitialStage is the F.3 Create-edge target a mint chooses when it is NOT the
	// default triaging: the sweep mints straight into parked(backlog-sweep) or
	// triaging, and the nightly doc batch into documenting. It is carried in the
	// IMMUTABLE spec so the TaskReconciler create-edge derives the stage with NO
	// post-create status write that must win a race against the reconciler's own
	// create-edge (fix C5). Empty = triaging.
	// +optional
	InitialStage string `json:"initialStage,omitempty"`
	// InitialStageReason is the stageReason paired with InitialStage (e.g.
	// backlog-sweep). Empty for the reason-less initial stages.
	// +optional
	InitialStageReason string `json:"initialStageReason,omitempty"`
}

// Stage* are the 16 members of the task-centric stage machine (contract F.1).
// conversing is the 16th: a POD-BEARING, NON-TERMINAL stage a Task enters when a
// comment needs a live agent on the other end. Because it is pod-bearing and
// non-terminal it needs NO exception in the reaper (which gates on TaskDone), in
// the concurrency accountant (queueTaskHoldsSlot = !TaskDone && !StagePodless) or
// in the F.3 table. That is the whole reason it is a stage rather than a warm pod
// kept alive through parked: the three carve-outs that would have needed each map
// to a past production incident.
const (
	StageTriaging      = "triaging"
	StageBrainstorming = "brainstorming"
	StageClarifying    = "clarifying"
	StageInvestigating = "investigating"
	StageRefining      = "refining"
	StageApproved      = "approved"
	StageImplementing  = "implementing"
	StageReviewing     = "reviewing"
	StageConversing    = "conversing"
	StageMerging       = "merging"
	StageDeploying     = "deploying"
	StageDelivered     = "delivered"
	StageDocumenting   = "documenting"
	StageRejected      = "rejected"
	StageFailed        = "failed"
	StageParked        = "parked"
)

// terminalStages is the closed set StageTerminal checks. delivered is
// deliberately NOT here: it is quasi-terminal (reaped separately at 48h by
// the reaper, once documentedBy is stamped or the Task provably needs no
// coverage), not a stage machine terminal.
var terminalStages = map[string]bool{
	StageRejected: true,
	StageFailed:   true,
	StageParked:   true,
}

// podlessStages is the closed set StagePodless checks: the eight stages
// (contract F.2) that run no agent pod - triaging/approved/merging/deploying
// are pure operator work, delivered/rejected/failed/parked spawn nothing.
// These stages run ONLY clock 3 (WORK), measured from stageEnteredAt, and
// never clock 1 (ADMISSION) - v6 gave merging a 24h admission clock, so the
// bounded merge cycle (mergeReentries) could never engage.
var podlessStages = map[string]bool{
	StageTriaging:  true,
	StageApproved:  true,
	StageMerging:   true,
	StageDeploying: true,
	StageDelivered: true,
	StageRejected:  true,
	StageFailed:    true,
	StageParked:    true,
}

// StageTerminal reports whether t's stage is one of the three closed-set
// terminals (rejected/failed/parked). delivered is quasi-terminal and is
// handled by the reaper, not this predicate.
func StageTerminal(t *Task) bool {
	return terminalStages[t.Status.Stage]
}

// StagePodless reports whether stage runs no agent pod (contract F.2). A
// podless stage's only clock is WORK, measured from stageEnteredAt.
func StagePodless(stage string) bool {
	return podlessStages[stage]
}

// StageIsTerminalOutcome reports whether entering stage is a TERMINAL OUTCOME of
// a Task, i.e. the thing operator_task_terminal_total{kind,stage,stageReason}
// counts (contract K.1 / D1). It is StageTerminal PLUS delivered: delivered is
// quasi-terminal for the REAPER (it is collected on its own schedule once
// documented), but it is absolutely an outcome for the ALERTS - it is the only
// SUCCESS outcome the platform has, and the failure-ratio rules divide by it.
func StageIsTerminalOutcome(stage string) bool {
	return terminalStages[stage] || stage == StageDelivered
}

// TaskDone reports whether a Task's work is over: a closed-set terminal, or
// delivered (quasi-terminal, pod-less, collected by the reaper at 48h). It is
// the stage-machine replacement for the deleted TaskTerminal.
func TaskDone(t *Task) bool {
	return StageTerminal(t) || t.Status.Stage == StageDelivered
}

// FoldStartedAt is the anchor for both fold-hold clocks: the explicit
// FoldInFlightSince stamp, else the generic stage clock, else the zero time (a
// marker so old it carries neither, which reads as expired - the safe direction,
// because the alternative is the unbounded hold of issue #467).
func FoldStartedAt(t *Task) time.Time {
	if t.Status.FoldInFlightSince != nil {
		return t.Status.FoldInFlightSince.Time
	}
	if t.Status.StageEnteredAt != nil {
		return t.Status.StageEnteredAt.Time
	}
	return time.Time{}
}

// FoldInFlightActive reports whether t's B.3 fold adoption CAN STILL COMPLETE,
// and is therefore the ONE predicate that may hold a member Task off the reaper
// or defer a stop edge.
//
// It is deliberately NOT "len(FoldInFlight) > 0". A fold adoption is the body of
// ONE submit_outcome request: steps 1-5 run inside it, and nothing outside it
// ever resumes one. So the marker means "an adoption is running" only while the
// umbrella could still be running that request, and two things falsify that:
//
//   - TaskDone: a delivered/rejected/failed/parked umbrella runs no agent pod
//     and will never submit another outcome. Its marker is a tombstone.
//   - FoldInFlightTTL: a live umbrella whose adoption started an hour ago lost
//     the request some other way (a crash between steps, a 500 on the wire).
//
// Issue #467 had neither check: the umbrella closed an issue it owned, was
// stopped at rejected(issue-closed) mid-adoption, and its 26 members were then
// skipped by the reaper on every pass, forever, with the counter climbing at a
// fixed rate and no mechanism anywhere able to clear it.
func FoldInFlightActive(t *Task, now time.Time) bool {
	if len(t.Status.FoldInFlight) == 0 {
		return false
	}
	if TaskDone(t) {
		return false
	}
	return now.Before(FoldStartedAt(t).Add(FoldInFlightTTL))
}

// MaxTaskNameLength is the RFC-1123 label budget TaskName enforces (49
// chars): the worst-case pod-name suffix "-documentation" is +14 against the
// 63-char RFC-1123 label limit. CRDs cannot constrain metadata.name length
// and there is no validating webhook, so TaskNameTooLong is the reconcile
// guard that fails a Task whose name still exceeds it to stage=failed,
// stageReason=name-too-long.
const MaxTaskNameLength = 49

// TaskName returns the CR name for a Task: <project>-<kind>-<YYYY-MM-DD>-
// <uid5>, capped at MaxTaskNameLength by truncating the PROJECT segment
// only - the kind/date/uid segments are semantically load-bearing and are
// never truncated.
func TaskName(project, kind string, t time.Time, uid string) string {
	suffix := fmt.Sprintf("-%s-%s-%s", kind, t.Format("2006-01-02"), uid)
	budget := MaxTaskNameLength - len(suffix)
	if budget < 1 {
		budget = 1
	}
	if len(project) > budget {
		project = project[:budget]
	}
	project = strings.TrimRight(project, "-")
	name := project + suffix
	if len(name) > MaxTaskNameLength {
		name = name[:MaxTaskNameLength]
	}
	return name
}

// TaskNameTooLong reports whether name exceeds the MaxTaskNameLength budget
// TaskName enforces. The reconciler calls this as a guard on every reconcile
// since CRDs cannot constrain metadata.name length.
func TaskNameTooLong(name string) bool {
	return len(name) > MaxTaskNameLength
}

// IntakeTaskName is the DETERMINISTIC name a natural-key intake mint (B.4 sweep
// OR a webhook primary mint) gives its Task, so a concurrent second mint for the
// same (project, kind, repoRef, number) natural key collides on AlreadyExists at
// the apiserver and is swallowed rather than producing a second owner. It is the
// mint's half of the no-lock, natural-key idempotency the reactive intake relies
// on (the Issue/MergeRequest CR controller-ownership is the other half).
//
// The name carries the LITERAL number so the (repo, number) natural key is
// unique on its face - the number is unique per repo, so two distinct keys can
// never share a name no matter how their repoRefs truncate. The 64-bit sha256
// suffix disambiguates across projects/kinds and across repoRefs that sanitize
// to the same truncated prefix, and keeps the name stable, DNS-1123-safe, and
// within MaxTaskNameLength for a long repoRef. (F1-1: the old scheme hid the
// number inside an 8-hex/32-bit suffix, so distinct keys could birthday-collide
// and silently starve the loser's issue - it never got owned.)
func IntakeTaskName(project, kind, repoRef string, number int) string {
	sum := sha256.Sum256([]byte(project + "|" + kind + "|" + repoRef + "|" + strconv.Itoa(number)))
	suffix := "-" + hex.EncodeToString(sum[:])[:16]
	num := "-" + strconv.Itoa(number)
	head := "mt-"
	if kind != "" {
		ki := sanitizeNamePart(kind, 1)
		if ki != "" {
			head += ki + "-"
		}
	}
	budget := MaxTaskNameLength - len(head) - len(num) - len(suffix)
	if budget < 0 {
		budget = 0
	}
	base := head + sanitizeNamePart(repoRef, budget) + num + suffix
	return strings.Trim(base, "-")
}

// sanitizeNamePart lowercases s, keeps [a-z0-9-], collapses other runs to a
// single '-', and caps the result at max chars.
func sanitizeNamePart(s string, max int) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(s) {
		if b.Len() >= max {
			break
		}
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		default:
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// Note is one entry in a Task's append-only journal (contract A.4). Notes ARE
// the continuation state read back by task_context(notes=all).
type Note struct {
	At metav1.Time `json:"at"`
	// Agent is the WRITER. The REST layer stamps it from Status.AgentKind; an
	// agent can NEVER produce "operator" (fix 19). The only writer of
	// agent="operator" is the operator itself, in-process.
	// +kubebuilder:validation:Enum=brainstorm;incident;clarify;refine;review;documentation;implement;operator
	Agent string `json:"agent"`
	// +kubebuilder:validation:Enum=note;plan;handoff
	Kind string `json:"kind"`
	// +kubebuilder:validation:MaxLength=4096
	Body string `json:"body"`
}

// TaskStats is the running usage/token accounting for a Task (contract A.4).
type TaskStats struct {
	TokensInput         int64 `json:"tokensInput,omitempty"`
	TokensOutput        int64 `json:"tokensOutput,omitempty"`
	TokensCacheRead     int64 `json:"tokensCacheRead,omitempty"`
	TokensCacheCreation int64 `json:"tokensCacheCreation,omitempty"`
	Turns               int   `json:"turns,omitempty"` // LIFETIME; checked against maxTurnsPerTask
	PodRuns             int   `json:"podRuns,omitempty"`
	WallSeconds         int64 `json:"wallSeconds,omitempty"`
	// +kubebuilder:validation:MaxItems=50
	AgentsRun  []string `json:"agentsRun,omitempty"`
	IssueCount int      `json:"issueCount,omitempty"`
	MRCount    int      `json:"mrCount,omitempty"`
	// PodRecreations counts pod respawns within the CURRENT stage. At
	// maxPodRecreations the stage -> failed. Reset to 0 on EVERY transition.
	PodRecreations int `json:"podRecreations,omitempty"`
	// NotesSpilled / NotesSpilledRefs: notes evicted to tatara-memory by the A.7
	// byte guard. NotesSpilledRefs ACCUMULATES, one track_id per spill batch
	// (fix M19). They are READ BACK via task_context(notes=all) (fix H10) - notes
	// are the continuation state, so a spilled note that cannot be read is
	// continuity silently lost.
	// +optional
	NotesSpilled int `json:"notesSpilled,omitempty"`
	// The MaxItems marker belongs on the LIST, not on the scalar above it
	// (addendum 2 - v4 put it on NotesSpilled int, where it is meaningless).
	// +optional
	// +kubebuilder:validation:MaxItems=50
	NotesSpilledRefs []string `json:"notesSpilledRefs,omitempty"`
}

// TaskEvent is one mid-flight SCM event, delivered at the TURN BOUNDARY.
// A BOT-authored event is NEVER enqueued (fix 2): the enqueue filter drops
// author == Project.spec.scm.botLogin, so the operator's own park comment can
// never un-park the Task the operator just parked.
type TaskEvent struct {
	At metav1.Time `json:"at"`
	// +kubebuilder:validation:Enum=issue_comment;mr_comment;mr_review;issue_edited;label;alert
	Kind   string `json:"kind"`
	Repo   string `json:"repo"`   // Repository CR name
	Number int    `json:"number"` // 0 for kind=alert
	Author string `json:"author"`
	// +kubebuilder:validation:MaxLength=4096
	Body string `json:"body"`
}

// ApprovalVerdict is the DURABLE record that the C.6 approval grammar PASSED
// for this Task, against one specific comment.
//
// It exists because the periodic un-park backstop (driveUnparks) can re-derive
// everything else about an identity-unverified park from live cluster state -
// which owned Issues are open, which are approved - but it can NEVER re-run the
// grammar: the grammar needs a freshly SYNCED forge thread and a webhook payload
// the backstop did not see. The verdict is that one missing input, made durable
// at the moment the fast path establishes it, so a fast path that then loses a
// cache race costs a DELAY rather than a permanent stall.
//
// It is evidence of a PAST pass, not a licence: the backstop still re-checks
// every owned Issue's live approval state before re-entering implementing, AND
// scopes the verdict to the CURRENT park - a verdict stamped before the Task's
// current StageEnteredAt is treated as stale and refused, so an approval
// consumed by an earlier park can never satisfy a later, unrelated one
// (grammarPassedFor, internal/controller/unpark.go).
type ApprovalVerdict struct {
	// At is when the grammar passed.
	At metav1.Time `json:"at"`
	// IssueRef is the Issue CR name whose thread carried the approving comment.
	// +kubebuilder:validation:MaxLength=253
	IssueRef string `json:"issueRef,omitempty"`
	// CommentExternalID is the forge comment id the grammar matched, i.e. the
	// Comment.ExternalID the C.6 single-use-evidence clause consumed. It is what
	// makes this verdict traceable back to a real human action. Empty for the
	// AutoApproveTataraProposals carve-out below, which cites no comment.
	// +kubebuilder:validation:MaxLength=128
	CommentExternalID string `json:"commentExternalId,omitempty"`
	// Author is the verified maintainer login whose comment passed - never the
	// bot login. EXCEPTION: under Project.spec.AutoApproveTataraProposals, a
	// bot-authored, integrity-anchor-verified proposal with ZERO maintainer
	// comments auto-approves (autoApproveApplies/autoApprovalEvidence,
	// internal/controller/approval_grammar.go) and is recorded here with
	// Author=AutoApproveLogin ("<tatara:auto>") - a deliberate, narrowly-gated
	// carve-out, not a maintainer approval. Required non-empty: it is the one
	// field every verdict this codebase ever writes always carries, so a
	// consumer refusing an empty Author cannot be fooled by a schema-legal but
	// otherwise-empty verdict.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Author string `json:"author,omitempty"`
	// Phrase is the matched approvalPhrases entry.
	// +kubebuilder:validation:MaxLength=128
	Phrase string `json:"phrase,omitempty"`
}

// TaskStatus defines the observed state of a Task.
type TaskStatus struct {
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// +kubebuilder:validation:Enum=triaging;brainstorming;clarifying;investigating;refining;approved;implementing;reviewing;conversing;merging;deploying;delivered;documenting;rejected;failed;parked
	// +optional
	Stage string `json:"stage,omitempty"`
	// StageEnteredAt is stamped on EVERY stage transition. It is the clock for the
	// POD-LESS stages (F.4).
	// +optional
	StageEnteredAt *metav1.Time `json:"stageEnteredAt,omitempty"`
	// StageWorkStartedAt is stamped when this stage's POD BECOMES READY (fix H12).
	// It is the clock for every POD-SPAWNING stage's deadline. StageEnteredAt is
	// NOT, because it starts ticking the moment the Task enters the stage - which
	// is when its QueuedEvent is ENQUEUED, not when it is admitted. With 3 agent
	// slots and 3-4 serial pod admissions per Task, a Task could burn its entire
	// 2h budget QUEUEING and die parked(stage-deadline) HAVING NEVER RUN A POD -
	// and that park has no re-entry rule. The stage deadline must measure WORK,
	// not queue wait. Cleared on every stage transition.
	// +optional
	StageWorkStartedAt *metav1.Time `json:"stageWorkStartedAt,omitempty"`
	// +kubebuilder:validation:Enum=brainstorm;incident;clarify;refine;review;documentation;implement
	// +optional
	AgentKind string `json:"agentKind,omitempty"`
	// PodStartedAt is stamped when the pod is CREATED (not when it becomes Ready),
	// and RE-stamped on every podRecreations respawn. It is:
	//   - the arming condition for clock 1 vs clock 2 (F.4), and
	//   - the base of the pod TTL: t0 = podStartedAt + agentPodTTLSeconds (G.7).
	//
	// LIFECYCLE, and it is LOAD-BEARING (fix V7-4):
	//   CLEARED on EVERY stage transition. Both this and StageWorkStartedAt.
	//
	// v6 declared this field with no doc comment and no clearing rule, while only
	// StageWorkStartedAt said "cleared on every stage transition". On the NORMAL
	// re-entry edges (reviewing -> implementing, merging -> reviewing, every
	// un-park) a STALE non-nil PodStartedAt then:
	//   (a) DISARMS clock 1 (which is armed only when PodStartedAt == nil) while
	//       the Task waits for admission - and clock 2 cannot run because
	//       its evaluator needs a pod that does not exist yet. THE TASK IS COVERED
	//       BY NO CLOCK AT ALL WHILE QUEUED: exactly the nil-case the three-clock
	//       model claims to exclude.
	//   (b) makes G.7's t0 = PodStartedAt + agentPodTTLSeconds ALREADY IN THE PAST
	//       for the fresh pod, so the operator TTL-stops a pod that just started -
	//       and under fix V6-6 the wrapper then 410s every turn it is given.
	// +optional
	PodStartedAt *metav1.Time `json:"podStartedAt,omitempty"`
	// Notes: append-only journal. IT IS the continuation state. Capped at 50 in
	// Go (drop-oldest, spilled to tatara-memory); MaxItems is a backstop only.
	// +optional
	// +kubebuilder:validation:MaxItems=60
	Notes []Note `json:"notes,omitempty"`
	// PendingEvents: capped at 20 in Go (drop-oldest BEFORE the write; an
	// API-server 422 is NOT retried and would hot-loop webhook redelivery).
	// Cleared by SET-DIFFERENCE inside RetryOnConflict, never by nil-assign
	// (fix 23).
	// +optional
	// +kubebuilder:validation:MaxItems=25
	PendingEvents []TaskEvent `json:"pendingEvents,omitempty"`
	// +optional
	Stats TaskStats `json:"stats,omitempty"`
	// +optional
	DeliveredAt *metav1.Time `json:"deliveredAt,omitempty"`
	// DocumentedBy is the NIGHTLY BATCH documentation Task that covered this
	// delivered Task (fix F2). Empty until a batch has covered it. The reaper
	// holds a delivered Task until it is either covered or provably needs no
	// coverage (zero merged MRs).
	// +optional
	DocumentedBy string `json:"documentedBy,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=50
	IssueRefs []string `json:"issueRefs,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=50
	MRRefs []string `json:"mrRefs,omitempty"`
	// StageReason is the machine reason for the current stage. MANDATORY on
	// parked/failed/rejected. Closed set: F.5.
	// +optional
	StageReason string `json:"stageReason,omitempty"`
	// ParkedFromStage is mostly OBSERVABILITY: the un-park TARGET is NEVER derived
	// from it (fix 2); it is re-derived from Issue.status.status and the owned-MR
	// state (F.6). It IS load-bearing for one gate: ReasonNoOutcome unpark
	// eligibility requires it to be implementing or reviewing (#406), so a park
	// from a pre-implement stage cannot auto-escalate straight into implementing.
	// +optional
	ParkedFromStage string `json:"parkedFromStage,omitempty"`
	// ApprovalVerdict is the durable C.6 grammar pass (see ApprovalVerdict). It is
	// written by the webhook fast path the moment the grammar passes, and read by
	// the periodic driveUnparks backstop, which cannot re-run the grammar itself.
	// Nil means no approving comment has ever been verified for this Task.
	// +optional
	ApprovalVerdict *ApprovalVerdict `json:"approvalVerdict,omitempty"`
	// ConversationLastEventAt is the IDLE CLOCK BASE for the conversing stage. It
	// is stamped on entry into conversing and RE-STAMPED by AppendTaskEvent on
	// every non-bot event queued while conversing, so the clock measures silence
	// rather than pod age.
	//
	// It is a field of its OWN and deliberately NOT stageWorkStartedAt. The TTL
	// stop nils stageWorkStartedAt and PodWatchReconciler re-stamps it when the
	// REPLACEMENT pod becomes ready, so an idle clock based on it would reset on
	// every pod rotation and an idle conversation would never park - it would
	// rotate a pod forever. Nothing in the pod lifecycle touches this field.
	// +optional
	ConversationLastEventAt *metav1.Time `json:"conversationLastEventAt,omitempty"`
	// BotRounds counts CONSECUTIVE agent-authored comment rounds on this Task with
	// no intervening human comment. It is reset to zero by any human comment.
	//
	// It is COUNTED AND EXPOSED, and deliberately NEVER ACTED ON (decision D7):
	// there is no ping-pong cap, and the stage machine is trusted to terminate an
	// agent-to-agent exchange because every agent run ends in an outcome that
	// moves the Task. The counter exists because the 2026-06 production incident
	// was exactly this shape - a reactivation loop that posted 40+ duplicate bot
	// comments - and nothing was counting, so a human found it by reading the
	// thread. operator_bot_rounds is its fleet-wide view.
	// +optional
	BotRounds int `json:"botRounds,omitempty"`
	// MergeCursor is the index into Spec.MergeOrder the sequential merge reached.
	// Persisted so a restarted operator resumes and never re-merges.
	// +optional
	MergeCursor int `json:"mergeCursor,omitempty"`
	// MergeReentries / DeployReentries bound the merging<->parked and
	// deploying<->parked 2-CYCLES (fix H7). v3 let them spin FOREVER on a red MR:
	// F.6 re-entered the stage on timeout, EVERY transition re-stamped
	// stageEnteredAt granting a fresh 4h, neither stage spawns a pod (so
	// maxTurnsPerTask and maxPodRecreations never accrue), and parkRetention never
	// fired because the Task kept LEAVING parked. The "every stage has an exit"
	// invariant was satisfied per-stage and violated GLOBALLY.
	// At maxMergeReentries (3): -> failed(merge-blocked) / failed(deploy-blocked).
	// This is the treatment maxReviewRounds already gets right on the
	// reviewing<->implementing cycle.
	// +optional
	MergeReentries int `json:"mergeReentries,omitempty"`
	// +optional
	DeployReentries int `json:"deployReentries,omitempty"`
	// HeadMoveReentries bounds the FOURTH cycle - the one that SPAWNS PODS
	// (fix M3-9). merging -> reviewing on a moved head does NOT touch
	// MergeReentries (only the PARKED path does), and ReviewRounds increments
	// only on request_changes. So reviewing -> merging -> (head moved) ->
	// reviewing -> ... had no counter at all, and spawned a REVIEW POD every lap.
	// H7 claimed "three cycles exist, all three bounded". There are four.
	// Cap 3 -> failed(head-moving).
	// +optional
	HeadMoveReentries int `json:"headMoveReentries,omitempty"`
	// HumanReviewRounds bounds the reviewing <-> parked(awaiting-human) cycle on a
	// kind=review Task (fix V7-9). Cap 5, then it STAYS parked.
	//
	// v6 claimed that cycle was "bounded by mr.status.reviewRounds". IT IS NOT:
	// ReviewRounds increments only on request_changes, so on the approve path the
	// cycle spawned ONE REVIEW POD PER HUMAN COMMENT, bounded only by
	// maxTurnsPerTask (300). It terminated - but not for the stated reason, and it
	// is a real cost amplifier on a chatty PR thread.
	// +optional
	HumanReviewRounds int `json:"humanReviewRounds,omitempty"`
	// FoldInFlight names the member Tasks a refine umbrella is mid-adoption of.
	// The reaper SKIPS any Task named here (fix 8), but ONLY while
	// FoldInFlightActive - see there.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	FoldInFlight []string `json:"foldInFlight,omitempty"`
	// FoldInFlightSince is when FoldInFlight was last written. It is the ANCHOR
	// for the reaper's TTL: without it a stranded marker is indistinguishable
	// from one written a second ago, which is how issue #467 pinned 26 Tasks
	// against a dead umbrella with nothing able to tell the difference. Absent
	// (a marker written by an older build) it falls back to StageEnteredAt.
	// +optional
	FoldInFlightSince *metav1.Time `json:"foldInFlightSince,omitempty"`
	// ResolvedModel is the MODEL env resolved for this Task's agent pod at spawn
	// (modelForKind: per-kind override else project-wide). Stamped once at
	// pod-creation; read by the token/terminal metrics so $ is priced by the
	// model that actually ran.
	// +optional
	ResolvedModel string `json:"resolvedModel,omitempty"`
	// ShortDescription is the first line of Spec.Goal, truncated to ~60 chars,
	// set on reconcile so `kubectl get task` is scannable without describe.
	// +optional
	ShortDescription string `json:"shortDescription,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Stage",type=string,JSONPath=`.status.stage`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.stageReason`
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=`.status.agentKind`
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.projectRef`,priority=1
// +kubebuilder:printcolumn:name="Turns",type=integer,JSONPath=`.status.stats.turns`
// +kubebuilder:printcolumn:name="Description",type=string,JSONPath=`.status.shortDescription`

// Task is one unit of agent-driven work, advanced through the F.1 stage machine.
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

// The /outcome idempotency condition's vocabulary. It is declared HERE, not in
// internal/restapi, because internal/controller must read it too and must never
// import internal/restapi. api/v1alpha1 imports nothing internal, so it is the
// only place both can reach.
const (
	// ConditionOutcomeAccepted is the DURABLE idempotency record of an accepted
	// submit_outcome. Its Message is sha256(agentKind|payload).
	ConditionOutcomeAccepted = "OutcomeAccepted"
	// OutcomeReasonClaimed is the Reason a bare CLAIM stamps. The kind handler's
	// commit OVERWRITES it with the kind's own reason ("Review", "Clarify", ...),
	// and that difference is the ONLY durable record of claimed-vs-committed.
	// internal/restapi's conditionReason("") must return exactly this; the two
	// may never drift, so it delegates to OutcomeReasonFor.
	OutcomeReasonClaimed = "Outcome"
)

const (
	// ConditionMemoryDegraded records that this Task's agent pod was spawned
	// while the project memory stack was not stably ready, so the agent runs
	// with no recall: the memory and code-graph tools fail for its whole turn.
	// The work is NOT held (a memory outage must not stop the platform) - this
	// condition is the per-Task drill-down a human reads after the memory-stack
	// alert fires, and operator_agent_pod_degraded_total is its fleet-wide view.
	ConditionMemoryDegraded = "MemoryDegraded"
	// ReasonSpawnedWithoutRecall is ConditionMemoryDegraded's only Reason.
	ReasonSpawnedWithoutRecall = "SpawnedWithoutRecall"
)

// OutcomeReasonFor is the condition Reason an outcome of agentKind commits. The
// empty kind is the bare claim.
func OutcomeReasonFor(agentKind string) string {
	if agentKind == "" {
		return OutcomeReasonClaimed
	}
	return strings.ToUpper(agentKind[:1]) + agentKind[1:]
}

// OutcomeCondition returns t's OutcomeAccepted condition, or nil.
func OutcomeCondition(t *Task) *metav1.Condition {
	for i := range t.Status.Conditions {
		if t.Status.Conditions[i].Type == ConditionOutcomeAccepted {
			return &t.Status.Conditions[i]
		}
	}
	return nil
}

// OutcomeCommitted reports whether an accepted outcome has been fully APPLIED
// (as opposed to merely CLAIMED). A bare claim stamps Reason "Outcome"; the kind
// handler's commit overwrites it with the kind's own reason.
func OutcomeCommitted(t *Task) bool {
	c := OutcomeCondition(t)
	return c != nil && c.Status == metav1.ConditionTrue && c.Reason != OutcomeReasonClaimed
}

// OutcomeCommittedFor reports whether the outcome committed on t was submitted
// BY agentKind - i.e. the CURRENT stage's own agent has finished and its commit
// landed.
//
// "Is anything committed" is NOT a safe guard and this is why the predicate is
// stage-scoped. The condition is per-TASK and survives across stages: an
// implement Task's commit stamps Reason=Implement AND enters reviewing in the
// same write, so OutcomeCommitted is already true the instant it arrives at
// reviewing. A guard keying on that alone would suppress the review pod that has
// not spawned yet and wedge every implement Task - a strictly worse failure than
// the one being fixed.
//
// It is self-scoping to kind=review at the reviewing stage, which is exactly the
// one case that needs it: every OTHER kind's commit calls stage.Enter in the same
// write, so its Reason can never name the NEW stage's agent kind, and no other
// stage can be committed-but-not-advanced.
//
// A pod-less stage (agentKind == "") never matches: it runs no agent.
func OutcomeCommittedFor(t *Task, agentKind string) bool {
	if agentKind == "" {
		return false
	}
	c := OutcomeCondition(t)
	return OutcomeCommitted(t) && c.Reason == OutcomeReasonFor(agentKind)
}
