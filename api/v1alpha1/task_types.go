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
// a proposal-born implement Task carries its proposal's repo.
//
// implement REPLACED clarify here in #521 and it is not cosmetic: every Task
// minted from now on carries kind=implement where it used to carry clarify, and
// IsKnownKind("implement") must be true or the QueuedEvent validator rejects
// every one of them.
//
// THE 61 LIVE Tasks that still carry spec.kind=clarify are NOT rewritten - the
// #521 migrator was withdrawn (see MEMORY.md 2026-08-08) and spec.kind is
// immutable anyway. They read back as clarify (an enum is validated on WRITE,
// and 1.33 ratcheting lets an unchanged invalid value through), triageTarget
// has no clarify row, and each one parks ONCE at triage-stalled with
// action=triage_unknown_kind before the 7-day park retention reaps it. Loud,
// bounded, and deliberately not special-cased.
//
// upgrade joined the set on 2026-08-13. The cron mints it with no repo at all
// and it picks its own blast radius from the enrolled set, so it must validate
// with an empty RepositoryRef; it belongs here rather than in
// projectScopedKinds because it DOES open merge requests, which is exactly the
// property projectScopedKinds denies.
var unconstrainedKinds = map[string]bool{
	"review":    true,
	"implement": true,
	"takeover":  true,
	"upgrade":   true,
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
	// a proposal-born implement Task, which carries the repo its proposal was
	// filed in).
	// +optional
	RepositoryRef string `json:"repositoryRef,omitempty"`
	// Goal is NON-EVICTABLE: the A.7 byte guard can spill comments and notes, but
	// it can never shrink the goal. It therefore needs a hard cap of its own
	// (fix L31) or it eats the budget the guard is defending.
	// The same cap applies to QueuedTaskBlueprint.Goal (B.7).
	// Go-side twin: GoalMaxBytes (limits.go).
	// +kubebuilder:validation:MaxLength=16384
	Goal string `json:"goal"`
	// Source is the originating SCM work item. It is the seed identity triaging
	// mints the Issue CR from and the base of the deterministic task branch.
	// Absent on brainstorm/refine Tasks and on alert-born incidents.
	// +optional
	Source *TaskSource `json:"source,omitempty"`
	// Kind is the ORIGIN. Immutable, baked into the name. NOT the running agent
	// kind (that is Status.AgentKind, driven by the F.2 state/origin table).
	// +kubebuilder:validation:Enum=brainstorm;incident;implement;refine;review;documentation;takeover;upgrade
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
	// Deprecated: RETIRED BY O3 together with Project.spec.agent.maxTurnsPerTask,
	// and read by no enforcement path. It WAS the per-Task override of the
	// LIFETIME turn backstop. Retained for API compatibility with already-created
	// Task objects; setting it has no effect.
	// +optional
	MaxTurnsPerTask int `json:"maxTurnsPerTask,omitempty"`
	// InitialState is the Create-edge target a mint chooses when it is NOT the
	// default `new`. It is carried in the IMMUTABLE spec so the TaskReconciler
	// create-edge derives the state with NO post-create status write that must
	// win a race against the reconciler's own create-edge (fix C5). Empty = new.
	//
	// #521 renamed this from InitialStage. The two-field shape survived the
	// rename because the create edge can now land in TWO orthogonal places at
	// once: a sweep-minted backlog owner is `new` AND parked(backlog-sweep), and
	// `parked` is no longer a state it could have been expressed as.
	// +kubebuilder:validation:Enum=new;refined;under-implementation
	// +optional
	InitialState string `json:"initialState,omitempty"`
	// InitialParkReason is the park flag a mint stamps ALONGSIDE InitialState
	// (e.g. backlog-sweep). Empty means the Task is minted un-parked. It is a
	// PARK reason, never a state reason: a mint that wants a terminal Task does
	// not exist.
	// +optional
	InitialParkReason string `json:"initialParkReason,omitempty"`
}

// State* are the 8 members of the task lifecycle. They name WHERE THE WORK IS,
// and nothing else: whether a Task is stalled is status.parkReason, and whether
// a live agent is attached is stage.Live(state). Those two are ORTHOGONAL to
// this enum, deliberately - the 16-stage machine folded all three axes into one
// value, which is how `parked` came to be simultaneously a stage, a terminal,
// and a pod-less marker, and how TaskDone(parked) ended up true.
//
// NAMING TRAP: `merged` and `deployed` name the PHASE, not the milestone.
// merged means "the verdict is approve and the operator owns the merge cursor",
// NOT "every MR is merged". Per-MR truth stays on mr.status.state.
const (
	StateNew                 = "new"
	StateRefined             = "refined"
	StateUnderImplementation = "under-implementation"
	StateAwaitingReview      = "awaiting-review"
	StateMerged              = "merged"
	StateDeployed            = "deployed"
	StateDone                = "done"
	StateRejected            = "rejected"
)

// TaskDone reports whether a Task's work is over. It is EXACTLY
// state in {done, rejected} - nothing else, and in particular NOT parked.
//
// This is issue #521's fix and it is structural. Under the stage machine
// terminalStages contained StageParked, so TaskDone(parked) was true, so
// intake.createTaskRaceSafe's live-twin branch (intake.go:311) was skipped for
// every parked Task, so the Create 409ed and control reached the delete at
// intake.go:332 - deleting a live Task and cascading its owned Issue and
// MergeRequest mirrors. Once done is exactly {done, rejected}, a parked Task
// takes the live-twin branch and that delete is structurally unreachable for
// it.
func TaskDone(t *Task) bool {
	return t.Status.State == StateDone || t.Status.State == StateRejected
}

// Parked reports whether a Task is stalled. Empty ParkReason means not parked
// and that is the whole test.
func Parked(t *Task) bool { return t.Status.ParkReason != "" }

// TaskIsTerminalOutcome reports whether entering state is a TERMINAL OUTCOME,
// i.e. what operator_task_terminal_total counts. Same set as TaskDone: under
// the 8-state model `done` absorbed `delivered`, so there is no quasi-terminal
// left to special-case.
func TaskIsTerminalOutcome(state string) bool {
	return state == StateDone || state == StateRejected
}

// FoldStartedAt is the anchor for both fold-hold clocks: the explicit
// FoldInFlightSince stamp, else the generic state clock, else the zero time (a
// marker so old it carries neither, which reads as expired - the safe direction,
// because the alternative is the unbounded hold of issue #467).
func FoldStartedAt(t *Task) time.Time {
	if t.Status.FoldInFlightSince != nil {
		return t.Status.FoldInFlightSince.Time
	}
	if t.Status.StateEnteredAt != nil {
		return t.Status.StateEnteredAt.Time
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
	// ID is the stable handle an agent quotes back as planNoteId. It is
	// sha256(at|kind|body) truncated to 16 hex, so it is derivable from the
	// note alone (no counter, no sequence) and CHANGES if the body changes -
	// which is exactly what the approval gate's plan pin needs: an approved
	// plan whose body was edited afterwards no longer matches the hash stored
	// on ApprovalEvidence.
	// +optional
	ID string      `json:"id,omitempty"`
	At metav1.Time `json:"at"`
	// Agent is the WRITER. The REST layer stamps it from Status.AgentKind; an
	// agent can NEVER produce "operator" (fix 19). The only writer of
	// agent="operator" is the operator itself, in-process.
	// clarify is RETAINED here and nowhere else. #521 deleted it as a live agent
	// kind, but this field is a historical RECORD of who wrote a note, not a
	// selector - notes already in etcd say clarify and are immutable. Dropping it
	// made the apiserver reject every status write to any Task carrying one,
	// because appending a note revalidates its siblings and ratcheting only
	// exempts unchanged keypaths. That broke enforce-live-pod-ceiling into a
	// reconcile error loop. No writer can produce it: AgentKindFor has no clarify.
	// +kubebuilder:validation:Enum=brainstorm;incident;refine;review;documentation;implement;upgrade;operator;clarify
	Agent string `json:"agent"`
	// +kubebuilder:validation:Enum=note;plan;handoff
	Kind string `json:"kind"`
	// Go-side twin: NoteBodyMaxBytes (limits.go). Change both together.
	// +kubebuilder:validation:MaxLength=4096
	Body string `json:"body"`
}

// NewNoteID derives a Note's stable id. It is a pure function of the note's
// own content, so any reader can re-derive it and a body edit invalidates it.
func NewNoteID(at metav1.Time, kind, body string) string {
	sum := sha256.Sum256([]byte(strconv.FormatInt(at.Unix(), 10) + "|" + kind + "|" + body))
	return "n-" + hex.EncodeToString(sum[:])[:16]
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
	// Go-side twin: TaskEventBodyMaxBytes (limits.go). Forge comments routinely
	// run past it - this platform's own agents write multi-KB ones - so every
	// writer MUST clamp. AppendTaskEvent does it for all of them (issue #495).
	// +kubebuilder:validation:MaxLength=4096
	Body string `json:"body"`
}

// TaskStatus defines the observed state of a Task.
type TaskStatus struct {
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// State is WHERE THE WORK IS. It is ORTHOGONAL to ParkReason (whether the
	// Task is stalled) and to stage.Live(State) (whether an agent pod is
	// attached). Collapsing those three axes into one value is what #521 fixed.
	// +kubebuilder:validation:Enum=new;refined;under-implementation;awaiting-review;merged;deployed;done;rejected
	// +optional
	State string `json:"state,omitempty"`
	// StateEnteredAt is stamped on EVERY state transition. It is the clock for
	// the OPERATOR-DRIVEN states (F.4).
	// +optional
	StateEnteredAt *metav1.Time `json:"stateEnteredAt,omitempty"`
	// StateReason is the machine reason for the current state. MANDATORY on
	// done and rejected. It is NOT a park reason: the two vocabularies are
	// disjoint and live in different fields on purpose.
	// +optional
	StateReason string `json:"stateReason,omitempty"`
	// ParkReason is the PARK FLAG. Empty means NOT PARKED, and that is the
	// whole test - there is no parked state to compare against. It is
	// ORTHOGONAL to State: a Task parks WHERE IT IS, and un-parking returns it
	// to the same state unless a rule says otherwise.
	//
	// THE WEDGE THIS INVITES, and the one mitigation: a writer that sets State
	// and forgets to clear ParkReason wedges the Task forever with a stale
	// reason - the same silent-drift genre as #521 itself. Mitigation:
	// stage.Enter asserts ParkReason == "" on every non-park edge and refuses
	// otherwise, and stage.Unpark is the ONE function that clears it. Nothing
	// else in the codebase may assign to this field.
	// +kubebuilder:validation:Enum=backlog-sweep;triage-stalled;name-too-long;stage-deadline;awaiting-human;identity-unverified;implement-declined;review-loop-exhausted;review-post-refused;merge-timeout;merge-blocked;merge-order-missing;deploy-timeout;deploy-blocked;no-outcome;turn-budget-exhausted;pod-recreation-exhausted;object-too-large;fold-adoption-unverified;admission-starved;agent-contract-mismatch;operator-error;head-moving;handoff-stalled;ownership-lost;merge-auth-refused;ci-red;ci-blocked
	// +optional
	ParkReason string `json:"parkReason,omitempty"`
	// +optional
	ParkedAt *metav1.Time `json:"parkedAt,omitempty"`
	// ParkedFromState is OBSERVABILITY. The un-park target is NEVER derived
	// from it. It is load-bearing for exactly one gate: no-outcome un-park
	// eligibility requires it to be under-implementation or awaiting-review
	// (#406).
	// +optional
	ParkedFromState string `json:"parkedFromState,omitempty"`
	// StateWorkStartedAt is stamped when this state's POD BECOMES READY (fix H12).
	// It is the clock for every POD-SPAWNING state's deadline. StateEnteredAt is
	// NOT, because it starts ticking the moment the Task enters the state - which
	// is when its QueuedEvent is ENQUEUED, not when it is admitted. With 3 agent
	// slots and 3-4 serial pod admissions per Task, a Task could burn its entire
	// 2h budget QUEUEING and die parked(stage-deadline) HAVING NEVER RUN A POD -
	// and that park has no re-entry rule. The state deadline must measure WORK,
	// not queue wait. Cleared on every state transition.
	// +optional
	StateWorkStartedAt *metav1.Time `json:"stateWorkStartedAt,omitempty"`
	// +kubebuilder:validation:Enum=brainstorm;incident;refine;review;documentation;implement;upgrade
	// +optional
	AgentKind string `json:"agentKind,omitempty"`
	// PodStartedAt is stamped when the pod is CREATED (not when it becomes Ready),
	// and RE-stamped on every podRecreations respawn. It is:
	//   - the arming condition for clock 1 vs clock 2 (F.4), and
	//   - the base of the pod TTL: t0 = podStartedAt + agentPodTTLSeconds (G.7).
	//
	// LIFECYCLE, and it is LOAD-BEARING (fix V7-4):
	//   CLEARED on EVERY state transition. Both this and StateWorkStartedAt.
	//
	// v6 declared this field with no doc comment and no clearing rule, while only
	// StateWorkStartedAt said "cleared on every state transition". On the NORMAL
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
	// ConversationLastEventAt is the HUMAN half of the IDLE CLOCK BASE for every
	// LIVE state. It is stamped on entry into a live state and RE-STAMPED by
	// AppendTaskEvent on every non-bot event queued while live, so the clock
	// measures silence rather than pod age.
	//
	// It is only ONE THIRD of the base. Nothing an AGENT does touches this field,
	// so an idle clock reading it alone parks a silently-working agent at 60
	// minutes, and a Task that queued for hours before its pod arrived is parked
	// before turn 0 is ever submitted. stage.ArmedClock therefore disarms the idle
	// clock entirely while a turn is in flight and otherwise measures from the
	// LATEST of this field, stateWorkStartedAt and the turn-complete annotation.
	// See stage.idleBase, which also records the pod-rotation ratchet that admits
	// and the three caps that bound it.
	//
	// It is a field of its OWN and deliberately NOT stateWorkStartedAt. The TTL
	// stop nils stateWorkStartedAt and PodWatchReconciler re-stamps it when the
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
	// StageElapsedCarrySeconds is the WORK-clock time already spent in the
	// current merging/deploying attempt, carried across a
	// parked(merge-timeout|deploy-timeout) <-> merging|deploying round-trip
	// (issue #480). stage.Enter re-stamps StageEnteredAt on EVERY transition
	// (fix V7-4) - deliberately, for every OTHER edge - so a bare re-entry into
	// the SAME stage after its OWN budget elapsed would otherwise buy a full
	// fresh budget every lap: a capped 3-retry MergeReentries/DeployReentries
	// cycle (this comment's own H7 fix, above) turned into ~16h of re-testing a
	// deterministically red CI, because the counter that bounds the LOOP and the
	// clock that measures the WORK were never the same field.
	//
	// Written in exactly two places, both in stage.Enter, never by a caller: (1)
	// accumulated with THIS attempt's elapsed time the instant a podless stage's
	// budget elapses into parked(merge-timeout|deploy-timeout) (before
	// StageEnteredAt is overwritten), and (2) preserved as-is (not reset to zero
	// like every other edge) on the matching un-park re-entry
	// (parked(merge-timeout)->merging, parked(deploy-timeout)->deploying). Every
	// OTHER transition resets it to zero, including a HEAD-MOVED merging exit
	// (fix M3-9) or a maintainer-driven fresh implementation
	// (enterFreshImplementing) - a genuinely fresh entry into merging/deploying
	// must never inherit a stale carry from an unrelated earlier cycle.
	//
	// READ BACK BY REPORTING, NOT BY THE DEADLINE (issue #513 split this).
	// operator_task_state_age_seconds, operator_merge_cursor_stalled_seconds and
	// deploy_waiting's stalled_seconds all add it to now.Sub(StageEnteredAt), via
	// stage.StageElapsedSeconds, so reported residency is CONTINUOUS across the
	// whole round trip instead of a sawtooth that resets every re-entry. That is
	// what #480 measured and it is the only thing this field decides.
	//
	// stage.ArmedClock's podless WORK clock does NOT subtract it. It used to, and
	// the deadline it produced was self-defeating: a timeout park happens only
	// once the budget is ALREADY spent, so every re-entry arrived over budget and
	// re-parked on the SAME reconcile pass (live in #513: laps of 7.2ms, 8.8ms and
	// 11.3ms, terminal failed 107s after the first timeout, all three
	// MaxDeployReentries spent buying zero deploy time). ArmedClock instead uses
	// "carry > 0 while IN merging or deploying" as the "this is a timeout
	// re-entry" discriminator - exact, because Enter zeroes the carry on every
	// non-timeout edge, whereas MergeReentries/DeployReentries stay set across an
	// unrelated later cycle, and stage-scoped because the carry is also live while
	// the Task is still PARKED, whose budget is the 7-day ParkRetention reap clock
	// - and gives that re-entry a fresh TimeoutReentryBudget window measured from
	// THIS entry.
	//
	// ONE TIMEOUT CYCLE is therefore bounded at base budget + 3 x 30m (deploying
	// 3h30m, merging 5h30m), far below the ~16h #480 killed. That is a PER-CYCLE
	// bound, not a total for the stage: a HEAD-MOVED or ci-red exit from merging
	// leaves via reviewing/implementing, where Enter zeroes this field (see above),
	// so the next merging entry opens a NEW cycle with a fresh full 4h base. The
	// number of such cycles is what MaxHeadMoveReentries / MaxCIRedReentries bound.
	// Unchanged from #480, which zeroed the carry on exactly the same edges.
	// +optional
	StageElapsedCarrySeconds int `json:"stageElapsedCarrySeconds,omitempty"`
	// HeadMoveReentries bounds the FOURTH cycle - the one that SPAWNS PODS
	// (fix M3-9). merging -> reviewing on a moved head does NOT touch
	// MergeReentries (only the PARKED path does), and ReviewRounds increments
	// only on request_changes. So reviewing -> merging -> (head moved) ->
	// reviewing -> ... had no counter at all, and spawned a REVIEW POD every lap.
	// H7 claimed "three cycles exist, all three bounded". There are four.
	// Cap 3 -> failed(head-moving).
	// +optional
	HeadMoveReentries int `json:"headMoveReentries,omitempty"`
	// CIRedReentries bounds the FIFTH cycle: the red-CI bounce back to
	// implementing (issue #476). A DETERMINISTICALLY red required check on the
	// reviewed head can never merge, so the Task is routed back to the agent that
	// can fix it instead of being promoted into merging (or left there polling a
	// verdict that will never change until the 4h budget parks it). Like every
	// other bounce it spawns pods, so it gets a counter.
	// Cap 3 -> failed(ci-blocked).
	// +optional
	CIRedReentries int `json:"ciRedReentries,omitempty"`
	// CIWaitSince is THE CI HOLD (PR B): the implement outcome was ACCEPTED, the
	// code is pushed, and the advance to awaiting-review is HELD because CI at
	// the pushed head is still pending/running. It is the timestamp of the accept
	// and it doubles as the flag - nil means no hold.
	//
	// IT IS DELIBERATELY NOT A PARK AND NOT A NEW STATE. Park would have been the
	// cheaper-looking option and it is the wrong one twice over: a parked Task
	// takes TaskReconciler's terminal early return, so nothing would drive the
	// hold to its conclusion, and CIRefreshCadence drops a parked MR back to the
	// 24h mirror cadence, so the ONE backstop that is supposed to clear this hold
	// when a webhook is missed would run once a day. A new state would have to be
	// taught to the stage enum, LegalFor's edge table, AgentKindFor and the budget
	// rows. Holding at under-implementation costs one field, keeps the 5m
	// CIRefreshCadenceActive backstop armed, and keeps Owns(&MergeRequest{})
	// waking the Task on every webhook stamp.
	//
	// IT IS NEVER A DEAD END. reconcileCIWait clears it three ways: green ->
	// awaiting-review, red -> the enterCIRed bounce, and CIWaitDeadline ->
	// awaiting-review anyway (fail OPEN, which is exactly the pre-PR-B
	// behaviour), so a forge that never delivers another CI event costs a bounded
	// wait and not a stranded Task.
	// +optional
	CIWaitSince *metav1.Time `json:"ciWaitSince,omitempty"`
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
	// PinnedPlanNoteID is the id of the NEWEST plan note, re-derived on every
	// note write. It exists because status.notes is capped at 50 with drop-oldest
	// and the evicted batch is spilled to tatara-memory: a long-running Task can
	// spill the very plan note the approval gate pinned, and the gate's
	// plan-note-missing refusal would then fire on a legitimate submit.
	//
	// It holds the ID ONLY. The body is not retained, deliberately: the pin the
	// gate re-checks is the sha256 stored on Issue.status.approval.planHash at
	// GRANT, so nothing here has to survive for the check to work.
	// +optional
	PinnedPlanNoteID string `json:"pinnedPlanNoteId,omitempty"`
	// ShortDescription is the first line of Spec.Goal, truncated to ~60 chars,
	// set on reconcile so `kubectl get task` is scannable without describe.
	// +optional
	ShortDescription string `json:"shortDescription,omitempty"`
	// LastTurnFinalText and LastTurnPushedRepos are THE LAST TURN'S CONTINUATION
	// STATE, and they exist for exactly one reader: the G.7 synthetic handoff
	// note (agent.TTLStopper.writeSyntheticNote).
	//
	// Both values arrive on every turn-complete callback
	// (turnCompletePayload.FinalText / .PushedRepos) and neither was persisted
	// anywhere, so ttlStop had nothing to build the note FROM and every synthetic
	// note ever written read exactly "TTL stop. Last turn's final text: (none).
	// Repos pushed: none." Dozens of those were confirmed in-cluster (#527): the
	// non-empty-notes invariant held syntactically and was worthless
	// semantically, and every TTL stop silently reset its Task's continuation
	// state, re-ran turn-0 and re-charged maxTurnsPerTask.
	//
	// LastTurnFinalText is truncated to the Note.Body budget on the way in: it can
	// only ever be RENDERED into a note, so retaining more than a note can hold
	// buys nothing and spends the A.7 object budget.
	//
	// LastTurnPushedRepos is OVERWRITTEN on every turn that produced anything,
	// including with an empty list. That is the point of retaining pushedRepos on
	// the wire at all (G.2): "the agent pushed nothing this turn" and "the agent
	// pushed tatara-operator two turns ago" must not render as the same note.
	//
	// BOTH fields hold the newest NON-EMPTY payload: a turn that produced neither
	// a final text nor a push - state="failed" is a real wrapper state - leaves
	// them alone rather than blanking them. Recording that emptiness would
	// discard the newest turn that DID produce something and make the very next
	// G.7 stop write its placeholder note, which is the loss these fields exist
	// to prevent.
	// +optional
	// +kubebuilder:validation:MaxLength=4096
	LastTurnFinalText string `json:"lastTurnFinalText,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=20
	LastTurnPushedRepos []string `json:"lastTurnPushedRepos,omitempty"`
	// LastTurnFailedRepos is the set of repos whose commit/push FAILED on the
	// finishing turn, and it is the other half of the same sentence
	// LastTurnPushedRepos starts.
	//
	// The wrapper's turn-end loop used to abort on the first repo that errored,
	// so a failure was total and loud. It now attempts every repo and joins the
	// errors (tatara-claude-code-wrapper#167), which turns one loud failure into a
	// partial success - and a SHORT pushedRepos list is indistinguishable from a
	// turn that simply had nothing to push in those repos. Without this field the
	// operator stamps a truthful-looking partial as the turn's whole result.
	//
	// It matters more than pushedRepos, not less: /workspace is the container
	// writable layer with no volume, so a repo that failed to push holds commits
	// that exist nowhere but on a disk about to disappear. It is rendered into the
	// G.7 synthetic handoff note for exactly that reason.
	//
	// Written under the same known/unknown gate as LastTurnPushedRepos: the poll
	// backstop's TurnResult carries no such field, so it leaves whatever the
	// callback recorded alone - but only for its OWN turn, see
	// LastTurnReposTurnID.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	LastTurnFailedRepos []string `json:"lastTurnFailedRepos,omitempty"`
	// LastTurnReposTurnID names the turn the two repo lists above describe. It
	// exists because the poll backstop stamps a NEWER turn's final text while
	// knowing nothing about that turn's repos, which would otherwise leave the
	// previous turn's lists attached to it.
	//
	// The two lists are then treated differently, and deliberately so: a stale
	// pushedRepos is optimistic and inert (the commits it names really are on
	// origin, they are simply older than the text beside them), while a stale
	// failedRepos is an active instruction to redo work that has since landed -
	// and it would additionally make a content-free stop compute contentFree=false,
	// re-disarming the #527 empty-synthetic detector. So the failures are dropped
	// when they belong to an older turn and the pushes are kept.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	LastTurnReposTurnID string `json:"lastTurnReposTurnId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Park",type=string,JSONPath=`.status.parkReason`
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=`.status.agentKind`
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.projectRef`,priority=1
// +kubebuilder:printcolumn:name="Turns",type=integer,JSONPath=`.status.stats.turns`
// +kubebuilder:printcolumn:name="Description",type=string,JSONPath=`.status.shortDescription`

// Task is one unit of agent-driven work, advanced through the 8-state machine.
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
	// ReasonSpawnedWithoutRecall is ConditionMemoryDegraded's Reason when the
	// project's memory stack exists but is not stably ready - a real degradation.
	ReasonSpawnedWithoutRecall = "SpawnedWithoutRecall"
	// ReasonMemoryDisabled is ConditionMemoryDegraded's Reason when the project
	// has spec.memory.enabled=false. The condition is recorded as FALSE with this
	// reason: the pod genuinely has no recall, but nothing is degraded, and a
	// True/SpawnedWithoutRecall here would make every turn of a memory-free
	// project look like an incident.
	ReasonMemoryDisabled = "MemoryDisabled"
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
