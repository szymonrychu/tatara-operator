package v1alpha1

import (
	"fmt"
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
	"review":  true,
	"clarify": true,
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

// infraIncidentKeywords are lower-case substrings that mark a Grafana alert as
// targeting the project's core memory/storage infrastructure: the memory stack
// (LightRAG retrieval surface, Postgres/CNPG, Neo4j) or its backing storage
// (CephFS PVCs, WAL, quorum). Matched case-insensitively against a Task's
// AlertRules. Kept intentionally narrow so only genuine infra alerts qualify for
// the admission-gate exemption below.
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
	if spec.Kind != "incident" {
		return false
	}
	for _, rule := range spec.AlertRules {
		if AlertTargetsCoreInfra(rule) {
			return true
		}
	}
	return false
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
	// +kubebuilder:validation:Enum=brainstorm;incident;clarify;refine;review;documentation
	// +optional
	Kind string `json:"kind,omitempty"`
	// DedupKey is the dedup identity for an incident Task: the alert-group hash
	// (sha256(groupKey)[:16]) that ties re-fires of the same alert to the same
	// tracked issue. It is the ONE dedup mechanism that does NOT fold into the
	// (repo, number) natural key: a firing alert arrives from Grafana with no
	// Issue and no MR to key on. Empty for non-incident Tasks.
	// +optional
	DedupKey string `json:"dedupKey,omitempty"`
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

// Stage* are the 15 members of the task-centric stage machine (contract F.1).
const (
	StageTriaging      = "triaging"
	StageBrainstorming = "brainstorming"
	StageClarifying    = "clarifying"
	StageInvestigating = "investigating"
	StageRefining      = "refining"
	StageApproved      = "approved"
	StageImplementing  = "implementing"
	StageReviewing     = "reviewing"
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
	// +kubebuilder:validation:Enum=issue_comment;mr_comment;mr_review;label;alert
	Kind   string `json:"kind"`
	Repo   string `json:"repo"`   // Repository CR name
	Number int    `json:"number"` // 0 for kind=alert
	Author string `json:"author"`
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

	// +kubebuilder:validation:Enum=triaging;brainstorming;clarifying;investigating;refining;approved;implementing;reviewing;merging;deploying;delivered;documenting;rejected;failed;parked
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
	// ParkedFromStage is OBSERVABILITY ONLY. The un-park TARGET is NEVER derived
	// from it (fix 2); it is re-derived from Issue.status.status and the owned-MR
	// state (F.6).
	// +optional
	ParkedFromStage string `json:"parkedFromStage,omitempty"`
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
	// The reaper SKIPS any Task named here (fix 8).
	// +optional
	// +kubebuilder:validation:MaxItems=20
	FoldInFlight []string `json:"foldInFlight,omitempty"`
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
