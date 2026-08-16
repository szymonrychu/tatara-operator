package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	QueueClassNormal = "normal"
	QueueClassAlert  = "alert"

	QueueStateQueued   = "Queued"
	QueueStateAdmitted = "Admitted"
	// QueueStateDone is retained for API/test compatibility. The dispatcher no
	// longer transitions events to Done; reconcileDone GC-deletes them instead.
	QueueStateDone = "Done"
)

// QueuedEventPayload is the Task blueprint a producer stashes; the dispatcher
// rebuilds the Task from it verbatim on admission. Producers keep ownership of
// label/goal/source construction so the dispatcher stays generic.
type QueuedEventPayload struct {
	// Goal is copied VERBATIM onto the rebuilt Task's Spec.Goal, which is capped
	// at GoalMaxBytes - so it needed the same cap and did not have it. Without
	// it an over-long goal was accepted here and only rejected at admission, one
	// layer away from the producer that could still have done something about it
	// (issue #495's class: a bounded field fed from an unbounded one).
	// Go-side twin: GoalMaxBytes (limits.go).
	// +kubebuilder:validation:MaxLength=16384
	Goal          string            `json:"goal,omitempty"`
	Kind          string            `json:"kind"`
	RepositoryRef string            `json:"repositoryRef,omitempty"`
	Source        *TaskSource       `json:"source,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
	// GenerateName is used when Name is empty; Name is a fixed deterministic
	// Task name that makes admission idempotent.
	GenerateName string `json:"generateName,omitempty"`
	Name         string `json:"name,omitempty"`
	// PodRepo feeds agent.StampPodName on the rebuilt Task. Provider is no
	// longer consumed for pod naming (issue #507 dropped the gh/gl segment)
	// but is kept for other callers.
	Provider string `json:"provider,omitempty"`
	PodRepo  string `json:"podRepo,omitempty"`
	// InitialState is the Create-edge target the minted Task carries, copied
	// verbatim onto TaskSpec.InitialState. Empty means `new`, i.e. triage.
	//
	// It exists for the kinds that have NO GATE TO FACE and therefore nothing to
	// triage: an upgrade Task is minted straight into under-implementation,
	// because refined's only exit into under-implementation is
	// submit_outcome(action=approved) and the upgrade outcome schema has no such
	// action. Without this the Task would sit at refined re-submitting against a
	// transition that does not exist.
	// +kubebuilder:validation:Enum=new;refined;under-implementation
	// +optional
	InitialState string `json:"initialState,omitempty"`
	// AlertRule is carried from the incident webhook onto the built Task's
	// Spec.AlertRules.
	// +optional
	AlertRule string `json:"alertRule,omitempty"`
	// DedupKey is carried from the incident webhook onto the built Task's
	// Spec.DedupKey (the incident alert-group dedup identity).
	// +optional
	DedupKey string `json:"dedupKey,omitempty"`
	// GroupKey is carried from the incident webhook onto the built Task's
	// Spec.GroupKey (the coarser incident correlation identity).
	// +optional
	GroupKey string `json:"groupKey,omitempty"`

	// AgentKind is the pod to spawn for a stage-driven admission (contract
	// B.7). It marks the ADMISSION-TICKET payload shape: when set, exactly one
	// of TaskRef / NewTask must also be set (ValidateQueuedEventSpec enforces
	// this). Kept +optional at the CRD level (unlike the contract's bare
	// literal) so a flat MINT payload (the sweep/webhook producers, which fill
	// only Kind/Goal/...) keeps validating unchanged.
	// +kubebuilder:validation:Enum=brainstorm;incident;clarify;refine;review;documentation;implement;upgrade
	// +optional
	AgentKind string `json:"agentKind,omitempty"`
	// TaskRef names an EXISTING Task (a stage-driven spawn).
	// Exactly one of TaskRef / NewTask is set when AgentKind is set.
	// +optional
	TaskRef string `json:"taskRef,omitempty"`
	// NewTask is the blueprint for a Task that does not exist yet (a mint).
	// +optional
	NewTask *QueuedTaskBlueprint `json:"newTask,omitempty"`

	// AdoptedUpgrade marks the ADOPTION payload shape: a dependency-upgrade
	// merge request that already exists on the forge, queued until the pool has
	// room. Mutually exclusive with the admission-ticket shape
	// (AgentKind+TaskRef) and with NewTask - see ValidateQueuedEventSpec.
	// +optional
	AdoptedUpgrade *AdoptedUpgradeRef `json:"adoptedUpgrade,omitempty"`
}

// QueuedTaskBlueprint is the mint blueprint for a Task that does not exist
// yet (contract B.7), carried on QueuedEventPayload.NewTask.
type QueuedTaskBlueprint struct {
	// Name is deterministic so admission is idempotent.
	Name string `json:"name"`
	// Kind is the ORIGIN kind.
	Kind string `json:"kind"`
	// Goal carries the SAME MaxLength=16384 cap as Task.spec.goal
	// (task_types.go) - it is NON-EVICTABLE at the far end.
	// Go-side twin: GoalMaxBytes (limits.go).
	// +kubebuilder:validation:MaxLength=16384
	Goal          string `json:"goal"`
	ProjectRef    string `json:"projectRef"`
	RepositoryRef string `json:"repositoryRef,omitempty"`
	// +optional
	IssueKeys []string `json:"issueKeys,omitempty"` // "<repo>#<number>"
	// +optional
	AlertRules  []string          `json:"alertRules,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// AdoptedUpgradeRef is the merge-request snapshot a QUEUED ADOPTION carries.
//
// IT EXISTS BECAUSE NEITHER EXISTING MINT SHAPE CAN CARRY IT. QueuedTaskBlueprint
// has no Source, no InitialState and no merge-request fields at all; the flat
// legacy payload's TaskSource carries Number/HeadSHA/Title but not HeadBranch,
// Body, Labels, Repo or HeadRepo. MintAdoptedUpgradeTask needs every one of
// those: HeadBranch becomes AnnTakeoverHeadBranch (which is what points both the
// review pod and the upgrade pod at the engine's branch), Title and Body feed
// the mirror snapshot, and Repo/HeadRepo are AdoptUpgradeMR's fork guard.
//
// IT IS A SNAPSHOT OF A LIVE FORGE OBJECT, which is exactly the objection that
// once ruled queued adoption out (MEMORY.md 2026-08-16). The webhook keeps it
// fresh: `synchronize` refreshes headSHA/title/body on a still-Queued event and
// `closed`/`merged` deletes one outright, and the mint re-runs the adoption
// predicates before it creates anything.
type AdoptedUpgradeRef struct {
	// Number is the merge request number/iid. Required.
	Number int `json:"number"`
	// +optional
	Title string `json:"title,omitempty"`
	// Author is the forge login that opened the merge request - the identity
	// adoptableAuthor rules on. Never a git commit author.
	Author string `json:"author"`
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`
	// +optional
	HeadBranch string `json:"headBranch,omitempty"`
	// Body carries the changelog the review agent reads. It is capped at
	// MergeRequestBodyMaxBytes (limits.go), NOT at GoalMaxBytes as it was until
	// the whole-branch review: this string's DESTINATION is
	// MergeRequest.status.body (mrSnapshot -> bindMRToTask -> SyncMergeRequest),
	// whose own marker is 65536, so the smaller cap made the adopted mirror's
	// body strictly shorter than the same merge request's body mirrored by any
	// other path, for a field that never reaches a Task goal at all.
	//
	// NOTHING TRUNCATED IT AT ALL BEFORE THAT REVIEW, which is the real defect
	// the cap value only frames: a grouped Renovate bump with per-dependency
	// release notes routinely runs past any of these numbers, the QueuedEvent
	// Create 422s, the webhook answers 500 on EVERY delivery for that merge
	// request (GitLab counts consecutive failures toward auto-disabling the
	// project hook), and the sweep fails enqueue_adopt_upgrade every pass
	// forever. ClampAdoptedUpgradeBody (internal/controller/upgrade_adopt.go) is
	// the one clamp; it runs inside AdoptedUpgradeRefFromPR so both producers
	// inherit it, and again on the D4 refresh write.
	// Go-side twin: MergeRequestBodyMaxBytes (limits.go).
	// +kubebuilder:validation:MaxLength=65536
	// +optional
	Body string `json:"body,omitempty"`
	// +optional
	Labels []string `json:"labels,omitempty"`
	// Repo and HeadRepo are AdoptUpgradeMR clause (d)'s fork guard, in the
	// forge's own slug namespace. An EMPTY HeadRepo fails the guard CLOSED.
	// +optional
	Repo string `json:"repo,omitempty"`
	// +optional
	HeadRepo string `json:"headRepo,omitempty"`
}

// validAgentKinds are the 7 AGENT kinds a QueuedEventPayload.AgentKind may name
// (contract B.7). `clarify` was one of them until #521 folded it into implement;
// `upgrade` took the seventh slot on 2026-08-13. Deliberately narrower than
// IsKnownKind (task_types.go), which is the ORIGIN vocabulary and also carries
// `takeover`.
var validAgentKinds = map[string]bool{
	"brainstorm":    true,
	"incident":      true,
	"refine":        true,
	"review":        true,
	"documentation": true,
	"implement":     true,
	"upgrade":       true,
}

type QueuedEventSpec struct {
	Seq           int64              `json:"seq"`
	Class         string             `json:"class"`
	Kind          string             `json:"kind"`
	Autonomous    bool               `json:"autonomous,omitempty"`
	ProjectRef    string             `json:"projectRef"`
	RepositoryRef string             `json:"repositoryRef,omitempty"`
	DedupKey      string             `json:"dedupKey,omitempty"`
	Payload       QueuedEventPayload `json:"payload"`

	// Priority orders admission ahead of Seq. Lower is more urgent.
	//   0 = incident              (redundant with the alert pool; kept for clarity)
	//   1 = webhook-originated    (a human is waiting)
	//   2 = cron/sweep-originated (proactive work)
	//
	// It is a *int, NOT an int (fix M17): Go's zero value for an int is 0, so
	// client-go would ALWAYS serialise "priority": 0, the field would never be
	// ABSENT from the request body, the CRD default would NEVER fire, and any
	// producer that forgot the field would land in the MOST URGENT tier. A nil
	// pointer is genuinely absent, so the default applies.
	// +kubebuilder:validation:Enum=0;1;2
	// +kubebuilder:default=2
	// +optional
	Priority *int `json:"priority,omitempty"`
}

// EffectiveQueuePriority returns spec.Priority, or 2 (the CRD default) when
// nil - i.e. when the QueuedEvent was built as a Go literal (fake client,
// tests) and never round-tripped through API-server defaulting.
func EffectiveQueuePriority(spec QueuedEventSpec) int {
	if spec.Priority != nil {
		return *spec.Priority
	}
	return 2
}

type QueuedEventStatus struct {
	State      string       `json:"state,omitempty"`
	TaskRef    string       `json:"taskRef,omitempty"`
	AdmittedAt *metav1.Time `json:"admittedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Seq",type=integer,JSONPath=`.spec.seq`
// +kubebuilder:printcolumn:name="Class",type=string,JSONPath=`.spec.class`
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
type QueuedEvent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              QueuedEventSpec   `json:"spec,omitempty"`
	Status            QueuedEventStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type QueuedEventList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QueuedEvent `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &QueuedEvent{}, &QueuedEventList{})
		return nil
	})
}

// ValidateQueuedEventSpec mirrors ValidateTaskSpec's kind/repo-scoping rules.
func ValidateQueuedEventSpec(spec QueuedEventSpec) error {
	if spec.Seq <= 0 {
		return fmt.Errorf("queuedevent: seq must be positive")
	}
	if spec.Class != QueueClassNormal && spec.Class != QueueClassAlert {
		return fmt.Errorf("queuedevent: invalid class %q", spec.Class)
	}
	if spec.ProjectRef == "" {
		return fmt.Errorf("queuedevent: projectRef required")
	}
	if !IsKnownKind(spec.Kind) {
		return fmt.Errorf("queuedevent: invalid kind %q", spec.Kind)
	}
	if projectScopedKinds[spec.Kind] && spec.RepositoryRef != "" {
		return fmt.Errorf("queuedevent: kind %q is project-scoped, repositoryRef must be empty", spec.Kind)
	}
	if repoScopedKinds[spec.Kind] && spec.RepositoryRef == "" {
		return fmt.Errorf("queuedevent: kind %q requires repositoryRef", spec.Kind)
	}
	if spec.Payload.AgentKind != "" {
		if !validAgentKinds[spec.Payload.AgentKind] {
			return fmt.Errorf("queuedevent: invalid payload.agentKind %q", spec.Payload.AgentKind)
		}
		hasTaskRef := spec.Payload.TaskRef != ""
		hasNewTask := spec.Payload.NewTask != nil
		if hasTaskRef == hasNewTask {
			return fmt.Errorf("queuedevent: exactly one of payload.taskRef / payload.newTask must be set")
		}
		if hasNewTask && len(spec.Payload.NewTask.Goal) > 16384 {
			return fmt.Errorf("queuedevent: payload.newTask.goal exceeds max length 16384")
		}
	}
	if a := spec.Payload.AdoptedUpgrade; a != nil {
		// The adoption payload MINTS. A ticket admits an existing Task's pod and
		// mints nothing, and a blueprint describes a different Task entirely, so a
		// payload claiming both leaves the dispatcher guessing which work it is.
		if spec.Payload.AgentKind != "" || spec.Payload.TaskRef != "" || spec.Payload.NewTask != nil {
			return fmt.Errorf("queuedevent: payload.adoptedUpgrade is exclusive with agentKind/taskRef/newTask")
		}
		if a.Number <= 0 {
			return fmt.Errorf("queuedevent: payload.adoptedUpgrade.number must be positive")
		}
		if spec.RepositoryRef == "" {
			return fmt.Errorf("queuedevent: payload.adoptedUpgrade requires repositoryRef")
		}
	}
	return nil
}
