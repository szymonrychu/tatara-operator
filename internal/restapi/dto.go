package restapi

import (
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// agentDTO no longer carries maxTurnsPerPod / maxTurnsPerTask / maxReviewRounds /
// maxPodRecreations. O3 retired all four ceilings, and the CRD fields they came
// from are Deprecated with zero readers - reporting a budget the operator does
// not enforce to the agent that thinks it is being budgeted is worse than
// omitting it. Every one was `omitempty`, so a consumer that still reads them
// sees the same absent-field/zero it already saw for any project that never set
// one.
type agentDTO struct {
	Model              string `json:"model,omitempty"`
	Image              string `json:"image,omitempty"`
	PermissionMode     string `json:"permissionMode,omitempty"`
	TurnTimeoutSeconds int    `json:"turnTimeoutSeconds,omitempty"`
}

type projectStatusDTO struct {
	WebhookURL string             `json:"webhookURL,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ProjectDTO is the stable JSON shape for a Project CRD (contract C.2.1).
type ProjectDTO struct {
	Name                string           `json:"name"`
	ScmSecretRef        string           `json:"scmSecretRef,omitempty"`
	TriggerLabel        string           `json:"triggerLabel,omitempty"`
	MaxConcurrentAgents int              `json:"maxConcurrentAgents,omitempty"`
	AgentPodTTLSeconds  int              `json:"agentPodTTLSeconds,omitempty"`
	MaxNewTasksPerSweep int              `json:"maxNewTasksPerSweep,omitempty"`
	MaxOpenTasks        int              `json:"maxOpenTasks,omitempty"`
	MaxBundleBytes      int              `json:"maxBundleBytes,omitempty"`
	Agent               agentDTO         `json:"agent"`
	Status              projectStatusDTO `json:"status"`
}

type repositoryStatusDTO struct {
	Phase              string             `json:"phase,omitempty"`
	LastIngestedCommit string             `json:"lastIngestedCommit,omitempty"`
	LastIngestTime     string             `json:"lastIngestTime,omitempty"`
	JobName            string             `json:"jobName,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// RepositoryDTO is the stable JSON shape for a Repository CRD.
type RepositoryDTO struct {
	Name          string              `json:"name"`
	ProjectRef    string              `json:"projectRef,omitempty"`
	URL           string              `json:"url,omitempty"`
	DefaultBranch string              `json:"defaultBranch,omitempty"`
	IngestEnabled bool                `json:"ingestEnabled"` // effective value; nil field defaults to true
	Status        repositoryStatusDTO `json:"status"`
}

type taskSourceDTO struct {
	Provider    string `json:"provider,omitempty"`
	IssueRef    string `json:"issueRef,omitempty"`
	URL         string `json:"url,omitempty"`
	AuthorLogin string `json:"authorLogin,omitempty"`
	IsPR        bool   `json:"isPR,omitempty"`
	Number      int    `json:"number,omitempty"`
}

type taskStatusDTO struct {
	PodName    string             `json:"podName,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	State              string                   `json:"state,omitempty"`
	StateEnteredAt     string                   `json:"stateEnteredAt,omitempty"`
	StateWorkStartedAt string                   `json:"stateWorkStartedAt,omitempty"`
	AgentKind          string                   `json:"agentKind,omitempty"`
	StateReason        string                   `json:"stateReason,omitempty"`
	ParkReason         string                   `json:"parkReason,omitempty"`
	ParkedAt           string                   `json:"parkedAt,omitempty"`
	ParkedFromState    string                   `json:"parkedFromState,omitempty"`
	Notes              []tatarav1alpha1.Note    `json:"notes,omitempty"`
	Stats              tatarav1alpha1.TaskStats `json:"stats,omitempty"`
	IssueRefs          []string                 `json:"issueRefs,omitempty"`
	MRRefs             []string                 `json:"mrRefs,omitempty"`
	MergeCursor        int                      `json:"mergeCursor,omitempty"`
	DeliveredAt        string                   `json:"deliveredAt,omitempty"`
	DocumentedBy       string                   `json:"documentedBy,omitempty"`
}

// TaskDTO is the stable JSON shape for a Task CRD (contract C.2.3, C.2.4).
type TaskDTO struct {
	Name          string         `json:"name"`
	ProjectRef    string         `json:"projectRef,omitempty"`
	RepositoryRef string         `json:"repositoryRef,omitempty"`
	Goal          string         `json:"goal,omitempty"`
	Kind          string         `json:"kind,omitempty"`
	DedupKey      string         `json:"dedupKey,omitempty"`
	Source        *taskSourceDTO `json:"source,omitempty"`
	Status        taskStatusDTO  `json:"status"`

	// C.2.3 index fields and the C.2.4 spec additions.
	Title          string   `json:"title,omitempty"`
	Body           string   `json:"body,omitempty"`
	Issues         []string `json:"issues,omitempty"`
	MRs            []string `json:"mrs,omitempty"`
	AgeSeconds     int64    `json:"ageSeconds,omitempty"`
	MergeOrder     []string `json:"mergeOrder,omitempty"`
	AlertRules     []string `json:"alertRules,omitempty"`
	DocumentsTasks []string `json:"documentsTasks,omitempty"`
}

func toProjectDTO(p tatarav1alpha1.Project) ProjectDTO {
	return ProjectDTO{
		Name: p.Name, ScmSecretRef: p.Spec.ScmSecretRef, TriggerLabel: p.Spec.TriggerLabel,
		MaxConcurrentAgents: p.Spec.MaxConcurrentAgents,
		AgentPodTTLSeconds:  p.Spec.AgentPodTTLSeconds,
		MaxNewTasksPerSweep: p.Spec.MaxNewTasksPerSweep,
		MaxOpenTasks:        p.Spec.MaxOpenTasks,
		MaxBundleBytes:      p.Spec.MaxBundleBytes,
		Agent: agentDTO{
			Model: p.Spec.Agent.Model, Image: p.Spec.Agent.Image,
			PermissionMode:     p.Spec.Agent.PermissionMode,
			TurnTimeoutSeconds: p.Spec.Agent.TurnTimeoutSeconds,
		},
		Status: projectStatusDTO{WebhookURL: p.Status.WebhookURL, Conditions: p.Status.Conditions},
	}
}

// rfc3339 renders a CRD timestamp, or "" when unset.
func rfc3339(t *metav1.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// refKey turns an Issue/MergeRequest CR name (iss-<repo>-<n> / mr-<repo>-<n>)
// back into the agent-facing natural key (<repo>#<n> / <repo>!<n>), the same
// form the bundle renders and scm_read takes.
func refKey(ref, prefix, sep string) string {
	s := strings.TrimPrefix(ref, prefix)
	i := strings.LastIndexByte(s, '-')
	if i <= 0 {
		return ref
	}
	return s[:i] + sep + s[i+1:]
}

func refKeys(refs []string, prefix, sep string) []string {
	if len(refs) == 0 {
		return nil
	}
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, refKey(r, prefix, sep))
	}
	return out
}

// taskTitle is the first line of spec.goal, capped; taskBody is its first 500
// chars (C.2.3). The Task CRD carries no separate title.
func taskTitle(goal string) string {
	line := goal
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	return truncateValidUTF8(strings.TrimSpace(line), 120)
}

func taskBody(goal string) string {
	return truncateValidUTF8(goal, 500)
}

func toRepositoryDTO(r tatarav1alpha1.Repository) RepositoryDTO {
	d := RepositoryDTO{
		Name: r.Name, ProjectRef: r.Spec.ProjectRef, URL: r.Spec.URL,
		DefaultBranch: r.Spec.DefaultBranch, IngestEnabled: tatarav1alpha1.BoolVal(r.Spec.IngestEnabled, true),
		Status: repositoryStatusDTO{
			Phase: r.Status.Phase, LastIngestedCommit: r.Status.LastIngestedCommit,
			JobName: r.Status.JobName, Conditions: r.Status.Conditions,
		},
	}
	// LastIngestTime is a pointer in the CRD status; omit when nil.
	if r.Status.LastIngestTime != nil && !r.Status.LastIngestTime.IsZero() {
		d.Status.LastIngestTime = r.Status.LastIngestTime.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	return d
}

// toTaskListDTO is toTaskDTO minus the three unbounded free-text fields:
// spec.goal, status.notes and status.conditions (#641). A LIST row already
// carries the C.2.3 index fields right beside them - Title, the goal's first
// line; Body, its first 500 chars - and those caps exist precisely so a list
// response does not have to ship the whole goal. The uncapped Goal was never
// removed from beside them, so the index fields added ~500 bytes per row and
// saved nothing: 57 Tasks came to 725 KB, of which goal was 72% and
// status.notes 17%.
//
// toTaskDTO itself is deliberately NOT touched. Its five single-Task callers
// (getTask, the note-created response, and three in outcome.go) return the full
// goal on purpose - task_get is the tool an agent uses to read it. All three
// zeroed fields are `omitempty`, so they are simply absent from a list row.
func toTaskListDTO(task tatarav1alpha1.Task) TaskDTO {
	d := toTaskDTO(task)
	d.Goal = ""
	d.Status.Notes = nil
	d.Status.Conditions = nil
	return d
}

func toTaskDTO(task tatarav1alpha1.Task) TaskDTO {
	d := TaskDTO{
		Name: task.Name, ProjectRef: task.Spec.ProjectRef, RepositoryRef: task.Spec.RepositoryRef,
		Goal: task.Spec.Goal, Kind: task.Spec.Kind, DedupKey: task.Spec.DedupKey,
		Status: taskStatusDTO{
			PodName:    task.Status.PodName,
			Conditions: task.Status.Conditions,

			State:              task.Status.State,
			StateEnteredAt:     rfc3339(task.Status.StateEnteredAt),
			StateWorkStartedAt: rfc3339(task.Status.StateWorkStartedAt),
			AgentKind:          task.Status.AgentKind,
			StateReason:        task.Status.StateReason,
			ParkReason:         task.Status.ParkReason,
			ParkedAt:           rfc3339(task.Status.ParkedAt),
			ParkedFromState:    task.Status.ParkedFromState,
			Notes:              task.Status.Notes,
			Stats:              task.Status.Stats,
			IssueRefs:          task.Status.IssueRefs,
			MRRefs:             task.Status.MRRefs,
			MergeCursor:        task.Status.MergeCursor,
			DeliveredAt:        rfc3339(task.Status.DeliveredAt),
			DocumentedBy:       task.Status.DocumentedBy,
		},
		Title:          taskTitle(task.Spec.Goal),
		Body:           taskBody(task.Spec.Goal),
		Issues:         refKeys(task.Status.IssueRefs, "iss-", "#"),
		MRs:            refKeys(task.Status.MRRefs, "mr-", "!"),
		AgeSeconds:     int64(time.Since(task.CreationTimestamp.Time).Seconds()),
		MergeOrder:     task.Spec.MergeOrder,
		AlertRules:     task.Spec.AlertRules,
		DocumentsTasks: task.Spec.DocumentsTasks,
	}
	if task.Spec.Source != nil {
		d.Source = &taskSourceDTO{
			Provider: task.Spec.Source.Provider, IssueRef: task.Spec.Source.IssueRef,
			URL: task.Spec.Source.URL, AuthorLogin: task.Spec.Source.AuthorLogin,
			IsPR: task.Spec.Source.IsPR, Number: task.Spec.Source.Number,
		}
	}
	return d
}
