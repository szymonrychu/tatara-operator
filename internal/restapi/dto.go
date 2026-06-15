package restapi

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

type agentDTO struct {
	Model              string `json:"model,omitempty"`
	Image              string `json:"image,omitempty"`
	PermissionMode     string `json:"permissionMode,omitempty"`
	MaxTurnsPerTask    int    `json:"maxTurnsPerTask,omitempty"`
	TurnTimeoutSeconds int    `json:"turnTimeoutSeconds,omitempty"`
}

type projectStatusDTO struct {
	WebhookURL string             `json:"webhookURL,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ProjectDTO is the stable JSON shape for a Project CRD.
type ProjectDTO struct {
	Name               string           `json:"name"`
	ScmSecretRef       string           `json:"scmSecretRef,omitempty"`
	TriggerLabel       string           `json:"triggerLabel,omitempty"`
	MaxConcurrentTasks int              `json:"maxConcurrentTasks,omitempty"`
	Agent              agentDTO         `json:"agent"`
	Status             projectStatusDTO `json:"status"`
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
	Phase            string                           `json:"phase,omitempty"`
	PodName          string                           `json:"podName,omitempty"`
	TurnsCompleted   int                              `json:"turnsCompleted,omitempty"`
	PrURL            string                           `json:"prURL,omitempty"`
	ResultSummary    string                           `json:"resultSummary,omitempty"`
	DiscoveredIssues []string                         `json:"discoveredIssues,omitempty"`
	ReviewVerdict    *tatarav1alpha1.ReviewVerdict    `json:"reviewVerdict,omitempty"`
	PROutcome        *tatarav1alpha1.PROutcome        `json:"prOutcome,omitempty"`
	IssueOutcome     *tatarav1alpha1.IssueOutcome     `json:"issueOutcome,omitempty"`
	ImplementOutcome *tatarav1alpha1.ImplementOutcome `json:"implementOutcome,omitempty"`
	ChangeSummary    *tatarav1alpha1.ChangeSummary    `json:"changeSummary,omitempty"`
	Handover         string                           `json:"handover,omitempty"`
	Conditions       []metav1.Condition               `json:"conditions,omitempty"`
	PendingComments  []string                         `json:"pendingComments,omitempty"`
}

// TaskDTO is the stable JSON shape for a Task CRD.
type TaskDTO struct {
	Name             string         `json:"name"`
	ProjectRef       string         `json:"projectRef,omitempty"`
	RepositoryRef    string         `json:"repositoryRef,omitempty"`
	Goal             string         `json:"goal,omitempty"`
	Kind             string         `json:"kind,omitempty"`
	ApprovalRequired bool           `json:"approvalRequired,omitempty"`
	Source           *taskSourceDTO `json:"source,omitempty"`
	MaxTurns         int            `json:"maxTurns,omitempty"`
	Status           taskStatusDTO  `json:"status"`
}

type subtaskStatusDTO struct {
	Phase  string `json:"phase,omitempty"`
	TurnID string `json:"turnId,omitempty"`
	Result string `json:"result,omitempty"`
}

// SubtaskDTO is the stable JSON shape for a Subtask CRD.
type SubtaskDTO struct {
	Name    string           `json:"name"`
	TaskRef string           `json:"taskRef,omitempty"`
	Title   string           `json:"title,omitempty"`
	Detail  string           `json:"detail,omitempty"`
	Order   int              `json:"order"`
	Status  subtaskStatusDTO `json:"status"`
}

func toProjectDTO(p tatarav1alpha1.Project) ProjectDTO {
	return ProjectDTO{
		Name: p.Name, ScmSecretRef: p.Spec.ScmSecretRef, TriggerLabel: p.Spec.TriggerLabel,
		MaxConcurrentTasks: p.Spec.MaxConcurrentTasks,
		Agent: agentDTO{
			Model: p.Spec.Agent.Model, Image: p.Spec.Agent.Image,
			PermissionMode: p.Spec.Agent.PermissionMode, MaxTurnsPerTask: p.Spec.Agent.MaxTurnsPerTask,
			TurnTimeoutSeconds: p.Spec.Agent.TurnTimeoutSeconds,
		},
		Status: projectStatusDTO{WebhookURL: p.Status.WebhookURL, Conditions: p.Status.Conditions},
	}
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

func toTaskDTO(task tatarav1alpha1.Task) TaskDTO {
	d := TaskDTO{
		Name: task.Name, ProjectRef: task.Spec.ProjectRef, RepositoryRef: task.Spec.RepositoryRef,
		Goal: task.Spec.Goal, MaxTurns: task.Spec.MaxTurns,
		Kind: task.Spec.Kind, ApprovalRequired: task.Spec.ApprovalRequired,
		Status: taskStatusDTO{
			Phase: task.Status.Phase, PodName: task.Status.PodName,
			TurnsCompleted: task.Status.TurnsCompleted, PrURL: task.Status.PrURL,
			ResultSummary: task.Status.ResultSummary, Conditions: task.Status.Conditions,
			DiscoveredIssues: task.Status.DiscoveredIssues,
			ReviewVerdict:    task.Status.ReviewVerdict,
			PROutcome:        task.Status.PROutcome,
			IssueOutcome:     task.Status.IssueOutcome,
			ImplementOutcome: task.Status.ImplementOutcome,
			ChangeSummary:    task.Status.ChangeSummary,
			Handover:         task.Status.Handover,
			PendingComments:  task.Status.PendingComments,
		},
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

func toSubtaskDTO(st tatarav1alpha1.Subtask) SubtaskDTO {
	return SubtaskDTO{
		Name: st.Name, TaskRef: st.Spec.TaskRef, Title: st.Spec.Title,
		Detail: st.Spec.Detail, Order: st.Spec.Order,
		Status: subtaskStatusDTO{Phase: st.Status.Phase, TurnID: st.Status.TurnID, Result: st.Status.Result},
	}
}
