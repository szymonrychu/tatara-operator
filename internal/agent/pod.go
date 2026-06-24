package agent

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/grafanamcp"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// wrapperPort is the wrapper's in-pod HTTP listener.
const wrapperPort = 8080

// CallbackHMACSecretKey is the data key holding the callback HMAC shared secret
// in the Secret referenced by PodConfig.CallbackHMACSecretName. The same key is
// read by the operator deployment via SecretKeyRef. Fixed by consumer code.
const CallbackHMACSecretKey = "callback-hmac-secret"

// PodNameAnnotation, when set on a Task at creation, holds the descriptive
// wrapper Pod/Service name. PodName prefers it; legacy Tasks created before this
// annotation existed fall back to wrapper-<task-name>.
const PodNameAnnotation = "tatara.dev/pod-name"

// Wrapper Pod/Service label keys. The orphan reaper selects on LabelManagedBy +
// LabelComponent and correlates back to the owning Task via LabelTask /
// LabelTaskUID, so it never has to reconstruct PodName.
const (
	LabelManagedBy = "tatara.dev/managed-by"
	LabelComponent = "app.kubernetes.io/component"
	LabelTask      = "tatara.dev/task"
	LabelTaskUID   = "tatara.dev/task-uid"

	ManagedByValue = "tatara-operator"
	ComponentAgent = "agent"
)

// repoEntry is the JSON shape of one entry in TATARA_REPOS.
type repoEntry struct {
	Name   string `json:"name"`
	URL    string `json:"url"`
	Branch string `json:"branch"`
}

// PodConfig holds the operator-level inputs the Pod/Service builders need that
// do not come from the CRDs.
type PodConfig struct {
	Namespace           string
	CallbackURL         string // full routable in-cluster base URL, e.g. http://tatara-operator-internal.tatara.svc:8082
	OIDCIssuer          string // OIDC issuer URL passed to the wrapper for token verification
	AnthropicSecretName string
	CLIOIDCSecretName   string
	ImagePullSecret     string
	OperatorURL         string // operator REST base URL for the agent's task_*/subtask_* MCP tools
	// CallbackHMACSecretName, when non-empty, is the name of the Secret holding
	// the callback HMAC shared secret under key config.CallbackHMACSecretKey. It
	// is injected into the wrapper Pod as CALLBACK_HMAC_SECRET via a SecretKeyRef
	// (NOT a literal env value) so the secret never appears in the Pod spec /
	// etcd object in plaintext, matching every other agent secret. The wrapper
	// signs its turn-complete callbacks with it; the operator verifies via
	// CallbackServer.CallbackSecret (finding 1/r3).
	CallbackHMACSecretName string

	// Resource requests/limits for the wrapper container. Parsed from camelCase
	// config scalars (cpu-request, cpu-limit, memory-request, memory-limit in the
	// operator ConfigMap). Empty strings mean no constraint is set (BestEffort).
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string

	// RunAsNonRoot and RunAsUser configure the container SecurityContext.
	// RunAsNonRoot=true rejects images that run as root. RunAsUser overrides the
	// image default UID when non-nil. Supply both to lock down the container.
	// ValidatePodSecurityContext enforces that RunAsUser is set whenever
	// RunAsNonRoot is true (the wrapper image runs as a named, non-numeric user,
	// so omitting RunAsUser produces an unsatisfiable kubelet contract).
	RunAsNonRoot bool
	RunAsUser    *int64

	// RunAttempt is the zero-based boot-crash-respawn counter for this agent
	// run. It is appended to PodName to form RUN_ID so that push-metrics from
	// successive respawns of the same Task are stored under distinct keys and do
	// not overwrite each other. Callers should pass the current
	// tatara.dev/boot-crash-attempts annotation value (0 on the first spawn).
	RunAttempt int

	// FSGroup, when non-nil, sets the pod-level SecurityContext fsGroup so
	// mounted-volume ownership matches the runtime group (the same fix the CI
	// runners needed for Ceph RBD). Nil means no pod-level SecurityContext.
	FSGroup *int64

	// NodeSelector, Tolerations and Affinity control Pod placement. All three
	// are cluster-specific and must be supplied by the helmfile, not baked into
	// the chart. Nil/empty means no scheduling constraint (default).
	NodeSelector map[string]string
	Tolerations  []corev1.Toleration
	Affinity     *corev1.Affinity

	// SerenaURL, when non-empty, is the in-cluster URL of the Serena
	// code-intelligence MCP server. Injected as TATARA_SERENA_URL into the agent
	// pod so the wrapper registers it as an HTTP MCP server (mirroring
	// TATARA_GRAFANA_MCP_URL). Empty by default; Phase 1 wires the code path only,
	// no server is deployed. Phase 2 sets this from tatara-helmfile.
	SerenaURL string

	// S3 conversation persistence (issue #114). Empty S3Bucket disables the
	// feature: BuildPod injects no S3/conversation env, so the wrapper behaves
	// exactly as before. S3SecretName holds the AWS creds
	// (keys AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY), injected via SecretKeyRef;
	// empty means the wrapper falls back to the default credential chain (IRSA).
	S3Endpoint       string
	S3Bucket         string
	S3Region         string
	S3KeyPrefix      string
	S3ForcePathStyle bool
	S3SecretName     string
}

// ValidatePodSecretRefs returns an error when any secret-name field required to
// build the wrapper Pod is empty. Call this before BuildPod/ensurePodAndService
// so that a mis-provisioned Project produces a clear operator-side error instead
// of an opaque kubelet CreateContainerConfigError.
func ValidatePodSecretRefs(project *tatarav1alpha1.Project, cfg PodConfig) error {
	if project.Spec.ScmSecretRef == "" {
		return fmt.Errorf("agent: ScmSecretRef is empty on Project %q; cannot build wrapper Pod", project.Name)
	}
	if cfg.AnthropicSecretName == "" {
		return fmt.Errorf("agent: AnthropicSecretName is empty in PodConfig; cannot build wrapper Pod")
	}
	if cfg.CLIOIDCSecretName == "" {
		return fmt.Errorf("agent: CLIOIDCSecretName is empty in PodConfig; cannot build wrapper Pod")
	}
	return nil
}

// ValidatePodSecurityContext returns an error when cfg.RunAsNonRoot is true but
// cfg.RunAsUser is nil. The wrapper image runs as a named (non-numeric) user,
// so RunAsNonRoot without RunAsUser is an unsatisfiable contract for the
// kubelet: it will reject the container with CreateContainerConfigError. Fail
// fast here (at config load / before BuildPod) so the error surfaces as a
// clear operator-side message rather than an opaque per-spawn kubelet error.
func ValidatePodSecurityContext(cfg PodConfig) error {
	if cfg.RunAsNonRoot && cfg.RunAsUser == nil {
		return fmt.Errorf("agent: RunAsNonRoot=true requires RunAsUser to be set (the wrapper image runs as a named user); set AGENT_RUN_AS_USER in the operator config")
	}
	return nil
}

// imagePullSecrets returns a one-element slice when cfg.ImagePullSecret is set,
// else nil, so the agent Pod can pull the wrapper image from a private registry.
func imagePullSecrets(cfg PodConfig) []corev1.LocalObjectReference {
	if cfg.ImagePullSecret == "" {
		return nil
	}
	return []corev1.LocalObjectReference{{Name: cfg.ImagePullSecret}}
}

// PodName returns the wrapper Pod (and Service) name for a Task. It prefers the
// descriptive name stamped at creation (PodNameAnnotation); legacy Tasks without
// it fall back to the deterministic wrapper-<task-name>.
func PodName(task *tatarav1alpha1.Task) string {
	if n := task.Annotations[PodNameAnnotation]; n != "" {
		return n
	}
	return "wrapper-" + task.Name
}

// providerAbbrev maps an SCM provider to its short Pod-name segment.
func providerAbbrev(provider string) string {
	switch provider {
	case "github":
		return "gh"
	case "gitlab":
		return "gl"
	default:
		return provider
	}
}

// podNameSuffix is the work-item segment of a wrapper Pod name: the brainstorm /
// health-check marker, the issue/mr number, or "scan" for Tasks not bound to a
// work item. The health-check activity reuses Kind "brainstorm", so it is
// disambiguated by the activity label to avoid a pod-name collision when both
// project-scoped activities target the same primary repo.
func podNameSuffix(task *tatarav1alpha1.Task) string {
	if task.Spec.Kind == "brainstorm" {
		if task.Labels[tatarav1alpha1.LabelActivity] == "healthCheck" {
			return "healthcheck"
		}
		return "brainstorm"
	}
	if task.Spec.Kind == "incident" {
		if g := task.Labels[tatarav1alpha1.LabelAlertGroup]; g != "" {
			return "incident-" + g
		}
		return "incident"
	}
	if s := task.Spec.Source; s != nil && s.Number > 0 {
		if s.IsPR {
			return fmt.Sprintf("mr-%d", s.Number)
		}
		return fmt.Sprintf("issue-%d", s.Number)
	}
	return "scan"
}

// BuildPodName composes the descriptive wrapper Pod/Service name for a Task:
//
//	tatara-<project>-<gh|gl>-<repo>-<issue-N|mr-N|brainstorm|scan>
//
// repoRef is dropped when empty (project-board items not bound to a repo). The
// result is sanitized to a DNS-1123 label and capped at 63 chars (the Service
// name limit).
func BuildPodName(projectName, provider, repoRef string, task *tatarav1alpha1.Task) string {
	parts := []string{"tatara", projectName, providerAbbrev(provider)}
	if repoRef != "" {
		parts = append(parts, repoRef)
	}
	parts = append(parts, podNameSuffix(task))
	return sanitizeDNS1123(strings.Join(parts, "-"))
}

// StampPodName sets the descriptive Pod-name annotation on a freshly-built Task,
// creating the annotation map when absent. Call it at Task creation, after Kind
// and Source are set.
func StampPodName(task *tatarav1alpha1.Task, projectName, provider, repoRef string) {
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[PodNameAnnotation] = BuildPodName(projectName, provider, repoRef, task)
}

// sanitizeDNS1123 lowercases s, collapses every run of non-[a-z0-9] into a single
// '-', trims leading/trailing '-', and caps the result at 63 chars (the DNS-1123
// label limit Services require).
func sanitizeDNS1123(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	prevDash := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevDash = false
		} else if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	return out
}

func podLabels(task *tatarav1alpha1.Task) map[string]string {
	l := map[string]string{
		"app.kubernetes.io/name": "tatara-operator",
		LabelComponent:           ComponentAgent,
		LabelManagedBy:           ManagedByValue,
		LabelTask:                task.Name,
	}
	// Task UID lets the reaper distinguish a live pod from one left over by a
	// prior Task incarnation that reused the same name. Omitted when empty (the
	// UID is unset on a Task object built in unit tests).
	if task.UID != "" {
		l[LabelTaskUID] = string(task.UID)
	}
	return l
}

// WrapperPodSelector is the label selector matching every wrapper Pod/Service
// the operator manages, used by the orphan reaper to enumerate candidates.
func WrapperPodSelector() map[string]string {
	return map[string]string{
		LabelManagedBy: ManagedByValue,
		LabelComponent: ComponentAgent,
	}
}

func ownerRef(task *tatarav1alpha1.Task) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: tatarav1alpha1.GroupVersion.String(),
		Kind:       "Task",
		Name:       task.Name,
		UID:        task.UID,
		Controller: &controller,
	}
}

func secretEnv(name, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}
}

// slugifyTitle lowercases s, collapses every run of non-[a-z0-9] into a single
// '-', trims leading/trailing '-', and caps at 40 chars (trimmed again so a cut
// never leaves a trailing '-').
func slugifyTitle(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	prevDash := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevDash = false
		} else if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 40 {
		out = strings.Trim(out[:40], "-")
	}
	return out
}

// branchKind maps a Task to a conventional branch prefix.
func branchKind(t *tatarav1alpha1.Task) string {
	switch t.Spec.Kind {
	case "issueLifecycle", "incident":
		return "fix"
	case "implement":
		return "feat"
	default: // review, brainstorm, healthCheck, selfImprove, triageIssue
		return "chore"
	}
}

// TaskBranch is the deterministic work branch all of the operator write-back,
// the turn prompts, and the wrapper agree on. When the Task carries an issue/PR
// number it is tatara/<kind>-<number>-<slug>; otherwise tatara/task-<task-name>.
func TaskBranch(t *tatarav1alpha1.Task) string {
	if t.Spec.Source != nil && t.Spec.Source.Number > 0 {
		base := fmt.Sprintf("tatara/%s-%d", branchKind(t), t.Spec.Source.Number)
		if slug := slugifyTitle(t.Spec.Source.Title); slug != "" {
			base += "-" + slug
		}
		if len(base) > 63 {
			base = strings.Trim(base[:63], "-")
		}
		return base
	}
	return "tatara/task-" + t.Name
}

// ConversationKey is the stable per-Task S3 object key under which the wrapper
// stores and restores the Claude conversation transcript (issue #114). It is
// keyed by the issue/PR number when present (human-readable and stable across
// the lifecycle phases, since the Task is 1:1 with the issue), else by the Task
// name. The wrapper's storage client prepends the configured key prefix.
// task.Status.ConversationObjectKey overrides this when set (e.g. a forked key
// for a brainstorm-derived issue, subtask 8).
func ConversationKey(task *tatarav1alpha1.Task) string {
	if task.Status.ConversationObjectKey != "" {
		return task.Status.ConversationObjectKey
	}
	if task.Spec.Source != nil && task.Spec.Source.Number > 0 {
		kind := "issue"
		if task.Spec.Source.IsPR {
			kind = "pr"
		}
		if task.Spec.RepositoryRef != "" {
			return fmt.Sprintf("%s/%s/%s-%d.jsonl", task.Spec.ProjectRef, task.Spec.RepositoryRef, kind, task.Spec.Source.Number)
		}
		return fmt.Sprintf("%s/%s-%d.jsonl", task.Spec.ProjectRef, kind, task.Spec.Source.Number)
	}
	return fmt.Sprintf("%s/task-%s.jsonl", task.Spec.ProjectRef, task.Name)
}

// BuildPod returns the wrapper Pod for a Task, owner-referenced to the Task.
// repos is the full list of Project Repositories; the task's own repo is placed
// first in TATARA_REPOS when repo is non-nil. When repo is nil (project-scoped
// task such as brainstorm/healthCheck), REPO_URL and REPO_BRANCH are omitted and
// TATARA_REPOS is set to all repos sorted by name (deterministic, no primary).
func BuildPod(project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, repos []tatarav1alpha1.Repository, memoryEndpoint string, cfg PodConfig) *corev1.Pod {
	env := []corev1.EnvVar{}
	// Branch wiring. Normal tasks push to TASK_BRANCH. An MR review (issue #114
	// decision 4) instead checks out the PR head READ-ONLY via CHECKOUT_BRANCH and
	// leaves TASK_BRANCH empty so the wrapper never pushes (and cannot clobber the
	// user's PR).
	taskBranchVal := TaskBranch(task)
	checkoutBranchVal := ""
	if task.Spec.Kind == "review" {
		if hb := task.Annotations[tatarav1alpha1.AnnReviewHeadBranch]; hb != "" {
			taskBranchVal = ""
			checkoutBranchVal = hb
		}
	}
	if repo != nil {
		env = append(env,
			corev1.EnvVar{Name: "REPO_URL", Value: repo.Spec.URL},
			corev1.EnvVar{Name: "REPO_BRANCH", Value: repo.Spec.DefaultBranch},
		)
	}
	env = append(env, []corev1.EnvVar{
		{Name: "MODEL", Value: project.Spec.Agent.Model},
		{Name: "EFFORT", Value: project.Spec.Agent.Effort},
		{Name: "PERMISSION_MODE", Value: project.Spec.Agent.PermissionMode},
		{Name: "TURN_TIMEOUT_SECONDS", Value: strconv.Itoa(project.Spec.Agent.TurnTimeoutSeconds)},
		{Name: "DEFAULT_CALLBACK_URL", Value: strings.TrimSuffix(cfg.CallbackURL, "/") + "/internal/turn-complete"},
		// Push-metrics: the wrapper Pod is too short-lived to be reliably
		// scraped, so it pushes its /metrics to the operator's push-receiver
		// (same internal listener as the callback), keyed by this run's id.
		// RUN_ID includes the RunAttempt counter so successive boot-crash
		// respawns of the same Task store their metrics under distinct keys
		// and do not overwrite each other. POD_NAME stays stable (equals the
		// Pod/Service name) for addressing the wrapper HTTP endpoint.
		{Name: "OPERATOR_PUSH_URL", Value: strings.TrimSuffix(cfg.CallbackURL, "/") + "/internal/metrics/push"},
		{Name: "RUN_ID", Value: PodName(task) + "-" + strconv.Itoa(cfg.RunAttempt)},
		{Name: "POD_NAME", Value: PodName(task)},
		// Task identity: lets the agent address MCP tools without repeating args.
		{Name: "TATARA_TASK", Value: task.Name},
		{Name: "TATARA_PROJECT", Value: project.Name},
		// Work branch the wrapper checks out and pushes; the operator opens the
		// PR from this same branch (see TaskBranch). Empty for review tasks, which
		// check out the PR head via CHECKOUT_BRANCH and never push.
		{Name: "TASK_BRANCH", Value: taskBranchVal},
		// Per-project memory endpoint: the agent's tatara-cli memory MCP reads
		// TATARA_MEMORY_URL to reach this Project's tatara-memory service.
		{Name: "TATARA_MEMORY_URL", Value: memoryEndpoint},
		// Operator REST URL: the agent's tatara-cli task_*/subtask_* MCP tools reach
		// the operator API at TATARA_OPERATOR_URL.
		{Name: "TATARA_OPERATOR_URL", Value: cfg.OperatorURL},
		// Per-project chat endpoint: the agent's tatara-cli chat MCP tools reach
		// the in-cluster chat service at TATARA_CHAT_URL instead of falling through
		// to the public ingress default (DefaultChatBaseURL in cli config.go).
		{Name: "TATARA_CHAT_URL", Value: fmt.Sprintf("http://chat-%s.%s.svc:8080", project.Name, cfg.Namespace)},
		// OIDC config: the wrapper enforces bearer tokens with this issuer and audience.
		{Name: "OIDC_ISSUER", Value: cfg.OIDCIssuer},
		{Name: "OIDC_AUDIENCE", Value: "tatara-claude-code-wrapper"},
		secretEnv("CLAUDE_CODE_OAUTH_TOKEN", cfg.AnthropicSecretName, "oauth-token"),
		secretEnv("GIT_TOKEN", project.Spec.ScmSecretRef, "token"),
		secretEnv("CLI_OIDC_CLIENT_ID", cfg.CLIOIDCSecretName, "client-id"),
		secretEnv("CLI_OIDC_CLIENT_SECRET", cfg.CLIOIDCSecretName, "client-secret"),
	}...)
	// Review tasks check out the PR head read-only (no push); see taskBranchVal.
	if checkoutBranchVal != "" {
		env = append(env, corev1.EnvVar{Name: "CHECKOUT_BRANCH", Value: checkoutBranchVal})
	}
	// Inject callback HMAC secret when configured so the wrapper can sign its
	// turn-complete callbacks (finding 1/r3). Delivered via SecretKeyRef (NOT a
	// literal env value) so the secret never lands in the Pod spec / etcd object
	// in plaintext, matching every other agent secret. Omitted when no secret
	// name is set so existing deployments without HMAC work unchanged.
	if cfg.CallbackHMACSecretName != "" {
		env = append(env, secretEnv("CALLBACK_HMAC_SECRET", cfg.CallbackHMACSecretName, CallbackHMACSecretKey))
	}

	// Conversation persistence (issue #114): inject the S3 connection config plus
	// this Task's stable conversation object key so the wrapper uploads the
	// transcript after each turn and restores it on boot. Gated on S3Bucket: with
	// no bucket configured NO S3 env is emitted and the wrapper behaves as before.
	// AWS creds come via SecretKeyRef from S3SecretName; an empty secret name
	// leaves the wrapper on the default credential chain (IRSA).
	//
	// Full resume vs compaction (decision 2) is mutually exclusive and gated by
	// the pending-handover-resume annotation, which maybeMarkHandoverResume sets
	// when LastTurnInputTokens crosses HandoverThresholdPercent (25%) of the
	// context window:
	//   - annotation unset (under threshold): emit CONVERSATION_SESSION_ID so the
	//     wrapper does a FULL conversation replay (claude --resume).
	//   - annotation "true" (at/over threshold): SKIP CONVERSATION_SESSION_ID so
	//     the wrapper starts fresh and implementPrompt injects the compacted
	//     "## Resume from handover" text instead. CONVERSATION_OBJECT_KEY is still
	//     emitted so the fresh (compacted) session is itself persisted and the
	//     next turn-complete records its new, shorter sessionId.
	if cfg.S3Bucket != "" {
		env = append(env,
			corev1.EnvVar{Name: "S3_ENDPOINT", Value: cfg.S3Endpoint},
			corev1.EnvVar{Name: "S3_BUCKET", Value: cfg.S3Bucket},
			corev1.EnvVar{Name: "S3_REGION", Value: cfg.S3Region},
			corev1.EnvVar{Name: "S3_KEY_PREFIX", Value: cfg.S3KeyPrefix},
			corev1.EnvVar{Name: "S3_FORCE_PATH_STYLE", Value: strconv.FormatBool(cfg.S3ForcePathStyle)},
			corev1.EnvVar{Name: "CONVERSATION_OBJECT_KEY", Value: ConversationKey(task)},
		)
		compacting := task.Annotations[tatarav1alpha1.AnnPendingHandoverResume] == "true"
		if task.Status.SessionID != "" && !compacting {
			env = append(env, corev1.EnvVar{Name: "CONVERSATION_SESSION_ID", Value: task.Status.SessionID})
		}
		// Fork source (issue #114 decision 3): on the first run of a
		// brainstorm-derived issue, the wrapper copies this parent conversation
		// onto the issue's own key and resumes it. Ignored once the issue has its
		// own conversation (CONVERSATION_SESSION_ID set, normal resume wins).
		if fk := task.Annotations[tatarav1alpha1.AnnForkFromConversationKey]; fk != "" {
			env = append(env, corev1.EnvVar{Name: "CONVERSATION_FORK_FROM_KEY", Value: fk})
		}
		if cfg.S3SecretName != "" {
			env = append(env,
				secretEnv("AWS_ACCESS_KEY_ID", cfg.S3SecretName, "AWS_ACCESS_KEY_ID"),
				secretEnv("AWS_SECRET_ACCESS_KEY", cfg.S3SecretName, "AWS_SECRET_ACCESS_KEY"),
			)
		}
	}

	// Lifecycle hooks: emit one HOOK_<NAME> env var per non-empty command so the
	// wrapper runs it at the matching lifecycle point. Omitted hooks produce no
	// env var (the wrapper treats an unset/empty command as "no hook").
	if h := project.Spec.Agent.Hooks; h != nil {
		for _, hk := range []struct{ name, cmd string }{
			{"HOOK_PRE_CLONE", h.PreClone},
			{"HOOK_POST_CLONE", h.PostClone},
			{"HOOK_CONVERSATION_START", h.ConversationStart},
			{"HOOK_CONVERSATION_RESTART", h.ConversationRestart},
			{"HOOK_AGENT_TURN_FINISHED", h.AgentTurnFinished},
			{"HOOK_CONVERSATION_FINISHED", h.ConversationFinished},
		} {
			if hk.cmd != "" {
				env = append(env, corev1.EnvVar{Name: hk.name, Value: hk.cmd})
			}
		}
	}

	if project.Spec.Grafana != nil && project.Spec.Grafana.Enabled {
		// Per-project read-only grafana-mcp endpoint. The wrapper registers this
		// as an HTTP MCP server so the agent can query Grafana for live debugging.
		env = append(env, corev1.EnvVar{
			Name:  "TATARA_GRAFANA_MCP_URL",
			Value: grafanamcp.MCPURL(project.Name, cfg.Namespace),
		})
	}

	if cfg.SerenaURL != "" {
		// Serena code-intelligence MCP endpoint (env-gated, off by default).
		// Mirrors TATARA_GRAFANA_MCP_URL: the wrapper registers it as an HTTP MCP
		// server when set. Phase 1 wires the path only; no server is deployed.
		env = append(env, corev1.EnvVar{Name: "TATARA_SERENA_URL", Value: cfg.SerenaURL})
	}

	// Work-item context: inject a human-readable ledger summary into the pod so the
	// agent knows upfront which issues/MRs it spans, without re-deriving from SCM.
	// Only emitted when WorkItems is non-empty to avoid a no-op env var on old Tasks.
	if ctx := tatarav1alpha1.WorkItemsContext(task); ctx != "" {
		env = append(env, corev1.EnvVar{Name: "TATARA_WORK_ITEMS", Value: ctx})
	}

	// Filter repos to the ledger-derived clone scope when the ledger is non-empty.
	// Spec.ReposInScope (declarative cross-repo Repository CR names) is unioned in
	// so a cross-repo task whose secondary repos are not yet in the ledger (they
	// only enter WorkItems via the Phase-3 backstop AFTER PRs exist) still clones
	// them. When the ledger is empty, fall back to the full project repo list for
	// backward-compatibility (pre-ledger Tasks carry no WorkItems).
	scopedRepos := repos
	if inScope := tatarav1alpha1.TaskReposInScope(task); len(inScope) > 0 {
		scopeSet := make(map[string]struct{}, len(inScope))
		for _, s := range inScope {
			scopeSet[s] = struct{}{}
		}
		// Declarative cross-repo scope is expressed as Repository CR names, a
		// different namespace from the owner/repo ledger slugs, so match it by Name.
		nameSet := make(map[string]struct{}, len(task.Spec.ReposInScope))
		for _, n := range task.Spec.ReposInScope {
			nameSet[n] = struct{}{}
		}
		var filtered []tatarav1alpha1.Repository
		for i := range repos {
			if _, ok := nameSet[repos[i].Name]; ok {
				filtered = append(filtered, repos[i])
				continue
			}
			if slug, err := scm.RepoSlugFromURL(repos[i].Spec.URL); err == nil {
				if _, ok := scopeSet[slug]; ok {
					filtered = append(filtered, repos[i])
				}
			}
		}
		if len(filtered) > 0 {
			scopedRepos = filtered
		}
	}

	if len(scopedRepos) > 0 {
		var entries []repoEntry
		if repo != nil {
			// Primary repo first, then the rest.
			entries = []repoEntry{{Name: repo.Name, URL: repo.Spec.URL, Branch: repo.Spec.DefaultBranch}}
			for i := range scopedRepos {
				if scopedRepos[i].Name != repo.Name {
					entries = append(entries, repoEntry{
						Name:   scopedRepos[i].Name,
						URL:    scopedRepos[i].Spec.URL,
						Branch: scopedRepos[i].Spec.DefaultBranch,
					})
				}
			}
		} else {
			// Project-scoped (no primary): sort all repos by name for determinism.
			sorted := make([]tatarav1alpha1.Repository, len(scopedRepos))
			copy(sorted, scopedRepos)
			// Sort by Name (stable deterministic order, same algorithm as brainstorm/healthCheck).
			for i := 1; i < len(sorted); i++ {
				for j := i; j > 0 && sorted[j].Name < sorted[j-1].Name; j-- {
					sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
				}
			}
			for _, r := range sorted {
				entries = append(entries, repoEntry{
					Name:   r.Name,
					URL:    r.Spec.URL,
					Branch: r.Spec.DefaultBranch,
				})
			}
		}
		// repoEntry contains only string fields so json.Marshal cannot fail;
		// ignoring the error is safe here.
		buf, _ := json.Marshal(entries)
		env = append(env, corev1.EnvVar{Name: "TATARA_REPOS", Value: string(buf)})
	}

	// Tool profile: gates the MCP tool surface the agent sees. Placed here,
	// with the other operator-set variables, so the operator-set profile is
	// authoritative -- a stray ExtraEnvs duplicate that comes after it is
	// ignored (first occurrence wins in a Pod env list).
	env = append(env, corev1.EnvVar{Name: "TATARA_TOOL_PROFILE", Value: toolProfileForKind(task.Spec.Kind)})

	// Operator-supplied extra envs are appended LAST, after every variable the
	// operator itself sets, so a stray extra named like a required variable
	// (e.g. TATARA_TASK) cannot silently shadow it -- the later duplicate in a
	// Pod env list does not override an earlier one for the operator's vars.
	env = append(env, project.Spec.Agent.ExtraEnvs...)

	labels := podLabels(task)
	if task.Spec.Kind == "brainstorm" && hasInternetSource(task.Annotations[tatarav1alpha1.AnnBrainstormSources]) {
		labels["tatara.io/egress"] = "internet"
	}

	wrapper := corev1.Container{
		Name:         "wrapper",
		Image:        project.Spec.Agent.Image,
		Env:          env,
		EnvFrom:      project.Spec.Agent.ExtraEnvsFrom,
		VolumeMounts: project.Spec.Agent.ExtraVolumeMounts,
		Ports:        []corev1.ContainerPort{{ContainerPort: wrapperPort}},
		Resources:    buildResourceRequirements(cfg),
		// On a non-zero exit the kubelet captures the tail of the container's
		// stdout/stderr into ContainerStatus.State.Terminated.Message, so a wrapper
		// that crashes before /readyz comes up leaves its cause on pod.Status for
		// handleBootCrash to surface (no logs API / RBAC needed). See bootCrashDetail.
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		SecurityContext:          buildSecurityContext(cfg),
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/readyz",
					Port: intstr.FromInt(wrapperPort),
				},
			},
		},
	}
	// Sidecars run alongside the wrapper (appended after it); init containers run
	// to completion before it. Both come straight from the Project spec.
	containers := append([]corev1.Container{wrapper}, project.Spec.Agent.ExtraSidecarContainers...)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            PodName(task),
			Namespace:       cfg.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(task)},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:    corev1.RestartPolicyNever,
			ImagePullSecrets: imagePullSecrets(cfg),
			NodeSelector:     cfg.NodeSelector,
			Tolerations:      cfg.Tolerations,
			Affinity:         cfg.Affinity,
			SecurityContext:  buildPodSecurityContext(cfg),
			InitContainers:   project.Spec.Agent.ExtraInitContainers,
			Volumes:          project.Spec.Agent.ExtraVolumes,
			Containers:       containers,
		},
	}
}

// BuildService returns the ClusterIP Service fronting the wrapper Pod. Its name
// equals the Pod name so the operator can address the wrapper at
// http://<name>.<ns>.svc:8080.
func BuildService(project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, cfg PodConfig) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            PodName(task),
			Namespace:       cfg.Namespace,
			Labels:          podLabels(task),
			OwnerReferences: []metav1.OwnerReference{ownerRef(task)},
		},
		Spec: corev1.ServiceSpec{
			Selector: podLabels(task),
			Ports: []corev1.ServicePort{{
				Port:       wrapperPort,
				TargetPort: intstr.FromInt(wrapperPort),
			}},
		},
	}
}

// BaseURL returns the in-cluster wrapper address for a Task's Service.
func BaseURL(task *tatarav1alpha1.Task, namespace string) string {
	return "http://" + PodName(task) + "." + namespace + ".svc:" + strconv.Itoa(wrapperPort)
}

// buildResourceRequirements constructs corev1.ResourceRequirements from the
// string scalars in PodConfig. Any empty string means no constraint for that
// resource dimension; the caller gets a zero-value entry for that key.
// Malformed quantities are skipped rather than panicking: resource.MustParse
// would crash the reconcile hot path on a single operator-config typo, turning
// a misconfiguration into a controller boot-crash. ValidatePodResourceQuantities
// catches typos at config load; this is the defence in depth on the hot path.
func buildResourceRequirements(cfg PodConfig) corev1.ResourceRequirements {
	req := corev1.ResourceList{}
	lim := corev1.ResourceList{}
	setQty := func(list corev1.ResourceList, name corev1.ResourceName, raw string) {
		if raw == "" {
			return
		}
		if q, err := resource.ParseQuantity(raw); err == nil {
			list[name] = q
		}
	}
	setQty(req, corev1.ResourceCPU, cfg.CPURequest)
	setQty(req, corev1.ResourceMemory, cfg.MemoryRequest)
	setQty(lim, corev1.ResourceCPU, cfg.CPULimit)
	setQty(lim, corev1.ResourceMemory, cfg.MemoryLimit)
	return corev1.ResourceRequirements{Requests: req, Limits: lim}
}

// ValidatePodResourceQuantities returns an error if any non-empty resource
// scalar in cfg is not a valid Kubernetes quantity. Call this at config load so
// a typo fails operator startup loudly instead of being silently dropped on the
// reconcile hot path.
func ValidatePodResourceQuantities(cfg PodConfig) error {
	for _, p := range []struct {
		name string
		raw  string
	}{
		{"cpuRequest", cfg.CPURequest},
		{"cpuLimit", cfg.CPULimit},
		{"memoryRequest", cfg.MemoryRequest},
		{"memoryLimit", cfg.MemoryLimit},
	} {
		if p.raw == "" {
			continue
		}
		if _, err := resource.ParseQuantity(p.raw); err != nil {
			return fmt.Errorf("agent: %s=%q is not a valid resource quantity: %w", p.name, p.raw, err)
		}
	}
	return nil
}

// buildSecurityContext returns a container SecurityContext from PodConfig.
// Returns nil when no security settings are configured.
func buildSecurityContext(cfg PodConfig) *corev1.SecurityContext {
	if !cfg.RunAsNonRoot && cfg.RunAsUser == nil {
		return nil
	}
	sc := &corev1.SecurityContext{}
	if cfg.RunAsNonRoot {
		b := true
		sc.RunAsNonRoot = &b
	}
	if cfg.RunAsUser != nil {
		sc.RunAsUser = cfg.RunAsUser
	}
	return sc
}

// buildPodSecurityContext returns a pod-level SecurityContext carrying FSGroup
// when configured, else nil (no constraint). FSGroup is pod-scoped, not
// container-scoped, so it lives here rather than in buildSecurityContext.
func buildPodSecurityContext(cfg PodConfig) *corev1.PodSecurityContext {
	if cfg.FSGroup == nil {
		return nil
	}
	return &corev1.PodSecurityContext{FSGroup: cfg.FSGroup}
}

// toolProfileForKind maps a Task Kind to the TATARA_TOOL_PROFILE value that
// the cli MCP server uses to gate the agent's tool surface. Returns "" for
// unknown/empty kinds (fail-open: the cli serves the full tool set).
func toolProfileForKind(kind string) string {
	switch kind {
	case "implement":
		return "implement"
	case "review":
		return "review"
	case "triageIssue":
		return "triage"
	case "brainstorm": // healthCheck shares Kind=brainstorm
		return "brainstorm"
	case "issueLifecycle":
		return "lifecycle"
	case "incident":
		return "incident"
	case "selfImprove":
		return "selfImprove"
	default:
		return "" // fail-open
	}
}

// hasInternetSource reports whether the comma-joined brainstorm sources list
// includes "internet", gating the egress NetworkPolicy pod label.
func hasInternetSource(csv string) bool {
	for _, s := range strings.Split(csv, ",") {
		if strings.TrimSpace(s) == "internet" {
			return true
		}
	}
	return false
}
