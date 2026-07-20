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
	"github.com/szymonrychu/tatara-operator/internal/slug"
	"github.com/szymonrychu/tatara-operator/internal/stage"
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
	// LabelAgentKind stamps the F.2 agent kind (stage.AgentKindFor) this Pod was
	// spawned for. The reaper compares it against the Task's CURRENT stage kind
	// to catch a pod left running past a stage advance (e.g. an incident's
	// investigating pod after the Task moved on to clarifying) - a class of
	// orphan none of the terminal/gone/idle rules covers.
	LabelAgentKind = "tatara.dev/agent-kind"

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
	OperatorURL         string // operator REST base URL for the agent's task_* MCP tools
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
		if g := task.Spec.DedupKey; g != "" {
			return "incident-" + g
		}
		return "incident"
	}
	if task.Spec.Kind == "refine" {
		return "refine"
	}
	if task.Spec.Kind == "documentation" {
		if sha := shortSourceHeadSHA(task); sha != "" {
			return "docs-" + sha
		}
		return "docs"
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
	return slug.SanitizeDNS1123(s, 63)
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
	// The agent kind this pod was built for (the CURRENT stage's kind, since
	// BuildPod is only ever called for the stage the Task is presently in). The
	// reaper reads this back to catch a pod left running past a stage advance;
	// omitted when the stage is pod-less (AgentKindFor returns "").
	if kind := stage.AgentKindFor(task.Status.Stage); kind != "" {
		l[LabelAgentKind] = kind
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
	return slug.SanitizeDNS1123(s, 40)
}

// branchKind maps a Task to a conventional branch prefix.
func branchKind(t *tatarav1alpha1.Task) string {
	switch t.Spec.Kind {
	case "incident":
		return "fix"
	case "implement":
		return "feat"
	case "documentation":
		return "docs"
	case "takeover":
		return "feat"
	default: // review, brainstorm, clarify, refine
		return "chore"
	}
}

// shortSourceHeadSHA returns the first 7 chars of the documentation Task's
// source-head-SHA annotation ("" if absent/short), used to disambiguate the
// SHA-keyed doc Task's branch/pod name (it has no issue/PR Number).
func shortSourceHeadSHA(t *tatarav1alpha1.Task) string {
	sha := t.Annotations[tatarav1alpha1.AnnSourceHeadSHA]
	if len(sha) < 7 {
		return sha
	}
	return sha[:7]
}

// TaskBranch is the deterministic work branch all of the operator write-back,
// the turn prompts, and the wrapper agree on. When the Task carries an issue/PR
// number it is tatara/<kind>-<number>-<slug>; a documentation Task (SHA-keyed,
// no Number) is tatara/docs-<short-sha>; otherwise tatara/task-<task-name>.
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
	if t.Spec.Kind == "documentation" {
		if sha := shortSourceHeadSHA(t); sha != "" {
			return fmt.Sprintf("tatara/%s-%s", branchKind(t), sha)
		}
	}
	return "tatara/task-" + t.Name
}

// branchEnvValues returns the (TASK_BRANCH, CHECKOUT_BRANCH) pair for a Task.
// Normal tasks push to TASK_BRANCH. An MR review (issue #114 decision 4) instead
// checks out the PR head READ-ONLY via CHECKOUT_BRANCH and leaves TASK_BRANCH
// empty, so the wrapper never pushes and cannot clobber the user's PR. A
// takeover Task pushes to the EXISTING MR head branch (AnnTakeoverHeadBranch)
// instead of the synthetic tatara/... branch, so the wrapper's bootstrap
// resumes and pushes back onto the same branch the abandoned MR already has.
func branchEnvValues(task *tatarav1alpha1.Task) (taskBranch, checkoutBranch string) {
	if task.Spec.Kind == kindReview {
		if hb := task.Annotations[tatarav1alpha1.AnnReviewHeadBranch]; hb != "" {
			return "", hb
		}
	}
	if hb := task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch]; hb != "" {
		return hb, ""
	}
	return TaskBranch(task), ""
}

// kindReview is the review Task kind (and the review agent kind - they share
// the string).
const kindReview = "review"

// AgentKind is the kind of AGENT the pod runs as. Under the stage machine that
// is status.agentKind, derived from the stage (contract F.2); it is NOT
// spec.Kind, which names the Task's ORIGIN. A legacy phase-driven Task carries
// no agentKind, so it falls back to spec.Kind and behaves exactly as before -
// the two models coexist until the cutover.
func AgentKind(task *tatarav1alpha1.Task) string {
	if task.Status.AgentKind != "" {
		return task.Status.AgentKind
	}
	return task.Spec.Kind
}

// agentRepoEnv is TATARA_REPO: the Repository CR name. Under the stage machine
// it is narrowed to the documentation agent (contract G.9) - every other agent
// kind is project-scoped and sees the whole enrolled repo set via TATARA_REPOS.
// Legacy phase-driven Tasks (no stage) keep the un-narrowed value.
func agentRepoEnv(task *tatarav1alpha1.Task) string {
	if task.Status.Stage == "" {
		return task.Spec.RepositoryRef
	}
	if AgentKind(task) == "documentation" {
		return task.Spec.RepositoryRef
	}
	return ""
}

// AgentEnv is the contract G.9 agent-pod env block: task identity, the agent
// kind (which the cli's MCP server and the wrapper's skill installer BOTH key
// their profiles on), the work branch, the pod TTL, and the wire contract
// version the wrapper is asserted against at pod-ready (G.10).
//
// The chat URL, the handoff register key and the conversation-persistence
// variables do not exist any more: all three were deleted at the cutover. The
// context bundle IS the continuation state.
func AgentEnv(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) []corev1.EnvVar {
	kind := AgentKind(task)
	taskBranch, _ := branchEnvValues(task)
	return []corev1.EnvVar{
		{Name: "TATARA_TASK", Value: task.Name},
		{Name: "TATARA_PROJECT", Value: project.Name},
		{Name: "TATARA_KIND", Value: kind},
		{Name: "TATARA_TOOL_PROFILE", Value: profileForKind(kind)},
		{Name: "TATARA_SKILL_PROFILE", Value: profileForKind(kind)},
		{Name: "TATARA_REPO", Value: agentRepoEnv(task)},
		{Name: "TASK_BRANCH", Value: taskBranch},
		{Name: "AGENT_POD_TTL_SECONDS", Value: strconv.Itoa(project.Spec.AgentPodTTLSeconds)},
		{Name: "TATARA_CONTRACT_VERSION", Value: strconv.Itoa(ContractVersion)},
	}
}

// BuildPod returns the wrapper Pod for a Task, owner-referenced to the Task.
// repos is the full list of Project Repositories; the task's own repo is placed
// first in TATARA_REPOS when repo is non-nil. When repo is nil (project-scoped
// task such as brainstorm), REPO_URL and REPO_BRANCH are omitted and
// TATARA_REPOS is set to all repos sorted by name (deterministic, no primary).
func BuildPod(project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, repos []tatarav1alpha1.Repository, memoryEndpoint string, cfg PodConfig) *corev1.Pod {
	env := []corev1.EnvVar{}
	_, checkoutBranchVal := branchEnvValues(task)
	targetRepo := ""
	if repo != nil {
		env = append(env,
			corev1.EnvVar{Name: "REPO_URL", Value: repo.Spec.URL},
			corev1.EnvVar{Name: "REPO_BRANCH", Value: repo.Spec.DefaultBranch},
		)
		targetRepo = repoComponentName(repo.Spec.URL)
	}
	env = append(env, []corev1.EnvVar{
		{Name: "MODEL", Value: modelForKindOnRepo(project, task.Spec.Kind, task.Labels[tatarav1alpha1.LabelActivity], targetRepo)},
		{Name: "EFFORT", Value: effortForKind(project, task.Spec.Kind, task.Labels[tatarav1alpha1.LabelActivity])},
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
		// Per-project memory endpoint: the agent's tatara-cli memory MCP reads
		// TATARA_MEMORY_URL to reach this Project's tatara-memory service.
		{Name: "TATARA_MEMORY_URL", Value: memoryEndpoint},
		// Operator REST URL: the agent's tatara-cli task_* MCP tools reach the
		// operator API at TATARA_OPERATOR_URL.
		{Name: "TATARA_OPERATOR_URL", Value: cfg.OperatorURL},
		// OIDC config: the wrapper enforces bearer tokens with this issuer and audience.
		{Name: "OIDC_ISSUER", Value: cfg.OIDCIssuer},
		{Name: "OIDC_AUDIENCE", Value: "tatara-claude-code-wrapper"},
		secretEnv("CLAUDE_CODE_OAUTH_TOKEN", cfg.AnthropicSecretName, "oauth-token"),
		secretEnv("GIT_TOKEN", project.Spec.ScmSecretRef, "token"),
		secretEnv("CLI_OIDC_CLIENT_ID", cfg.CLIOIDCSecretName, "client-id"),
		secretEnv("CLI_OIDC_CLIENT_SECRET", cfg.CLIOIDCSecretName, "client-secret"),
	}...)
	// The G.9 contract block (task identity, agent kind, the two profiles, the
	// pod TTL and the contract version). Placed before ExtraEnvs so the
	// operator-set values are authoritative: first occurrence wins in a Pod env
	// list, so a stray extra named like a required variable cannot shadow it.
	env = append(env, AgentEnv(project, task)...)
	// Review tasks check out the PR head read-only (no push); see branchEnvValues.
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

	if len(repos) > 0 {
		var entries []repoEntry
		if repo != nil {
			// Primary repo first, then the rest.
			entries = []repoEntry{{Name: repo.Name, URL: repo.Spec.URL, Branch: repo.Spec.DefaultBranch}}
			for i := range repos {
				if repos[i].Name != repo.Name {
					entries = append(entries, repoEntry{
						Name:   repos[i].Name,
						URL:    repos[i].Spec.URL,
						Branch: repos[i].Spec.DefaultBranch,
					})
				}
			}
		} else {
			// Project-scoped (no primary): sort all repos by name for determinism.
			sorted := make([]tatarav1alpha1.Repository, len(repos))
			copy(sorted, repos)
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

	// Bot git identity: attribute agent commits to the configured bot account
	// (not the generic tatara-agent default). The wrapper reads GIT_USER_NAME
	// and GIT_USER_EMAIL via bootstrap/repo.go. Only set when non-empty so a
	// Project without BotEmail keeps the wrapper default.
	if project.Spec.Scm != nil {
		if project.Spec.Scm.BotLogin != "" {
			env = append(env, corev1.EnvVar{Name: "GIT_USER_NAME", Value: project.Spec.Scm.BotLogin})
		}
		if project.Spec.Scm.BotEmail != "" {
			env = append(env, corev1.EnvVar{Name: "GIT_USER_EMAIL", Value: project.Spec.Scm.BotEmail})
		}
	}

	// The skills repo/ref the wrapper clones. TATARA_SKILL_PROFILE (which subset
	// of them to install) is part of the G.9 block, set by AgentEnv above.
	skillsRef := "main"
	if project.Spec.Agent.SkillsRef != "" {
		skillsRef = project.Spec.Agent.SkillsRef
	}
	env = append(env,
		corev1.EnvVar{Name: "TATARA_SKILLS_REPO", Value: skillsRepoDefault},
		corev1.EnvVar{Name: "TATARA_SKILLS_REF", Value: skillsRef},
	)

	// TATARA_WORKSPACE_FULL_CLONE signals the wrapper to clone every project
	// repo with full history and all branches. Set for project-scoped kinds that
	// need cross-branch context (incident forensics, refine backlog history); left
	// empty for brainstorm (a read-only code-quality proposer that only needs the
	// current default-branch source) and for repo-scoped kinds, so the wrapper's
	// default depth-1 shallow clone is used.
	fullCloneVal := ""
	if repo == nil && task.Spec.Kind != "brainstorm" {
		fullCloneVal = "true"
	}
	env = append(env, corev1.EnvVar{Name: "TATARA_WORKSPACE_FULL_CLONE", Value: fullCloneVal})

	// Documentation Tasks are repo-scoped to the docs repo; the triggering
	// component repo and SHA range ride as annotations so the skill can
	// shallow-clone the source repo and diff base..head.
	if task.Spec.Kind == "documentation" {
		env = append(env,
			corev1.EnvVar{Name: "TATARA_SOURCE_REPO", Value: task.Annotations[tatarav1alpha1.AnnSourceRepo]},
			corev1.EnvVar{Name: "TATARA_SOURCE_BASE_SHA", Value: task.Annotations[tatarav1alpha1.AnnSourceBaseSHA]},
			corev1.EnvVar{Name: "TATARA_SOURCE_HEAD_SHA", Value: task.Annotations[tatarav1alpha1.AnnSourceHeadSHA]},
		)
	}

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

// kindProfiles maps an AGENT kind to its profile value, shared by both
// TATARA_TOOL_PROFILE and TATARA_SKILL_PROFILE (contract G.9): the cli MCP
// server and the wrapper's skill installer carry the same map and key on the
// exact same agent kind.
//
// A missing key returns "". The cli then FAILS CLOSED: it serves ONLY the
// always-on tool set and does NOT register submit_outcome, so the pod cannot
// report a terminal outcome at all. It is not fail-open, whatever earlier
// comments here claimed. healthCheck shares Kind=brainstorm, so it is not a
// distinct entry.
var kindProfiles = map[string]string{
	"implement":     "implement",
	"review":        "review",
	"clarify":       "clarify",
	"brainstorm":    "brainstorm",
	"incident":      "incident",
	"refine":        "refine",
	"documentation": "documentation",
}

// profileForKind looks up kind in kindProfiles, returning "" (fail-open) for
// an unknown/empty kind.
func profileForKind(kind string) string {
	return kindProfiles[kind]
}

// skillsRepoDefault is the canonical clone URL for the tatara-agent-skills repo.
const skillsRepoDefault = "https://github.com/szymonrychu/tatara-agent-skills"

// resolveByKind resolves a per-kind override for a Task Kind+activity:
// healthCheck shares Kind=brainstorm but is recurring classification work, a
// prime Sonnet candidate, so a healthCheck-activity task first checks the
// "healthCheck" pseudo-key in byKind, then falls back to the kind entry (e.g.
// brainstorm), then fallback. Non-healthCheck tasks skip straight to the kind
// entry. A nil map or empty override value falls through. Shared by
// modelForKind/effortForKind so the precedence is defined exactly once.
func resolveByKind(byKind map[string]string, kind, activity, fallback string) string {
	if activity == "healthCheck" {
		if v := byKind["healthCheck"]; v != "" {
			return v
		}
	}
	if v := byKind[kind]; v != "" {
		return v
	}
	return fallback
}

// kindDefaultModel is the locked per-kind model tier for the 7-kind model
// (design decision, cross-repo contract). It is the fallback when the project
// sets no per-kind ModelByKind override: opus for the reasoning kinds
// (brainstorm/incident/clarify/implement/review), sonnet for the cheaper
// recurring kinds (documentation/refine). A project ModelByKind override still
// wins (resolveByKind precedence). Kinds absent here (retired legacy kinds) fall
// back to the project-wide Model as before.
var kindDefaultModel = map[string]string{
	"brainstorm":    "claude-opus-4-8",
	"incident":      "claude-opus-4-8",
	"clarify":       "claude-opus-4-8",
	"implement":     "claude-opus-4-8",
	"review":        "claude-opus-4-8",
	"documentation": "claude-sonnet-5",
	"refine":        "claude-sonnet-5",
}

// modelForKind resolves the MODEL env for a Task Kind+activity. The fallback is
// the locked per-kind default (kindDefaultModel) when one exists, else the
// project-wide Model. See resolveByKind for the healthCheck pseudo-key precedence
// and the project ModelByKind override.
func modelForKind(project *tatarav1alpha1.Project, kind, activity string) string {
	fallback := project.Spec.Agent.Model
	if def, ok := kindDefaultModel[kind]; ok {
		fallback = def
	}
	return resolveByKind(project.Spec.Agent.ModelByKind, kind, activity, fallback)
}

// helmfileTargetRepo is the terminal self-heal repo (the tier-revert flow opens
// its revert MR here). Keyed on the repo component name (URL slug), matching the
// controller's helmfileRepoName.
const helmfileTargetRepo = "tatara-helmfile"

// modelFloorKinds are the reasoning kinds pinned to their locked opus default
// when the task targets the self-heal repo (helmfileTargetRepo). documentation
// and refine (the cheap, freely-tierable kinds) are deliberately absent.
var modelFloorKinds = map[string]bool{
	"brainstorm": true, "incident": true, "clarify": true, "implement": true, "review": true,
}

// modelFloorAppliesOnRepo reports whether the tier-revert self-heal model floor
// applies to a (kind, activity) task targeting targetRepo. The floor pins a
// reasoning-kind task on tatara-helmfile to its locked opus default: the
// tier-revert incident opens its revert MR against tatara-helmfile and the very
// implement/review that must FIX a broken or downgraded ModelByKind lives on that
// repo, so it must not run on the same broken tier being reverted. healthCheck (a
// brainstorm-kind recurring classification) is exempt so it stays tierable, and
// component-repo tiering (any other repo) is unaffected: this is the narrow
// "bypass modelForKind for tier-revert-originated Tasks" the floor sanctions,
// identified structurally by the terminal-repo target rather than a propagated
// origin marker (any helmfile change deserves opus reasoning regardless).
func modelFloorAppliesOnRepo(kind, activity, targetRepo string) bool {
	return targetRepo == helmfileTargetRepo && activity != "healthCheck" && modelFloorKinds[kind]
}

// modelForKindOnRepo resolves the MODEL env with the tier-revert self-heal floor
// applied when the task targets the terminal tatara-helmfile repo. targetRepo is
// the repo component name (URL slug). Empty/other repos resolve via modelForKind.
func modelForKindOnRepo(project *tatarav1alpha1.Project, kind, activity, targetRepo string) string {
	if modelFloorAppliesOnRepo(kind, activity, targetRepo) {
		if def, ok := kindDefaultModel[kind]; ok {
			return def
		}
	}
	return modelForKind(project, kind, activity)
}

// repoComponentName returns the repo component (URL slug tail) for a repository
// URL, e.g. "tatara-helmfile" for ".../szymonrychu/tatara-helmfile". Empty on a
// parse failure (the floor then does not apply, matching non-helmfile targets).
func repoComponentName(repoURL string) string {
	if _, name, err := scm.OwnerRepo(repoURL); err == nil {
		return name
	}
	return ""
}

// ModelForKind exports the MODEL resolution for controller callers that need to
// stamp the resolved model on Task.Status at pod-creation. repoURL is the target
// repository URL so the tier-revert self-heal floor is applied consistently with
// BuildPod.
func ModelForKind(project *tatarav1alpha1.Project, kind, activity, repoURL string) string {
	return modelForKindOnRepo(project, kind, activity, repoComponentName(repoURL))
}

// effortForKind resolves the EFFORT env for a Task Kind+activity, keying on
// the same "healthCheck" pseudo-key precedence as modelForKind.
func effortForKind(project *tatarav1alpha1.Project, kind, activity string) string {
	return resolveByKind(project.Spec.Agent.EffortByKind, kind, activity, project.Spec.Agent.Effort)
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
