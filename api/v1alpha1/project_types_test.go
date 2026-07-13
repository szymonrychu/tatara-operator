package v1alpha1_test

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
)

func boolPtrPT(v bool) *bool { return &v }

func TestProjectFields(t *testing.T) {
	p := v1alpha1.Project{
		Spec: v1alpha1.ProjectSpec{
			ScmSecretRef:        "scm-secret",
			TriggerLabel:        "tatara",
			MaxConcurrentAgents: 3,
			Agent: v1alpha1.AgentSpec{
				Model:              "claude-sonnet-4-6",
				Image:              "wrapper:latest",
				PermissionMode:     "bypassPermissions",
				MaxTurnsPerTask:    50,
				TurnTimeoutSeconds: 1800,
			},
		},
		Status: v1alpha1.ProjectStatus{
			WebhookURL: "https://example/operator/webhooks/p",
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
		},
	}
	if p.Spec.Agent.MaxTurnsPerTask != 50 {
		t.Fatalf("MaxTurnsPerTask = %d, want 50", p.Spec.Agent.MaxTurnsPerTask)
	}
	if p.Status.WebhookURL == "" {
		t.Fatal("WebhookURL empty")
	}
}

func TestProjectSpec_AutoApproveTataraProposals_DefaultsFalse(t *testing.T) {
	var s v1alpha1.ProjectSpec
	if s.AutoApproveTataraProposals {
		t.Fatal("AutoApproveTataraProposals must be zero-value false")
	}
}

func TestRepositoryFields(t *testing.T) {
	r := v1alpha1.Repository{
		Spec: v1alpha1.RepositorySpec{
			ProjectRef:    "p",
			URL:           "https://example/repo.git",
			DefaultBranch: "main",
			IngestEnabled: boolPtrPT(true),
		},
		Status: v1alpha1.RepositoryStatus{
			Phase:              "Ingested",
			LastIngestedCommit: "abc123",
			JobName:            "ingest-1",
		},
	}
	if r.Spec.DefaultBranch != "main" {
		t.Fatalf("DefaultBranch = %q, want main", r.Spec.DefaultBranch)
	}
	if r.Status.Phase != "Ingested" {
		t.Fatalf("Phase = %q, want Ingested", r.Status.Phase)
	}
}

func TestProject_MemorySpecStatusDeepCopy(t *testing.T) {
	p := &v1alpha1.Project{
		Spec: v1alpha1.ProjectSpec{
			Memory: &v1alpha1.MemorySpec{
				PgInstances:  2,
				PgStorage:    "20Gi",
				Neo4jStorage: "5Gi",
			},
		},
		Status: v1alpha1.ProjectStatus{
			Memory: &v1alpha1.MemoryStatus{
				Phase:    "Ready",
				Endpoint: "http://mem-acme.tatara.svc:8080",
			},
		},
	}
	cp := p.DeepCopy()
	if cp.Spec.Memory == p.Spec.Memory {
		t.Fatal("spec.memory pointer not deep-copied")
	}
	if cp.Status.Memory == p.Status.Memory {
		t.Fatal("status.memory pointer not deep-copied")
	}
	if !reflect.DeepEqual(cp.Spec.Memory, p.Spec.Memory) {
		t.Fatalf("spec.memory mismatch: %+v vs %+v", cp.Spec.Memory, p.Spec.Memory)
	}
	if !reflect.DeepEqual(cp.Status.Memory, p.Status.Memory) {
		t.Fatalf("status.memory mismatch: %+v vs %+v", cp.Status.Memory, p.Status.Memory)
	}
	// Mutating the copy must not affect the original.
	cp.Spec.Memory.PgInstances = 9
	if p.Spec.Memory.PgInstances == 9 {
		t.Fatal("mutating copy mutated original (shallow copy)")
	}
}

func TestProject_MemoryNilSafe(t *testing.T) {
	p := &v1alpha1.Project{}
	cp := p.DeepCopy()
	if cp.Spec.Memory != nil || cp.Status.Memory != nil {
		t.Fatal("nil memory must deep-copy to nil")
	}
}

// TestProjectLifecycleConfigFields asserts the new lifecycle config fields
// exist on AgentSpec and ScmSpec and round-trip correctly through DeepCopy.
func TestProjectLifecycleConfigFields(t *testing.T) {
	p := &v1alpha1.Project{
		Spec: v1alpha1.ProjectSpec{
			ScmSecretRef: "scm",
			Agent: v1alpha1.AgentSpec{
				ContextWindowTokens:      200000,
				HandoverThresholdPercent: 50,
				MaxLifecycleIterations:   10,
			},
			Scm: &v1alpha1.ScmSpec{
				Provider:                "github",
				Owner:                   "acme",
				BotLogin:                "bot",
				BabysitDeadlineMinutes:  60,
				ConversationIdleMinutes: 60,
			},
		},
	}

	cp := p.DeepCopy()

	if cp.Spec.Agent.ContextWindowTokens != 200000 {
		t.Errorf("ContextWindowTokens = %d, want 200000", cp.Spec.Agent.ContextWindowTokens)
	}
	if cp.Spec.Agent.HandoverThresholdPercent != 50 {
		t.Errorf("HandoverThresholdPercent = %d, want 50", cp.Spec.Agent.HandoverThresholdPercent)
	}
	if cp.Spec.Agent.MaxLifecycleIterations != 10 {
		t.Errorf("MaxLifecycleIterations = %d, want 10", cp.Spec.Agent.MaxLifecycleIterations)
	}
	if cp.Spec.Scm.BabysitDeadlineMinutes != 60 {
		t.Errorf("BabysitDeadlineMinutes = %d, want 60", cp.Spec.Scm.BabysitDeadlineMinutes)
	}
	if cp.Spec.Scm.ConversationIdleMinutes != 60 {
		t.Errorf("ConversationIdleMinutes = %d, want 60", cp.Spec.Scm.ConversationIdleMinutes)
	}
	// Mutation safety
	cp.Spec.Agent.ContextWindowTokens = 999
	if p.Spec.Agent.ContextWindowTokens == 999 {
		t.Error("mutating copy mutated original")
	}
}

// TestAgentSpec_HooksAndExtrasDeepCopy asserts the lifecycle hooks and extra*
// pod-shaping fields exist on AgentSpec and deep-copy independently of the
// original (no shared slice/pointer backing).
func TestAgentSpec_HooksAndExtrasDeepCopy(t *testing.T) {
	p := &v1alpha1.Project{
		Spec: v1alpha1.ProjectSpec{
			Agent: v1alpha1.AgentSpec{
				Hooks: &v1alpha1.LifecycleHooks{
					PreClone:             "echo pre $1",
					PostClone:            "echo post $1",
					ConversationStart:    "echo start",
					ConversationRestart:  "echo restart",
					AgentTurnFinished:    "echo turn",
					ConversationFinished: "echo finished",
				},
				ExtraEnvs:              []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
				ExtraEnvsFrom:          []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}}}},
				ExtraVolumeMounts:      []corev1.VolumeMount{{Name: "vol", MountPath: "/data"}},
				ExtraVolumes:           []corev1.Volume{{Name: "vol"}},
				ExtraSidecarContainers: []corev1.Container{{Name: "sidecar", Image: "busybox"}},
				ExtraInitContainers:    []corev1.Container{{Name: "init", Image: "busybox"}},
			},
		},
	}

	cp := p.DeepCopy()

	if cp.Spec.Agent.Hooks == p.Spec.Agent.Hooks {
		t.Fatal("hooks pointer not deep-copied")
	}
	if !reflect.DeepEqual(cp.Spec.Agent, p.Spec.Agent) {
		t.Fatalf("agent spec mismatch after deep copy:\n%+v\nvs\n%+v", cp.Spec.Agent, p.Spec.Agent)
	}
	// Mutating the copy must not affect the original (independent backing arrays).
	cp.Spec.Agent.Hooks.PreClone = "mutated"
	cp.Spec.Agent.ExtraEnvs[0].Value = "mutated"
	if p.Spec.Agent.Hooks.PreClone == "mutated" {
		t.Fatal("mutating copied hook mutated original (shallow copy)")
	}
	if p.Spec.Agent.ExtraEnvs[0].Value == "mutated" {
		t.Fatal("mutating copied extra env mutated original (shared slice)")
	}
}

// TestAgentSpec_HooksNilSafe guards the common no-hooks case.
func TestAgentSpec_HooksNilSafe(t *testing.T) {
	p := &v1alpha1.Project{}
	cp := p.DeepCopy()
	if cp.Spec.Agent.Hooks != nil {
		t.Fatal("nil hooks must deep-copy to nil")
	}
	if cp.Spec.Agent.ExtraEnvs != nil || cp.Spec.Agent.ExtraSidecarContainers != nil {
		t.Fatal("nil extras must deep-copy to nil")
	}
}

func TestProjectRegisteredInScheme(t *testing.T) {
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if !s.Recognizes(v1alpha1.GroupVersion.WithKind("Project")) {
		t.Fatal("Project kind not recognized by scheme")
	}
	if !s.Recognizes(v1alpha1.GroupVersion.WithKind("Repository")) {
		t.Fatal("Repository kind not recognized by scheme")
	}
}

func TestBrainstormActivity_MaxOpenProposalsField(t *testing.T) {
	b := v1alpha1.BrainstormActivity{MaxOpenProposals: 3}
	if b.MaxOpenProposals != 3 {
		t.Fatalf("MaxOpenProposals = %d, want 3", b.MaxOpenProposals)
	}
}

func TestBrainstormActivity_StaleProposalDaysField(t *testing.T) {
	b := v1alpha1.BrainstormActivity{StaleProposalDays: 14}
	if b.StaleProposalDays != 14 {
		t.Fatalf("StaleProposalDays = %d, want 14", b.StaleProposalDays)
	}
	// Zero value (unset) must remain the disabled sentinel.
	var z v1alpha1.BrainstormActivity
	if z.StaleProposalDays != 0 {
		t.Fatalf("StaleProposalDays zero value = %d, want 0 (reaper disabled)", z.StaleProposalDays)
	}
}

func TestAgentSpec_MaxTaskTokensField(t *testing.T) {
	a := v1alpha1.AgentSpec{MaxTaskTokens: 3_000_000}
	if a.MaxTaskTokens != 3_000_000 {
		t.Fatalf("MaxTaskTokens = %d, want 3000000", a.MaxTaskTokens)
	}
	var z v1alpha1.AgentSpec
	if z.MaxTaskTokens != 0 {
		t.Fatalf("MaxTaskTokens zero value = %d, want 0 (backstop disabled)", z.MaxTaskTokens)
	}
}

// TestMemorySpec_DefaultFieldsExist guards Finding 7: the MemorySpec fields
// that carry +kubebuilder:default markers exist and accept the default values
// so the CRD schema and the Go struct stay in sync.
func TestMemorySpec_DefaultFieldsExist(t *testing.T) {
	m := v1alpha1.MemorySpec{
		PgInstances:  1,
		PgStorage:    "10Gi",
		Neo4jStorage: "10Gi",
	}
	if m.PgInstances != 1 {
		t.Errorf("PgInstances = %d, want 1", m.PgInstances)
	}
	if m.PgStorage != "10Gi" {
		t.Errorf("PgStorage = %q, want 10Gi", m.PgStorage)
	}
	if m.Neo4jStorage != "10Gi" {
		t.Errorf("Neo4jStorage = %q, want 10Gi", m.Neo4jStorage)
	}
}

func TestQueueDefaults(t *testing.T) {
	// nil Queue: capacity falls back to MaxConcurrentAgents (fix B1/A.6 repoint,
	// NOT MaxConcurrentTasks), alert to 1.
	p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxConcurrentAgents: 5, MaxOpenTasks: 6}}
	if got := p.QueueCapacity(); got != 5 {
		t.Fatalf("QueueCapacity nil-queue = %d, want 5", got)
	}
	if got := p.AlertCapacity(); got != 1 {
		t.Fatalf("AlertCapacity nil-queue = %d, want 1", got)
	}
	// explicit Queue overrides.
	p2 := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxConcurrentAgents: 5, Queue: &v1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 3}}}
	if p2.QueueCapacity() != 2 || p2.AlertCapacity() != 3 {
		t.Fatalf("explicit queue not honoured: %d/%d", p2.QueueCapacity(), p2.AlertCapacity())
	}
	// hard floor when nothing set anywhere.
	p3 := &v1alpha1.Project{}
	if p3.QueueCapacity() != 3 || p3.AlertCapacity() != 1 {
		t.Fatalf("hard floors wrong: %d/%d", p3.QueueCapacity(), p3.AlertCapacity())
	}
}

// TestQueueCapacity is the contract A.6 repointed helper: Queue.Capacity wins
// when set, else MaxConcurrentAgents when set, else the hard floor of 3.
func TestQueueCapacity(t *testing.T) {
	tests := []struct {
		name string
		proj v1alpha1.Project
		want int
	}{
		{"queue capacity wins", v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxConcurrentAgents: 5, Queue: &v1alpha1.QueueSpec{Capacity: 2}}}, 2},
		{"falls back to MaxConcurrentAgents", v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxConcurrentAgents: 7}}, 7},
		{"MaxConcurrentAgents zero floors at 3", v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxConcurrentAgents: 0}}, 3},
		{"nothing set floors at 3", v1alpha1.Project{}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.proj.QueueCapacity(); got != tt.want {
				t.Fatalf("QueueCapacity() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestQueueCapacity_PauseMustNotUseFloor documents WHY the full-project pause
// kill switch (fix S2, contract A.6) cannot be implemented as
// `QueueCapacity() == 0`: MaxConcurrentAgents == 0 still floors QueueCapacity
// at 3, so a `== 0` check on QueueCapacity would NEVER fire and would
// silently un-pause the project. The pause must be a direct
// `proj.Spec.MaxConcurrentAgents == 0` check at the top of admit().
func TestQueueCapacity_PauseMustNotUseFloor(t *testing.T) {
	p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxConcurrentAgents: 0}}
	if p.QueueCapacity() == 0 {
		t.Fatal("QueueCapacity() must never be 0 (floors at 3); a QueueCapacity()==0 pause check can never fire")
	}
	if p.QueueCapacity() != 3 {
		t.Fatalf("QueueCapacity() with MaxConcurrentAgents=0 = %d, want the hard floor 3", p.QueueCapacity())
	}
	if p.Spec.MaxConcurrentAgents != 0 {
		t.Fatal("the pause signal itself lives only on Spec.MaxConcurrentAgents")
	}
}

func TestAlertCapacity_DefaultsToOne(t *testing.T) {
	p := &v1alpha1.Project{}
	if got := p.AlertCapacity(); got != 1 {
		t.Fatalf("AlertCapacity() = %d, want 1", got)
	}
	p2 := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxConcurrentAgents: 10}}
	if got := p2.AlertCapacity(); got != 1 {
		t.Fatalf("AlertCapacity() unaffected by MaxConcurrentAgents = %d, want 1", got)
	}
}

func TestDefaultApprovalPhrases(t *testing.T) {
	want := []string{"lgtm", "approve", "approved", "ship it", "go ahead", "go", "implement it"}
	got := v1alpha1.DefaultApprovalPhrases()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultApprovalPhrases() = %v, want %v", got, want)
	}
}

// TestApprovalPhrases_EmptyNeverMeansAny asserts the D-B grammar decision:
// an empty/unset ScmSpec.ApprovalPhrases (or a nil Scm block) resolves to the
// DEFAULT closed wordlist, never to "any text approves".
func TestApprovalPhrases_EmptyNeverMeansAny(t *testing.T) {
	// nil Scm block.
	p := &v1alpha1.Project{}
	got := v1alpha1.EffectiveApprovalPhrases(p)
	if !reflect.DeepEqual(got, v1alpha1.DefaultApprovalPhrases()) {
		t.Fatalf("nil Scm: EffectiveApprovalPhrases() = %v, want the default list", got)
	}
	if len(got) == 0 {
		t.Fatal("an empty phrase list can NEVER mean 'any text approves'")
	}

	// explicit empty slice.
	p2 := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{Scm: &v1alpha1.ScmSpec{ApprovalPhrases: []string{}}}}
	got2 := v1alpha1.EffectiveApprovalPhrases(p2)
	if !reflect.DeepEqual(got2, v1alpha1.DefaultApprovalPhrases()) {
		t.Fatalf("empty ApprovalPhrases: EffectiveApprovalPhrases() = %v, want the default list", got2)
	}

	// explicit custom list overrides.
	custom := []string{"do it"}
	p3 := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{Scm: &v1alpha1.ScmSpec{ApprovalPhrases: custom}}}
	got3 := v1alpha1.EffectiveApprovalPhrases(p3)
	if !reflect.DeepEqual(got3, custom) {
		t.Fatalf("custom ApprovalPhrases not honoured: got %v, want %v", got3, custom)
	}
}

// TestModelFor asserts AgentSpec.ModelFor/EffortFor key on the AGENT kind
// passed by the caller, not the Task origin kind - the fix for the live cost
// regression where values/project-*/common.yaml keys modelByKind on the
// retired "triageIssue" kind, which is silently ignored and falls back to the
// Opus/high default.
func TestModelFor(t *testing.T) {
	a := v1alpha1.AgentSpec{
		Model:  "claude-opus-4-8",
		Effort: "xhigh",
		ModelByKind: map[string]string{
			"review":      "claude-sonnet-5",
			"triageIssue": "claude-haiku-4-5", // retired kind: must NOT match "implement"
		},
		EffortByKind: map[string]string{
			"review":      "medium",
			"triageIssue": "low",
		},
	}
	if got := a.ModelFor("review"); got != "claude-sonnet-5" {
		t.Fatalf("ModelFor(review) = %q, want claude-sonnet-5", got)
	}
	if got := a.EffortFor("review"); got != "medium" {
		t.Fatalf("EffortFor(review) = %q, want medium", got)
	}
	// implement has no explicit key -> falls back to Model/Effort, and must NOT
	// pick up the dead "triageIssue" entry.
	if got := a.ModelFor("implement"); got != "claude-opus-4-8" {
		t.Fatalf("ModelFor(implement) = %q, want fallback claude-opus-4-8 (must not match triageIssue)", got)
	}
	if got := a.EffortFor("implement"); got != "xhigh" {
		t.Fatalf("EffortFor(implement) = %q, want fallback xhigh (must not match triageIssue)", got)
	}
}

func TestModelFor_EmptyMapsFallBackToProjectWide(t *testing.T) {
	var a v1alpha1.AgentSpec
	a.Model = "claude-sonnet-5"
	a.Effort = "high"
	if got := a.ModelFor("brainstorm"); got != "claude-sonnet-5" {
		t.Fatalf("ModelFor with nil map = %q, want claude-sonnet-5", got)
	}
	if got := a.EffortFor("brainstorm"); got != "high" {
		t.Fatalf("EffortFor with nil map = %q, want high", got)
	}
}

// TestOperatorConstants pins the contract A.6 constant block (fix L30): these
// are exported so internal/controller and internal/stage share one
// definition instead of re-declaring them.
func TestOperatorConstants(t *testing.T) {
	if v1alpha1.ParkRetention != 7*24*time.Hour {
		t.Errorf("ParkRetention = %v, want 168h", v1alpha1.ParkRetention)
	}
	if v1alpha1.DeliveredRetention != 48*time.Hour {
		t.Errorf("DeliveredRetention = %v, want 48h", v1alpha1.DeliveredRetention)
	}
	if v1alpha1.RejectedRetention != 24*time.Hour {
		t.Errorf("RejectedRetention = %v, want 24h", v1alpha1.RejectedRetention)
	}
	if v1alpha1.FailedRetention != 7*24*time.Hour {
		t.Errorf("FailedRetention = %v, want 168h", v1alpha1.FailedRetention)
	}
	if v1alpha1.DocStageBudget != 2*time.Hour {
		t.Errorf("DocStageBudget = %v, want 2h", v1alpha1.DocStageBudget)
	}
	if v1alpha1.AdmissionStarvedBudget != 24*time.Hour {
		t.Errorf("AdmissionStarvedBudget = %v, want 24h", v1alpha1.AdmissionStarvedBudget)
	}
	// PodReadyTimeout IS agentBootDeadline (internal/controller/task_controller.go:35):
	// 5m, not 15m. internal/controller carries the equality assertion.
	if v1alpha1.PodReadyTimeout != 5*time.Minute {
		t.Errorf("PodReadyTimeout = %v, want 5m (== agentBootDeadline)", v1alpha1.PodReadyTimeout)
	}
	if v1alpha1.MaxMergeReentries != 3 {
		t.Errorf("MaxMergeReentries = %d, want 3", v1alpha1.MaxMergeReentries)
	}
	if v1alpha1.MaxDeployReentries != 3 {
		t.Errorf("MaxDeployReentries = %d, want 3", v1alpha1.MaxDeployReentries)
	}
	if v1alpha1.MaxHeadMoveReentries != 3 {
		t.Errorf("MaxHeadMoveReentries = %d, want 3", v1alpha1.MaxHeadMoveReentries)
	}
	if v1alpha1.MaxHumanReviewRounds != 5 {
		t.Errorf("MaxHumanReviewRounds = %d, want 5", v1alpha1.MaxHumanReviewRounds)
	}
	if v1alpha1.CIPollMinInterval != 20*time.Second {
		t.Errorf("CIPollMinInterval = %v, want 20s", v1alpha1.CIPollMinInterval)
	}
	if v1alpha1.ObjectByteBudget != 800_000 {
		t.Errorf("ObjectByteBudget = %d, want 800000", v1alpha1.ObjectByteBudget)
	}
}

// TestProjectSpec_NewCapacityFields asserts the contract A.6 ProjectSpec
// additions exist and round-trip through DeepCopy.
func TestProjectSpec_NewCapacityFields(t *testing.T) {
	p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{
		MaxConcurrentAgents: 3,
		AgentPodTTLSeconds:  3600,
		MaxNewTasksPerSweep: 5,
		MaxOpenTasks:        6,
		MaxBundleBytes:      400000,
	}}
	cp := p.DeepCopy()
	if cp.Spec.MaxConcurrentAgents != 3 {
		t.Errorf("MaxConcurrentAgents = %d, want 3", cp.Spec.MaxConcurrentAgents)
	}
	if cp.Spec.AgentPodTTLSeconds != 3600 {
		t.Errorf("AgentPodTTLSeconds = %d, want 3600", cp.Spec.AgentPodTTLSeconds)
	}
	if cp.Spec.MaxNewTasksPerSweep != 5 {
		t.Errorf("MaxNewTasksPerSweep = %d, want 5", cp.Spec.MaxNewTasksPerSweep)
	}
	if cp.Spec.MaxOpenTasks != 6 {
		t.Errorf("MaxOpenTasks = %d, want 6", cp.Spec.MaxOpenTasks)
	}
	if cp.Spec.MaxBundleBytes != 400000 {
		t.Errorf("MaxBundleBytes = %d, want 400000", cp.Spec.MaxBundleBytes)
	}
}

// TestAgentSpec_NewCaps asserts the contract A.6 AgentSpec additions exist.
func TestAgentSpec_NewCaps(t *testing.T) {
	a := v1alpha1.AgentSpec{
		MaxTurnsPerPod:    40,
		MaxTurnsPerTask:   300,
		MaxReviewRounds:   3,
		MaxPodRecreations: 3,
	}
	if a.MaxTurnsPerPod != 40 {
		t.Errorf("MaxTurnsPerPod = %d, want 40", a.MaxTurnsPerPod)
	}
	if a.MaxTurnsPerTask != 300 {
		t.Errorf("MaxTurnsPerTask = %d, want 300", a.MaxTurnsPerTask)
	}
	if a.MaxReviewRounds != 3 {
		t.Errorf("MaxReviewRounds = %d, want 3", a.MaxReviewRounds)
	}
	if a.MaxPodRecreations != 3 {
		t.Errorf("MaxPodRecreations = %d, want 3", a.MaxPodRecreations)
	}
}

// TestScmSpec_ApprovalPhrasesField asserts the field exists and deep-copies
// independently.
func TestScmSpec_ApprovalPhrasesField(t *testing.T) {
	p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{Scm: &v1alpha1.ScmSpec{
		Provider: "github", Owner: "acme", BotLogin: "bot",
		ApprovalPhrases: []string{"lgtm", "go ahead"},
	}}}
	cp := p.DeepCopy()
	if !reflect.DeepEqual(cp.Spec.Scm.ApprovalPhrases, []string{"lgtm", "go ahead"}) {
		t.Fatalf("ApprovalPhrases = %v", cp.Spec.Scm.ApprovalPhrases)
	}
	cp.Spec.Scm.ApprovalPhrases[0] = "mutated"
	if p.Spec.Scm.ApprovalPhrases[0] == "mutated" {
		t.Fatal("mutating copy mutated original (shared slice)")
	}
}

func TestBudgetConfigCopiesSpawnCeilingByKind(t *testing.T) {
	p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{TokenBudget: &v1alpha1.TokenBudgetSpec{
		Enabled: true, Mode: "claudeSubscription",
		SpawnCeilingByKind: map[string]int32{"brainstorm": 40, "incident": 98},
	}}}
	cfg := p.BudgetConfig(budget.Config{})
	if cfg.SpawnCeilingByKind["brainstorm"] != 40 || cfg.SpawnCeilingByKind["incident"] != 98 {
		t.Fatalf("ceilings not copied: %+v", cfg.SpawnCeilingByKind)
	}
}
