# Incident-handling + git-identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mark incident-originated issues with `tatara-incident`, surface the alert rule as a typed Task field with a codifying dedup test, and attribute agent commits to the bot identity.

**Architecture:** All operator-side. Three independent components touching `api/v1alpha1` (new fields), `internal/controller` (label), `internal/webhook`+`internal/queue` (alert-rule plumbing), `internal/agent/pod.go` (git identity). New API fields require `make manifests`.

**Tech Stack:** Go (controller-runtime/kubebuilder), envtest (`KUBEBUILDER_ASSETS`), `mise exec -- go test`.

## Global Constraints

- Newest stable Go; pin in go.mod (already set). KISS. No tech-debt. No deferral.
- JSON slog only; no new metrics surface needed.
- TDD: failing test first for every unit.
- Spec field naming: `ScmSpec.IncidentLabel` (`incidentLabel`), `ScmSpec.BotEmail` (`botEmail`), `ProposedIssueSpec.Incident` (`incident`), `TaskSpec.AlertRule` (`alertRule`), `QueuedEventPayload.AlertRule` (`alertRule`).
- Default incident label = `tatara-incident`.
- After any `api/v1alpha1` field add: run `mise exec -- make manifests generate` and commit the regenerated CRDs (`config/crd/bases/*` + `crd-bases/` if mirrored).
- Test run: `KUBEBUILDER_ASSETS=$(mise exec -- go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 -p path) mise exec -- go test ./... -race -count=1`.
- The incident label is ADDITIVE: it must NOT be added to `managedPhaseLabels`/`lifecycleLabels` (those are mutually-exclusive phase labels swept by `setLifecycleLabel`).

---

## Component A: incident marker label

### Task A1: API fields (IncidentLabel + ProposedIssue.Incident)

**Files:**
- Modify: `api/v1alpha1/project_types.go` (ScmSpec, after `BrainstormingLabel` ~line 56)
- Modify: `api/v1alpha1/task_types.go` (ProposedIssueSpec, ~line 18-28)

**Interfaces:**
- Produces: `ScmSpec.IncidentLabel string`, `ProposedIssueSpec.Incident bool`.

- [ ] **Step 1: Add the fields**

In `project_types.go` ScmSpec, after the `BrainstormingLabel` field:
```go
	// IncidentLabel marks a proposal issue that originated from an incident
	// investigation. Additive: applied alongside BrainstormingLabel, never
	// swept by the phase-label reconciler. Defaults to "tatara-incident".
	IncidentLabel string `json:"incidentLabel,omitempty"`
```

In `task_types.go` ProposedIssueSpec, after `SystemicID`:
```go
	// Incident is true when this proposal was filed by an incident-investigation
	// agent; createProposal then adds the incident label to the tracker issue.
	Incident bool `json:"incident,omitempty"`
```

- [ ] **Step 2: Regenerate manifests**

Run: `mise exec -- make manifests generate`
Expected: CRD YAML under `config/crd/bases/` updated with `incidentLabel` and `incident` properties; build still compiles.

- [ ] **Step 3: Verify build**

Run: `mise exec -- go build ./...`
Expected: no errors.

- [ ] **Step 4: Commit**
```bash
git add api/v1alpha1/project_types.go api/v1alpha1/task_types.go config/crd
git commit -m "feat(api): add ScmSpec.IncidentLabel and ProposedIssueSpec.Incident"
```

### Task A2: incidentLabel resolver helper

**Files:**
- Modify: `internal/controller/labels.go` (add helper after `legacyLabels`)
- Test: `internal/controller/labels_test.go`

**Interfaces:**
- Consumes: `ScmSpec.IncidentLabel`.
- Produces: `func incidentLabel(s *tatarav1alpha1.ScmSpec) string`.

- [ ] **Step 1: Write the failing test**

Add to `labels_test.go`:
```go
func TestIncidentLabel_DefaultAndOverride(t *testing.T) {
	if got := incidentLabel(nil); got != "tatara-incident" {
		t.Fatalf("nil scm: want tatara-incident, got %q", got)
	}
	if got := incidentLabel(&tatarav1alpha1.ScmSpec{}); got != "tatara-incident" {
		t.Fatalf("empty: want tatara-incident, got %q", got)
	}
	if got := incidentLabel(&tatarav1alpha1.ScmSpec{IncidentLabel: "oncall"}); got != "oncall" {
		t.Fatalf("override: want oncall, got %q", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/controller/ -run TestIncidentLabel_DefaultAndOverride`
Expected: FAIL (`undefined: incidentLabel`).

- [ ] **Step 3: Implement the helper**

In `labels.go` after `legacyLabels`:
```go
// incidentLabel returns the additive label for incident-originated proposals.
// It is NOT a managed phase label (never swept by setLifecycleLabel).
func incidentLabel(s *tatarav1alpha1.ScmSpec) string {
	if s != nil && s.IncidentLabel != "" {
		return s.IncidentLabel
	}
	return "tatara-incident"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/controller/ -run TestIncidentLabel_DefaultAndOverride`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/controller/labels.go internal/controller/labels_test.go
git commit -m "feat(controller): add incidentLabel resolver (default tatara-incident)"
```

### Task A3: stamp Incident origin in proposeIssue

**Files:**
- Modify: `internal/restapi/handlers.go` (`proposeIssue` ~407, add `inflightIncidentTask` helper near `inflightBrainstormConversationKey` ~383)
- Test: `internal/restapi/handlers_test.go` (or the existing propose-issue test file)

**Interfaces:**
- Consumes: `tatarav1alpha1.TaskList`, `Task.Spec.Kind`, `Task.Status.Phase`.
- Produces: `func (s *Server) inflightIncidentTask(ctx context.Context, project string) bool`; sets `ProposedIssueSpec.Incident`.

- [ ] **Step 1: Write the failing test**

Locate the existing propose-issue test setup (search `proposeIssue` in `internal/restapi/*_test.go`). Add:
```go
func TestProposeIssue_StampsIncidentWhenIncidentInflight(t *testing.T) {
	// Reuse the existing propose-issue test harness pattern: a Server with a
	// fake client seeded with a Project + Repository. Seed an in-flight incident
	// Task for the project, POST propose_issue, then assert the created Task.
	s, ns := newProposeTestServer(t) // existing helper; adapt name to the file
	seedProject(t, s, ns, "proj", "repo")
	seedTask(t, s, ns, &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "inc-1", Namespace: ns},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "incident"},
	})
	rec := postProposeIssue(t, s, "proj", proposeIssueReq{
		RepositoryRef: "repo", Title: "Fix the broken thing in module X",
		Body: "details", Kind: "bug",
	})
	if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	task := newestProposalTask(t, s, ns, "proj")
	if task.Spec.ProposedIssue == nil || !task.Spec.ProposedIssue.Incident {
		t.Fatalf("incident-inflight proposal must set ProposedIssue.Incident=true")
	}
}

func TestProposeIssue_NoIncidentWhenOnlyBrainstormInflight(t *testing.T) {
	s, ns := newProposeTestServer(t)
	seedProject(t, s, ns, "proj", "repo")
	seedTask(t, s, ns, &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "bs-1", Namespace: ns},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "brainstorm"},
	})
	postProposeIssue(t, s, "proj", proposeIssueReq{
		RepositoryRef: "repo", Title: "Fix the broken thing in module X",
		Body: "details", Kind: "bug",
	})
	task := newestProposalTask(t, s, ns, "proj")
	if task.Spec.ProposedIssue != nil && task.Spec.ProposedIssue.Incident {
		t.Fatalf("brainstorm-only proposal must NOT set Incident")
	}
}
```
Note: if the test file lacks `newProposeTestServer`/`postProposeIssue`/`newestProposalTask`/`seedTask` helpers, write minimal inline equivalents using `httptest` + the fake client already used by sibling tests in that file. Match the existing harness style.

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/restapi/ -run TestProposeIssue_Stamps`
Expected: FAIL (`Incident` not set / helper undefined).

- [ ] **Step 3: Implement helper + stamp**

Add near `inflightBrainstormConversationKey` in `handlers.go`:
```go
// inflightIncidentTask reports whether the project has a non-terminal incident
// Task. Agent identity is shared OIDC, so an incident-investigation agent is
// inferred from the project's in-flight incident work (same project-level
// inference inflightBrainstormConversationKey uses).
func (s *Server) inflightIncidentTask(ctx context.Context, project string) bool {
	var tasks tatarav1alpha1.TaskList
	if err := s.c.List(ctx, &tasks, client.InNamespace(s.ns)); err != nil {
		return false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != project || t.Spec.Kind != "incident" {
			continue
		}
		if t.Status.Phase == "Succeeded" || t.Status.Phase == "Failed" {
			continue
		}
		return true
	}
	return false
}
```
In `proposeIssue`, where the `ProposedIssue` is built (line ~466), set the field:
```go
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: req.RepositoryRef, Title: req.Title, Body: req.Body, Kind: req.Kind,
				SystemicID: req.SystemicID,
				Incident:   s.inflightIncidentTask(r.Context(), projName),
			},
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/restapi/ -run TestProposeIssue_`
Expected: PASS (both new tests + existing propose-issue tests).

- [ ] **Step 5: Commit**
```bash
git add internal/restapi/handlers.go internal/restapi/*_test.go
git commit -m "feat(restapi): stamp ProposedIssue.Incident when incident investigation in flight"
```

### Task A4: apply incident label in createProposal

**Files:**
- Modify: `internal/controller/writeback.go` (`createProposal` ~626, the `labels := []string{brainstorming}` block)
- Test: `internal/controller/writeback_test.go` (or the proposal test file)

**Interfaces:**
- Consumes: `incidentLabel`, `task.Spec.ProposedIssue.Incident`.

- [ ] **Step 1: Write the failing test**

Find the existing createProposal test (search `createProposal` in `internal/controller/*_test.go`; there is a fake SCM writer capturing `CreateIssue` labels). Add:
```go
func TestCreateProposal_AddsIncidentLabelWhenIncident(t *testing.T) {
	// Reuse the createProposal harness: a TaskReconciler with a fake SCM writer
	// that records IssueReq.Labels. Seed a Task whose ProposedIssue.Incident=true.
	r, proj, fw := newProposalReconciler(t) // adapt to existing helper names
	task := proposalTask(t, "repo")
	task.Spec.ProposedIssue.Incident = true
	if _, err := r.createProposal(context.Background(), proj, task); err != nil {
		t.Fatalf("createProposal: %v", err)
	}
	labels := fw.lastCreateIssueLabels()
	require.Contains(t, labels, "tatara-brainstorming")
	require.Contains(t, labels, "tatara-incident")
}

func TestCreateProposal_NoIncidentLabelWhenNotIncident(t *testing.T) {
	r, proj, fw := newProposalReconciler(t)
	task := proposalTask(t, "repo")
	task.Spec.ProposedIssue.Incident = false
	if _, err := r.createProposal(context.Background(), proj, task); err != nil {
		t.Fatalf("createProposal: %v", err)
	}
	labels := fw.lastCreateIssueLabels()
	require.Contains(t, labels, "tatara-brainstorming")
	require.NotContains(t, labels, "tatara-incident")
}
```
If helper names differ, adapt to the sibling createProposal test's harness (same file).

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/controller/ -run TestCreateProposal_.*Incident`
Expected: FAIL (incident label absent).

- [ ] **Step 3: Implement**

In `createProposal`, change the label block (~626):
```go
	brainstorming, _, _, _ := lifecycleLabels(proj.Spec.Scm)
	labels := []string{brainstorming}
	if task.Spec.ProposedIssue.Incident {
		labels = append(labels, incidentLabel(proj.Spec.Scm))
	}
	body := task.Spec.ProposedIssue.Body
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/controller/ -run TestCreateProposal`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/controller/writeback.go internal/controller/*_test.go
git commit -m "feat(controller): add incident label to incident-originated proposals"
```

---

## Component B: alert-rule typed field + dedup codifying test

### Task B1: API fields (TaskSpec.AlertRule + QueuedEventPayload.AlertRule)

**Files:**
- Modify: `api/v1alpha1/task_types.go` (TaskSpec; QueuedEventPayload)

**Interfaces:**
- Produces: `TaskSpec.AlertRule string`, `QueuedEventPayload.AlertRule string`.

- [ ] **Step 1: Add fields**

In `TaskSpec`:
```go
	// AlertRule names the Grafana alert rule that produced an incident Task
	// (commonLabels.alertname, falling back to groupKey). Descriptive only; the
	// dedup key is the tatara.dev/alert-group hash label.
	AlertRule string `json:"alertRule,omitempty"`
```
In `QueuedEventPayload` (same file):
```go
	// AlertRule is carried from the incident webhook onto the built Task.
	AlertRule string `json:"alertRule,omitempty"`
```

- [ ] **Step 2: Regenerate manifests**

Run: `mise exec -- make manifests generate`
Expected: CRDs gain `alertRule` on Task and QueuedEvent payload.

- [ ] **Step 3: Verify build**

Run: `mise exec -- go build ./...`
Expected: no errors.

- [ ] **Step 4: Commit**
```bash
git add api/v1alpha1/task_types.go config/crd
git commit -m "feat(api): add AlertRule to TaskSpec and QueuedEventPayload"
```

### Task B2: populate AlertRule in createIncidentTask + BuildTaskFromQueuedEvent

**Files:**
- Modify: `internal/webhook/server.go` (`createIncidentTask` ~885)
- Modify: `internal/webhook/grafana.go` (add `alertRuleName`)
- Modify: `internal/queue/enqueue.go` (`BuildTaskFromQueuedEvent`)
- Test: `internal/webhook/grafana_test.go`, `internal/queue/enqueue_test.go`

**Interfaces:**
- Consumes: `GrafanaAlert.CommonLabels`, `GrafanaAlert.GroupKey`, `QueuedEventPayload.AlertRule`.
- Produces: `func alertRuleName(a GrafanaAlert) string`; `Task.Spec.AlertRule` set by `BuildTaskFromQueuedEvent`.

- [ ] **Step 1: Write the failing tests**

In `grafana_test.go`:
```go
func TestAlertRuleName_AlertnameThenGroupKey(t *testing.T) {
	a := GrafanaAlert{CommonLabels: map[string]string{"alertname": "HighCPU"}, GroupKey: "gk"}
	if got := alertRuleName(a); got != "HighCPU" {
		t.Fatalf("want HighCPU, got %q", got)
	}
	b := GrafanaAlert{GroupKey: "gk-only"}
	if got := alertRuleName(b); got != "gk-only" {
		t.Fatalf("fallback: want gk-only, got %q", got)
	}
}
```
In `enqueue_test.go`:
```go
func TestBuildTaskFromQueuedEvent_CopiesAlertRule(t *testing.T) {
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}}
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-x", Namespace: "ns"},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", AlertRule: "HighCPU"},
		},
	}
	task, err := BuildTaskFromQueuedEvent(qe, proj, scheme())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if task.Spec.AlertRule != "HighCPU" {
		t.Fatalf("want AlertRule=HighCPU, got %q", task.Spec.AlertRule)
	}
}
```
Use the existing `scheme()` helper in `enqueue_test.go` (search; if absent, build a runtime.Scheme with `tatarav1alpha1.AddToScheme`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/webhook/ -run TestAlertRuleName && mise exec -- go test ./internal/queue/ -run TestBuildTaskFromQueuedEvent_CopiesAlertRule`
Expected: FAIL (`alertRuleName` undefined; `AlertRule` not copied).

- [ ] **Step 3: Implement**

In `grafana.go`:
```go
// alertRuleName is the human-readable rule identity for an incident Task:
// the alertname common label, falling back to the raw group key.
func alertRuleName(a GrafanaAlert) string {
	if n := a.CommonLabels["alertname"]; n != "" {
		return n
	}
	return a.GroupKey
}
```
In `server.go` `createIncidentTask`, add to the payload literal:
```go
	payload := tatarav1.QueuedEventPayload{
		Kind:         "incident",
		Goal:         goal,
		GenerateName: "incident-",
		AlertRule:    alertRuleName(alert),
		Labels:       map[string]string{tatarav1.LabelActivity: "incident", tatarav1.LabelAlertGroup: groupHash},
		Annotations:  map[string]string{tatarav1.AnnGrafanaAlert: alertCtx},
	}
```
In `enqueue.go` `BuildTaskFromQueuedEvent`, add to the `Spec` literal:
```go
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: p.RepositoryRef,
			Goal:          p.Goal,
			Kind:          p.Kind,
			AlertRule:     p.AlertRule,
			Source:        p.Source,
			SystemicGroup: p.SystemicGroup,
		},
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/webhook/ ./internal/queue/`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/webhook/server.go internal/webhook/grafana.go internal/queue/enqueue.go internal/webhook/grafana_test.go internal/queue/enqueue_test.go
git commit -m "feat(incident): carry alert-rule name onto incident Task"
```

### Task B3: codify state-gated dedup

**Files:**
- Test: `internal/queue/enqueue_test.go`

**Interfaces:**
- Consumes: `EnqueueEvent`, `dedupExists`, `TaskTerminal`.

- [ ] **Step 1: Write the test**

```go
func TestEnqueueEvent_DedupGatedByTaskTerminalState(t *testing.T) {
	c := fakeClientWithScheme(t) // existing helper in this test file
	seq := newTestSeqSource(c)   // existing helper
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}}
	pay := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}

	// First firing creates a QueuedEvent.
	_, created1, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "rulehash", pay)
	require.NoError(t, err)
	require.True(t, created1)

	// Simulate consumption: build the Task (carries the dedup label) and mark it
	// non-terminal (Running), then delete the QueuedEvent so only the Task gates.
	qe := firstQueuedEvent(t, c, "ns")
	task, err := BuildTaskFromQueuedEvent(qe, proj, c.Scheme())
	require.NoError(t, err)
	require.NoError(t, c.Create(context.Background(), task))
	task.Status.Phase = "Running"
	require.NoError(t, c.Status().Update(context.Background(), task))
	require.NoError(t, c.Delete(context.Background(), qe))

	// Second firing: non-terminal Task with same dedup key -> NO new event.
	_, created2, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "rulehash", pay)
	require.NoError(t, err)
	require.False(t, created2, "dedup while incident Task non-terminal")

	// Mark the Task terminal; third firing -> fresh event created.
	task.Status.Phase = "Succeeded"
	require.NoError(t, c.Status().Update(context.Background(), task))
	_, created3, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "rulehash", pay)
	require.NoError(t, err)
	require.True(t, created3, "re-investigate once prior incident Task is terminal")
}
```
Adapt helper names (`fakeClientWithScheme`, `newTestSeqSource`, `firstQueuedEvent`) to the actual helpers in `enqueue_test.go`. If a status subresource is not wired on the fake client, register the Task status subresource via the fake builder (`WithStatusSubresource(&tatarav1alpha1.Task{})`).

- [ ] **Step 2: Run test to verify it passes**

Run: `mise exec -- go test ./internal/queue/ -run TestEnqueueEvent_DedupGatedByTaskTerminalState -v`
Expected: PASS (asserting the already-live behavior; if it fails, the dedup regressed and must be fixed, not the test).

- [ ] **Step 3: Commit**
```bash
git add internal/queue/enqueue_test.go
git commit -m "test(queue): codify incident dedup gated by Task terminal state"
```

---

## Component C: clone-as-bot commit identity

### Task C1: API field (ScmSpec.BotEmail)

**Files:**
- Modify: `api/v1alpha1/project_types.go` (ScmSpec, after `BotLogin` ~line 247)

**Interfaces:**
- Produces: `ScmSpec.BotEmail string`.

- [ ] **Step 1: Add field**

After `BotLogin`:
```go
	// BotEmail is the git commit author email for agent commits (the bot's
	// noreply/commit email). When empty the wrapper's default identity stands.
	BotEmail string `json:"botEmail,omitempty"`
```

- [ ] **Step 2: Regenerate manifests**

Run: `mise exec -- make manifests generate`
Expected: Project CRD gains `botEmail`.

- [ ] **Step 3: Verify build**

Run: `mise exec -- go build ./...`
Expected: no errors.

- [ ] **Step 4: Commit**
```bash
git add api/v1alpha1/project_types.go config/crd
git commit -m "feat(api): add ScmSpec.BotEmail"
```

### Task C2: set git identity env on agent pod

**Files:**
- Modify: `internal/agent/pod.go` (env assembly, after the `secretEnv(...GIT_TOKEN...)` block ~450)
- Test: `internal/agent/pod_test.go` (or `pod_grafana_test.go` style env-assertion test)

**Interfaces:**
- Consumes: `project.Spec.Scm.BotLogin`, `project.Spec.Scm.BotEmail`.

- [ ] **Step 1: Write the failing test**

In a pod test (match the existing pod-building test harness; search for a test that calls the pod-spec builder and inspects env):
```go
func TestPodEnv_SetsBotGitIdentity(t *testing.T) {
	proj := baseProject(t) // existing helper producing a Project with Scm
	proj.Spec.Scm.BotLogin = "szymonrychu-bot"
	proj.Spec.Scm.BotEmail = "143486966+szymonrychu-bot@users.noreply.github.com"
	pod := buildPodForTest(t, proj) // adapt to the actual builder entrypoint
	env := envMap(pod) // helper: map[string]string of container env Name->Value
	if env["GIT_USER_NAME"] != "szymonrychu-bot" {
		t.Fatalf("GIT_USER_NAME=%q", env["GIT_USER_NAME"])
	}
	if env["GIT_USER_EMAIL"] != "143486966+szymonrychu-bot@users.noreply.github.com" {
		t.Fatalf("GIT_USER_EMAIL=%q", env["GIT_USER_EMAIL"])
	}
}

func TestPodEnv_OmitsGitEmailWhenUnset(t *testing.T) {
	proj := baseProject(t)
	proj.Spec.Scm.BotLogin = "szymonrychu-bot"
	proj.Spec.Scm.BotEmail = ""
	pod := buildPodForTest(t, proj)
	env := envMap(pod)
	if _, ok := env["GIT_USER_EMAIL"]; ok {
		t.Fatal("GIT_USER_EMAIL must be omitted when BotEmail empty")
	}
}
```
If `envMap`/`buildPodForTest`/`baseProject` are not present, write minimal inline equivalents matching the existing pod test in that package (the package already has pod tests, e.g. `pod_grafana_test.go`).

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/agent/ -run TestPodEnv_Sets`
Expected: FAIL (env vars absent).

- [ ] **Step 3: Implement**

After the static env block that includes `secretEnv("GIT_TOKEN", ...)` (~line 453, after the `env = append(env, []corev1.EnvVar{...}...)` that ends the GIT_TOKEN group), add:
```go
	if project.Spec.Scm != nil {
		if project.Spec.Scm.BotLogin != "" {
			env = append(env, corev1.EnvVar{Name: "GIT_USER_NAME", Value: project.Spec.Scm.BotLogin})
		}
		if project.Spec.Scm.BotEmail != "" {
			env = append(env, corev1.EnvVar{Name: "GIT_USER_EMAIL", Value: project.Spec.Scm.BotEmail})
		}
	}
```
Place it where `project` is in scope (the builder already references `project.Spec.ScmSecretRef` at line 450, so `project` is available).

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/agent/ -run TestPodEnv_`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/agent/pod.go internal/agent/*_test.go
git commit -m "feat(agent): set GIT_USER_NAME/GIT_USER_EMAIL from bot identity on agent pods"
```

---

## Final: full suite + manifests sanity

### Task F1: full race suite + lint

- [ ] **Step 1: Run the full suite**

Run: `KUBEBUILDER_ASSETS=$(mise exec -- go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 -p path) mise exec -- go test ./... -race -count=1`
Expected: all PASS.

- [ ] **Step 2: Lint**

Run: `mise exec -- golangci-lint run`
Expected: clean.

- [ ] **Step 3: Confirm no uncommitted manifest drift**

Run: `mise exec -- make manifests generate && git status --porcelain`
Expected: empty (all regenerated CRDs already committed).

---

## Self-Review

1. **Spec coverage:** A (label) -> A1-A4; B (alert-rule field + dedup test) -> B1-B3; C (clone-as-bot) -> C1-C2; CRD/deploy -> manifests steps + F1. Deploy itself (helmfile) is post-merge, handled separately per the HARD contract, not a code task. All spec sections covered.
2. **Placeholder scan:** test helper names flagged as "adapt to existing harness" are the only soft spots; each names the concrete file + fallback (write inline equivalent matching siblings). No TODO/TBD in implementation steps; all code blocks complete.
3. **Type consistency:** `incidentLabel(*ScmSpec) string`, `inflightIncidentTask(ctx, string) bool`, `alertRuleName(GrafanaAlert) string`, fields `IncidentLabel`/`Incident`/`AlertRule`/`BotEmail` consistent across all tasks and the spec.
