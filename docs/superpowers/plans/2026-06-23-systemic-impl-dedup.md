# Systemic-group implementation dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When one brainstorm produces N connected issues (shared `systemicId`), spawn at most ONE implementation agent per `(systemicId, repo)` - the lead - which resolves all same-repo siblings in one combined PR and is aware of the cross-repo group; non-lead siblings spawn no agent and get an idempotent marker comment.

**Architecture:** Lead election is stateless and re-derived each scan from the open-issue candidates already listed in `issueScan` (which spans all project repos). The systemic group is threaded onto the lead Task via `QueuedEventPayload` -> `BuildTaskFromQueuedEvent` -> `TaskSpec.SystemicGroup`, mirroring the existing `ReposInScope` field, and injected into the Implement prompt. Non-lead siblings are skipped in the dedup loop and marked via an idempotent comment posted through `scanWriter`.

**Tech Stack:** Go (kubebuilder operator), controller-runtime, mise-pinned toolchain, table-driven tests.

## Global Constraints

- Newest stable Go; build/test/lint via `mise exec --` / `mise run` only, never bare `go`.
- KISS. No premature abstraction. No tech-debt; if complex, note in `MEMORY.md`.
- JSON logs via `log/slog`-style structured fields. Log every business action at INFO with structured fields.
- Metrics for everything that counts/fails: counters for events.
- Operator chart RBAC is hand-maintained in `templates/rbac.yaml`; no new CRD/controller here so no RBAC edit expected.
- CRD changes require `make generate` (deepcopy) AND `make manifests` (CRD yaml + chart CRD).
- No docstrings/comments on unchanged code. No em dashes, smart quotes, arrows.

## Key existing references

- `internal/controller/projectscan.go:778` `issueScan` - lists open issues per repo into `cands` (all project repos), then dedup + `createScanTask` loop at `:888`.
- `internal/controller/projectscan.go:369` `createScanTask` - builds `QueuedEventPayload`, dedupKey `kind + "\x00" + repo#number`. Call sites: `:737` (mrScan MRCI), `:755` (mrScan review), `:952` (issueScan).
- `internal/controller/projectscan.go:264` `candidate` struct - has `repo`, `number`, `labels`, `title`.
- `internal/controller/projectscan.go:313` `candidatesFromIssues` - copies `i.Labels`, `i.Title` onto candidates.
- `internal/controller/projectscan.go:1554` `proposalBacklogCount` - existing `tatara/systemic-` grouping precedent.
- `internal/controller/projectscan.go:482` `scanWriter` - returns `scm.SCMWriter` + token.
- `internal/controller/projectscan.go:1501` `botCommentedOnIssue` - idempotency precedent (lists comments).
- `internal/scm/scm.go:117` `SCMWriter.Comment(ctx, token, issueRef, body)`; `:155` `ListIssueComments`.
- `internal/queue/enqueue.go:141` `BuildTaskFromQueuedEvent` - maps payload to `TaskSpec`.
- `api/v1alpha1/queuedevent_types.go:47` `QueuedEventPayload`.
- `api/v1alpha1/task_types.go:148` `TaskSpec`; `:169-176` `ReposInScope` (model for the new field).
- `internal/controller/lifecycle.go:1355` `implementPrompt`; `:1363` `ReposInScope` injection (model).

---

### Task 1: SystemicGroup type + payload/spec fields + codegen

**Files:**
- Modify: `api/v1alpha1/task_types.go` (add type + `TaskSpec.SystemicGroup`)
- Modify: `api/v1alpha1/queuedevent_types.go:47` (add `QueuedEventPayload.SystemicGroup`)
- Modify: `internal/queue/enqueue.go:163-169` (map payload -> spec)
- Test: `internal/queue/enqueue_test.go` (or the existing build-task test file)
- Regen: `api/v1alpha1/zz_generated.deepcopy.go`, `config/crd/...`, `charts/.../crds/...`

**Interfaces:**
- Produces:
  ```go
  // SystemicGroup describes the systemic-improvement group a lead issue owns.
  type SystemicGroup struct {
      SystemicID       string   `json:"systemicId"`
      SameRepoSiblings []int    `json:"sameRepoSiblings,omitempty"` // sibling issue numbers in THIS repo, closed by the lead PR
      CrossRepo        []string `json:"crossRepo,omitempty"`        // "owner/repo#N - title" references, context only
  }
  ```
  `TaskSpec.SystemicGroup *SystemicGroup` (json `systemicGroup,omitempty`); `QueuedEventPayload.SystemicGroup *SystemicGroup` (same json tag).

- [ ] **Step 1: Write the failing test** in `internal/queue/enqueue_test.go`

```go
func TestBuildTaskFromQueuedEvent_SystemicGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = tatarav1alpha1.AddToScheme(scheme)
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}}
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe1", Namespace: "ns"},
		Spec: tatarav1alpha1.QueuedEventSpec{Payload: tatarav1alpha1.QueuedEventPayload{
			Kind: "issueLifecycle", RepositoryRef: "r", Goal: "g", GenerateName: "scan-",
			SystemicGroup: &tatarav1alpha1.SystemicGroup{
				SystemicID: "abc", SameRepoSiblings: []int{12, 15}, CrossRepo: []string{"o/r2#3 - x"},
			},
		}},
	}
	task, err := queue.BuildTaskFromQueuedEvent(qe, proj, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.SystemicGroup == nil || task.Spec.SystemicGroup.SystemicID != "abc" ||
		len(task.Spec.SystemicGroup.SameRepoSiblings) != 2 || len(task.Spec.SystemicGroup.CrossRepo) != 1 {
		t.Fatalf("SystemicGroup not mapped: %+v", task.Spec.SystemicGroup)
	}
}
```

- [ ] **Step 2: Run it, expect FAIL** (unknown field `SystemicGroup`)

Run: `mise exec -- go test ./internal/queue/ -run SystemicGroup -v`
Expected: compile error / FAIL.

- [ ] **Step 3: Add the type + fields**

In `api/v1alpha1/task_types.go` add the `SystemicGroup` struct (above) and to `TaskSpec`:
```go
	// SystemicGroup, when set, marks this Task as the lead for a brainstorm
	// systemic group: it resolves SameRepoSiblings in one combined PR and is
	// aware of CrossRepo siblings (reference only).
	// +optional
	SystemicGroup *SystemicGroup `json:"systemicGroup,omitempty"`
```
In `api/v1alpha1/queuedevent_types.go` `QueuedEventPayload` add:
```go
	// +optional
	SystemicGroup *SystemicGroup `json:"systemicGroup,omitempty"`
```

- [ ] **Step 4: Map payload -> spec** in `internal/queue/enqueue.go` inside `BuildTaskFromQueuedEvent`, after `Source: p.Source,` add `SystemicGroup: p.SystemicGroup,` to the `TaskSpec` literal.

- [ ] **Step 5: Regenerate**

Run: `mise run generate || mise exec -- make generate` then `mise run manifests || mise exec -- make manifests`
Expected: `zz_generated.deepcopy.go` gains `SystemicGroup` deepcopy; CRD yaml under `config/crd` and `charts/*/crds` (or `templates/crds.yaml`) gains the `systemicGroup` schema. (Per `operator-crd-templating-helm-adoption`, CRDs are templated; ensure both the config/crd base and the chart copy update.)

- [ ] **Step 6: Run test + build, expect PASS**

Run: `mise exec -- go test ./internal/queue/ -run SystemicGroup -v && mise exec -- go build ./...`
Expected: PASS, clean build.

- [ ] **Step 7: Commit**

```bash
git add api/v1alpha1 internal/queue config charts
git commit -m "feat: add SystemicGroup to TaskSpec and QueuedEventPayload"
```

---

### Task 2: lead election + grouping helpers (pure functions)

**Files:**
- Modify: `internal/controller/projectscan.go` (add helpers near `candidate` definition, ~`:340`)
- Test: `internal/controller/projectscan_test.go` (or a new `systemic_dedup_test.go`)

**Interfaces:**
- Consumes: `candidate` (`projectscan.go:264`).
- Produces:
  ```go
  const systemicLabelPrefix = "tatara/systemic-"

  // systemicIDOf returns the systemicId from a tatara/systemic-<id> label, or "".
  func systemicIDOf(labels []string) string

  // systemicDecision is the per-candidate role within its systemic group.
  type systemicDecision struct {
      sid              string
      isLead           bool
      leadNumber       int      // the elected lead issue number in this candidate's repo
      sameRepoSiblings []int    // non-lead sibling numbers in this repo (set on the lead only)
      crossRepo        []string // "owner/repo#N - title" for siblings in OTHER repos (set on the lead only)
  }

  // electSystemicLeads groups candidates by (systemicId, repo); the lowest open
  // issue number per (sid, repo) is the lead. Returns a map keyed by "repo#number"
  // for every candidate carrying a systemic label. Candidates with no systemic
  // label are absent from the map.
  func electSystemicLeads(cands []candidate) map[string]systemicDecision
  ```

- [ ] **Step 1: Write the failing test**

```go
func TestElectSystemicLeads(t *testing.T) {
	cands := []candidate{
		{repo: "o/r1", number: 15, labels: []string{"tatara/systemic-abc"}, title: "C"},
		{repo: "o/r1", number: 12, labels: []string{"tatara/systemic-abc"}, title: "A"},
		{repo: "o/r2", number: 9, labels: []string{"tatara/systemic-abc"}, title: "B"},
		{repo: "o/r1", number: 7, labels: []string{"bug"}, title: "standalone"},
	}
	got := electSystemicLeads(cands)
	if _, ok := got["o/r1#7"]; ok {
		t.Fatal("standalone (no systemic label) must not be in the map")
	}
	lead := got["o/r1#12"]
	if !lead.isLead || lead.leadNumber != 12 {
		t.Fatalf("o/r1#12 should be lead: %+v", lead)
	}
	if len(lead.sameRepoSiblings) != 1 || lead.sameRepoSiblings[0] != 15 {
		t.Fatalf("lead sameRepoSiblings want [15]: %+v", lead.sameRepoSiblings)
	}
	if len(lead.crossRepo) != 1 || lead.crossRepo[0] != "o/r2#9 - B" {
		t.Fatalf("lead crossRepo want [o/r2#9 - B]: %+v", lead.crossRepo)
	}
	sib := got["o/r1#15"]
	if sib.isLead || sib.leadNumber != 12 {
		t.Fatalf("o/r1#15 should be non-lead pointing at 12: %+v", sib)
	}
	r2lead := got["o/r2#9"]
	if !r2lead.isLead || r2lead.leadNumber != 9 {
		t.Fatalf("o/r2#9 should be its repo's lead: %+v", r2lead)
	}
}
```

- [ ] **Step 2: Run it, expect FAIL**

Run: `mise exec -- go test ./internal/controller/ -run ElectSystemicLeads -v`
Expected: FAIL (undefined `electSystemicLeads`).

- [ ] **Step 3: Implement the helpers** in `projectscan.go`

```go
const systemicLabelPrefix = "tatara/systemic-"

func systemicIDOf(labels []string) string {
	for _, l := range labels {
		if strings.HasPrefix(l, systemicLabelPrefix) {
			return strings.TrimPrefix(l, systemicLabelPrefix)
		}
	}
	return ""
}

type systemicDecision struct {
	sid              string
	isLead           bool
	leadNumber       int
	sameRepoSiblings []int
	crossRepo        []string
}

func electSystemicLeads(cands []candidate) map[string]systemicDecision {
	// group[sid] = candidates sharing that systemic id (across repos).
	group := map[string][]candidate{}
	for _, c := range cands {
		if c.isPR {
			continue
		}
		if sid := systemicIDOf(c.labels); sid != "" {
			group[sid] = append(group[sid], c)
		}
	}
	out := map[string]systemicDecision{}
	for sid, members := range group {
		if len(members) < 2 {
			continue // a lone issue needs no dedup; treat as standalone
		}
		// lead per repo = lowest open issue number in that repo.
		leadByRepo := map[string]int{}
		for _, m := range members {
			if cur, ok := leadByRepo[m.repo]; !ok || m.number < cur {
				leadByRepo[m.repo] = m.number
			}
		}
		for _, m := range members {
			key := fmt.Sprintf("%s#%d", m.repo, m.number)
			d := systemicDecision{sid: sid, leadNumber: leadByRepo[m.repo]}
			d.isLead = m.number == leadByRepo[m.repo]
			if d.isLead {
				for _, o := range members {
					if o.repo == m.repo && o.number != m.number {
						d.sameRepoSiblings = append(d.sameRepoSiblings, o.number)
					} else if o.repo != m.repo {
						d.crossRepo = append(d.crossRepo, fmt.Sprintf("%s#%d - %s", o.repo, o.number, o.title))
					}
				}
				sort.Ints(d.sameRepoSiblings)
				sort.Strings(d.crossRepo)
			}
			out[key] = d
		}
	}
	return out
}
```
Add `"sort"` to imports if absent.

- [ ] **Step 4: Run test, expect PASS**

Run: `mise exec -- go test ./internal/controller/ -run ElectSystemicLeads -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/projectscan.go internal/controller/*_test.go
git commit -m "feat: systemic lead-per-repo election helpers"
```

---

### Task 3: implementPrompt injection of the systemic group

**Files:**
- Modify: `internal/controller/lifecycle.go:1355` `implementPrompt`
- Test: `internal/controller/lifecycle_test.go` (find the existing implementPrompt test, add a case)

**Interfaces:**
- Consumes: `task.Spec.SystemicGroup` (Task 1).

- [ ] **Step 1: Write the failing test**

```go
func TestImplementPrompt_SystemicGroup(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t"},
		Spec: tatarav1alpha1.TaskSpec{
			Goal: "Triage issue o/r1#12", ProjectRef: "p",
			SystemicGroup: &tatarav1alpha1.SystemicGroup{
				SystemicID: "abc", SameRepoSiblings: []int{15},
				CrossRepo: []string{"o/r2#9 - B"},
			},
		},
	}
	got := implementPrompt(task)
	if !strings.Contains(got, "Closes #15") {
		t.Fatalf("prompt must instruct closing same-repo sibling: %s", got)
	}
	if !strings.Contains(got, "o/r2#9") {
		t.Fatalf("prompt must reference cross-repo sibling: %s", got)
	}
}
```

- [ ] **Step 2: Run it, expect FAIL**

Run: `mise exec -- go test ./internal/controller/ -run ImplementPrompt_SystemicGroup -v`
Expected: FAIL.

- [ ] **Step 3: Implement injection** in `implementPrompt`, after the `ReposInScope` block (`lifecycle.go:1367`), before `lifecyclePhaseGuidance`:

```go
	if g := task.Spec.SystemicGroup; g != nil && len(g.SameRepoSiblings) > 0 {
		closes := make([]string, 0, len(g.SameRepoSiblings))
		for _, n := range g.SameRepoSiblings {
			closes = append(closes, fmt.Sprintf("Closes #%d", n))
		}
		base += "\n\n**You lead systemic improvement group " + g.SystemicID +
			".** Resolve these sibling issues in this same repo within ONE combined PR and " +
			"close them from the PR body: " + strings.Join(closes, ", ") + "."
	}
	if g := task.Spec.SystemicGroup; g != nil && len(g.CrossRepo) > 0 {
		base += "\n\nRelated work in OTHER repos (reference for context, do NOT edit them here; " +
			"each is led by its own agent): " + strings.Join(g.CrossRepo, "; ") + "."
	}
```
Ensure `fmt` is imported (it is).

- [ ] **Step 4: Run test, expect PASS**

Run: `mise exec -- go test ./internal/controller/ -run ImplementPrompt_SystemicGroup -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/lifecycle.go internal/controller/lifecycle_test.go
git commit -m "feat: inject systemic-group lead instructions into implement prompt"
```

---

### Task 4: idempotent sibling marker comment helper

**Files:**
- Modify: `internal/controller/projectscan.go` (near `botCommentedOnIssue:1501`)
- Test: `internal/controller/projectscan_test.go` (or systemic_dedup_test.go) using the existing fake SCM reader/writer pattern (search the test file for an existing fake implementing `scm.SCMReader`/`scm.SCMWriter`).

**Interfaces:**
- Consumes: `scm.SCMReader.ListIssueComments`, `scm.SCMWriter.Comment`.
- Produces:
  ```go
  // systemicMarker is the idempotency marker + human-facing body for a collapsed sibling.
  func systemicMarker(lead int) string // returns "Tracked by #<lead> (systemic group). No separate agent."

  // commentSiblingMarker posts the marker once. It is a no-op when a comment whose
  // body contains the marker already exists (reconcile-safe).
  func commentSiblingMarker(ctx context.Context, reader scm.SCMReader, writer scm.SCMWriter, token, repo string, number, lead int) error
  ```
  `issueRef` for `writer.Comment` is `fmt.Sprintf("%s#%d", repo, number)` (matches dedupKey convention; GitHub. For GitLab `#` issue addressing is correct per `gitlab-read-owner-split` notes - issues use `#`, MRs use `!`).

- [ ] **Step 1: Write the failing test** (use existing fakes; illustrative shape):

```go
func TestCommentSiblingMarker_Idempotent(t *testing.T) {
	marker := systemicMarker(12)
	reader := &fakeSCM{comments: map[string][]scm.IssueComment{
		"o/r1#15": {{Author: "bot", Body: "earlier " + marker + " trailing"}},
	}}
	writer := &fakeSCM{}
	if err := commentSiblingMarker(context.Background(), reader, writer, "tok", "o/r1", 15, 12); err != nil {
		t.Fatal(err)
	}
	if writer.commentCalls != 0 {
		t.Fatalf("marker already present, must not re-post; got %d calls", writer.commentCalls)
	}
	// fresh issue -> posts once
	reader2 := &fakeSCM{comments: map[string][]scm.IssueComment{}}
	writer2 := &fakeSCM{}
	_ = commentSiblingMarker(context.Background(), reader2, writer2, "tok", "o/r1", 16, 12)
	if writer2.commentCalls != 1 {
		t.Fatalf("fresh issue must post once; got %d", writer2.commentCalls)
	}
}
```
NOTE: adapt the fake to whatever `*_test.go` already defines. If no shared fake exists, add a minimal one implementing only `ListIssueComments` (reader) and `Comment` (writer) with a `commentCalls` counter and a `comments map[string][]scm.IssueComment` keyed by `repo#number`.

- [ ] **Step 2: Run it, expect FAIL**

Run: `mise exec -- go test ./internal/controller/ -run CommentSiblingMarker -v`
Expected: FAIL (undefined).

- [ ] **Step 3: Implement**

```go
func systemicMarker(lead int) string {
	return fmt.Sprintf("Tracked by #%d (systemic group). No separate agent.", lead)
}

func commentSiblingMarker(ctx context.Context, reader scm.SCMReader, writer scm.SCMWriter, token, repo string, number, lead int) error {
	owner, name, _ := strings.Cut(repo, "/")
	marker := systemicMarker(lead)
	if comments, err := reader.ListIssueComments(ctx, owner, name, number); err == nil {
		for _, c := range comments {
			if strings.Contains(c.Body, marker) {
				return nil // already marked; reconcile-safe
			}
		}
	}
	return writer.Comment(ctx, token, fmt.Sprintf("%s#%d", repo, number), marker)
}
```

- [ ] **Step 4: Run test, expect PASS**

Run: `mise exec -- go test ./internal/controller/ -run CommentSiblingMarker -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/projectscan.go internal/controller/*_test.go
git commit -m "feat: idempotent systemic-sibling marker comment helper"
```

---

### Task 5: wire dedup gate into issueScan + createScanTask group param + metrics

**Files:**
- Modify: `internal/controller/projectscan.go` (`createScanTask:369`, `issueScan:778-962`, 3 call sites)
- Modify: the Metrics type (search `func (m *Metrics) ScanItem` / `ScanTaskCreated` - likely `internal/metrics/metrics.go` or `internal/controller/metrics*.go`) to add a counter.
- Test: `internal/controller/projectscan_test.go` integration-style test for `issueScan` if one exists; otherwise extend the helper tests to assert the decision-driven branch via a focused unit on the new `createScanTask` signature is not feasible (it hits the queue) - instead assert behavior through `electSystemicLeads` wiring + a fake-backed `issueScan` test mirroring existing issueScan tests. Search for an existing `TestIssueScan`-style test and follow its harness.

**Interfaces:**
- Consumes: `electSystemicLeads` (Task 2), `commentSiblingMarker` (Task 4), `TaskSpec.SystemicGroup`/payload field (Task 1).
- Changes `createScanTask` signature to accept the group:
  ```go
  func (r *ProjectReconciler) createScanTask(ctx context.Context, proj *..., repo *..., labelCand, srcCand candidate, activity, kind, goal string, extraAnnotations map[string]string, systemicGroup *tatarav1alpha1.SystemicGroup) (bool, error)
  ```

- [ ] **Step 1: Add the metric.** In the Metrics definition file, add a counter mirroring the existing `ScanItem` pattern:
```go
// in the metrics struct + registration
SystemicSiblingsCollapsed *prometheus.CounterVec // labels: project
SystemicGroupsLed         *prometheus.CounterVec // labels: project
```
with `func (m *Metrics) SystemicSiblingCollapsed(project string)` and `func (m *Metrics) SystemicGroupLed(project string)` incrementers. Register both in the metrics constructor next to the existing scan counters. (Match the exact registration idiom used by `ScanItem`.)

- [ ] **Step 2: Add the `systemicGroup` param to `createScanTask`** and set it on the payload:
In the `QueuedEventPayload{...}` literal (`projectscan.go:383`) add `SystemicGroup: systemicGroup,`. Update the 3 existing call sites to pass `nil`:
  - `:737` mrScan MRCI -> append `, nil`
  - `:755` mrScan review -> append `, nil`
  - `:952` issueScan -> will pass the lead group (Step 4)

- [ ] **Step 3: Compute decisions once in `issueScan`.** After `cands` is fully built and gated (after the reporter-intake gate, ~`:837`, before the dedup loop), add:
```go
	systemicLeads := electSystemicLeads(cands)
```

- [ ] **Step 4: Gate non-leads + pass lead group.** In the `eligible` createScanTask loop (`:888-961`), at the TOP of the loop body (right after `repo, ok := r.matchRepoForSlug(...)` success), insert:
```go
		key := fmt.Sprintf("%s#%d", c.repo, c.number)
		if d, ok := systemicLeads[key]; ok && !d.isLead {
			// Collapsed sibling: no implementation agent. Mark idempotently and skip.
			if w, token, werr := r.scanWriter(ctx, proj); werr == nil {
				if cerr := commentSiblingMarker(ctx, reader, w, token, c.repo, c.number, d.leadNumber); cerr != nil {
					l.Error(cerr, "issueScan: systemic sibling marker comment", "action", "systemic_sibling_mark",
						"resource_id", proj.Name, "issue", key, "lead", d.leadNumber)
				}
			}
			r.Metrics.SystemicSiblingCollapsed(proj.Name)
			r.Metrics.ScanItem("issueScan", "skipped_systemic_sibling")
			l.Info("issueScan: collapsed systemic sibling (no separate agent)",
				"action", "systemic_dedup", "resource_id", proj.Name,
				"issue", key, "systemic_id", d.sid, "lead", d.leadNumber)
			continue
		}
```
Then change the lead's `createScanTask` call (`:952`) to build + pass the group:
```go
		var sg *tatarav1alpha1.SystemicGroup
		if d, ok := systemicLeads[key]; ok && d.isLead && len(d.sameRepoSiblings) > 0 {
			sg = &tatarav1alpha1.SystemicGroup{SystemicID: d.sid, SameRepoSiblings: d.sameRepoSiblings, CrossRepo: d.crossRepo}
			r.Metrics.SystemicGroupLed(proj.Name)
			l.Info("issueScan: systemic group lead", "action", "systemic_dedup", "resource_id", proj.Name,
				"issue", key, "systemic_id", d.sid, "same_repo_siblings", len(d.sameRepoSiblings), "cross_repo", len(d.crossRepo))
		}
		goal := fmt.Sprintf("Triage issue %s#%d", c.repo, c.number)
		ok2, err := r.createScanTask(ctx, proj, &repo, c, c, "issueScan", "issueLifecycle", goal, nil, sg)
```
(Note: `key` is already computed at loop top; reuse it.)

- [ ] **Step 5: Build + run the full controller package tests, expect PASS**

Run: `mise exec -- go build ./... && mise exec -- go test ./internal/controller/ ./internal/queue/ -v`
Expected: PASS (existing tests unaffected; new tests pass).

- [ ] **Step 6: Lint**

Run: `mise run lint || mise exec -- golangci-lint run`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add internal/controller internal/metrics
git commit -m "feat: dedup implementation agents within a systemic group (lead-per-repo)"
```

---

## Self-review (author)

- Spec coverage: lead election (T2), dedup gate + lead-per-repo (T5), combined-PR via Closes injection (T3), comment-only marking idempotent (T4), SystemicID on Task (T1), metrics+logs (T5). All covered.
- Cross-repo derivation: `issueScan` lists all project repos into `cands`, so `electSystemicLeads` sees the whole group - resolves the spec's open question (placement = `issueScan`, group-wide).
- Type consistency: `SystemicGroup{SystemicID, SameRepoSiblings []int, CrossRepo []string}` used identically in T1/T2/T3/T5. `createScanTask` new trailing `systemicGroup` param consistent across all call sites.
- Out of scope honored: no MaxPerRepo, no sequential re-election, no DAG.
