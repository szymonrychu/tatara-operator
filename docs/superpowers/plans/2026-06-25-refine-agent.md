# Refine Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A project-scoped `refine` agent that runs first each cron cycle, loads all open + recently-closed issues across the project's repos, and autonomously closes duplicates/implemented issues, edits scope, and splits broad ones - with Task CRs following via the operator's ledger reconcile.

**Architecture:** New `refine` Task kind dispatched as a cron barrier (piggyback the scan cadence; defer scans/brainstorm until the refine Task is terminal). New SCM `EditIssue` + closed-issue lister, restapi issue list/close/edit/create endpoints, cli `refine` tool profile + 4 MCP tools. Operator-propagated Task changes (agent never writes Task CRs).

**Tech Stack:** Go (controller-runtime/kubebuilder), envtest, `mise exec -- go test`; tatara-cli MCP server (Go).

## Global Constraints

- Newest stable Go; KISS; no tech-debt; no stubs. JSON slog. Metrics on every action.
- TDD: failing test first per unit.
- Names (exact): Task kind `refine`; tool profile `refine`; `ProjectStatus.LastRefine *metav1.Time`; `RefineActivity` with `ClosedLookbackDays int` (default 30) under `ScmCron.Refine`; SCM `EditIssue(ctx, token, repo string, number int, req EditIssueReq) error` with `EditIssueReq{Title, Body *string; Labels *[]string}`; SCM `ListClosedIssues(ctx, owner, repo string, since time.Time) ([]IssueRef, error)`; `IssueRef.State string` + `IssueRef.ClosedAt time.Time`; restapi routes `GET /projects/{p}/issues`, `POST /projects/{p}/issues/{repo}/{number}/close`, `PATCH /projects/{p}/issues/{repo}/{number}`, `POST /projects/{p}/issues/{repo}`; cli tools `list_issues`, `close_issue`, `edit_issue`, `create_issue`.
- After any `api/v1alpha1` field add: `mise exec -- make manifests generate`, commit regenerated CRDs.
- Operator test run: `KUBEBUILDER_ASSETS=$(mise exec -- go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 -p path) mise exec -- go test ./... -race -count=1`.
- cli test run: `cd tatara-cli && mise exec -- go test ./... -race -count=1`.
- Deploy: operator + cli merge to main -> wrapper TATARA_CLI_VERSION pin bump -> tatara-helmfile chart pins + image tags. The cli `tatara mcp` tools/list must serve WITHOUT a token (wrapper build-guard).
- Two repos: operator at `~/Documents/tatara/tatara-operator`, cli at `~/Documents/tatara/tatara-cli`. Components A-D + F are operator; E is cli. Build cli LAST (its e2e/profile tests assert the tool surface).

---

## Component A: operator API + SCM primitives

### Task A1: API fields (kind, LastRefine, RefineActivity, IssueRef state)

**Files:**
- Modify: `api/v1alpha1/task_types.go` (Kind enum + projectScopedKinds map), `api/v1alpha1/project_types.go` (ScmCron, ProjectStatus)

**Interfaces:**
- Produces: kind `"refine"` valid + project-scoped; `ScmCron.Refine RefineActivity{ClosedLookbackDays int}`; `ProjectStatus.LastRefine *metav1.Time`.

- [ ] **Step 1: Write the failing test**

In `api/v1alpha1/task_types_test.go`:
```go
func TestValidateTaskSpec_Refine(t *testing.T) {
	if err := v1alpha1.ValidateTaskSpec(v1alpha1.TaskSpec{Kind: "refine"}); err != nil {
		t.Fatalf("refine with empty repositoryRef must be valid (project-scoped): %v", err)
	}
	if err := v1alpha1.ValidateTaskSpec(v1alpha1.TaskSpec{Kind: "refine", RepositoryRef: "r"}); err == nil {
		t.Fatalf("refine with a repositoryRef must be rejected (project-scoped)")
	}
	if !v1alpha1.IsProjectScopedKind("refine") {
		t.Fatalf("refine must be project-scoped")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./api/v1alpha1/ -run TestValidateTaskSpec_Refine`
Expected: FAIL (refine not a known/project-scoped kind).

- [ ] **Step 3: Implement**

In `task_types.go`: add `"refine"` to the `knownKinds`/validation map (mirror how `"incident"` is registered) and to whatever `IsProjectScopedKind` consults (the same set `incident` belongs to - find the project-scoped set used by `ValidateTaskSpec` for incident and add `refine`).
In `project_types.go`, add to `ScmCron`:
```go
	// Refine configures the project-refiner pre-step. No schedule: refine fires
	// off the existing scan cadence as a mandatory barrier before scans/brainstorm.
	Refine RefineActivity `json:"refine,omitempty"`
```
and the type + status field:
```go
// RefineActivity configures the cron-cycle refiner pre-step.
type RefineActivity struct {
	// ClosedLookbackDays bounds how far back closed issues are loaded for
	// already-implemented detection. Default 30 when zero.
	ClosedLookbackDays int `json:"closedLookbackDays,omitempty"`
}
```
In `ProjectStatus`:
```go
	// LastRefine is the last time the project's refine pre-step completed.
	LastRefine *metav1.Time `json:"lastRefine,omitempty"`
```

- [ ] **Step 4: Regenerate + verify**

Run: `mise exec -- make manifests generate && mise exec -- go test ./api/v1alpha1/ -run TestValidateTaskSpec_Refine`
Expected: PASS; CRDs gain `refine`/`closedLookbackDays`/`lastRefine`.

- [ ] **Step 5: Commit**
```bash
git add api/v1alpha1/ config/crd
git commit -m "feat(api): add refine task kind, RefineActivity cron config, LastRefine status"
```

### Task A2: IssueRef state/closedAt + SCM ListClosedIssues

**Files:**
- Modify: `internal/scm/scm.go` (IssueRef + SCMReader interface), `internal/scm/github_scan.go`, `internal/scm/gitlab_scan.go`
- Test: `internal/scm/github_scan_test.go`, `internal/scm/gitlab_scan_test.go`

**Interfaces:**
- Produces: `IssueRef.State string` (`open`/`closed`), `IssueRef.ClosedAt time.Time`; `SCMReader.ListClosedIssues(ctx, owner, repo string, since time.Time) ([]IssueRef, error)`.

- [ ] **Step 1: Write the failing test**

In `github_scan_test.go` (mirror the existing ListOpenIssues test's httptest server):
```go
func TestGitHubListClosedIssues_FiltersSinceAndPRs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// assert state=closed + since passed
		if r.URL.Query().Get("state") != "closed" {
			t.Errorf("want state=closed, got %q", r.URL.Query().Get("state"))
		}
		_, _ = w.Write([]byte(`[
			{"number":5,"title":"done thing","state":"closed","closed_at":"2026-06-20T00:00:00Z","user":{"login":"szymonrychu-bot"}},
			{"number":6,"title":"a pr","state":"closed","closed_at":"2026-06-20T00:00:00Z","pull_request":{"url":"x"},"user":{"login":"x"}}
		]`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL, token: "t"} // match the existing GitHub test ctor in this file
	got, err := c.ListClosedIssues(context.Background(), "o", "r", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Number != 5 || got[0].State != "closed" {
		t.Fatalf("want 1 closed non-PR issue #5, got %+v", got)
	}
}
```
Add the analogous `TestGitLabListClosedIssues_*` in `gitlab_scan_test.go` (GitLab `GET /issues?state=closed&updated_after=<since>`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/scm/ -run ListClosedIssues`
Expected: FAIL (`ListClosedIssues` undefined).

- [ ] **Step 3: Implement**

Add to `IssueRef` (scm.go) after `IsPR`:
```go
	State    string    `json:"state,omitempty"`    // open | closed
	ClosedAt time.Time `json:"closedAt,omitempty"`
```
Add to the `SCMReader` interface:
```go
	// ListClosedIssues returns issues closed at/after `since` (PRs filtered out).
	ListClosedIssues(ctx context.Context, owner, repo string, since time.Time) ([]IssueRef, error)
```
Implement in `github_scan.go` mirroring `ListOpenIssues` but with `?state=closed&since=<RFC3339>` and parse `closed_at`+`state` (skip items with `pull_request`). Set `State:"closed"`. In `gitlab_scan.go` mirror with `?state=closed&updated_after=<since>` (GitLab has no PRs in /issues, so no IsPR filter). Also set `State:"open"` in the existing `ListOpenIssues` parsers (both providers) so open results carry State.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/scm/ -run "ListClosedIssues|ListOpenIssues"`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/scm/
git commit -m "feat(scm): ListClosedIssues + IssueRef State/ClosedAt"
```

### Task A3: SCM EditIssue

**Files:**
- Modify: `internal/scm/scm.go` (SCMWriter interface + EditIssueReq), `internal/scm/github.go`, `internal/scm/gitlab.go`
- Test: `internal/scm/github_test.go`, `internal/scm/gitlab_test.go`

**Interfaces:**
- Produces: `EditIssueReq{Title, Body *string; Labels *[]string}`; `SCMWriter.EditIssue(ctx, token, repo string, number int, req EditIssueReq) error`.

- [ ] **Step 1: Write the failing test**

In `github_test.go`:
```go
func TestGitHubEditIssue_PatchesOnlyProvided(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("want PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"number":7}`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL, token: "t"}
	title := "new title"
	if err := c.EditIssue(context.Background(), "t", "o/r", 7, EditIssueReq{Title: &title}); err != nil {
		t.Fatal(err)
	}
	if gotBody["title"] != "new title" {
		t.Fatalf("title not patched: %+v", gotBody)
	}
	if _, ok := gotBody["body"]; ok {
		t.Fatalf("body must NOT be sent when Body is nil: %+v", gotBody)
	}
}

func TestGitHubEditIssue_404Benign(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL, token: "t"}
	body := "x"
	if err := c.EditIssue(context.Background(), "t", "o/r", 7, EditIssueReq{Body: &body}); err != nil {
		t.Fatalf("404 (issue gone) must be benign: %v", err)
	}
}
```
Add the GitLab analogue in `gitlab_test.go` (PUT `/projects/:enc/issues/:iid`; GitLab labels are a comma-joined `labels` param).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/scm/ -run EditIssue`
Expected: FAIL (`EditIssue` undefined).

- [ ] **Step 3: Implement**

In `scm.go`:
```go
// EditIssueReq is a PATCH: only non-nil fields are sent.
type EditIssueReq struct {
	Title  *string
	Body   *string
	Labels *[]string
}
```
Add to `SCMWriter` interface: `EditIssue(ctx context.Context, token, repo string, number int, req EditIssueReq) error`.
Implement `EditIssue` in github.go (build a map with only non-nil fields, `PATCH {apiBase}/repos/{repo}/issues/{number}`, reuse the existing authed-request helper used by CreateIssue) and gitlab.go (`PUT {apiBase}/projects/{enc}/issues/{number}`, labels joined by `,`). For BOTH: treat a 404 as benign (return nil) - mirror the GitLab idempotency-tolerance pattern in `gitlab.go` Approve/RequestChanges (`var he *HTTPError; if errors.As(err, &he) && he.Status == http.StatusNotFound { return nil }`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/scm/ -run EditIssue`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/scm/
git commit -m "feat(scm): EditIssue (PATCH only-provided, 404 benign) for github+gitlab"
```

---

## Component B: operator restapi issue endpoints

### Task B1: GET /projects/{p}/issues (aggregate open + recent-closed)

**Files:**
- Modify: `internal/restapi/handlers.go` (handler), `internal/restapi/server.go` or wherever chi routes register
- Test: `internal/restapi/issues_handler_test.go` (new)

**Interfaces:**
- Consumes: `SCMReader.ListOpenIssues`, `SCMReader.ListClosedIssues`; the project's repos.
- Produces: `GET /projects/{p}/issues?closedSinceDays=N` -> JSON `{"issues":[{repo,number,title,body,author,labels,state,closedAt,isPr}]}` aggregated across all project repos (open + closed-since), PRs excluded.

- [ ] **Step 1: Write the failing test**

Mirror the existing restapi handler tests (find one using a fake SCMReader, e.g. the scan/lifecycle handler tests). Seed a Project + 2 Repositories + a fake reader returning open issues for repo A and closed issues for repo B; assert the response aggregates both, excludes PRs, and respects `closedSinceDays`. Use the actual restapi test harness (fake client + injected reader).

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/restapi/ -run TestListProjectIssues`
Expected: FAIL (route 404 / handler undefined).

- [ ] **Step 3: Implement**

Add `func (s *Server) listProjectIssues(w http.ResponseWriter, r *http.Request)`: resolve project (chi param `p`), list its Repositories (mirror `projectRepoSlugs`), for each repo call `ListOpenIssues` + (when `closedSinceDays>0`, default 30) `ListClosedIssues(since)`, filter `IsPR`, marshal an `[]issueDTO`. Register `r.Get("/projects/{p}/issues", s.listProjectIssues)` next to the existing project routes. Reader resolution mirrors `proposeIssue`'s reader/token acquisition.

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/restapi/ -run TestListProjectIssues`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/restapi/
git commit -m "feat(restapi): GET /projects/{p}/issues aggregates open+recent-closed across repos"
```

### Task B2: close / edit / create issue endpoints

**Files:**
- Modify: `internal/restapi/handlers.go`, route registration
- Test: `internal/restapi/issues_handler_test.go`

**Interfaces:**
- Consumes: `SCMWriter.CloseIssue`, `SCMWriter.EditIssue`, `SCMWriter.CreateIssue`.
- Produces: `POST /projects/{p}/issues/{repo}/{number}/close` {comment}; `PATCH /projects/{p}/issues/{repo}/{number}` {title?,body?,labels?}; `POST /projects/{p}/issues/{repo}` {title,body,labels}.

- [ ] **Step 1: Write the failing tests**

```go
func TestCloseProjectIssue_RequiresCommentAndRepoInProject(t *testing.T) {
	// fake writer records CloseIssue(repo, number, comment). Seed Project+Repo.
	// POST .../close with {"comment":"duplicate of r#1"} -> 200, writer.closed == {repo,number,comment}.
	// POST with empty comment -> 400. POST for a repo not in the project -> 400.
}
func TestEditProjectIssue_PatchesProvided(t *testing.T) {
	// PATCH .../{number} {"body":"narrowed scope"} -> writer.EditIssue called with Body set, Title nil.
}
func TestCreateProjectIssue_Splits(t *testing.T) {
	// POST /projects/{p}/issues/{repo} {title,body} -> writer.CreateIssue called, returns created ref.
}
```
Adapt to the actual writer-fake + harness in the restapi test package.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/restapi/ -run "TestCloseProjectIssue|TestEditProjectIssue|TestCreateProjectIssue"`
Expected: FAIL.

- [ ] **Step 3: Implement**

Three handlers; each resolves project + validates the `{repo}` belongs to it (reuse the repo-belongs-to-project check from `proposeIssue`), resolves writer+token, calls the SCM method, records `operator_scm_writes_total`. `close` requires a non-empty `comment` (400 otherwise). `edit` builds `EditIssueReq` from only the present JSON keys (decode into a `map[string]json.RawMessage` or a struct of pointers). `create` mirrors `proposeIssue`'s CreateIssue call but WITHOUT the ProposedIssue/approval path - a direct create returning the ref. Register all three routes.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/restapi/ -run "TestCloseProjectIssue|TestEditProjectIssue|TestCreateProjectIssue"`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/restapi/
git commit -m "feat(restapi): close/edit/create project issue endpoints for the refiner"
```

---

## Component C: operator refine goal + dispatch + cron barrier

### Task C1: refine goal builder

**Files:**
- Create: `internal/refine/goal.go`, `internal/refine/goal_test.go`

**Interfaces:**
- Produces: `func GoalProject(repoSlugs []string, lookbackDays int) string`.

- [ ] **Step 1: Write the failing test**
```go
func TestGoalProject_MentionsActionsAndScope(t *testing.T) {
	g := refine.GoalProject([]string{"szymonrychu/tatara-operator", "szymonrychu/tatara-cli"}, 30)
	for _, want := range []string{"list_issues", "close_issue", "edit_issue", "create_issue", "duplicate", "already", "30", "tatara-operator", "tatara-cli"} {
		if !strings.Contains(g, want) {
			t.Fatalf("goal missing %q", want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/refine/ -run TestGoalProject`
Expected: FAIL (package/function undefined).

- [ ] **Step 3: Implement**

`GoalProject` returns a goal instructing the agent to: call `list_issues` (open + closed within `lookbackDays` days) across the listed repos; cluster related issues across repos; for each: close already-implemented (cite the implementing PR from `task_list` or a closed sibling) via `close_issue` with an explanatory comment; close duplicates via `close_issue` ("duplicate of <repo>#N"), keep the canonical; tighten scope drift via `edit_issue`; split too-broad issues via `create_issue` child issues linking the parent + `edit_issue` the parent to residual scope (or close it). Rules: judge implemented-ness from `task_list` + closed siblings; never touch PRs; every close/edit explains itself in a comment; be idempotent (skip issues already linked/resolved). Embed the repo list + lookback number.

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/refine/ -run TestGoalProject`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/refine/
git commit -m "feat(refine): project refiner goal builder"
```

### Task C2: refine dispatch + cron barrier

**Files:**
- Modify: `internal/controller/projectscan.go` (the scan reconcile entry that evaluates `activityDue` and dispatches), add a refine dispatch + barrier; `internal/controller/project_controller.go` if the scan entry lives there
- Test: `internal/controller/projectscan_refine_test.go` (new)

**Interfaces:**
- Consumes: `activityDue`, `createScanTask`/`createBrainstormTask` dispatch pattern, `refine.GoalProject`, `tatarav1alpha1.TaskTerminal`, `Status.LastRefine`.
- Produces: a refine barrier - while a refine Task is non-terminal OR a scan/brainstorm is due without a completed refine this cycle, the scans/brainstorm are deferred; `LastRefine` stamped when the refine Task is terminal.

- [ ] **Step 1: Write the failing tests**

In `projectscan_refine_test.go` (use the existing projectscan reconcile test harness - fake client + a Project with cron schedules set so an activity is due):
```go
func TestRefineBarrier_DefersScansUntilRefineTerminal(t *testing.T) {
	// Project with issueScan due and no LastRefine. First reconcile:
	//  - dispatches a refine QueuedEvent/Task (kind refine, dedup refine-<proj>)
	//  - does NOT dispatch issueScan/brainstorm.
	// Seed the refine Task non-terminal -> reconcile again: still no issueScan.
	// Mark the refine Task terminal (Succeeded) -> reconcile: LastRefine stamped,
	//   issueScan now dispatches.
}
func TestRefineBarrier_FailedRefineStillReleases(t *testing.T) {
	// refine Task Failed -> LastRefine stamped, scans proceed (no wedge).
}
func TestRefine_OnePerProjectPerCycle(t *testing.T) {
	// With a refine already in flight, a second reconcile does NOT create a 2nd
	// refine (dedup refine-<proj>).
}
```
Adapt seed/dispatch-assertion helpers to the actual projectscan test harness (e.g. how `projectscan_brainstorm_*_test.go` assert dispatched QueuedEvents).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise exec -- go test ./internal/controller/ -run TestRefine`
Expected: FAIL.

- [ ] **Step 3: Implement**

In the scan reconcile, BEFORE dispatching any due mrScan/issueScan/brainstorm:
```go
// refine barrier: refine runs first each cycle. While a refine Task is
// non-terminal, defer scans/brainstorm. A terminal refine (any outcome)
// stamps LastRefine and releases the gate.
anyScanDue := mrDue || issueDue || brainstormDue // from existing activityDue calls
if anyScanDue {
	refineTask, err := r.inflightRefineTask(ctx, proj) // lists Tasks kind=refine, project, non-terminal
	if err != nil { return ctrl.Result{}, err }
	if refineTask != nil {
		// refine still running: defer scans this reconcile.
		return ctrl.Result{RequeueAfter: scanRequeue}, nil
	}
	if r.refineNeededThisCycle(proj) { // LastRefine older than the due cycle base
		if _, err := r.createRefineTask(ctx, proj); err != nil { return ctrl.Result{}, err }
		return ctrl.Result{RequeueAfter: scanRequeue}, nil
	}
	// else: a refine already completed for this cycle -> fall through to scans.
}
```
Add `createRefineTask` (mirror `createBrainstormTask`: dedup `refine-<proj>`, `Kind:"refine"`, project-scoped, goal `refine.GoalProject(slugs, lookbackDays)` with `lookbackDays = proj.Spec.Scm.Cron.Refine.ClosedLookbackDays` defaulting 30). Add `inflightRefineTask` (list Tasks, kind refine, project, `!TaskTerminal`). Add `refineNeededThisCycle` (true when `LastRefine` is nil or before the earliest due-activity base). Stamp `LastRefine` when a refine Task transitions terminal: the cleanest place is at the top of the barrier - when no in-flight refine exists but the most recent refine Task is terminal and newer than `LastRefine`, stamp it (`stampScan`-style status update) before evaluating `refineNeededThisCycle`. Ensure: a terminal refine of ANY outcome stamps (gate the stamp on `TaskTerminal`, not on Succeeded).

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise exec -- go test ./internal/controller/ -run TestRefine`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/controller/
git commit -m "feat(controller): refine cron barrier - refine runs first, defers scans until terminal"
```

---

## Component D: operator ledger propagation

### Task D1: refiner-closed issue drives its Task terminal

**Files:**
- Modify: `internal/controller/ledger.go` (or wherever issue-closed -> Task reconcile lives) only if a gap exists
- Test: `internal/controller/ledger_refine_test.go` (new)

**Interfaces:**
- Consumes: the existing work-item-ledger issue-state reconcile.

- [ ] **Step 1: Write the failing test**
```go
func TestLedger_RefinerClosedIssueClosesTask(t *testing.T) {
	// Seed a Task bound (via WorkItems / source) to issue repo#N (open).
	// Flip the issue to closed (as the refiner's close_issue would).
	// Reconcile -> the bound Task reaches terminal (Done/closed work-item),
	//   NOT re-opened or left dangling.
}
```
Adapt to the actual ledger reconcile harness (how `ledger_test.go`/`writeback_ledger_test.go` seed Tasks + drive reconcile).

- [ ] **Step 2: Run test to verify it (fails or already passes)**

Run: `mise exec -- go test ./internal/controller/ -run TestLedger_RefinerClosedIssueClosesTask`
Expected: If it PASSES, the existing ledger already covers refiner closes (close via the same SCM path reads as a closed issue) - keep the test as a regression guard and skip step 3. If it FAILS, implement step 3.

- [ ] **Step 3: Implement (only if step 2 failed)**

Extend the ledger reconcile so an issue read as `closed` (regardless of who closed it) drives its bound Task terminal and marks the work-item resolved. Make the close-detection source-agnostic (it should not depend on a tatara-authored close marker; a refiner close is a plain SCM close).

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/controller/ -run TestLedger_RefinerClosedIssueClosesTask`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/controller/
git commit -m "test(ledger): refiner-closed issue drives its Task terminal"
```

### Task D2: operator full-suite verify

- [ ] **Step 1: Full -race suite + lint + manifest drift**

Run: `KUBEBUILDER_ASSETS=$(mise exec -- go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 -p path) mise exec -- go test ./... -race -count=1 && mise exec -- golangci-lint run && mise exec -- make manifests generate && git status --porcelain`
Expected: all PASS, lint clean, empty porcelain.

- [ ] **Step 2: Commit any residual**
```bash
git add -A && git commit -m "chore(refine): operator suite green" --allow-empty
```

---

## Component E: cli refine profile + MCP tools (tatara-cli repo)

> Work in `~/Documents/tatara/tatara-cli`. Build this LAST (after operator restapi exists so the tool contracts match).

### Task E1: refine tool profile

**Files:**
- Modify: `internal/mcp/profiles.go` (`toolProfileForKind` + a `refine` profile list)
- Test: `internal/mcp/profiles_test.go`

**Interfaces:**
- Produces: `toolProfileForKind("refine") == "refine"`; the `refine` profile grants `list_issues`, `close_issue`, `edit_issue`, `create_issue`, `comment_on_issue`, `task_list`, `task_get`, `repo_list`, `report_internal_issue`, + memory + code-graph read groups; EXCLUDES `propose_issue`, `task_update`, `subtask_*`.

- [ ] **Step 1: Write the failing test**
```go
func TestProfile_Refine(t *testing.T) {
	if toolProfileForKind("refine") != "refine" {
		t.Fatal("refine kind must map to refine profile")
	}
	tools := profileTools("refine") // the function the package uses to resolve a profile's tool set
	must := []string{"list_issues", "close_issue", "edit_issue", "create_issue", "comment_on_issue", "task_list", "repo_list"}
	for _, m := range must {
		if !contains(tools, m) {
			t.Fatalf("refine profile missing %q", m)
		}
	}
	for _, no := range []string{"propose_issue", "task_update", "subtask_create"} {
		if contains(tools, no) {
			t.Fatalf("refine profile must NOT grant %q", no)
		}
	}
}
```
Adapt `profileTools`/`contains` to the actual profile-resolution helpers in the package.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/Documents/tatara/tatara-cli && mise exec -- go test ./internal/mcp/ -run TestProfile_Refine`
Expected: FAIL.

- [ ] **Step 3: Implement**

Add `case "refine": return "refine"` to `toolProfileForKind`; add a `groupRefine`/profile entry granting the tools above (compose existing groups: the issue/task read tools + the new write tools + `groupMemory` + `groupCodeGraph`).

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/mcp/ -run TestProfile_Refine`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/mcp/profiles.go internal/mcp/profiles_test.go
git commit -m "feat(mcp): refine tool profile"
```

### Task E2: the 4 new MCP tools

**Files:**
- Modify: `internal/mcp/tools.go` (tool registration + handlers), the operator-API client used by the handlers
- Test: `internal/mcp/tools_test.go` (or the e2e/tool round-trip test pattern)

**Interfaces:**
- Consumes: the restapi endpoints from Component B.
- Produces: tools `list_issues` {state?, closedSinceDays?}, `close_issue` {repo, number, comment}, `edit_issue` {repo, number, title?, body?, labels?}, `create_issue` {repo, title, body, labels?}.

- [ ] **Step 1: Write the failing test**

Mirror the existing tool round-trip tests (a tool that calls the operator API via a fake/httptest backend). Assert each tool POSTs/GETs the right path + body and surfaces the response. Use the actual tool-test harness in the package.
```go
func TestTool_CloseIssue_CallsCloseEndpoint(t *testing.T) {
	// fake operator API asserts POST /projects/{p}/issues/{repo}/{number}/close {comment}
	// invoke the close_issue tool with {repo,number,comment} -> backend hit, success surfaced.
}
// + TestTool_ListIssues, TestTool_EditIssue, TestTool_CreateIssue analogues.
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mise exec -- go test ./internal/mcp/ -run "TestTool_(ListIssues|CloseIssue|EditIssue|CreateIssue)"`
Expected: FAIL.

- [ ] **Step 3: Implement**

Register the 4 tools via the `op(...)` pattern in tools.go with JSON schemas:
- `list_issues`: `{"properties":{"state":{"enum":["open","all"]},"closedSinceDays":{"type":"integer"}}}` -> `GET /projects/{p}/issues?...`.
- `close_issue`: `{"properties":{"repo":{"type":"string"},"number":{"type":"integer"},"comment":{"type":"string"}},"required":["repo","number","comment"]}` -> `POST .../close`.
- `edit_issue`: `{"properties":{"repo","number","title","body","labels":{"type":"array"}},"required":["repo","number"]}` -> `PATCH .../{number}`.
- `create_issue`: `{"properties":{"repo","title","body","labels"},"required":["repo","title","body"]}` -> `POST /projects/{p}/issues/{repo}`.
Each handler resolves the project from the cli's task context (same way existing tools resolve the project), calls the operator API client, returns the structured result. The project name comes from the cli's configured task/project context (same source `propose_issue`/`task_list` use).

- [ ] **Step 4: Run test to verify it passes**

Run: `mise exec -- go test ./internal/mcp/ -run "TestTool_(ListIssues|CloseIssue|EditIssue|CreateIssue)"`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add internal/mcp/
git commit -m "feat(mcp): list_issues/close_issue/edit_issue/create_issue tools"
```

### Task E3: cli full verify + tools/list tokenless guard

- [ ] **Step 1: Full suite + token-less tools/list**

Run: `mise exec -- go test ./... -race -count=1 && mise exec -- go run ./cmd/tatara mcp --help >/dev/null`
Then confirm `tatara mcp` serves `tools/list` WITHOUT a token (the e2e/profiles tests assert the surface; run them): `mise exec -- go test ./internal/mcp/ -run "Profile|e2e"`.
Expected: all PASS (the new tools appear in the full profile; refine profile excludes propose_issue/task_update).

- [ ] **Step 2: Commit residual**
```bash
git add -A && git commit -m "chore(refine): cli suite green" --allow-empty
```

---

## Component F: cross-repo integration sanity

### Task F1: contract alignment check

- [ ] **Step 1: Verify the tool<->endpoint contracts line up**

Cross-check, by reading both sides, that each cli tool's path/body matches the operator restapi route/handler exactly (method, path params `{p}/{repo}/{number}`, JSON field names: `comment`, `title`, `body`, `labels`, `state`, `closedSinceDays`). Fix any mismatch in whichever side diverges from this plan's Global Constraints names.

- [ ] **Step 2: Both suites green**

Run operator: `KUBEBUILDER_ASSETS=$(mise exec -- go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use 1.31.0 -p path) mise exec -- go test ./... -race -count=1` (in operator).
Run cli: `mise exec -- go test ./... -race -count=1` (in cli).
Expected: both green.

---

## Self-Review

1. **Spec coverage:** kind+barrier (A1, C2), goal (C1), SCM EditIssue + closed lister (A2, A3), restapi list/close/edit/create (B1, B2), cli profile + tools (E1, E2), ledger propagation (D1), metrics/logging (folded into B2/C2 handler impls + the spec's metric names), scope open+recent-closed (A2 + B1 `closedSinceDays`), splits direct-create (B2 create + E2 `create_issue`, propose_issue excluded E1), fully-autonomous (no approval gate in any handler), one-per-cycle (C2 dedup), failed-refine-releases (C2). All spec sections covered.
2. **Placeholder scan:** test-helper names flagged "adapt to actual harness" are the only soft spots; each names the concrete sibling pattern to copy. D1 is conditional (test-first; implement only if the existing ledger doesn't already cover it) - explicitly structured, not a placeholder.
3. **Type consistency:** `EditIssueReq{Title,Body *string;Labels *[]string}`, `EditIssue(ctx,token,repo,number,req)`, `ListClosedIssues(ctx,owner,repo,since)`, `IssueRef.State/ClosedAt`, kind/profile `refine`, `LastRefine`, `RefineActivity.ClosedLookbackDays`, routes + tool names consistent across A-F and the spec's Global Constraints.
