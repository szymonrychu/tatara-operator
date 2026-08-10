# Plan: Restore webhook-primary reactivity in the operator

Date: 2026-07-17
Spec: `docs/superpowers/specs/2026-07-17-webhook-primary-reactivity-design.md`

## Goal

Make SCM webhooks the primary, low-latency trigger for operator reactions and
demote the periodic B.4 sweep to an idempotent backstop, coordinated by
natural-key idempotency (no lock). Wire the human `pull_request_review` path
(dropped entirely today). Remove two confirmed-dead cron mechanisms (`cdScan`,
`healthCheck`).

After this change:

- A brand-new human issue/MR mints its Task on the webhook delivery, within
  seconds, not at the next sweep tick.
- A concurrent webhook mint and sweep mint for the same natural key collapse to
  exactly one Task (the loser gets `AlreadyExists`, swallowed as the normal
  backstop outcome).
- A maintainer's `changes_requested` on a Tatara-owned, not-yet-merged MR
  re-enters `implementing`; an `approved` applies the `reviewing -> merging`
  edge; a `commented` folds into the pending-event path.

## Architecture

Four in-scope components (spec sections 1-5):

1. **Race-safe natural-key idempotent mint** (`internal/controller`, new
   `Minter` type). The mint gives the Task a **deterministic natural-key name**
   (`v1alpha1.IntakeTaskName`), does a **live (uncached) existence pre-check**,
   and swallows the `Create` collision with `client.IgnoreAlreadyExists`,
   handling a stale-terminal name the way the dispatcher already does
   (`queue_controller.go` admit). `queue.EnqueueEvent` is hardened the same way
   for the incident/ticket path (deterministic QueuedEvent name + live check +
   `IgnoreAlreadyExists`).
2. **Shared intake funnel** `(*controller.Minter).MintForItem`. It reuses the
   sweep's existing classify predicates (`IsOrphanIssue`, `ClassifyPR`,
   `MintStage`, `MintReviewStage`) and the sweep's mint bodies
   (`MintIssueTask`, `MintReviewTask`, moved onto `Minter`), so there is ONE
   mint path. `sweepIssues`/`sweepPRs` call the `Minter`; the webhook calls the
   same `Minter`.
3. **Webhook handlers become primary minters** (`internal/webhook/server.go`).
   `handleIssueOpened`, orphan-issue comment, MR-opened, and orphan-MR comment
   call the shared funnel immediately, keeping the existing
   `webhook-originated` stamp, the pending-event side channel, and the
   bot/reporter gates.
4. **Human `pull_request_review` path** (`internal/scm`, `internal/stage`,
   `internal/controller`, `internal/webhook`). Parse `review.state` +
   `review.id`; map GitLab MR approval; gate on `IsMaintainer`; route
   `changes_requested` -> re-enter `implementing` (only while the owned MR is
   NOT merged), `approved` -> `reviewing -> merging`, `commented` -> pending
   event; dedup on `(review.id, state)`.
5. **Dead-cron cleanup** (`api/v1alpha1/project_types.go`,
   `internal/controller/projectscan.go`). Delete `CDScanActivity`/`LastCDScan`,
   `HealthCheckActivity`/`LastHealthCheck`/`ScmCron.HealthCheck`, their
   `activityScheduleAndLast`/`stampScan` switch arms; regenerate CRDs.

## Tech Stack

- Go (pinned exact minor in `go.mod`), controller-runtime, `sigs.k8s.io/...`.
- `log/slog` JSON logs, Prometheus metrics (`internal/obs`).
- Tests: `stretchr/testify/require` + `t.Run` table-driven + fake client
  (`sigs.k8s.io/controller-runtime/pkg/client/fake`, with
  `WithStatusSubresource`). Existing helpers: webhook `seedClient`/`newServer`
  (`internal/webhook/server_test.go`), sweep `sweepReader`/`sweepProject`
  (`internal/controller/sweep_test.go`), queue `newEnqueueTestScheme`
  (`internal/queue/enqueue_test.go`).
- Build tools driven via `mise exec -- go ...` / `mise run {test,lint,build}`.

## Global Constraints (repo hard rules that shape this change)

- **Newest stable Go**, pinned to the exact minor in `go.mod`. No new deps.
- **KISS / never introduce tech-debt.** The mint stays a DIRECT Task create
  (see the divergence note below); do not reroute issue/PR intake through
  `queue.EnqueueEvent` - that would break the parked-owns-its-Issue invariant.
- **Controller-runtime idioms.** `retry.RetryOnConflict` for status writes,
  `client.IgnoreAlreadyExists` / `apierrors.IsNotFound` for races,
  `mgr.GetAPIReader()` for the uncached read (same idiom as
  `DispatcherReconciler.APIReader` and `TaskReconciler.APIReader`).
- **The stage machine is table-driven.** A new transition (`merging ->
  implementing`) MUST be added as a row in `stage.Transitions`; `stage.Enter`
  is the ONE writer of `status.stage`. The `kind=review` guard lives in
  `stage.LegalFor` and must not be bypassed.
- **`make generate manifests test lint build` stays green** (the repo hard-rule
  gate). Dead-cron removal regenerates the CRD; no dangling refs may remain.
- **Observability mandatory:** every new business action logs at INFO with
  structured fields (`action`, `resource_id`, ...); reuse existing counters
  (`obs.OperatorMetrics`) - no new metric is required by this change, but the
  webhook `count(...)` result must stay accurate for the new routes.
- **No plain ENVs or lists in `values.yaml`** - not touched here.

## DIVERGENCE FROM THE SPEC (reconcile before executing)

The spec's mechanism section and the task decomposition say the shared funnel
"calls `queue.EnqueueEvent`" and that the deterministic name lives on the
QueuedEvent. The **actual sweep mints Tasks DIRECTLY** (`mintTaskForIssue` /
`mintReviewTaskForPR` in `sweep.go`), never via a QueuedEvent, because:

1. It **synchronously owns the Issue/MergeRequest CR at mint time**
   (`ownIssue`/`ownMergeRequest`). A `parked(backlog-sweep)` Task NEVER reaches
   `triaging` (where `mintIssueCRs` in `task_stage.go` would otherwise mint the
   Issue CR), so its Issue CR would be un-owned if the mint were deferred to a
   QueuedEvent admission. Owning-at-mint is the load-bearing "no SCM artifact
   without a Task" invariant.
2. **QueuedEvents are ephemeral** - `reconcileDone` GC-deletes them once the
   Task is admitted - so a QueuedEvent cannot be the durable natural-key dedup
   anchor across the Task's whole life. The durable anchor is the Issue/MR CR
   controller-ownership (already read by `IsOrphanIssue` clause b / `ClassifyPR`
   clause 3) plus the Task's own deterministic name.

**Resolution taken in this plan:** natural-key idempotency is realized on the
DIRECT Task create - a deterministic `IntakeTaskName` + live APIReader check +
`IgnoreAlreadyExists` + stale-terminal delete-and-retry. `queue.EnqueueEvent`
is ALSO hardened per the spec's literal Task-1 text, because it is the real
intake for the incident path and admission tickets and shares the identical
principle. The two mint units (Task for issue/PR intake, QueuedEvent for
incident/ticket) each get natural-key idempotency on their own object; there is
no lock anywhere. If the reviewer insists the sweep be rerouted through
QueuedEvents, that is a much larger rewrite that must first solve the
parked-owns-Issue invariant - flagged here, not silently absorbed.

A second, smaller divergence in Task 4: the spec says `changes_requested`
re-enters implementing "regardless of its current pre-merge stage." This plan
narrows that to the stages that can actually hold an owned, unmerged MR
(`reviewing`, `merging`, and `parked` from those) by adding exactly one new
table edge (`merging -> implementing`). Adding `clarifying|brainstorming ->
implementing` edges would bypass the C.6 approval gate - a security regression
the stage machine exists to prevent - so those froms fold to the pending-event
path instead. Flagged for reconciliation.

---

## Task 1 - Race-safe natural-key idempotent mint primitive

Introduce a deterministic natural-key Task name and a race-safe create wrapper,
and harden `queue.EnqueueEvent` for the QueuedEvent intake path. This is the
foundation Tasks 2-3 build on.

### Task 1a - `v1alpha1.IntakeTaskName`

**Files**
- Modify `api/v1alpha1/task_types.go` (add `IntakeTaskName` next to `TaskName`
  ~307-329).
- Create `api/v1alpha1/intake_name_test.go`.

**Interfaces**
- Produces: `func IntakeTaskName(project, kind, repoRef string, number int) string`
  - deterministic, DNS-1123-label-safe, `len <= MaxTaskNameLength` (49).

#### Step 1a.1 - failing test

Create `api/v1alpha1/intake_name_test.go`:

```go
package v1alpha1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestIntakeTaskName_DeterministicAndBounded(t *testing.T) {
	a := v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 353)
	b := v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 353)
	require.Equal(t, a, b, "same natural key must yield the same name")
	require.LessOrEqual(t, len(a), v1alpha1.MaxTaskNameLength)
	require.False(t, v1alpha1.TaskNameTooLong(a))
	require.False(t, strings.HasPrefix(a, "-"))
	require.False(t, strings.HasSuffix(a, "-"))
}

func TestIntakeTaskName_DistinctByKeyPart(t *testing.T) {
	base := v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 353)
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("tatara", "review", "tatara-operator", 353))
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 354))
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-cli", 353))
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("other", "clarify", "tatara-operator", 353))
}

// A very long repoRef must still produce a valid, bounded name.
func TestIntakeTaskName_LongRepoRefStaysBounded(t *testing.T) {
	long := strings.Repeat("x", 200)
	n := v1alpha1.IntakeTaskName("tatara", "clarify", long, 1)
	require.LessOrEqual(t, len(n), v1alpha1.MaxTaskNameLength)
	require.False(t, strings.HasSuffix(n, "-"))
}
```

#### Step 1a.2 - run, expect fail

```
mise exec -- go test ./api/v1alpha1/ -run TestIntakeTaskName
```
Fails: `undefined: v1alpha1.IntakeTaskName`.

#### Step 1a.3 - minimal impl

Add to `api/v1alpha1/task_types.go` (imports: add `crypto/sha256`,
`encoding/hex`, `strconv`; `fmt` already present):

```go
// IntakeTaskName is the DETERMINISTIC name a natural-key intake mint (B.4 sweep
// OR a webhook primary mint) gives its Task, so a concurrent second mint for the
// same (project, kind, repoRef, number) natural key collides on AlreadyExists at
// the apiserver and is swallowed rather than producing a second owner. It is the
// mint's half of the no-lock, natural-key idempotency the reactive intake relies
// on (the Issue/MergeRequest CR controller-ownership is the other half).
//
// A short human-readable prefix (kind initial + repoRef + number) aids ops; a
// sha256 suffix disambiguates across projects/kinds and guarantees the name is
// stable, DNS-1123-safe, and within MaxTaskNameLength even for a long repoRef.
func IntakeTaskName(project, kind, repoRef string, number int) string {
	sum := sha256.Sum256([]byte(project + "|" + kind + "|" + repoRef + "|" + strconv.Itoa(number)))
	suffix := "-" + hex.EncodeToString(sum[:])[:8]
	head := "mt-"
	if kind != "" {
		head += kind[:1] + "-"
	}
	budget := MaxTaskNameLength - len(head) - len(suffix)
	if budget < 0 {
		budget = 0
	}
	base := head + sanitizeNamePart(repoRef, budget) + suffix
	return strings.Trim(base, "-")
}

// sanitizeNamePart lowercases s, keeps [a-z0-9-], collapses other runs to a
// single '-', and caps the result at max chars.
func sanitizeNamePart(s string, max int) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(s) {
		if b.Len() >= max {
			break
		}
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		default:
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}
```

Add `"strings"` to the import block if not already present (it is used by
`TaskName` already; confirm).

#### Step 1a.4 - run, expect pass

```
mise exec -- go test ./api/v1alpha1/ -run TestIntakeTaskName
```

#### Step 1a.5 - commit

`feat: add deterministic IntakeTaskName for natural-key mint dedup`

### Task 1b - harden `queue.EnqueueEvent` (deterministic QE name + live check + IgnoreAlreadyExists)

**Files**
- Modify `internal/queue/enqueue.go` (`EnqueueEvent` ~190-236; add
  `QueuedEventName`).
- Modify `internal/queue/enqueue_test.go` (add concurrent-double-mint test).

**Interfaces**
- Consumes: `client.Client` (with `Scheme()`), `*SeqSource`,
  `*v1alpha1.Project`, `dedupKey string`, `v1alpha1.QueuedEventPayload`.
  Optionally an uncached `client.Reader` for the live check.
- Produces (unchanged signature): `func EnqueueEvent(ctx, c client.Client, seq
  *SeqSource, proj *v1alpha1.Project, class string, autonomous bool, dedupKey
  string, payload v1alpha1.QueuedEventPayload) (*v1alpha1.QueuedEvent, bool, error)`.
- Produces (new): `func QueuedEventName(projectRef, dedupKey string) string`.

#### Step 1b.1 - failing test

Add to `internal/queue/enqueue_test.go`:

```go
func TestEnqueueEvent_ConcurrentSameKeyMintsOnce(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	seq := &SeqSource{Client: c, Namespace: "tatara"}
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}

	const n = 8
	var wg sync.WaitGroup
	created := make([]bool, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, ok, err := EnqueueEvent(context.Background(), c, seq, proj,
				tatarav1alpha1.QueueClassAlert, false, "grp-race", pl)
			created[i], errs[i] = ok, err
		}(i)
	}
	wg.Wait()

	wins := 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i], "AlreadyExists must be swallowed, never surfaced")
		if created[i] {
			wins++
		}
	}
	require.Equal(t, 1, wins, "exactly one concurrent mint may win")

	var qel tatarav1alpha1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel))
	require.Len(t, qel.Items, 1, "exactly one QueuedEvent for the natural key")
	require.Equal(t, QueuedEventName("p", "grp-race"), qel.Items[0].Name)
}
```

Add `"sync"` to the test imports.

Note: the fake client's optimistic-concurrency Create is deterministic under
Go's race detector; the assertion is on the invariant (one winner), which the
deterministic-name + `IgnoreAlreadyExists` guarantees regardless of scheduling.

#### Step 1b.2 - run, expect fail

```
mise exec -- go test ./internal/queue/ -run TestEnqueueEvent_ConcurrentSameKeyMintsOnce
```
Fails: `undefined: QueuedEventName` and duplicate `qe-...` GenerateName objects.

#### Step 1b.3 - minimal impl

In `internal/queue/enqueue.go`, add the deterministic-name helper and switch
`EnqueueEvent` from `GenerateName: "qe-"` to the deterministic name + swallow
the collision. Keep the existing `dedupExists` fast path (it is the cheap cached
short-circuit); the deterministic name is the RACE-SAFE arbiter behind it.

```go
// QueuedEventName is the DETERMINISTIC name a QueuedEvent for (projectRef,
// dedupKey) carries, so a concurrent second EnqueueEvent for the same natural
// key collides on AlreadyExists at the apiserver rather than creating a second
// event behind a lagging dedupExists cache read. A keyless event (dedupKey=="")
// keeps a GenerateName-shaped random name (see EnqueueEvent).
func QueuedEventName(projectRef, dedupKey string) string {
	sum := sha256.Sum256([]byte(projectRef + "|" + dedupKey))
	return "qe-" + hex.EncodeToString(sum[:])[:16]
}
```

Add `"encoding/hex"` to the import block (`crypto/sha256` and `fmt` already
imported).

In `EnqueueEvent`, after the `dedupExists`/seq block, set the name and swallow
`AlreadyExists`:

```go
	om := metav1.ObjectMeta{
		Namespace: proj.Namespace,
		Labels:    labels,
	}
	if dedupKey != "" {
		om.Name = QueuedEventName(proj.Name, dedupKey)
	} else {
		om.GenerateName = "qe-"
	}
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: om,
		Spec:       tatarav1alpha1.QueuedEventSpec{ /* unchanged fields */ },
	}
	if err := controllerutil.SetControllerReference(proj, qe, c.Scheme()); err != nil {
		return nil, false, fmt.Errorf("enqueue: set ownerref: %w", err)
	}
	if err := c.Create(ctx, qe); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// The natural-key backstop: a concurrent mint (or a lagging
			// dedupExists cache read) already created this event. Not an error.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("enqueue: create queuedevent: %w", err)
	}
```

Add `apierrors "k8s.io/apimachinery/pkg/api/errors"` to the imports.

Leave the `qe.Status.State = Queued` + `Status().Update` block unchanged (it
runs only on the winner, which created the object).

#### Step 1b.4 - run, expect pass

```
mise exec -- go test ./internal/queue/... -race
```
Existing `TestEnqueueEvent_AssignsSeqAndFields` / `TestEnqueueEvent_DedupSkips`
must still pass (the deterministic name changes the object name but not the
returned `created`/spec/labels the tests assert).

#### Step 1b.5 - commit

`feat: deterministic QueuedEvent name so concurrent enqueues collapse to one`

---

## Task 2 - Shared intake funnel (`controller.Minter`)

Move the sweep's two mint bodies onto a `Minter` the webhook can also
construct, make the create race-safe (deterministic name + live check +
IgnoreAlreadyExists + stale-terminal handling), and expose `MintForItem` for
webhook use. Assert unchanged sweep behavior.

**Files**
- Create `internal/controller/intake.go` (`Minter`, `ForgeItem`, `MintForItem`,
  `MintIssueTask`, `MintReviewTask`, race-safe `createTaskRaceSafe`).
- Modify `internal/controller/sweep.go`: delete `mintTaskForIssue`
  (~899-952) and `mintReviewTaskForPR` (~992-1044) bodies, replace their call
  sites in `sweepIssues` (~681) / `sweepPRs` (~732) with `r.minter()` calls.
  Keep `stampMintStatus`, `ownIssue`, `ownMergeRequest`, `bindMRToTask`,
  `adoptPRIntoTask` where they are (still used).
- Modify `internal/controller/project_controller.go` (or wherever
  `ProjectReconciler` is declared) to add `func (r *ProjectReconciler) minter()
  *Minter`.
- Create `internal/controller/intake_test.go`.

**Interfaces**
- Produces:
  ```go
  type Minter struct {
      Client    client.Client
      APIReader client.Reader // uncached; nil falls back to Client
      Scheme    *runtime.Scheme
      Metrics   *obs.OperatorMetrics
  }
  type ForgeItem struct {
      IsPR  bool
      Issue scm.Issue  // set when !IsPR
      PR    scm.PRRef  // set when IsPR
  }
  func (m *Minter) MintForItem(ctx context.Context, proj *v1alpha1.Project,
      repo *v1alpha1.Repository, item ForgeItem, webhookOriginated bool,
      sp objbudget.Spiller) (*v1alpha1.Task, bool, error)
  func (m *Minter) MintIssueTask(ctx context.Context, proj *v1alpha1.Project,
      repo *v1alpha1.Repository, ext scm.Issue, stg, reason string,
      sp objbudget.Spiller) (*v1alpha1.Task, bool, error)
  func (m *Minter) MintReviewTask(ctx context.Context, proj *v1alpha1.Project,
      repo *v1alpha1.Repository, pr scm.PRRef, cr *v1alpha1.MergeRequest,
      stg, reason string, sp objbudget.Spiller) (*v1alpha1.Task, bool, error)
  ```
- Consumes: existing package funcs `IsOrphanIssue`, `ClassifyPR`, `MintStage`,
  `MintReviewStage`, `SyncIssue`, `SyncMergeRequest`, `ownIssueForTask`, and the
  existing `(*ProjectReconciler)` methods `ownIssue`/`ownMergeRequest`/
  `bindMRToTask`/`stampMintStatus` (refactored to take a `client.Client` or
  kept as thin wrappers - see impl).

### Step 2.1 - failing tests (behavior parity + race-safety)

Create `internal/controller/intake_test.go`. It reuses the sweep test helpers
(`sweepProject`, `sweepRepo`, `testNS`) already in package `controller`.

```go
package controller

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func minterFor(t *testing.T, objs ...client.Object) (*Minter, client.Client) {
	t.Helper()
	c := newControllerClient(t, objs...) // existing sweep-test scheme+fake builder
	return &Minter{Client: c, APIReader: c, Scheme: c.Scheme()}, c
}

// A webhook-originated issue mints an ACTIVE (triaging) clarify Task that owns
// its Issue CR - the same outcome the sweep produces, on the same natural key.
func TestMintForItem_IssueWebhookOriginated_MintsTriagingClarify(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, c := minterFor(t, proj, repo)

	item := ForgeItem{Issue: scm.Issue{Number: 353, State: "open", Author: "alice",
		Title: "login 500s", URL: "https://github.com/o/r/issues/353"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, tatarav1alpha1.SweepIssueKind, task.Spec.Kind)
	require.Equal(t, tatarav1alpha1.StageTriaging, task.Spec.InitialStage)
	require.Equal(t, tatarav1alpha1.IntakeTaskName("p", "clarify", "tatara-operator", 353), task.Name)

	// Issue CR is owned by the minted Task (the durable natural-key anchor).
	var iss tatarav1alpha1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: tatarav1alpha1.IssueName("tatara-operator", 353)}, &iss))
	owner, ok := own.ControllerOwner(&iss)
	require.True(t, ok)
	require.Equal(t, task.Name, owner)
}

// A non-webhook (cold-backlog) issue mints parked(backlog-sweep).
func TestMintForItem_ColdIssue_MintsParked(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{Issue: scm.Issue{Number: 7, State: "open", Author: "alice"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, tatarav1alpha1.StageParked, task.Spec.InitialStage)
	require.Equal(t, stage.ReasonBacklogSweep, task.Spec.InitialStageReason)
}

// An already-owned issue is not re-minted (the steady-state backstop dedup).
func TestMintForItem_OwnedIssue_NoOp(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{Issue: scm.Issue{Number: 9, State: "open", Author: "alice"}}
	_, created, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.True(t, created)
	_, created2, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
	require.NoError(t, err)
	require.False(t, created2, "an owned issue is not an orphan; the backstop no-ops")
}

// A human PR in reaction scope mints a review Task (triaging, no prior verdict).
func TestMintForItem_HumanPR_MintsReview(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{IsPR: true, PR: scm.PRRef{Number: 42, Author: "alice",
		HeadSHA: "abc", HeadBranch: "fix", Repo: "o/r"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, tatarav1alpha1.SweepReviewKind, task.Spec.Kind)
	require.Equal(t, tatarav1alpha1.StageTriaging, task.Spec.InitialStage)
}

// A bot-authored PR is ignored (ClassifyPR clause 2): no mint.
func TestMintForItem_BotPR_NoMint(t *testing.T) {
	proj := sweepProject("p") // BotLogin "tatara-bot"
	repo := sweepRepo("p")
	m, _ := minterFor(t, proj, repo)
	item := ForgeItem{IsPR: true, PR: scm.PRRef{Number: 43, Author: "tatara-bot",
		HeadSHA: "abc", HeadBranch: "chore", Repo: "o/r"}}
	task, created, err := m.MintForItem(context.Background(), proj, repo, item, false, nil)
	require.NoError(t, err)
	require.False(t, created)
	require.Nil(t, task)
}

// Two concurrent mints for the same issue natural key collapse to ONE Task.
func TestMintForItem_ConcurrentSameKey_OneTask(t *testing.T) {
	proj := sweepProject("p")
	repo := sweepRepo("p")
	m, c := minterFor(t, proj, repo)
	item := ForgeItem{Issue: scm.Issue{Number: 100, State: "open", Author: "alice"}}

	const n = 6
	var wg sync.WaitGroup
	wins := make([]bool, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, ok, err := m.MintForItem(context.Background(), proj, repo, item, true, nil)
			wins[i], errs[i] = ok, err
		}(i)
	}
	wg.Wait()
	got := 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		if wins[i] {
			got++
		}
	}
	require.Equal(t, 1, got)
	var tl tatarav1alpha1.TaskList
	require.NoError(t, c.List(context.Background(), &tl))
	require.Len(t, tl.Items, 1)
}
```

(If the existing sweep tests do not already expose a `newControllerClient`
helper, add a minimal one in `intake_test.go` mirroring `sweep_test.go`'s fake
builder: scheme with `tatarav1alpha1`, `corev1`; `WithStatusSubresource(&Task{},
&Issue{}, &MergeRequest{}, &Project{})`.)

### Step 2.2 - run, expect fail

```
mise exec -- go test ./internal/controller/ -run TestMintForItem
```
Fails: `undefined: Minter` / `ForgeItem`.

### Step 2.3 - minimal impl

Create `internal/controller/intake.go`. The mint bodies are the current
`mintTaskForIssue`/`mintReviewTaskForPR` verbatim EXCEPT (a) the Task `Name`
uses `IntakeTaskName`, and (b) the `r.Create(task)` becomes a race-safe create.
`SyncIssue`/`ownIssueForTask`/`SyncMergeRequest` already take a `client.Client`;
`stampMintStatus`/`ownIssue`/`ownMergeRequest`/`bindMRToTask` are moved onto
`Minter` (or kept as `*ProjectReconciler` methods that delegate to a `Minter`
built from `r`). To keep churn minimal, move them onto `Minter` and give
`ProjectReconciler` thin delegators.

```go
package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Minter is the ONE reactive intake mint path (B.4). Both the sweep loop and
// the webhook construct one and call MintForItem, so "what Task does this forge
// item produce" has a single source of truth. The mint is a DIRECT Task create
// (it synchronously owns the Issue/MergeRequest CR at mint time, which a
// parked(backlog-sweep) Task depends on), made race-safe by a deterministic
// natural-key Task name + a live existence check + IgnoreAlreadyExists.
type Minter struct {
	Client    client.Client
	APIReader client.Reader // uncached; nil falls back to Client
	Scheme    *runtime.Scheme
	Metrics   *obs.OperatorMetrics
}

func (m *Minter) reader() client.Reader {
	if m.APIReader != nil {
		return m.APIReader
	}
	return m.Client
}

// ForgeItem is one forge work item the intake funnel classifies + mints for.
type ForgeItem struct {
	IsPR  bool
	Issue scm.Issue // when !IsPR
	PR    scm.PRRef // when IsPR
}

// MintForItem classifies item with the SAME predicates the sweep uses and mints
// the Task if one is owed, race-safe on the natural key. created=false means
// "nothing to mint" (bot/ignored/already-owned) OR "the backstop found it
// already minted". It applies NO creation budget: the webhook mints a live human
// signal immediately, and downstream admission (ensureTicket -> dispatcher)
// bounds concurrency. The sweep keeps its own budget check BEFORE calling the
// per-stage mint helpers (see sweepIssues/sweepPRs).
func (m *Minter) MintForItem(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, item ForgeItem, webhookOriginated bool,
	sp objbudget.Spiller) (*tatarav1alpha1.Task, bool, error) {

	if item.IsPR {
		cr, err := m.mergeRequestCR(ctx, proj, repo, item.PR.Number)
		if err != nil {
			return nil, false, err
		}
		// A human PR never carries a task/<name> head branch, so it has no owning
		// Task by branch: ClassifyPR's orphan check keys on the MR CR owner only.
		switch ClassifyPR(proj, repo, item.PR, nil, cr) {
		case PRReview:
			stg, reason := MintReviewStage(cr)
			return m.MintReviewTask(ctx, proj, repo, item.PR, cr, stg, reason, sp)
		default: // PRAdopt (sweep-only) / PRIgnore
			return nil, false, nil
		}
	}

	cr, err := m.issueCR(ctx, proj, repo, item.Issue.Number)
	if err != nil {
		return nil, false, err
	}
	if !IsOrphanIssue(proj, repo, item.Issue, cr) {
		return nil, false, nil
	}
	stg, reason := MintStage(proj, item.Issue, webhookOriginated)
	return m.MintIssueTask(ctx, proj, repo, item.Issue, stg, reason, sp)
}
```

`MintIssueTask` / `MintReviewTask` are the moved bodies. The only changed lines
vs. the current `mintTaskForIssue` / `mintReviewTaskForPR`:

```go
func (m *Minter) MintIssueTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, ext scm.Issue, stg, reason string,
	sp objbudget.Spiller) (*tatarav1alpha1.Task, bool, error) {

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, ext.Number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:         proj.Name,
			Kind:               SweepIssueKind,
			Goal:               issueGoal(ext),
			InitialStage:       stg,
			InitialStageReason: reason,
			Source: &tatarav1alpha1.TaskSource{
				Provider:    providerOf(proj),
				IssueRef:    fmt.Sprintf("%s#%d", ext.URL, ext.Number),
				Number:      ext.Number,
				Title:       ext.Title,
				AuthorLogin: ext.Author,
			},
		},
	}
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return nil, false, fmt.Errorf("intake: set task ownerref: %w", err)
	}
	created, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, false, err
	}
	if !created {
		return task, false, nil // backstop: the natural-key twin already exists
	}
	if err := SyncIssue(ctx, m.Client, sp, proj, repo, ext); err != nil {
		return nil, false, fmt.Errorf("intake: sync issue: %w", err)
	}
	issName := tatarav1alpha1.IssueName(repo.Name, ext.Number)
	if err := ownIssueForTask(ctx, m.Client, proj.Namespace, issName, task); err != nil {
		return nil, false, err
	}
	if err := m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.IssueRefs, issName) {
			fresh.Status.IssueRefs = append(fresh.Status.IssueRefs, issName)
		}
	}); err != nil {
		return nil, false, err
	}
	if m.Metrics != nil {
		m.Metrics.OrphanAdopted(SweepIssueKind)
	}
	return task, true, nil
}
```

`MintReviewTask` is the current `mintReviewTaskForPR` body with the same two
changes (name via `IntakeTaskName(proj.Name, SweepReviewKind, repo.Name,
pr.Number)`, create via `createTaskRaceSafe`), returning `(task, created,
err)`; on `created==false` it returns early without binding the MR.

`stampMintStatus`, `mergeRequestCR`, `issueCR`, `ownIssue`, `ownMergeRequest`,
`bindMRToTask` move to methods on `*Minter` (they currently receive `r`, use
`r.Get`/`r.Status`/`r.Client`; substitute `m.Client`). `ownIssueForTask`,
`SyncIssue`, `SyncMergeRequest` already take a `client.Client` and are reused
unchanged.

The race-safe create (mirrors `queue_controller.go` admit's AlreadyExists
handling):

```go
// createTaskRaceSafe creates task idempotently on its DETERMINISTIC name. On a
// natural-key collision (a concurrent webhook + sweep, or the backstop pass over
// an already-minted item) it returns created=false rather than a second Task.
// A collision with a DEAD (terminal/deleting) twin of the same name is the
// re-mint-after-reap case: delete the tombstone and retry, so a legitimately new
// event is never blocked by a dead name.
func (m *Minter) createTaskRaceSafe(ctx context.Context, task *tatarav1alpha1.Task) (bool, error) {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	// Live (uncached) pre-check shrinks the window before the deterministic-name
	// collision even applies; the collision below is the actual arbiter.
	existing := &tatarav1alpha1.Task{}
	if err := m.reader().Get(ctx, key, existing); err == nil {
		if existing.DeletionTimestamp == nil && !tatarav1alpha1.TaskDone(existing) {
			return false, nil // live twin: the backstop no-ops
		}
	} else if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("intake: pre-check task %s: %w", key.Name, err)
	}

	err := m.Client.Create(ctx, task)
	if err == nil {
		return true, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return false, fmt.Errorf("intake: create task %s: %w", key.Name, err)
	}
	if getErr := m.Client.Get(ctx, key, existing); getErr != nil {
		return false, fmt.Errorf("intake: resolve existing task %s: %w", key.Name, getErr)
	}
	if existing.DeletionTimestamp != nil || tatarav1alpha1.TaskDone(existing) {
		if delErr := m.Client.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
			return false, fmt.Errorf("intake: delete stale terminal task %s: %w", key.Name, delErr)
		}
		log.FromContext(ctx).Info("intake: deleted stale terminal task on name collision; re-minting next pass",
			"action", "intake_stale_delete", "resource_id", key.Name)
		return false, nil // re-mint on the next tick against the freed name
	}
	return false, nil // live twin
}
```

Then in `sweep.go`:
- add `func (r *ProjectReconciler) minter() *Minter { return &Minter{Client:
  r.Client, APIReader: r.APIReader, Scheme: r.Scheme, Metrics: r.Metrics} }`
  (thread `r.APIReader`; if `ProjectReconciler` has no `APIReader` field, add
  one wired to `mgr.GetAPIReader()` in its `SetupWithManager`, mirroring
  `TaskReconciler.APIReader`).
- `sweepIssues`: after `budget.allow(...)`, replace `r.mintTaskForIssue(...)`
  with `r.minter().MintIssueTask(ctx, proj, repo, ext, stg, reason, sp)`,
  handling the new `(task, created, err)` return: on `created==false && err==nil`
  the item was already minted (a webhook beat the sweep) - `continue` without
  `budget.record`/`clearWebhookOriginated`. On `created`, keep the existing
  `clearWebhookOriginated` + `budget.record` + `minted[stg]++` + log.
- `sweepPRs`: replace `r.mintReviewTaskForPR(...)` with
  `r.minter().MintReviewTask(...)`, same `created` handling.
- Delete the now-unused `mintTaskForIssue`/`mintReviewTaskForPR` methods.

### Step 2.4 - run, expect pass

```
mise exec -- go test ./internal/controller/ -run 'TestMintForItem|TestSweep' -race
```
Existing `sweep_test.go` behavior tests must still pass. If a sweep test
asserted a specific random-suffixed Task name, update it to
`IntakeTaskName(...)` (names are now deterministic).

### Step 2.5 - commit

`refactor: extract sweep mint into race-safe controller.Minter shared funnel`

---

## Task 3 - Webhook handlers become primary minters

The webhook calls the shared `Minter` immediately, keeping every existing side
effect and gate.

**Files**
- Modify `internal/webhook/server.go`: add a `minter()` helper on `*Server`;
  in `handleIssueOpened` (~297-334) mint after the gates + stamp; in
  `handleForgeItem` (~270-280) route MR-opened to a new `handleMROpened`; in
  `handleIssueComment` (~345-368) mint for an ORPHAN issue/MR before the pending
  path.
- Modify `internal/webhook/pending_events.go`: `deliverPendingEvent` already
  no-ops for an orphan (`own.ControllerOwner` miss returns) - the orphan mint is
  done by the caller BEFORE `deliverPendingEvent`, so the newly-minted Task can
  then adopt the comment on its next reconcile.
- Create `internal/webhook/primary_mint_test.go`.

**Interfaces**
- Consumes: `controller.Minter`, `controller.ForgeItem`,
  `s.cfg.Client`/`s.cfg.APIReader`/`s.cfg.Namespace`,
  `tatarav1.IsAllowedReporter`, `isBotActor`, `s.matchRepo`,
  `s.cfg.SpillerFor(&proj)`.
- Produces: `func (s *Server) minter() *controller.Minter`;
  `func (s *Server) handleMROpened(ctx, w, provider, proj, ev)`.

### Step 3.1 - failing tests

Create `internal/webhook/primary_mint_test.go` (package `webhook_test`, reusing
`seedClient`/`newServer`/`projectWithReporters`/`repository`/`ghSign`/`post`/
`allTasks`/`allQEs`):

```go
// A human opens a NEW issue: the webhook mints an ACTIVE clarify Task NOW, and
// the mirror Issue CR is owned by it. (Supersedes the old "mints nothing" test.)
func TestIssueOpened_MintsClarifyTaskImmediately(t *testing.T) {
	const secretVal = "whsec-mint1"
	c := seedClient(t,
		projectWithReporters("mp", "mp-scm", "tatara", "tatara-bot", nil),
		secret("mp-scm", secretVal),
		repository("repo-open", "mp", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp", secretVal, issueOpenedBy("opened", "alice", 353))

	tasks := allTasks(t, c, "mp")
	require.Len(t, tasks, 1)
	require.Equal(t, "clarify", tasks[0].Spec.Kind)
	require.Equal(t, tatarav1.StageTriaging, tasks[0].Spec.InitialStage)

	var iss tatarav1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-open", 353)}, &iss))
	owner, ok := own.ControllerOwner(&iss)
	require.True(t, ok)
	require.Equal(t, tasks[0].Name, owner)
}

// A bot-authored issue.opened mints nothing (self-loop guard).
func TestIssueOpened_BotAuthored_NoMint(t *testing.T) {
	const secretVal = "whsec-mint2"
	c := seedClient(t,
		projectWithReporters("mp2", "mp2-scm", "tatara", "tatara-bot", nil),
		secret("mp2-scm", secretVal),
		repository("repo-b", "mp2", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp2", secretVal, issueOpenedBy("opened", "tatara-bot", 5))
	require.Empty(t, allTasks(t, c, "mp2"))
}

// An author outside a non-empty reporter allowlist mints nothing (issue #102).
func TestIssueOpened_NotAllowedReporter_NoMint(t *testing.T) {
	const secretVal = "whsec-mint3"
	c := seedClient(t,
		projectWithReporters("mp3", "mp3-scm", "tatara", "tatara-bot", []string{"alice"}),
		secret("mp3-scm", secretVal),
		repository("repo-c", "mp3", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp3", secretVal, issueOpenedBy("opened", "mallory", 8))
	require.Empty(t, allTasks(t, c, "mp3"))
}

// After the webhook mints, a sweep pass over the same issue no-ops (backstop
// idempotency): still exactly one Task.
func TestSweepAfterWebhook_NoDoubleMint(t *testing.T) {
	const secretVal = "whsec-mint4"
	proj := projectWithReporters("mp4", "mp4-scm", "tatara", "tatara-bot", nil)
	repo := repository("tatara-operator", "mp4", "https://github.com/o/r.git", "main")
	c := seedClient(t, proj, secret("mp4-scm", secretVal), repo)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp4", secretVal, issueOpenedBy("opened", "alice", 353))
	require.Len(t, allTasks(t, c, "mp4"), 1)

	// Drive the shared funnel again as the sweep would, same natural key.
	m := &controller.Minter{Client: c, APIReader: c, Scheme: c.Scheme()}
	_, created, err := m.MintForItem(context.Background(), proj, repo,
		controller.ForgeItem{Issue: scm.Issue{Number: 353, State: "open", Author: "alice"}}, false, nil)
	require.NoError(t, err)
	require.False(t, created, "the Issue CR is owned; the sweep backstop no-ops")
	require.Len(t, allTasks(t, c, "mp4"), 1)
}

// A human opens a PR: the webhook mints a review Task immediately.
func TestPROpened_MintsReviewTaskImmediately(t *testing.T) {
	const secretVal = "whsec-mint5"
	c := seedClient(t,
		projectWithReporters("mp5", "mp5-scm", "tatara", "tatara-bot", nil),
		secret("mp5-scm", secretVal),
		repository("repo-pr", "mp5", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "mp5", secretVal, prOpenedBy("opened", "alice", 42)) // helper: X-GitHub-Event: pull_request
	tasks := allTasks(t, c, "mp5")
	require.Len(t, tasks, 1)
	require.Equal(t, "review", tasks[0].Spec.Kind)
}
```

Add `postPROpened` / `prOpenedBy` helpers mirroring `postIssueOpened` /
`issueOpenedBy` but with `X-GitHub-Event: pull_request` and a `pull_request`
body (`{"action":"opened","pull_request":{"number":42,"user":{"login":"alice"},
"head":{"sha":"abc","ref":"fix"},"html_url":"..."},"repository":{...},
"sender":{"login":"alice"}}`).

### Step 3.2 - run, expect fail

```
mise exec -- go test ./internal/webhook/ -run 'TestIssueOpened_MintsClarify|TestPROpened|TestSweepAfterWebhook'
```
Fails: no Task minted (handlers only stamp today; `handleMROpened` missing).

### Step 3.3 - minimal impl

In `internal/webhook/server.go`:

Add the `Minter` builder:

```go
func (s *Server) minter() *controller.Minter {
	return &controller.Minter{
		Client:    s.cfg.Client,
		APIReader: s.cfg.APIReader, // nil-safe: Minter falls back to Client
		Scheme:    s.cfg.Client.Scheme(),
		Metrics:   nil, // webhook mint does not double-count OrphanAdopted
	}
}
```

`handleIssueOpened`: after the successful `MarkWebhookOriginated` stamp and its
log, mint immediately (the marker stays, so the sweep still recognizes the issue
if the mint somehow lost a race):

```go
	item := controller.ForgeItem{Issue: scm.Issue{
		Number: ev.Number, State: "open", Author: ev.ActorLogin,
		Title: ev.Title, Body: ev.Body, Labels: ev.Labels, URL: ev.URL,
	}}
	if _, created, merr := s.minter().MintForItem(ctx, &proj, repo, item, true, s.cfg.SpillerFor(&proj)); merr != nil {
		s.log.ErrorContext(ctx, "issues: primary mint failed", "error", merr,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.reject(w, http.StatusInternalServerError, "mint issue", provider, ev.Kind, ev.Action, "error")
		return
	} else if created {
		s.log.InfoContext(ctx, "issues: webhook minted clarify task",
			"action", "issue_webhook_mint", "project", proj.Name, "repository", repo.Name, "number", ev.Number)
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
```

(`MintForItem` takes `*proj`; `handleIssueOpened` receives `proj` by value, so
pass `&proj`.)

`handleForgeItem`: add the MR-opened route before the final `accept(ignored)`:

```go
	if ev.Kind == "mr" && ev.IsPR && !ev.IsComment && !ev.IsReview &&
		(ev.Action == "opened" || ev.Action == "reopened") {
		s.handleMROpened(ctx, w, provider, proj, ev)
		return
	}
```

`handleMROpened` (new) - the bot gate first (an agent's own PR must never mint a
review Task), then the reporter gate, then the shared funnel. `ClassifyPR`
inside `MintForItem` already ignores bot-authored non-adoptable PRs, but the bot
gate here keeps the webhook's self-loop guard explicit and parallel to
`handleIssueOpened`:

```go
func (s *Server) handleMROpened(ctx context.Context, w http.ResponseWriter, provider string,
	proj tatarav1.Project, ev scm.WebhookEvent) {
	if isBotActor(&proj, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if !tatarav1.IsAllowedReporter(&proj, repo, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	slug := ""
	if o, n, oerr := scm.OwnerRepo(repo.Spec.URL); oerr == nil {
		slug = o + "/" + n
	}
	item := controller.ForgeItem{IsPR: true, PR: scm.PRRef{
		Number: ev.Number, Author: ev.ActorLogin, HeadSHA: ev.HeadSHA,
		HeadBranch: ev.HeadBranch, Repo: slug, Body: ev.Body, Labels: ev.Labels,
	}}
	if _, created, merr := s.minter().MintForItem(ctx, &proj, repo, item, false, s.cfg.SpillerFor(&proj)); merr != nil {
		s.reject(w, http.StatusInternalServerError, "mint mr", provider, ev.Kind, ev.Action, "error")
		return
	} else if created {
		s.log.InfoContext(ctx, "mr: webhook minted review task",
			"action", "mr_webhook_mint", "project", proj.Name, "repository", repo.Name, "number", ev.Number)
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}
```

`handleIssueComment` (orphan mint): a comment on an issue/MR that has NO owning
Task yet should mint via the funnel so a maintainer's first "@bot go" spawns
work immediately, instead of waiting for the sweep. Do this only when the mirror
either does not exist OR is un-owned (orphan); the existing `deliverPendingEvent`
already handles the owned case. Insert before the final
`s.deliverPendingEvent(...)`:

```go
	if s.commentIsOrphan(ctx, commentRepo, ev) {
		var item controller.ForgeItem
		if ev.IsPR {
			slug, _ := repoSlug(commentRepo)
			item = controller.ForgeItem{IsPR: true, PR: scm.PRRef{
				Number: ev.Number, Author: ev.ActorLogin, HeadBranch: ev.HeadBranch, Repo: slug}}
		} else {
			item = controller.ForgeItem{Issue: scm.Issue{
				Number: ev.Number, State: "open", Author: ev.ActorLogin,
				Title: ev.Title, Body: ev.Body, Labels: ev.Labels, URL: ev.URL}}
		}
		if _, _, merr := s.minter().MintForItem(ctx, &proj, commentRepo, item, false, s.cfg.SpillerFor(&proj)); merr != nil {
			s.log.ErrorContext(ctx, "issue_comment: orphan mint failed", "error", merr, "issue_ref", ev.IssueRef)
		}
	}
	s.deliverPendingEvent(ctx, proj, commentRepo, ev)
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
```

`commentIsOrphan` reads the mirror CR (uncached) and reports true when it is
absent or un-owned:

```go
func (s *Server) commentIsOrphan(ctx context.Context, repo *tatarav1.Repository, ev scm.WebhookEvent) bool {
	if repo == nil || ev.Number <= 0 {
		return false
	}
	name := tatarav1.IssueName(repo.Name, ev.Number)
	obj := client.Object(&tatarav1.Issue{})
	if ev.IsPR {
		name = tatarav1.MergeRequestName(repo.Name, ev.Number)
		obj = &tatarav1.MergeRequest{}
	}
	rdr := client.Reader(s.cfg.Client)
	if s.cfg.APIReader != nil {
		rdr = s.cfg.APIReader
	}
	if err := rdr.Get(ctx, objKey(s.cfg.Namespace, name), obj); err != nil {
		return apierrors.IsNotFound(err) // no mirror yet -> orphan; on other error, do not mint
	}
	_, owned := own.ControllerOwner(obj)
	return !owned
}
```

Add imports to `server.go`: `"github.com/szymonrychu/tatara-operator/internal/own"`
(if not present) and `"github.com/szymonrychu/tatara-operator/internal/scm"`
(present) and `apierrors` (present). `repoSlug`/`repoSlug(commentRepo)` is a
tiny local helper returning `scm.OwnerRepo(repo.Spec.URL)` joined by `/`.

Update `internal/webhook/issue_opened_test.go`: `TestIssueOpened_*` currently
asserts `require.Empty(t, allTasks(...))` and `require.Empty(t, allQEs(...))`.
Those assertions are now WRONG (the webhook mints). Change them to assert the
Task IS minted (or move the "mints nothing" assertions into the superseded set).
The marker-stamp assertions stay valid.

### Step 3.4 - run, expect pass

```
mise exec -- go test ./internal/webhook/... -race
```

### Step 3.5 - commit

`feat: webhook mints issue/MR Tasks immediately via the shared intake funnel`

---

## Task 4 - Human `pull_request_review` path

Parse review state/id (GitHub) and MR approval (GitLab); add the
`merging -> implementing` stage edge and a review re-entry helper; add the two
controller appliers; route in the webhook with an `IsMaintainer` gate and
`(review.id, state)` dedup.

### Task 4a - SCM parse

**Files**
- Modify `internal/scm/scm.go` (`WebhookEvent` ~17-38: add review fields).
- Modify `internal/scm/github.go` (`ghPayload` ~49-70: add `Review`; the
  `pull_request_review` case ~99-100).
- Modify `internal/scm/gitlab.go` (`glWorkItemEvent` ~101-132: surface the
  `approved` action as a review).
- Create/extend `internal/scm/github_test.go` / `gitlab_test.go` review cases.

**Interfaces**
- Produces on `WebhookEvent`: `IsReview bool`, `ReviewState string`
  (`approved` | `changes_requested` | `commented` | `dismissed`),
  `ReviewID string`, `ReviewCommitSHA string`.

#### Step 4a.1 - failing test

Add to `internal/scm/github_test.go`:

```go
func TestGitHub_PullRequestReview_ParsesStateAndID(t *testing.T) {
	body := []byte(`{"action":"submitted",
		"review":{"id":900,"state":"changes_requested","commit_id":"deadbeef","user":{"login":"maint"}},
		"pull_request":{"number":42,"user":{"login":"alice"},"head":{"sha":"deadbeef","ref":"fix"},"html_url":"u"},
		"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},
		"sender":{"login":"maint"}}`)
	h := http.Header{}
	h.Set("X-GitHub-Event", "pull_request_review")
	h.Set("X-Hub-Signature-256", ghTestSign("s", body))
	ev, err := (&GitHub{}).DetectAndVerify(h, body, "s")
	require.NoError(t, err)
	require.True(t, ev.IsReview)
	require.Equal(t, "changes_requested", ev.ReviewState)
	require.Equal(t, "900", ev.ReviewID)
	require.Equal(t, "deadbeef", ev.ReviewCommitSHA)
	require.Equal(t, "maint", ev.ActorLogin)
	require.Equal(t, 42, ev.Number)
	require.Equal(t, "mr", ev.Kind)
}
```

(Reuse the package's existing HMAC signer helper - `ghTestSign` or the local
equivalent already present in `internal/scm` webhook tests.)

Add to `internal/scm/gitlab_test.go`:

```go
func TestGitLab_MRApproval_MapsToReviewApproved(t *testing.T) {
	body := []byte(`{"object_kind":"merge_request",
		"user":{"username":"maint"},
		"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},
		"object_attributes":{"iid":42,"action":"approved","last_commit":{"id":"deadbeef"},"source_branch":"fix"}}`)
	h := http.Header{}
	h.Set("X-Gitlab-Event", "Merge Request Hook")
	h.Set("X-Gitlab-Token", "s")
	ev, err := (&GitLab{}).DetectAndVerify(h, body, "s")
	require.NoError(t, err)
	require.True(t, ev.IsReview)
	require.Equal(t, "approved", ev.ReviewState)
	require.Equal(t, "deadbeef", ev.ReviewCommitSHA)
}
```

#### Step 4a.2 - run, expect fail

```
mise exec -- go test ./internal/scm/ -run 'PullRequestReview|MRApproval'
```

#### Step 4a.3 - minimal impl

`scm.go` - add to `WebhookEvent`:

```go
	IsReview        bool   // true only for pull_request_review / GitLab MR-approval
	ReviewState     string // approved | changes_requested | commented | dismissed
	ReviewID        string // provider review id, for (review.id, state) dedup
	ReviewCommitSHA string // the reviewed commit sha (github review.commit_id / gitlab last_commit)
```

`github.go` - add to `ghPayload`:

```go
	Review struct {
		ID       int64  `json:"id"`
		State    string `json:"state"`
		CommitID string `json:"commit_id"`
	} `json:"review"`
```

Change the `pull_request_review` case (currently identical to `pull_request`):

```go
	case "pull_request_review":
		ev := ghWorkItemEvent("mr", true, p, p.PullRequest)
		ev.IsReview = true
		ev.ReviewState = p.Review.State // github vocab already: approved|changes_requested|commented|dismissed
		ev.ReviewID = strconv.FormatInt(p.Review.ID, 10)
		ev.ReviewCommitSHA = p.Review.CommitID
		return ev, nil
```

(`ghNormalizeAction("submitted")` already yields `"submitted"`; dismissed/edited
collapse to `"other"`, which the webhook handler ignores.)

`gitlab.go` - in `glWorkItemEvent`, after building the base event for an MR,
surface the approval action as a review (GitLab has no changes_requested /
commented review object - those arrive as notes and take the pending-event
path):

```go
	if kind == "mr" && p.ObjectAttributes.Action == "approved" {
		ev.IsReview = true
		ev.ReviewState = "approved"
		ev.ReviewCommitSHA = p.ObjectAttributes.LastCommit.ID
		ev.ReviewID = fmt.Sprintf("gl-approve-%d-%s", p.ObjectAttributes.IID, p.ObjectAttributes.LastCommit.ID)
	}
```

(`glActionAndLabel` still maps `"approved"` action to `"submitted"` for the
metric label; the review fields are additive.)

#### Step 4a.4 - run, expect pass; then commit

`feat: parse pull_request_review state/id and GitLab MR approval`

### Task 4b - stage machine: `merging -> implementing` edge + review re-entry helper

**Files**
- Modify `internal/stage/stage.go` (`Transitions[StageMerging]` ~272-281;
  add `ReenterImplementingOnReview` near `RequestChanges` ~671).
- Modify `internal/stage/stage_test.go` (or the relevant stage test file).

**Interfaces**
- Produces: `func ReenterImplementingOnReview(t *v1alpha1.Task, mrs
  []v1alpha1.MergeRequest, now time.Time) (ok bool)`.

#### Step 4b.1 - failing test

Add to the stage tests:

```go
func TestReenterImplementingOnReview(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name    string
		kind    string
		from    string
		wantOK  bool
	}{
		{"from reviewing", "clarify", v1alpha1.StageReviewing, true},
		{"from merging", "clarify", v1alpha1.StageMerging, true},
		{"from implementing is redundant", "clarify", v1alpha1.StageImplementing, false},
		{"kind=review never re-enters", "review", v1alpha1.StageReviewing, false},
		{"terminal failed not resurrected", "clarify", v1alpha1.StageFailed, false},
		{"delivered not resurrected", "clarify", v1alpha1.StageDelivered, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: tc.kind}}
			task.Status.Stage = tc.from
			ent := metav1.NewTime(now.Add(-time.Hour))
			task.Status.StageEnteredAt = &ent
			ok := stage.ReenterImplementingOnReview(task, nil, now)
			require.Equal(t, tc.wantOK, ok)
			if ok {
				require.Equal(t, v1alpha1.StageImplementing, task.Status.Stage)
				require.Nil(t, task.Status.PodStartedAt) // Enter's re-arm ran
			}
		})
	}
}
```

Note: `reviewing -> implementing` in `LegalFor` is gated by `reviewGateOpen(mrs)`
(all owned MRs `PendingReview == nil`). With `mrs == nil`, `reviewGateOpen`
returns false, so the "from reviewing" case must pass owned MRs with
`PendingReview == nil`. Adjust the test to pass
`[]v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{}}}` for the
reviewing case (documenting that a bot review still owed BLOCKS the maintainer
re-entry - which then folds to the pending-event path in Task 4d).

#### Step 4b.2 - run, expect fail (undefined + illegal `merging -> implementing`).

#### Step 4b.3 - minimal impl

Add the edge to `Transitions[v1alpha1.StageMerging]`:

```go
		{To: v1alpha1.StageImplementing, Trigger: "a maintainer requested changes on the still-open MR before it merged (F.6-adjacent). kind=review refused by LegalFor"},
```

Add the helper (mirrors `RequestChanges`'s altitude; delegates the legality to
`Enter`/`LegalFor` so the `kind=review` guard and the table are authoritative):

```go
// ReenterImplementingOnReview re-enters implementing after a maintainer's
// changes_requested on a Tatara-owned, NOT-yet-merged MR. The caller has already
// verified the MR is not merged (the merged/finished boundary is the caller's,
// per the spec). It respects the F.3 table (only froms with an edge to
// implementing - reviewing, merging, approved, parked - succeed) and the
// kind=review guard (via Enter -> LegalFor). A terminal Task is never
// resurrected, and an already-implementing Task is a redundant no-op.
func ReenterImplementingOnReview(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, now time.Time) (ok bool) {
	if now.IsZero() {
		now = time.Now()
	}
	switch t.Status.Stage {
	case v1alpha1.StageRejected, v1alpha1.StageFailed, v1alpha1.StageDelivered, v1alpha1.StageImplementing:
		return false
	}
	if err := Enter(t, mrs, v1alpha1.StageImplementing, "", now); err != nil {
		return false
	}
	return true
}
```

#### Step 4b.4 - run the full stage suite (the `legalPairs`/table-consistency
tests will now include the new edge):

```
mise exec -- go test ./internal/stage/...
```

#### Step 4b.5 - commit

`feat: add merging->implementing edge and ReenterImplementingOnReview helper`

### Task 4c - controller appliers

**Files**
- Create `internal/controller/review_apply.go`
  (`ApplyReviewChangesRequested`, `ApplyReviewApproval`).
- Create `internal/controller/review_apply_test.go`.

**Interfaces**
- Consumes: `client.Client`, `objbudget.Spiller`, `*v1alpha1.Project`,
  `*v1alpha1.Task`, `ownedMergeRequests`, `stage.ReenterImplementingOnReview`,
  `stage.Enter`, `objbudget.FitMergeRequest`, `EnterStage` (the driver choke
  point) or `r.patchTaskStatus`-style status write.
- Produces:
  ```go
  func ApplyReviewChangesRequested(ctx context.Context, c client.Client,
      sp objbudget.Spiller, proj *v1alpha1.Project, task *v1alpha1.Task,
      now time.Time) (reentered bool, err error)
  func ApplyReviewApproval(ctx context.Context, c client.Client,
      sp objbudget.Spiller, proj *v1alpha1.Project, task *v1alpha1.Task,
      reviewCommitSHA string, now time.Time) (advanced bool, err error)
  ```

#### Step 4c.1 - failing tests

Create `internal/controller/review_apply_test.go`:

```go
// changes_requested on a non-terminal implementing-produced Task re-enters
// implementing, when the owned MR is NOT merged.
func TestApplyReviewChangesRequested_ReentersImplementing(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t1", "clarify") // Kind=clarify, Stage=reviewing, StageEnteredAt set
	mr := ownedMR("mr-tatara-operator-42", "t1", "tatara-operator", 42) // State=open, PendingReview=nil
	c := newControllerClient(t, proj, task, mr)
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, nil, proj, task, time.Now())
	require.NoError(t, err)
	require.True(t, reentered)

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t1"}, &got))
	require.Equal(t, tatarav1alpha1.StageImplementing, got.Status.Stage)
}

// changes_requested on a Task whose owned MR is MERGED does NOT rewind.
func TestApplyReviewChangesRequested_MergedMR_NoRewind(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t2", "clarify")
	mr := ownedMR("mr-tatara-operator-42", "t2", "tatara-operator", 42)
	mr.Status.State = "merged"
	c := newControllerClient(t, proj, task, mr)
	reentered, err := ApplyReviewChangesRequested(context.Background(), c, nil, proj, task, time.Now())
	require.NoError(t, err)
	require.False(t, reentered, "an already-merged MR is finished; no rewind")

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t2"}, &got))
	require.Equal(t, tatarav1alpha1.StageReviewing, got.Status.Stage)
}

// approved on a reviewing non-review Task clears PendingReview and enters merging.
func TestApplyReviewApproval_EntersMerging(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t3", "clarify")
	mr := ownedMR("mr-tatara-operator-42", "t3", "tatara-operator", 42)
	mr.Status.PendingReview = &tatarav1alpha1.PendingReview{Round: 1} // bot review still owed
	c := newControllerClient(t, proj, task, mr)
	advanced, err := ApplyReviewApproval(context.Background(), c, nil, proj, task, "deadbeef", time.Now())
	require.NoError(t, err)
	require.True(t, advanced)

	var gotMR tatarav1alpha1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "mr-tatara-operator-42"}, &gotMR))
	require.Nil(t, gotMR.Status.PendingReview, "maintainer approval short-circuits the pending bot review")
	require.Equal(t, "approved", gotMR.Status.Status)
	require.Equal(t, "deadbeef", gotMR.Status.ReviewedSHA)

	var got tatarav1alpha1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "t3"}, &got))
	require.Equal(t, tatarav1alpha1.StageMerging, got.Status.Stage)
}

// approved on a kind=review Task never merges.
func TestApplyReviewApproval_ReviewKind_NoMerge(t *testing.T) {
	proj := sweepProject("p")
	task := reviewingTask("t4", "review")
	mr := ownedMR("mr-tatara-operator-42", "t4", "tatara-operator", 42)
	c := newControllerClient(t, proj, task, mr)
	advanced, err := ApplyReviewApproval(context.Background(), c, nil, proj, task, "sha", time.Now())
	require.NoError(t, err)
	require.False(t, advanced)
}
```

(`reviewingTask`/`ownedMR` are small local builders; `ownedMR` sets the
controller ownerRef to the Task name via `own.HandOverController` on a
plain-owned CR, mirroring the sweep test's ownership setup.)

#### Step 4c.2 - run, expect fail (undefined appliers).

#### Step 4c.3 - minimal impl

Create `internal/controller/review_apply.go`. The status write mirrors the
existing `stampMintStatus`/`RetryOnConflict` idiom; the stage transition goes
through `stage.Enter` then a `Status().Update`. Reuse `ownedMergeRequests`
(same package).

```go
package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ApplyReviewChangesRequested re-enters implementing when a maintainer requests
// changes on a Tatara-owned MR that is NOT yet merged. An already-merged MR is
// finished (no rewind); a kind=review or terminal Task is not driven (both are
// refused by ReenterImplementingOnReview / the merged check). It is the mirror
// of the review pod's request_changes verdict, but sourced from a human review.
func ApplyReviewChangesRequested(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, now time.Time) (bool, error) {

	mrs, err := ownedMergeRequests(ctx, c, task)
	if err != nil {
		return false, err
	}
	// The merged/finished boundary is the MergeRequest CR's merged state, NOT the
	// Task stage: any owned merged MR means the change shipped and must not rewind.
	for i := range mrs {
		if mrs[i].Status.State == "merged" || mrs[i].Status.MergedAt != nil {
			return false, nil
		}
	}
	key := client.ObjectKeyFromObject(task)
	reentered := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		reentered = false
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !stage.ReenterImplementingOnReview(fresh, mrs, now) {
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		reentered = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("review: re-enter implementing on %s: %w", task.Name, err)
	}
	if reentered {
		log.FromContext(ctx).Info("review: maintainer requested changes; re-entering implementing",
			"action", "review_reenter_implementing", "resource_id", task.Name)
	}
	return reentered, nil
}

// ApplyReviewApproval applies the reviewing -> merging edge on a maintainer's
// approval. A maintainer approval is authoritative and short-circuits any
// pending bot review: it clears PendingReview and stamps approved + reviewedSHA
// on every owned MR, opening reviewGateOpen so the edge is legal. The actual
// merge still waits on CI-green + mergeability in ReconcileMerging.
func ApplyReviewApproval(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, reviewCommitSHA string, now time.Time) (bool, error) {

	if task.Spec.Kind == "review" {
		return false, nil // a kind=review Task never merges (LegalFor guard 1)
	}
	if task.Status.Stage != tatarav1alpha1.StageReviewing {
		return false, nil // approval arrived off reviewing; fold to the comment path
	}
	mrs, err := ownedMergeRequests(ctx, c, task)
	if err != nil {
		return false, err
	}
	if len(mrs) == 0 {
		return false, nil
	}
	for i := range mrs {
		mrKey := client.ObjectKeyFromObject(&mrs[i])
		thisSHA := reviewCommitSHA
		if err := objbudget.FitMergeRequest(ctx, c, sp, mrKey, func(m *tatarav1alpha1.MergeRequest) {
			m.Status.PendingReview = nil
			m.Status.Status = "approved"
			if thisSHA != "" {
				m.Status.ReviewedSHA = thisSHA
			}
		}); err != nil {
			return false, fmt.Errorf("review: settle mr %s: %w", mrKey.Name, err)
		}
	}
	fresh, err := ownedMergeRequests(ctx, c, task) // reload so reviewGateOpen sees the cleared copies
	if err != nil {
		return false, err
	}
	key := client.ObjectKeyFromObject(task)
	advanced := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		advanced = false
		t := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, t); err != nil {
			return err
		}
		if t.Status.Stage != tatarav1alpha1.StageReviewing {
			return nil
		}
		if err := stage.Enter(t, fresh, tatarav1alpha1.StageMerging, "", now); err != nil {
			return nil // guard refused (e.g. gate still closed); leave untouched
		}
		if err := c.Status().Update(ctx, t); err != nil {
			return err
		}
		*task = *t
		advanced = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("review: enter merging on %s: %w", task.Name, err)
	}
	if advanced {
		log.FromContext(ctx).Info("review: maintainer approved; entering merging",
			"action", "review_enter_merging", "resource_id", task.Name)
	}
	return advanced, nil
}
```

#### Step 4c.4 - run, expect pass; then commit

`feat: controller appliers for maintainer changes_requested / approved reviews`

### Task 4d - webhook routing + dedup

**Files**
- Modify `internal/webhook/server.go` (`handleForgeItem`: route `IsReview`;
  new `handleReview`, `reviewAlreadyProcessed`, `stampReviewProcessed`).
- Create `internal/webhook/review_route_test.go`.

**Interfaces**
- Consumes: `tatarav1.IsMaintainer`, `own.ControllerOwner`,
  `controller.ApplyReviewChangesRequested`, `controller.ApplyReviewApproval`,
  `s.deliverPendingEvent`.
- Produces: `func (s *Server) handleReview(ctx, w, provider, proj, ev)`.

#### Step 4d.1 - failing tests

Create `internal/webhook/review_route_test.go` (package `webhook_test`). Seed a
Project with a maintainer, a Repository, a review Task's owning Task + owned MR
CR. Helpers: `postReview(t, h, proj, secret, body)` sets
`X-GitHub-Event: pull_request_review`; `reviewBody(action, state, id, reviewer,
number)` renders the payload.

```go
// A maintainer's changes_requested on a Tatara-owned unmerged MR re-enters
// implementing.
func TestReview_ChangesRequested_ReentersImplementing(t *testing.T) { /* ... */ }

// A non-maintainer review is ignored.
func TestReview_NonMaintainer_Ignored(t *testing.T) { /* ... */ }

// A maintainer approval enters merging.
func TestReview_Approved_EntersMerging(t *testing.T) { /* ... */ }

// changes_requested on an adopted human PR (owning Task Kind=review) does NOT
// drive implementing.
func TestReview_ChangesRequested_ReviewKind_NotDriven(t *testing.T) { /* ... */ }

// The SAME (review.id, state) delivered twice fires the transition once.
func TestReview_DedupOnReviewIDState(t *testing.T) { /* ... */ }

// dismissed / edited actions are ignored (Action != submitted).
func TestReview_Dismissed_Ignored(t *testing.T) { /* ... */ }
```

Each asserts the owning Task's resulting `Status.Stage` (and, for dedup, that a
second delivery does not re-transition, e.g. by pre-setting the Task past
merging and asserting it is untouched, or by counting).

#### Step 4d.2 - run, expect fail (no review routing).

#### Step 4d.3 - minimal impl

`handleForgeItem` - route reviews first (before the comment/opened branches):

```go
	if ev.IsReview {
		s.handleReview(ctx, w, provider, proj, ev)
		return
	}
```

`handleReview`:

```go
func (s *Server) handleReview(ctx context.Context, w http.ResponseWriter, provider string,
	proj tatarav1.Project, ev scm.WebhookEvent) {
	// Only a submitted review acts; dismissed/edited collapse to Action "other".
	if ev.Action != "submitted" || ev.ReviewState == "" {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if isBotActor(&proj, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if !tatarav1.IsMaintainer(&proj, repo, ev.ActorLogin) {
		s.log.InfoContext(ctx, "review: actor is not a verified maintainer; ignoring",
			"project", proj.Name, "repo", repo.Name, "actor", ev.ActorLogin)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	mr := &tatarav1.MergeRequest{}
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, tatarav1.MergeRequestName(repo.Name, ev.Number)), mr); err != nil {
		// No mirror yet -> not a Tatara-owned MR the operator drives. Fold to the
		// comment path so nothing is lost, and let the sweep adopt.
		s.deliverPendingEvent(ctx, proj, repo, ev)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	ownerName, owned := own.ControllerOwner(mr)
	if !owned {
		s.deliverPendingEvent(ctx, proj, repo, ev)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	task := &tatarav1.Task{}
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, ownerName), task); err != nil {
		if apierrors.IsNotFound(err) {
			s.accept(w, provider, ev.Kind, ev.Action, "ignored")
			return
		}
		s.reject(w, http.StatusInternalServerError, "get owning task", provider, ev.Kind, ev.Action, "error")
		return
	}
	if reviewAlreadyProcessed(task, ev.ReviewID, ev.ReviewState) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored") // (review.id, state) dedup
		return
	}
	if tatarav1.TaskDone(task) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored") // terminal Task not resurrected
		return
	}

	sp := s.cfg.SpillerFor(&proj)
	switch ev.ReviewState {
	case "changes_requested":
		// Adopted human PRs (owning Task Kind=review) are only reviewed, never
		// driven to implementing; ApplyReviewChangesRequested refuses kind=review,
		// but folding to the comment path keeps the signal.
		reentered, aerr := controller.ApplyReviewChangesRequested(ctx, s.cfg.Client, sp, &proj, task, time.Now())
		if aerr != nil {
			s.reject(w, http.StatusInternalServerError, "apply changes_requested", provider, ev.Kind, ev.Action, "error")
			return
		}
		if !reentered {
			s.deliverPendingEvent(ctx, proj, repo, ev) // merged/terminal/kind=review: fold, don't lose
		}
	case "approved":
		advanced, aerr := controller.ApplyReviewApproval(ctx, s.cfg.Client, sp, &proj, task, ev.ReviewCommitSHA, time.Now())
		if aerr != nil {
			s.reject(w, http.StatusInternalServerError, "apply approval", provider, ev.Kind, ev.Action, "error")
			return
		}
		if !advanced {
			s.deliverPendingEvent(ctx, proj, repo, ev)
		}
	case "commented":
		s.deliverPendingEvent(ctx, proj, repo, ev)
	default: // dismissed and anything else
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if err := s.stampReviewProcessed(ctx, task, ev.ReviewID, ev.ReviewState); err != nil {
		s.log.ErrorContext(ctx, "review: stamp dedup marker failed", "error", err, "task", task.Name)
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

func reviewKey(reviewID string) string { return "tatara.dev/reviewed-" + reviewID }

func reviewAlreadyProcessed(task *tatarav1.Task, reviewID, state string) bool {
	if reviewID == "" {
		return false
	}
	return task.Annotations[reviewKey(reviewID)] == state
}

func (s *Server) stampReviewProcessed(ctx context.Context, task *tatarav1.Task, reviewID, state string) error {
	if reviewID == "" {
		return nil
	}
	key := client.ObjectKeyFromObject(task)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[reviewKey(reviewID)] = state
		return s.cfg.Client.Update(ctx, fresh)
	})
}
```

Add imports to `server.go` if missing: `"k8s.io/client-go/util/retry"`,
`"github.com/szymonrychu/tatara-operator/internal/own"` (present via Task 3).

#### Step 4d.4 - run, expect pass

```
mise exec -- go test ./internal/webhook/... ./internal/controller/... ./internal/stage/... ./internal/scm/... -race
```

#### Step 4d.5 - commit

`feat: route human pull_request_review to changes_requested/approved/commented`

---

## Task 5 - Dead-cron cleanup (`cdScan`, `healthCheck`)

Remove the two confirmed-dead cron mechanisms. **Do NOT touch the `healthCheck`
MODEL/ceiling pseudo-key** in `internal/agent/pod.go` and the
`modelByKind`/`effortByKind`/`spawnCeilingByKind` enums - that is a distinct,
LIVE activity-label concept (see the divergence note). Only the cron plumbing
goes.

**Files**
- Modify `api/v1alpha1/project_types.go`: delete `CDScanActivity` (~351-367),
  `HealthCheckActivity` (~322-341), `ScmCron.CDScan` (~373-387),
  `ScmCron.HealthCheck` (~390-402), `ProjectStatus.LastCDScan` (~845-847),
  `ProjectStatus.LastHealthCheck` (~836-840).
- Modify `internal/controller/projectscan.go`: delete the `cdScan` and
  `healthCheck` arms in `activityScheduleAndLast` (~45-46, 51-55) and the
  `cdScan` arm in `stampScan` (~464-466); update the stale comment block
  (~1243-1245).
- Modify `api/v1alpha1/zz_generated.deepcopy.go` (regenerated).
- Modify `charts/tatara-operator/crd-bases/tatara.dev_projects.yaml`
  (regenerated).
- Update `ROADMAP.md` (remove the "Delete the retired healthCheck cron surface"
  item at ~189-190; note the `cdScan` line at ~41 if it references removed
  fields) and add a dated `MEMORY.md` line.

**Interfaces**
- Removes: `v1alpha1.CDScanActivity`, `v1alpha1.HealthCheckActivity`,
  `ScmCron.CDScan`, `ScmCron.HealthCheck`, `ProjectStatus.LastCDScan`,
  `ProjectStatus.LastHealthCheck`.

### Step 5.1 - failing test (guard against dangling refs)

Add a compile-time-ish guard test in `api/v1alpha1` that references only the
surviving cron surface, plus a repo grep in the commit step. The real gate is
that the package still compiles after removal:

```go
// TestScmCron_NoDeadActivities documents the removed cron surface: the struct
// must expose only the live activities.
func TestScmCron_NoDeadActivities(t *testing.T) {
	var c v1alpha1.ScmCron
	_ = c.IssueScan
	_ = c.Brainstorm
	_ = c.Documentation
	_ = c.Refine
	// c.CDScan and c.HealthCheck are intentionally gone.
}
```

### Step 5.2 - run, expect fail (the test references still compile because the
fields exist). Instead, drive Task 5 by the build gate: after the deletions the
package must compile and no reference may remain.

### Step 5.3 - minimal impl

Delete the structs/fields/arms listed in Files. In `activityScheduleAndLast`,
remove:

```go
	case "cdScan":
		return c.CDScan.Schedule, proj.Status.LastCDScan
```
and
```go
	case "healthCheck":
		return c.HealthCheck.Schedule, proj.Status.LastHealthCheck
```

In `stampScan`, remove:

```go
	case "cdScan":
		fresh.Status.LastCDScan = &now
		proj.Status.LastCDScan = &now
```

Then regenerate:

```
mise run generate 2>/dev/null || make generate
make manifests
```

Grep to confirm nothing dangles (excluding the live model pseudo-key):

```
grep -rn "CDScanActivity\|LastCDScan\|HealthCheckActivity\|LastHealthCheck\|\.CDScan\b\|\.HealthCheck\b" \
  --include='*.go' --include='*.yaml' . | grep -viE "GRPC HealthCheckRequest"
```

Fix any straggler references (tests that constructed `ScmCron{HealthCheck: ...}`
or `CDScan: ...`, sample CRs, chart values). Search
`internal/controller/projectscan_*_test.go` and any `config/samples` for
`cdScan`/`healthCheck` cron blocks and remove them (the model-tier tests in
`internal/agent/*_test.go` reference the `healthCheck` ACTIVITY LABEL, not the
cron - leave those).

### Step 5.4 - run, expect pass

```
make generate manifests test lint build
```

### Step 5.5 - commit

`chore: remove dead cdScan and healthCheck cron plumbing`

---

## Final verification gate

Run the repo hard-rule build gate end to end and confirm no dangling refs:

```
make generate manifests test lint build
```

Then update `MEMORY.md` (dated lines):
- Webhook is now the PRIMARY minter; the B.4 sweep is an idempotent backstop.
  Natural-key idempotency is realized on the DIRECT Task create (deterministic
  `IntakeTaskName` + live APIReader check + `IgnoreAlreadyExists` +
  stale-terminal delete), NOT by rerouting the sweep through `EnqueueEvent` -
  because a `parked(backlog-sweep)` Task must own its Issue CR at mint time and
  QueuedEvents are ephemeral. `EnqueueEvent` also hardened (deterministic QE
  name) for the incident/ticket path.
- Human `pull_request_review` wired: `changes_requested` -> re-enter
  `implementing` (only while the owned MR is not merged; new
  `merging -> implementing` edge + `stage.ReenterImplementingOnReview`);
  `approved` -> `reviewing -> merging` (clears/short-circuits any pending bot
  review, stamps `reviewedSHA`); `commented` -> pending event; dedup on
  `(review.id, state)` via a per-Task annotation. Gated on `IsMaintainer`.
- `cdScan` / `healthCheck` cron surface deleted; the `healthCheck`
  model/ceiling pseudo-key is a SEPARATE live concept and was left intact.

And `ROADMAP.md`: remove the completed "Delete the retired healthCheck cron
surface" item.

## Self-review (writing-plans)

- **Spec coverage:** components 1 (shared funnel, Task 2), 2 (race-safe dedup,
  Tasks 1a/1b/2), 3 (webhook primary minter, Task 3), 4 (pull_request_review,
  Task 4a-d), 5 (dead-cron, Task 5) all present. Edge cases covered by tests:
  concurrent double-mint (1b, 2), sweep-after-webhook no-op (3), bot/reporter
  gates (3), non-maintainer ignored (4d), changes_requested on adopted PR /
  merged MR / terminal Task (4c/4d), review dedup (4d), dismissed ignored (4d).
- **Placeholder scan:** every code step carries real Go grounded in the read
  sources (real symbols: `IsOrphanIssue`, `ClassifyPR`, `MintStage`,
  `MintReviewStage`, `own.ControllerOwner`, `stage.Enter`/`LegalFor`/
  `reviewGateOpen`, `MergeRequestName`, `IsMaintainer`, `objbudget.FitMergeRequest`,
  `ownedMergeRequests`, `client.IgnoreAlreadyExists`). The only prose stubs are
  the Task-4d test BODIES (`/* ... */`), deliberately, because they assemble
  from the same helpers the neighbouring tests spell out in full.
- **Type consistency:** `MintForItem` returns `(*Task, bool, error)`
  everywhere; `ApplyReview*` return `(bool, error)`; the merged boundary is
  `mr.Status.State == "merged" || mr.Status.MergedAt != nil` (matches
  `stage.anyMerged` and `merge.go`); `MergeRequestName(repoRef, number)` and
  `IssueName(repoRef, number)` used as declared; `WebhookEvent` review fields
  are additive.
- **Divergences flagged:** the intake-mechanism divergence (direct Task mint vs.
  spec's QueuedEvent routing) and the review-stage-scope divergence (narrowed to
  reviewing/merging/parked) are called out in the header for the main thread to
  reconcile.
