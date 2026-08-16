# Queued, event-driven adoption of dependency upgrade merge requests - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn an adoptable dependency merge request's webhook delivery into a durable `QueuedEvent` that the dispatcher admits the instant a pool slot frees, replacing the one-shot `sweep-requested` annotation and the per-pass `maxOpenUpgrades` adoption headroom.

**Architecture:** The webhook enqueues a priority-2 `QueuedEvent` carrying a typed PR snapshot (`payload.adoptedUpgrade`); the dispatcher's existing mint branch routes that payload to `Minter.MintAdoptedUpgradeTask` instead of the generic `BuildTaskFromQueuedEvent`; the sweep's `PRAdoptUpgrade` arm enqueues under the same dedup key and loses its headroom accounting entirely. `synchronize` refreshes a still-`Queued` event and `closed`/`merged` deletes one, so a snapshot never goes stale behind a full pool. `maxOpenUpgrades` survives as the upgrade CRON's own knob, with `openUpgradeLaneCount` taught to ignore adopted work.

**Tech Stack:** Go 1.26.3, controller-runtime, kubebuilder CRD codegen (`controller-gen` v0.18.0), Prometheus client_golang, envtest 1.33.0. All tools via `mise exec --`.

## Global Constraints

- **Go**: 1.26.3, pinned in `.mise.toml` and `go.mod`. Never a bare `go`; always `mise exec -- go ...` or `mise run {lint,test,build}`.
- **KISS, no tech debt.** Three similar lines beat a premature abstraction. Any complexity that must exist gets a dated `MEMORY.md` line.
- **Boy-scout adjacent easy fixes** without asking. Dead code left behind by this change is deleted in the same change, not deferred.
- **JSON logs** via the controller-runtime logger already in each file; every business action at INFO with structured fields (`action`, `resource_id`, `project`, `repo`, `number`).
- **Metrics for everything that counts or can fail.** Every new counter is SEEDED in `obs.SeedSweepErrorsForProject` so `increase()` is not blind to its first increment.
- **Writing rules:** no em dashes, no smart quotes, no arrows, no decorative Unicode. Plain hyphens and straight quotes. This applies to code comments and commit messages.
- **Priority for queued adoptions is exactly 2** (`queue.WithPriority(2)`), per design decision D3. Not 1.
- **Adopted upgrade Tasks are bounded by `Project.QueueCapacity()` and `MaxLivePods` only** (D1). No per-kind admission gate is introduced anywhere.
- **Backward compatibility:** a `QueuedEvent` already Queued when the new operator starts has `payload.adoptedUpgrade == nil` and MUST keep taking the unchanged `BuildTaskFromQueuedEvent` path. An adopted upgrade Task minted by the OLD code carries no `tatara.dev/upgrade-origin` label and MUST still be excluded from the cron's lane count (Task 9 fallback).

---

## Findings: where the real code contradicts the spec

Read this section before Task 1. Each item changed the plan below.

**(a) Payload carrier - the spec is RIGHT, and here is the proof it needed.**
`QueuedTaskBlueprint` (`api/v1alpha1/queuedevent_types.go:92-110`) carries only Name/Kind/Goal/ProjectRef/RepositoryRef/IssueKeys/AlertRules/Labels/Annotations. The flat legacy payload adds `Source *TaskSource`, whose fields (`task_types.go:20-39`) are Provider/IssueRef/URL/AuthorLogin/IsPR/Number/HeadSHA/Title. Neither shape can carry `HeadBranch`, `Body`, `Labels[]`, `Repo` or `HeadRepo`, and `MintAdoptedUpgradeTask` needs all five (`AnnTakeoverHeadBranch` from HeadBranch, `mrSnapshot` from Body/Title, `AdoptUpgradeMR`'s fork guard from Repo/HeadRepo). **Decision: add the typed `AdoptedUpgrade *AdoptedUpgradeRef` field as specified.**

**(b) Import cycle - there is none, because there is no import.**
`DispatcherReconciler` (`internal/controller/queue_controller.go:36`) and `Minter` (`internal/controller/intake.go:31`) are in the SAME package, `controller`. The dispatcher can call `(&Minter{...}).MintAdoptedUpgradeTask` with a plain struct literal. What it genuinely lacks is a `SpillerFor` field: `MintAdoptedUpgradeTask` -> `bindMRToTask` needs an `objbudget.Spiller`. `cmd/manager/wire.go` already has the `spillerFor` closure in scope at line 533 where the dispatcher is constructed (it is defined at line 390, same `addReconcilers` function). **Decision: add `SpillerFor func(*Project) objbudget.Spiller` to `DispatcherReconciler` and wire it in Task 5.**

**(c) Label/annotation constants - one exists, one must be added, and the prefix is inconsistent.**
- EXISTS: `queue.LabelQueuedEvent`, `queue.LabelMintedBy`, `queue.LabelDedupKey` (`internal/queue/enqueue.go:20-38`); `tatarav1alpha1.AnnTakeoverHeadBranch`; `tatarav1alpha1.LabelActivity = "tatara.io/activity"`.
- MUST BE ADDED: `tatara.dev/upgrade-origin`. Nothing in the tree declares it.
- CONTRADICTION, minor: its nearest neighbours `LabelSourceKind`/`LabelActivity` use the `tatara.io/` prefix, while every annotation uses `tatara.dev/`. The spec says `tatara.dev/upgrade-origin`. **Decision: follow the spec (`tatara.dev/`), and say so in the const's doc comment so the next reader does not "fix" it to match `LabelActivity`.**

**(d) Removing `upgrade_headroom_bound` breaks FOUR test sites and ONE chart file - and the chart file is in THIS repo, not `tatara-observability`.**
The spec says "any alert or dashboard referencing it in `tatara-observability` must be checked". The live reference is `charts/tatara-operator/templates/prometheusrule.yaml:218` (`TataraSweepSkipPersistent` excludes `reason!="upgrade_headroom_bound"`) plus a long prose paragraph in the alert's `description`. Test sites that break:
- `internal/obs/sweep_metrics_test.go:104` - `const wantPerProject = 2 * 9` becomes `2 * 8`.
- `internal/obs/sweep_metrics_test.go:169` `TestSweepSkipReasonsMatchSweepConstants` - fails BOTH ways, so the constant and the seed-list member must go together.
- `internal/obs/sweep_skip_alert_test.go:18,34` - `upgradeHeadroomSkipReason` and `steadyStateSkipReasons` must lose the member, or the test demands a chart exclusion for a reason no producer emits.
- `internal/controller/sweep_adopt_upgrade_test.go:300-314` and the whole of `internal/controller/sweep_adopt_headroom_test.go` - both test the adoption window that is being deleted.
**Decision: Task 8 removes the constant, the seed-list member, the chart exclusion + its description prose, and both obs tests' references, and deletes `sweep_adopt_headroom_test.go` outright.**
A SECOND dead series comes with it: `fail("count_upgrade_lanes", ...)` (`sweep.go:1395`) is the only producer of the `count_upgrade_lanes` seeded reason, and `TestSweepSeedReasonsCoverEveryFailSite` fails both ways too, so `sweepSeedReasons` must lose it in the same commit.

**(e) `handleMRSynchronize` / `handleMRClosed` have everything they need.**
Both take `proj tatarav1.Project` by value and call `s.matchRepo(ctx, proj.Name, ev.Repo)` on their first lines (`internal/webhook/mirror_refresh.go:232` and `:302`). With `proj.Name` + `repo.Name` + `ev.Number` the deterministic event name is `queue.QueuedEventName(proj.Name, queue.AdoptUpgradeDedupKey(repo.Name, ev.Number))` - a single Get, no listing. `s.cfg.Seq` and `s.cfg.Client` are both already present.

**(f) NEW CONTRADICTION - the spec's error-handling claim "the `AdoptUpgradeMR` predicates still apply at mint time" is FALSE.**
`MintAdoptedUpgradeTask` (`internal/controller/upgrade_adopt.go:121`) never calls `AdoptUpgradeMR`. Today the predicate runs in `ClassifyPR` inside `sweepPRs`, one stack frame up. If the dispatcher calls the mint directly, NO predicate runs at admit time at all. **Decision: Task 5 re-runs `AdoptUpgradeMR` at admit, against the snapshot plus a freshly-read mirror and live owner. A refusal deletes the event.**

**(g) NEW CONTRADICTION, and it is a safety hole - `scm.WebhookEvent` carries no head repo, so the fork guard cannot be evaluated from a webhook.**
`AdoptUpgradeMR` clause (d) is `pr.HeadRepo == "" || pr.HeadRepo != pr.Repo -> refuse`, and it FAILS CLOSED on empty by design. `scm.WebhookEvent` (`internal/scm/scm.go:17-52`) has `HeadBranch` but no `HeadRepo`, and neither `ghWorkItemEvent` (`github.go:235`) nor `glWorkItemEvent` (`gitlab.go:161`) decodes one - even though both raw payloads carry it (`pull_request.head.repo.full_name`, `object_attributes.source.path_with_namespace`). Without it, either the fork guard is bypassed for every webhook-originated adoption, or (f)'s re-check refuses every one of them and the feature does nothing. `MEMORY.md` 2026-08-16 records that `scm.WebhookEvent.HeadRepo` and both parsers existed in the first attempt at this fix and were deleted with it. **Decision: Task 1 re-adds them.**

**(h) `requestRepoSweep` has exactly ONE caller, so the spec's "it stays for its other callers if any" resolves to "there are none".**
`grep` over non-test Go: the only call site is `server.go:803`. Once replaced, `requestRepoSweep` is dead, and with it the only writer of `tatarav1alpha1.SweepRequestedAnnotation` - whose only reader is the forward half of `reposDueForScan` (`projectscan.go:463-490`). A marker no producer stamps is dead code. **Decision: Task 11 deletes the writer, the reader, the constant and their tests. It is LAST so a reviewer can drop it without unpicking anything else.**

**(i) `make manifests` writes ONE copy of the CRD, not two.**
There is no `config/crd/bases` directory in this repo. `Makefile:19` sets `CHART_CRD_DIR := charts/tatara-operator/crd-bases` and `manifests` (`Makefile:37`) points `controller-gen`'s `output:crd:artifacts:config` straight at it. **Decision: `make generate manifests` regenerates `zz_generated.deepcopy.go` plus `charts/tatara-operator/crd-bases/tatara.dev_queuedevents.yaml`, and that is the complete generated surface. Do not go looking for a second copy.**

**(j) `MEMORY.md` currently records the OPPOSITE decision.**
The 2026-08-16 entry says "**QueuedEvent + dispatcher was considered and rejected**", with the reason "an adoption event is a POINTER to a live forge object, so queueing it behind a cap that may hold for hours means minting a review pod for a merge request Renovate has since force-pushed, superseded or closed." D4 is exactly the answer to that objection. **Decision: Task 11 appends a dated entry recording the reversal and the mechanism that made it safe. Leaving the two entries side by side with no reconciliation is the "implementer reads ONE section" failure `CLAUDE.md` calls out.**

**(k) The dedup key must ALSO land on the minted Task, or a redelivery re-enqueues.**
`queue.dedupExists` checks live `QueuedEvent`s AND live `Task`s carrying `LabelDedupKey`. `MintAdoptedUpgradeTask` builds its Task by hand and stamps no such label, so once the event is GC'd a redelivered `pull_request.opened` would enqueue a second event for the same merge request. **Decision: Task 4 takes a label stamp from the caller (Task 3's `queue.MintStamp`) so the adopted Task carries `LabelQueuedEvent` + `LabelMintedBy` + `LabelDedupKey` exactly like a `BuildTaskFromQueuedEvent` mint.**

---

## File structure

| File | Change |
|---|---|
| `internal/scm/scm.go` | +`WebhookEvent.HeadRepo` |
| `internal/scm/github.go` | decode `pull_request.head.repo.full_name` |
| `internal/scm/gitlab.go` | decode `object_attributes.source.path_with_namespace` |
| `api/v1alpha1/queuedevent_types.go` | +`AdoptedUpgradeRef`, +`Payload.AdoptedUpgrade`, +validation rule |
| `api/v1alpha1/annotations.go` | +`LabelUpgradeOrigin`, +`UpgradeOriginAdopted` |
| `api/v1alpha1/zz_generated.deepcopy.go` | GENERATED |
| `charts/tatara-operator/crd-bases/tatara.dev_queuedevents.yaml` | GENERATED |
| `internal/queue/enqueue.go` | +`AdoptUpgradeDedupKey`, +`MintStamp`, +`IsAdoptedUpgradeMint`; `BuildTaskFromQueuedEvent` uses `MintStamp` |
| `internal/controller/upgrade_adopt.go` | `MintAdoptedUpgradeTask` takes a label stamp; stamps upgrade-origin; +`AdoptedUpgradeRefFromPR`, +`prRefFromAdopted` |
| `internal/controller/queue_controller.go` | +`SpillerFor` field; mint branch routes the adopted payload; +`admitAdoptedUpgrade` |
| `cmd/manager/wire.go` | wire `SpillerFor` into `DispatcherReconciler` |
| `internal/webhook/server.go` | `handleMROpened` enqueues; delete `requestRepoSweep` (Task 11) |
| `internal/webhook/mirror_refresh.go` | `handleMRSynchronize` refreshes; `handleMRClosed` deletes |
| `internal/controller/sweep.go` | delete headroom + `SweepSkipUpgradeHeadroom`; `PRAdoptUpgrade` arm enqueues |
| `internal/controller/projectscan.go` | `openUpgradeLaneCount` excludes adopted |
| `internal/obs/sweep_metrics.go` | seed-list edits; +2 counters |
| `charts/tatara-operator/templates/prometheusrule.yaml` | drop the retired exclusion + its prose |
| `MEMORY.md`, `ROADMAP.md` | reversal record |

---

## Task 1: Head repo on the webhook event

**Why first:** the fork guard (`AdoptUpgradeMR` clause d) fails CLOSED on an empty `HeadRepo`, so every later task that re-checks the predicate against a webhook-built snapshot depends on this field existing. See finding (g).

**Files:**
- Modify: `internal/scm/scm.go:17-52` (add `HeadRepo` to `WebhookEvent`)
- Modify: `internal/scm/github.go:33-50` (`ghWorkItem.Head`), `internal/scm/github.go:235-261` (`ghWorkItemEvent`)
- Modify: `internal/scm/gitlab.go` (`glPayload.ObjectAttributes`, `glWorkItemEvent` at `:161-189`)
- Test: `internal/scm/webhook_headrepo_test.go` (new)

**Interfaces:**
- Produces: `scm.WebhookEvent.HeadRepo string` - the head branch's repository in the SAME namespace as `WebhookEvent.Repo`'s slug: a GitHub `owner/name` full_name, a GitLab `group/project` path_with_namespace. EMPTY when the forge did not report it, and every consumer fails closed on empty.

- [ ] **Step 1: Write the failing tests**

Create `internal/scm/webhook_headrepo_test.go`. Both parsers are exercised through the same public entry points the existing `github_test.go` / `gitlab_test.go` use - read those two files first and copy their `ParseWebhook` call shape verbatim rather than inventing one.

```go
package scm

import "testing"

// THE FORK GUARD NEEDS A HEAD REPO, AND THE WEBHOOK IS WHERE IT COMES FROM.
// AdoptUpgradeMR clause (d) refuses any merge request whose head repo is not
// the base repo, and it fails CLOSED on an empty value - so an adoption event
// built from a delivery that never decoded head.repo can never be admitted.
func TestParseWebhook_GitHubPROpenedCarriesHeadRepo(t *testing.T) {
	body := []byte(`{"action":"opened","pull_request":{"number":7,` +
		`"title":"chore(deps): bump","user":{"login":"tatara-bot"},` +
		`"head":{"sha":"abc","ref":"renovate/dep","repo":{"full_name":"o/r"}},` +
		`"html_url":"https://github.com/o/r/pull/7"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"tatara-bot"}}`)
	ev := parseGitHubForTest(t, body) // see github_test.go for the real helper name
	if ev.HeadRepo != "o/r" {
		t.Fatalf("HeadRepo = %q, want %q", ev.HeadRepo, "o/r")
	}
}

// A FORK PR REPORTS A DIFFERENT HEAD REPO, and that difference is the entire
// signal: a fork may name its head branch renovate/anything.
func TestParseWebhook_GitHubForkPRReportsTheForkAsHeadRepo(t *testing.T) {
	body := []byte(`{"action":"opened","pull_request":{"number":8,` +
		`"title":"drive-by","user":{"login":"stranger"},` +
		`"head":{"sha":"def","ref":"renovate/evil","repo":{"full_name":"stranger/r"}},` +
		`"html_url":"https://github.com/o/r/pull/8"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"stranger"}}`)
	ev := parseGitHubForTest(t, body)
	if ev.HeadRepo != "stranger/r" {
		t.Fatalf("HeadRepo = %q, want %q", ev.HeadRepo, "stranger/r")
	}
}

// A PAYLOAD WITH NO head.repo LEAVES IT EMPTY, and empty is what every
// consumer fails closed on. It is never defaulted to the base repo: that would
// turn "the forge did not say" into "it is not a fork".
func TestParseWebhook_GitHubPRWithoutHeadRepoLeavesItEmpty(t *testing.T) {
	body := []byte(`{"action":"opened","pull_request":{"number":9,` +
		`"title":"t","user":{"login":"u"},"head":{"sha":"ghi","ref":"b"},` +
		`"html_url":"https://github.com/o/r/pull/9"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"u"}}`)
	if ev := parseGitHubForTest(t, body); ev.HeadRepo != "" {
		t.Fatalf("HeadRepo = %q, want empty", ev.HeadRepo)
	}
}

// GitLab reports the same fact as object_attributes.source.path_with_namespace.
func TestParseWebhook_GitLabMROpenedCarriesHeadRepo(t *testing.T) {
	body := []byte(`{"object_kind":"merge_request",` +
		`"user":{"username":"tatara-bot"},` +
		`"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},` +
		`"object_attributes":{"iid":11,"title":"chore(deps): bump","action":"open",` +
		`"source_branch":"renovate/dep","last_commit":{"id":"abc"},` +
		`"source":{"path_with_namespace":"g/p"},` +
		`"url":"https://gitlab.com/g/p/-/merge_requests/11"}}`)
	ev := parseGitLabForTest(t, body) // see gitlab_test.go for the real helper name
	if ev.HeadRepo != "g/p" {
		t.Fatalf("HeadRepo = %q, want %q", ev.HeadRepo, "g/p")
	}
}
```

Replace `parseGitHubForTest` / `parseGitLabForTest` with whatever the two existing test files already use to drive a raw body through the parser. Do NOT add a new helper if one exists.

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/scm/... -run HeadRepo -count=1`
Expected: FAIL - `ev.HeadRepo` undefined (the field does not exist yet).

- [ ] **Step 3: Add the field**

In `internal/scm/scm.go`, inside `WebhookEvent`, immediately after `HeadBranch`:

```go
	// HeadRepo identifies the repository the HEAD branch lives in, in the same
	// namespace as WebhookEvent.Repo's slug (a GitHub full_name, a GitLab
	// path_with_namespace). It is the webhook-path input to AdoptUpgradeMR
	// clause (d): a merge request whose head is NOT the base repo is a FORK
	// merge request and is never adopted, whatever its head branch is named.
	// EMPTY means the forge did not report it, and every consumer fails CLOSED
	// on empty - it is never defaulted to the base repo.
	HeadRepo string // PR/MR source repository slug; empty when unreported
```

- [ ] **Step 4: Decode it on both providers**

`internal/scm/github.go` - extend the anonymous `Head` struct on `ghWorkItem` (line 42):

```go
	Head    struct {
		SHA  string `json:"sha"`
		Ref  string `json:"ref"`
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
```

and in `ghWorkItemEvent`, beside `HeadBranch: wi.Head.Ref,`:

```go
		HeadRepo:     wi.Head.Repo.FullName,
```

`internal/scm/gitlab.go` - add to the `ObjectAttributes` struct of `glPayload` (find it near the top of the file):

```go
		Source struct {
			PathWithNamespace string `json:"path_with_namespace"`
		} `json:"source"`
```

and in `glWorkItemEvent`, beside `HeadBranch: p.ObjectAttributes.SourceBranch,`:

```go
		HeadRepo:     p.ObjectAttributes.Source.PathWithNamespace,
```

- [ ] **Step 5: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/scm/... -race -count=1`
Expected: PASS, including every pre-existing scm test.

- [ ] **Step 6: Lint and commit**

```bash
mise exec -- gofmt -s -w internal/scm
mise exec -- golangci-lint run ./internal/scm/... || [ $? -eq 5 ]
git add internal/scm
git commit -m "feat(scm): decode the merge request head repo on both webhook parsers"
```

---

## Task 2: The typed adoption payload on QueuedEvent

**Files:**
- Modify: `api/v1alpha1/queuedevent_types.go` (new `AdoptedUpgradeRef`, new payload field, new rule in `ValidateQueuedEventSpec`)
- Modify: `api/v1alpha1/annotations.go` (new label constants, after the `LabelActivity` block at `:143-149`)
- Generated: `api/v1alpha1/zz_generated.deepcopy.go`, `charts/tatara-operator/crd-bases/tatara.dev_queuedevents.yaml`
- Test: `api/v1alpha1/queuedevent_adopted_test.go` (new)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type AdoptedUpgradeRef struct { Number int; Title, Author, HeadSHA, HeadBranch, Body, Repo, HeadRepo string; Labels []string }`
  - `QueuedEventPayload.AdoptedUpgrade *AdoptedUpgradeRef` (json `adoptedUpgrade`, optional)
  - `const LabelUpgradeOrigin = "tatara.dev/upgrade-origin"`, `const UpgradeOriginAdopted = "adopted"`

- [ ] **Step 1: Write the failing tests**

Create `api/v1alpha1/queuedevent_adopted_test.go`:

```go
package v1alpha1

import "testing"

func adoptedSpec() QueuedEventSpec {
	return QueuedEventSpec{
		Seq:           1,
		Class:         QueueClassNormal,
		Kind:          "upgrade",
		ProjectRef:    "proj",
		RepositoryRef: "charts",
		DedupKey:      "adopt-upgrade|charts|41",
		Payload: QueuedEventPayload{
			Kind:          "upgrade",
			RepositoryRef: "charts",
			AdoptedUpgrade: &AdoptedUpgradeRef{
				Number: 41, Title: "chore(deps): bump cilium", Author: "tatara-bot",
				HeadSHA: "abc", HeadBranch: "renovate/cilium",
				Repo: "szymonrychu/charts", HeadRepo: "szymonrychu/charts",
			},
		},
	}
}

// THE HAPPY SHAPE VALIDATES. An adoption event is a MINT with a typed PR
// snapshot: no agentKind, no taskRef, no newTask.
func TestValidateQueuedEventSpec_AdoptedUpgradeMintIsValid(t *testing.T) {
	if err := ValidateQueuedEventSpec(adoptedSpec()); err != nil {
		t.Fatalf("ValidateQueuedEventSpec = %v, want nil", err)
	}
}

// MUTUALLY EXCLUSIVE WITH THE ADMISSION-TICKET SHAPE. A ticket admits an
// EXISTING Task's pod and mints nothing; an adoption snapshot mints a Task that
// does not exist. A payload claiming both describes two different pieces of
// work and the dispatcher would have to guess which one it is.
func TestValidateQueuedEventSpec_AdoptedUpgradeRejectsTheTicketShape(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AgentKind = "upgrade"
	spec.Payload.TaskRef = "some-task"
	if err := ValidateQueuedEventSpec(spec); err == nil {
		t.Fatal("ValidateQueuedEventSpec = nil, want an error for adoptedUpgrade + the ticket shape")
	}
}

// ...AND WITH THE B.7 BLUEPRINT, for the same reason.
func TestValidateQueuedEventSpec_AdoptedUpgradeRejectsTheBlueprintShape(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AgentKind = "upgrade"
	spec.Payload.NewTask = &QueuedTaskBlueprint{Name: "t", Kind: "upgrade", Goal: "g", ProjectRef: "proj"}
	if err := ValidateQueuedEventSpec(spec); err == nil {
		t.Fatal("ValidateQueuedEventSpec = nil, want an error for adoptedUpgrade + newTask")
	}
}

// A MERGE REQUEST NUMBER IS THE WHOLE IDENTITY. Without it the mint has no
// deterministic Task name and no merge request to bind.
func TestValidateQueuedEventSpec_AdoptedUpgradeRequiresANumber(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AdoptedUpgrade.Number = 0
	if err := ValidateQueuedEventSpec(spec); err == nil {
		t.Fatal("ValidateQueuedEventSpec = nil, want an error for adoptedUpgrade with number 0")
	}
}

// BACKWARD COMPATIBILITY. Every event already Queued when this ships has a nil
// AdoptedUpgrade and must keep validating byte-identically.
func TestValidateQueuedEventSpec_LegacyFlatMintUnchanged(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AdoptedUpgrade = nil
	spec.Payload.Goal = "do a thing"
	if err := ValidateQueuedEventSpec(spec); err != nil {
		t.Fatalf("ValidateQueuedEventSpec = %v, want nil for a legacy flat mint", err)
	}
}
```

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./api/v1alpha1/... -run AdoptedUpgrade -count=1`
Expected: FAIL - `AdoptedUpgradeRef` undefined.

- [ ] **Step 3: Add the type, the field and the rule**

In `api/v1alpha1/queuedevent_types.go`, after the `QueuedTaskBlueprint` type (line 110):

```go
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
	// Body carries the changelog the review agent reads. Capped at the same
	// budget the goal is: an engine release-notes body is unbounded upstream.
	// +kubebuilder:validation:MaxLength=16384
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
```

Add to `QueuedEventPayload`, after `NewTask`:

```go
	// AdoptedUpgrade marks the ADOPTION payload shape: a dependency-upgrade
	// merge request that already exists on the forge, queued until the pool has
	// room. Mutually exclusive with the admission-ticket shape
	// (AgentKind+TaskRef) and with NewTask - see ValidateQueuedEventSpec.
	// +optional
	AdoptedUpgrade *AdoptedUpgradeRef `json:"adoptedUpgrade,omitempty"`
```

Add to `ValidateQueuedEventSpec`, just before the final `return nil`:

```go
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
```

In `api/v1alpha1/annotations.go`, immediately after the `LabelSourceKind`/`LabelActivity` block:

```go
// LabelUpgradeOrigin discriminates the two producers of an `upgrade` Task, and
// it exists so a draining dependency-engine backlog does not silence the upgrade
// CRON. maxOpenUpgrades bounds the cron ALONE (design D2); adopted merge requests
// are bounded by the general pool. Without a discriminator openUpgradeLaneCount
// would read a Renovate batch as "lanes full" and the cron would stop proposing
// bumps for as long as the batch lasted, invisibly.
//
// The prefix is tatara.dev/, NOT the tatara.io/ its neighbours above use. That
// split predates this label (every annotation in this file is tatara.dev/, both
// scan labels are tatara.io/); do not "fix" one to match the other, it would
// orphan every live object carrying the old key.
const (
	LabelUpgradeOrigin = "tatara.dev/upgrade-origin"
	// UpgradeOriginAdopted marks work born from an EXISTING third-party merge
	// request. The cron's own mints carry no upgrade-origin label at all.
	UpgradeOriginAdopted = "adopted"
)
```

- [ ] **Step 4: Regenerate**

Run: `mise exec -- make generate manifests`

This regenerates exactly two files - `api/v1alpha1/zz_generated.deepcopy.go` (a `DeepCopyInto`/`DeepCopy` pair for `AdoptedUpgradeRef` plus the pointer copy inside `QueuedEventPayload.DeepCopyInto`) and `charts/tatara-operator/crd-bases/tatara.dev_queuedevents.yaml` (a new `spec.payload.adoptedUpgrade` object). There is NO `config/crd/bases` in this repo; the chart copy IS the generated copy (see finding (i)).

Verify: `git diff --stat` shows those two files and nothing else generated.
Verify: `grep -c adoptedUpgrade charts/tatara-operator/crd-bases/tatara.dev_queuedevents.yaml` is non-zero.

- [ ] **Step 5: Run the tests, confirm they pass**

Run: `mise exec -- go test ./api/... -race -count=1`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
mise exec -- gofmt -s -w api
mise exec -- golangci-lint run ./api/... || [ $? -eq 5 ]
git add api charts/tatara-operator/crd-bases
git commit -m "feat(api): carry a dependency merge request snapshot on QueuedEvent"
```

---

## Task 3: Queue helpers - dedup key, mint stamp, adoption predicate

**Files:**
- Modify: `internal/queue/enqueue.go` (new `AdoptUpgradeDedupKey`, `MintStamp`, `IsAdoptedUpgradeMint`; `BuildTaskFromQueuedEvent` refactored onto `MintStamp`)
- Test: `internal/queue/adopt_helpers_test.go` (new)

**Interfaces:**
- Consumes: `tatarav1alpha1.AdoptedUpgradeRef` (Task 2).
- Produces:
  - `func AdoptUpgradeDedupKey(repo string, number int) string` -> `"adopt-upgrade|<repo>|<number>"`, where `repo` is the **Repository CR name** (not the forge slug), so the webhook and the sweep derive an identical key from what each has in hand.
  - `func MintStamp(qe *tatarav1alpha1.QueuedEvent) map[string]string` -> the exact label set a mint stamps on its Task: `LabelQueuedEvent` always, `LabelMintedBy` when `qe.UID != ""`, `LabelDedupKey` when `qe.Spec.DedupKey != ""`.
  - `func IsAdoptedUpgradeMint(qe *tatarav1alpha1.QueuedEvent) bool` -> `qe.Spec.Payload.AdoptedUpgrade != nil`.

- [ ] **Step 1: Write the failing tests**

Create `internal/queue/adopt_helpers_test.go`:

```go
package queue

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// THE KEY IS DERIVED FROM WHAT BOTH PRODUCERS HAVE IN HAND. The webhook has a
// matched Repository CR and the delivery's number; the sweep has the same
// Repository CR and the listing row's number. Keying on the Repository CR NAME
// rather than the forge slug is what makes the two collide on AlreadyExists
// instead of double-enqueueing.
func TestAdoptUpgradeDedupKey(t *testing.T) {
	if got := AdoptUpgradeDedupKey("charts", 1026); got != "adopt-upgrade|charts|1026" {
		t.Fatalf("AdoptUpgradeDedupKey = %q", got)
	}
	if AdoptUpgradeDedupKey("charts", 1026) == AdoptUpgradeDedupKey("charts", 1027) {
		t.Fatal("two merge requests in one repo must not share a dedup key")
	}
	if AdoptUpgradeDedupKey("charts", 1) == AdoptUpgradeDedupKey("containers", 1) {
		t.Fatal("two repos must not share a dedup key for the same number")
	}
}

func TestIsAdoptedUpgradeMint(t *testing.T) {
	plain := &tatarav1alpha1.QueuedEvent{}
	if IsAdoptedUpgradeMint(plain) {
		t.Fatal("a plain mint is not an adoption")
	}
	adopted := &tatarav1alpha1.QueuedEvent{Spec: tatarav1alpha1.QueuedEventSpec{
		Payload: tatarav1alpha1.QueuedEventPayload{
			AdoptedUpgrade: &tatarav1alpha1.AdoptedUpgradeRef{Number: 1},
		}}}
	if !IsAdoptedUpgradeMint(adopted) {
		t.Fatal("an adoptedUpgrade payload IS an adoption")
	}
}

// MintStamp IS THE MINT-ACCOUNTABILITY LABEL SET, and it is extracted so the
// adoption mint - which builds its Task by hand rather than through
// BuildTaskFromQueuedEvent - cannot drift from it. All three labels are
// load-bearing: LabelQueuedEvent is what mapTaskToQE and reconcileDone follow,
// LabelMintedBy is the #443 idempotency link, and LabelDedupKey is what stops a
// redelivered webhook enqueueing a second event once the first is GC'd.
func TestMintStamp_CarriesAllThreeLabels(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-abc", UID: types.UID("u-1")},
		Spec:       tatarav1alpha1.QueuedEventSpec{DedupKey: "adopt-upgrade|charts|41"},
	}
	got := MintStamp(qe)
	if got[LabelQueuedEvent] != "qe-abc" {
		t.Errorf("LabelQueuedEvent = %q", got[LabelQueuedEvent])
	}
	if got[LabelMintedBy] != "u-1" {
		t.Errorf("LabelMintedBy = %q", got[LabelMintedBy])
	}
	if got[LabelDedupKey] != dedupLabel("adopt-upgrade|charts|41") {
		t.Errorf("LabelDedupKey = %q", got[LabelDedupKey])
	}
}

// A UID-LESS EVENT (a Go literal that never met an API server) adopts nothing,
// so it stamps no minted-by link - mintedTask treats an unset UID that way too.
func TestMintStamp_OmitsMintedByWithoutAUID(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{ObjectMeta: metav1.ObjectMeta{Name: "qe-abc"}}
	if _, ok := MintStamp(qe)[LabelMintedBy]; ok {
		t.Fatal("MintStamp must omit LabelMintedBy for a UID-less event")
	}
}

// THE REFACTOR MUST NOT MOVE THE GENERIC MINT. BuildTaskFromQueuedEvent's own
// output has to keep carrying exactly what MintStamp returns.
func TestBuildTaskFromQueuedEvent_StampsExactlyMintStamp(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-xyz", Namespace: "tatara", UID: types.UID("u-2")},
		Spec: tatarav1alpha1.QueuedEventSpec{
			DedupKey: "k1",
			Payload:  tatarav1alpha1.QueuedEventPayload{Kind: "implement", Goal: "g", GenerateName: "t-"},
		},
	}
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: "tatara"}}
	task, err := BuildTaskFromQueuedEvent(qe, proj, testScheme(t)) // reuse this package's existing scheme helper
	if err != nil {
		t.Fatalf("BuildTaskFromQueuedEvent: %v", err)
	}
	for k, v := range MintStamp(qe) {
		if task.Labels[k] != v {
			t.Errorf("task label %s = %q, want %q", k, task.Labels[k], v)
		}
	}
}
```

Reuse whatever scheme helper the existing `internal/queue` tests already have instead of `testScheme(t)`; grep the package's `_test.go` files first.

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/queue/... -run 'AdoptUpgradeDedupKey|IsAdoptedUpgradeMint|MintStamp' -count=1`
Expected: FAIL - undefined `AdoptUpgradeDedupKey`, `IsAdoptedUpgradeMint`, `MintStamp`.

- [ ] **Step 3: Implement**

In `internal/queue/enqueue.go`, add `strconv` to the imports and add, after `QueuedEventName`:

```go
// AdoptUpgradeDedupKey is the natural key of ONE queued dependency-upgrade
// adoption. Both producers derive it from the same two facts they each already
// hold - the Repository CR's NAME and the merge request number - so a webhook
// enqueue and a sweep enqueue of the same merge request collide on
// AlreadyExists at the API server and burn no sequence number.
//
// The Repository CR name, deliberately, not the forge slug: the webhook resolves
// a Repository via matchRepo and the sweep iterates Repository CRs, so the CR
// name is the one identifier both sides have without a second lookup.
func AdoptUpgradeDedupKey(repo string, number int) string {
	return "adopt-upgrade|" + repo + "|" + strconv.Itoa(number)
}

// IsAdoptedUpgradeMint reports whether qe mints an ADOPTED dependency-upgrade
// Task rather than a generic one. The payload field IS the discriminator - there
// is no mirrored label to drift from it.
func IsAdoptedUpgradeMint(qe *tatarav1alpha1.QueuedEvent) bool {
	return qe != nil && qe.Spec.Payload.AdoptedUpgrade != nil
}

// MintStamp is the label set EVERY mint puts on the Task it creates, extracted
// so the two minting paths cannot drift. BuildTaskFromQueuedEvent uses it for
// the generic mint; Minter.MintAdoptedUpgradeTask takes it as a parameter,
// because it builds its Task by hand.
//
// All three are load-bearing and the adoption path needs each for a different
// reason: LabelQueuedEvent is what mapTaskToQE follows to re-trigger admission
// and what reconcileDone matches to GC the spent event, LabelMintedBy is the
// #443 idempotency link the dispatcher's mintedTask reads, and LabelDedupKey is
// what makes dedupExists refuse a SECOND enqueue for a merge request whose event
// has already been admitted and collected.
func MintStamp(qe *tatarav1alpha1.QueuedEvent) map[string]string {
	stamp := map[string]string{LabelQueuedEvent: qe.Name}
	if qe.UID != "" {
		stamp[LabelMintedBy] = string(qe.UID)
	}
	if qe.Spec.DedupKey != "" {
		stamp[LabelDedupKey] = dedupLabel(qe.Spec.DedupKey)
	}
	return stamp
}
```

In `BuildTaskFromQueuedEvent`, replace the three lines that stamp those labels (currently `labels[LabelQueuedEvent] = qe.Name` through the `if qe.Spec.DedupKey != ""` block) with:

```go
	for k, v := range MintStamp(qe) {
		labels[k] = v
	}
```

- [ ] **Step 4: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/queue/... -race -count=1`
Expected: PASS, including every pre-existing queue test.

- [ ] **Step 5: Lint and commit**

```bash
mise exec -- gofmt -s -w internal/queue
mise exec -- golangci-lint run ./internal/queue/... || [ $? -eq 5 ]
git add internal/queue
git commit -m "feat(queue): add the adoption dedup key and extract the mint label stamp"
```

---

## Task 4: The adopted mint takes a label stamp and marks its origin

**Files:**
- Modify: `internal/controller/upgrade_adopt.go` (`MintAdoptedUpgradeTask` signature + label block; new `AdoptedUpgradeRefFromPR` and `prRefFromAdopted`)
- Modify: `internal/controller/sweep.go:1817` (the one existing call site, to keep the tree compiling; Task 8 rewrites that arm entirely)
- Test: `internal/controller/upgrade_adopt_stamp_test.go` (new)

**Interfaces:**
- Consumes: `queue.MintStamp` (Task 3), `tatarav1alpha1.AdoptedUpgradeRef` + `LabelUpgradeOrigin` (Task 2).
- Produces:
  - `func (m *Minter) MintAdoptedUpgradeTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, pr scm.PRRef, sp objbudget.Spiller, stamp map[string]string) (*tatarav1alpha1.Task, MintOutcome, error)` - `stamp` may be nil.
  - `func AdoptedUpgradeRefFromPR(pr scm.PRRef) *tatarav1alpha1.AdoptedUpgradeRef`
  - `func prRefFromAdopted(a *tatarav1alpha1.AdoptedUpgradeRef) scm.PRRef` (unexported; the dispatcher is in the same package)

- [ ] **Step 1: Write the failing tests**

Create `internal/controller/upgrade_adopt_stamp_test.go`. Reuse `projectWithAdoptPrefix`, `adoptRepo`, `renovatePR` and the client helper from `sweep_adopt_upgrade_test.go` / `sweep_adopt_headroom_test.go` - read those first.

```go
package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// EVERY ADOPTED TASK SAYS SO, or the upgrade cron reads a draining Renovate
// backlog as "lanes full" and stops proposing bumps for as long as the backlog
// lasts, with no error and no log (design D2).
func TestMintAdoptedUpgradeTask_StampsTheAdoptedOrigin(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	c := newMirrorClient(t, proj, repo)
	m := &Minter{Client: c, Scheme: c.Scheme()}

	task, outcome, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, renovatePR(), nil, nil)
	if err != nil || outcome != MintCreated {
		t.Fatalf("mint = (%v, %v)", outcome, err)
	}
	if got := task.Labels[tatarav1alpha1.LabelUpgradeOrigin]; got != tatarav1alpha1.UpgradeOriginAdopted {
		t.Fatalf("upgrade-origin = %q, want %q", got, tatarav1alpha1.UpgradeOriginAdopted)
	}
}

// THE MINT-ACCOUNTABILITY LABELS COME FROM THE CALLER, and without them the
// dispatcher's event is admitted and NEVER reaped: reconcileDone matches the
// Task by LabelQueuedEvent, mapTaskToQE re-triggers admission through it, and
// mintedTask resolves #443 idempotency through LabelMintedBy.
func TestMintAdoptedUpgradeTask_CarriesTheCallersMintStamp(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	c := newMirrorClient(t, proj, repo)
	m := &Minter{Client: c, Scheme: c.Scheme()}

	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-1", Namespace: proj.Namespace, UID: types.UID("u-9")},
		Spec:       tatarav1alpha1.QueuedEventSpec{DedupKey: queue.AdoptUpgradeDedupKey(repo.Name, 41)},
	}
	task, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, renovatePR(), nil, queue.MintStamp(qe))
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	for k, want := range queue.MintStamp(qe) {
		if task.Labels[k] != want {
			t.Errorf("task label %s = %q, want %q", k, task.Labels[k], want)
		}
	}
}

// A NIL STAMP IS LEGAL and stamps only the origin: the mint has one caller
// today, but a nil map must never panic on a range.
func TestMintAdoptedUpgradeTask_NilStampIsSafe(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	c := newMirrorClient(t, proj, repo)
	m := &Minter{Client: c, Scheme: c.Scheme()}
	if _, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, renovatePR(), nil, nil); err != nil {
		t.Fatalf("mint with a nil stamp: %v", err)
	}
}

// THE SNAPSHOT ROUND-TRIPS. Everything AdoptUpgradeMR and MintAdoptedUpgradeTask
// read off a PRRef must survive a trip through the CRD and back, or a queued
// adoption mints from a lossy copy - and the LOSSY FIELD IS THE FORK GUARD.
func TestAdoptedUpgradeRefRoundTripsThePRRef(t *testing.T) {
	pr := scm.PRRef{
		Repo: "szymonrychu/charts", HeadRepo: "szymonrychu/charts", Number: 41,
		Title: "chore(deps): bump", Author: "tatara-bot", HeadSHA: "abc",
		HeadBranch: "renovate/cilium", Body: "notes", Labels: []string{"deps"},
	}
	got := prRefFromAdopted(AdoptedUpgradeRefFromPR(pr))
	if got.Repo != pr.Repo || got.HeadRepo != pr.HeadRepo || got.Number != pr.Number ||
		got.Title != pr.Title || got.Author != pr.Author || got.HeadSHA != pr.HeadSHA ||
		got.HeadBranch != pr.HeadBranch || got.Body != pr.Body || len(got.Labels) != 1 {
		t.Fatalf("round trip lost data: %+v", got)
	}
}
```

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/controller/... -run 'MintAdoptedUpgradeTask_Stamps|MintStamp|AdoptedUpgradeRef' -count=1`
Expected: FAIL - too many arguments to `MintAdoptedUpgradeTask`; `AdoptedUpgradeRefFromPR` undefined.

- [ ] **Step 3: Implement**

In `internal/controller/upgrade_adopt.go`, change the signature and the label block:

```go
func (m *Minter) MintAdoptedUpgradeTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, pr scm.PRRef,
	sp objbudget.Spiller, stamp map[string]string) (*tatarav1alpha1.Task, MintOutcome, error) {
```

and add to the doc comment, after the numbered list:

```
//  3. Stamps LabelUpgradeOrigin=adopted, so openUpgradeLaneCount can exclude
//     this Task from the CRON's maxOpenUpgrades budget (design D2). Adopted
//     work is bounded by the general pool; the cron's knob counts only its own.
//
// `stamp` is the minting QueuedEvent's label set (queue.MintStamp) and may be
// nil. It is TAKEN, not derived, because this mint builds its Task by hand
// rather than through BuildTaskFromQueuedEvent - and without it the event that
// admitted this Task is never reaped: reconcileDone finds the Task by
// LabelQueuedEvent and mintedTask resolves idempotency by LabelMintedBy.
```

In the `task := &tatarav1alpha1.Task{...}` literal, add a `Labels` field to the `ObjectMeta`:

```go
			Labels: adoptedTaskLabels(stamp),
```

and add the helper plus the two converters at the end of the file:

```go
// adoptedTaskLabels merges the caller's mint stamp with the adoption origin
// marker. The origin is stamped LAST and unconditionally: it is this repo's own
// invariant, not the caller's to override.
func adoptedTaskLabels(stamp map[string]string) map[string]string {
	labels := make(map[string]string, len(stamp)+1)
	for k, v := range stamp {
		labels[k] = v
	}
	labels[tatarav1alpha1.LabelUpgradeOrigin] = tatarav1alpha1.UpgradeOriginAdopted
	return labels
}

// AdoptedUpgradeRefFromPR snapshots a listing/delivery PRRef into the CRD shape
// a QueuedEvent carries. Everything AdoptUpgradeMR and MintAdoptedUpgradeTask
// read is copied, HeadRepo included - the fork guard fails CLOSED on an empty
// one, so a lossy copy silently disarms adoption rather than breaking it loudly.
func AdoptedUpgradeRefFromPR(pr scm.PRRef) *tatarav1alpha1.AdoptedUpgradeRef {
	return &tatarav1alpha1.AdoptedUpgradeRef{
		Number: pr.Number, Title: pr.Title, Author: pr.Author,
		HeadSHA: pr.HeadSHA, HeadBranch: pr.HeadBranch, Body: pr.Body,
		Labels: pr.Labels, Repo: pr.Repo, HeadRepo: pr.HeadRepo,
	}
}

// prRefFromAdopted is AdoptedUpgradeRefFromPR's inverse, used at admit time.
// UpdatedAt is deliberately left zero: nothing on the adoption path reads it.
func prRefFromAdopted(a *tatarav1alpha1.AdoptedUpgradeRef) scm.PRRef {
	if a == nil {
		return scm.PRRef{}
	}
	return scm.PRRef{
		Number: a.Number, Title: a.Title, Author: a.Author,
		HeadSHA: a.HeadSHA, HeadBranch: a.HeadBranch, Body: a.Body,
		Labels: a.Labels, Repo: a.Repo, HeadRepo: a.HeadRepo,
	}
}
```

In `internal/controller/sweep.go:1817`, add the sixth argument so the tree compiles (Task 8 replaces this whole arm):

```go
			tk, outcome, aerr := r.minter().MintAdoptedUpgradeTask(ctx, proj, repo, pr, sp, nil)
```

- [ ] **Step 4: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/controller/... -run 'Adopt|Upgrade' -count=1`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
mise exec -- gofmt -s -w internal/controller
mise exec -- golangci-lint run ./internal/controller/... || [ $? -eq 5 ]
git add internal/controller
git commit -m "feat(controller): mark adopted upgrade tasks and take the mint label stamp"
```

---

## Task 5: The dispatcher admits a queued adoption

**Files:**
- Modify: `internal/controller/queue_controller.go` (new `SpillerFor` field on `DispatcherReconciler`; new branch inside the mint arm at `:551-630`; new `admitAdoptedUpgrade` method)
- Modify: `cmd/manager/wire.go:533-541` (wire `SpillerFor: spillerFor`)
- Test: `internal/controller/queue_adopt_admit_test.go` (new)

**Interfaces:**
- Consumes: `queue.IsAdoptedUpgradeMint`, `queue.MintStamp` (Task 3); `Minter.MintAdoptedUpgradeTask`, `prRefFromAdopted` (Task 4); `AdoptUpgradeMR`, `Minter.mergeRequestCR`, `Minter.resolveLiveMROwner` (existing, `sweep.go`).
- Produces: `func (r *DispatcherReconciler) admitAdoptedUpgrade(ctx context.Context, proj *tatarav1alpha1.Project, q *tatarav1alpha1.QueuedEvent) (*tatarav1alpha1.Task, adoptVerdict, error)` with `adoptVerdict` in `{adoptMinted, adoptDropped, adoptRetry}`.

- [ ] **Step 1: Write the failing tests**

Create `internal/controller/queue_adopt_admit_test.go`. Reuse this package's existing dispatcher test scaffolding - grep for `DispatcherReconciler{` in `*_test.go` and copy the client/scheme construction verbatim.

```go
package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// adoptionEvent is the QueuedEvent the webhook and the sweep both produce.
func adoptionEvent(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) *tatarav1alpha1.QueuedEvent {
	p := 2
	key := queue.AdoptUpgradeDedupKey(repo.Name, number)
	return &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{
			Name: queue.QueuedEventName(proj.Name, key), Namespace: proj.Namespace,
			UID: types.UID("uid-" + key),
		},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: int64(number), Class: tatarav1alpha1.QueueClassNormal, Kind: "upgrade",
			ProjectRef: proj.Name, RepositoryRef: repo.Name, DedupKey: key, Priority: &p,
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "upgrade", RepositoryRef: repo.Name,
				AdoptedUpgrade: &tatarav1alpha1.AdoptedUpgradeRef{
					Number: number, Author: "szymonrychu-bot", Title: "chore(deps): bump",
					HeadSHA: "sha", HeadBranch: "renovate/cilium",
					Repo: "charts", HeadRepo: "charts",
				},
			},
		},
		Status: tatarav1alpha1.QueuedEventStatus{State: tatarav1alpha1.QueueStateQueued},
	}
}

// THE HEADLINE. An adoption payload mints through MintAdoptedUpgradeTask, NOT
// through BuildTaskFromQueuedEvent: only that path binds the merge-request
// mirror, stamps AnnTakeoverHeadBranch, sets MergeOrder and seeds the adopted
// significance floor. A generic mint would produce a Task the merge corridor
// parks operator-error on.
func TestAdmit_AdoptedUpgradePayloadMintsThroughTheAdoptionFunnel(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	qe := adoptionEvent(proj, repo, 41)
	c := newMirrorClient(t, proj, repo, qe)
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budgetDecisionOff(), budgetConfigOff(), budgetSubscriptionOff(), time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}

	var task tatarav1alpha1.Task
	name := AdoptedUpgradeTaskName(proj.Name, repo.Name, 41)
	if err := c.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: name}, &task); err != nil {
		t.Fatalf("adopted task %s not minted: %v", name, err)
	}
	if task.Spec.Kind != "upgrade" {
		t.Errorf("kind = %q, want upgrade", task.Spec.Kind)
	}
	if task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch] != "renovate/cilium" {
		t.Errorf("head branch annotation = %q", task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch])
	}
	if task.Labels[queue.LabelQueuedEvent] != qe.Name {
		t.Errorf("queued-event label = %q, want %q", task.Labels[queue.LabelQueuedEvent], qe.Name)
	}
	if task.Labels[queue.LabelMintedBy] != string(qe.UID) {
		t.Errorf("minted-by label = %q", task.Labels[queue.LabelMintedBy])
	}

	var fresh tatarav1alpha1.QueuedEvent
	if err := c.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Name}, &fresh); err != nil {
		t.Fatalf("get event: %v", err)
	}
	if fresh.Status.State != tatarav1alpha1.QueueStateAdmitted || fresh.Status.TaskRef != name {
		t.Fatalf("event status = %+v, want Admitted -> %s", fresh.Status, name)
	}
}

// THE PREDICATES RUN AT ADMIT, NOT ONLY AT ENQUEUE. A queued adoption may wait,
// and MintAdoptedUpgradeTask does not check AdoptUpgradeMR itself - so without
// this the dispatcher would adopt a FORK merge request whose head repo the
// webhook could not report. The guard fails CLOSED on an empty head repo.
func TestAdmit_AdoptedUpgradeRefusedByThePredicateIsDropped(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	qe := adoptionEvent(proj, repo, 42)
	qe.Spec.Payload.AdoptedUpgrade.HeadRepo = "" // the forge did not say; fail closed
	c := newMirrorClient(t, proj, repo, qe)
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budgetDecisionOff(), budgetConfigOff(), budgetSubscriptionOff(), time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}
	var task tatarav1alpha1.Task
	err := c.Get(ctx, types.NamespacedName{
		Namespace: proj.Namespace, Name: AdoptedUpgradeTaskName(proj.Name, repo.Name, 42)}, &task)
	if err == nil {
		t.Fatal("a fork-guard refusal must mint nothing")
	}
	var gone tatarav1alpha1.QueuedEvent
	if err := c.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Name}, &gone); err == nil {
		t.Fatal("a refused adoption event must be deleted, not left Queued forever")
	}
}

// A DELETED REPOSITORY LEAVES NOTHING TO ADOPT INTO. The event is dropped rather
// than retried forever: the merge request is unreachable and the Project's owner
// reference will not collect the event on its own.
func TestAdmit_AdoptedUpgradeWithNoRepositoryIsDropped(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	qe := adoptionEvent(proj, repo, 43)
	c := newMirrorClient(t, proj, qe) // repo deliberately NOT seeded
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budgetDecisionOff(), budgetConfigOff(), budgetSubscriptionOff(), time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}
	var gone tatarav1alpha1.QueuedEvent
	if err := c.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Name}, &gone); err == nil {
		t.Fatal("an adoption event whose repository is gone must be dropped")
	}
}

// BACKWARD COMPATIBILITY. An event already Queued when this ships carries no
// adoptedUpgrade and must still take BuildTaskFromQueuedEvent unchanged.
func TestAdmit_LegacyMintPayloadIsUnaffected(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	qe := adoptionEvent(proj, adoptRepo(), 44)
	qe.Spec.Payload.AdoptedUpgrade = nil
	qe.Spec.Payload.Goal = "a plain upgrade"
	qe.Spec.Payload.GenerateName = "upgrade-"
	qe.Spec.RepositoryRef, qe.Spec.Payload.RepositoryRef = "", ""
	c := newMirrorClient(t, proj, qe)
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budgetDecisionOff(), budgetConfigOff(), budgetSubscriptionOff(), time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl); err != nil || len(tl.Items) != 1 {
		t.Fatalf("legacy mint produced %d tasks (err %v), want 1", len(tl.Items), err)
	}
	if _, adopted := tl.Items[0].Labels[tatarav1alpha1.LabelUpgradeOrigin]; adopted {
		t.Fatal("a legacy mint must not be marked adopted")
	}
}
```

`budgetDecisionOff()` / `budgetConfigOff()` / `budgetSubscriptionOff()` stand in for the zero-value `budget.Decision`, `budget.Config` and `budget.Subscription` the existing dispatcher tests already pass; use whatever those tests use (grep `r.admit(` in `internal/controller/*_test.go`) rather than adding helpers.

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/controller/... -run 'Admit_Adopted|Admit_Legacy' -count=1`
Expected: FAIL - the adopted event mints through `BuildTaskFromQueuedEvent`, so `AdoptedUpgradeTaskName(...)` is never created.

- [ ] **Step 3: Add the field and the wiring**

In `internal/controller/queue_controller.go`, add to `DispatcherReconciler`:

```go
	// SpillerFor resolves the A.7 byte-budget spiller for a Project. The
	// dispatcher needs one for exactly ONE path: an adopted dependency-upgrade
	// mint, whose bindMRToTask CREATES the merge-request mirror and therefore
	// goes through the objbudget guard. Nil is safe (unit tests, and every
	// non-adoption admission, which writes no mirror).
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
```

with `"github.com/szymonrychu/tatara-operator/internal/objbudget"` added to the imports.

In `cmd/manager/wire.go`, add to the `DispatcherReconciler` literal at line 533:

```go
		SpillerFor:        spillerFor,
```

(`spillerFor` is the closure defined at line 390 in the same `addReconcilers` function; no new plumbing is needed.)

- [ ] **Step 4: Route the mint branch**

In `admitPool`, inside the `if existing == nil {` block, AFTER the `room <= 0` live-ceiling gate and IN PLACE of the current `queue.BuildTaskFromQueuedEvent` + `r.Create` sequence, branch:

```go
					if queue.IsAdoptedUpgradeMint(q) {
						// THE ADOPTION FUNNEL, not the generic build. Only
						// MintAdoptedUpgradeTask binds the merge-request mirror (which it
						// also CREATES - an unadopted merge request has none), stamps
						// AnnTakeoverHeadBranch so both pods check out the engine's
						// branch, sets MergeOrder and seeds the adopted significance
						// floor. A generic mint would produce a Task whose merge corridor
						// parks operator-error on the first reconcile.
						minted, verdict, aerr := r.admitAdoptedUpgrade(ctx, proj, q)
						if aerr != nil {
							return aerr
						}
						switch verdict {
						case adoptDropped:
							continue // the event is gone; no slot burned
						case adoptRetry:
							requeue = true
							continue // still Queued; no slot burned
						}
						existing = minted
					} else {
						task, buildErr := queue.BuildTaskFromQueuedEvent(q, proj, r.Scheme)
						if buildErr != nil {
							return buildErr
						}
						createErr := r.Create(ctx, task)
						switch {
						case createErr == nil:
							existing = task // the API server filled in the minted name
						case apierrors.IsAlreadyExists(createErr):
							// Only an explicitly NAMED mint (payload.name) can still collide.
							existing = &tatarav1alpha1.Task{}
							if getErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, existing); getErr != nil {
								return getErr
							}
						default:
							// Leave Queued; requeue. Slot not consumed (inflight derives from Admitted).
							return createErr
						}
					}
					// SPEND the live-mint budget: unchanged, and it applies to BOTH
					// arms - an adopted Task becomes live exactly like any other.
					mintBudget--
```

- [ ] **Step 5: Add `admitAdoptedUpgrade`**

Add near `admitTicket` in the same file:

```go
// adoptVerdict is what admitAdoptedUpgrade decided about an adoption event.
type adoptVerdict int

const (
	// adoptMinted: the adopted upgrade Task exists (created, or a live twin).
	adoptMinted adoptVerdict = iota
	// adoptDropped: the adoption is no longer owed. The event has been DELETED.
	// It burns no slot and is never requeued.
	adoptDropped
	// adoptRetry: the mint is still OWED but did not happen (a dead twin held
	// the deterministic name and was just deleted). Stays Queued, burns no slot.
	adoptRetry
)

// admitAdoptedUpgrade mints the upgrade Task for a QUEUED dependency-upgrade
// adoption, and it re-asks the adoption question first.
//
// THE PREDICATES DO NOT RUN ANYWHERE ELSE ON THIS PATH. In the sweep they ran in
// ClassifyPR one frame above the mint; MintAdoptedUpgradeTask has never checked
// AdoptUpgradeMR itself. So without this re-ask an admission would adopt a fork
// merge request the webhook could not classify (scm.WebhookEvent reports a head
// repo, but an old delivery or a forge that omits it leaves the guard's input
// empty and the guard fails CLOSED), or one a human has since taken over.
//
// THE MIRROR AND THE LIVE OWNER ARE READ FRESH, not carried on the event: they
// are the two inputs that can change while an event waits, and an unadopted
// merge request has no mirror at all until this mint's own bind creates one.
func (r *DispatcherReconciler) admitAdoptedUpgrade(ctx context.Context, proj *tatarav1alpha1.Project,
	q *tatarav1alpha1.QueuedEvent) (*tatarav1alpha1.Task, adoptVerdict, error) {

	lg := log.FromContext(ctx)
	drop := func(reason string) (*tatarav1alpha1.Task, adoptVerdict, error) {
		obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, reason).Inc()
		lg.Info("queue: dropped a queued dependency-upgrade adoption",
			"action", "queue_adopt_drop", "resource_id", q.Name, "project", proj.Name,
			"repository", q.Spec.RepositoryRef, "number", q.Spec.Payload.AdoptedUpgrade.Number,
			"reason", reason)
		if err := r.Delete(ctx, q); err != nil && !apierrors.IsNotFound(err) {
			return nil, adoptRetry, err
		}
		return nil, adoptDropped, nil
	}

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: q.Namespace, Name: q.Spec.RepositoryRef}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return drop("repository_gone")
		}
		return nil, adoptRetry, err
	}

	pr := prRefFromAdopted(q.Spec.Payload.AdoptedUpgrade)
	m := &Minter{Client: r.Client, APIReader: r.APIReader, Scheme: r.Scheme,
		Metrics: r.Metrics, SpillerFor: r.SpillerFor, Activity: SweepActivity}
	cr, err := m.mergeRequestCR(ctx, proj, &repo, pr.Number)
	if err != nil {
		return nil, adoptRetry, err
	}
	liveOwner, err := m.resolveLiveMROwner(ctx, proj, cr, SweepActivity)
	if err != nil {
		return nil, adoptRetry, err
	}
	if !AdoptUpgradeMR(proj, pr, nil, liveOwner, cr) {
		return drop("not_adoptable")
	}

	task, outcome, err := m.MintAdoptedUpgradeTask(ctx, proj, &repo, pr, m.spillerFor(proj), queue.MintStamp(q))
	if err != nil {
		return nil, adoptRetry, err
	}
	switch outcome {
	case MintTombstoneDeleted:
		// A dead twin held the deterministic name and has just been deleted; the
		// mint is still OWED. Stay Queued and let the prompt requeue re-drive it.
		return nil, adoptRetry, nil
	case MintNotOwed:
		return drop("mint_not_owed")
	}
	lg.Info("queue: adopted a dependency upgrade merge request into an upgrade task",
		"action", "queue_adopt_upgrade_mr", "resource_id", task.Name, "project", proj.Name,
		"repository", repo.Name, "number", pr.Number, "head_branch", pr.HeadBranch,
		"author", pr.Author, "outcome", string(outcome))
	return task, adoptMinted, nil
}
```

Add `"github.com/szymonrychu/tatara-operator/internal/obs"` if it is not already imported (it is, for `obs.OperatorMetrics`).

`obs.AdoptionEventDroppedTotal` is created in Task 6; if Task 6 has not landed yet, add the counter there first or stub it in this task and let Task 6 seed it. Prefer landing Task 6's `internal/obs/sweep_metrics.go` counter block ahead of this step.

- [ ] **Step 6: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/controller/... -race -count=1`
Run: `mise exec -- go build ./...`
Expected: PASS / clean build.

- [ ] **Step 7: Lint and commit**

```bash
mise exec -- gofmt -s -w internal/controller cmd
mise exec -- golangci-lint run ./... || [ $? -eq 5 ]
git add internal/controller cmd/manager/wire.go
git commit -m "feat(queue): admit queued dependency-upgrade adoptions through the adoption funnel"
```

---

## Task 6: The webhook enqueues instead of pulling the sweep forward

**Files:**
- Modify: `internal/webhook/server.go:798-806` (the `AdoptionCandidate` arm)
- Modify: `internal/obs/sweep_metrics.go` (two new counters + their seeding)
- Modify: `internal/webhook/upgrade_adoption_test.go` (rewrite the marker assertions as queue assertions)
- Test: `internal/webhook/upgrade_adoption_test.go` (extended)

**Interfaces:**
- Consumes: `queue.EnqueueEvent`, `queue.WithPriority`, `queue.AdoptUpgradeDedupKey` (Task 3); `controller.AdoptedUpgradeRefFromPR` (Task 4).
- Produces:
  - `obs.AdoptionEnqueuedTotal *prometheus.CounterVec` labels `{project, activity}` where activity is `webhook` or `sweep`.
  - `obs.AdoptionEventDroppedTotal *prometheus.CounterVec` labels `{project, reason}` where reason is one of `merged`, `closed`, `not_adoptable`, `mint_not_owed`, `repository_gone`.

- [ ] **Step 1: Add the counters (no test of their own; every producer's test asserts them)**

In `internal/obs/sweep_metrics.go`, after `MintOutcomeTotal`:

```go
// AdoptionEnqueuedTotal counts dependency-upgrade merge requests turned into a
// QUEUED adoption, by project and by which producer saw it first. The webhook is
// the fast path and the sweep is the backstop, so a healthy project's `sweep`
// rate is near zero: a sustained `sweep` rate means webhook deliveries are being
// lost, which is invisible on the mint counters (the sweep mints them either way).
var AdoptionEnqueuedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_adoption_enqueued_total",
	Help: "Dependency-upgrade merge requests enqueued for adoption, by project and producer activity (sweep|webhook).",
}, []string{"project", "activity"})

// AdoptionEventDroppedTotal counts QUEUED adoptions that never became a Task, by
// reason. It is the freshness half of design D4: a queued snapshot is a pointer
// to a live forge object, and merged/closed are the two ways that object stops
// being worth a pod. A non-zero rate is the mechanism WORKING - it is the count
// of agent pods not burned on merge requests that resolved while they waited.
var AdoptionEventDroppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_adoption_event_dropped_total",
	Help: "Queued dependency-upgrade adoptions dropped before minting, by project and reason.",
}, []string{"project", "reason"})

// adoptionActivities / adoptionDropReasons are the closed label sets of the two
// counters above, seeded for the same reason every counter here is: a CounterVec
// with no WithLabelValues call has NO series, so increase(...[1h]) is blind to
// the first increment after every pod roll.
var adoptionActivities = []string{"sweep", WebhookActivity}
var adoptionDropReasons = []string{"merged", "closed", "not_adoptable", "mint_not_owed", "repository_gone"}
```

In `SeedSweepErrorsForProject`, before the `MintOutcomeTotal` block:

```go
	enq := func(l ...string) { AdoptionEnqueuedTotal.WithLabelValues(l...) }
	seedLabels(enq, []string{project}, adoptionActivities)
	drop := func(l ...string) { AdoptionEventDroppedTotal.WithLabelValues(l...) }
	seedLabels(drop, []string{project}, adoptionDropReasons)
```

In `init()`, add both to `ctrlmetrics.Registry.MustRegister(...)`.

- [ ] **Step 2: Write the failing webhook tests**

Rewrite the marker assertions in `internal/webhook/upgrade_adoption_test.go`. Replace the `sweepRequest` helper with:

```go
// adoptionEvent reads the QueuedEvent an adoptable delivery is expected to have
// created, by its DETERMINISTIC name - the same name a sweep enqueue would
// compute, which is what makes the two collide instead of double-enqueueing.
func adoptionEvent(t *testing.T, c client.Client, project, repoName string, number int) *tatarav1.QueuedEvent {
	t.Helper()
	var qe tatarav1.QueuedEvent
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: ns,
		Name:      queue.QueuedEventName(project, queue.AdoptUpgradeDedupKey(repoName, number)),
	}, &qe)
	require.NoError(t, err, "an adoptable delivery must create a queued adoption")
	return &qe
}
```

and add:

```go
// THE FIX, RESTATED. An adoptable delivery becomes a DURABLE QUEUE ENTRY. The
// sweep-requested annotation was one shot and self-consuming: the pass it pulled
// forward computed one headroom for the whole pass, adopted up to that many
// merge requests, and CLEARED the annotation of every one it skipped. Nothing
// re-drove them, so the third merge request of a three-MR Renovate run sat
// unadopted for four hours while both its siblings were already merged.
func TestPROpened_AdoptableUpgradeMR_EnqueuesAQueuedAdoption(t *testing.T) {
	const secretVal = "whsec-a1"
	c := seedClient(t,
		adoptionProject("ad1", "ad1-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad1-scm", secretVal),
		repository("charts", "ad1", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad1", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 77))

	qe := adoptionEvent(t, c, "ad1", "charts", 77)
	require.NotNil(t, qe.Spec.Payload.AdoptedUpgrade)
	require.Equal(t, 77, qe.Spec.Payload.AdoptedUpgrade.Number)
	require.Equal(t, "renovate/cilium", qe.Spec.Payload.AdoptedUpgrade.HeadBranch)
	require.Equal(t, "sha-77", qe.Spec.Payload.AdoptedUpgrade.HeadSHA)
	require.Equal(t, "charts", qe.Spec.RepositoryRef)
	require.Equal(t, tatarav1.QueueClassNormal, qe.Spec.Class)

	// PRIORITY 2, DELIBERATELY (design D3). Priority 1 is "a human is waiting",
	// and admitPool sorts on (priority, seq) - so a twelve-MR Renovate run at
	// priority 1 would drain ahead of the next stage of every task already
	// underway, with the starvation guard reserving exactly one slot after an
	// hour of it. No human waits on a bump.
	require.Equal(t, 2, tatarav1.EffectiveQueuePriority(qe.Spec))

	require.Empty(t, allTasks(t, c, "ad1"),
		"the webhook enqueues; the leader-elected dispatcher is the only minter")
}

// THE MULTI-REPLICA BURST COLLAPSES ON THE NATURAL KEY. Five processes sharing
// only an API server: the deterministic QueuedEvent name makes four of the five
// Creates fail AlreadyExists, which EnqueueEvent treats as "not an error, not
// created" - so no sequence number is burned either.
func TestPROpened_AdoptableUpgradeMR_ConcurrentDeliveriesEnqueueOnce(t *testing.T) {
	const secretVal = "whsec-a3"
	c := seedClient(t,
		adoptionProject("ad3", "ad3-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad3-scm", secretVal),
		repository("charts", "ad3", baseRepoURL, "main"),
	)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, _ := newServer(t, c)
			postPROpened(t, h, "ad3", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 90))
		}()
	}
	wg.Wait()

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	n := 0
	for i := range qel.Items {
		if qel.Items[i].Spec.Payload.AdoptedUpgrade != nil {
			n++
		}
	}
	require.Equal(t, 1, n, "five concurrent deliveries must produce exactly one queued adoption")
}

// AN ENQUEUE FAILURE IS A 500, matching this file's MintTombstoneDeleted policy:
// the mint is still OWED, and a silent 202 discards the only fast signal this
// merge request gets. The forge redelivers within seconds.
func TestPROpened_AdoptableUpgradeMR_EnqueueFailureIs500(t *testing.T) {
	// Drive the failure by seeding a client whose Create on QueuedEvent errors;
	// reuse this package's existing failing-client helper if one exists, and add
	// a minimal interceptor client if not.
}
```

- [ ] **Step 3: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/webhook/... -run Adoptable -count=1`
Expected: FAIL - no `QueuedEvent` is created; the handler still stamps the annotation.

- [ ] **Step 4: Replace the handler arm**

In `internal/webhook/server.go`, replace lines 798-806 (the `AdoptionCandidate` branch) with:

```go
func (s *Server) handleMROpened(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	// ActorLogin, not AuthorLogin: it is the login the rest of this handler
	// treats as the merge request's author, so the identity this arm tests and
	// the identity AdoptUpgradeMR later rules on are one value.
	if controller.AdoptionCandidate(&proj, ev.HeadBranch, ev.ActorLogin) {
		s.enqueueAdoption(ctx, w, provider, &proj, ev)
		return
	}
	...
```

and replace the whole `requestRepoSweep` function (server.go:591-630) with:

```go
// enqueueAdoption turns an adoptable dependency-upgrade delivery into a DURABLE
// queue entry, which is the whole of this change.
//
// THE OLD FAST PATH WAS ONE SHOT AND SELF-CONSUMING. It stamped
// SweepRequestedAnnotation to pull the repository's issueScan slot forward, and
// the pass that ran computed one adoptHeadroom for the whole pass, adopted up to
// that many merge requests oldest-first, and cleared the annotation of every one
// it skipped. Nothing re-drove the remainder: the third merge request of a
// three-MR Renovate run was still unadopted four hours later with both siblings
// already merged and zero live upgrade lanes. The cap behaved as a rate limiter
// (2 per 4 hours per repository) rather than as a concurrency bound.
//
// THE HANDLER STILL MINTS NOTHING, AND THAT PART OF THE OLD ARGUMENT SURVIVES
// INTACT. This server runs on EVERY replica (HandlerRunnable.NeedLeaderElection()
// is false) behind a load-balancing Service, so a check-then-create in this
// function is a distributed race no in-process lock closes. What changed is
// WHERE the bound lives: the queue IS the buffer, admission is the
// leader-elected DispatcherReconciler's single serialized writer, and a
// QueuedEvent's deterministic natural key makes five concurrent deliveries
// collide at the API server on AlreadyExists.
//
// ENQUEUE IS NEVER CAPACITY-GATED. Gating the producer is precisely the defect
// being removed; the surplus waits in the queue and admits the moment a slot
// frees.
//
// A FAILURE IS A 500. Unlike the old best-effort marker - whose every failure
// cost latency and nothing else, because the ordinary sweep slot still adopted -
// a lost enqueue is a lost signal, so the caller redelivers. Same policy as
// MintTombstoneDeleted elsewhere in this file.
func (s *Server) enqueueAdoption(ctx context.Context, w http.ResponseWriter, provider string,
	proj *tatarav1.Project, ev scm.WebhookEvent) {

	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		// Not an enrolled repository, so there is nothing to adopt into and the
		// sweep would never look at it either.
		s.log.InfoContext(ctx, "mr: adoptable merge request on an unmatched repository; ignoring",
			"action", "mr_adopt_unmatched", "project", proj.Name, "issue_ref", ev.IssueRef,
			"head_branch", ev.HeadBranch)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	pr := scm.PRRef{
		Number: ev.Number, Title: ev.Title, Author: ev.ActorLogin,
		HeadSHA: ev.HeadSHA, HeadBranch: ev.HeadBranch, Body: ev.Body, Labels: ev.Labels,
		Repo: repoSlug(repo), HeadRepo: ev.HeadRepo,
	}
	payload := tatarav1.QueuedEventPayload{
		Kind:           "upgrade",
		RepositoryRef:  repo.Name,
		Provider:       provider,
		PodRepo:        repo.Name,
		AdoptedUpgrade: controller.AdoptedUpgradeRefFromPR(pr),
	}
	dedupKey := queue.AdoptUpgradeDedupKey(repo.Name, ev.Number)
	// Priority 2 is the cron/sweep tier (design D3). Priority 1 means a human is
	// waiting on a thread, and admitPool sorts (priority, seq): a twelve-MR
	// Renovate run at priority 1 would drain ahead of the next stage of every
	// task already underway. Priority 2 still admits the instant a slot frees.
	_, created, eerr := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, proj,
		tatarav1.QueueClassNormal, true, dedupKey, payload, queue.WithPriority(2))
	if eerr != nil {
		s.log.ErrorContext(ctx, "mr: enqueue adoption failed; the signal is still owed",
			"action", "mr_adopt_enqueue_failed", "error", eerr,
			"project", proj.Name, "repository", repo.Name, "number", ev.Number)
		s.reject(w, http.StatusInternalServerError, "enqueue adoption", provider, ev.Kind, ev.Action, "error")
		return
	}
	if created {
		obs.AdoptionEnqueuedTotal.WithLabelValues(proj.Name, obs.WebhookActivity).Inc()
		s.log.InfoContext(ctx, "mr: adoptable dependency merge request queued for adoption",
			"action", "mr_adopt_enqueued", "resource_id", queue.QueuedEventName(proj.Name, dedupKey),
			"project", proj.Name, "repository", repo.Name, "number", ev.Number,
			"head_branch", ev.HeadBranch, "author", ev.ActorLogin)
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}
```

- [ ] **Step 5: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/webhook/... ./internal/obs/... -race -count=1`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
mise exec -- gofmt -s -w internal/webhook internal/obs
mise exec -- golangci-lint run ./... || [ $? -eq 5 ]
git add internal/webhook internal/obs
git commit -m "feat(webhook): queue an adoptable dependency merge request instead of pulling the sweep forward"
```

---

## Task 7: The webhook keeps a queued adoption fresh

**Files:**
- Modify: `internal/webhook/mirror_refresh.go` (`handleMRSynchronize` at `:231`, `handleMRClosed` at `:301`; new `refreshQueuedAdoption` and `dropQueuedAdoption`)
- Test: `internal/webhook/upgrade_adoption_freshness_test.go` (new)

**Interfaces:**
- Consumes: `queue.QueuedEventName`, `queue.AdoptUpgradeDedupKey` (Task 3); `obs.AdoptionEventDroppedTotal` (Task 6).
- Produces: nothing consumed elsewhere.

- [ ] **Step 1: Write the failing tests**

Create `internal/webhook/upgrade_adoption_freshness_test.go`:

```go
package webhook_test

// A QUEUED ADOPTION IS A POINTER TO A LIVE FORGE OBJECT, which is exactly why
// queueing was once rejected outright (MEMORY.md 2026-08-16). These two handlers
// are the answer: Renovate force-pushes each successive bump onto the same
// branch, keeping the same number and the same merge request, and a merge
// request can merge or close entirely while its event waits behind a full pool.

// SYNCHRONIZE REFRESHES A STILL-QUEUED SNAPSHOT. Without it the dispatcher mints
// against a head SHA the engine has already replaced, and AdoptUpgradeMR clause
// (g) - which binds a refusal marker to a head SHA - rules on the wrong tree.
func TestMRSynchronize_RefreshesAStillQueuedAdoption(t *testing.T) {
	// seed: project + repo + a Queued adoption event for number 77 at sha-77
	// act:  post a synchronize delivery for 77 with head sha-78 and a new title
	// assert: the event's payload.adoptedUpgrade.headSHA is sha-78 and the title
	//         and body are the delivery's; status is still Queued
}

// AN ALREADY-ADMITTED EVENT IS LEFT ALONE. Its Task exists, its mirror exists,
// and MergeRequest.status.headSHA is the authority from that point on
// (stampMRHead, which this same handler already drives). Rewriting the spent
// event's payload would be a write with no reader.
func TestMRSynchronize_LeavesAnAdmittedAdoptionAlone(t *testing.T) {
	// seed: the same event with status.state = Admitted and a taskRef
	// act:  post a synchronize delivery with a new head sha
	// assert: payload.adoptedUpgrade.headSHA is UNCHANGED
}

// A MERGE DELETES THE QUEUED EVENT. Admitting it would burn an agent pod on a
// merge request that no longer exists to review, and nothing downstream would
// notice until the review pod read a merged MR.
func TestMRClosed_DeletesAStillQueuedAdoption(t *testing.T) {
	// seed: a Queued adoption event for 77
	// act:  post a pull_request closed delivery with merged=true for 77
	// assert: the event is GONE, and operator_adoption_event_dropped_total
	//         {reason="merged"} increased by 1
}

// A PLAIN CLOSE DELETES IT TOO, under its own reason so the two are separable
// on a dashboard: a merged bump is the happy path racing the queue, a closed one
// is a human or the engine withdrawing the proposal.
func TestMRClosed_DeletesAStillQueuedAdoptionOnAPlainClose(t *testing.T) {
	// assert: reason="closed"
}

// AN ADMITTED EVENT IS NOT DELETED. Its Task owns the merge request now and the
// stage machine converges a merged/closed MR on its own (merging already treats
// State=="merged" as done). Deleting the event here would strip the Task's
// LabelQueuedEvent accounting mid-flight.
func TestMRClosed_LeavesAnAdmittedAdoptionAlone(t *testing.T) {
	// assert: the event still exists
}

// NO QUEUED ADOPTION IS THE NORMAL CASE. Every merge request in the platform
// gets synchronize and closed deliveries; almost none of them has a queued
// adoption. A NotFound must be silent and must not change the response.
func TestMRSynchronizeAndClosed_NoQueuedAdoptionIsSilent(t *testing.T) {
	// act: post both deliveries for a repo with no adoption event at all
	// assert: both answer 202 and nothing errors
}
```

Fill each body using this file's existing helpers (`seedClient`, `newServer`, `postPROpened` and its synchronize/closed siblings, `adoptionProject`, `repository`, `secret`). Read `internal/webhook/mirror_refresh_test.go:160-240` for the exact synchronize/closed posting shape.

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/webhook/... -run 'MRSynchronize_Refreshes|MRClosed_Deletes' -count=1`
Expected: FAIL - the handlers do not touch QueuedEvents.

- [ ] **Step 3: Implement**

Add to `internal/webhook/mirror_refresh.go`:

```go
// queuedAdoption returns the STILL-QUEUED adoption event for (proj, repo,
// number), or nil when there is none or it has already been admitted.
//
// It is a single Get on the DETERMINISTIC name, not a list: QueuedEventName is a
// pure function of (project, dedupKey) and AdoptUpgradeDedupKey is a pure
// function of (repository CR name, number), both of which this handler already
// holds. Every merge request in the platform receives synchronize and closed
// deliveries and almost none has a queued adoption, so the miss path has to be
// one cheap read.
func (s *Server) queuedAdoption(ctx context.Context, proj *tatarav1.Project,
	repo *tatarav1.Repository, number int) *tatarav1.QueuedEvent {

	if repo == nil || number <= 0 {
		return nil
	}
	name := queue.QueuedEventName(proj.Name, queue.AdoptUpgradeDedupKey(repo.Name, number))
	var qe tatarav1.QueuedEvent
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, name), &qe); err != nil {
		return nil
	}
	if qe.Spec.Payload.AdoptedUpgrade == nil {
		return nil
	}
	// Admitted work is the Task's and the mirror's from here on. The empty state
	// is a QueuedEvent whose post-Create status update was lost, which is still
	// effectively Queued (isQueued's own rule).
	if qe.Status.State != "" && qe.Status.State != tatarav1.QueueStateQueued {
		return nil
	}
	return &qe
}

// refreshQueuedAdoption re-snapshots a still-queued adoption from a synchronize
// delivery. Renovate FORCE-PUSHES each successive bump onto the same branch,
// keeping the same number and the same merge request, so an event that waits
// behind a full pool is pointing at a tree that no longer exists - and
// AdoptUpgradeMR clause (g) binds its refusal marker to a head SHA, so minting
// against a stale one rules on the wrong tree.
func (s *Server) refreshQueuedAdoption(ctx context.Context, proj *tatarav1.Project,
	repo *tatarav1.Repository, ev scm.WebhookEvent) {

	qe := s.queuedAdoption(ctx, proj, repo, ev.Number)
	if qe == nil {
		return
	}
	qe.Spec.Payload.AdoptedUpgrade.HeadSHA = ev.HeadSHA
	if ev.Title != "" {
		qe.Spec.Payload.AdoptedUpgrade.Title = ev.Title
	}
	if ev.Body != "" {
		qe.Spec.Payload.AdoptedUpgrade.Body = ev.Body
	}
	if err := s.cfg.Client.Update(ctx, qe); err != nil {
		// BEST EFFORT, and unlike the enqueue this one really is: the event is
		// still queued, the dispatcher still adopts, and the worst case is a mint
		// at the previous head - which the merge corridor's own head pin catches.
		s.log.ErrorContext(ctx, "mr: refresh of a queued adoption failed; the stale snapshot still mints",
			"action", "mr_adopt_refresh_failed", "error", err,
			"project", proj.Name, "repository", repo.Name, "number", ev.Number)
		return
	}
	s.log.InfoContext(ctx, "mr: refreshed a queued dependency-upgrade adoption",
		"action", "mr_adopt_refreshed", "resource_id", qe.Name, "project", proj.Name,
		"repository", repo.Name, "number", ev.Number, "head_sha", ev.HeadSHA)
}

// dropQueuedAdoption deletes a still-queued adoption for a merge request that
// merged or closed while it waited. Admitting it would spend an agent pod on a
// merge request there is nothing left to review.
func (s *Server) dropQueuedAdoption(ctx context.Context, proj *tatarav1.Project,
	repo *tatarav1.Repository, number int, reason string) {

	qe := s.queuedAdoption(ctx, proj, repo, number)
	if qe == nil {
		return
	}
	if err := s.cfg.Client.Delete(ctx, qe); err != nil && !apierrors.IsNotFound(err) {
		s.log.ErrorContext(ctx, "mr: dropping a queued adoption failed; the dispatcher's own predicate re-check is the backstop",
			"action", "mr_adopt_drop_failed", "error", err,
			"project", proj.Name, "repository", repo.Name, "number", number)
		return
	}
	obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, reason).Inc()
	s.log.InfoContext(ctx, "mr: dropped a queued dependency-upgrade adoption",
		"action", "mr_adopt_dropped", "resource_id", qe.Name, "project", proj.Name,
		"repository", repo.Name, "number", number, "reason", reason)
}
```

Call them. In `handleMRSynchronize`, immediately before `s.accept(...)`:

```go
	s.refreshQueuedAdoption(ctx, &proj, repo, ev)
```

In `handleMRClosed`, immediately after `state` is computed and the mirror stamped:

```go
	// `state` is already merged|closed and is exactly the drop reason: a merged
	// bump is the happy path racing the queue, a closed one is a withdrawal.
	s.dropQueuedAdoption(ctx, &proj, repo, ev.Number, state)
```

- [ ] **Step 4: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/webhook/... -race -count=1`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
mise exec -- gofmt -s -w internal/webhook
mise exec -- golangci-lint run ./internal/webhook/... || [ $? -eq 5 ]
git add internal/webhook
git commit -m "feat(webhook): refresh a queued adoption on synchronize and drop it on close"
```

---

## Task 8: The sweep enqueues and loses its adoption headroom

**Files:**
- Modify: `internal/controller/sweep.go` - delete `SweepSkipUpgradeHeadroom` (`:155-161`), delete the `adoptHeadroom` block (`:1375-1399`), drop the `adoptHeadroom *int` parameter (`:1715`) and its argument (`:1422`), delete the adoption window (`:1762-1792`), rewrite the `PRAdoptUpgrade` arm (`:1806-1828`)
- Modify: `internal/obs/sweep_metrics.go` - drop `"upgrade_headroom_bound"` from `sweepSkipReasons`, drop `"count_upgrade_lanes"` from `sweepSeedReasons`, add `"enqueue_adopt_upgrade"`
- Modify: `internal/obs/sweep_metrics_test.go:104` - `2 * 9` becomes `2 * 8`
- Modify: `internal/obs/sweep_skip_alert_test.go:12-34` - drop `upgradeHeadroomSkipReason` and its `steadyStateSkipReasons` membership
- Modify: `charts/tatara-operator/templates/prometheusrule.yaml:218,223` - drop the `reason!="upgrade_headroom_bound"` matcher and the paragraph of description prose that explains it
- Delete: `internal/controller/sweep_adopt_headroom_test.go` (its entire subject is the deleted window)
- Modify: `internal/controller/sweep_adopt_upgrade_test.go:290-320` (the headroom test)

**Interfaces:**
- Consumes: `queue.EnqueueEvent`, `queue.AdoptUpgradeDedupKey` (Task 3); `AdoptedUpgradeRefFromPR` (Task 4); `obs.AdoptionEnqueuedTotal` (Task 6).
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Add to `internal/controller/sweep_adopt_upgrade_test.go`:

```go
// THE SWEEP IS A BACKSTOP NOW, NOT A PACER. It enqueues under the SAME dedup key
// the webhook uses, so a merge request the webhook already queued collides on
// AlreadyExists and burns no sequence number - and one whose delivery was lost
// while the operator was down is picked up on the next pass.
func TestSweepPRs_AdoptableMergeRequestIsEnqueuedNotMinted(t *testing.T) {
	// seed: project armed for adoption, repo, three adoptable PRs (41, 42, 43)
	//       and NO upgrade cron capacity at all
	// act:  one sweepPRs pass
	// assert: three QueuedEvents exist, one per number, each with a
	//         payload.adoptedUpgrade and priority 2
	// assert: ZERO Tasks were created by the pass - the dispatcher is the minter
	// assert: maxOpenUpgrades did NOT bound the count (design D1): all three are
	//         queued even though the cron's cap is 1
}

// A SECOND PASS OVER THE SAME MERGE REQUEST IS A NO-OP. The natural key is
// deterministic, so the Create collides and EnqueueEvent reports created=false.
func TestSweepPRs_ASecondPassDoesNotDoubleEnqueue(t *testing.T) {
	// act: two sweepPRs passes over the same PR
	// assert: exactly one QueuedEvent, and operator_adoption_enqueued_total
	//         {activity="sweep"} increased by exactly 1
}

// A MERGE REQUEST A LIVE TASK ALREADY OWNS PRODUCES NO SECOND EVENT. ClassifyPR
// sends it to PRIgnore on the mirror's live controller owner, exactly as before.
func TestSweepPRs_AnAlreadyAdoptedMergeRequestEnqueuesNothing(t *testing.T) {
	// seed: seedAdoptedLane (moved here from the deleted headroom test file)
	// assert: no QueuedEvent for that number
}
```

Move `seedAdoptedLane` and `enginePR` out of `sweep_adopt_headroom_test.go` into `sweep_adopt_upgrade_test.go` before deleting the former; drop `headroomProject` (nothing needs a lane cap any more).

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/controller/... -run 'SweepPRs_Adoptable|SweepPRs_ASecond' -count=1`
Expected: FAIL - the arm mints instead of enqueueing.

- [ ] **Step 3: Delete the headroom**

In `internal/controller/sweep.go`:

- Delete the `SweepSkipUpgradeHeadroom` constant and its whole doc comment (`:155-161`).
- Delete the `adoptHeadroom := 0` block and the comment above it (`:1375-1399`).
- Change the `sweepPRs` call (`:1420-1422`) to drop `&adoptHeadroom`.
- Change `sweepPRs`'s signature (`:1712-1715`) to drop `adoptHeadroom *int`.
- Delete the ADOPTION WINDOW block (`:1762-1792`: `adoptableNums`, the sort, the truncation and the `adoptable` map) and its comment.

- [ ] **Step 4: Rewrite the arm**

Replace the whole `case PRAdoptUpgrade:` body with:

```go
		case PRAdoptUpgrade:
			// THE SWEEP ENQUEUES, IT NO LONGER MINTS - and the cap it used to
			// enforce here is gone with it. adoptHeadroom was recomputed once per
			// pass and a pass is gated on the issueScan cron, so a merge request
			// the pass had no lane for was skipped upgrade_headroom_bound and waited
			// up to four hours for the next slot even when every lane had freed
			// minutes later. Adopted work is now an ordinary queue citizen bounded
			// by QueueCapacity and MaxLivePods (design D1), and admission re-runs on
			// every Task write, so the surplus admits the moment a slot frees.
			//
			// SAME DEDUP KEY AS THE WEBHOOK, deliberately: the deterministic
			// QueuedEvent name makes a duplicate collide on AlreadyExists at the API
			// server, which EnqueueEvent reports as created=false and which burns no
			// sequence number. That is what makes this a true backstop for a
			// delivery lost while the operator was down, rather than a second
			// producer racing the first.
			payload := tatarav1alpha1.QueuedEventPayload{
				Kind:           adoptedUpgradeKind,
				RepositoryRef:  repo.Name,
				Provider:       providerOf(proj),
				PodRepo:        repo.Name,
				AdoptedUpgrade: AdoptedUpgradeRefFromPR(pr),
			}
			dedupKey := queue.AdoptUpgradeDedupKey(repo.Name, pr.Number)
			_, created, eerr := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj,
				tatarav1alpha1.QueueClassNormal, true, dedupKey, payload, queue.WithPriority(2))
			if eerr != nil {
				fail("enqueue_adopt_upgrade", eerr, "repo", repo.Name, "number", pr.Number)
				continue
			}
			if created {
				obs.AdoptionEnqueuedTotal.WithLabelValues(proj.Name, SweepActivity).Inc()
				l.Info("sweep: queued a dependency upgrade merge request for adoption",
					"action", "sweep_enqueue_adopt_upgrade",
					"resource_id", queue.QueuedEventName(proj.Name, dedupKey), "activity", activity,
					"repo", repo.Name, "number", pr.Number, "head_branch", pr.HeadBranch,
					"author", pr.Author)
			}
```

Add `"github.com/szymonrychu/tatara-operator/internal/queue"` to `sweep.go`'s imports if absent, and confirm `slices` is still used elsewhere in the file (the deleted window was a user of `slices.Sort`); drop the import if it is now unused.

- [ ] **Step 5: Retire the dead metric vocabulary**

`internal/obs/sweep_metrics.go`:
- remove `"upgrade_headroom_bound",` from `sweepSkipReasons`
- remove `"count_upgrade_lanes"` from `sweepSeedReasons` and add `"enqueue_adopt_upgrade"` in its place, updating the trailing comment:

```go
	// The adoption arm no longer mints and no longer counts lanes - it ENQUEUES,
	// and the dispatcher mints under the general pool. `adopt_upgrade_mr` remains
	// for the admit-time mint's own failures; `enqueue_adopt_upgrade` is the
	// sweep's enqueue failing, which is the one way a merge request the operator
	// CAN see still ends a pass with no queue entry.
	"adopt_upgrade_mr", "enqueue_adopt_upgrade",
```

`internal/obs/sweep_metrics_test.go:104`: `const wantPerProject = 2 * 8`, and update the comment above it from "The 9th member is upgrade_headroom_bound..." to name the eight surviving members' shape instead.

`internal/obs/sweep_skip_alert_test.go`: delete the `upgradeHeadroomSkipReason` const and remove it from `steadyStateSkipReasons` (leaving `mintBudgetSkipReason` alone), and rewrite the two doc paragraphs that explain the upgrade deferral - the deferral no longer exists.

`charts/tatara-operator/templates/prometheusrule.yaml:218`: drop `reason!="upgrade_headroom_bound",` from the `TataraSweepSkipPersistent` expr. Line 223: delete the sentences of the `description` that discuss `upgrade_headroom_bound`, keeping the `mint_budget_bound` explanation intact.

- [ ] **Step 6: Delete the obsolete test file and fix the survivor**

```bash
git rm internal/controller/sweep_adopt_headroom_test.go
```

after moving `seedAdoptedLane` / `enginePR` into `sweep_adopt_upgrade_test.go` (Step 1). In `sweep_adopt_upgrade_test.go`, delete the headroom-skip test at lines ~290-320 (the one asserting four `SweepSkipUpgradeHeadroom` increments).

- [ ] **Step 7: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/controller/... ./internal/obs/... -race -count=1`
Run: `mise exec -- make chart-lint`
Expected: PASS. `TestSweepSkipReasonsMatchSweepConstants`, `TestSweepSeedReasonsCoverEveryFailSite` and `TestSweepSkipPersistentAlertExcludesTheSteadyStateDeferrals` are the three that pin this step; all three must be green.

- [ ] **Step 8: Lint and commit**

```bash
mise exec -- gofmt -s -w internal
mise exec -- golangci-lint run ./... || [ $? -eq 5 ]
git add -A internal charts
git commit -m "refactor(sweep): enqueue adoptable dependency merge requests and retire the adoption headroom"
```

---

## Task 9: The upgrade cron counts only its own work

**Files:**
- Modify: `internal/controller/projectscan.go:1228-1277` (`openUpgradeLaneCount`)
- Test: `internal/controller/projectscan_upgrade_lanes_test.go` (new)

**Interfaces:**
- Consumes: `tatarav1alpha1.LabelUpgradeOrigin` / `UpgradeOriginAdopted` (Task 2); `queue.IsAdoptedUpgradeMint` (Task 3).
- Produces: `func isAdoptedUpgradeTask(t *tatarav1alpha1.Task) bool` (unexported, `projectscan.go`).

- [ ] **Step 1: Write the failing tests**

Create `internal/controller/projectscan_upgrade_lanes_test.go`:

```go
package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// upgradeTask builds a live upgrade Task with the given labels and source.
func upgradeTask(t *testing.T, name string, labels map[string]string, src *tatarav1alpha1.TaskSource) *tatarav1alpha1.Task {
	t.Helper()
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "lanes-proj", Kind: "upgrade", Goal: "g", Source: src,
		},
	}
}

// THE CRON MUST NOT FALL SILENT BEHIND A RENOVATE BACKLOG (design D2). Adopted
// work is bounded by the general pool, not by maxOpenUpgrades - so counting it
// here would read a draining backlog as "lanes full" and stop the agent that
// proactively hunts for bumps, for as long as the backlog lasted, with no error
// and no log anywhere.
func TestOpenUpgradeLaneCount_IgnoresAdoptedTasks(t *testing.T) {
	// seed: one cron-minted upgrade Task (no labels, no Source) and three
	//       adopted ones (LabelUpgradeOrigin=adopted)
	// assert: openUpgradeLaneCount == 1
}

// ...AND IGNORES ADOPTED EVENTS THAT HAVE NOT MINTED YET, or the same
// suppression happens one step earlier: the count's QueuedEvent half exists
// precisely to catch work already committed to.
func TestOpenUpgradeLaneCount_IgnoresQueuedAdoptions(t *testing.T) {
	// seed: one cron-minted upgrade QueuedEvent (no adoptedUpgrade payload) and
	//       three adoption events
	// assert: openUpgradeLaneCount == 1
}

// BACKWARD COMPATIBILITY, AND IT IS NOT HYPOTHETICAL. Adopted Tasks minted by
// the PREVIOUS operator carry no upgrade-origin label, and on a live cluster
// several are in flight at upgrade time. Without the structural fallback the
// cron would be suppressed by them for their whole remaining lifetime - which is
// exactly the D2 failure this task exists to prevent, arriving through the door
// nobody watched.
//
// The fallback is exact, not a heuristic: createUpgradeTask sets NO Source at
// all, and MintAdoptedUpgradeTask always sets one with IsPR=true and a number.
func TestOpenUpgradeLaneCount_IgnoresPreUpgradeAdoptedTasks(t *testing.T) {
	// seed: one cron-minted upgrade Task (Source nil) and one adopted-shaped Task
	//       with NO upgrade-origin label but Source{IsPR:true, Number:41}
	// assert: openUpgradeLaneCount == 1
}

// THE CRON'S OWN WORK STILL COUNTS, both halves, or maxOpenUpgrades stops
// bounding anything at all.
func TestOpenUpgradeLaneCount_StillCountsCronWork(t *testing.T) {
	// seed: two cron Tasks and one cron QueuedEvent
	// assert: openUpgradeLaneCount == 3
}
```

Fill the bodies with this package's existing fake-client helper (grep `newMirrorClient` / `sweepProject` in `internal/controller/*_test.go`).

- [ ] **Step 2: Run the tests, confirm they fail**

Run: `mise exec -- go test ./internal/controller/... -run OpenUpgradeLaneCount -count=1`
Expected: FAIL - adopted work is counted.

- [ ] **Step 3: Implement**

In `internal/controller/projectscan.go`, add above `openUpgradeLaneCount`:

```go
// isAdoptedUpgradeTask reports whether an upgrade Task was born from an EXISTING
// third-party merge request rather than from the cron.
//
// TWO TESTS, AND THE SECOND IS THE UPGRADE PATH. The label is the explicit
// marker MintAdoptedUpgradeTask stamps from now on. The Source shape is the
// structural fallback for every adopted Task minted BEFORE this label existed:
// on a live cluster several are in flight at upgrade time, and without the
// fallback the cron would be suppressed by them for their whole remaining
// lifetime - the exact D2 failure the label exists to prevent, arriving through
// the one door a label cannot cover. It is exact rather than heuristic:
// createUpgradeTask sets NO Source at all, and every adopted mint sets one with
// IsPR true and a merge request number.
func isAdoptedUpgradeTask(t *tatarav1alpha1.Task) bool {
	if t.Labels[tatarav1alpha1.LabelUpgradeOrigin] == tatarav1alpha1.UpgradeOriginAdopted {
		return true
	}
	return t.Spec.Source != nil && t.Spec.Source.IsPR && t.Spec.Source.Number > 0
}
```

In `openUpgradeLaneCount`'s Task loop, after the kind filter:

```go
		if isAdoptedUpgradeTask(t) {
			continue // adopted work is bounded by the general pool (design D2)
		}
```

In its QueuedEvent loop, after the kind filter:

```go
		if queue.IsAdoptedUpgradeMint(q) {
			continue // the not-yet-minted half of the same exclusion
		}
```

Extend the function's doc comment:

```
// IT COUNTS THE CRON'S OWN WORK ONLY (design D2). maxOpenUpgrades governs the
// CRON - the agent that proactively hunts for dependency bumps - and nothing
// else. Adopted third-party merge requests are ordinary queue citizens bounded
// by QueueCapacity and MaxLivePods, and counting them here would make a draining
// Renovate backlog read as "lanes full": the cron would fall silent for as long
// as the backlog lasted, with no error, no log and no counter.
```

- [ ] **Step 4: Run the tests, confirm they pass**

Run: `mise exec -- go test ./internal/controller/... -race -count=1`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
mise exec -- gofmt -s -w internal/controller
mise exec -- golangci-lint run ./internal/controller/... || [ $? -eq 5 ]
git add internal/controller
git commit -m "fix(scan): keep the upgrade cron counting only its own lanes"
```

---

## Task 10: Envtest for the sequence that actually broke

**Files:**
- Test: `internal/controller/queue_adopt_envtest_test.go` (new)

**Interfaces:**
- Consumes: everything from Tasks 2-9.
- Produces: nothing.

This is the whole point of the change, expressed as one test: three adoptions against a saturated pool, and the third admits on the FIRST one's terminal write, not on a sweep tick.

- [ ] **Step 1: Write the failing test**

Create `internal/controller/queue_adopt_envtest_test.go`, using the envtest scaffolding in `internal/controller/suite_test.go` (`k8sClient`, `testNS`, `timeout`, `interval`):

```go
package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// THE MEASURED DEFECT, AS A TEST.
//
// 2026-08-16 on szymonrychu/charts: three Renovate merge requests arrived within
// 30 seconds, the pass that ran adopted !1024 and !1025 and skipped !1026
// upgrade_headroom_bound, and the annotation that pulled the pass forward was
// cleared by that same pass. Six minutes later 1024 was done and the lanes were
// empty; twenty minutes after that 1024 and 1025 were MERGED and !1026 was still
// untouched, waiting for a 0 */4 * * * cron.
//
// The immediacy needs NO new trigger. DispatcherReconciler already watches Task
// (mapTaskToQE) and every doReconcile runs a full admit pass over the whole
// queue, so a Task's terminal write already re-evaluates admission. What was
// missing was never the trigger - it was the work being in the queue at all.
func TestDispatcher_ThirdQueuedAdoptionAdmitsOnTheFirstTaskGoingTerminal(t *testing.T) {
	ctx := context.Background()

	// A project with room for exactly ONE agent, so the pool is saturated by the
	// first admission and the second and third must wait.
	//   - create the Project (maxConcurrentAgents: 1) and one Repository
	//   - create three adoption QueuedEvents, seq 1..3, priority 2, for merge
	//     requests 41, 42 and 43
	//   - run the dispatcher's admit pass
	//
	// ASSERT round 1: exactly ONE event is Admitted (the lowest seq), and the
	// other two are still Queued. Depth is 2.
	//
	// ACT: drive the admitted event's Task terminal (status.state = done) and run
	// admit again.
	//
	// ASSERT round 2: the SECOND event is now Admitted. Its Task exists under
	// AdoptedUpgradeTaskName(proj, repo, 42). No sweep ran, no cron fired, and no
	// SweepRequestedAnnotation was involved at any point.
	//
	// ACT: drive that one terminal too, admit again.
	//
	// ASSERT round 3: the THIRD event is Admitted and its Task exists. This is the
	// assertion the old shape could not satisfy at any speed.
	//
	// ASSERT ordering: the three Tasks were minted in seq order (41, 42, 43),
	// because admitPool sorts on (priority, seq) and all three share priority 2.
	_ = ctx
	_ = time.Second
	_ = types.NamespacedName{}
	_ = tatarav1alpha1.QueueStateAdmitted
	t.Fatal("implement me")
}

// PRIORITY 2 DOES NOT LEAPFROG WORK ALREADY UNDERWAY (design D3). A twelve-MR
// Renovate run at priority 1 would drain ahead of the next stage of every task
// in flight, and the one-hour starvation guard would reserve exactly one slot
// after an hour of that.
func TestDispatcher_QueuedAdoptionsDoNotOutrankAnInFlightTaskTicket(t *testing.T) {
	// seed: one priority-2 admission ticket for a live Task (seq 10) and one
	//       priority-2 adoption (seq 11), pool capacity 1
	// assert: the TICKET admits first - equal priority, lower seq - so a bump
	//         never takes the slot a half-done task's next pod needs
	t.Fatal("implement me")
}
```

Both bodies must be written out in full by the implementer; the comments above are the assertion contract, not a substitute for the code. Model the Project/Repository/QueuedEvent construction on `adoptionEvent` from Task 5 and on the envtest fixtures already in `internal/controller/project_controller_setup_test.go`.

- [ ] **Step 2: Run the test, confirm it fails**

Run: `KUBEBUILDER_ASSETS="$(mise exec -- go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21 use 1.33.0 -p path)" mise exec -- go test ./internal/controller/... -run ThirdQueuedAdoption -count=1`
Expected: FAIL on the `t.Fatal("implement me")`, then FAIL on real assertions until the bodies are written.

- [ ] **Step 3: Write the bodies until it passes**

No production code should be needed. If a body cannot be made to pass, that is a defect in Tasks 5-9, not in this test - fix the production code, do not weaken the assertion.

- [ ] **Step 4: Run the full suite**

Run: `mise exec -- make test`
Expected: PASS across every package.

- [ ] **Step 5: Commit**

```bash
git add internal/controller
git commit -m "test(queue): pin that a queued adoption admits on a task going terminal"
```

---

## Task 11: Delete the retired fast path, record the reversal

**Files:**
- Modify: `internal/webhook/server.go` - delete the `requestRepoSweep` remnants if Task 6 left any, and the paragraphs of `handleMROpened`'s doc comment that describe the annotation mechanism
- Modify: `internal/controller/projectscan.go:463-490` - delete the `SweepRequestedAnnotation` forward half of `reposDueForScan`
- Modify: `api/v1alpha1/annotations.go:8-32` - delete `SweepRequestedAnnotation` and its doc block
- Modify: `internal/webhook/upgrade_adoption_test.go` and any `projectscan` test asserting the marker - delete those cases
- Modify: `MEMORY.md`, `ROADMAP.md`

**Interfaces:** none.

**Scope note for the reviewer:** this task is separable. If you would rather keep `SweepRequestedAnnotation` as a general "sweep this repo sooner" mechanism, drop steps 2-4 and keep step 5. It is last precisely so that decision costs nothing to reverse. The argument FOR deleting it: `grep` over non-test Go shows `requestRepoSweep` at `server.go:803` was its ONLY writer, and `reposDueForScan` its only reader, so after Task 6 it is a marker no producer stamps - dead code, which `CLAUDE.md` hard rule 4 forbids leaving behind.

- [ ] **Step 1: Confirm there is no writer left**

Run: `grep -rn "SweepRequestedAnnotation" --include "*.go" .`
Expected: only `annotations.go` (the const), `projectscan.go` (the reader) and test files. If any non-test writer remains, STOP and re-do Task 6.

- [ ] **Step 2: Write the failing test**

Add to whichever `projectscan` test file covers `reposDueForScan` (grep for it):

```go
// THE PULLED-FORWARD SWEEP SLOT IS GONE WITH ITS ONLY WRITER. An adoptable
// delivery is a durable QueuedEvent now, so nothing stamps the marker and a
// repository's issueScan slot is governed by its cron alone. A Repository CR
// still carrying a stale annotation from the previous release must NOT be
// treated as due: that would turn the 30s project reconcile into a 30s forge
// listing loop for as long as the annotation survived.
func TestReposDueForScan_IgnoresAStaleSweepRequestedAnnotation(t *testing.T) {
	// seed: a repo whose LastIssueScan is fresh (not due) and which carries
	//       "tatara.dev/sweep-requested" set to now
	// assert: it is NOT due
}
```

- [ ] **Step 3: Run it, confirm it fails**

Run: `mise exec -- go test ./internal/controller/... -run ReposDueForScan_Ignores -count=1`
Expected: FAIL - the annotation still makes the repo due.

- [ ] **Step 4: Delete the mechanism**

Delete the `SweepRequestedAnnotation` branch from `reposDueForScan` (`projectscan.go:463-490`), the constant and its doc block (`annotations.go:8-32`), and every test asserting the marker (`sweepRequest` helper and its callers in `internal/webhook/upgrade_adoption_test.go`, plus any `projectscan` cases). Keep the new test from Step 2 - it is the regression guard for a live cluster whose Repository CRs still carry the annotation.

Trim `handleMROpened`'s doc comment: the paragraphs "SO WHY NOT JUST MINT IT HERE?" and "THE LEADER'S SWEEP ALREADY HAS ALL OF IT" describe a mechanism that no longer exists. Replace them with a short block that keeps the two facts that are still true (this handler cannot hold a budget; the candidate is a shape, not a verdict) and points at `enqueueAdoption` for the rest.

- [ ] **Step 5: Record the reversal**

Append to `MEMORY.md`:

```
- 2026-08-16 (queued adoption REVERSES the 2026-08-16 entry above, and here is what changed) **The `SweepRequestedAnnotation` fast path fixed the wrong half: it made the SIGNAL prompt and left the CAP a rate limiter.** Measured on `szymonrychu/charts`: three Renovate merge requests at 06:35, the pulled-forward pass adopted !1024 and !1025 and skipped !1026 `upgrade_headroom_bound`, and that same pass CLEARED the annotation it had just consumed. At 07:00 !1026 was still unadopted with zero live upgrade lanes and both siblings already merged. `adoptHeadroom` is computed once per pass and a pass is gated on the `issueScan` cron, so `maxOpenUpgrades` behaved as 2-per-4-hours-per-repository rather than as a concurrency bound. **The entry above rejected "QueuedEvent + dispatcher" on ONE objection and that objection is now answered structurally**: "an adoption event is a POINTER to a live forge object" is true, so the webhook now REFRESHES a still-`Queued` event on `synchronize` (Renovate force-pushes each bump onto the same branch, keeping the number and the merge request) and DELETES one on `closed`/`merged`, and the dispatcher re-runs `AdoptUpgradeMR` against a freshly-read mirror and live owner before it mints. Three things that were NOT obvious. **`MintAdoptedUpgradeTask` never called `AdoptUpgradeMR` itself** - the predicate ran one frame up in `ClassifyPR`, so a dispatcher calling the mint directly would have adopted with no predicate at all; the re-check is not belt-and-braces, it is the only check on that path. **`scm.WebhookEvent` had no `HeadRepo`**, which the 2026-08-16 entry records as deliberately deleted along with the first attempt - so the fork guard (clause d, which fails CLOSED on empty) could not be evaluated from a delivery, and it had to come back on both parsers. **The minted Task must carry `LabelDedupKey`** as well as `LabelQueuedEvent` and `LabelMintedBy`: `dedupExists` checks live Tasks too, and without it a redelivered `pull_request.opened` enqueues a second event once the first is collected. `maxOpenUpgrades` survives, governing the CRON alone, with `openUpgradeLaneCount` excluding adopted work by label AND by `Source.IsPR` shape - the second test is for adopted Tasks minted before the label existed, which on a live cluster are in flight at upgrade time and would otherwise silence the cron for their whole remaining life. Consequence to expect on `infrastructure` (`maxConcurrentAgents: 7`, `maxLivePods: 6`): a Renovate batch may now occupy up to six concurrent pods where it occupied two, competing with issue and review work for the same pool.
```

Append to `ROADMAP.md` under the appropriate phase heading (or add one) a single completed line naming this change and the retired `upgrade_headroom_bound` series, so a dashboard owner knows the series went away deliberately.

- [ ] **Step 6: Full verification**

```bash
mise exec -- make generate manifests
git diff --exit-code   # generated output must already be committed
mise exec -- make lint
mise exec -- make test
mise exec -- make chart-lint
mise exec -- make rbac-check
```

Expected: all clean. `git diff --exit-code` proving no generated drift is the specific check that catches a forgotten `make manifests` after Task 2.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "chore: retire the pulled-forward sweep marker and record the queued-adoption reversal"
```

---

## Self-review against the spec

- **D1 (general pool, not a per-kind cap)** - Task 8 deletes `adoptHeadroom`; Task 5 admits through the existing `admitPool` capacity and `liveRoomFor` ceiling with no new gate. Covered.
- **D2 (`maxOpenUpgrades` keeps governing the cron)** - Task 2 adds the label, Task 4 stamps it, Task 9 splits the count on both halves plus a compatibility fallback. Covered.
- **D3 (priority 2)** - asserted in Task 6's webhook test, set in Task 8's sweep arm, and pinned against ticket ordering in Task 10's second test. Covered.
- **D4 (webhook keeps events fresh)** - Task 7, all six cases including the already-admitted no-op. Covered.
- **Components 1-6** - webhook (Tasks 6, 7), API (Task 2), dispatcher (Task 5), sweep (Task 8), projectscan (Task 9), upgrade_adopt (Task 4). Covered.
- **Error handling** - 500 on enqueue failure (Task 6); the "predicates still apply at mint time" claim is made TRUE by Task 5's explicit re-check rather than assumed (finding f); enqueue is never capacity-gated (Task 8's arm has no budget call); Project owner reference is untouched, so a deleted project still collects its events.
- **Observability** - `MintOutcomeTotal{kind="upgrade"}` keeps counting through `createTaskRaceSafe`, unchanged and now incremented from the dispatcher (verified: `intake.go:417` keys on `task.Spec.Kind`, which is `upgrade`); `upgrade_headroom_bound` loses its producer, its seed entry, its chart exclusion and its two obs tests (Task 8); two new counters, both seeded (Task 6).
- **Testing** - enqueue predicate table tests (Task 6), webhook/sweep dedup collision (Tasks 6 and 8), the saturated-pool envtest (Task 10), synchronize/closed including the admitted case (Task 7), `openUpgradeLaneCount` regression (Task 9), and "a merge request a live Task owns produces no second event" (Task 8).
- **Out of scope, and honoured** - no change to `AdoptUpgradeMR`/`ClassifyPR`'s adoptability rules (Task 5 CALLS the predicate, it does not edit it); no change to agent behaviour post-adoption; no change to the cron schedule or the `maxOpenUpgrades` value; the only alert change is deleting a matcher for a reason with no producer left.
