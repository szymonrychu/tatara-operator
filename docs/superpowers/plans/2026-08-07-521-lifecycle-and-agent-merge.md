# #521 Lifecycle and Agent Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse tatara's 16-stage Task machine into an 8-state model with orthogonal `parkReason` and liveness, fold the `clarify` agent kind into `implement` behind an extended server-side approval gate, and migrate the 118 live Tasks in one shot.

**Architecture:** `status.stage`/`status.stageReason` are DELETED and replaced by `status.state` (8 closed values), `status.parkReason` (closed enum, `""` == not parked), `status.parkedAt`, `status.parkedFromState`. Parked stops being a stage and becomes a flag; conversing stops being a stage and becomes a per-state liveness property. The `clarify` agent kind is deleted and its three decisions become `action` values on the implement outcome, gated by an extended citation check that additionally binds a declared `approvingMaintainer` and pins the plan-note hash. A one-shot migrator runs before `mgr.Start()` and refuses to start the manager if it cannot finish.

**Tech Stack:** Go 1.26.3, controller-runtime, controller-gen v0.18.0, envtest 1.33.0, Helm 4.2.1, mise, pre-commit. Python 3 for the tatara-agent-skills validators.

---

## Global Constraints

Copied verbatim from the governing `CLAUDE.md` files and the reconciled design. Every task's requirements implicitly include this section.

- **KISS, always.** "Three similar lines is better than a premature abstraction."
- **NEVER introduce tech-debt.** If a thing is complex, call it out in `MEMORY.md` with the rationale. Never defer cleanup to "later".
- **Finish the whole scope.** No follow-ups, no phase 2, no minimal-for-now. Genuinely out-of-scope adjacents are listed at the end of this plan, never silently dropped.
- **Observability is mandatory.** JSON logs via `log/slog`; every business action at INFO with structured fields (`action`, `resource_id`, ...); Prometheus counters/histograms/gauges for anything that counts, times out, or can fail.
- **Charts are cluster-agnostic.** No baked `imagePullSecrets`, ingress host/class, storage class, node affinity, or secret names. Cluster specifics live only in `tatara-helmfile`.
- **No plain ENVs or lists in `values.yaml`.** camelCase scalar -> kebab-case ConfigMap/Secret key -> workload via `envFrom`; list-shaped data renders into a templated ConfigMap.
- **Agents never merge PRs.** Merge is an operator action. Auto-merge is never armed.
- **semver push-CD.** Every merged PR carries a literal `semver:major|minor|patch` label; the pipeline cuts the tag from it. Never hand-edit a deploy pin; never re-run a green release job.
- **Writing conventions.** No em dashes, smart quotes, arrows, or decorative Unicode. Plain hyphens and straight quotes. No docstrings/comments on code not being changed.
- **Branch flow.** Worktree off `main` -> develop -> merge to that repo's `main` -> cleanup. Build and deploy from `main` only.
- **Legality vs authorisation stays separated verbatim.** `internal/stage` owns the transition table and park legality and does NO approval reasoning. `restapi.verifyApprovalScope` owns authorisation. Nothing in this plan moves that line.
- **Contract version goes 3 -> 4** in exactly three repos, in one landing: `tatara-operator/internal/agent/session.go:67`, `tatara-cli/internal/mcp/contract.go:14`, `tatara-claude-code-wrapper/internal/version/version.go:20`.

---

## P0. Read this before touching anything

### P0.1 The local checkouts are STALE. Work from `origin/main`.

Measured on 2026-08-07 in `/Users/szymonri/Documents/tatara-new/code`:

| repo | local HEAD behind `origin/main` by | what the drift changes |
|---|---|---|
| `tatara-operator` | 2 commits (`a87d40d`, `ebb5352`) | `intake.go` +10, `takeover_mint.go` +4, `restapi/outcome.go` +4, new `ownership_redeliver.go` (86 lines), `docbatch.go` +5 |
| `tatara-cli` | 2 commits (`fcad78e`, `11b28cc`) | **`ContractVersion` is 3 on `origin/main`, 2 locally.** `clarifyOutcomeSchema` already carries `approval_citations` |
| `tatara-claude-code-wrapper` | 7 commits | **`ContractVersion` is 3 on `origin/main`, 2 locally.** |
| `tatara-agent-skills` | 2 commits (`074b220`, `3a01267`) | **The whole agent-judged approval gate rewrite of the clarify skills landed there.** `plugin.json` is 1.8.0 |
| `tatara-helmfile` | 5 commits | operator chart+image pinned at `1.42.0`/`v1.42.0`; wrapper image at `v1.3.5`; `skillsRef` at `v1.8.0` |

- [ ] **Step P0.1.1: Refresh every submodule before any other step**

```bash
cd /Users/szymonri/Documents/tatara-new
git submodule update --remote
for r in tatara-operator tatara-cli tatara-claude-code-wrapper tatara-agent-skills tatara-helmfile; do
  echo "== $r"; git -C code/$r log --oneline -1; git -C code/$r status -sb | head -1
done
```

Expected: every repo reports `## main...origin/main` with no `[behind N]`.

- [ ] **Step P0.1.2: Re-verify the three contract constants after the refresh**

```bash
grep -n "ContractVersion = " code/tatara-operator/internal/agent/session.go \
  code/tatara-cli/internal/mcp/contract.go \
  code/tatara-claude-code-wrapper/internal/version/version.go
```

Expected: all three print `3`. If any prints something else, STOP and re-read the design doc's contract block before proceeding.

### P0.2 MR1 must already be merged

This plan assumes MR1 (the sweep orphan-liveness fix) has landed on `tatara-operator/main`, i.e.:

- `IsOrphanIssue` returns `(bool, reason)`.
- A closed `SweepSkip` vocabulary exists in `internal/controller/sweep.go` alongside `SweepSkipMRClaimed`.
- `MintForItem` / `MintIssueTask` / `MintReviewTask` return a `MintOutcome` enum (`MintCreated | MintExistingLive | MintTombstoneDeleted | MintNotOwed`) instead of a bare bool.

- [ ] **Step P0.2.1: Confirm MR1 landed**

```bash
cd code/tatara-operator
grep -n "MintOutcome\|MintTombstoneDeleted\|SweepSkipOwnerGone" internal/controller/sweep.go internal/controller/intake.go | head -20
```

Expected: non-empty. If empty, MR1 has not landed - STOP, this plan's MR6 rewrites `resume.go`'s only caller of `MintForItem` and needs the typed return.

### P0.3 The conversing pre-flight, and its off-ramp

`conversing` is weeks old (#475, #514, #516). MR6 generalises its mechanism. Do NOT generalise an unfinished mechanism.

- [ ] **Step P0.3.1: Run the pre-flight against the live cluster**

```bash
# 1. Force-deleted agent pods: the signal that graceful handoff does not work.
#    Any non-zero 7d increase means TTL stop is falling through to force delete.
promtool query instant "$PROM" 'increase(operator_agent_pod_ttl_expired_total{outcome="force_deleted"}[7d])'
promtool query instant "$PROM" 'sum by (outcome) (operator_agent_pod_ttl_expired_total)'

# 2. Conversing entry refusals.
promtool query instant "$PROM" 'sum by (project,reason) (increase(operator_conversing_entry_declined_total[7d]))'
promtool query instant "$PROM" 'sum by (stageReason,kind) (increase(operator_unpark_declined_total[7d]))'

# 3. Conversing occupancy and closure.
promtool query instant "$PROM" 'operator_conversing_pods'
promtool query instant "$PROM" 'sum(increase(operator_conversing_closed_total[7d]))'
```

Equivalent via the grafana MCP server: `query_prometheus` against the default Prometheus datasource with the same expressions.

**Reading taken 2026-08-07 (recorded here so the implementer compares against a baseline, not against nothing):**

| query | value | note |
|---|---|---|
| `increase(operator_agent_pod_ttl_expired_total{outcome="force_deleted"}[7d])` | **~4** | Split across `agent_kind` incident (x2) and refine. NON-ZERO. |
| `operator_conversing_entry_declined_total` (cumulative) | **~27** | Across projects `tatara` and `infrastructure`. |
| `rate(operator_conversing_entry_declined_total[15m])` | **0** | Quiet right now. |
| `operator_conversing_pods` | **0** on all three projects | No conversation is live. |
| `operator_illegal_stage_transition_total` | **no series, ever** | Not present in the metric catalogue at all. |
| `operator_sweep_mint_cap_hit_total` | **no series** | Confirms the design doc's claim; the budget is not what skipped the orphans. |
| `operator_sweep_skipped_total` | **0** on all three project series, `reason=mr_claimed_by_other_task` only | The pre-MR1 vocabulary has exactly one member, as the design says. |

Two measurement caveats that must be respected:

1. **Prometheus retention here is ~7-8 days.** `up{job="tatara-operator"}` has data at `now-7d` and none at `now-8d`. Any "30d" figure is the same week seen through a wider lookback. A 30-day baseline does not exist. Do not claim one.
2. **`operator_conversing_entry_declined_total` only ever carries `reason="unresolved"`.** The label is never populated with a real vocabulary member. That is exactly the defect class this codebase already legislates against ("a decline that cannot name its condition"). Fixing it is folded into Task 6.8, not deferred.

- [ ] **Step P0.3.2: Apply the decision rule and record it**

A bare `force_deleted > 0` rule trips on the reading above and is too blunt: the interesting number is the FRACTION of TTL stops that could not hand off gracefully. Run:

```bash
promtool query instant "$PROM" \
  'sum(increase(operator_agent_pod_ttl_expired_total{outcome="force_deleted"}[7d]))
   / clamp_min(sum(increase(operator_agent_pod_ttl_expired_total[7d])), 1)'
promtool query instant "$PROM" 'sum by (outcome) (increase(operator_agent_pod_ttl_expired_total[7d]))'
```

| observation | decision |
|---|---|
| force-delete fraction <= 0.10 AND `rate(operator_conversing_entry_declined_total[7d])` is flat | Proceed with the full MR6 including Task 6.7 (liveness) and Task 6.8 (live-pod ceiling). |
| force-delete fraction > 0.10, OR conversing-entry declines are climbing | **OFF-RAMP.** Stabilise `conversing` first in a separate MR (its own `systematic-debugging` session; `StopWithHandoff` at `internal/agent/ttlstop.go:208-239` is where to start, and the `reason="unresolved"` label is the first thing to fix because without it the failure cannot be attributed). Then land MR6 WITHOUT Task 6.7 and Task 6.8: keep `conversing` as a distinct live state whose entry/exit and ceiling code is carried over unchanged onto the new state names, and ship the kind fold + state model + gate + migrator only. Re-open the liveness generalisation once the fraction is clean. |

**On the 2026-08-07 reading the denominator was not captured, so the fraction is UNKNOWN and the gate is UNRESOLVED.** The implementer MUST run the two queries above and record the result before writing any MR6 code. If the fraction cannot be computed because the total-TTL-stop series is also sparse, treat that as "insufficient data" and take the off-ramp: generalising a mechanism whose failure rate you cannot measure is exactly what the design doc warns against.

Write the observed numbers and the decision into `MEMORY.md` as a dated line before writing any code. The off-ramp is NOT a deferral under ground rule 3: it is a scope split the design doc itself authorises, and it must be recorded as such, with the follow-on conversing-stabilisation MR opened as a tracked issue in the same session.

### P0.4 The live Task census, measured

Verified on 2026-08-07 with `kubectl get tasks -A`. Total **118**, matching the design doc's 52/38/5/2/1/9/7/4 exactly:

| count | stage | stageReason | spec.kind | parkedFromStage |
|---|---|---|---|---|
| 52 | parked | backlog-sweep | clarify | (create) |
| 17 | parked | awaiting-human | incident | clarifying |
| 15 | parked | awaiting-human | review | (create) |
| 8 | rejected | tracked-elsewhere | incident | - |
| 6 | parked | awaiting-human | clarify | clarifying |
| 6 | delivered | - | refine | - |
| 4 | parked | no-outcome | incident | investigating |
| 2 | parked | review-loop-exhausted | incident | reviewing |
| 1 | rejected | issue-closed | clarify | conversing |
| 1 | parked | ownership-lost | clarify | implementing |
| 1 | parked | no-outcome | refine | refining |
| 1 | failed | pod-recreation-exhausted | review | - |
| 1 | failed | operator-error | incident | merging |
| 1 | failed | merge-blocked | incident | merging |
| 1 | failed | merge-blocked | clarify | merging |
| 1 | delivered | - | brainstorm | - |

Two measured facts that CORRECT the design doc:

1. **`status.agentKind` is EMPTY on all 118 Tasks.** Every one is in a pod-less stage, and `stage.Enter` sets `AgentKind = AgentKindFor(to)` which is `""` for parked/rejected/failed/delivered. So the kind-match gate at `internal/restapi/outcome.go:300` (`if env.Kind != task.Status.AgentKind`) cannot 409 on the current population, because no pod exists to submit an outcome. **The migrator's AgentKind rewrite is a defensive no-op today and must be written to be idempotent rather than assumed to fire.**
2. **`spec.kind == "clarify"` on 61 Tasks** (52 + 6 + 1 + 1 backlog/awaiting-human/ownership-lost/merge-blocked, plus the 1 rejected(issue-closed)). The real breakage is NOT the outcome gate: it is that under the 8-state model there is no `clarifying` state for `spec.kind = clarify` to route into out of `new`, so those 61 Tasks would wedge with no legal edge. That is the actual argument for the spec-kind rewrite, and it is stronger than the 409 argument. Use it in the MR description.
3. **Zero agent pods are running** (`kubectl get pods -l app.kubernetes.io/component=agent` returns none). The migrator's "delete their pods" step is therefore also a defensive no-op on today's population. Write it, test it, and expect it to delete nothing.
4. **Cluster is Kubernetes `v1.33.0`.** This is exactly the version whose status-subresource ratcheting fix (PR #129506) the design relies on. Three Projects exist: `tatara`, `infrastructure`, `mtg`.

### P0.5 ORDERING CORRECTION: MR4 must release before MR3 merges

The design doc orders MR3 (skills) before MR4 (cli). **That order fails CI.**

`tatara-agent-skills/.github/scripts/validate_tool_calls.py` fetches
`https://github.com/szymonrychu/tatara-cli/releases/latest/download/tool-manifest.json`
(the pin file `.github/tool-manifest-version` currently contains the literal string `latest`)
and HARD-FAILS on any `submit_outcome(action="...")` literal in a `SKILL.md` whose value
is not in the fetched manifest's enum union. Only a fetch *failure* is a soft warning;
a successful fetch of a *stale* manifest is a hard error and `lint.yml` goes red, which
also means `release.yml` (a `workflow_run` on lint success) never cuts a skills tag.

This is not a hypothesis. `tatara-agent-skills/MEMORY.md` records it happening on
2026-07-28 for exactly this shape:

> `validate_tool_calls.py` now hard-fails locally on all three `action="exhausted"` mentions because it fetches tatara-cli's currently-RELEASED manifest ("latest" pin) which predates this fix; expected and self-resolving once tatara-cli's paired change ships a release

MR3 introduces `submit_outcome(action="approved")`, `action="discuss"` and `action="rejected"` in prose. All three will be flagged.

**Resolution (binding on this plan): swap MR3 and MR4.** The corrected order is
MR2 -> **MR4 (cli)** -> **MR3 (skills)** -> MR5 (wrapper) -> MR6 + MR7.
MR2's hard gate is unaffected: it exists so a skills merge does not ship live, and MR2 still precedes both.

The alternative - pinning `.github/tool-manifest-version` to the exact `vX.Y.Z` MR4 cuts - also requires MR4 to have released first, so it buys nothing and costs a pin to hand-maintain. Rejected.

---

## Merge choreography

| step | repo | MR | semver | what is observable between this step and the next |
|---|---|---|---|---|
| 0 | - | MR1 (not this plan) | patch | Sweep starts minting again; `operator_sweep_orphan_stranded_seconds` drops to 0. Platform is doing work under the OLD model. |
| 1 | `tatara-helmfile` | **MR2** pin guard | n/a (deploy repo) | Nothing changes at runtime - the pins are already concrete. What changes is that CI now REFUSES a floating `skillsRef` or wrapper tag. `helmfile diff` must be empty. |
| 2 | `tatara-cli` | **MR4** submit_outcome schema + ContractVersion 4 | **major** | The released `tool-manifest.json` now carries the merged action enum. Nothing in the cluster changes: the wrapper image is still pinned to a v1.3.x build carrying the OLD cli. No pods run either way (measured: zero agent pods). |
| 3 | `tatara-agent-skills` | **MR3** skill fold + profile hard-locks | **major** | `validate_tool_calls.py` now goes green against step 2's manifest. Skills are NOT live: MR2 pinned `skillsRef` to `v1.8.0`, so pods keep installing the old skill set until MR7. |
| 4 | `tatara-claude-code-wrapper` | **MR5** ContractVersion 4 | **major** | A new wrapper image `vX.Y.Z` exists in Harbor. Not deployed: the Project CRs still pin `v1.3.5`. |
| 5 | `tatara-operator` | **MR6** the big one | **major** | **THE OUTAGE WINDOW OPENS.** Helm applies the narrowed CRD, then the new Deployment. The migrator runs pre-`mgr.Start()` and rewrites all 118 Tasks. Any pod spawned in this window asserts contract 4 against a contract-3 wrapper and fails at pod-ready with `failed(agent-contract-mismatch)` - loud, BEFORE turn 0, zero tokens. |
| 6 | `tatara-helmfile` | **MR7** move `skillsRef` + wrapper tag, ONE commit | n/a (deploy repo) | Window closes. Pods spawn on the contract-4 wrapper with the folded skill set. |

**Steps 5 and 6 land in the same maintenance slot.** Target window: minutes, not hours. Both MRs are prepared, reviewed and green BEFORE either is merged; step 6 is merged the moment step 5's CD pipeline reports the operator chart published.

**Accepted outage window:** between step 5 merging and step 7's helmfile apply completing, every agent pod spawn fails at `AssertContractVersion` (`internal/agent/session.go:115`, called from `internal/controller/podwatch.go:181`). The Task goes to `parked(agent-contract-mismatch)`. This is the documented precedent from the 2 -> 3 bump. It is loud, pre-work, and burns zero tokens. Measured mitigation: today the cluster runs zero agent pods and every one of the 118 Tasks is in a pod-less state, so the realistic blast radius in this window is zero Tasks.

**Rollback:** MR6 is NOT symmetrically reversible. The CRD narrowing plus the migrator's writes mean a downgrade to the old operator finds `status.state` set and `status.stage` empty, and every Task reads as stage-less. The rollback is forward-only: fix and re-release. Say this in MR6's description. Apply the chart and the image TOGETHER (`helm upgrade --wait`), never `kubectl set image`.

---

## Semver justification, per MR

| MR | repo | level | why |
|---|---|---|---|
| MR2 | tatara-helmfile | n/a | Deploy repo. Its CD is `helmfile apply` on merge, not a semver tag. |
| MR4 | tatara-cli | **major** | `submit_outcome`'s `clarify` schema is DELETED and the implement `action` enum gains three values. `ContractVersion` 3 -> 4. Any pod running an older operator against this cli is broken. Backward-incompatible by definition. |
| MR3 | tatara-agent-skills | **major** | A skill directory (`skills/clarify/`) is deleted and two skills change profile. Any consumer pinning `skillsRef` to this tag and running a contract-3 pod gets a skill set that documents tools its cli does not serve. |
| MR5 | tatara-claude-code-wrapper | **major** | `ContractVersion` 3 -> 4. The operator refuses this image unless it is also on 4. Breaking by the contract's own definition ("Bump this in the same release that ships a breaking agent-facing change"). |
| MR6 | tatara-operator | **major** | CRD field removal (`status.stage`, `status.stageReason`), enum narrowing, an agent kind deleted, and `ContractVersion` 3 -> 4. Three independent breaking changes. |
| MR7 | tatara-helmfile | n/a | Deploy repo, same as MR2. |

---

## Plan document placement

`docs/superpowers/plans/` exists in exactly two of the five repos:

- `tatara-operator/docs/superpowers/plans/` - **this file**, the master plan.
- `tatara-claude-code-wrapper/docs/superpowers/plans/` - gets a short companion, `2026-08-07-521-contract-version-4.md` (MR5 only).

`tatara-cli`, `tatara-agent-skills` and `tatara-helmfile` have **no `docs/` tree at all** and therefore no plans convention. Their MRs (MR4, MR3, MR2, MR7) are specified in full in this document and are NOT duplicated into those repos. Their MR descriptions must link back to this file by path.

---

# MR2 - tatara-helmfile: make the pins un-floatable

**Repo:** `tatara-helmfile`. **Semver:** n/a (deploy repo). **Gates:** MR3 and MR4, hard.

## Why this MR is not what the design doc says it is

The design doc says MR2 "pins `spec.agent.skillsRef` and the wrapper image tag on every Project". **Measured on `origin/main` 2026-08-07, they are already pinned:**

| file | line | current value |
|---|---|---|
| `values/project-tatara/common.yaml` | 40 | `image: harbor.szymonrichert.pl/containers/tatara-claude-code-wrapper:v1.3.5` |
| `values/project-tatara/common.yaml` | 53 | `skillsRef: v1.8.0` |
| `values/project-infrastructure/common.yaml` | 36 | `image: ...tatara-claude-code-wrapper:v1.3.5` |
| `values/project-infrastructure/common.yaml` | 46 | `skillsRef: v1.8.0` |
| `values/project-mtg/common.yaml` | 29 | `image: ...tatara-claude-code-wrapper:v1.3.5` |
| `values/project-mtg/common.yaml` | 36 | `skillsRef: v1.8.0` |

So the decoupling MR2 exists to provide is ALREADY IN EFFECT. Writing a no-op MR to satisfy a plan is exactly the kind of ceremony the KISS rule forbids.

But the hazard MR2 exists to close is real and permanent: `internal/agent/pod.go:733-742` defaults `TATARA_SKILLS_REF` to the literal `"main"` when `Project.spec.agent.skillsRef` is empty, and `profileForKind` (`internal/agent/pod.go:950-952`) returns `""` for an unknown kind, which makes the cli's `resolveProfile` fail closed and NOT register `submit_outcome` at all. An empty `skillsRef` on any future Project therefore ships every skills merge live to that project, silently.

**MR2 is therefore: make an unpinned `skillsRef` or a floating wrapper tag impossible to merge.** That is a real change, it is the same protection, and it survives future Projects.

## Files

- Modify: `.github/scripts/check_pin_coverage.py`
- Modify: `.github/scripts/test_check_pin_coverage.py`
- Modify: `values/project-tatara/common.yaml`, `values/project-infrastructure/common.yaml`, `values/project-mtg/common.yaml` (comment only, one line each, naming the guard)

## Interfaces

- Produces: a CI guard that MR7 must satisfy. MR7 changes the two pinned values and must keep them concrete.

## Task 2.1: the pin guard

- [ ] **Step 1: Write the failing test**

Append to `.github/scripts/test_check_pin_coverage.py`:

```python
def test_agent_pins_must_be_concrete(tmp_path):
    """A floating skillsRef or wrapper tag ships every upstream merge live."""
    values = tmp_path / "values" / "project-x"
    values.mkdir(parents=True)
    (values / "common.yaml").write_text(
        "project:\n"
        "  spec:\n"
        "    agent:\n"
        "      image: harbor.szymonrichert.pl/containers/tatara-claude-code-wrapper:latest\n"
        "      skillsRef: main\n"
    )
    problems = check_pin_coverage.check_agent_pins(tmp_path)
    assert len(problems) == 2
    assert any("skillsRef" in p for p in problems)
    assert any("wrapper image tag" in p for p in problems)


def test_agent_pins_accept_a_semver_tag(tmp_path):
    values = tmp_path / "values" / "project-x"
    values.mkdir(parents=True)
    (values / "common.yaml").write_text(
        "project:\n"
        "  spec:\n"
        "    agent:\n"
        "      image: harbor.szymonrichert.pl/containers/tatara-claude-code-wrapper:v1.3.5\n"
        "      skillsRef: v1.8.0\n"
    )
    assert check_pin_coverage.check_agent_pins(tmp_path) == []


def test_agent_pins_reject_an_absent_skillsref(tmp_path):
    """Absent is worse than floating: pod.go defaults TATARA_SKILLS_REF to 'main'."""
    values = tmp_path / "values" / "project-x"
    values.mkdir(parents=True)
    (values / "common.yaml").write_text(
        "project:\n"
        "  spec:\n"
        "    agent:\n"
        "      image: harbor.szymonrichert.pl/containers/tatara-claude-code-wrapper:v1.3.5\n"
    )
    problems = check_pin_coverage.check_agent_pins(tmp_path)
    assert len(problems) == 1
    assert "skillsRef is absent" in problems[0]
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-helmfile
mise exec -- python3 -m pytest .github -q
```

Expected: FAIL with `AttributeError: module 'check_pin_coverage' has no attribute 'check_agent_pins'`.

- [ ] **Step 3: Write the minimal implementation**

Add to `.github/scripts/check_pin_coverage.py`:

```python
import re

# A concrete pin is a vX.Y.Z tag. "main", "latest", a branch name or an absent
# value all mean "whatever upstream merged last", which is how a skills merge
# ships live to a running fleet (tatara-operator internal/agent/pod.go:733-742
# defaults TATARA_SKILLS_REF to "main" when spec.agent.skillsRef is empty).
SEMVER_TAG = re.compile(r"^v\d+\.\d+\.\d+$")
WRAPPER_IMAGE = "tatara-claude-code-wrapper"


def check_agent_pins(root):
    """Return a list of human-readable problems, one per unpinned agent input."""
    problems = []
    for path in sorted(root.glob("values/project-*/common.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        agent = ((doc.get("project") or {}).get("spec") or {}).get("agent") or {}

        ref = agent.get("skillsRef")
        if ref is None:
            problems.append(f"{path}: skillsRef is absent; the operator defaults it to 'main'")
        elif not SEMVER_TAG.match(str(ref)):
            problems.append(f"{path}: skillsRef={ref!r} is not a vX.Y.Z tag")

        image = str(agent.get("image") or "")
        if WRAPPER_IMAGE in image:
            tag = image.rsplit(":", 1)[-1] if ":" in image else ""
            if not SEMVER_TAG.match(tag):
                problems.append(f"{path}: wrapper image tag {tag!r} is not a vX.Y.Z tag")
    return problems
```

and call it from `main()` alongside the existing chart-pin coverage check, appending its problems to the same failure list so one run reports everything.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
mise exec -- python3 -m pytest .github -q
mise exec -- python3 .github/scripts/check_pin_coverage.py
```

Expected: pytest all green; `check_pin_coverage.py` exits 0 against the real `values/` tree (all six pins are already concrete).

- [ ] **Step 5: Confirm the deploy is a no-op**

```bash
mise exec -- helmfile -e default diff --detailed-exitcode --suppress-secrets
```

Expected: exit code 0 (no changes). If anything renders, STOP - something other than this guard changed.

- [ ] **Step 6: Commit**

```bash
git add .github/scripts/check_pin_coverage.py .github/scripts/test_check_pin_coverage.py values/
git commit -m "ci: refuse a floating skillsRef or wrapper image tag

The operator defaults TATARA_SKILLS_REF to 'main' when spec.agent.skillsRef is
empty (internal/agent/pod.go:733-742), so an unpinned Project ships every
tatara-agent-skills merge to a live fleet the moment it lands. The three
Projects are already pinned; this makes unpinning them un-mergeable."
```

## Verification (MR2)

```bash
mise exec -- python3 -m pytest .github -q
mise exec -- python3 .github/scripts/check_pin_coverage.py
mise exec -- pre-commit run --all-files
mise exec -- helmfile -e default diff --detailed-exitcode --suppress-secrets
```

`helmfile lint` is not a target here; `lint.yaml` runs pytest + `check_pin_coverage.py`, `diff.yaml` runs the helmfile diff on the PR.

---

# MR4 - tatara-cli: the submit_outcome schema (LANDS BEFORE MR3)

**Repo:** `tatara-cli`. **Semver:** `major`. **Gates:** MR3 (its released `tool-manifest.json` is what MR3's CI validates against).

## What changes

`clarify` is deleted as a profile. Its three decisions become `action` values on the implement schema. `documentation` gets its OWN schema instead of aliasing implement's, because otherwise a documentation agent inherits the three new gate actions.

## Files

- Modify: `internal/mcp/outcome.go` (line refs are `origin/main` as of 2026-08-07)
- Modify: `internal/mcp/profiles.go`
- Modify: `internal/mcp/contract.go`
- Modify: `internal/mcp/contract_test.go`
- Modify: `internal/mcp/outcome_test.go`
- Modify: `internal/mcp/profiles_test.go`
- Modify: `internal/mcp/server_test.go`
- Delete: `internal/mcp/testdata/outcome-clarify.schema.json`
- Delete: `internal/mcp/testdata/profile-tools-clarify.txt`
- Modify: `internal/mcp/testdata/outcome-implement.schema.json`
- Create: `internal/mcp/testdata/outcome-documentation.schema.json` (currently a copy of implement's; becomes genuinely distinct)
- Modify: `internal/mcp/testdata/agent-kinds.txt`
- Modify: `MEMORY.md`, `ROADMAP.md`

## Interfaces

- Produces: `submit_outcome` for profile `implement` accepting
  `action in {submitted, declined, approved, discuss, rejected}`, plus
  `approving_maintainer` (string), `approval_citations` ([]{id,quote}),
  `plan_note_id` (string), `reason` (string).
- Produces: wire field names `approvingMaintainer`, `planNoteId` (via `outcomeArgMap`).
- Produces: `ContractVersion = 4`.
- Consumes: nothing from other MRs.

## Task 4.1: split documentation off the implement schema

Do this FIRST, before adding the new actions, so the golden diff is legible.

- [ ] **Step 1: Write the failing test**

Replace `TestOutcome_DocumentationSchemaEqualsImplement` (`internal/mcp/outcome_test.go:572`) with:

```go
func TestOutcome_DocumentationActionEnumIsSubmittedOrDeclinedOnly(t *testing.T) {
	tool, ok := OutcomeTool("documentation")
	require.True(t, ok)

	var schema struct {
		Properties struct {
			Action struct {
				Enum []string `json:"enum"`
			} `json:"action"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(tool.InputSchema, &schema))
	require.ElementsMatch(t, []string{"submitted", "declined"}, schema.Properties.Action.Enum,
		"a documentation agent has no approval gate to drive; it must never be able to emit approved/discuss/rejected")
}
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-cli
mise exec -- go test ./internal/mcp/ -run 'TestOutcome_Documentation' -v
```

Expected: FAIL - `TestOutcome_DocumentationSchemaEqualsImplement` still exists and the new test does not compile until the old one is removed. After removing the old one it passes trivially (both schemas are identical today), which is correct: this test is the guard that Task 4.2 must not break.

- [ ] **Step 3: Write the minimal implementation**

In `internal/mcp/outcome.go`, add after `implementOutcomeSchema` (which ends at line 25):

```go
// documentationOutcomeSchema was `implementOutcomeSchema` until the clarify
// fold. It is now its OWN const: implement's action enum grew the three
// approval-gate actions, and a documentation agent has no gate to drive - it
// writes docs and opens an MR. Sharing the const would have handed it three
// actions its operator-side handler has no branch for.
const documentationOutcomeSchema = `{"type":"object","properties":{
  "task":{"type":"string"},
  "action":{"type":"string","enum":["submitted","declined"]},
  "title":{"type":"string","description":"MR title. Required when action=submitted."},
  "body":{"type":"string","description":"MR body. Required when action=submitted."},
  "change_significance":{"type":"string","enum":["major","minor","patch"],
    "description":"Required when action=submitted. major=backward-incompatible; minor=backward-compatible feature; patch=fix. YOU own this level - a reviewer may raise it but can never lower it."},
  "merge_order":{"type":"array","items":{"type":"string"},
    "description":"REQUIRED when this task's MRs span more than one repo: the Repository CR names in dependency order, first-merged first. There is NO default. Get it wrong and a downstream repo ships against an API that has not merged yet."},
  "decline_reason":{"type":"string","description":"Required when action=declined."}},
 "required":["action"],"additionalProperties":false}`
```

and change `outcomeSchemas["documentation"]` (line 102) from `json.RawMessage(implementOutcomeSchema)` to `json.RawMessage(documentationOutcomeSchema)`.

Add `validateDocumentationOutcome` as an exact copy of today's `validateImplementOutcome`, and point `validateOutcome`'s `case "documentation"` at it (splitting the shared `case "implement", "documentation":` at line 189-190).

Copy `testdata/outcome-implement.schema.json` to `testdata/outcome-documentation.schema.json` unchanged.

- [ ] **Step 4: Run tests to verify they pass**

```bash
mise exec -- make test
```

Expected: PASS. `TestOutcomeTool_SchemaGoldens` compares against the two now-distinct goldens and both still match.

- [ ] **Step 5: Commit**

```bash
git add internal/mcp/outcome.go internal/mcp/outcome_test.go internal/mcp/testdata/
git commit -m "refactor(mcp): give documentation its own outcome schema

It aliased implement's. implement's action enum is about to grow the approval
gate's three actions and documentation must not inherit them."
```

## Task 4.2: merge clarify's decisions into the implement action enum

- [ ] **Step 1: Write the failing tests**

In `internal/mcp/outcome_test.go`, DELETE `TestOutcome_ClarifyRequiresDecisionAndReason` (line 238) and `TestClarifySchemaCarriesApprovalCitations` (line 542). Add:

```go
func TestOutcome_ImplementActionEnumCarriesTheGateActions(t *testing.T) {
	tool, ok := OutcomeTool("implement")
	require.True(t, ok)

	var schema struct {
		Properties struct {
			Action struct {
				Enum []string `json:"enum"`
			} `json:"action"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(tool.InputSchema, &schema))
	require.ElementsMatch(t,
		[]string{"submitted", "declined", "approved", "discuss", "rejected"},
		schema.Properties.Action.Enum)
}

func TestOutcome_ImplementApprovedRequiresTheGateFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		args map[string]any
		want string
	}{
		{"no approving_maintainer", map[string]any{
			"action": "approved", "reason": "r", "plan_note_id": "n-1",
			"approval_citations": []any{map[string]any{"id": "c1", "quote": "go ahead"}},
		}, "approving_maintainer required"},
		{"no plan_note_id", map[string]any{
			"action": "approved", "reason": "r", "approving_maintainer": "szymonrychu",
			"approval_citations": []any{map[string]any{"id": "c1", "quote": "go ahead"}},
		}, "plan_note_id required"},
		{"no reason", map[string]any{
			"action": "approved", "approving_maintainer": "szymonrychu", "plan_note_id": "n-1",
			"approval_citations": []any{map[string]any{"id": "c1", "quote": "go ahead"}},
		}, "reason required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOutcome("implement", tc.args)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestOutcome_ImplementDiscussAndRejectedNeedOnlyAReason(t *testing.T) {
	for _, action := range []string{"discuss", "rejected"} {
		require.NoError(t, validateOutcome("implement",
			map[string]any{"action": action, "reason": "because"}))
		require.ErrorContains(t,
			validateOutcome("implement", map[string]any{"action": action}),
			"reason required")
	}
}

func TestOutcome_ImplementSubmittedStillRefusesTheGateFields(t *testing.T) {
	err := validateOutcome("implement", map[string]any{
		"action": "submitted", "title": "t", "body": "b", "change_significance": "patch",
		"approving_maintainer": "szymonrychu",
	})
	require.ErrorContains(t, err, "approving_maintainer is only valid when action=approved")
}

func TestOutcome_ApprovedMapsTheNewWireFields(t *testing.T) {
	got, err := buildOutcomePayload("implement", map[string]any{
		"action": "approved", "reason": "maintainer said go", "plan_note_id": "n-7",
		"approving_maintainer": "szymonrychu",
		"approval_citations":   []any{map[string]any{"id": "c1", "quote": "go ahead"}},
	})
	require.NoError(t, err)
	require.Equal(t, "szymonrychu", got["approvingMaintainer"])
	require.Equal(t, "n-7", got["planNoteId"])
	require.Contains(t, got, "approvalCitations")
	require.NotContains(t, got, "approving_maintainer")
	require.NotContains(t, got, "plan_note_id")
}

func TestOutcomeTool_HasNoClarifyProfile(t *testing.T) {
	_, ok := OutcomeTool("clarify")
	require.False(t, ok, "clarify is deleted; a pod claiming it must get NO submit_outcome (fail closed)")
}
```

Update `TestOutcomeTool_ExistsForAllSevenAgentKinds` (line 15) to `TestOutcomeTool_ExistsForAllSixAgentKinds` and drop `clarify` from its list. Update `TestOutcomeArgMapCoversEverySnakeCaseSchemaKey` (line 519) - it is a mechanical guard and should pass unchanged once `approving_maintainer` and `plan_note_id` are in `outcomeArgMap`; if it fails, that is the guard doing its job.

- [ ] **Step 2: Run to verify they fail**

```bash
mise exec -- go test ./internal/mcp/ -run 'TestOutcome_Implement|TestOutcomeTool_Has|TestOutcome_Approved' -v
```

Expected: FAIL - the action enum has two members, `approving_maintainer` is unknown, `OutcomeTool("clarify")` still returns a tool.

- [ ] **Step 3: Write the implementation**

`internal/mcp/outcome.go`:

1. Replace line 17 with:

```go
  "action":{"type":"string","enum":["submitted","declined","approved","discuss","rejected"]},
```

2. Insert into `implementOutcomeSchema`'s properties, after `decline_reason` (line 24):

```go
  "reason":{"type":"string","description":"Required for action=approved, action=discuss and action=rejected. For approved, say in plain words WHO approved and WHY you read their comment as approval."},
  "approving_maintainer":{"type":"string","description":"Required for action=approved: the login of the maintainer whose comment you are citing as the go-ahead. It is a DECLARATION, not an authority - the operator refuses if it is not a verified maintainer, and refuses again if it does not match the author of the comment you cited. The citation stays the sole authority."},
  "plan_note_id":{"type":"string","description":"Required for action=approved: the id returned by the task_note(kind=\"plan\") call that wrote the plan the maintainer approved. The operator hashes that note's body at grant and re-checks the hash before you write code, so a plan swapped after approval is refused."},
  "approval_citations":{"type":"array","items":{"type":"object","properties":{
      "id":{"type":"string"},"quote":{"type":"string"}},
    "required":["id","quote"]},
    "description":"Required for action=approved whenever a maintainer has commented: ONE entry per issue this task owns. id is the external_id of the maintainer comment you are citing, copied verbatim from the <comment external_id=\"...\"> attribute already in your turn-0 bundle - do NOT re-crawl to find it; it does NOT have to be the newest comment on the thread. quote is a VERBATIM substring of that same comment's body. YOU judge whether the comment approves; the operator re-reads the comment itself and refuses if the id does not name a maintainer-authored non-bot comment on that issue, if your quote is not in it, or if it already approved once. If a LATER maintainer comment withdraws the approval you would otherwise cite, send action=discuss instead - do not cite a withdrawn approval. Omit only when no human has commented at all."}},
```

3. Delete `clarifyOutcomeSchema` (lines 44-53 on `origin/main`).
4. Delete `outcomeSchemas["clarify"]` (line 104).
5. Delete `outcomeDescriptions["clarify"]` (line 118) and rewrite `outcomeDescriptions["implement"]` (line 115) to name all five actions:

```go
	"implement": "Finish this implement turn. Five actions. action=approved reports that a maintainer gave you the go-ahead on the plan you wrote: set approving_maintainer, plan_note_id and approval_citations, and the operator re-reads the cited comment and refuses if the citation does not hold up - a refusal is a normal result, not an error, and you keep talking. action=discuss holds the conversation open with a reason. action=rejected closes the issue with a reason. action=submitted opens the MR with the title, body and change_significance you own (plus merge_order when this task's MRs span more than one repo). action=declined declines the work with a decline_reason. This is the only way an implement task terminates.",
```

6. Add to `outcomeArgMap` (line 127):

```go
	"approving_maintainer": "approvingMaintainer",
	"plan_note_id":         "planNoteId",
```

(`approval_citations -> approvalCitations` is already there at line 133.)

7. Delete `case "clarify":` from `validateOutcome` (line 194) and delete `validateClarifyOutcome` (line 253). Extend `validateImplementOutcome` (line 207):

```go
	case "approved":
		for _, k := range []string{"reason", "approving_maintainer", "plan_note_id"} {
			if strings.TrimSpace(argString(a, k)) == "" {
				return fmt.Errorf("submit_outcome: %s required when action=approved", k)
			}
		}
		// Shape only. WHETHER a citation was needed, whether the cited id names a
		// maintainer's comment on that issue, whether the quote really occurs in
		// the body, and whether approving_maintainer agrees with the citation are
		// the OPERATOR's calls - it holds the mirror. A refusal there is a 200
		// with granted=false, not an error here.
		for i, raw := range outcomeList(a, "approval_citations") {
			c, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("submit_outcome: approval_citations[%d] must be an object with id and quote", i)
			}
			if strings.TrimSpace(argString(c, "id")) == "" {
				return fmt.Errorf("submit_outcome: approval_citations[%d].id required: the comment's external_id from your bundle", i)
			}
			if strings.TrimSpace(argString(c, "quote")) == "" {
				return fmt.Errorf("submit_outcome: approval_citations[%d].quote required: a VERBATIM substring of that comment's body", i)
			}
		}
		return nil
	case "discuss", "rejected":
		if strings.TrimSpace(argString(a, "reason")) == "" {
			return fmt.Errorf("submit_outcome: reason required when action=%s", argString(a, "action"))
		}
		return nil
```

and, in the existing `case "submitted":` and `case "declined":` arms, refuse the gate fields:

```go
		for _, k := range []string{"approving_maintainer", "plan_note_id", "approval_citations"} {
			if _, ok := a[k]; ok {
				return fmt.Errorf("submit_outcome: %s is only valid when action=approved", k)
			}
		}
```

`internal/mcp/profiles.go`:

8. Delete `kindProfiles["clarify"]` (line 21) and `profiles["clarify"]` (line 54).
9. Widen `profiles["implement"]` to the union of its current set with clarify's: add `"issue_write"`, `"memory_query"`, `"memory_describe"`. Its existing set already has `scm_read`, `code_search`, `code_context`, `code_explain`. Justification, in a comment: the merged agent conducts the conversation on the issue AND writes the code, so it needs the issue-writing and recall tools clarify had. It does NOT gain `task_list` (contract D.6 denies it to implement) and it does NOT gain `memory_write`/`memory_entity`/`memory_edges`.

- [ ] **Step 4: Update the goldens and the remaining tests**

```bash
rm internal/mcp/testdata/outcome-clarify.schema.json internal/mcp/testdata/profile-tools-clarify.txt
```

Regenerate `testdata/outcome-implement.schema.json` and `testdata/profile-tools-implement.txt` from the new code (the goldens are compared literally - update them by hand from the test failure output, or add a `-update` flag run if the test already supports one). Remove `clarify` from `testdata/agent-kinds.txt`.

In `internal/mcp/profiles_test.go`: rename `TestKindProfiles_HasAllSevenAgentKindsIncludingClarify` to `TestKindProfiles_HasAllSixAgentKindsAndNoClarify` and assert `clarify` is absent. `TestAgentKinds_MatchTheOperatorsGolden` and `TestProfileGatingTable_IsContractD6Verbatim` update against the new goldens.

In `internal/mcp/server_test.go`: `TestTotalToolSurfaceIsTwentyOne` and `TestNewServer_RefineAllowSetIsThirteen` are count pins - recompute and rename if the numbers move. The tool COUNT should not change (no tool is added or removed, only re-profiled); if it does, that is a bug in step 3.

- [ ] **Step 5: Run the full suite**

```bash
mise exec -- make test
mise exec -- make lint
mise exec -- pre-commit run --all-files
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add internal/mcp/
git commit -m "feat(mcp)!: fold the clarify outcome into implement

clarify is deleted as a profile. Its implement/close/discuss decisions become
approved/discuss/rejected actions on the implement schema, alongside the new
approving_maintainer and plan_note_id gate fields. The implement tool profile
takes over clarify's issue_write and memory-read grants."
```

## Task 4.3: ContractVersion 4

- [ ] **Step 1: Write the failing test**

Rewrite `internal/mcp/contract_test.go` in full:

```go
package mcp

import "testing"

func TestCheckContractVersion_Match(t *testing.T) {
	if err := CheckContractVersion("4"); err != nil {
		t.Fatalf("matching version must be accepted, got %v", err)
	}
}

func TestCheckContractVersion_UnsetIsAllowed(t *testing.T) {
	if err := CheckContractVersion(""); err != nil {
		t.Fatalf("unset TATARA_CONTRACT_VERSION must be allowed (workstation, tests), got %v", err)
	}
}

func TestCheckContractVersion_MismatchIsFatal(t *testing.T) {
	for _, got := range []string{"1", "3", "five", "4.0", " 4"} {
		if err := CheckContractVersion(got); err == nil {
			t.Fatalf("TATARA_CONTRACT_VERSION=%q must be refused", got)
		}
	}
}

func TestContractVersionIsFour(t *testing.T) {
	if ContractVersion != 4 {
		t.Fatalf("ContractVersion = %d, want 4 (must match tatara-operator and tatara-claude-code-wrapper)", ContractVersion)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
mise exec -- go test ./internal/mcp/ -run TestContractVersion -v
```

Expected: FAIL - `ContractVersion = 3, want 4`.

- [ ] **Step 3: Write the implementation**

`internal/mcp/contract.go:14`: `const ContractVersion = 4`. Extend its doc comment with one line naming the 3 -> 4 cause: the clarify profile deletion and the merged implement action enum.

- [ ] **Step 4: Run tests**

```bash
mise exec -- make test && mise exec -- make lint
```

- [ ] **Step 5: Commit and update MEMORY.md**

```bash
git add internal/mcp/contract.go internal/mcp/contract_test.go MEMORY.md
git commit -m "feat(mcp)!: contract version 4

submit_outcome's clarify profile is deleted and implement's action enum gained
approved/discuss/rejected. An operator on contract 3 must refuse this cli."
```

`MEMORY.md` line to add:

```
2026-08-XX: contract 3 -> 4 (#521). The clarify PROFILE is deleted from tatara-cli, not aliased: leaving kind=clarify valid with an optional approvingMaintainer would preserve a live path to approval that skips the new gate, which is the exact hole #521 exists to close. documentation stopped sharing implementOutcomeSchema in the same change - implement's action enum grew three gate actions a documentation agent has no handler for. The implement tool profile absorbed clarify's issue_write/memory_query/memory_describe grants; it deliberately did NOT gain task_list (D.6) or the memory WRITE tools.
```

## Verification (MR4)

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-cli
mise exec -- make test
mise exec -- make lint
mise exec -- pre-commit run --all-files
mise exec -- go run ./cmd/tatara tool-manifest | jq '.tools[] | select(.name=="submit_outcome") | .enums.action'
```

The last command must print all five implement actions plus the other profiles' unioned values. That output is what `tatara-agent-skills` CI will validate against in MR3 - eyeball it before merging.

**Post-merge, before MR3:** confirm the release published the asset.

```bash
curl -sfL https://github.com/szymonrychu/tatara-cli/releases/latest/download/tool-manifest.json \
  | jq '.tools[] | select(.name=="submit_outcome") | .enums.action'
```

Must contain `approved`, `discuss`, `rejected`. **MR3 does not open until this passes.**

---

# MR3 - tatara-agent-skills: the skill fold (LANDS AFTER MR4 RELEASES)

**Repo:** `tatara-agent-skills`. **Semver:** `major`.

## Files

- Delete: `skills/clarify/tatara-clarify-conversation/SKILL.md` and the `skills/clarify/` directory
- Move: `skills/clarify/tatara-triage-judgment/SKILL.md` -> `skills/implement/tatara-triage-judgment/SKILL.md`
- Create: `skills/implement/tatara-implement-gate/SKILL.md`
- Modify: `skills/investigation/tatara-research-followup/SKILL.md` (frontmatter `profiles` only, plus the clarify-kind prose)
- Modify: `skills/implement/tatara-implement-workflow/SKILL.md`
- Modify: `skills/review/tatara-review-checklist/SKILL.md`
- Modify: `skills/mcp/tatara-mcp-outcome/SKILL.md`
- Modify: `skills/mcp/tatara-mcp-scm/SKILL.md`
- Modify: `.github/scripts/validate_profiles.py`
- Modify: `.claude-plugin/plugin.json` (the `description` field hardcodes "all 42 skills")
- Modify: `ROADMAP.md` (delete the stale hard-lock item), `MEMORY.md`
- Modify: `docs/eval/` fixtures that name the clarify kind (grep first)

## Interfaces

- Consumes: MR4's released `tool-manifest.json`, whose `submit_outcome.enums.action` must already contain `approved`, `discuss`, `rejected`.
- Produces: a skills tag `vX.Y.Z` that MR7 pins `spec.agent.skillsRef` to.

## Task 3.1: resolve the hard-lock contradiction (do this first, it is a fact-finding step)

`validate_profiles.py`'s docstring claims all seven profiles are hard-locked. `ROADMAP.md:3` and `MEMORY.md`'s 2026-07-12 entry claim only `clarify` and `refine` are.

**The code is ground truth: `EXPECTED_PROFILE_SKILLS` (validate_profiles.py:52-103) has all seven keys populated, each with an exact set.** The docstring is right and `ROADMAP.md`/`MEMORY.md` are stale - they describe the state before the lock was widened, and the ROADMAP backlog item was never closed.

Consequence for this MR: **there is no "land the implement and review hard-locks" work to do; they already exist.** The design doc's instruction was written against the stale ROADMAP. What IS required is that the `implement` and `review` sets are UPDATED correctly in the same commit that moves the skills, or CI goes red - which is the same protection the instruction was reaching for. Deleting clarify's key and folding its members into `implement` therefore is not a net loss of coverage.

- [ ] **Step 1: Verify the claim before acting on it**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-agent-skills
python3 -c "
import ast,sys
src=open('.github/scripts/validate_profiles.py').read()
tree=ast.parse(src)
for node in tree.body:
    if isinstance(node,ast.Assign) and node.targets[0].id=='EXPECTED_PROFILE_SKILLS':
        d=ast.literal_eval(node.value)
        print(sorted(d)); print({k:len(v) for k,v in d.items()})
"
```

Expected: all seven profile names, each with a non-empty set. If this prints only `clarify` and `refine`, the ROADMAP is right and the code is stale - in that case ALSO add the five missing locks in this MR, as the design doc instructs.

- [ ] **Step 2: Delete the stale ROADMAP item**

Remove the `validate_profiles.py only hard-locks the clarify and refine skill sets` bullet from `ROADMAP.md`. Add a `MEMORY.md` line:

```
2026-08-XX: ROADMAP's "only clarify and refine are hard-locked" item (2026-07-12) was stale - EXPECTED_PROFILE_SKILLS has had all seven keys populated for some time and validate_profiles.py's own docstring says so. Dropped the item rather than inheriting the contradiction. Verify with the ast dump in the #521 plan, do not trust either doc.
```

## Task 3.2: delete the clarify skill set, move triage-judgment

- [ ] **Step 1: Write the failing test**

`validate_profiles.py` IS the test. Update `EXPECTED_PROFILE_SKILLS`:

```python
    # "clarify" is DELETED: the kind was folded into implement (#521).
    "implement": {
        "tatara-implement-conflict-resolution",
        "tatara-implement-gate",
        "tatara-implement-takeover",
        "tatara-implement-workflow",
        "tatara-mcp-code-graph",
        "tatara-mcp-scm",
        "tatara-pipeline-waiting",
        "tatara-research-followup",
        "tatara-triage-judgment",
    },
```

and delete the whole `"clarify": {...}` block (lines 71-78 on `origin/main`). Update the docstring's `implement` bullet:

```
- `implement`: the merged implement kind - it conducts the issue conversation,
  runs the approval gate, and writes the code. It gets the three implement
  skills, the gate skill, the triage-judgment rubric and the research-followup
  discipline the deleted clarify profile used to own, plus code-graph, SCM and
  pipeline-waiting references. Must never pick up `task_list`-broad-context
  skills (D.6: implement has no `task_list`).
```

and delete the `clarify` bullet.

- [ ] **Step 2: Run to verify it fails**

```bash
python3 -m pip install --quiet pyyaml
python3 .github/scripts/validate_profiles.py
```

Expected: FAIL - `tatara-triage-judgment` and `tatara-research-followup` tag `clarify`, not `implement`; `tatara-implement-gate` does not exist; `tatara-clarify-conversation` tags a profile that is no longer expected.

- [ ] **Step 3: Move and retag**

```bash
git mv skills/clarify/tatara-triage-judgment skills/implement/tatara-triage-judgment
git rm -r skills/clarify/tatara-clarify-conversation
rmdir skills/clarify 2>/dev/null || true
```

Then in `skills/implement/tatara-triage-judgment/SKILL.md`, change:

```yaml
profiles: ["clarify", "refine"]
```

to

```yaml
profiles: ["implement", "refine"]
```

and rewrite its `description` and the kind table. The current table reads:

```
| your kind | the outcome |
|---|---|
| `clarify` | `submit_outcome(decision="implement"\|"close"\|"discuss", reason, approval_citations)` |
| `refine` | `submit_outcome(folds=[...], closes=[...], links=[...])` - a close is an entry in `closes[]`, with its `reason` |
```

Replace with:

```
| your kind | the outcome |
|---|---|
| `implement` | `submit_outcome(action="approved"\|"rejected"\|"discuss", reason, approving_maintainer, plan_note_id, approval_citations)` |
| `refine` | `submit_outcome(folds=[...], closes=[...], links=[...])` - a close is an entry in `closes[]`, with its `reason` |
```

Then sweep the body: every `decision="implement"` becomes `action="approved"`, every `decision="close"` becomes `action="rejected"`, every `decision="discuss"` becomes `action="discuss"`. Every `parks at identity-unverified` becomes `returns granted:false and you keep talking` (the refusal no longer parks - see MR6 Task 6.6). Add one paragraph naming the two NEW refusals the operator can return, `approver-not-maintainer` and `approver-mismatch`, and what to do about each.

In `skills/investigation/tatara-research-followup/SKILL.md`, change:

```yaml
profiles: ["clarify"]
```

to

```yaml
profiles: ["implement"]
```

and rewrite its `description` first clause from "on a clarify turn" to "on an implement turn, before or during the approval gate". Keep the silence-over-noise discipline verbatim - it matters MORE under a long-lived pod, not less: a live pod that posts on every wake is the forty-comment loop. Say that explicitly in one added sentence.

- [ ] **Step 4: Run the validators**

```bash
python3 .github/scripts/validate_skills.py
python3 .github/scripts/validate_profiles.py
```

Expected: `validate_profiles.py` now fails ONLY on the missing `tatara-implement-gate`.

- [ ] **Step 5: Commit**

```bash
git add -A skills/ .github/scripts/validate_profiles.py ROADMAP.md MEMORY.md
git commit -m "feat(skills)!: delete the clarify skill set, move triage-judgment to implement"
```

## Task 3.3: the new tatara-implement-gate skill

- [ ] **Step 1: Create the skill**

Create `skills/implement/tatara-implement-gate/SKILL.md`. It absorbs Branch A of the deleted clarify skill and carries the withdrawal-veto paragraph verbatim from the operator's `internal/controller/assignment.go:151-154`:

```markdown
---
name: tatara-implement-gate
description: "STEP 0 of every implement turn on an unapproved issue: digest the ask, research it against the code, write the plan to task_note(kind=\"plan\") AND post it in the issue thread, then STOP and wait. On a go-ahead, call submit_outcome(action=approved) with the approving maintainer, the citation and the plan note id. Write no code until the operator returns granted:true."
profiles: ["implement"]
---

# The implement gate

You are ONE agent for the whole issue. You digest the ask, you agree a plan with
the maintainer, and then you write the code. This skill covers everything up to
and including the go-ahead. `tatara-implement-workflow` covers what happens
after.

**You do not write code before the gate opens.** Not a scaffold, not a branch,
not a "just to see if it works". The gate is the only thing standing between an
agent's reading of a thread and a merged, tagged, auto-deployed release.

## 1. Digest and research

Your turn-0 bundle carries every Issue your Task owns, each with its full
comment thread, plus every prior note. Do NOT re-crawl the forge to reconstruct
history that is already in your prompt.

Read the issue. Identify what outcome the human wants, what is ambiguous, and
what a reasonable engineer would need to know before implementing. Then ground
it in the code with `code_search`, `code_explain` and `code_context(rel="related")`,
and where the ask spans repos, one `explorer` subagent per implicated repo. See
`tatara-research-followup` for the research discipline, and obey its
silence-over-noise rule without exception: if no human has replied since your
last comment, post nothing.

If the code-graph tools return `MEMORY_DEGRADED`, read the on-disk repos
directly instead, report it ONCE, and continue the turn.

## 2. Write the plan, twice

The plan goes in TWO places and both are load-bearing:

    task_note(kind="plan", body="...")

is the continuation state and the thing the operator HASHES. Keep the id it
returns - you need it for `plan_note_id`.

    issue_write(action="comment", repo=..., number=..., body="...")

is what the maintainer actually reads. Post the same plan. If they diverge, the
maintainer approves one thing and the operator pins another.

A plan is: the scope, the repos in play, the approach, the constraints you found
in the code, and the 1-3 real ambiguities you still need answered. Do not ask
questions answerable from the issue text or the code.

## 3. Stop

Submit `submit_outcome(action="discuss", reason="...")` and stop. There is no
polling loop and no wall-clock wait. Your pod may stay warm; the operator will
give you the maintainer's reply as a new turn when it arrives. Sitting in a poll
loop burns your turn budget and buys nothing.

**Never answer your own last comment.** If the most recent comment on an issue
is your own with no human reply since, do not post again.

## 4. On a go-ahead, report it with evidence

    submit_outcome(action="approved",
                   reason="...",
                   approving_maintainer="<their login>",
                   plan_note_id="<the id task_note returned>",
                   approval_citations=[{"id": "...", "quote": "..."}])

- **You judge what the comment MEANS.** There is no wordlist. "go ahead, I
  approve!", "continue", "yep do it" are all approvals if that is what the
  maintainer meant.
- **The operator judges who wrote it and whether you quoted it honestly.** It
  re-reads that exact comment from its own mirror and refuses if the comment is
  not on that issue, if the author is not a verified maintainer, if the author is
  the bot, if your quote does not occur in the body it holds, or if that comment
  has already been consumed as approval evidence.
- `approving_maintainer` is a DECLARATION, not a second authority. The operator
  refuses with `approver-not-maintainer` if that login is not a maintainer, and
  with `approver-mismatch` if it is not the author of the comment you cited. The
  citation remains the sole authority; the login must simply AGREE with it.
- `plan_note_id` names the plan the maintainer approved. The operator hashes that
  note's body at grant and re-checks the hash before you write code. If you edit
  the plan after approval, the gate refuses.
- One go-ahead on one issue does not approve a Task that owns four. Every live
  Issue that a human has commented on needs its own comment and its own citation.
  A live Issue with NO human comment at all needs neither.

**IT DOES NOT CHECK RECENCY, SO THE WITHDRAWAL VETO IS YOURS. Read every
maintainer comment newer than the one you want to cite. A benign follow-up
("ping me when the PR is up") leaves the go-ahead standing; one that takes it
back ("actually hold off") means `action=discuss` instead. Nothing downstream
catches this.**

## 5. Read the result before you do anything else

`submit_outcome(action="approved")` returns `granted: true` or
`granted: false, reason: "...", declared: "..."`.

- **`granted: true`** - the gate is open. Go to `tatara-implement-workflow` and
  start writing code.
- **`granted: false`** - you are still alive and the conversation is still open.
  Post the returned `reason` in the thread so the maintainer can see what was
  missing, then submit `action="discuss"` and keep talking. Do NOT resubmit the
  same citation; it will be refused the same way. Do NOT start writing code.

A refusal is not an error and not a park. It is the gate working.

## 6. Changing the plan after approval

If the work turns out to need a different plan, say so in the thread, write a
NEW `task_note(kind="plan")`, and go back to step 3. That is the cheap path and
it is the intended one. Do not keep coding against a plan you have abandoned in
order to dodge a second gate - the plan note is the continuation state, and a
stale one is worse than a second approval round.

## Anti-patterns

- Writing any code, opening any branch, or opening an MR before `granted: true`.
- Posting a plan comment without writing the matching `task_note(kind="plan")`,
  or writing a note whose body differs from the comment.
- Reporting `action="approved"` on an issue whose thread has no maintainer
  comment you can honestly read as a go-ahead.
- Citing a comment a later maintainer comment took back. The operator does not
  check recency; you are the veto.
- Paraphrasing an `approval_citations` quote instead of copying it verbatim.
- Setting `approving_maintainer` to someone other than the author of the comment
  you cited.
- Re-posting a comment that only re-requests approval when no human has replied.
- Answering under your own last comment.
- Polling or waiting for a human reply instead of submitting `discuss`.
- Re-crawling forge history already present in the turn-0 bundle.
```

- [ ] **Step 2: Run the validators**

```bash
python3 .github/scripts/validate_skills.py
python3 .github/scripts/validate_profiles.py
python3 .github/scripts/validate_tool_calls.py
```

Expected: all three exit 0. `validate_tool_calls.py` is the one that proves MR4 released first - if it reports `"approved" is not a valid action for submit_outcome`, **STOP: MR4 has not published its manifest.** Do not work around it.

- [ ] **Step 3: Commit**

```bash
git add skills/implement/tatara-implement-gate/
git commit -m "feat(skills): add tatara-implement-gate

Absorbs Branch A of the deleted clarify skill: plan, post, stop, cite, and read
granted before writing a line of code."
```

## Task 3.4: edit the four downstream skills

- [ ] **Step 1: `tatara-implement-workflow`**

Add a `## 0a. The gate is a precondition` section immediately after `## 0. Understand your context`:

```markdown
## 0a. The gate is a precondition

If your Task's issue has not been approved yet, you are not in this skill yet.
Go to `tatara-implement-gate`, agree a plan, and come back when
`submit_outcome(action="approved")` returned `granted: true`. There is no path
into code that does not go through it.
```

In `## 4. Commit discipline`, restate the per-turn push rule with the warm-pod reason:

```markdown
**Push at the end of every turn, without exception.** Your pod now lives across
several turns instead of one, so the window in which uncommitted work can be
lost to a TTL rotation, an eviction, or a node drain is much longer than it used
to be. Uncommitted work is more valuable and more fragile than it was. `git add
-A && git commit && git push` is the last thing you do before `task_note`.
```

Add an issue-of-record paragraph to `## 3. Several issues under one Task`:

```markdown
**One issue is the record.** The issue you were gated on is where the
conversation lives for the whole life of this Task, through code, review and
merge. Do not open a second issue to discuss the same work, and do not move the
conversation to the MR thread - a maintainer following the issue will not see
it.
```

- [ ] **Step 2: `tatara-review-checklist`**

Add to `## Step 7 - Finish`:

```markdown
**A verdict does not end you.** Your pod stays live after `submit_outcome`. If
the head moves and you are asked to review again, you are the SAME agent with
the same notes - diff the new head against your PRIOR findings note rather than
re-reviewing from scratch, and say in your findings which of your earlier points
were addressed. Repeating a finding the implementer already fixed is how a
review loop becomes a ping-pong.
```

- [ ] **Step 3: `tatara-mcp-outcome`**

Delete the `### clarify` section entirely. Rewrite `### implement / documentation` into two sections. The implement one:

```markdown
### implement

```
submit_outcome(action="approved", reason, approving_maintainer, plan_note_id, approval_citations)
submit_outcome(action="discuss", reason)
submit_outcome(action="rejected", reason)
submit_outcome(action="submitted", title, body, change_significance, merge_order?)
submit_outcome(action="declined", decline_reason)
```

The first three are the GATE (see `tatara-implement-gate`); the last two are the
code (see `tatara-implement-workflow`). One agent, five actions, one turn each.

- `action="approved"` returns `{granted: true}` or
  `{granted: false, reason, declared}`. **`granted:false` is a normal result, not
  an error, and it does NOT stop you.** Post the reason in the thread and submit
  `action="discuss"`. Never write code on a `granted:false`.
- `approving_maintainer` must be the author of the comment you cited.
  A mismatch is refused with `approver-mismatch`; a non-maintainer login is
  refused with `approver-not-maintainer`.
- `plan_note_id` is the id `task_note(kind="plan")` returned. The operator hashes
  that note at grant and re-checks it before you write code.
- `change_significance` is `major` / `minor` / `patch`. YOU own this level. A
  reviewer may raise it. Nobody can lower it. It becomes the release tag.
- `merge_order` is REQUIRED the moment your change spans more than one repo.
- `action=declined` needs a real reason. "Not doing this" is not one.
```

and a `### documentation` section carrying the old two-action shape verbatim, with one line saying it deliberately has no gate.

- [ ] **Step 4: `tatara-mcp-scm`**

Change the `profiles` frontmatter from

```yaml
profiles: ["implement", "review", "clarify", "refine", "brainstorm", "incident", "documentation"]
```

to

```yaml
profiles: ["implement", "review", "refine", "brainstorm", "incident", "documentation"]
```

and add a create-once-then-comment-only paragraph to `## issue_write`:

```markdown
**Create once, then comment.** Under the merged implement kind you are the same
agent from the first triage comment to the merged MR, so you will be back on
this thread many times. `issue_write(action="create")` is for a genuinely new
piece of work, not for restating one you already filed. If you find yourself
about to create an issue whose subject you have already commented on, comment
instead.
```

- [ ] **Step 5: `.claude-plugin/plugin.json`**

Update the `description` field's skill count. Do NOT touch `version` - it is pipeline-owned.

```bash
find skills -name SKILL.md | wc -l   # use this number in the description
```

- [ ] **Step 6: Run every validator and the plugin-manifest check**

```bash
python3 .github/scripts/validate_skills.py
python3 -m pip install --quiet pyyaml && python3 .github/scripts/validate_profiles.py
python3 .github/scripts/validate_tool_calls.py
python3 - <<'PY'
import json,pathlib
mk=json.loads(pathlib.Path(".claude-plugin/marketplace.json").read_text())
pl=json.loads(pathlib.Path(".claude-plugin/plugin.json").read_text())
assert "name" in mk and isinstance(mk.get("plugins"), list)
assert "name" in pl and "description" in pl
print("plugin manifests OK")
PY
grep -rn "clarify" skills/ docs/ .github/ README.md || echo "no stale clarify references"
```

The final grep must return only deliberate historical references (e.g. a MEMORY note). Any live instruction naming the clarify kind is a bug.

- [ ] **Step 7: Commit and update MEMORY.md**

```
2026-08-XX: clarify skill set deleted (#521). tatara-triage-judgment moved to skills/implement/ and retagged ["implement","refine"]; tatara-research-followup retagged ["implement"] because its silence-over-noise discipline matters MORE under a long-lived pod, not less. New tatara-implement-gate absorbs Branch A of the dead clarify skill and carries the withdrawal-veto paragraph verbatim from the operator's assignment.go so the two cannot drift. ORDERING: this MR could not merge until tatara-cli published a release carrying the new action enum - validate_tool_calls.py fetches the "latest" tatara-cli release manifest and HARD-fails on an unknown enum literal (same shape as the 2026-07-28 action="exhausted" entry). The design doc had MR3 before MR4; that order fails CI. ROADMAP's stale hard-lock item dropped: all seven profiles have been locked for some time.
```

## Verification (MR3)

There is no `mise` and no `.pre-commit-config.yaml` in this repo (`ROADMAP.md` records that the latter was planned and never landed). The verification is exactly what `lint.yml` runs, in order:

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-agent-skills
python3 .github/scripts/validate_skills.py
python3 -m pip install --quiet pyyaml && python3 .github/scripts/validate_profiles.py
python3 .github/scripts/validate_tool_calls.py
```

---

# MR5 - tatara-claude-code-wrapper: ContractVersion 4

**Repo:** `tatara-claude-code-wrapper`. **Semver:** `major`.

## The design doc's "no test edits needed" claim is FALSE. Verified.

The design doc says "its tests compare symbolically, so NO wrapper test edits needed - the one repo where the bump is a one-line change". Checked against `origin/main`:

| site | symbolic? |
|---|---|
| `internal/session/session_test.go:87` `require.Equal(t, version.ContractVersion, m.Snapshot().ContractVersion)` | YES - safe |
| `internal/httpapi/messages_test.go:388` `require.Equal(t, float64(version.ContractVersion), got["contractVersion"], ...)` | YES - safe (this one WAS a literal `float64(2)` two commits ago and was fixed) |
| `internal/httpapi/messages_test.go:48` `session.Snapshot{State: session.Ready, ContractVersion: version.ContractVersion}` | YES - safe |
| **`internal/version/version_test.go:5-8` `TestContractVersionIsThree` / `if ContractVersion != 3`** | **NO - hardcoded. This test MUST be edited.** |

So MR5 is a two-file change, not a one-line change. It is still the smallest MR in the landing.

## Files

- Modify: `internal/version/version.go` (line 20)
- Modify: `internal/version/version_test.go` (lines 5-8)
- Modify: `MEMORY.md`

## Task 5.1

- [ ] **Step 1: Write the failing test**

Replace `internal/version/version_test.go`'s pin test:

```go
func TestContractVersionIsFour(t *testing.T) {
	if ContractVersion != 4 {
		t.Fatalf("ContractVersion = %d, want 4 (must match tatara-operator and tatara-cli)", ContractVersion)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-claude-code-wrapper
mise exec -- go test ./internal/version/ -run TestContractVersion -v
```

Expected: FAIL - `ContractVersion = 3, want 4`.

- [ ] **Step 3: Write the implementation**

`internal/version/version.go:20`: `const ContractVersion = 4`.

- [ ] **Step 4: Prove the OTHER tests really are symbolic**

```bash
mise exec -- make test
grep -rn "ContractVersion" --include="*_test.go" .
```

Expected: `make test` green with no further edits, and every remaining hit reads `version.ContractVersion`, never a numeric literal. If any test fails, the symbolic claim was wrong for that site too - fix it and note the site in `MEMORY.md`.

- [ ] **Step 5: Commit**

```bash
git add internal/version/
git commit -m "feat!: contract version 4

tatara-cli deleted the clarify submit_outcome profile and merged its decisions
into implement's action enum. The operator refuses this image unless it is on 4."
```

## Verification (MR5)

```bash
mise exec -- make test
mise exec -- make lint
mise exec -- make chart-test
mise exec -- pre-commit run --all-files
```

## Companion plan file

`tatara-claude-code-wrapper` HAS a `docs/superpowers/plans/` convention. Write `docs/superpowers/plans/2026-08-07-521-contract-version-4.md` containing: the one-paragraph why, the corrected test-audit table above, and a pointer to this master plan by path. Nothing else - the MR is two lines.

---

# MR6 - tatara-operator: the whole model

**Repo:** `tatara-operator`. **Semver:** `major`. **Lands in the same maintenance slot as MR7.**

## The shape of the change

| deleted | replaced by |
|---|---|
| `status.stage` (16 values) | `status.state` (8 values) |
| `status.stageReason` | `status.parkReason` (28 values, `""` == not parked) + `status.stateReason` |
| `StageParked` constant (102 prod + 312 test refs) | the `parkReason != ""` flag |
| `StageConversing` | `stage.Live(state)` liveness |
| `StageClarifying` | folded into `refined` |
| `StageFailed` | park with a timer-backoff reason at `new`/`refined` |
| `StageTriaging`/`StageBrainstorming`/`StageInvestigating`/`StageRefining`/`StageApproved`/`StageImplementing`/`StageReviewing`/`StageMerging`/`StageDeploying`/`StageDelivered`/`StageDocumenting` | the 8 states |
| `terminalStages`, `StageTerminal`, `podlessStages` | `operatorDriven = {merged, deployed}` |
| `stage.HasReentry`, `reentryReasons` | `stage.UnparkClass(parkReason)` |
| `stage.UnparkTargetForBindingRepair` + its call site | (nothing - deleted) |
| `internal/controller/resume.go` (291 lines) | (nothing - the bug class it worked around is gone) |
| `clarify` agent kind, `clarifyPayload`, `o.clarify` | `implement` kind, `implementPayload` action values, `o.gate` |

## The 8 states, and the field shape

```go
// api/v1alpha1/task_types.go
//
// State* are the 8 members of the task lifecycle. They name WHERE THE WORK IS,
// and nothing else: whether a Task is stalled is status.parkReason, and whether
// a live agent is attached is stage.Live(state). Those two are ORTHOGONAL to
// this enum, deliberately - the 16-stage machine folded all three axes into one
// value, which is how `parked` came to be simultaneously a stage, a terminal,
// and a pod-less marker, and how TaskDone(parked) ended up true.
//
// NAMING TRAP: `merged` and `deployed` name the PHASE, not the milestone.
// merged means "the verdict is approve and the operator owns the merge cursor",
// NOT "every MR is merged". Per-MR truth stays on mr.status.state.
const (
	StateNew                 = "new"
	StateRefined             = "refined"
	StateUnderImplementation = "under-implementation"
	StateAwaitingReview      = "awaiting-review"
	StateMerged              = "merged"
	StateDeployed            = "deployed"
	StateDone                = "done"
	StateRejected            = "rejected"
)
```

```go
	// +kubebuilder:validation:Enum=new;refined;under-implementation;awaiting-review;merged;deployed;done;rejected
	// +optional
	State string `json:"state,omitempty"`
	// StateEnteredAt is stamped on EVERY state transition.
	// +optional
	StateEnteredAt *metav1.Time `json:"stateEnteredAt,omitempty"`
	// StateReason is the machine reason for the current state. MANDATORY on
	// done and rejected.
	// +optional
	StateReason string `json:"stateReason,omitempty"`
	// ParkReason is the PARK FLAG. Empty means NOT PARKED, and that is the
	// whole test - there is no parked state to compare against. It is
	// ORTHOGONAL to State: a Task parks WHERE IT IS, and un-parking returns it
	// to the same state unless a rule says otherwise.
	//
	// THE WEDGE THIS INVITES, and the one mitigation: a writer that sets State
	// and forgets to clear ParkReason wedges the Task forever with a stale
	// reason - the same silent-drift genre as #521 itself. Mitigation:
	// stage.Enter asserts ParkReason == "" on every non-park edge and refuses
	// otherwise, and stage.Unpark is the ONE function that clears it. Nothing
	// else in the codebase may assign to this field.
	// +kubebuilder:validation:Enum=backlog-sweep;triage-stalled;name-too-long;stage-deadline;awaiting-human;identity-unverified;implement-declined;review-loop-exhausted;review-post-refused;merge-timeout;merge-blocked;merge-order-missing;deploy-timeout;deploy-blocked;no-outcome;turn-budget-exhausted;pod-recreation-exhausted;object-too-large;fold-adoption-unverified;admission-starved;agent-contract-mismatch;operator-error;head-moving;handoff-stalled;ownership-lost;merge-auth-refused;ci-red;ci-blocked
	// +optional
	ParkReason string `json:"parkReason,omitempty"`
	// +optional
	ParkedAt *metav1.Time `json:"parkedAt,omitempty"`
	// ParkedFromState is OBSERVABILITY. The un-park target is NEVER derived
	// from it. It is load-bearing for exactly one gate: no-outcome un-park
	// eligibility requires it to be under-implementation or awaiting-review
	// (#406).
	// +optional
	ParkedFromState string `json:"parkedFromState,omitempty"`
```

All six re-entry counters (`MergeReentries`, `DeployReentries`, `HumanReviewRounds`, `CIRedReentries`, `HeadMoveReentries`, `StageElapsedCarrySeconds`) stay exactly where they are and keep their names. They are read ACROSS the park boundary and a `ParkState` struct would degenerate.

## The transition table: 21 edges

```
(create)              -> new | refined
new                   -> refined | rejected
refined               -> under-implementation | done | rejected
under-implementation  -> awaiting-review | refined | rejected
awaiting-review       -> under-implementation | merged | done | rejected
merged                -> deployed | awaiting-review | under-implementation | rejected
deployed              -> done
done | rejected       -> (reap)
```

**`refined -> done` is a 21st edge the design doc's table does not have, and it is REQUIRED.** The design's 20-edge table gives the non-code kinds no terminal path: a `brainstorm` Task's outcome is `propose`/`skip` and it finishes without ever opening an MR; the same is true of `refine` (folds/closes/links) and of `incident` (`file_issue` mints a tracker and stops). Under the 20-edge table those three kinds can only reach `done` via `awaiting-review -> done` or `deployed -> done`, neither of which they traverse. One edge fixes all three. State this in the MR description as a deliberate amendment with its reason.

## Agent kind is no longer a function of state alone

The old `agentKinds` table mapped stage -> kind and worked because stages were kind-specific (`brainstorming`, `investigating`, `refining`, `clarifying`). With eight generic states it cannot: a `brainstorm` Task in `refined` needs a brainstorm agent, not an implement one.

```go
// AgentKindFor is the F.2 table, now keyed on (state, spec.kind) because the
// state enum is kind-agnostic. It returns "" for an operator-driven state.
func AgentKindFor(state, specKind string) string {
	switch state {
	case v1alpha1.StateRefined, v1alpha1.StateUnderImplementation:
		return originAgentKinds[specKind]
	case v1alpha1.StateAwaitingReview:
		return AgentReview
	default:
		// new is operator triage; merged and deployed are operatorDriven; done
		// and rejected run nothing.
		return ""
	}
}

// originAgentKinds maps Task.spec.kind (the ORIGIN) to the agent kind that runs
// the work states. It is DATA, not a switch with a default: an origin kind not
// in it maps to "", which fails closed at pod spawn.
var originAgentKinds = map[string]string{
	"implement":     AgentImplement,
	"takeover":      AgentImplement,
	"review":        AgentReview,
	"brainstorm":    AgentBrainstorm,
	"incident":      AgentIncident,
	"refine":        AgentRefine,
	"documentation": AgentDocumentation,
}
```

`AgentClarify` is deleted. `Spec.Kind`'s enum loses `clarify` and gains `implement`; `unconstrainedKinds` in `api/v1alpha1/task_types.go:58-62` swaps `"clarify"` for `"implement"`. **This is a consequence the design doc does not spell out and it is not optional:** the migrator rewrites 61 Tasks' `spec.kind` from `clarify` to `implement`, and `IsKnownKind("implement")` must be true or the QueuedEvent validator rejects every one of them.

## Liveness

```go
// internal/stage/liveness.go
//
// liveStates are the states that carry a LIVE agent pod - one whose clock is
// the IDLE timer on status.conversationLastEventAt rather than a work budget,
// and into which a queued event is delivered as a further turn. It is the
// `conversing` stage's mechanism, promoted to a property so it composes with
// parkReason instead of competing with it.
//
// A parked Task is NEVER live, whatever its state says: park is what takes the
// pod down. That is enforced by the reconciler (see repairParkedWithLivePod),
// not by this table, because the table is pure and the pod is not.
var liveStates = map[string]bool{
	v1alpha1.StateRefined:             true,
	v1alpha1.StateUnderImplementation: true,
	v1alpha1.StateAwaitingReview:      true,
}

// Live reports whether state carries a live agent pod.
func Live(state string) bool { return liveStates[state] }

// operatorDriven replaces podlessStages. Two members, not eight: new runs the
// triage pass and then leaves immediately, done/rejected run nothing and are
// reaped, and the three live states are covered by Live. What is left is the
// two states where the OPERATOR does the work - merging and deploying.
var operatorDriven = map[string]bool{
	v1alpha1.StateMerged:   true,
	v1alpha1.StateDeployed: true,
}

// OperatorDriven reports whether the operator, not an agent, advances state.
func OperatorDriven(state string) bool { return operatorDriven[state] }
```

## Park-reason classification: three closed sets on one axis

The 36 members of the current `Reasons` slice split with no remainder:

- **RejectReasons (6)** -> `rejected`: `declined`, `false-positive`, `tracked-elsewhere`, `issue-closed`, `mr-closed-externally`, `mr-taken-over`.
- **DoneReasons (2)** -> `done`: `doc-timeout`, `mr-merged-externally`.
- **ParkReasons (28)**: everything else.

The 28 park reasons divide on ONE axis, who un-parks:

```go
// UnparkClass is the ONE axis park reasons divide on: who un-parks. It replaces
// stage.HasReentry, whose boolean could not distinguish "a human's comment
// resumes this" from "a timer retries this" from "nothing ever does".
type UnparkClass int

const (
	UnparkNever UnparkClass = iota // ages out at ParkRetention and is reaped
	UnparkHuman                    // a non-bot comment resumes it
	UnparkTimer                    // a backoff timer retries it, bounded by a counter
)

var unparkClasses = map[string]UnparkClass{
	// A human's comment. These are the four that were comment-driven before.
	ReasonBacklogSweep:       UnparkHuman,
	ReasonAwaitingHuman:      UnparkHuman,
	ReasonIdentityUnverified: UnparkHuman,
	ReasonHandoffStalled:     UnparkHuman,

	// A backoff timer. THE MAINTAINER'S "the backup reconcile loop retries it"
	// rule, made real: every reason that used to target `failed` is here, and a
	// timer un-park re-enters at new or refined with a bounded counter.
	ReasonMergeTimeout:           UnparkTimer,
	ReasonDeployTimeout:          UnparkTimer,
	ReasonNoOutcome:              UnparkTimer,
	ReasonTriageStalled:          UnparkTimer,
	ReasonOperatorError:          UnparkTimer,
	ReasonObjectTooLarge:         UnparkTimer,
	ReasonAgentContractMismatch:  UnparkTimer,
	ReasonMergeOrderMissing:      UnparkTimer,
	ReasonAdmissionStarved:       UnparkTimer,
	ReasonMergeAuthRefused:       UnparkTimer,
	ReasonFoldAdoptionUnverified: UnparkTimer,

	// Nobody. These are exhaustion terminals: re-entering one would escape its
	// own cap one lap at a time.
	ReasonStageDeadline:          UnparkNever,
	ReasonNameTooLong:            UnparkNever,
	ReasonImplementDeclined:      UnparkNever,
	ReasonReviewLoopExhausted:    UnparkNever,
	ReasonReviewPostRefused:      UnparkNever,
	ReasonMergeBlocked:           UnparkNever,
	ReasonDeployBlocked:          UnparkNever,
	ReasonTurnBudgetExhausted:    UnparkNever,
	ReasonPodRecreationExhausted: UnparkNever,
	ReasonHeadMoving:             UnparkNever,
	ReasonCIRed:                  UnparkNever,
	ReasonCIBlocked:              UnparkNever,
	ReasonOwnershipLost:          UnparkNever,
}
```

`ownership-lost` stays `UnparkNever` in the TABLE and is the ONE documented exception: `takeover_mint.go` clears it directly through `stage.UnparkTakeover`, the only function permitted to clear `parkReason` and change `State` in the same write. Everything else goes through `stage.Unpark`, which never changes `State`.

## Decisions carried from the reconciled design, and their reasons

These belong in the MR description. They are the questions a reviewer will ask, already answered.

**One live pod per Task; serialise.** The July objection (`MEMORY.md:1054`) is that a per-Task-named pod carried across a stage change silently runs the wrong kind, model, profile and skills. That objection DISSOLVES on `refined -> under-implementation`, where the clarify/implement merge makes all four identical - and only there. It does NOT dissolve on `under-implementation -> awaiting-review`. Re-keying pod names to `(task, kind)` so implement and review could be simultaneously live was priced at roughly 3x the operator effort, for latency the maintainer did not ask to remove. **Decision: serialise. One live pod per Task, kind follows state, `EnterStage` tears the old one down exactly as it does today.**

**Do not adopt `kubernetes-sigs/agent-sandbox`.** Its differentiator is PVC checkpoint/resume; tatara deliberately keeps continuation in `Task.status.notes` plus git and treats the workspace as transient (`internal/agent/ttlstop.go:37-41`). The pod holds an interactive `claude` over a PTY, E2B documents that live PTY streams are dropped on pause, and `MEMORY.md` (2026-07-27) already records that even mid-turn PTY injection races the Stop hook - which is why `/v1/interject` stays deleted. The transferable idea, a claim object decoupling workload from backing pod, is already implemented: the Task CR is that object.

**Dispatch stays as-is.** Authenticated push to the wrapper's existing HTTP API. ARC-style long-poll is only correct if agent pods ever run outside the cluster, and they do not.

**Kind-fold non-issues, stated so nobody writes a migration for them.** Pod names read a stored annotation, not a computed kind (`internal/agent/pod.go:207 podNameIDSegment`, `:305 StampPodName`), so a Task whose kind changes keeps its pod name and nothing breaks. `TaskName` embeds the kind in the CR name but **nothing parses it back out** - grepped, zero readers. Neither needs migrating.

**Migration alternatives, all rejected:**

- *v1alpha2 plus a conversion webhook.* Conversion is never invoked for same-version objects, and the chart has no webhook Service, no serving cert and no cert-manager Certificate. Days of new API machinery for 118 objects.
- *Widen-then-narrow across two releases.* Two releases and a deferred phase 2, which ground rule 3 forbids outright.
- *Bulk delete and re-mint.* The reaper uses `DeletePropagationBackground`, so it cascades owned Issue and MergeRequest CRs. Switching to orphan propagation instead manufactures exactly the dangling-ownerRef population MR1 exists to fix.

**Why the migration is safe.** The migrator's writes are valid on their face - it sets one of the 8 states and one of the 28 park reasons - so ratcheting is not needed for the migration itself. Zero Tasks are in an active state (measured: all 118 are parked, rejected, failed or delivered), so the rollout window in which Helm has applied the narrowed CRD but the OLD operator Deployment is still running sees only mirror-sync writes, which status-subresource ratcheting covers at v1.33.0. Use `helm upgrade --wait`.

**Dangling ownerRefs are NOT a `RepairZeroController` case.** That handles "no ref carries `controller=true`"; this is "a ref names a Task that does not exist". They ship with MR1, not here.

## Test helpers

Several tests below call fixture helpers that do not exist yet. Add them to `internal/controller/export_test.go` (which already exists for exactly this) and to a new `internal/stage/export_test.go` and `internal/restapi/export_test.go`. They are ordinary constructors, not abstractions:

| helper | package | returns |
|---|---|---|
| `task(state string)` | `stage_test` | a `*v1alpha1.Task` in that state, `spec.kind=implement`, `StateEnteredAt` set |
| `liveTask(state string)` | `controller_test` | as above plus `StateWorkStartedAt` and a `PodName` |
| `taskIn(name, state, parkReason string)` | `controller_test` | a named Task with the park flag pre-set (bypassing `stage.Park`, so the test can build an invalid state deliberately) |
| `taskIdleSince(name, state string, at time.Time)` | `controller_test` | `taskIn` plus `ConversationLastEventAt` |
| `fakeReaderWith(objs ...client.Object)` | `controller_test` | `fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()` |
| `projectWith(maxAgents, maxLive int)` | `controller_test` | a `*v1alpha1.Project` |
| `legacyTask(stage, reason, kind, parkedFrom string)` | `migrate_test` | a Task carrying the OLD fields, built from an unstructured literal since the typed fields are gone |
| `postApproved`, `decodeGate`, `validApproval`, `reloadIssue`, `rewritePlanNote` | `restapi_test` | thin wrappers over the existing `postOutcome` helper |

`legacyTask` is the only non-obvious one: after Task 6.1 the Go type has no `Stage` field, so the migrator's input must be built as an `unstructured.Unstructured` (or the migrator must read `status.stage` through the unstructured path). **Decide this in Task 6.11 before writing the mapper** - it determines whether `MapOne` takes a `*v1alpha1.Task` or a `map[string]any`. Recommendation: `MapOne(old LegacyStatus) Plan` where `LegacyStatus` is a tiny struct the migrator populates from an unstructured read. That keeps the mapper pure and testable without any cluster.

## MR6 task list

The tasks are ordered by dependency. Tasks 6.1 through 6.3 are one long compile-broken window; that is intentional and it is what makes the sweep exhaustive. Do NOT try to keep `go build` green across 6.1-6.3.

---

## Task 6.1: the API types

**Files:**
- Modify: `api/v1alpha1/task_types.go`
- Modify: `api/v1alpha1/constants.go`
- Create: `api/v1alpha1/state_test.go`
- Delete: `api/v1alpha1/stage_test.go` (3 `StageParked` + 3 `StageConversing` + 3 `StageClarifying` + 3 `StageFailed` refs; its subject no longer exists)
- Regenerate: `api/v1alpha1/zz_generated.deepcopy.go`, `charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml`

**Interfaces:**
- Produces: `StateNew`..`StateRejected`, `TaskStatus.{State,StateEnteredAt,StateReason,ParkReason,ParkedAt,ParkedFromState}`, `Parked(t) bool`, `TaskDone(t) bool`, `Note.ID`, `MaxLivePods(p)`, `DefaultMaxLivePods`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing tests**

Create `api/v1alpha1/state_test.go`:

```go
package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// PURE UNIT. No envtest.
func TestTaskDoneIsExactlyDoneAndRejected(t *testing.T) {
	for _, state := range []string{v1alpha1.StateDone, v1alpha1.StateRejected} {
		tk := &v1alpha1.Task{Status: v1alpha1.TaskStatus{State: state}}
		require.True(t, v1alpha1.TaskDone(tk), "%s must be done", state)
	}
	for _, state := range []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
	} {
		tk := &v1alpha1.Task{Status: v1alpha1.TaskStatus{State: state}}
		require.False(t, v1alpha1.TaskDone(tk), "%s must NOT be done", state)
	}
}

// THE #521 REGRESSION TEST. TaskDone(parked) was TRUE, which skipped intake's
// live-twin branch at intake.go:311 and let the Create 409 fall through to the
// delete at intake.go:332.
func TestTaskDoneIsFalseForEveryParkedNonTerminalState(t *testing.T) {
	for _, state := range []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
	} {
		tk := &v1alpha1.Task{Status: v1alpha1.TaskStatus{
			State: state, ParkReason: "awaiting-human", ParkedAt: &metav1.Time{},
		}}
		require.False(t, v1alpha1.TaskDone(tk),
			"a parked %s Task is stalled, not finished; TaskDone(parked)==true is issue #521", state)
		require.True(t, v1alpha1.Parked(tk))
	}
}

func TestParkedIsTheEmptyStringTest(t *testing.T) {
	require.False(t, v1alpha1.Parked(&v1alpha1.Task{}))
	require.True(t, v1alpha1.Parked(&v1alpha1.Task{
		Status: v1alpha1.TaskStatus{ParkReason: "backlog-sweep"}}))
}

func TestNoteIDIsDeterministicAndStable(t *testing.T) {
	at := metav1.Unix(1700000000, 0)
	a := v1alpha1.NewNoteID(at, "plan", "the plan body")
	b := v1alpha1.NewNoteID(at, "plan", "the plan body")
	c := v1alpha1.NewNoteID(at, "plan", "the plan body edited")
	require.Equal(t, a, b, "the same note must hash to the same id or planNoteId cannot be quoted back")
	require.NotEqual(t, a, c)
	require.Regexp(t, `^n-[0-9a-f]{16}$`, a)
}

func TestSpecKindEnumHasImplementAndNotClarify(t *testing.T) {
	require.True(t, v1alpha1.IsKnownKind("implement"),
		"the migrator rewrites 61 Tasks to spec.kind=implement; the QueuedEvent validator must accept it")
	require.False(t, v1alpha1.IsKnownKind("clarify"))
}
```

Add to `api/v1alpha1/constants_test.go` (or create it) the **single highest-value new test in this change**:

```go
// THE CEILING INVARIANT, MACHINE-CHECKED. Until now this was prose in
// constants.go:31-37. At 5 versus 3 the live-pod ceiling could never bind:
// three chatty conversations saturate 100% of a project's agent concurrency
// before the live-pod cap does anything, starving every implement/review/merge
// Task indefinitely (2026-07-28 final review IMPORTANT 1). STRICTLY below, not
// at-or-below: equal caps leave zero slots for non-conversational work.
func TestMaxLivePodsIsStrictlyBelowMaxConcurrentAgents(t *testing.T) {
	require.Less(t, v1alpha1.DefaultMaxLivePods, v1alpha1.DefaultMaxConcurrentAgents,
		"DefaultMaxLivePods (%d) must be STRICTLY below DefaultMaxConcurrentAgents (%d)",
		v1alpha1.DefaultMaxLivePods, v1alpha1.DefaultMaxConcurrentAgents)
}

// The same invariant against a CONFIGURED Project, not just the defaults - a
// maintainer can raise maxLivePods per project and the ceiling must still bind.
func TestMaxLivePodsIsStrictlyBelowMaxConcurrentAgentsForAnyProject(t *testing.T) {
	for _, tc := range []struct{ live, agents, wantLive int }{
		{0, 0, v1alpha1.DefaultMaxLivePods},   // both unset: defaults
		{5, 3, 2},                             // over-configured: clamped below
		{2, 8, 2},                             // sane: unchanged
		{7, 8, 7},                             // sane at scale: unchanged
		{8, 8, 7},                             // equal: clamped to agents-1
	} {
		p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{
			MaxLivePods: tc.live, MaxConcurrentAgents: tc.agents}}
		got := v1alpha1.MaxLivePods(p)
		require.Equal(t, tc.wantLive, got)
		require.Less(t, got, v1alpha1.MaxConcurrentAgents(p),
			"MaxLivePods must be strictly below MaxConcurrentAgents for every configuration")
	}
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-operator
mise exec -- go test ./api/... 2>&1 | head -30
```

Expected: compile failure - `undefined: v1alpha1.StateNew`, `undefined: v1alpha1.Parked`, `undefined: v1alpha1.NewNoteID`, `undefined: v1alpha1.DefaultMaxLivePods`.

- [ ] **Step 3: Write the implementation**

In `api/v1alpha1/task_types.go`:

1. Delete the whole `Stage*` const block (lines 189-206) and the `terminalStages` (212-216), `podlessStages` (224-233), `StageTerminal` (238-240), `StagePodless` (244-246), `StageIsTerminalOutcome` (254-256) declarations. Add the `State*` block from the design above.
2. Replace `TaskDone`:

```go
// TaskDone reports whether a Task's work is over. It is EXACTLY
// state in {done, rejected} - nothing else, and in particular NOT parked.
//
// This is issue #521's fix and it is structural. Under the stage machine
// terminalStages contained StageParked, so TaskDone(parked) was true, so
// intake.createTaskRaceSafe's live-twin branch (intake.go:311) was skipped for
// every parked Task, so the Create 409ed and control reached the delete at
// intake.go:332 - deleting a live Task and cascading its owned Issue and
// MergeRequest mirrors. Once done is exactly {done, rejected}, a parked Task
// takes the live-twin branch and that delete is structurally unreachable for
// it.
func TaskDone(t *Task) bool {
	return t.Status.State == StateDone || t.Status.State == StateRejected
}

// Parked reports whether a Task is stalled. Empty ParkReason means not parked
// and that is the whole test.
func Parked(t *Task) bool { return t.Status.ParkReason != "" }

// TaskIsTerminalOutcome reports whether entering state is a TERMINAL OUTCOME,
// i.e. what operator_task_terminal_total counts. Same set as TaskDone: under
// the 8-state model `done` absorbed `delivered`, so there is no quasi-terminal
// left to special-case.
func TaskIsTerminalOutcome(state string) bool {
	return state == StateDone || state == StateRejected
}
```

3. Replace the `Stage`/`StageEnteredAt`/`StageReason`/`ParkedFromStage` status fields with the `State`/`StateEnteredAt`/`StateReason`/`ParkReason`/`ParkedAt`/`ParkedFromState` block from the design above. Keep `StageWorkStartedAt`, `PodStartedAt`, `ConversationLastEventAt` and all six counters with their existing names and doc comments.
4. Update the `+kubebuilder:printcolumn` markers: `State` from `.status.state`, `Park` from `.status.parkReason`, drop the old two.
5. `Spec.Kind` enum marker: `+kubebuilder:validation:Enum=brainstorm;incident;implement;refine;review;documentation;takeover`. In `unconstrainedKinds` (line 58) swap `"clarify": true` for `"implement": true`.
6. `Note.Agent` enum marker: drop `clarify`, keep `implement`.
7. Add `Note.ID`:

```go
type Note struct {
	// ID is the stable handle an agent quotes back as planNoteId. It is
	// sha256(at|kind|body) truncated to 16 hex, so it is derivable from the
	// note alone (no counter, no sequence) and CHANGES if the body changes -
	// which is exactly what the approval gate's plan pin needs: an approved
	// plan whose body was edited afterwards no longer matches the hash stored
	// on ApprovalEvidence.
	// +optional
	ID string `json:"id,omitempty"`
	At metav1.Time `json:"at"`
	// +kubebuilder:validation:Enum=brainstorm;incident;refine;review;documentation;implement;operator
	Agent string `json:"agent"`
	// +kubebuilder:validation:Enum=note;plan;handoff
	Kind string `json:"kind"`
	// +kubebuilder:validation:MaxLength=4096
	Body string `json:"body"`
}

// NewNoteID derives a Note's stable id.
func NewNoteID(at metav1.Time, kind, body string) string {
	sum := sha256.Sum256([]byte(strconv.FormatInt(at.Unix(), 10) + "|" + kind + "|" + body))
	return "n-" + hex.EncodeToString(sum[:])[:16]
}
```

In `api/v1alpha1/constants.go`: rename `DefaultMaxConversingPods` to `DefaultMaxLivePods` (value stays 2), and rewrite its doc comment to say "live pod" instead of "conversing". Add `DefaultMaxConcurrentAgents = 3` as a real constant if it is currently only a literal in `ProjectSpec`'s doc - the strict-inequality test needs both sides to be constants.

In `api/v1alpha1/project_types.go`: rename `MaxConversingPods(p)` to `MaxLivePods(p)` and `Spec.MaxConversingPods` to `Spec.MaxLivePods`, and make `MaxLivePods` CLAMP:

```go
// MaxLivePods is the per-project ceiling on simultaneously LIVE agent pods.
//
// IT CLAMPS. A configured value at or above MaxConcurrentAgents can never bind
// - the agent-concurrency cap saturates first - so a project configured that
// way silently has no live-pod ceiling at all. Clamping to
// MaxConcurrentAgents-1 keeps at least one slot for non-conversational work,
// whatever a maintainer types. Asserted by
// TestMaxLivePodsIsStrictlyBelowMaxConcurrentAgentsForAnyProject.
func MaxLivePods(p *Project) int {
	want := DefaultMaxLivePods
	if p != nil && p.Spec.MaxLivePods > 0 {
		want = p.Spec.MaxLivePods
	}
	if ceiling := MaxConcurrentAgents(p) - 1; want > ceiling {
		want = ceiling
	}
	if want < 1 {
		want = 1
	}
	return want
}
```

Delete `api/v1alpha1/stage_test.go`.

- [ ] **Step 4: Run the tests**

```bash
mise exec -- go test ./api/... -run 'TestTaskDone|TestParked|TestNoteID|TestMaxLivePods|TestSpecKind' -v
```

Expected: PASS. The rest of the repo does not compile yet - that is expected and is Task 6.3's work.

- [ ] **Step 5: Regenerate the artefacts**

```bash
mise exec -- make generate
mise exec -- make manifests
git diff --stat api/v1alpha1/zz_generated.deepcopy.go charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml
```

Both files MUST show a diff. Then verify the CRD carries exactly the new enum:

```bash
mise exec -- yq '.spec.versions[0].schema.openAPIV3Schema.properties.status.properties.state.enum' \
  charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml
mise exec -- yq '.spec.versions[0].schema.openAPIV3Schema.properties.status.properties.parkReason.enum | length' \
  charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml
mise exec -- yq '.spec.versions[0].schema.openAPIV3Schema.properties.status.properties.stage' \
  charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml
```

Expected: the 8 state values; `28` park reasons; `null` for `stage`. **Both generated files are committed.** Never hand-edit either.

- [ ] **Step 6: Commit**

```bash
git add api/ charts/tatara-operator/crd-bases/
git commit -m "feat(api)!: replace status.stage with the 8-state model

status.stage/stageReason are deleted, not aliased, so the compiler enumerates
all 102 production sites. status.state carries WHERE THE WORK IS; status.parkReason
carries WHETHER IT IS STALLED; the two are orthogonal. TaskDone is now exactly
{done, rejected} - TaskDone(parked)==true is issue #521 itself."
```

---

## Task 6.2: the stage package

**Files:**
- Modify: `internal/stage/stage.go` (53 `StageParked` refs, 15 `StageConversing`, 11 `StageClarifying`, 38 `StageFailed`)
- Create: `internal/stage/liveness.go`
- Create: `internal/stage/park.go`
- Modify: `internal/stage/stage_test.go` (123 `StageParked` refs - the single biggest test file in the change)
- Modify: `internal/stage/conversing_test.go`, `conversing_clock_test.go`, `conversing_unpark_test.go`, `unpark_decline_test.go`, `ci_red_test.go`
- Create: `internal/stage/park_test.go`, `internal/stage/liveness_test.go`

**Interfaces:**
- Consumes: `v1alpha1.State*`, `v1alpha1.Parked`, `v1alpha1.MaxLivePods`.
- Produces: `stage.Enter(t, mrs, to, reason, now) error`, `stage.Park(t, reason, now) error`, `stage.Unpark(in UnparkInput) (decline string)`, `stage.UnparkTakeover(t, to, now) error`, `stage.Live(state) bool`, `stage.OperatorDriven(state) bool`, `stage.AgentKindFor(state, specKind) string`, `stage.UnparkClassFor(reason) (UnparkClass, bool)`, `stage.ResidencyExceeded(t, now) bool`.

- [ ] **Step 1: Write the failing tests (park.go first - it carries the wedge mitigation)**

Create `internal/stage/park_test.go`. **All PURE UNIT - `internal/stage` imports nothing that needs a cluster and must stay that way.**

```go
package stage_test

// THE WEDGE MITIGATION. A future writer sets State and forgets to clear
// ParkReason, wedging the Task forever with a stale reason - the same silent
// drift genre as #521. Enter is the ONE place a state changes and it REFUSES a
// non-park edge on a parked Task.
func TestEnterRefusesANonParkEdgeWhileParked(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))

	err := stage.Enter(tk, nil, v1alpha1.StateUnderImplementation, "", now)
	require.ErrorAs(t, err, new(*stage.StillParkedError))
	require.Equal(t, v1alpha1.StateRefined, tk.Status.State, "the Task must be untouched")
	require.Equal(t, stage.ReasonAwaitingHuman, tk.Status.ParkReason)
}

func TestUnparkIsTheOnlyThingThatClearsParkReason(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "szymonrychu"}}

	decline := stage.Unpark(stage.UnparkInput{Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now})
	require.Equal(t, stage.DeclineNone, decline)
	require.Empty(t, tk.Status.ParkReason)
	require.Nil(t, tk.Status.ParkedAt)
	require.Equal(t, v1alpha1.StateRefined, tk.Status.State,
		"un-parking returns a Task to WHERE IT WAS; it never moves state")
}

// The ONE documented exception, and it is narrow: takeover_mint clears the flag
// AND moves state, because a re-taken MR resumes at merged, not at wherever the
// ownership flip happened to catch it.
func TestUnparkTakeoverIsTheOnlyStateMovingUnpark(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonOwnershipLost, now))

	require.NoError(t, stage.UnparkTakeover(tk, v1alpha1.StateMerged, now))
	require.Empty(t, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateMerged, tk.Status.State)
}

func TestUnparkTakeoverRefusesAParkThatIsNotOwnershipLost(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.Error(t, stage.UnparkTakeover(tk, v1alpha1.StateMerged, now))
}

func TestParkStampsTheWholeTupleAtomically(t *testing.T) {
	tk := task(v1alpha1.StateAwaitingReview)
	require.NoError(t, stage.Park(tk, stage.ReasonReviewLoopExhausted, now))
	require.Equal(t, stage.ReasonReviewLoopExhausted, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.ParkedFromState)
	require.NotNil(t, tk.Status.ParkedAt)
	require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.State,
		"a park does not move the Task; that non-atomicity IS the #521 bug shape")
}

func TestParkRefusesAReasonOutsideTheClosedSet(t *testing.T) {
	require.Error(t, stage.Park(task(v1alpha1.StateNew), "declined", now),
		"declined is a RejectReason and must go through Enter(rejected), not Park")
	require.Error(t, stage.Park(task(v1alpha1.StateNew), "not-a-reason", now))
}

func TestEveryParkReasonHasAnUnparkClass(t *testing.T) {
	for _, r := range stage.ParkReasons {
		_, ok := stage.UnparkClassFor(r)
		require.True(t, ok, "park reason %q has no UnparkClass; the axis must be total", r)
	}
	require.Len(t, stage.ParkReasons, 28)
	require.Len(t, stage.RejectReasons, 6)
	require.Len(t, stage.DoneReasons, 2)
	require.Len(t, stage.Reasons, 36, "the three sets must partition Reasons with no remainder")
}
```

Create `internal/stage/liveness_test.go`:

```go
package stage_test

func TestLiveIsExactlyThreeStates(t *testing.T) {
	live := map[string]bool{}
	for _, s := range stage.AllStates() {
		if stage.Live(s) {
			live[s] = true
		}
	}
	require.Equal(t, map[string]bool{
		v1alpha1.StateRefined:             true,
		v1alpha1.StateUnderImplementation: true,
		v1alpha1.StateAwaitingReview:      true,
	}, live)
}

func TestLiveAndOperatorDrivenAreDisjointAndCoverTheWorkStates(t *testing.T) {
	for _, s := range stage.AllStates() {
		require.False(t, stage.Live(s) && stage.OperatorDriven(s),
			"%s cannot be both live and operator-driven", s)
	}
}

func TestAgentKindForIsKeyedOnStateAndOriginKind(t *testing.T) {
	require.Equal(t, stage.AgentImplement,
		stage.AgentKindFor(v1alpha1.StateUnderImplementation, "implement"))
	require.Equal(t, stage.AgentBrainstorm,
		stage.AgentKindFor(v1alpha1.StateRefined, "brainstorm"),
		"a brainstorm Task in refined needs a brainstorm agent, not an implement one")
	require.Equal(t, stage.AgentReview,
		stage.AgentKindFor(v1alpha1.StateAwaitingReview, "implement"))
	require.Equal(t, "", stage.AgentKindFor(v1alpha1.StateMerged, "implement"))
	require.Equal(t, "", stage.AgentKindFor(v1alpha1.StateRefined, "unknown-kind"),
		"an unknown origin kind must fail CLOSED, not spawn a default agent")
}

func TestClarifyIsNotAnAgentKind(t *testing.T) {
	for _, s := range stage.AllStates() {
		for _, k := range []string{"implement", "review", "brainstorm", "incident", "refine", "documentation", "takeover"} {
			require.NotEqual(t, "clarify", stage.AgentKindFor(s, k))
		}
	}
}
```

Rewrite `internal/stage/stage_test.go`'s table tests against the new names. The three structural tests that must survive verbatim in spirit:

- `TestEveryStateHasABudget` (was `TestEveryStageHasABudget`) - every member of `AllStates()` has a `Budget` row.
- `TestEveryStateHasAnOnElapseEdge` - every member has an `OnElapse` row.
- `TestTransitionsAndLegalPairsAgree` - `Transitions` collapsed equals `legalPairs`.

Add:

```go
func TestTransitionTableHasExactlyTwentyOneEdges(t *testing.T) {
	n := 0
	for _, edges := range stage.Transitions {
		n += len(edges)
	}
	require.Equal(t, 21, n,
		"the table is the contract; a new edge is a design decision, not a diff")
}

// refined -> done is the 21st edge and it is REQUIRED: brainstorm, refine and
// incident Tasks finish without ever opening an MR, so awaiting-review->done
// and deployed->done are both unreachable for them.
func TestRefinedToDoneExistsForTheNonCodeKinds(t *testing.T) {
	for _, kind := range []string{"brainstorm", "refine", "incident"} {
		tk := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: kind},
			Status: v1alpha1.TaskStatus{State: v1alpha1.StateRefined}}
		require.True(t, stage.LegalFor(tk, nil, v1alpha1.StateRefined, v1alpha1.StateDone),
			"a %s Task has no other path to done", kind)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
mise exec -- go test ./internal/stage/... 2>&1 | head -40
```

Expected: compile failure on every undefined symbol.

- [ ] **Step 3: Write the implementation**

Create `internal/stage/liveness.go` with the `liveStates`/`Live`/`operatorDriven`/`OperatorDriven`/`originAgentKinds`/`AgentKindFor` code from the design section above, plus:

```go
// AllStates returns the 8 members of the state enum, in lifecycle order.
func AllStates() []string {
	return []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
		v1alpha1.StateDone, v1alpha1.StateRejected,
	}
}
```

Create `internal/stage/park.go`:

```go
// StillParkedError is returned by Enter when a caller tries to move a PARKED
// Task's state without un-parking it first. It exists because `parkReason` is a
// stringly flag, and a stringly flag a writer can forget to clear is a new
// silent wedge in the same genre as #521. There is exactly one way out of a
// park and it is Unpark (or UnparkTakeover, the one documented exception).
type StillParkedError struct {
	State      string
	ParkReason string
}

func (e *StillParkedError) Error() string {
	return fmt.Sprintf("task is parked at %s(%s); un-park before entering another state", e.State, e.ParkReason)
}

// Park is the ONE way a Task is parked. It stamps the whole tuple in one
// mutation - reason, parkedAt, parkedFromState - because a park that is not
// atomic with its reason is the #521 bug shape: annotations were rejected as a
// representation for exactly this, since they are not on the status subresource
// and cannot be one write.
//
// It does NOT change State. A Task parks WHERE IT IS.
func Park(t *v1alpha1.Task, reason string, now time.Time) error {
	if !IsParkReason(reason) {
		return &UnknownReasonError{Reason: reason}
	}
	if t.Status.ParkReason != "" {
		return nil // idempotent: already parked, first reason wins
	}
	stamp := metav1.NewTime(now)
	t.Status.ParkReason = reason
	t.Status.ParkedAt = &stamp
	t.Status.ParkedFromState = t.Status.State
	if reason == ReasonMergeTimeout || reason == ReasonDeployTimeout {
		if t.Status.StateEnteredAt != nil {
			t.Status.StageElapsedCarrySeconds += int(now.Sub(t.Status.StateEnteredAt.Time).Seconds())
		}
	}
	return nil
}

// clearPark is the ONLY assignment to ParkReason outside Park. It is unexported
// on purpose: Unpark and UnparkTakeover are its only callers.
func clearPark(t *v1alpha1.Task) {
	t.Status.ParkReason = ""
	t.Status.ParkedAt = nil
}
```

Rewrite `Enter` to guard first:

```go
func Enter(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, to, reason string, now time.Time) error {
	if t.Status.ParkReason != "" {
		return &StillParkedError{State: t.Status.State, ParkReason: t.Status.ParkReason}
	}
	from := t.Status.State
	if from == "" {
		from = Create
	}
	if !LegalFor(t, mrs, from, to) {
		return &IllegalTransitionError{From: from, To: to}
	}
	// ... reason validation, then the same four stamps as before, keyed on
	// StateEnteredAt instead of StageEnteredAt, and AgentKindFor(to, t.Spec.Kind)
}
```

Rewrite `UnparkInput` (drop `ConversingHasRoom`, add `LiveHasRoom`) and `Unpark` to return only a decline string - the target no longer exists as a concept, because un-parking does not move state. `UnparkDetailed` collapses into `Unpark`; delete the two-function split. Delete `HasReentry`, `reentryReasons`, and `UnparkTargetForBindingRepair`.

Add `ResidencyExceeded` (its consumer is Task 6.4):

```go
// ResidencyExceeded is THE MANDATORY MITIGATION for the one genuine regression
// in promoting liveness to a property: a live state arms the IDLE clock instead
// of the WORK clock, so under-implementation loses the absolute residency bound
// the old `implementing` stage had (6h, stage.go:721 before this change). An
// agent ping-ponging with a reviewer would otherwise sit forever.
//
// It is a SEPARATE check, not a second return value from ArmedClock: ArmedClock
// returns exactly one clock by construction and that is the property that makes
// the three-clock model auditable. Whichever of the two fires first wins.
//
// It reads StateElapsedSeconds, so it is cumulative across a park/un-park round
// trip - deliberately. A Task that spends six hours in under-implementation
// across three re-entries has spent six hours there.
func ResidencyExceeded(t *v1alpha1.Task, now time.Time) bool {
	cap, ok := residencyCaps[t.Status.State]
	if !ok {
		return false
	}
	return StateElapsedSeconds(t, now) > cap.Seconds()
}

// residencyCaps is the ABSOLUTE bound on time spent in a live state, whatever
// the idle clock says. Only the live states have one: an operator-driven state
// already has a work budget, and new/done/rejected are not where an agent runs.
var residencyCaps = map[string]time.Duration{
	v1alpha1.StateRefined:             24 * time.Hour, // was clarifying's budget
	v1alpha1.StateUnderImplementation: 6 * time.Hour,  // was implementing's budget
	v1alpha1.StateAwaitingReview:      4 * time.Hour,  // was reviewing's budget
}
```

- [ ] **Step 4: Run the tests**

```bash
mise exec -- go test ./internal/stage/... -v 2>&1 | tail -40
```

Expected: PASS. `internal/stage` is pure and self-contained, so it goes green before the rest of the repo compiles. That is the point of doing it second.

- [ ] **Step 5: Commit**

```bash
git add internal/stage/
git commit -m "feat(stage)!: 8 states, park as a flag, liveness as a property

parkReason is a stringly flag, so Enter refuses every non-park edge on a parked
Task and Unpark is the one function that clears it. Un-parking returns a Task to
WHERE IT WAS; UnparkTakeover is the single documented exception that also moves
state. ResidencyExceeded is the mandatory absolute bound a live state's idle
clock would otherwise have removed."
```

---

## Task 6.3: the compiler-driven sweep (102 production sites)

**This task exists because the design forbids aliasing.** `StageParked` is DELETED, not repointed, precisely so `go build` enumerates every site. Work the compiler's list, in this exact order, and do not add a shim to make it shorter.

**Ordered sweep, by file, largest first. Production only - tests are Task 6.14.**

| # | file | `StageParked` | also | note |
|---|---|---|---|---|
| 1 | `internal/controller/sweep.go` | 9 | 1 `StageFailed` | `taskStillPushes` is one of the four `TaskDone` carve-outs; see Task 6.10 |
| 2 | `internal/controller/task_stage.go` | 8 | 3 `StageConversing`, 1 `StageClarifying`, 3 `StageFailed` | `reconcileClocks` is Task 6.4; the follow-up-turn branch at 1003-1006 is Task 6.7 |
| 3 | `internal/restapi/outcome.go` | 4 | 1 `StageClarifying`, 1 `StageFailed` | the gate is Tasks 6.7/6.8 |
| 4 | `internal/controller/unpark.go` | 3 | 1 `StageConversing` | `ApplyUnpark`/`NeedsConversingRoom`/`CountActiveTasks` are consumed cross-package by `internal/webhook/pending_events.go` - move both in lockstep |
| 5 | `internal/controller/reaper.go` | 3 | 1 `StageFailed` | `unparkFires` at 1277-1317 independently re-derives F.6 semantics and must stay in agreement; `unpark_backstop_test.go` asserts that |
| 6 | `internal/controller/ownership.go` | 3 | - | |
| 7 | `internal/controller/reviewpost.go` | 2 | - | |
| 8 | `internal/controller/docbatch.go` | 2 | 1 `StageFailed` | |
| 9 | `internal/webhook/server.go` | 1 | - | line 421, `TaskDone(t) && stage != parked` carve-out; Task 6.10 |
| 10 | `internal/webhook/pending_events.go` | 1 | 1 `StageFailed` | the `ConversingEntryDeclined` emitters at 195/227/279 - fix the `reason="unresolved"` label here |
| 11 | `internal/obs/stage_metrics.go` | 1 | 1 each of the other three | metric label vocabulary; Task 6.13 |
| 12 | `internal/controller/transition.go` | 1 | - | `EnterStage` - THE choke point every transition goes through |
| 13 | `internal/controller/takeover_mint.go` | 1 | - | becomes `stage.UnparkTakeover`'s only caller |
| 14 | `internal/controller/resume.go` | 1 | - | **deleted whole in Task 6.9 - do not fix it, delete it** |
| 15 | `internal/controller/ownership_standdown_merge.go` | 1 | - | |
| 16 | `internal/controller/mirror.go` | 1 | - | |
| 17 | `internal/controller/merge.go` | 1 | 4 `StageFailed` | |
| 18 | `internal/controller/issue_controller.go` | 1 | - | |
| 19 | `internal/controller/conversing.go` | 1 | 2 `StageConversing`, 1 `StageClarifying` | becomes `internal/controller/livepods.go`; Tasks 6.5/6.7 |
| 20 | `internal/controller/comment_authorship.go` | 1 | 2 `StageConversing`, 1 `StageClarifying` | |
| 21 | `internal/controller/queue_controller.go` | 0 | `queueTaskHoldsSlot`, `ticketSpent` | stage-literal-keyed; Task 6.10 |
| 22 | `internal/controller/podwatch.go` | 0 | 1 `StageFailed` | |
| 23 | `internal/agent/pod.go` | 0 | 1 `StageConversing`, `kindProfiles` | delete the `clarify` entry; Task 6.7 |
| 24 | `internal/agent/ttlstop.go` | 0 | 1 `StageConversing` | |
| 25 | `internal/objbudget/objbudget.go` | 0 | 1 `StageFailed` | |
| 26 | `internal/queue/enqueue.go` | 0 | - | check for stage literals |
| 27 | `internal/prompt/bundle.go` | 0 | `StageClarifying` in its test | |

- [ ] **Step 1: Get the compiler's list**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-operator
mise exec -- go build ./... 2>&1 | grep -v "^#" | sort -u > /tmp/521-sweep.txt
wc -l /tmp/521-sweep.txt
```

This file is the work list. Keep it and tick it down.

- [ ] **Step 2: Sweep in the table's order, one file per commit**

For each file: replace `Status.Stage` with `Status.State`, `Status.StageReason` with `Status.ParkReason` or `Status.StateReason` (decide per site: a park reason goes to `ParkReason`, a done/rejected reason to `StateReason`), `StageParked` comparisons with `v1alpha1.Parked(t)`, `StageConversing` comparisons with `stage.Live(t.Status.State)`, `StagePodless(...)` with `stage.OperatorDriven(...)`, `StageTerminal(t)` with `v1alpha1.TaskDone(t)`, and `stage.AgentKindFor(s)` with `stage.AgentKindFor(s, t.Spec.Kind)`.

**Two mechanical rules that prevent the commonest mistake:**

1. A site that reads `stage == StageParked` almost always wants `v1alpha1.Parked(t)`. A site that reads `stage != StageParked` inside a `TaskDone` expression almost always wants to be DELETED - see Task 6.10.
2. A site that WRITES `stage.Enter(t, mrs, StageParked, reason, now)` becomes `stage.Park(t, reason, now)` and does NOT call `Enter`. If the reason is one of the 6 RejectReasons or 2 DoneReasons, it becomes `stage.Enter(t, mrs, StateRejected|StateDone, reason, now)` instead.

- [ ] **Step 3: Verify the sweep is complete**

```bash
mise exec -- go build ./... && echo BUILD OK
grep -rn "StageParked\|StageConversing\|StageClarifying\|StageFailed\|StageTriaging\|StageApproved\|StageImplementing\|StageReviewing\|StageMerging\|StageDeploying\|StageDelivered\|StageDocumenting\|StageBrainstorming\|StageInvestigating\|StageRefining\|StageRejected\|StagePodless\|StageTerminal\|Status.Stage\b\|StageReason" --include="*.go" . | grep -v "_test.go"
```

Expected: `BUILD OK` and the grep returns NOTHING. A single remaining production hit means a site was papered over.

- [ ] **Step 4: Commit**

One commit per file in the table, each `refactor(<pkg>): <file> onto the 8-state model`.

---

## Task 6.4: the residency bound (MANDATORY, NOT DEFERRABLE)

**Files:** Modify `internal/controller/task_stage.go` (`reconcileClocks`, lines 100-284), `internal/controller/task_stage_test.go`.

**Interfaces:** Consumes `stage.ResidencyExceeded`, `stage.ArmedClock`. Produces the `residency-exceeded` INFO log action and `operator_task_residency_exceeded_total{state,kind}`.

If this slips, an implement agent runs forever. It is not a follow-up.

- [ ] **Step 1: Write the failing tests**

In `internal/controller/task_stage_test.go`. **PURE UNIT** (`reconcileClocks` is already tested against a fake client in `conversing_clock_test.go` and `task_stage_test.go:1860-1894`; keep it that way, envtest is slow and buys nothing here).

```go
// THE ONE GENUINE REGRESSION in promoting liveness to a property, and its
// mandatory mitigation. A live state arms the IDLE clock, so a chatty
// under-implementation Task resets its deadline on every human comment and the
// 6h absolute bound the old `implementing` stage had is gone. This is that
// bound, restored as a separate check.
func TestReconcileClocks_ResidencyExceededParksALiveStateWhoseIdleClockKeepsResetting(t *testing.T) {
	now := time.Now()
	tk := liveTask(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptr(metav1.NewTime(now.Add(-7 * time.Hour)))
	// The idle clock is FRESH - a comment landed a minute ago - so ArmedClock
	// alone would never fire. That is exactly the ping-pong shape.
	tk.Status.ConversationLastEventAt = ptr(metav1.NewTime(now.Add(-1 * time.Minute)))
	tk.Status.StateWorkStartedAt = ptr(metav1.NewTime(now.Add(-7 * time.Hour)))

	r := newTaskReconciler(t, proj, tk)
	_, handled, err := r.reconcileClocks(ctx, proj, tk, now)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, v1alpha1.Parked(tk))
	require.Equal(t, stage.ReasonStageDeadline, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateUnderImplementation, tk.Status.State,
		"a park does not move the Task")
}

func TestReconcileClocks_ResidencyIsCumulativeAcrossAParkRoundTrip(t *testing.T) {
	now := time.Now()
	tk := liveTask(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptr(metav1.NewTime(now.Add(-1 * time.Hour)))
	tk.Status.StageElapsedCarrySeconds = int((5*time.Hour + 30*time.Minute).Seconds())
	tk.Status.ConversationLastEventAt = ptr(metav1.NewTime(now.Add(-1 * time.Minute)))
	tk.Status.StateWorkStartedAt = ptr(metav1.NewTime(now.Add(-1 * time.Hour)))

	r := newTaskReconciler(t, proj, tk)
	_, handled, err := r.reconcileClocks(ctx, proj, tk, now)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, v1alpha1.Parked(tk),
		"6h30m of cumulative residency exceeds the 6h under-implementation cap")
}

func TestReconcileClocks_ResidencyDoesNotFireUnderTheCap(t *testing.T) {
	now := time.Now()
	tk := liveTask(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptr(metav1.NewTime(now.Add(-5 * time.Hour)))
	tk.Status.ConversationLastEventAt = ptr(metav1.NewTime(now.Add(-1 * time.Minute)))
	tk.Status.StateWorkStartedAt = ptr(metav1.NewTime(now.Add(-5 * time.Hour)))

	r := newTaskReconciler(t, proj, tk)
	_, handled, err := r.reconcileClocks(ctx, proj, tk, now)
	require.NoError(t, err)
	require.False(t, handled)
	require.False(t, v1alpha1.Parked(tk))
}

func TestReconcileClocks_ResidencyDoesNotApplyToOperatorDrivenStates(t *testing.T) {
	now := time.Now()
	tk := liveTask(v1alpha1.StateMerged)
	tk.Status.StateEnteredAt = ptr(metav1.NewTime(now.Add(-100 * time.Hour)))

	// merged has its own 4h WORK budget; residency must not double-park it with
	// a second, different reason.
	r := newTaskReconciler(t, proj, tk)
	_, _, err := r.reconcileClocks(ctx, proj, tk, now)
	require.NoError(t, err)
	require.Equal(t, stage.ReasonMergeTimeout, tk.Status.ParkReason,
		"the work clock owns merged; residency must not shadow it")
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
mise exec -- go test ./internal/controller/ -run TestReconcileClocks_Residency -v
```

Expected: FAIL - the Task is not parked; no residency check exists.

- [ ] **Step 3: Write the implementation**

In `reconcileClocks`, immediately BEFORE the `stage.ArmedClock` call at what is currently line 215:

```go
	// THE ABSOLUTE RESIDENCY BOUND. It runs BEFORE ArmedClock and it is a
	// SEPARATE check, not a second return value: ArmedClock returns exactly one
	// clock by construction, and that single-clock property is what makes the
	// model auditable. Whichever fires first wins, and residency is the one that
	// fires when the idle clock never will.
	//
	// Only the live states have a cap. It is measured with StateElapsedSeconds,
	// so it is CUMULATIVE across a park/un-park round trip - a Task that has
	// spent six hours in under-implementation across three re-entries has spent
	// six hours there, and buying a fresh 6h per re-entry is the unbounded-loop
	// shape #480 killed for merging.
	if stage.ResidencyExceeded(task, now) {
		l.Info("state residency budget exceeded",
			"action", "residency_exceeded", "resource_id", task.Name,
			"state", task.Status.State, "kind", task.Spec.Kind,
			"elapsed_seconds", stage.StateElapsedSeconds(task, now))
		r.Metrics.ResidencyExceeded(task.Status.State, task.Spec.Kind)
		mrs, mrErr := ownedMergeRequests(ctx, r.Client, task)
		if mrErr != nil {
			return ctrl.Result{}, true, mrErr
		}
		return ctrl.Result{}, true, r.park(ctx, proj, task, mrs, stage.ReasonStageDeadline, now)
	}
```

Add the metric in `internal/obs/task_metrics.go`:

```go
	residencyExceededTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_task_residency_exceeded_total",
		Help: "Tasks parked because a live state's absolute residency cap was reached even though its idle clock had not.",
	}, []string{"state", "kind"}),
```

- [ ] **Step 3b: Generalise ArmedClock's idle case - PREDICATE ONLY**

`ArmedClock`'s named idle case (`internal/stage/stage.go:831-848`) **stays a named case.** Generalise ONLY its predicate, from `stg == v1alpha1.StageConversing` to `Live(t.Status.State)`. Do not restructure the function, do not merge the case into the generic selector, and do not touch the five dated regressions its surrounding comments carry - that function is the densest incident history in the repo and every paragraph in it was paid for.

```go
	// THE IDLE CLOCK. A named case, not an inference. A LIVE state runs a pod,
	// so the generic selector below would arm clock 1 or clock 2 or measure
	// clock 3 from stateWorkStartedAt - all three of which describe pod age, and
	// none of which describes how long the human has been silent. The budget
	// here is the table default; reconcileClocks substitutes the project's
	// scm.conversationIdleMinutes, which is the only per-project knob in the
	// clock model and is why the substitution lives at the caller rather than in
	// this pure package.
	//
	// The ABSOLUTE bound this clock cannot provide is ResidencyExceeded, checked
	// separately by reconcileClocks BEFORE this function runs.
	if Live(stg) {
		if t.Status.ConversationLastEventAt == nil {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		elapse, ok := OnElapse(stg)
		if !ok {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		return ClockWork, t.Status.ConversationLastEventAt.Time, budget, elapse
	}
```

And in `reconcileClocks`, the per-project substitution at what is currently `task_stage.go:216-223` changes its condition the same way and no more:

```go
	if stage.Live(task.Status.State) && clock == stage.ClockWork {
		budget = tatarav1alpha1.ConversationIdle(proj)
	}
```

Add a test asserting the predicate widened correctly, because this is the change that silently gives `refined` and `awaiting-review` an idle clock they never had:

```go
func TestArmedClock_ArmsTheIdleClockForEveryLiveState(t *testing.T) {
	for _, state := range []string{
		v1alpha1.StateRefined, v1alpha1.StateUnderImplementation, v1alpha1.StateAwaitingReview,
	} {
		tk := liveTask(state)
		tk.Status.ConversationLastEventAt = ptr(metav1.NewTime(now.Add(-10 * time.Minute)))
		clock, since, budget, _ := stage.ArmedClock(tk, false)
		require.Equal(t, stage.ClockWork, clock, "%s must run the idle clock", state)
		require.Equal(t, tk.Status.ConversationLastEventAt.Time, since,
			"the idle clock measures SILENCE, never pod age - a pod TTL rotation re-stamps stateWorkStartedAt on a conversation that is very much alive")
		require.Equal(t, v1alpha1.ConversationIdleDefault, budget)
	}
}

func TestArmedClock_ANonLiveStateStillRunsTheOrdinaryClocks(t *testing.T) {
	tk := liveTask(v1alpha1.StateMerged)
	clock, since, budget, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock)
	require.Equal(t, tk.Status.StateEnteredAt.Time, since, "operator-driven runs clock 3 from stateEnteredAt")
	require.Equal(t, 4*time.Hour, budget)
	require.Equal(t, stage.ReasonMergeTimeout, edge.Reason)
}

func TestArmedClock_ALiveStateWithNoConversationStampArmsNothing(t *testing.T) {
	tk := liveTask(v1alpha1.StateRefined)
	tk.Status.ConversationLastEventAt = nil
	clock, _, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockNone, clock,
		"an unarmed idle clock is why Enter stamps ConversationLastEventAt on entry into a live state")
}
```

`stage.Enter` must therefore stamp `ConversationLastEventAt = now` on entry into ANY live state, where today it stamps it only on entry into `conversing` (`stage.go:698-700`). That is a one-line predicate change in `Enter` with the same doc comment, and `queue_controller.go`'s admission write - which hand-copies `Enter`'s result field-by-field rather than using the mutated Task - must copy the field too. It already does for conversing; confirm it still does for the widened predicate.

- [ ] **Step 4: Run tests**

```bash
mise exec -- go test ./internal/controller/ -run TestReconcileClocks -v
```

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(clocks): absolute residency bound on every live state

Generalising liveness replaces a live state's WORK clock with an IDLE clock, so
under-implementation lost the 6h absolute bound `implementing` had. Without
this an agent ping-ponging with a reviewer sits forever."
```

---

## Task 6.5: the live-pod ceiling

**Files:** rename `internal/controller/conversing.go` -> `internal/controller/livepods.go`; rename `conversing_capacity_test.go` -> `livepods_capacity_test.go`; modify `internal/webhook/pending_events.go`; modify `internal/obs/task_metrics.go`.

**Interfaces:** Produces `controller.LiveHasRoom(ctx, reader, proj) (bool, error)`, `controller.countLive`, `enforceLivePodCeiling`. Consumes `v1alpha1.MaxLivePods`, `stage.Live`.

**THIS PREDICATE FAILS SILENTLY BY UNDER-COUNTING. Test it hardest.** The old predicate was `t.Status.Stage == StageConversing` (`conversing.go:104`). The new one is `stage.Live(t.Status.State) && t.Status.ParkReason == ""`. Get the second clause wrong and a parked Task with a stale live state is counted, over-counting (annoying); drop the FIRST clause's precision and every `refined` Task counts whether or not it holds a pod, under-counting nothing but over-counting everything. The dangerous direction is under-counting: forget that `merged -> awaiting-review` re-enters a live state and the ceiling stops bounding pods, which is a cost blowout with no error anywhere.

- [ ] **Step 1: Write the failing tests**

Create `internal/controller/livepods_capacity_test.go`. **PURE UNIT with a fake client** - `countLive` takes a `client.Reader`.

```go
func TestCountLive_CountsExactlyTheUnparkedLiveStates(t *testing.T) {
	tasks := []client.Object{
		taskIn("a", v1alpha1.StateRefined, ""),
		taskIn("b", v1alpha1.StateUnderImplementation, ""),
		taskIn("c", v1alpha1.StateAwaitingReview, ""),
		taskIn("d", v1alpha1.StateNew, ""),      // not live
		taskIn("e", v1alpha1.StateMerged, ""),   // operator-driven
		taskIn("f", v1alpha1.StateDeployed, ""), // operator-driven
		taskIn("g", v1alpha1.StateDone, ""),
		taskIn("h", v1alpha1.StateRejected, ""),
	}
	live, err := countLive(ctx, fakeReaderWith(tasks...), proj)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, names(live))
}

// THE SILENT-UNDER-COUNT GUARD. A parked Task holds no pod - park is what takes
// the pod down - so counting it would over-count and refuse a legitimate new
// conversation. Not counting a LIVE one is the dangerous direction and the next
// test covers it.
func TestCountLive_ExcludesAParkedTaskInALiveState(t *testing.T) {
	live, err := countLive(ctx, fakeReaderWith(
		taskIn("a", v1alpha1.StateRefined, ""),
		taskIn("b", v1alpha1.StateRefined, stage.ReasonAwaitingHuman),
		taskIn("c", v1alpha1.StateUnderImplementation, stage.ReasonNoOutcome),
	), proj)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a"}, names(live))
}

// THE REGRESSION THIS TEST EXISTS FOR: merged -> awaiting-review is a legal edge
// (a maintainer requests changes on a merging Task), so a Task can RE-ENTER a
// live state from an operator-driven one. A predicate written as "has it ever
// been live" or keyed on parkedFromState under-counts it, and the ceiling
// silently stops bounding pods - a cost blowout with no error anywhere.
func TestCountLive_CountsATaskThatReEnteredALiveStateFromMerged(t *testing.T) {
	tk := taskIn("a", v1alpha1.StateAwaitingReview, "")
	tk.Status.ParkedFromState = v1alpha1.StateMerged
	tk.Status.MergeReentries = 2
	live, err := countLive(ctx, fakeReaderWith(tk), proj)
	require.NoError(t, err)
	require.Len(t, live, 1)
}

func TestCountLive_SkipsOtherProjectsAndDeletingTasks(t *testing.T) {
	other := taskIn("x", v1alpha1.StateRefined, "")
	other.Spec.ProjectRef = "some-other-project"
	deleting := taskIn("y", v1alpha1.StateRefined, "")
	deleting.DeletionTimestamp = ptr(metav1.Now())
	deleting.Finalizers = []string{"keep"}

	live, err := countLive(ctx, fakeReaderWith(other, deleting, taskIn("a", v1alpha1.StateRefined, "")), proj)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a"}, names(live))
}

func TestLiveHasRoom_BindsAtTheClampedCeiling(t *testing.T) {
	proj := projectWith(3 /*maxConcurrentAgents*/, 0 /*maxLivePods: default 2*/)
	require.Equal(t, 2, v1alpha1.MaxLivePods(proj))

	ok, err := LiveHasRoom(ctx, fakeReaderWith(taskIn("a", v1alpha1.StateRefined, "")), proj)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = LiveHasRoom(ctx, fakeReaderWith(
		taskIn("a", v1alpha1.StateRefined, ""),
		taskIn("b", v1alpha1.StateUnderImplementation, "")), proj)
	require.NoError(t, err)
	require.False(t, ok)
}

// The eviction cap survives the rename: StopWithHandoff blocks on real timers
// up to ~2*TurnTimeoutSeconds+60s per Task and ProjectReconciler runs
// MaxConcurrentReconciles=1 ACROSS EVERY PROJECT (2026-07-28 final review
// CRITICAL 2).
func TestEnforceLivePodCeiling_EvictsAtMostOnePerPass(t *testing.T) {
	proj := projectWith(3, 0)
	tasks := []client.Object{
		taskIdleSince("a", v1alpha1.StateRefined, now.Add(-3*time.Hour)),
		taskIdleSince("b", v1alpha1.StateRefined, now.Add(-2*time.Hour)),
		taskIdleSince("c", v1alpha1.StateRefined, now.Add(-1*time.Hour)),
		taskIdleSince("d", v1alpha1.StateRefined, now),
	}
	r := newProjectReconciler(t, append(tasks, proj)...)
	requeue, err := r.enforceLivePodCeiling(ctx, proj, now)
	require.NoError(t, err)
	require.Equal(t, livePodEvictionRequeue, requeue, "overflow remains; requeue quickly")
	require.Equal(t, 1, countParked(t, r, tasks), "exactly one eviction per pass")
	require.True(t, v1alpha1.Parked(reload(t, r, "a")), "longest-idle first")
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
mise exec -- go test ./internal/controller/ -run 'TestCountLive|TestLiveHasRoom|TestEnforceLivePod' -v
```

- [ ] **Step 3: Write the implementation**

`git mv internal/controller/conversing.go internal/controller/livepods.go`, then:

```go
// countLive returns proj's LIVE Tasks: those in a live state and not parked.
//
// BOTH CLAUSES ARE LOAD-BEARING AND THIS PREDICATE FAILS SILENTLY. `live` alone
// counts a parked Task whose pod is already gone and refuses a legitimate new
// conversation. `!parked` alone counts every Task in the project. And a
// predicate that tried to be cleverer - "was it ever live", "did it come from a
// live state" - under-counts a Task that re-entered awaiting-review from merged,
// which is a legal edge, and the ceiling then stops bounding pods at all: a cost
// blowout with no error and no counter anywhere. Keep it to the two clauses.
func countLive(ctx context.Context, r client.Reader, proj *v1alpha1.Project) ([]v1alpha1.Task, error) {
	var tl v1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("livepods: list tasks: %w", err)
	}
	out := make([]v1alpha1.Task, 0, len(tl.Items))
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || !stage.Live(t.Status.State) || v1alpha1.Parked(t) {
			continue
		}
		if t.DeletionTimestamp != nil {
			continue
		}
		out = append(out, *t)
	}
	return out, nil
}

func LiveHasRoom(ctx context.Context, r client.Reader, proj *v1alpha1.Project) (bool, error) {
	live, err := countLive(ctx, r, proj)
	if err != nil {
		return false, err
	}
	return len(live) < v1alpha1.MaxLivePods(proj), nil
}
```

Rename `enforceConversingCeiling` -> `enforceLivePodCeiling`, `maxConversingEvictionsPerPass` -> `maxLivePodEvictionsPerPass` (value stays 1), `conversingEvictionRequeue` -> `livePodEvictionRequeue` (stays 5s), `conversingHandoffAndPark` -> `liveHandoffAndPark`. Delete `conversingEntryStages` and `ConversingEntryEligible` - entry into a live state is now the transition table's business, not a second parallel table.

Rename the metrics: `operator_conversing_pods` -> `operator_live_pods`, `operator_conversing_entry_declined_total` -> `operator_live_entry_declined_total`, `operator_conversing_closed_total` -> `operator_live_closed_total`.

**Fix the `reason="unresolved"` defect while you are here.** `internal/webhook/pending_events.go:195,227,279` emit `ConversingEntryDeclined(project, reason)` and the measured cluster shows `reason` is ALWAYS the literal `unresolved`. That is a counter that cannot say which condition refused - the exact defect the `SweepSkip` and `UnparkDecline` vocabularies exist to prevent. Give it a closed vocabulary reusing `stage.Decline*` where the cause is a decline, and add `live-ceiling-full`, `not-a-live-state`, `task-parked` for the three local refusals. Add a test:

```go
func TestLiveEntryDeclined_NeverEmitsAnUnresolvedReason(t *testing.T) {
	// Every emitter must name its condition. The measured cluster on 2026-08-07
	// had 27 declines, every one labelled "unresolved".
	for _, r := range liveEntryDeclineReasons {
		require.NotEqual(t, "unresolved", r)
		require.NotEmpty(t, r)
	}
	require.NotEmpty(t, liveEntryDeclineReasons)
}
```

- [ ] **Step 4: Run tests and commit**

```bash
mise exec -- go test ./internal/controller/ ./internal/webhook/ ./internal/obs/ -count=1
git add internal/controller/livepods.go internal/controller/livepods_capacity_test.go internal/webhook/ internal/obs/
git commit -m "feat!: generalise the conversing ceiling to a live-pod ceiling

The predicate is now live(state) && parkReason == \"\". Both clauses are
load-bearing and the failure mode is silent under-counting. Also fixes the
decline counter's reason label, which was the literal \"unresolved\" on every
one of the 27 declines in the live cluster."
```

---

## Task 6.6: the parked-with-a-live-pod invariant repair

**Files:** Modify `internal/controller/task_controller.go`, `internal/controller/livepods.go`; create `internal/controller/park_invariant_test.go`.

The design calls `parkReason != "" && live != ""` transient-only. **Make it an invariant the Task reconciler repairs on sight.** Otherwise a parked Task keeps a live pod burning an admission slot with no clock armed - the two mechanisms are orthogonal, so nothing else notices.

- [ ] **Step 1: Write the failing test**

```go
// PURE UNIT (fake client). parkReason != "" AND a live pod is a transient state
// by design - Park stamps the flag and the pod teardown follows - but nothing
// bounds the gap. A crash between the two leaves a parked Task holding a pod
// that burns an admission slot with NO clock armed: ArmedClock disarms on park,
// residency does not apply to a parked Task, and the pod's own TTL only rotates
// it into a replacement. That is a permanent, silent slot leak.
func TestTaskReconcile_RepairsAParkedTaskThatStillHoldsALivePod(t *testing.T) {
	tk := liveTask(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonNoOutcome, now))
	tk.Status.PodName = "impl-tatara-operator-i521"
	pod := agentPod(tk)

	r := newTaskReconciler(t, proj, tk, pod)
	_, err := r.Reconcile(ctx, req(tk))
	require.NoError(t, err)

	got := reload(t, r, tk.Name)
	require.Empty(t, got.Status.PodName, "the live pod reference must be cleared")
	require.True(t, podGone(t, r, pod), "the pod itself must be stopped")
	require.Equal(t, stage.ReasonNoOutcome, got.Status.ParkReason, "the park itself is untouched")
	require.Equal(t, float64(1), testutil.ToFloat64(
		r.Metrics.ParkedWithLivePodRepairedCounter().WithLabelValues(proj.Name, stage.ReasonNoOutcome)))
}

func TestTaskReconcile_LeavesAnUnparkedLiveTaskAlone(t *testing.T) {
	tk := liveTask(v1alpha1.StateUnderImplementation)
	tk.Status.PodName = "impl-tatara-operator-i521"
	pod := agentPod(tk)

	r := newTaskReconciler(t, proj, tk, pod)
	_, err := r.Reconcile(ctx, req(tk))
	require.NoError(t, err)
	require.False(t, podGone(t, r, pod))
	require.Equal(t, "impl-tatara-operator-i521", reload(t, r, tk.Name).Status.PodName)
}

func TestTaskReconcile_ParkedWithNoPodIsANoOp(t *testing.T) {
	tk := liveTask(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))

	r := newTaskReconciler(t, proj, tk)
	_, err := r.Reconcile(ctx, req(tk))
	require.NoError(t, err)
	require.Equal(t, float64(0), testutil.ToFloat64(
		r.Metrics.ParkedWithLivePodRepairedCounter().WithLabelValues(proj.Name, stage.ReasonAwaitingHuman)))
}
```

- [ ] **Step 2: Run to verify it fails, then implement**

In `TaskReconciler.Reconcile`, before the state dispatch:

```go
	// THE PARK/LIVE INVARIANT. parkReason != "" and a live pod is a state the
	// design calls transient, and it IS - Park stamps the flag and the pod
	// teardown follows in the same reconcile. Nothing bounds the gap, though: a
	// crash, a conflict retry that loses the pod delete, or an eviction between
	// the two leaves a parked Task holding a pod that burns an admission slot
	// with NO clock armed. ArmedClock disarms on park, ResidencyExceeded does
	// not apply to a parked Task, and the pod TTL only rotates it into a
	// replacement. Repair it on sight and COUNT it: if this counter is ever
	// non-zero at a steady rate, the transient is not transient.
	if v1alpha1.Parked(task) && task.Status.PodName != "" {
		l.Info("parked task still holds a live pod; stopping it",
			"action", "parked_live_pod_repaired", "resource_id", task.Name,
			"park_reason", task.Status.ParkReason, "pod", task.Status.PodName)
		r.Metrics.ParkedWithLivePodRepaired(proj.Name, task.Status.ParkReason)
		if err := r.stopAgentPod(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, r.clearPodRef(ctx, task)
	}
```

Metric in `internal/obs/task_metrics.go`:

```go
	parkedWithLivePodRepairedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_task_parked_with_live_pod_repaired_total",
		Help: "Parked Tasks found still holding a live agent pod and repaired. A sustained non-zero rate means the park-then-stop sequence is not completing.",
	}, []string{"project", "park_reason"}),
```

- [ ] **Step 3: Run tests and commit**

---

## Task 6.7: the kind fold

**Files:**
- Modify: `internal/restapi/outcome.go` (payload structs at 77-140, dispatch at 312-354, `o.clarify` at 1221 -> `o.gate`)
- Modify: `internal/agent/pod.go` (`kindProfiles` at 938-947)
- Modify: `internal/controller/assignment.go` (the `stage.AgentClarify` job text at ~131-160)
- Modify: `internal/prompt/bundle.go`
- Modify: `internal/restapi/outcome_test.go` (61 `"clarify"` literals, 37 `StageClarifying`, ~14 clarify-named Test funcs at 604-1186)
- Modify: `internal/restapi/handlers_v2_test.go` (24 `"clarify"`, 13 `StageClarifying`)

**Interfaces:**
- Produces: `implementPayload` carrying `Action`, `ApprovingMaintainer`, `PlanNoteID`, `ApprovalCitations`, `Reason`.
- Produces: `o.gate(p implementPayload)` - the renamed `o.clarify`, still `verifyApprovalScope`'s only caller.
- Consumes: MR4's wire field names.

- [ ] **Step 1: Write the failing tests**

Rename every `TestOutcome_Clarify_*` to `TestOutcome_Gate_*` and rewrite them against `kind: "implement"` with the new action values. The full list to migrate (`internal/restapi/outcome_test.go`, all **PURE UNIT with `fake.NewClientBuilder()` - no restapi test uses envtest and none should start**):

| current name | line | becomes |
|---|---|---|
| `TestOutcome_Clarify_ImplementRequiresApprovalOnEveryOwnedIssue` | 604 | `TestOutcome_Gate_ApprovedRequiresApprovalOnEveryOwnedIssue` |
| `TestOutcome_Clarify_AutoApproveIncrementsCounter` | 683 | `TestOutcome_Gate_AutoApproveIncrementsCounter` |
| `TestOutcome_Clarify_CitationsReachTheVerifierVerbatim` | 709 | `TestOutcome_Gate_CitationsReachTheVerifierVerbatim` |
| `TestOutcome_Clarify_CitationRefusalIs200AndParks` | 734 | `TestOutcome_Gate_RefusalIs200AndDoesNotPark` (behaviour CHANGES - Task 6.8) |
| `TestOutcome_Clarify_ClosedIssueIsNotALicence` | 854 | `TestOutcome_Gate_ClosedIssueIsNotALicence` |
| `TestOutcome_Clarify_ClosedIssueDoesNotBlockALiveOne` | 958 | `TestOutcome_Gate_ClosedIssueDoesNotBlockALiveOne` |
| `TestOutcome_Clarify_GrantIsAuditedWithTheApprover` | 1017 | `TestOutcome_Gate_GrantIsAuditedWithTheApprover` |
| `TestOutcome_Clarify_DiscussParksAwaitingHuman` | 1130 | `TestOutcome_Gate_DiscussParksAwaitingHuman` |
| `TestOutcome_Clarify_CloseRejectsAndQueuesTheIssueClose` | 1162 | `TestOutcome_Gate_RejectedRejectsAndQueuesTheIssueClose` |
| `TestOutcome_Clarify_ReasonAlwaysRequired` | 1174 | `TestOutcome_Gate_ReasonAlwaysRequired` |

Add:

```go
func TestOutcome_KindClarifyIsRejected(t *testing.T) {
	// No permanent alias. Leaving kind=clarify valid preserves a live path to
	// approval that skips the new gate - the exact hole #521 exists to close.
	rec := postOutcome(t, srv, task, `{"kind":"clarify","payload":{"decision":"implement","reason":"r"}}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "unknown outcome kind")
}

func TestOutcome_ImplementSubmittedStillWorksUnderTheMergedKind(t *testing.T) {
	// The fold must not regress the code path. This is the whole reason the
	// implement handler is EXTENDED rather than replaced.
	task := taskIn(v1alpha1.StateUnderImplementation, "implement")
	rec := postOutcome(t, srv, task, `{"kind":"implement","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, v1alpha1.StateAwaitingReview, reload(t, srv, task).Status.State)
}
```

- [ ] **Step 2: Run to verify they fail, then implement**

`internal/restapi/outcome.go`:

1. Extend `implementPayload` (line 77):

```go
type implementPayload struct {
	Action             string   `json:"action"`
	Title              string   `json:"title,omitempty"`
	Body               string   `json:"body,omitempty"`
	ChangeSignificance string   `json:"changeSignificance,omitempty"`
	MergeOrder         []string `json:"mergeOrder,omitempty"`
	Reason             string   `json:"reason,omitempty"`
	// The gate fields. Present only on action=approved; the handler refuses
	// them on submitted/declined so a code outcome cannot smuggle an approval.
	ApprovingMaintainer string                            `json:"approvingMaintainer,omitempty"`
	PlanNoteID          string                            `json:"planNoteId,omitempty"`
	ApprovalCitations   []tatarav1alpha1.ApprovalCitation `json:"approvalCitations,omitempty"`
}
```

2. Delete `clarifyPayload` (lines 135-140).
3. In the dispatch switch (line 312), delete `case "clarify":` and route inside `o.implement`:

```go
	case "implement":
		var p implementPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		switch p.Action {
		case "approved", "discuss", "rejected":
			oc.gate(p)
		default:
			oc.implement(p)
		}
	case "documentation":
		var p implementPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.implement(p)
```

4. Rename `o.clarify` (line 1221) to `o.gate`, change its parameter to `implementPayload`, and map `p.Decision` -> `p.Action` with the three new values: `implement` -> `approved`, `close` -> `rejected`, `discuss` -> `discuss`. Its `close` branch enters `StateRejected` with `ReasonDeclined`; its `discuss` branch calls `stage.Park(t, ReasonAwaitingHuman, now)` and no longer calls `Enter`. Its `approved` branch is Task 6.8.

`internal/agent/pod.go`: delete `kindProfiles["clarify"]` (line 941). The comment at 928-937 already documents that a missing key fails CLOSED - keep it verbatim, it is now load-bearing for a kind that used to exist.

`internal/controller/assignment.go`: rename the `case stage.AgentClarify:` arm to `case stage.AgentImplement:` and merge it with the existing implement arm. The withdrawal-veto paragraph at lines 151-154 MOVES into the merged arm unchanged - `tatara-implement-gate/SKILL.md` carries the same text verbatim and the two must not drift. Add a one-line comment saying so and naming the skill file.

- [ ] **Step 3: Verify no `clarify` remains**

```bash
grep -rn "clarify\|Clarify" --include="*.go" . | grep -v "_test.go" | grep -v "MEMORY"
```

Expected: nothing except historical comments. Then:

```bash
mise exec -- go test ./internal/restapi/ ./internal/agent/ ./internal/controller/ -count=1
```

- [ ] **Step 4: Commit**

---

## Task 6.8: the gate extension

**Files:** Modify `internal/restapi/outcome.go` (`o.gate`, `verifyApprovalScope` at 1446-1481), `internal/controller/approval_grammar.go` (refusal constants at 58-91, `verifyOneIssue` at 227-317), `api/v1alpha1/issue_types.go` (`ApprovalEvidence` at 172-182), `internal/restapi/handlers_v2.go` (`postNote` at 326+, so it returns the note id).

**Interfaces:**
- Produces: refusal codes `approver-not-maintainer`, `approver-mismatch`, `plan-note-missing`, `plan-hash-mismatch` (8 total, up from 6).
- Produces: `ApprovalEvidence.PlanHash string`.
- Produces: a 200 `{granted, reason, declared}` refusal body that does NOT park.

- [ ] **Step 1: Write the failing tests**

`internal/restapi/outcome_test.go`, all PURE UNIT:

```go
// ADDITION 1. THIS is the reporter-self-approval case the maintainer asked to be
// legible. Today it collapses into the generic citation-not-maintainer, which
// says "your citation is bad" when the truth is "the person you named is not a
// maintainer".
func TestOutcome_Gate_ApproverNotMaintainerIsItsOwnRefusal(t *testing.T) {
	rec := postApproved(t, srv, task, approvedPayload{
		ApprovingMaintainer: "some-drive-by",
		Citations:           []cite{{ID: "c1", Quote: "go ahead"}},
		PlanNoteID:          planNote.ID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var got gateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.False(t, got.Granted)
	require.Equal(t, controller.ApprovalRefusedApproverNotMaintainer, got.Reason)
	require.Equal(t, "some-drive-by", got.Declared)
}

// ADDITION 2. This is what stops the username becoming a second, weaker
// authority. The citation remains the sole authority; the username is a
// DECLARATION that must AGREE with it.
func TestOutcome_Gate_ApproverMismatchIsItsOwnRefusal(t *testing.T) {
	// c1 was authored by szymonrychu; the agent declares a different maintainer.
	rec := postApproved(t, srv, task, approvedPayload{
		ApprovingMaintainer: "other-maintainer",
		Citations:           []cite{{ID: "c1", Quote: "go ahead"}},
		PlanNoteID:          planNote.ID,
	})
	var got gateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.False(t, got.Granted)
	require.Equal(t, controller.ApprovalRefusedApproverMismatch, got.Reason)
}

func TestOutcome_Gate_ApproverMatchIsCaseInsensitive(t *testing.T) {
	rec := postApproved(t, srv, task, approvedPayload{
		ApprovingMaintainer: "SzymonRychu",
		Citations:           []cite{{ID: "c1", Quote: "go ahead"}},
		PlanNoteID:          planNote.ID,
	})
	require.True(t, decodeGate(t, rec).Granted, "forge logins are case-insensitive")
}

// ADDITION 3. Plan pinning. NEW in the merged model: previously approval ended
// the clarify Task and a FRESH implement pod started, so no artifact sat between
// approval and execution. Now the same live agent brainstorms, is approved, and
// implements - so the plan it was approved on is an artifact it can edit.
func TestOutcome_Gate_StoresThePlanHashOnTheEvidence(t *testing.T) {
	rec := postApproved(t, srv, task, validApproval(planNote))
	require.True(t, decodeGate(t, rec).Granted)

	iss := reloadIssue(t, srv, "iss-1")
	require.Equal(t,
		fmt.Sprintf("%x", sha256.Sum256([]byte(planNote.Body))),
		iss.Status.Approval.PlanHash)
}

func TestOutcome_Gate_RefusesAnUnknownPlanNoteID(t *testing.T) {
	rec := postApproved(t, srv, task, approvedPayload{
		ApprovingMaintainer: "szymonrychu",
		Citations:           []cite{{ID: "c1", Quote: "go ahead"}},
		PlanNoteID:          "n-deadbeefdeadbeef",
	})
	got := decodeGate(t, rec)
	require.False(t, got.Granted)
	require.Equal(t, controller.ApprovalRefusedPlanNoteMissing, got.Reason)
}

func TestOutcome_Gate_SubmittedRefusesAPlanEditedAfterApproval(t *testing.T) {
	require.True(t, decodeGate(t, postApproved(t, srv, task, validApproval(planNote))).Granted)

	// The agent rewrites the plan, then tries to ship code against it.
	rewritePlanNote(t, srv, task, "a completely different plan")

	rec := postOutcome(t, srv, task,
		`{"kind":"implement","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.False(t, decodeGate(t, rec).Granted)
	require.Equal(t, controller.ApprovalRefusedPlanHashMismatch, decodeGate(t, rec).Reason)
	require.Equal(t, v1alpha1.StateRefined, reload(t, srv, task).Status.State,
		"the cheap path out is back to refined, not a park")
}

// ADDITION 4. A refusal is a 200 and it does NOT park. The agent is still alive
// under the merged model and should be told no and keep talking. Under the old
// model the clarify pod was dead after its one turn, so parking was the only way
// to hold the work; that is no longer true.
func TestOutcome_Gate_RefusalIs200AndDoesNotPark(t *testing.T) {
	rec := postApproved(t, srv, task, approvedPayload{
		ApprovingMaintainer: "szymonrychu",
		Citations:           []cite{{ID: "c1", Quote: "a quote that is not in the comment"}},
		PlanNoteID:          planNote.ID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	got := decodeGate(t, rec)
	require.False(t, got.Granted)
	require.Equal(t, controller.ApprovalRefusedQuoteAbsent, got.Reason)

	tk := reload(t, srv, task)
	require.False(t, v1alpha1.Parked(tk), "a refusal must NOT park; the agent keeps talking")
	require.Equal(t, v1alpha1.StateRefined, tk.Status.State)
}

func TestOutcome_Gate_TheSixExistingRefusalsStillFireUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
	}{
		{"no maintainer has commented", controller.ApprovalRefusedNoMaintainer},
		{"maintainer commented but no citation", controller.ApprovalRefusedNoCitation},
		{"citation names a non-maintainer comment", controller.ApprovalRefusedCitationNotMaintainer},
		{"quote is not in the body", controller.ApprovalRefusedQuoteAbsent},
		{"that comment already approved once", controller.ApprovalRefusedEvidenceReplayed},
		{"the task owns no live issue", controller.ApprovalRefusedNoLiveIssue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, decodeGate(t, postApproved(t, srv, setupFor(t, tc.name), someApproval())).Reason)
		})
	}
}

func TestApprovalRefusalVocabularyIsTen(t *testing.T) {
	require.Len(t, controller.ApprovalRefusals, 10,
		"the SIX that existed, plus approver-not-maintainer, approver-mismatch, plan-note-missing and plan-hash-mismatch")
	require.ElementsMatch(t, []string{
		controller.ApprovalRefusedNoMaintainer,
		controller.ApprovalRefusedNoCitation,
		controller.ApprovalRefusedCitationNotMaintainer,
		controller.ApprovalRefusedQuoteAbsent,
		controller.ApprovalRefusedEvidenceReplayed,
		controller.ApprovalRefusedNoLiveIssue,
		controller.ApprovalRefusedApproverNotMaintainer,
		controller.ApprovalRefusedApproverMismatch,
		controller.ApprovalRefusedPlanNoteMissing,
		controller.ApprovalRefusedPlanHashMismatch,
	}, controller.ApprovalRefusals)

	// The vocabulary is a metric LABEL VALUE. Every member must be
	// Prometheus-safe and none may be empty, or the counter that names which
	// refusal fired becomes the counter that cannot.
	for _, r := range controller.ApprovalRefusals {
		require.Regexp(t, `^[a-z][a-z-]*[a-z]$`, r)
	}
}
```

Plus, in `internal/restapi/handlers_v2_test.go`:

```go
func TestPostNote_ReturnsTheNoteID(t *testing.T) {
	rec := postNote(t, srv, task, `{"kind":"plan","body":"the plan"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	var got struct {
		NoteID string `json:"noteId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Regexp(t, `^n-[0-9a-f]{16}$`, got.NoteID,
		"the agent cannot quote a planNoteId it was never given")
}
```

- [ ] **Step 2: Run to verify they fail, then implement**

`internal/controller/approval_grammar.go`, after the existing six constants (58-91):

```go
	// ApprovalRefusedApproverNotMaintainer: the agent DECLARED an approving
	// maintainer who is not one. Today this collapses into the generic
	// citation-not-maintainer, which reports the wrong thing: the citation may
	// be fine and the declared login is what is wrong. This is the
	// reporter-self-approval case, made legible.
	ApprovalRefusedApproverNotMaintainer = "approver-not-maintainer"
	// ApprovalRefusedApproverMismatch: the declared approver is not the author
	// of the comment that was cited. This is what stops the username becoming a
	// second, weaker authority - the citation stays the SOLE authority and the
	// username is a declaration that must AGREE with it.
	ApprovalRefusedApproverMismatch = "approver-mismatch"
	// ApprovalRefusedPlanNoteMissing: planNoteId names no note on this Task.
	ApprovalRefusedPlanNoteMissing = "plan-note-missing"
	// ApprovalRefusedPlanHashMismatch: the plan note's body changed between the
	// grant and the attempt to write code against it.
	ApprovalRefusedPlanHashMismatch = "plan-hash-mismatch"
)

// ApprovalRefusals is the closed vocabulary. It is the `reason` label on
// operator_approval_refused_total and the `reason` field in the 200 refusal
// body, so it must stay small, stable, and total.
var ApprovalRefusals = []string{
	ApprovalRefusedNoMaintainer,
	ApprovalRefusedNoCitation,
	ApprovalRefusedCitationNotMaintainer,
	ApprovalRefusedQuoteAbsent,
	ApprovalRefusedEvidenceReplayed,
	ApprovalRefusedNoLiveIssue,
	ApprovalRefusedApproverNotMaintainer,
	ApprovalRefusedApproverMismatch,
	ApprovalRefusedPlanNoteMissing,
	ApprovalRefusedPlanHashMismatch,
}
```

In `verifyOneIssue`, after the `cited == nil` check (line 291) and BEFORE the quote check, add the two cross-checks. They go here, not in `verifyApprovalScope`, because both need the cited comment in hand:

```go
	// The DECLARED approver is checked against the CITED comment's author, in
	// this order: not-a-maintainer first (the more specific, more actionable
	// failure), then mismatch.
	if declared != "" {
		if !v1alpha1.IsMaintainer(proj, repo, declared) {
			return nil, ApprovalRefusedApproverNotMaintainer
		}
		if !strings.EqualFold(declared, cited.Author) {
			return nil, ApprovalRefusedApproverMismatch
		}
	}
```

Thread `declared string` through `VerifyApproval` and `verifyApprovalScope`.

Add `PlanHash` to `ApprovalEvidence` (`api/v1alpha1/issue_types.go:172`):

```go
	// PlanHash is sha256 of the plan note's body AS IT STOOD AT GRANT. It is
	// re-checked on the transition out of the gate into code.
	//
	// This is NEW in the merged model and it exists because the model created
	// the gap: previously approval ended the clarify Task and a fresh implement
	// pod started, so no artifact sat between approval and execution. Now the
	// same live agent brainstorms, is approved, and implements - so the plan it
	// was approved on is an artifact it can edit afterwards. Empty on
	// auto-approval evidence (there is no plan to pin).
	// +optional
	PlanHash string `json:"planHash,omitempty"`
```

In `o.gate`'s approved branch, resolve `p.PlanNoteID` against `task.Status.Notes`, refuse `ApprovalRefusedPlanNoteMissing` if absent, and set `ev.PlanHash = fmt.Sprintf("%x", sha256.Sum256([]byte(note.Body)))` before writing the evidence.

In `o.implement`'s `submitted` branch, re-check the pin before entering `awaiting-review`. On mismatch, return the 200 refusal AND enter `StateRefined` - **the cheap path out, per the design's risk 5.** Do NOT park: an agent that finds the plan-gate expensive will simply stop updating its plan note, which destroys the note's value as continuation state.

Replace `o.ok("implement-unverified")` with a dedicated writer:

```go
// gateResponse is the 200 body a REFUSED gate returns. It is NOT an error and
// it does NOT park: under the merged model the agent is still alive and should
// be told no and keep talking. `declared` echoes back the approvingMaintainer
// the agent sent, so the refusal is self-explaining in the pod's transcript.
type gateResponse struct {
	Granted  bool   `json:"granted"`
	Reason   string `json:"reason,omitempty"`
	Declared string `json:"declared,omitempty"`
}
```

In `postNote` (`internal/restapi/handlers_v2.go`), stamp `note.ID = v1alpha1.NewNoteID(note.At, note.Kind, note.Body)` and return `{"noteId": note.ID}` alongside the existing body.

**Selective quoting stays OPEN and the MR description must say so.** "go ahead" really is a substring of "do not go ahead until CI is green". Plan-pinning closes approve-then-swap-scope, a different attack. The mitigation is DETECTION: the operator posts a confirmation comment naming the approver and quoting what was cited, so a bypass is visible within minutes. Implement that comment in this task, reusing the `PendingComments` drain, and test it:

```go
func TestOutcome_Gate_GrantPostsAConfirmationCommentNamingTheApproverAndTheQuote(t *testing.T) {
	require.True(t, decodeGate(t, postApproved(t, srv, task, validApproval(planNote))).Granted)
	iss := reloadIssue(t, srv, "iss-1")
	require.Len(t, iss.Status.PendingComments, 1)
	body := iss.Status.PendingComments[0].Body
	require.Contains(t, body, "szymonrychu")
	require.Contains(t, body, "go ahead")
	require.Contains(t, body, planNote.ID)
}
```

- [ ] **Step 3: Run tests, run `make generate`/`make manifests` (ApprovalEvidence changed), commit**

```bash
mise exec -- make generate && mise exec -- make manifests
mise exec -- go test ./internal/restapi/ ./internal/controller/ ./api/... -count=1
git add api/ internal/ charts/tatara-operator/crd-bases/
git commit -m "feat(gate)!: bind the declared approver and pin the plan

approvingMaintainer is a DECLARATION that must agree with the citation, not a
second authority: approver-not-maintainer and approver-mismatch are now their own
refusals instead of collapsing into citation-not-maintainer. The plan note is
hashed at grant and re-checked before code ships. A refusal is a 200 with
granted:false and does NOT park - the agent is alive and should keep talking.

NOT DOING, deliberately: no recency rule (an earlier approving comment must stay
citable or ordinary threads deadlock); no approver!=author rule (the reporter is
the only maintainer on most issues here, so it would deadlock the platform's
commonest shape - the bot-identity exclusion transfers, the human one does not);
author_association deferred (stronger in kind but defends against maintainerLogins
drift, which cannot occur in a one-name list). SELECTIVE QUOTING REMAINS OPEN:
\"go ahead\" is a substring of \"do not go ahead until CI is green\". Mitigation is
detection - the operator posts a confirmation comment naming the approver and the
quote, so a bypass is visible within minutes."
```

---

## Task 6.9: delete resume.go

**Files:** Delete `internal/controller/resume.go` (291 lines), `internal/controller/resume_deploy_test.go`, `internal/controller/resume_pacing_test.go`. Modify `internal/controller/project_controller.go` (line 450 and the doc references at 66, 194, 432).

**Do NOT patch `resumeOne` to check `created`.** That leaves the sever at `resume.go:150` running and produces a NEW split state: the issue orphaned, the Task alive. The whole file's reason to exist is gone.

**Why it is gone:** `resumeNoReentryParks` existed because a human reply to a no-re-entry park had nowhere to go - the Task was `parked`, `TaskDone(parked)` was true, and `HasReentry(reason)` was false, so `driveUnparks` skipped it. Its answer was to sever the issue and re-mint. Under the new model, a Task parked with an `UnparkNever` reason is still in its state with `TaskDone` false, so the sweep's ordinary re-mint path handles the issue and the reaper collects the Task at `ParkRetention`. Nothing needs a sever.

The caller-graph is clean: **every symbol resume.go defines is called only from inside resume.go, except `resumeNoReentryParksPaced`, whose only production caller is `project_controller.go:450`.** `forgeItemFromMirror` has one extra caller and it is a test (`resume_deploy_test.go:97`), which is deleted with it.

- [ ] **Step 1: Write the test that proves the bug is gone (BEFORE deleting)**

Add to `internal/controller/intake_test.go` (**PURE UNIT with a fake client** - `intake_test.go` currently uses envtest for other cases; this one does not need it):

```go
// THE #521 REGRESSION TEST, at the exact line the trace ended at.
// resume.go:47 -> :150 sever -> :161 MintForItem -> intake.go:311. Under the
// stage machine TaskDone(parked) was true, so the live-twin branch at :311 was
// skipped, Create 409ed, and control reached the delete at :332-339 - deleting
// a live Task and cascading its owned Issue and MergeRequest mirrors.
func TestCreateTaskRaceSafe_TakesTheLiveTwinBranchForAParkedTask(t *testing.T) {
	for _, reason := range []string{
		stage.ReasonBacklogSweep, stage.ReasonAwaitingHuman, stage.ReasonNoOutcome,
		stage.ReasonReviewLoopExhausted, stage.ReasonStageDeadline,
	} {
		t.Run(reason, func(t *testing.T) {
			existing := taskIn(v1alpha1.StateRefined, "implement")
			existing.Name = v1alpha1.IntakeTaskName("tatara", "implement", "repo", 521)
			require.NoError(t, stage.Park(existing, reason, now))

			m := newMinter(t, existing)
			created, twin, err := m.createTaskRaceSafe(ctx, sameNamedTask(existing))
			require.NoError(t, err)
			require.False(t, created)
			require.NotNil(t, twin, "a parked Task is a LIVE twin; the delete must be unreachable for it")
			require.Equal(t, existing.Name, twin.Name)

			var got v1alpha1.Task
			require.NoError(t, m.Client.Get(ctx, key(existing), &got),
				"the existing Task must still exist")
		})
	}
}
```

- [ ] **Step 2: Run it - it should already PASS after Task 6.1**

```bash
mise exec -- go test ./internal/controller/ -run TestCreateTaskRaceSafe_TakesTheLiveTwin -v
```

Expected: PASS, because `TaskDone` is now exactly `{done, rejected}`. This is the confirmation that re-pointing the constant WOULD have been enough for THIS branch - and Step 3 is why it was not enough overall.

- [ ] **Step 3: Delete**

```bash
git rm internal/controller/resume.go internal/controller/resume_deploy_test.go internal/controller/resume_pacing_test.go
```

`resume_deploy_test.go` also holds two tests for a DIFFERENT function - `TestEnqueueDeployTimeoutComment_FirstOnly` (line 144) and `TestEnqueueDeployTimeoutComment_DoesNotClobberRefireCooldown` (line 171) exercise `task_stage.go`'s `enqueueDeployTimeoutComment`, not resume.go. **Move those two into `internal/controller/task_stage_test.go` before deleting the file.** Losing them is a silent coverage regression.

In `project_controller.go`, delete the `resumeRequeue, err := r.resumeNoReentryParksPaced(...)` block at line 450 and the requeue folding that consumes it, plus the doc-comment references at 66, 194 and 432.

Also delete `internal/controller/brainstorm_resume.go`? **NO.** It is an unrelated feature (resuming a paused brainstorm on a foreign push). Do not conflate it with resume.go.

- [ ] **Step 4: Verify and commit**

```bash
mise exec -- go build ./... && mise exec -- go test ./internal/controller/ -count=1
git commit -am "refactor!: delete resume.go entirely

Its whole reason to exist was that TaskDone(parked) was true, so a human reply
to a no-re-entry park had nowhere to go and it severed the issue and re-minted.
With TaskDone == {done, rejected} the sweep's ordinary re-mint handles the issue
and the reaper collects the Task at ParkRetention. Patching resumeOne to check
`created` would have left the sever at :150 running and produced a NEW split
state: issue orphaned, Task alive.

Moved TestEnqueueDeployTimeoutComment_* into task_stage_test.go - they lived in
resume_deploy_test.go but test task_stage.go."
```

---

## Task 6.10: the TaskDone carve-outs

**Files:** `internal/webhook/server.go` (line ~421), `internal/controller/sweep.go` (`taskStillPushes`, ~1194), `internal/controller/intake.go` (`createTaskRaceSafe`, 305-341), `internal/controller/queue_controller.go` (`queueTaskDone` 219, `queueTaskHoldsSlot` 229, `ticketSpent` 248).

Four call sites currently work around `TaskDone` folding `parked` into "terminal". Two of them are `TaskDone(t) && stage != parked` written as De Morgan duals of each other. **They collapse to a bare `TaskDone(t)`.** Both are harmless today and lethal to the next reader: the design already fighting itself.

- [ ] **Step 1: Write the failing tests**

```go
// internal/webhook/server_test.go - PURE UNIT
func TestWebhook_AParkedTaskIsNotFilteredAsTerminal(t *testing.T) {
	tk := taskIn(v1alpha1.StateAwaitingReview, "implement")
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	rec := postReviewEvent(t, srv, tk)
	require.NotEqual(t, "ignored", acceptedResult(t, rec),
		"a parked Task is not terminal; the applier decides which parks resume")
}

// internal/controller/sweep_test.go - PURE UNIT
func TestTaskStillPushes_IsExactlyNotDone(t *testing.T) {
	for _, state := range []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
	} {
		require.True(t, taskStillPushes(taskIn(state, "implement")))
		parked := taskIn(state, "implement")
		require.NoError(t, stage.Park(parked, stage.ReasonOwnershipLost, now))
		require.True(t, taskStillPushes(parked), "a parked(ownership-lost) Task resumes pushing")
	}
	for _, state := range []string{v1alpha1.StateDone, v1alpha1.StateRejected} {
		require.False(t, taskStillPushes(taskIn(state, "implement")))
	}
}

// internal/controller/queue_controller_test.go - PURE UNIT
func TestQueueTaskHoldsSlot_ExcludesParkedAndOperatorDrivenStates(t *testing.T) {
	require.True(t, queueTaskHoldsSlot(taskIn(v1alpha1.StateUnderImplementation, "implement")))
	require.False(t, queueTaskHoldsSlot(taskIn(v1alpha1.StateMerged, "implement")),
		"operator-driven runs no agent")
	require.False(t, queueTaskHoldsSlot(taskIn(v1alpha1.StateNew, "implement")),
		"new is operator triage and leaves immediately")
	require.False(t, queueTaskHoldsSlot(taskIn(v1alpha1.StateDone, "implement")))

	parked := taskIn(v1alpha1.StateUnderImplementation, "implement")
	require.NoError(t, stage.Park(parked, stage.ReasonAwaitingHuman, now))
	require.False(t, queueTaskHoldsSlot(parked),
		"THE SLOT LEAK: a parked Task runs no pod and must not eat the pool")
}
```

- [ ] **Step 2: Implement**

`internal/webhook/server.go:421`:

```go
	// A truly-terminal Task (done/rejected) is never resurrected. A PARKED Task
	// is NOT terminal any more - TaskDone stopped folding it in with #521 - so
	// there is nothing left to carve out here. The appliers route a park by its
	// reason and remain the authority on which parks resume.
	if tatarav1.TaskDone(task) {
```

`internal/controller/sweep.go`:

```go
// taskStillPushes reports whether t may still push to its branch. It is exactly
// !TaskDone. The old `|| stage == StageParked` clause is gone with #521: parked
// stopped being a terminal, so a parked(ownership-lost) Task is already covered
// by !TaskDone.
func taskStillPushes(t *tatarav1alpha1.Task) bool {
	return !tatarav1alpha1.TaskDone(t)
}
```

`internal/controller/queue_controller.go`:

```go
// queueTaskHoldsSlot reports whether a Task still occupies a pod slot.
//
// THREE exclusions, and the third is new with #521. An OPERATOR-DRIVEN state
// (merged/deploying) runs no agent, and neither does `new` (operator triage,
// which leaves immediately) - counting them re-creates the lane-starvation trap
// (operator-laneoccupancy-starves-recovery-2026-06-15). And a PARKED Task runs
// no pod at all: park is what takes the pod down. It is NOT treated as done,
// though - it re-acquires a slot when it un-parks - so its QueuedEvent is kept,
// not GC'd.
func queueTaskHoldsSlot(t *tatarav1alpha1.Task) bool {
	return !tatarav1alpha1.TaskDone(t) &&
		!tatarav1alpha1.Parked(t) &&
		stage.Live(t.Status.State)
}
```

Rewrite `ticketSpent`'s stage-literal switch against `stage.AgentKindFor(t.Status.State, t.Spec.Kind)` and the `StateNew` case.

- [ ] **Step 3: Run tests and commit**

---

## Task 6.11: the one-shot migrator - WITHDRAWN, NOT SHIPPED

> **SUPERSEDED 2026-08-08. THIS TASK WAS BUILT AND THEN DELETED WHOLE.** CRD
> structural pruning is applied on the READ path, not only on write: once the
> narrowed CRD is served, no GET returns `status.stage` on any object, so the
> migrator specified below has nothing to read and cannot work. Measured against
> envtest k8s 1.33.0. The maintainer ruled to SKIP THE MIGRATION AND ACCEPT THE
> RESET - every live Task boots stateless and re-derives through the ordinary
> create edge - with one guard, `internal/controller/reset_guard.go:terminalResetTarget`,
> which keeps already-finished Tasks out of the live lifecycle on evidence that
> survives pruning. `internal/migrate` and its `cmd/manager` wiring do not exist.
> Everything below is kept as the record of what was tried. See MEMORY.md
> 2026-08-08 and the resolved ROADMAP.md item.


**Files:**
- Create: `internal/migrate/migrate.go`
- Create: `internal/migrate/migrate_test.go`
- Create: `internal/migrate/migrate_envtest_test.go`
- Modify: `cmd/manager/main.go` (between `buildManager` and `mgr.Start(ctx)`)
- Modify: `charts/tatara-operator/templates/rbac.yaml` if new verbs are needed

**Interfaces:**
- Produces: `migrate.Tasks(ctx, c client.Client, ns string, now time.Time) (Report, error)`, `migrate.MapOne(t *v1alpha1.Task) (Plan, bool)`.
- Consumes: `v1alpha1.State*`, `stage.Reason*`.

**Contract:**
1. It runs SYNCHRONOUSLY in `cmd/manager` BEFORE `mgr.Start()`, against `mgr.GetAPIReader()` for reads and a direct client for writes - the informer cache is not running yet.
2. It is IDEMPOTENT PER OBJECT. A Task already carrying `status.state` is skipped.
3. **If it cannot complete, the manager REFUSES TO START.** `run()` returns the error, the process exits non-zero, the pod CrashLoopBackOffs. That is correct: a mixed population is the silent-drift shape that produced #521. Restart mid-migration is a non-event - K of 118 are done, the next boot finishes the rest.
4. It rewrites `Spec.Kind` clarify -> implement AND `Status.AgentKind` clarify -> implement, and deletes the Task's agent pod so it respawns on the implement profile. Both are defensive on today's population (measured: `Status.AgentKind` is empty on all 118, and zero agent pods exist) and must be written to be no-ops when there is nothing to do, not assumed to fire.
5. Every migrated object gets `metadata.annotations["tatara.dev/migrated-from"] = "<oldStage>/<oldReason>"` as a recovery audit trail.

**The mapping, keyed on the measured census (Task P0.4):**

| n | old stage / reason | -> state | -> parkReason | note |
|---|---|---|---|---|
| 52 | parked / backlog-sweep | `new` | `backlog-sweep` | The strongest argument FOR the orthogonal flag: 52 objects whose whole job is owning an Issue at zero agent cost, which today requires wearing a fake terminal stage. |
| 17 | parked / awaiting-human (kind=incident, from clarifying) | `new` | `awaiting-human` | |
| 15 | parked / awaiting-human (kind=review, from (create)) | `awaiting-review` | `awaiting-human` | |
| 6 | parked / awaiting-human (kind=clarify, from clarifying) | `new` | `awaiting-human` | |
| 4 | parked / no-outcome (from investigating) | `new` | `no-outcome` | per `parkedFromStage` |
| 1 | parked / no-outcome (from refining) | `new` | `no-outcome` | per `parkedFromStage` |
| 2 | parked / review-loop-exhausted (from reviewing) | `awaiting-review` | `review-loop-exhausted` | per `parkedFromStage` |
| 1 | parked / ownership-lost (from implementing) | `under-implementation` | `ownership-lost` | per `parkedFromStage` |
| 8 | rejected / tracked-elsewhere | `rejected` | `""` | `stateReason=tracked-elsewhere` |
| 1 | rejected / issue-closed | `rejected` | `""` | `stateReason=issue-closed` |
| 6 | delivered (kind=refine) | `done` | `""` | |
| 1 | delivered (kind=brainstorm) | `done` | `""` | |
| 4 | failed / {pod-recreation-exhausted, operator-error, merge-blocked x2} | `rejected` | `""` | **Deliberate deviation, scoped to the migration only.** These are past their useful life with retention clocks running, and resurrecting a `merge-blocked` Task re-arms work against stale MRs. The maintainer's failed -> new/refined rule governs FUTURE failures, not this backlog. `stateReason` carries the old failure reason. |

Total 118.

The generic rule for anything NOT in that table (a Task minted between plan-writing and rollout):

```
old stage       -> new state
triaging        -> new
brainstorming   -> refined
clarifying      -> refined
investigating   -> refined
refining        -> refined
conversing      -> refined (or awaiting-review if parkedFromStage was reviewing)
approved        -> refined
implementing    -> under-implementation
reviewing       -> awaiting-review
merging         -> merged
deploying       -> deployed
delivered       -> done
documenting     -> under-implementation
rejected        -> rejected
failed          -> rejected            (migration-only, see above)
parked          -> map(parkedFromStage) with parkReason = old stageReason
```

- [ ] **Step 1: Write the failing tests - the pure mapper first**

Create `internal/migrate/migrate_test.go`. **PURE UNIT: `MapOne` takes a Task and returns a Plan, no client.** This is where the bucket coverage lives, because 118 objects through envtest is slow and buys nothing the mapper does not already prove.

```go
// THE LIVE CENSUS, measured 2026-08-07 with kubectl. The buckets sum to 118 and
// the sum is asserted, so a mapping that silently drops a bucket fails here
// rather than in production.
func TestMapOne_CoversTheLiveCensusExactly(t *testing.T) {
	type bucket struct {
		n                                  int
		stage, reason, kind, parkedFrom    string
		wantState, wantPark, wantStateReas string
	}
	buckets := []bucket{
		{52, "parked", "backlog-sweep", "clarify", "(create)", "new", "backlog-sweep", ""},
		{17, "parked", "awaiting-human", "incident", "clarifying", "new", "awaiting-human", ""},
		{15, "parked", "awaiting-human", "review", "(create)", "awaiting-review", "awaiting-human", ""},
		{6, "parked", "awaiting-human", "clarify", "clarifying", "new", "awaiting-human", ""},
		{4, "parked", "no-outcome", "incident", "investigating", "new", "no-outcome", ""},
		{1, "parked", "no-outcome", "refine", "refining", "new", "no-outcome", ""},
		{2, "parked", "review-loop-exhausted", "incident", "reviewing", "awaiting-review", "review-loop-exhausted", ""},
		{1, "parked", "ownership-lost", "clarify", "implementing", "under-implementation", "ownership-lost", ""},
		{8, "rejected", "tracked-elsewhere", "incident", "", "rejected", "", "tracked-elsewhere"},
		{1, "rejected", "issue-closed", "clarify", "conversing", "rejected", "", "issue-closed"},
		{6, "delivered", "", "refine", "", "done", "", ""},
		{1, "delivered", "", "brainstorm", "", "done", "", ""},
		{1, "failed", "pod-recreation-exhausted", "review", "", "rejected", "", "pod-recreation-exhausted"},
		{1, "failed", "operator-error", "incident", "merging", "rejected", "", "operator-error"},
		{1, "failed", "merge-blocked", "incident", "merging", "rejected", "", "merge-blocked"},
		{1, "failed", "merge-blocked", "clarify", "merging", "rejected", "", "merge-blocked"},
	}

	total := 0
	for _, b := range buckets {
		total += b.n
		tk := legacyTask(b.stage, b.reason, b.kind, b.parkedFrom)
		plan, ok := migrate.MapOne(tk)
		require.True(t, ok, "bucket %+v must map", b)
		require.Equal(t, b.wantState, plan.State, "bucket %+v", b)
		require.Equal(t, b.wantPark, plan.ParkReason, "bucket %+v", b)
		require.Equal(t, b.wantStateReas, plan.StateReason, "bucket %+v", b)
		require.Equal(t, b.stage+"/"+b.reason, plan.MigratedFrom)
	}
	require.Equal(t, 118, total, "the live census on 2026-08-07 was 118 Tasks; update this plan and this test together")
}

// The 61 spec.kind=clarify Tasks. THE REAL BREAKAGE is not the outcome gate
// (status.agentKind is empty on all 118, so nothing can 409): it is that under
// the 8-state model there is no clarifying state for spec.kind=clarify to route
// into out of `new`, so all 61 would wedge with no legal edge.
func TestMapOne_RewritesSpecKindClarifyToImplement(t *testing.T) {
	tk := legacyTask("parked", "backlog-sweep", "clarify", "(create)")
	plan, ok := migrate.MapOne(tk)
	require.True(t, ok)
	require.Equal(t, "implement", plan.SpecKind)
	require.True(t, v1alpha1.IsKnownKind(plan.SpecKind))
}

func TestMapOne_RewritesStatusAgentKindClarifyToImplementAndAsksForAPodDelete(t *testing.T) {
	// Defensive: measured, status.agentKind is EMPTY on all 118 live Tasks.
	// This path must exist and must be a no-op when there is nothing to do.
	tk := legacyTask("clarifying", "", "clarify", "")
	tk.Status.AgentKind = "clarify"
	tk.Status.PodName = "clarify-tatara-operator-i500"
	plan, ok := migrate.MapOne(tk)
	require.True(t, ok)
	require.Equal(t, "implement", plan.AgentKind)
	require.True(t, plan.DeletePod)

	quiet := legacyTask("parked", "backlog-sweep", "clarify", "(create)")
	plan, _ = migrate.MapOne(quiet)
	require.False(t, plan.DeletePod, "no pod, nothing to delete")
}

func TestMapOne_IsIdempotentPerObject(t *testing.T) {
	tk := legacyTask("parked", "backlog-sweep", "clarify", "(create)")
	plan, ok := migrate.MapOne(tk)
	require.True(t, ok)
	migrate.Apply(tk, plan)

	_, ok = migrate.MapOne(tk)
	require.False(t, ok, "a Task already carrying status.state is skipped")
}

func TestMapOne_CoversEveryLegacyStageValue(t *testing.T) {
	// Totality. A Task minted between plan-writing and rollout must still map.
	for _, stg := range []string{
		"triaging", "brainstorming", "clarifying", "investigating", "refining",
		"approved", "implementing", "reviewing", "conversing", "merging",
		"deploying", "delivered", "documenting", "rejected", "failed", "parked",
	} {
		tk := legacyTask(stg, "", "implement", "implementing")
		plan, ok := migrate.MapOne(tk)
		require.True(t, ok, "legacy stage %q must map", stg)
		require.Contains(t, stage.AllStates(), plan.State)
	}
}

func TestMapOne_ParkedDerivesItsStateFromParkedFromStage(t *testing.T) {
	require.Equal(t, v1alpha1.StateAwaitingReview,
		mustMap(t, legacyTask("parked", "review-loop-exhausted", "implement", "reviewing")).State)
	require.Equal(t, v1alpha1.StateUnderImplementation,
		mustMap(t, legacyTask("parked", "no-outcome", "implement", "implementing")).State)
	require.Equal(t, v1alpha1.StateNew,
		mustMap(t, legacyTask("parked", "backlog-sweep", "implement", "(create)")).State,
		"(create) is not a stage; a backlog-sweep park has never run and belongs at new")
}

func TestMapOne_FailedBecomesRejectedNotNewOrRefined(t *testing.T) {
	// Deliberate deviation, SCOPED TO THE MIGRATION ONLY. These four are past
	// their useful life with retention clocks running, and resurrecting a
	// merge-blocked Task re-arms work against stale MRs. The maintainer's
	// failed -> new/refined rule governs FUTURE failures.
	plan := mustMap(t, legacyTask("failed", "merge-blocked", "incident", "merging"))
	require.Equal(t, v1alpha1.StateRejected, plan.State)
	require.Equal(t, "merge-blocked", plan.StateReason)
	require.Empty(t, plan.ParkReason)
}

func TestMapOne_EveryPlanProducesAValidCRDValue(t *testing.T) {
	// The migrator's writes must be valid ON THEIR FACE, so status-subresource
	// ratcheting is not needed for the migration itself.
	for _, stg := range legacyStages {
		for _, reason := range append(stage.Reasons, "") {
			tk := legacyTask(stg, reason, "implement", "implementing")
			plan, ok := migrate.MapOne(tk)
			if !ok {
				continue
			}
			require.Contains(t, stage.AllStates(), plan.State)
			if plan.ParkReason != "" {
				require.True(t, stage.IsParkReason(plan.ParkReason), "%s/%s -> %q", stg, reason, plan.ParkReason)
			}
		}
	}
}
```

Create `internal/migrate/migrate_envtest_test.go`. **ENVTEST-BASED, and only these three tests are** - they need a real apiserver because what they prove is CRD validation and the refuse-to-start contract, neither of which a fake client models.

```go
// ENVTEST. The fake client does not enforce CRD enums, so only a real apiserver
// can prove the migrator's writes are accepted.
func TestMigrateTasks_WritesAreAcceptedByTheNarrowedCRD(t *testing.T) {
	// Seed all 16 buckets, run, and Get each one back.
}

// ENVTEST. THE REFUSE-TO-START CONTRACT. A mixed population is the silent-drift
// shape that produced #521, so a migration that cannot finish must crash-loop
// the manager rather than run against half-migrated objects.
func TestMigrateTasks_ReturnsAnErrorTheManagerRefusesToStartOn(t *testing.T) {
	// Seed a Task whose update will be rejected (e.g. an object over the byte
	// budget), run, assert a non-nil error and that the Report names the object.
}

// ENVTEST. Restart mid-migration is a non-event: K of 118 done, next boot
// finishes the rest.
func TestMigrateTasks_ResumesAfterAPartialRun(t *testing.T) {
	// Seed 10, migrate 4 by hand, run, assert exactly 6 updated and 4 skipped.
}
```

- [ ] **Step 2: Run to verify they fail, then implement**

```go
// Package migrate is the ONE-SHOT #521 state migration. It runs synchronously
// in cmd/manager BEFORE mgr.Start(), against the manager's uncached API reader,
// because the informer cache is not running yet.
//
// IF IT CANNOT COMPLETE, THE MANAGER REFUSES TO START. CrashLoopBackOff is the
// correct outcome: a population where some Tasks carry status.state and some
// carry status.stage is exactly the silent-drift shape that produced #521. A
// restart mid-migration is a non-event - the pass is idempotent per object, K of
// N are already done, and the next boot finishes the rest.
//
// DELETE THIS PACKAGE once every cluster has booted an operator that ran it. It
// is not a compatibility layer and it must not become one.
package migrate
```

In `cmd/manager/main.go`, between `buildManager` and `mgr.Start(ctx)`:

```go
	// THE ONE-SHOT #521 MIGRATION. Before the cache, before the reconcilers,
	// before anything can observe a half-migrated Task. mgr.GetAPIReader() is a
	// direct, uncached read available the moment ctrl.NewManager returns.
	rep, err := migrate.Tasks(ctx, mgr.GetAPIReader(), mgr.GetClient(), cfg.Namespace, time.Now())
	logger.Info("state migration complete",
		"action", "state_migration", "migrated", rep.Migrated, "skipped", rep.Skipped,
		"pods_deleted", rep.PodsDeleted, "total", rep.Total)
	if err != nil {
		return fmt.Errorf("state migration did not complete; refusing to start: %w", err)
	}
```

- [ ] **Step 3: Check whether the migrator needs new RBAC verbs**

It uses `get`/`list`/`update` on `tasks`, `update` on `tasks/status`, and `delete` on `pods`. All are already granted (`charts/tatara-operator/templates/rbac.yaml`: `tatara.dev/tasks` has the full verb set, `tasks/status` has `get,update,patch`, core `pods` has the full set). **No RBAC change is expected.** Prove it:

```bash
mise exec -- make rbac-check
```

`hack/check-rbac-drift.sh` asserts SET-EQUALITY between the `+kubebuilder:rbac` markers under `./internal/controller/...` and the hand-maintained chart `rbac.yaml`. **The migrator lives in `internal/migrate`, which controller-gen does not scan**, so it contributes no markers - meaning `make rbac-check` cannot catch a missing verb for it. If a verb IS missing, the failure is a runtime 403 at boot, and the fix is to hand-edit `charts/tatara-operator/templates/rbac.yaml` to match. Add a test that names the verbs the migrator uses so the requirement is at least written down:

```go
func TestMigratorUsesOnlyAlreadyGrantedVerbs(t *testing.T) {
	require.ElementsMatch(t,
		[]string{"tasks:get", "tasks:list", "tasks:update", "tasks/status:update", "pods:delete", "pods:get"},
		migrate.RequiredVerbs,
		"internal/migrate carries no kubebuilder:rbac markers and make rbac-check cannot see it; keep this list honest and cross-check charts/tatara-operator/templates/rbac.yaml by hand")
}
```

- [ ] **Step 4: Run and commit**

```bash
mise exec -- make test
mise exec -- make rbac-check
git add internal/migrate/ cmd/manager/main.go
git commit -m "feat: one-shot state migration, pre-mgr.Start, refuse-to-start on failure"
```

---

## Task 6.12: ContractVersion 4 and the comms edit

**Files:** `internal/agent/session.go:67`, `internal/agent/contract_test.go:109-118`, `internal/promptguidance/promptguidance.go:48-58`, `internal/controller/assignment.go:96`, `internal/controller/assignment_conciseness_test.go`.

- [ ] **Step 1: Write the failing tests**

```go
// internal/agent/contract_test.go
func TestContractVersionIsFour(t *testing.T) {
	if agent.ContractVersion != 4 {
		t.Fatalf("ContractVersion = %d, want 4", agent.ContractVersion)
	}
}
```

Also update `contract_test.go:53` (`require.Equal(t, 3, *info.ContractVersion)`) to 4.

```go
// internal/controller/assignment_conciseness_test.go
func TestConcisenessGuidance_AddressesAMatterExpert(t *testing.T) {
	require.Contains(t, promptguidance.ConcisenessGuidance, "matter expert")
	require.NotContains(t, promptguidance.ConcisenessGuidance, "clarify",
		"the clarify kind is deleted; a dead reference in guidance appended to EVERY turn-0 is a drift magnet")
	require.NotContains(t, promptguidance.ConcisenessGuidance, "discuss message")
}

func TestConcisenessGuidanceIsAppendedToEveryTurn0Unconditionally(t *testing.T) {
	for _, kind := range []string{"implement", "review", "brainstorm", "incident", "refine", "documentation"} {
		body := assignmentFor(taskOfKind(kind), proj, repo)
		require.Contains(t, body, promptguidance.ConcisenessGuidance,
			"%s turn-0 must carry the conciseness guidance", kind)
	}
}
```

- [ ] **Step 2: Implement**

`internal/agent/session.go:67`: `const ContractVersion = 4`. Extend its doc comment with the 3 -> 4 cause.

`internal/promptguidance/promptguidance.go`, `ConcisenessGuidance` (lines 48-58): **two edited lines only.**

1. Strengthen "senior dev/devops reader, not a beginner" to matter-expert phrasing: "You are writing for a matter expert in this system: the person who built it. Assume the architecture, the vocabulary and the failure modes are already known. Say what changed and why it is not obvious; skip what a reader could derive."
2. Delete the dead "clarify/discuss message" reference at line 53.

**Do NOT duplicate this into seven skills free to drift.** It is appended to every turn-0 assignment unconditionally at `internal/controller/assignment.go:96`, which is why it belongs in the operator.

- [ ] **Step 3: Run and commit**

---

## Task 6.13: the chart

**Files:** `charts/tatara-operator/Chart.yaml`, `charts/tatara-operator/templates/prometheusrule.yaml`, `charts/tatara-operator/dashboards/tatara-loop.json`, `charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml` (already regenerated).

- [ ] **Step 1: `kubeVersion`**

```yaml
apiVersion: v2
name: tatara-operator
description: Kubernetes operator orchestrating the tatara agentic-development loop (Project/Repository/Task/Issue/MergeRequest/QueuedEvent CRDs)
type: application
# 2.0.0: the #521 lifecycle and agent merge. BREAKING - Task.status.stage and
# status.stageReason are gone (status.state + status.parkReason replace them),
# the clarify agent kind is deleted, and TATARA_CONTRACT_VERSION is 4. The
# operator runs a one-shot state migration before mgr.Start() and REFUSES TO
# START if it cannot complete.
version: 2.0.0
appVersion: "0.5.0"
# The migration relies on status-subresource ratcheting during the window in
# which Helm has applied the narrowed CRD but the OLD operator Deployment is
# still running - it writes mirror-sync updates against a narrowed enum. That
# was fixed for exactly this release by kubernetes/kubernetes#129506. Helm
# refuses the upgrade below it rather than letting the operator crash-loop
# mysteriously. Cluster measured at server v1.33.0 on 2026-08-07.
kubeVersion: ">=1.33.0-0"
```

- [ ] **Step 2: `make chart-lint`'s metric allowlist**

`chart-lint` asserts that every metric named in the PrometheusRule exists. The rename of `operator_conversing_*` to `operator_live_*` and the new `operator_task_residency_exceeded_total` / `operator_task_parked_with_live_pod_repaired_total` must be reflected in BOTH the PrometheusRule and the Makefile's hardcoded metric list (the `for m in ...` loop). Add the two new metrics to the PrometheusRule with real alerts:

- `operator_task_residency_exceeded_total` increasing: warn. It means agents are hitting the absolute bound rather than finishing.
- `operator_task_parked_with_live_pod_repaired_total` increasing at a sustained rate: warn. The transient is not transient.

- [ ] **Step 3: Run the chart checks**

```bash
mise exec -- make chart-lint
mise exec -- helm template charts/tatara-operator | grep -c 'helm.sh/resource-policy: keep'   # must be 6
mise exec -- helm template charts/tatara-operator --kube-version 1.32.0 && echo "BUG: kubeVersion did not bind" || echo "kubeVersion binds"
```

- [ ] **Step 4: Update the Grafana dashboard**

`charts/tatara-operator/dashboards/tatara-loop.json` keys on `stage` labels. Update every panel to `state` and add a park-reason breakdown panel. `chart-lint` validates it parses as JSON; it does not validate the queries, so eyeball them.

- [ ] **Step 5: Note the downstream observability debt**

`tatara-observability`'s deployed alert rules key on the OLD stage vocabulary (`ROADMAP.md:39` already records this as open). Renaming `stage` to `state` breaks them. **This is genuinely out of scope for these five MRs** (it is a sixth repo) and is listed in the Open items section - it must be filed as an issue in the same session, not silently dropped.

---

## Task 6.14: the test migration, MEMORY, and final verification

**This is the bulk of the work and the plan treats it as such: 312 `StageParked` references across 51 test files, plus 12 `StageConversing`, 24 `StageClarifying` and 25 `StageFailed` in `stage_test.go` alone.**

**Ordered by size. One commit per file.**

| # | file | `StageParked` | envtest? |
|---|---|---|---|
| 1 | `internal/stage/stage_test.go` | 123 | no (pure) |
| 2 | `internal/controller/task_stage_test.go` | 30 | no (fake client) |
| 3 | `internal/controller/sweep_test.go` | 22 | no (fake client) |
| 4 | `internal/restapi/outcome_test.go` | 12 | no (fake client) |
| 5 | `internal/controller/reaper_test.go` | 7 | **yes** |
| 6 | `internal/webhook/pending_events_test.go` | 6 | no (the envtest variant is `pending_events_envtest_test.go`) |
| 7 | `internal/controller/unpark_backstop_test.go` | 6 | no |
| 8 | `internal/controller/parked_binding_repair_test.go` | 6 | no |
| 9 | `internal/controller/issue_apply_proposal_test.go` | 6 | no |
| 10 | `internal/controller/resume_deploy_test.go` | 5 | no - **DELETED in Task 6.9** |
| 11 | `internal/stage/conversing_test.go` | 4 (+24 `StageConversing`) | no - **rename to `live_test.go`** |
| 12 | `internal/controller/reaper_proposal_retention_test.go` | 4 | **yes** |
| 13 | `internal/controller/ownership_test.go` | 4 | **yes** |
| 14 | `internal/controller/mrbinding_backstop_test.go` | 4 | no |
| 15-51 | 37 more files at 1-3 refs each | 1-3 | mixed |

**Only 5 files in the whole repo genuinely need envtest for this change**, and none of them is new: `internal/controller/reaper_test.go`, `reaper_proposal_retention_test.go`, `ownership_test.go`, `intake_test.go`, plus the new `internal/migrate/migrate_envtest_test.go`. Everything else uses `fake.NewClientBuilder()` or is pure. **Do not convert a fake-client test to envtest to "be safe" - `make test` boots one shared control plane for the whole `controller` package and every added envtest test is real wall-clock on every CI run.**

- [ ] **Step 1: Get the test compiler's list**

```bash
mise exec -- go vet ./... 2>&1 | grep -v "^#" | sort -u > /tmp/521-test-sweep.txt
wc -l /tmp/521-test-sweep.txt
```

- [ ] **Step 2: Migrate, largest file first, one commit per file**

Three mechanical substitutions cover most of it:

```
Status.Stage:      StageParked, StageReason: X     ->  Status.State: <state>, ParkReason: X
Status.Stage:      StageFailed, StageReason: X     ->  Status.State: StateRejected, StateReason: X
Status.Stage:      StageDelivered                  ->  Status.State: StateDone
require .Status.Stage == StageParked               ->  require v1alpha1.Parked(tk)
```

Everything else is a judgement call per test, which is the point: 312 sites is not a `sed`, and a `sed` would produce tests that compile and assert nothing.

- [ ] **Step 3: Rename the test files whose subject changed**

```bash
git mv internal/stage/conversing_test.go internal/stage/live_test.go
git mv internal/stage/conversing_clock_test.go internal/stage/live_clock_test.go
git mv internal/stage/conversing_unpark_test.go internal/stage/live_unpark_test.go
git mv internal/controller/conversing_capacity_test.go internal/controller/livepods_capacity_test.go
git mv internal/controller/conversing_clock_test.go internal/controller/livepods_clock_test.go
git mv internal/controller/conversing_entry_test.go internal/controller/livepods_entry_test.go
git mv internal/controller/conversing_exit_test.go internal/controller/livepods_exit_test.go
git mv internal/controller/conversing_turn_test.go internal/controller/livepods_turn_test.go
git mv internal/controller/conversing_risk_register_test.go internal/controller/livepods_risk_register_test.go
```

- [ ] **Step 4: Prove the sweep is complete**

```bash
grep -rn "StageParked\|StageConversing\|StageClarifying\|StageFailed\|StageTriaging\|StageApproved\|StageImplementing\|StageReviewing\|StageMerging\|StageDeploying\|StageDelivered\|StageDocumenting\|StageBrainstorming\|StageInvestigating\|StageRefining\|StagePodless\|StageTerminal\|Status\.Stage\b\|StageReason\|AgentClarify\|MaxConversingPods" --include="*.go" .
```

Expected: **NOTHING**, in test files too. Any hit is an unmigrated site.

- [ ] **Step 5: MEMORY.md and ROADMAP.md**

`MEMORY.md`, dated lines (this is the file's whole purpose - a future reader must not have to re-derive any of this):

```
2026-08-XX (#521, the lifecycle and agent merge): status.stage/stageReason DELETED, not aliased - re-pointing StageParked would have let one site keep comparing against a value that can never match, and the deletion is what made the compiler enumerate all 102 production sites. TaskDone is now exactly {done, rejected}: TaskDone(parked)==true is the whole #521 bug, because it skipped intake.createTaskRaceSafe's live-twin branch (intake.go:311) so the Create 409ed and control reached the delete at :332, deleting a live Task and cascading its owned mirrors. resume.go deleted ENTIRELY (291 lines) rather than patched - patching resumeOne to check `created` leaves the sever at :150 running and produces a NEW split state (issue orphaned, Task alive).
2026-08-XX (#521, the parkReason wedge): parkReason is a stringly flag and a writer that sets State and forgets to clear it wedges a Task forever with a stale reason - the same silent-drift genre as #521. ONE mitigation: stage.Enter returns *StillParkedError on every non-park edge while parked, and stage.Unpark is the only function that clears the flag. UnparkTakeover is the ONE documented exception that clears the flag AND moves state, because a re-taken MR resumes at merged, not wherever the ownership flip caught it.
2026-08-XX (#521, the live-pod ceiling): the predicate is live(state) && parkReason == "". BOTH clauses are load-bearing and the failure mode is SILENT UNDER-COUNTING: merged -> awaiting-review is a legal edge, so a Task can re-enter a live state from an operator-driven one, and any predicate cleverer than these two clauses misses it and stops bounding pods entirely - a cost blowout with no error and no counter. Also fixed operator_conversing_entry_declined_total's reason label, which was the literal "unresolved" on all 27 declines in the live cluster - a counter that cannot name its condition is the defect the SweepSkip and UnparkDecline vocabularies exist to prevent.
2026-08-XX (#521, the residency bound): generalising liveness replaces a live state's WORK clock with an IDLE clock, so under-implementation lost the 6h absolute bound `implementing` had (old stage.go:721). ResidencyExceeded is a SEPARATE check in reconcileClocks, not a second return from ArmedClock - ArmedClock returns exactly one clock by construction and that is what makes the model auditable. It reads StateElapsedSeconds, so it is cumulative across a park round trip; a fresh cap per re-entry is the unbounded-loop shape #480 killed for merging.
2026-08-XX (#521, the 21st edge): the design's 20-edge table gave brainstorm, refine and incident Tasks no terminal path - they finish without opening an MR, so awaiting-review->done and deployed->done are both unreachable for them. Added refined -> done. One edge, three kinds.
2026-08-XX (#521, spec.kind): the migrator rewrites 61 Tasks' spec.kind from clarify to implement, which required ADDING implement to the Spec.Kind enum and to unconstrainedKinds - implement was never a valid ORIGIN kind. The design doc did not spell this out. And the real breakage was never the outcome kind-match gate at outcome.go:300: status.agentKind is EMPTY on all 118 live Tasks (every one is in a pod-less stage), so nothing could 409. It is that spec.kind=clarify has no state to route into out of `new`.
2026-08-XX (#521, the migration): 118 Tasks, measured 52/38/5/2/1/9/7/4, migrated in one synchronous pass before mgr.Start(). If it cannot complete the manager REFUSES TO START - a mixed population is the silent-drift shape that produced #521. Restart mid-migration is a non-event: idempotent per object. failed -> rejected is a DELIBERATE deviation scoped to the migration only: those four are past their useful life with retention clocks running, and resurrecting a merge-blocked Task re-arms work against stale MRs. The maintainer's failed -> new/refined rule governs FUTURE failures.
2026-08-XX (#521, ordering): the design doc had MR3 (skills) before MR4 (cli). That order FAILS CI - tatara-agent-skills' validate_tool_calls.py fetches the LATEST tatara-cli release manifest and hard-fails on an unknown enum literal, and the skills MR introduces action="approved"/"discuss"/"rejected". Same shape as that repo's own 2026-07-28 action="exhausted" entry. Swapped: MR4 releases first.
2026-08-XX (#521, NOT doing): no recency rule on the citation (an EARLIER approving comment must stay citable or ordinary threads deadlock - deliberately removed in July). No approver!=author rule (forge platforms ship it, but here the reporter IS the only maintainer on most issues; the bot-identity exclusion transfers, the human one does not). author_association DEFERRED (server-computed and unforgeable, genuinely stronger in kind, but it defends against maintainerLogins drift which cannot occur in a one-name list; costs a new Comment field plus mirror/webhook plumbing plus a CRD bump - revisit at a second maintainer). SELECTIVE QUOTING REMAINS OPEN: "go ahead" is a substring of "do not go ahead until CI is green". Plan-pinning closes approve-then-swap-scope, a DIFFERENT attack. Mitigation is detection, not prevention: the operator posts a confirmation comment naming the approver and quoting what was cited.
```

`ROADMAP.md`: mark the #521 item shipped with the chart version; add the `tatara-observability` alert-vocabulary migration as a new open item.

- [ ] **Step 6: FULL VERIFICATION**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-operator
mise exec -- make generate
mise exec -- make manifests
git diff --exit-code api/v1alpha1/zz_generated.deepcopy.go charts/tatara-operator/crd-bases/ \
  && echo "generated artefacts are committed and current" \
  || { echo "FAIL: regenerate and commit"; exit 1; }
mise exec -- make lint
mise exec -- make test
mise exec -- make rbac-check
mise exec -- make chart-lint
mise exec -- pre-commit run --all-files
```

All six must pass. `make ci` runs `generate manifests lint test rbac-check chart-lint` in one go and is the single command CI uses; run it last as the gate.

---

# MR7 - tatara-helmfile: move the pins, ONE commit

**Repo:** `tatara-helmfile`. **Semver:** n/a. **Lands in the same maintenance slot as MR6.**

## Files

- Modify: `values/project-tatara/common.yaml` (lines 40, 53)
- Modify: `values/project-infrastructure/common.yaml` (lines 36, 46)
- Modify: `values/project-mtg/common.yaml` (lines 29, 36)

The operator chart/image pins in `helmfile.yaml.gotmpl:85,104,116,132` and `values/tatara-operator/common.yaml:10` are moved by MR6's OWN CD pipeline (`release.yml`'s `bump` job calls `cd-release@main` in `mode: bump`, which rewrites five pins atomically in one auto-merged PR). **Do not hand-edit them here** - the hard rule is "never hand-edit a deploy pin".

- [ ] **Step 1: Wait for MR6's CD to publish**

```bash
gh -R szymonrychu/tatara-operator release view --json tagName,publishedAt
```

The operator's `bump` PR into `tatara-helmfile` must be merged and applied BEFORE this MR merges, or the Projects get a contract-4 wrapper against a contract-3 operator - the same mismatch in the other direction.

- [ ] **Step 2: Move both pins in every Project, in one commit**

```bash
cd /Users/szymonri/Documents/tatara-new/code/tatara-helmfile
WRAPPER=$(gh -R szymonrychu/tatara-claude-code-wrapper release view --json tagName -q .tagName)
SKILLS=$(gh -R szymonrychu/tatara-agent-skills release view --json tagName -q .tagName)
echo "wrapper=$WRAPPER skills=$SKILLS"
for f in values/project-tatara/common.yaml values/project-infrastructure/common.yaml values/project-mtg/common.yaml; do
  mise exec -- yq -i ".project.spec.agent.image = \"harbor.szymonrichert.pl/containers/tatara-claude-code-wrapper:$WRAPPER\"" "$f"
  mise exec -- yq -i ".project.spec.agent.skillsRef = \"$SKILLS\"" "$f"
done
git diff
```

- [ ] **Step 3: Verify the guard from MR2 still passes**

```bash
mise exec -- python3 .github/scripts/check_pin_coverage.py
mise exec -- python3 -m pytest .github -q
```

- [ ] **Step 4: Diff the blast radius before merging**

```bash
mise exec -- helmfile -e default diff --detailed-exitcode --suppress-secrets
```

Expected: exactly six changed values across three `tatara-project` releases, and NOTHING else. If the operator chart version also moves in this diff, MR6's `bump` PR has not landed - **STOP and wait for it.**

- [ ] **Step 5: Commit**

```bash
git add values/
git commit -m "cd: move every Project to the contract-4 wrapper and the folded skill set

ONE commit, both pins, all three Projects. MR6 and this are a single operation
split across two repos: between the operator landing and this applying, every
agent pod fails AssertContractVersion at pod-ready - loud, pre-work, zero tokens."
```

- [ ] **Step 6: Watch the apply and confirm the window closed**

```bash
kubectl -n tatara get pods -l app.kubernetes.io/component=agent
kubectl -n tatara get tasks -o custom-columns=NAME:.metadata.name,STATE:.status.state,PARK:.status.parkReason | sort -k2
promtool query instant "$PROM" 'sum(increase(operator_agent_contract_mismatch_total[30m]))'
```

Expected: the mismatch counter stops increasing; Tasks show `state` values from the 8-value enum and no empty `state`.

---

# Post-landing verification (all five repos)

- [ ] **All 118 Tasks migrated, none left stage-shaped**

```bash
kubectl get tasks -A -o json | jq -r '
  [.items[] | {state: (.status.state // "MISSING"), park: (.status.parkReason // "")}]
  | group_by(.state + "/" + .park) | map({k: .[0].state + "/" + .[0].park, n: length}) | .[]'
kubectl get tasks -A -o json | jq '[.items[] | select(.status.state == null)] | length'
```

The second command MUST print `0`. Expected first-command distribution, from the mapping table:
`new/backlog-sweep=52`, `new/awaiting-human=23`, `awaiting-review/awaiting-human=15`,
`new/no-outcome=5`, `awaiting-review/review-loop-exhausted=2`,
`under-implementation/ownership-lost=1`, `rejected/=13`, `done/=7`. Total 118.

- [ ] **The audit trail is present**

```bash
kubectl get tasks -A -o json | jq -r '[.items[] | select(.metadata.annotations["tatara.dev/migrated-from"] == null)] | length'
```

MUST print `0`.

- [ ] **No spec.kind=clarify survives**

```bash
kubectl get tasks -A -o json | jq '[.items[] | select(.spec.kind == "clarify")] | length'
```

MUST print `0`. It was 61 before.

- [ ] **The gate works end to end**

Comment a go-ahead on a live issue, watch the Task reach `under-implementation`, and confirm the operator posted its confirmation comment naming the approver and the quote.

---

# Open questions I could not resolve from the code

These are real and the implementer must resolve them before or during the task that hits them. None is a placeholder - each names the decision and the evidence available.

1. **What is `new`'s budget, and does an un-parked `new` Task run a pod?** The old `triaging` stage had a 5-minute budget and was pod-less; the old `parked(backlog-sweep)` had NO clock at all (`ArmedClock`'s one exemption, `stage.go:827-829`). Under the merged model `new` carries both populations - 52 backlog-sweep owners and every freshly-minted Task. The exemption must survive (`parkReason == backlog-sweep` disarms every clock), but whether `new` itself gets the 5-minute triage budget or something longer is not derivable from the design doc. **Recommendation: keep 5 minutes and keep the backlog-sweep exemption keyed on `parkReason`, exactly as today.** Verify against `TestEveryStateHasABudget`.

2. **Does `Spec.InitialStage` become `Spec.InitialState`, and what are its legal values?** It is in the IMMUTABLE spec (`task_types.go:174`) so the create-edge derives the state with no post-create status write racing the reconciler (fix C5). The 52 backlog-sweep Tasks carry `InitialStage: parked` + `InitialStageReason: backlog-sweep` today - but `parked` is no longer a state. **The migrator must rewrite `Spec.InitialStage` too, which the design doc does not mention.** Recommendation: `InitialState: new` + a new `Spec.InitialParkReason: backlog-sweep`. Confirm by reading how `TaskReconciler`'s create-edge consumes them.

3. **How does `planNoteId` survive the 50-note spill?** Notes are capped at 50 with drop-oldest and spilled to `tatara-memory` (`handlers_v2.go:378-398`). A long-running Task can spill the plan note the gate pinned, at which point `ApprovalRefusedPlanNoteMissing` fires on a legitimate submit. **Recommendation: exempt the pinned plan note from the spill.** That needs a `Task.status.pinnedPlanNoteID` or an equivalent, and it is a real CRD addition. Decide in Task 6.8; do not discover it in production.

4. **What is `awaiting-review`'s agent kind for a `documentation` Task?** `AgentKindFor(StateAwaitingReview, _)` returns `AgentReview` unconditionally in the design above. That is right for implement and documentation MRs, but a `kind=review` Task in `awaiting-review` is reviewing a HUMAN's PR - same agent kind, different semantics, and `LegalFor`'s GUARD 1 must keep refusing `under-implementation` and `merged` for it. Confirm GUARD 1 survives the rename with a test named `TestLegalFor_KindReviewCanNeverReachUnderImplementationOrMerged`.

5. **Does the confirmation comment (the selective-quoting mitigation) risk the forty-comment loop?** It posts once per grant, and a grant is once per Task, so no. But it is a BOT comment on the issue-of-record and the merged agent is long-lived - confirm the enqueue filter still drops it (`TaskEvent`'s doc: a bot-authored event is never enqueued) so the operator's own confirmation cannot wake the Task that produced it.

6. **The exact 7d force-delete FRACTION.** P0.3.2's gate needs the denominator and the 2026-08-07 reading did not capture it. Prometheus retention here is ~7-8 days, so there is no 30-day baseline to fall back on. **This gate is UNRESOLVED and it decides whether Tasks 6.5 and 6.7 ship in MR6 at all.**

7. **`tatara-observability`'s alert rules key on the old stage vocabulary.** A sixth repo, genuinely out of scope for these five MRs, already recorded as open at `tatara-operator/ROADMAP.md:39`. It must be filed as an issue in the same session, not silently dropped: after MR6 the deployed alerts key on labels that no longer exist, so the platform ships with its own alerting blind.
