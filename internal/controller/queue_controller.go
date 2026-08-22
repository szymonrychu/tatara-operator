package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// isQueued reports whether a QueuedEvent state is effectively Queued.
// State=="" handles a QE whose Status().Update failed after Create (stranded ghost).
func isQueued(state string) bool {
	return state == tatarav1alpha1.QueueStateQueued || state == ""
}

type DispatcherReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Metrics *obs.OperatorMetrics
	// BudgetDefaults is the operator-wide token-budget config (issue #189). Each
	// Project layers its spec.tokenBudget over this. The zero value is disabled,
	// so the admission gate is inert until configured.
	BudgetDefaults budget.Config
	// Usage is the fleet-wide Claude account usage snapshot (claudeSubscription
	// mode). Nil-safe: a nil store yields an empty snapshot (nothing per-kind held).
	Usage *accountusage.Store
	// UsagePollInterval is the account-usage poll cadence (claudeSubscription
	// mode). It is the requeue delay after a per-kind/pool account-usage hold so
	// held work re-evaluates once the next poll refreshes the store; a zero value
	// falls back to 60s (see usageRequeueAfter).
	UsagePollInterval time.Duration
	// APIReader is an UNCACHED read path used only for the admission inflight
	// count (contract B.7 fix M28): the single-writer dispatcher's own Admitted
	// QueuedEvents from reconcile N might not be in r.Client's informer cache
	// yet at reconcile N+1, which could transiently over-admit a burst by the
	// size of one admit pass. Reading inflight counts straight from the API
	// server removes that exposure. SetupWithManager wires this to
	// mgr.GetAPIReader() when unset; nil (unit tests with no manager) falls
	// back to the qes/tasks slices the caller already listed.
	APIReader client.Reader
	// SpillerFor resolves the A.7 byte-budget spiller for a Project. The
	// dispatcher needs one for exactly ONE path: an adopted dependency-upgrade
	// mint, whose bindMRToTask CREATES the merge-request mirror and therefore
	// goes through the objbudget guard. Nil is safe (unit tests, and every
	// non-adoption admission, which writes no mirror).
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
	// BackstopEvents is the leader-only admission backstop's output channel
	// (issue #395). DispatcherReconciler is otherwise purely watch-driven, so a
	// QueuedEvent left Queued across a rollout/leader-handoff window with no
	// fresh watch trigger can stall admission indefinitely. When non-nil,
	// SetupWithManager wires it into the controller via
	// WatchesRawSource(source.Channel), and EnqueuePending/RunBackstop push a
	// GenericEvent per pending QueuedEvent onto it on a fixed schedule,
	// independent of any watch event. A nil channel (every pre-existing unit
	// test in this file) makes EnqueuePending/RunBackstop a no-op.
	BackstopEvents chan event.GenericEvent
}

// RBAC note (Task A10, no-op): the accountusage poller/mirror (cmd/manager)
// needs get on the OAuth Secret and get;create;update on ConfigMaps in the
// operator namespace. No new +kubebuilder:rbac markers are added here because
// access is already granted: project_controller.go already carries a
// namespaced "" /secrets marker (get;list;watch;create;update;patch;delete)
// and repository_controller.go already carries a namespaced "" /configmaps
// marker (get;list;watch;create;update;patch;delete); both are coalesced into
// the SAME namespaced Role in charts/tatara-operator/templates/rbac.yaml,
// scoped to the operator's own namespace (Release.Namespace). That Role is
// the hand-maintained source of truth the chart renders (see
// hack/check-rbac-drift.sh); `make rbac-check` confirms the +kubebuilder
// markers still mirror it with zero drift, so no chart or marker change is
// required for the poller/mirror to read the OAuth Secret and manage the
// account-usage ConfigMap.

// ceilingKey resolves which SpawnCeilingByKind entry governs a queued event,
// mirroring modelForKind/effortForKind precedence (internal/agent/pod.go): a
// non-empty LabelActivity value that is itself a configured ceiling key wins, so
// healthCheck work (enqueued as Kind=brainstorm + activity=healthCheck) is
// governed by the healthCheck ceiling rather than brainstorm's. Otherwise the
// event's Spec.Kind is used.
func ceilingKey(cfg budget.Config, q *tatarav1alpha1.QueuedEvent) string {
	if act := q.Spec.Payload.Labels[tatarav1alpha1.LabelActivity]; act != "" {
		if _, ok := cfg.SpawnCeilingByKind[act]; ok {
			return act
		}
	}
	return q.Spec.Kind
}

func taskByName(tasks []tatarav1alpha1.Task, name string) *tatarav1alpha1.Task {
	for i := range tasks {
		if tasks[i].Name == name {
			return &tasks[i]
		}
	}
	return nil
}

// starvationBudget is the age past which a Queued priority-2 event reserves
// itself a normal-pool slot regardless of how much priority 0/1 work is
// queued ahead of it (contract B.7 fix M21). Without it a busy project can
// starve priority-2 work (the nightly doc batch, the refine groom) forever.
const starvationBudget = time.Hour

// effectivePriority is tatarav1alpha1.EffectiveQueuePriority, spelled out at
// the call sites below via the exported helper; kept as a one-line wrapper
// so the sort/reserve logic below reads at the same altitude.
func effectivePriority(q *tatarav1alpha1.QueuedEvent) int {
	return tatarav1alpha1.EffectiveQueuePriority(q.Spec)
}

// queueOrderBefore reports whether a is admitted ahead of b: (effective
// priority, seq) ascending, so FIFO is preserved WITHIN a priority. Shared by
// admitPool's sort and the backstop's per-pool head pick (pendingPoolHeads) so
// both agree on which event is the head of a pool.
func queueOrderBefore(a, b *tatarav1alpha1.QueuedEvent) bool {
	pa, pb := effectivePriority(a), effectivePriority(b)
	if pa != pb {
		return pa < pb
	}
	return a.Spec.Seq < b.Spec.Seq
}

// hasStarvingPriority2 reports whether queued (already filtered to one
// class, Queued state) contains a priority-2 event that has waited longer
// than starvationBudget.
func hasStarvingPriority2(queued []*tatarav1alpha1.QueuedEvent, now time.Time) bool {
	for _, q := range queued {
		if effectivePriority(q) == 2 && now.Sub(q.CreationTimestamp.Time) > starvationBudget {
			return true
		}
	}
	return false
}

// queuedEventIndexDedupKey is a controller-local alias for queue.DedupKeyIndex
// (contract B.7 addendum 7): DispatcherReconciler owns the QueuedEvent
// controller so it registers this index; task_controller.go's
// registerFieldIndexes owns the Task/Repository indexes.
const queuedEventIndexDedupKey = queue.DedupKeyIndex

// registerFieldIndexes registers the QueuedEvent.spec.dedupKey field index
// (contract B.7 addendum 7, item 3: the AUTHORITATIVE in-flight QueuedEvent
// dedup lookup, never a label selector). Called from SetupWithManager.
func (r *DispatcherReconciler) registerFieldIndexes(idx client.FieldIndexer) error {
	return idx.IndexField(context.Background(), &tatarav1alpha1.QueuedEvent{}, queuedEventIndexDedupKey, queue.DedupKeyIndexer)
}

// listProjectVia lists QueuedEvents and Tasks scoped to proj.Namespace/
// proj.Name via the given reader. Reconcile passes r.Client (the manager's
// cached client); admit's inflight recount passes r.APIReader (uncached,
// contract B.7 fix M28) when one is wired.
func (r *DispatcherReconciler) listProjectVia(ctx context.Context, reader client.Reader, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.QueuedEvent, []tatarav1alpha1.Task, error) {
	var qel tatarav1alpha1.QueuedEventList
	if err := reader.List(ctx, &qel, client.InNamespace(proj.Namespace)); err != nil {
		return nil, nil, err
	}
	var tl tatarav1alpha1.TaskList
	if err := reader.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return nil, nil, err
	}
	return filterQEsByProject(qel.Items, proj.Name), filterTasksByProject(tl.Items, proj.Name), nil
}

// mintedTask returns the Task q has ALREADY minted, or nil when it has minted
// none. It is the mint-idempotency primitive that replaces the AlreadyExists
// collision a deterministic Task name used to give the admit path for free
// (issue #443).
//
// The link is queue.LabelMintedBy - the minting event's UID, stamped by
// BuildTaskFromQueuedEvent and never rewritten. LabelQueuedEvent is not usable
// for this: admitTicket moves it onto the CURRENT per-stage ticket as soon as
// the Task takes its first stage step, and its value (the event NAME) repeats
// across cycles of the same dedup key.
//
// The read goes through APIReader when one is wired, for the same reason the
// inflight recount does (fix M28): a Task this dispatcher created moments ago
// may not be in the informer cache yet, and a cache miss here mints a duplicate.
func (r *DispatcherReconciler) mintedTask(ctx context.Context, q *tatarav1alpha1.QueuedEvent) (*tatarav1alpha1.Task, error) {
	if q.UID == "" {
		return nil, nil
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var tl tatarav1alpha1.TaskList
	if err := reader.List(ctx, &tl, client.InNamespace(q.Namespace),
		client.MatchingLabels{queue.LabelMintedBy: string(q.UID)}); err != nil {
		return nil, fmt.Errorf("queue: list tasks minted by %s: %w", q.Name, err)
	}
	var found *tatarav1alpha1.Task
	for i := range tl.Items {
		// Oldest wins, so two passes racing on the same event agree on one Task.
		if t := &tl.Items[i]; found == nil || t.CreationTimestamp.Before(&found.CreationTimestamp) {
			found = t
		}
	}
	return found, nil
}

// queueTaskDone reports whether a Task's work is over. It is exactly
// TaskDone - {done, rejected} - and NOT parked: #521 stopped folding parked in,
// and a parked Task keeps its QueuedEvent because it re-acquires a slot when it
// un-parks.
func queueTaskDone(t *tatarav1alpha1.Task) bool {
	return tatarav1alpha1.TaskDone(t)
}

// queueTaskHoldsSlot reports whether a Task still occupies a pod slot.
//
// THREE exclusions, and the third is new with #521. An OPERATOR-DRIVEN state
// (merged/deployed) runs no agent, and neither does `new` (operator triage,
// which leaves immediately) - counting them re-creates the lane-starvation trap
// (operator-laneoccupancy-starves-recovery-2026-06-15). And a PARKED Task runs
// no pod at all: park is what takes the pod down. It is NOT treated as done,
// though - it re-acquires a slot when it un-parks - so its QueuedEvent is kept,
// not GC'd.
func queueTaskHoldsSlot(t *tatarav1alpha1.Task) bool {
	if queueTaskDone(t) || tatarav1alpha1.Parked(t) {
		return false
	}
	// A Task whose CREATE EDGE HAS NOT RUN YET (state == "") is mid-mint and
	// will land on a pod state within one reconcile. It holds the lane its own
	// mint admission reserved, and dropping it over-admits the pool by exactly
	// the number of in-flight mints - the shape that saturated the pool before
	// the queue existed. The old predicate got this free from
	// StagePodless(""), which was false; stage.Live("") is false, so it has to
	// be explicit.
	if t.Status.State == "" {
		return true
	}
	return stage.Live(t.Status.State)
}

// ticketSpent reports whether an Admitted QueuedEvent's pod slot is spent
// because the Task has LEFT the stage the ticket was cut for (contract B.7:
// "reconcileDone GC-deletes it once the pod is gone AND the Task has left the
// stage that requested it"). Without this, a Task advancing implementing ->
// reviewing would hold its implement ticket forever AND take a second slot for
// its review ticket.
//
// A MINT payload (no agentKind: the legacy flat event or the B.7 blueprint)
// reserved a lane only to bring the Task into existence. Once the Task leaves
// the create -> triaging bootstrap and cuts its OWN per-stage pod-ticket, that
// ticket is the accurate slot accounting and the mint MUST release. Without
// this an incident's alert-class create event stays inflight and deadlocks
// against its own alert-class pod-ticket at AlertCapacity=1: the Task holds the
// single alert slot its investigating pod needs, so the pod never spawns and
// the slot never frees (2026-07-13, found on first live incident post-cutover).
func ticketSpent(q *tatarav1alpha1.QueuedEvent, t *tatarav1alpha1.Task) bool {
	want := q.Spec.Payload.AgentKind
	if want == "" {
		switch t.Status.State {
		case "", tatarav1alpha1.StateNew:
			return false // still bootstrapping; the mint holds the lane
		default:
			return true // in the state machine; the per-state ticket accounts for the slot
		}
	}
	if cur := stage.AgentKindFor(t.Status.State, t.Spec.Kind); cur != "" {
		return cur != want
	}
	// The state runs no agent. `new` is PRE-pod (the ticket it enqueues is for
	// the NEXT state's pod) and an unstamped state is a Task the machine has not
	// touched yet. merged/deployed/done/rejected are all POST-pod: the ticket is
	// spent.
	switch t.Status.State {
	case "", tatarav1alpha1.StateNew:
		return false
	default:
		return true
	}
}

// poolInflight counts Admitted QueuedEvents of class whose Task still holds a
// pod slot, plus live Tasks created before the queue existed (no
// LabelQueuedEvent label, no stage) so capacity is not over-admitted at cutover.
func (r *DispatcherReconciler) poolInflight(qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task, class string) int {
	n := 0
	for i := range qes {
		q := &qes[i]
		if q.Spec.Class != class || q.Status.State != tatarav1alpha1.QueueStateAdmitted {
			continue
		}
		t := taskByName(tasks, q.Status.TaskRef)
		if t != nil && queueTaskHoldsSlot(t) && !ticketSpent(q, t) {
			n++
		}
	}
	return n
}

// patchQueuedEventStatus Get-fresh + mutate + Status().Update's a QueuedEvent's
// status, retrying on conflict. Mirrors patchTaskStatus/repository_controller.go's
// patchStatus (issue #348): every reconcile of ONE QueuedEvent walks and admits
// the WHOLE project pool, so a single Reconcile call can write Status().Update on
// several QueuedEvents that were never its own req.NamespacedName - and the next
// QueuedEvent's own reconcile, moments later, lists that same pool off a cache
// that may not have caught up yet. That self-conflict (not a different writer)
// was the entire source of the queuedevents.tatara.dev 409 storm: every
// queue_admit -> queue_done cycle logged one, purely from the raw
// r.Status().Update this replaces.
func patchQueuedEventStatus(ctx context.Context, c client.Client, q *tatarav1alpha1.QueuedEvent, mutate func(*tatarav1alpha1.QueuedEvent) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.QueuedEvent{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(q), fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			*q = *fresh
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*q = *fresh
		return nil
	})
}

// admit drains the alert pool to AlertCapacity, then the normal pool to
// QueueCapacity, each in strict ascending seq order (pure per-project FIFO within a
// pool; head-of-line blocking accepted). Wired in Task 8 Reconcile.
// Returns requeue=true when a stale terminal Task was deleted so Reconcile can
// signal a prompt re-attempt via ctrl.Result{RequeueAfter: time.Second}.
// heldOnLiveCeiling is set when a MINT was held by the per-project live-pod
// ceiling (#570): like a pool_full hold it leaves the event Queued and burns no
// slot, and the caller folds it into the same waiting/blockedPoll backstop.
func (r *DispatcherReconciler) admit(ctx context.Context, proj *tatarav1alpha1.Project,
	qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task,
	d budget.Decision, cfg budget.Config, sub budget.Subscription, now time.Time) (requeue bool, heldOnUsage bool, heldOnLiveCeiling bool, err error) {

	// Full project pause (contract B.7: maxConcurrentAgents=0 is the kill
	// switch and must create NO agent work at all, not fall back to
	// QueueCapacity's hard floor of 3 - QueueCapacity is NEVER used to
	// implement this, see its doc comment). This is the sole chokepoint where
	// a QueuedEvent becomes a Task (both normal and alert pools, i.e. every
	// pod-spawn), so gating here fully holds the project: no scan/lifecycle/
	// brainstorm/incident Task is ever created while paused. The >0
	// concurrency-cap semantics below are unchanged.
	if proj.Spec.MaxConcurrentAgents == 0 {
		for i := range qes {
			q := &qes[i]
			if !isQueued(q.Status.State) {
				continue
			}
			if r.Metrics != nil {
				r.Metrics.AdmissionBlocked(proj.Name, q.Spec.Class, "", "project_paused")
			}
			log.FromContext(ctx).Info("queue: admission held, project paused",
				"action", "project_paused_skip", "project", proj.Name, "repo", q.Spec.RepositoryRef,
				"resource_id", q.Name, "class", q.Spec.Class, "kind", q.Spec.Kind, "reason", "max_concurrent_agents_zero")
		}
		return false, false, false, nil
	}

	// Uncached inflight recount (contract B.7 fix M28): when an APIReader is
	// wired (production, via SetupWithManager), the capacity check below reads
	// straight from the API server instead of trusting the qes/tasks slices
	// Reconcile listed through the cache, removing the burst-over-admission
	// exposure. Falls back to the caller-supplied slices when APIReader is nil
	// (unit tests with no manager).
	inflightQEs, inflightTasks := qes, tasks
	if r.APIReader != nil {
		var lerr error
		inflightQEs, inflightTasks, lerr = r.listProjectVia(ctx, r.APIReader, proj)
		if lerr != nil {
			return false, false, false, lerr
		}
	}

	// claudeSubscription mode decides the account-usage hold PER EVENT (below):
	// the pool-class Decision is fleet-percent-derived and would freeze whole
	// pools at the coarse proactive/emergency thresholds, pre-empting the per-kind
	// spawn ceilings that are the point of subscription mode. customWindow (and
	// disabled) mode keeps the wholesale per-pool short-circuit unchanged.
	subscription := cfg.Enabled && cfg.Mode == budget.ModeClaudeSubscription

	// THE LIVE-POD CEILING, ON THE CREATION SIDE (#570). mintBudget is how many
	// NEW live conversations this project may still create; it is HOISTED (one
	// answer for the whole pass, both pools) and SPENT as this pass actually
	// mints, the same budget-not-boolean discipline driveUnparks' liveRoomBudget
	// uses and for the same reason: a boolean computed once and reused for every
	// event in a burst bounds nothing.
	//
	// It is computed LAZILY, on the first event that would actually Create a
	// Task, so no comment, ticket, or already-minted adoption pays for
	// liveMintBudget's List. -1 means "not computed yet".
	//
	// ONE budget across BOTH pools is what keeps the alert pool's priority
	// intact under the new gate: admitPool runs alert before normal and spends
	// from the same number, so when a project has exactly one live slot left an
	// incident takes it and proactive work waits - the same ordering the pool
	// capacities already encode, now also true of the live ceiling.
	mintBudget := -1
	liveRoomFor := func(ctx context.Context) (int, error) {
		if mintBudget >= 0 {
			return mintBudget, nil
		}
		reader := client.Reader(r.Client)
		if r.APIReader != nil {
			reader = r.APIReader
		}
		b, err := liveMintBudget(ctx, r.Client, reader, proj, now)
		if err != nil {
			return 0, err
		}
		mintBudget = b
		return mintBudget, nil
	}

	admitPool := func(class string, cap int) error {
		// One line per pool per pass, not one per held event: a project sitting
		// at its ceiling with a deep queue would otherwise reproduce the #496
		// log storm the pool_full path was fixed for.
		ceilingLogged := false
		queued := make([]*tatarav1alpha1.QueuedEvent, 0)
		for i := range qes {
			if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
				queued = append(queued, &qes[i])
			}
		}
		// Token-budget gate (issue #189): hold this pool's work when window usage
		// has reached the pool's threshold. The normal pool (proactive/reactive
		// work) pauses at the proactive threshold; the alert pool (incidents) only
		// at the higher emergency threshold, so incidents keep flowing while
		// proactive work is paused. A disabled budget yields the zero Decision, so
		// neither flag is set and admission is unchanged.
		poolBlocked := (class == tatarav1alpha1.QueueClassNormal && d.ProactiveBlocked) ||
			(class == tatarav1alpha1.QueueClassAlert && d.EmergencyBlocked)
		if !subscription && poolBlocked {
			if len(queued) > 0 {
				if r.Metrics != nil {
					r.Metrics.AdmissionBlocked(proj.Name, class, "", "token_budget")
				}
				log.FromContext(ctx).Info("queue: admission held by token budget",
					"action", "admission_blocked", "class", class, "reason", "token_budget",
					"used_percent", d.UsedPercent, "queued", len(queued))
			}
			return nil
		}
		inflight := r.poolInflight(inflightQEs, inflightTasks, class)
		// Ordering is (priority, seq) ascending (contract B.7 fix B3): FIFO is
		// preserved WITHIN a priority. A webhook-originated event (priority 1,
		// a human waiting on a thread) is admitted ahead of cron/sweep work
		// (priority 2) queued earlier; incidents (priority 0) also sort first
		// here, though the alert pool above already reserves their capacity.
		sort.Slice(queued, func(i, j int) bool { return queueOrderBefore(queued[i], queued[j]) })
		// Priority-2 starvation reservation (contract B.7 fix M21): the normal
		// pool reserves at least one slot for priority 2 once a priority-2
		// event has waited > starvationBudget, so priority 0/1 work cannot
		// crowd it out forever (the nightly doc batch, the refine groom).
		reserve := 0
		if class == tatarav1alpha1.QueueClassNormal && hasStarvingPriority2(queued, now) {
			reserve = 1
		}
		for _, q := range queued {
			if inflight >= cap {
				// Issue #440: the pool is full with work still Queued. Every sibling
				// refusal path (project_paused, token_budget, kind_ceiling) emits;
				// this one used to break silently, so a saturated pool was
				// indistinguishable from an empty one. Reached only from inside the
				// loop, so queued is non-empty and this fires at most once per pool
				// per pass. Admission behaviour is unchanged.
				//
				// Since #496 removed the per-event self-poll, this counts ADMISSION
				// PASSES that found the pool full, not wasted retries: its rate is now
				// a measure of how often admission is attempted against a saturated
				// pool, and no longer scales with queue depth. HOW saturated, and for
				// how long, is carried by operator_queue_depth / operator_queue_inflight
				// / operator_queue_age_seconds{state="Queued"} - gauges of the actual
				// condition, which is what the alert rule should key on.
				if r.Metrics != nil {
					r.Metrics.AdmissionBlocked(proj.Name, class, "", "pool_full")
				}
				log.FromContext(ctx).Info("queue: admission held, pool at capacity",
					"action", "admission_blocked", "project", proj.Name, "class", class,
					"reason", "pool_full", "cap", cap, "inflight", inflight, "queued", len(queued))
				break
			}
			if reserve > 0 && effectivePriority(q) != 2 && inflight >= cap-reserve {
				continue // slot reserved for a starving priority-2 event later in this list
			}
			// claudeSubscription account-usage hold, decided per event: a kind with
			// a configured spawn ceiling is governed ONLY by that ceiling (so
			// incident:98 admits until ~98% while brainstorm:40 is already held); a
			// kind without one falls through to the pool-class proactive/emergency
			// decision. No-op in customWindow mode (handled by the short-circuit
			// above), so subscription==false leaves admission unchanged.
			if subscription {
				key := ceilingKey(cfg, q)
				held := poolBlocked
				reason := "token_budget"
				if _, hasCeiling := cfg.SpawnCeilingByKind[key]; hasCeiling {
					held = budget.KindBlocked(cfg, sub, key, now)
					reason = "kind_ceiling"
				}
				// refine is a scan-pipeline BARRIER (projectscan.go runScans defers
				// mrScan/issueScan/brainstorm/healthCheck until a refine Task reaches
				// a terminal state). A refine event held Queued here never runs,
				// never becomes terminal, and wedges that barrier - and every scan
				// behind it - forever. So refine always admits, regardless of any
				// configured spawnCeilingByKind["refine"].
				if key == "refine" {
					held = false
				}
				if held {
					heldOnUsage = true
					if r.Metrics != nil {
						r.Metrics.AdmissionBlocked(proj.Name, class, q.Spec.Kind, reason)
					}
					log.FromContext(ctx).Info("queue: admission held by account usage",
						"action", "admission_blocked", "class", class, "kind", q.Spec.Kind,
						"reason", reason, "used_percent", d.UsedPercent)
					continue // leave Queued; requeued to re-evaluate after next poll
				}
			}
			// Two payload shapes reach this chokepoint (contract B.7). A TICKET
			// (payload.taskRef) admits an EXISTING Task's pod and mints nothing - in
			// the task-centric model the Task already exists by the time it reaches a
			// pod stage. Everything else MINTS: the B.7 blueprint (payload.newTask)
			// and the legacy flat payload the sweep and the webhook still produce.
			var taskName string
			if queue.IsAdmissionTicket(q) {
				name, verdict, err := r.admitTicket(ctx, q, now)
				if err != nil {
					return err
				}
				switch verdict {
				case ticketDropped:
					continue // the QE is gone; no slot burned
				case ticketDeferred:
					requeue = true
					continue // still Queued; no slot burned
				}
				taskName = name
			} else {
				// MINT IDEMPOTENCY (issue #443). Ask FIRST whether this event has
				// already minted its Task, and adopt it if so. Do NOT delete this
				// guard as redundant: a mint is GenerateName-named now, so Create
				// can never collide and AlreadyExists is no longer the idempotency
				// primitive it was. A failure anywhere between the Create below and
				// the Admitted status patch further down leaves the event Queued,
				// and without this lookup the next pass would mint a SECOND Task -
				// a second pod, a second branch, a second token spend.
				existing, err := r.mintedTask(ctx, q)
				if err != nil {
					return err
				}
				if existing != nil && queue.IsAdoptedUpgradeMint(q) {
					// THE ONLY CONVERGENCE PATH A QUEUED ADOPTION HAS, and it has to be
					// here rather than inside the mint branch below. An adopted mint is
					// FOUR writes and only the Task create carries the mint stamp
					// atomically; if the bind or the semver-floor seed then fails, the
					// Task persists and the lookup above finds it on every later pass -
					// so the mint branch, and with it convergeAdoptedMint's
					// MintExistingLive arm, is never entered again. The
					// mr-binding backstop does not cover the gap either: it re-binds the
					// mirror and never touches the floor, so a mint that died at
					// seedAdoptedSemverFloor would merge with an empty significance, cut
					// no tag, and sit in `deploying` until its budget parks it. That is
					// exactly the wedge AdoptedSignificanceFloor exists to close.
					if cerr := r.convergeAdoptedAdmission(ctx, proj, q, existing); cerr != nil {
						return cerr
					}
				}
				if existing == nil {
					// THE GATE, and it sits HERE rather than at the top of the
					// mint branch on purpose: an event whose Task already exists
					// (the #443 adoption above) is not creating anything, is
					// already counted by liveMintBudget, and holding it would
					// strand a Task that has already been paid for. Only a
					// genuine Create is gated.
					room, roomErr := liveRoomFor(ctx)
					if roomErr != nil {
						return roomErr
					}
					if room <= 0 {
						heldOnLiveCeiling = true
						if r.Metrics != nil {
							r.Metrics.AdmissionBlocked(proj.Name, class, q.Spec.Kind, "live_ceiling")
						}
						if !ceilingLogged {
							ceilingLogged = true
							log.FromContext(ctx).Info("queue: mint held, project is at its live-pod ceiling (an owed resumption reserves a slot); the event stays queued",
								"action", "admission_blocked", "project", proj.Name, "class", class,
								"reason", "live_ceiling", "resource_id", q.Name, "kind", q.Spec.Kind,
								"ceiling", tatarav1alpha1.MaxLivePods(proj))
						}
						continue // leave Queued; no slot burned
					}
					// created is whether THIS pass brought a Task into existence, and
					// therefore whether the live-mint budget is spent below. The generic
					// arm always says yes (its AlreadyExists case is reachable only for
					// an explicitly named mint and was always counted this way); the
					// adoption arm can find a live twin the sweep or an earlier event
					// already minted, which liveMintBudget has ALREADY counted - charging
					// for it again contradicts the `existing == nil` gate's own rationale.
					created := true
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
						created = verdict == adoptCreated
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
					// SPEND the live-mint budget: a Task that will become live now
					// exists because of this pass, and the NEXT mint in the same
					// pass must see the remaining room rather than the room the
					// pass started with. liveMintBudget's own `pending` term only
					// catches this across passes; within one pass the spend is
					// what bounds a burst. It applies to BOTH arms above - an
					// adopted Task becomes live exactly like any other, and design
					// D1 gives adoption no cap of its own precisely because this
					// general one already bounds it.
					if created {
						mintBudget--
					}
				}
				if queueTaskDone(existing) {
					// The Task this event owns is DEAD (a named mint colliding with a
					// corpse, or our own mint that ran and terminated before the status
					// patch landed). Delete it and signal a prompt requeue so the next
					// pass mints a fresh one. Continue processing the rest of the pool's
					// queued events in this same pass so sibling events are not abandoned.
					if delErr := r.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
						return delErr
					}
					log.FromContext(ctx).Info("queue: deleted stale terminal task on re-admit",
						"action", "queue_stale_delete", "resource_id", q.Name, "task", existing.Name)
					requeue = true
					continue
				}
				taskName = existing.Name
			}
			admittedAt := metav1.Now()
			if err := patchQueuedEventStatus(ctx, r.Client, q, func(fresh *tatarav1alpha1.QueuedEvent) bool {
				fresh.Status.State = tatarav1alpha1.QueueStateAdmitted
				fresh.Status.TaskRef = taskName
				fresh.Status.AdmittedAt = &admittedAt
				return true
			}); err != nil {
				return err
			}
			inflight++
			if r.Metrics != nil {
				r.Metrics.QueueAdmitted(class, q.Spec.Kind)
			}
			log.FromContext(ctx).Info("queue: admitted",
				"action", "queue_admit", "resource_id", q.Name, "task", taskName,
				"class", class, "seq", q.Spec.Seq, "kind", q.Spec.Kind)
		}
		return nil
	}

	if err := admitPool(tatarav1alpha1.QueueClassAlert, proj.AlertCapacity()); err != nil {
		return false, heldOnUsage, heldOnLiveCeiling, err
	}
	if err := admitPool(tatarav1alpha1.QueueClassNormal, proj.QueueCapacity()); err != nil {
		return false, heldOnUsage, heldOnLiveCeiling, err
	}
	return requeue, heldOnUsage, heldOnLiveCeiling, nil
}

// adoptVerdict is what admitAdoptedUpgrade decided about an adoption event.
type adoptVerdict int

const (
	// adoptCreated: THIS pass created the adopted upgrade Task, so it spends a
	// live-mint slot.
	adoptCreated adoptVerdict = iota
	// adoptExisting: the Task was already there (the sweep's own mint, or an
	// earlier event's). It is admitted, but liveMintBudget already counts it, so
	// it spends nothing.
	adoptExisting
	// adoptDropped: the adoption is no longer owed. The event has been DELETED.
	// It burns no slot and is never requeued.
	adoptDropped
	// adoptRetry: the mint is still OWED but did not happen (a dead twin held
	// the deterministic name and was just deleted). Stays Queued, burns no slot.
	adoptRetry
)

// adoptAdmitErrorReason is admitAdoptedUpgrade's SweepErrorsTotal{activity=
// QueueActivity} reason: a genuine machinery failure on the admit-time
// re-check-then-mint path (a Get, resolveLiveMROwner, MintAdoptedUpgradeTask,
// or drop's own Delete), as opposed to a refusal AdoptionEventDroppedTotal
// already counts. Scanned by TestQueueAdmitErrorReasonIsSeeded
// (internal/obs) against obs.queueSeedReasons - keep the two in sync.
const adoptAdmitErrorReason = "admit_adopted_upgrade"

// admitAdoptedUpgrade mints the upgrade Task for a QUEUED dependency-upgrade
// adoption, and it re-asks the adoption question first.
//
// THE PREDICATES DO NOT RUN ANYWHERE ELSE ON THIS PATH. In the sweep they ran in
// ClassifyPR one frame above the mint; MintAdoptedUpgradeTask has never checked
// AdoptUpgradeMR itself. So with no re-ask, nothing at all would validate a
// snapshot between enqueue and admission.
//
// WHAT THE RE-ASK ACTUALLY GUARANTEES, precisely, because two of AdoptUpgradeMR's
// clauses read the SNAPSHOT and the dispatcher deliberately makes no forge call:
//
//   - FRESH, and the whole point of re-asking: the mirror CR and its LIVE owner.
//     They are the two inputs that genuinely change while an event waits, and
//     they carry clauses (e) and (h) - a Task that took the merge request over
//     while it queued, and a mirror stood down to an external push. An unadopted
//     merge request has no mirror at all until this mint's own bind creates one,
//     which is why they are read here rather than carried on the event.
//   - IMMUTABLE, so the snapshot is authoritative: head branch, author, and head
//     repo. Clause (d) still fails CLOSED on an empty head repo, which is what
//     keeps a fork out when a forge or an old delivery would not say.
//
// TWO GAPS, NAMED SO THEY ARE DISCOVERABLE. Both are snapshot-sourced facts a
// human can change after the enqueue, and neither is closed here:
//
//  1. adoptionRefused is keyed on pr.HeadSHA, which comes from the snapshot. If
//     the engine force-pushes a new tree onto the branch and a human then refuses
//     THAT SHA, this check compares against the OLD one and lets the adoption
//     through. Task 7 refreshes headSHA on a `synchronize` delivery, which is what
//     closes this half; a re-read here would need a forge call the design keeps
//     out of the dispatcher.
//  2. the trigger-label escape hatch, clause (f), reads pr.Labels off the
//     snapshot. A label applied AFTER the enqueue is invisible at admission, so
//     the merge request adopts rather than falling through to a review Task. The
//     recovery is the one clause (g) documents: AnnAdoptionRefused on the mirror,
//     or merging it yourself.
func (r *DispatcherReconciler) admitAdoptedUpgrade(ctx context.Context, proj *tatarav1alpha1.Project,
	q *tatarav1alpha1.QueuedEvent) (*tatarav1alpha1.Task, adoptVerdict, error) {

	lg := log.FromContext(ctx)
	// fail COUNTS a genuine machinery error on this path (a Get, the live-owner
	// resolve, the mint itself, or drop's own Delete) - the one class of failure
	// AdoptionEventDroppedTotal does NOT cover, since that counter's own
	// contract is "a non-zero rate is the mechanism WORKING" and a transient API
	// error is the opposite of that. Before this, admitAdoptedUpgrade's errors
	// only surfaced through the generic controller-runtime reconcile-error
	// counter (no reason breakdown) - the same blind spot #495 found in
	// sweep.go's own fail(reason, ...) sites, here for the dispatcher's admit
	// path instead. ONE reason for every site: unlike sweep.go's per-read
	// breakdown, every site here is one step of the SAME re-check-then-mint
	// sequence, already disambiguated by the ERROR log line and its resource_id.
	fail := func(err error) (*tatarav1alpha1.Task, adoptVerdict, error) {
		obs.SweepErrorsTotal.WithLabelValues(proj.Name, QueueActivity, adoptAdmitErrorReason).Inc()
		lg.Error(err, "queue: admit_adopted_upgrade", "action", "queue_error",
			"resource_id", q.Name, "project", proj.Name, "reason", adoptAdmitErrorReason)
		return nil, adoptRetry, err
	}
	// COUNTED ONLY WHEN THIS CALL ITSELF DISPOSED OF THE EVENT. A transient
	// Delete failure leaves the event Queued and this whole function re-runs
	// next pass, so counting before the delete would count one refusal twice -
	// it goes through fail, not obs.AdoptionEventDroppedTotal, since the event
	// was NOT dropped.
	//
	// A NotFound means someone ELSE already disposed of it - in practice the
	// webhook's dropQueuedAdoption (mirror_refresh.go), which deletes a still-
	// Queued adoption on merged/closed and counts it under ITS OWN reason.
	// Counting it again here under THIS reason would double-count one merge
	// request's drop across two different reason labels, which is exactly what
	// AdoptionEventDroppedTotal's "how many drops, and why" contract cannot
	// tolerate. The control flow is still correct - the event is gone, there is
	// nothing left to admit, so this still returns the dropped verdict - it
	// just must not increment a second time. Do not simplify this branch back
	// to unconditionally counting; that reintroduces the double count.
	drop := func(reason string) (*tatarav1alpha1.Task, adoptVerdict, error) {
		err := r.Delete(ctx, q)
		switch {
		case err == nil:
			obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, reason).Inc()
			lg.Info("queue: dropped a queued dependency-upgrade adoption",
				"action", "queue_adopt_drop", "resource_id", q.Name, "project", proj.Name,
				"repo", q.Spec.RepositoryRef, "number", q.Spec.Payload.AdoptedUpgrade.Number,
				"reason", reason)
		case apierrors.IsNotFound(err):
			lg.Info("queue: adoption event already gone before this drop's own delete, not double-counted",
				"action", "queue_adopt_drop_already_gone", "resource_id", q.Name, "project", proj.Name,
				"repo", q.Spec.RepositoryRef, "number", q.Spec.Payload.AdoptedUpgrade.Number,
				"reason", reason)
		default:
			return fail(err)
		}
		return nil, adoptDropped, nil
	}

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: q.Namespace, Name: q.Spec.RepositoryRef}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return drop("repository_gone")
		}
		return fail(err)
	}

	pr := PRRefFromAdopted(q.Spec.Payload.AdoptedUpgrade)
	m := r.minter()
	cr, err := m.mergeRequestCR(ctx, proj, &repo, pr.Number)
	if err != nil {
		return fail(err)
	}
	// THE FRESHNESS BACKSTOP FOR A MERGED/CLOSED MERGE REQUEST, and it is FREE:
	// cr is already fetched above for clause (e)'s live-owner check below, no new
	// read added. The PRIMARY defense is the webhook
	// (handleMRSynchronize/handleMRClosed, internal/webhook/mirror_refresh.go),
	// which refreshes or drops the queued snapshot the moment a
	// synchronize/close delivery arrives; this is the second gate for when that
	// delivery is lost or its own Delete fails, and the event is still sitting
	// Queued when this pass reaches it.
	//
	// IT IS NOT UNIVERSAL, and does not pretend to be. fitMR
	// (internal/webhook/mirror_refresh.go) only UPDATES an existing
	// MergeRequest mirror CR, it never CREATES one - so an adoption that has
	// never been admitted before has cr == nil here regardless of the live
	// merge request's real state, and this check cannot see it. The webhook
	// handlers above remain the ONLY defense for that common, first-admission
	// case; this only catches a merge request whose mirror CR already existed
	// (e.g. a prior review Task, or an earlier adoption attempt) before it
	// merged or closed out from under a still-Queued event.
	if cr != nil && cr.Status.State == "merged" {
		return drop("merged")
	}
	if cr != nil && cr.Status.State == "closed" {
		return drop("closed")
	}
	liveOwner, err := m.resolveLiveMROwner(ctx, proj, cr, QueueActivity)
	if err != nil {
		return fail(err)
	}
	// owner is nil, not looked up: taskForBranch matches on agent.TaskBranch and
	// can never see an engine's renovate/* branch, so the lookup the sweep does
	// there answers nil for every adoption. The clause that actually stops a
	// re-adoption is liveOwner.
	if !AdoptUpgradeMR(proj, pr, nil, liveOwner, cr) {
		return drop("not_adoptable")
	}

	task, outcome, err := m.MintAdoptedUpgradeTask(ctx, proj, &repo, pr, m.spillerFor(proj), queue.MintStamp(q))
	if err != nil {
		return fail(err)
	}
	if outcome == MintTombstoneDeleted {
		// A dead twin held the deterministic name and has just been deleted; the
		// mint is still OWED. Stay Queued and let the prompt requeue re-drive it.
		// There is no MintNotOwed arm here on purpose: every MintNotOwed return in
		// MintAdoptedUpgradeTask carries a non-nil error, which the check above
		// already consumed, so the outcome cannot reach this point with a Task to
		// admit or a reason to drop.
		return nil, adoptRetry, nil
	}
	lg.Info("queue: adopted a dependency upgrade merge request into an upgrade task",
		"action", "queue_adopt_upgrade_mr", "resource_id", task.Name, "project", proj.Name,
		"repo", repo.Name, "number", pr.Number, "head_branch", pr.HeadBranch,
		"author", pr.Author, "outcome", string(outcome))
	if outcome == MintCreated {
		return task, adoptCreated, nil
	}
	return task, adoptExisting, nil
}

// convergeAdoptedAdmission re-runs the two writes an adopted mint makes AFTER its
// Task exists - the mirror bind and the semver floor - for an event whose Task the
// #443 idempotency lookup already found. See its call site for why the mint
// branch's own converge arm can never be reached again once that lookup hits.
//
// A GONE REPOSITORY IS NOT A REFUSAL HERE, unlike in admitAdoptedUpgrade. The Task
// exists and is about to be admitted; there is nothing left to adopt into and
// nothing to undo, so this logs and returns rather than deleting an event that has
// already minted.
func (r *DispatcherReconciler) convergeAdoptedAdmission(ctx context.Context, proj *tatarav1alpha1.Project,
	q *tatarav1alpha1.QueuedEvent, task *tatarav1alpha1.Task) error {

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: q.Namespace, Name: q.Spec.RepositoryRef}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			log.FromContext(ctx).Info("queue: skipped converging an adopted upgrade task, its repository is gone",
				"action", "queue_adopt_converge_skip", "resource_id", task.Name, "project", proj.Name,
				"repo", q.Spec.RepositoryRef, "number", q.Spec.Payload.AdoptedUpgrade.Number)
			return nil
		}
		return err
	}
	m := r.minter()
	pr := PRRefFromAdopted(q.Spec.Payload.AdoptedUpgrade)
	return m.convergeAdoptedMint(ctx, proj, &repo, mrSnapshot(proj, &repo, pr), task, m.spillerFor(proj))
}

// minter builds the intake funnel the dispatcher's adoption path mints through.
// Activity is QueueActivity: an owner ref this repairs is repaired at ADMISSION,
// not in a sweep pass, and obs.ownerActivities seeds that value.
func (r *DispatcherReconciler) minter() *Minter {
	return &Minter{Client: r.Client, APIReader: r.APIReader, Scheme: r.Scheme,
		Metrics: r.Metrics, SpillerFor: r.SpillerFor, Activity: QueueActivity}
}

// ticketVerdict is what admitTicket decided about an admission ticket.
type ticketVerdict int

const (
	// ticketAdmitted: the existing Task's pod may now be created.
	ticketAdmitted ticketVerdict = iota
	// ticketDropped: the ticket is stale (Task gone, terminal, or past pods). It
	// has been DELETED. It burns no slot and is never requeued.
	ticketDropped
	// ticketDeferred: the Task has not reached its pod stage yet. The ticket stays
	// Queued and burns no slot.
	ticketDeferred
)

// admitTicket admits a QueuedEvent that names an EXISTING Task (contract B.7,
// F.3): the QE is an ADMISSION TICKET for that Task's pod, not a Task-creation
// request. Nothing is minted here, ever.
//
// The STAGE MACHINE is the source of truth for which pod to spawn;
// payload.agentKind is advisory and only logged when the two disagree. The Task
// is stamped with the queued-event label, which is what makes its slot
// accountable (poolInflight), maps its events back to this QueuedEvent
// (SetupWithManager), and is the pod-ensure path's handle on the ticket.
func (r *DispatcherReconciler) admitTicket(ctx context.Context, q *tatarav1alpha1.QueuedEvent, now time.Time) (string, ticketVerdict, error) {
	lg := log.FromContext(ctx)
	// UNCACHED read when a manager is wired: a Task created moments ago may not be
	// in the informer cache yet, and a cache miss here would DROP a live ticket.
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	task := &tatarav1alpha1.Task{}
	err := reader.Get(ctx, types.NamespacedName{Namespace: q.Namespace, Name: q.Spec.Payload.TaskRef}, task)
	if apierrors.IsNotFound(err) {
		return "", ticketDropped, r.dropTicket(ctx, q, "task_gone")
	}
	if err != nil {
		return "", ticketDeferred, err
	}
	if task.DeletionTimestamp != nil || queueTaskDone(task) {
		return "", ticketDropped, r.dropTicket(ctx, q, "task_terminal")
	}

	from := task.Status.State
	if stage.AgentKindFor(from, task.Spec.Kind) == "" {
		switch from {
		case "", tatarav1alpha1.StateNew:
			// Not at a pod state yet: `new` is pure operator triage and picks the
			// next state from spec.kind. The ticket waits, Queued.
			return "", ticketDeferred, nil
		default:
			// merged / deployed: the Task is past its pods. Stale ticket.
			//
			// THE OLD `approved -> implementing` ADMISSION EDGE IS GONE (#521).
			// approved was a POD-LESS gate the Task waited in, and admission was
			// what moved it into implementing. `refined` replaced it and is a LIVE
			// state that runs the gate agent itself, so admission spawns a pod
			// there like any other live state, and the ONLY thing that moves a Task
			// to under-implementation is restapi's approval gate granting.
			return "", ticketDropped, r.dropTicket(ctx, q, "state_runs_no_agent")
		}
	}
	agentKind := stage.AgentKindFor(from, task.Spec.Kind)
	if want := q.Spec.Payload.AgentKind; want != "" && want != agentKind {
		lg.Info("queue: ticket agentKind disagrees with the stage, the STAGE wins",
			"action", "queue_ticket_kind_mismatch", "resource_id", q.Name, "task", task.Name,
			"payloadAgentKind", want, "state", from, "agentKind", agentKind)
	}

	if task.Labels[queue.LabelQueuedEvent] != q.Name {
		if err := patchTaskAnnotations(ctx, r.Client, task, func(fresh *tatarav1alpha1.Task) bool {
			if fresh.Labels[queue.LabelQueuedEvent] == q.Name {
				return false
			}
			if fresh.Labels == nil {
				fresh.Labels = map[string]string{}
			}
			fresh.Labels[queue.LabelQueuedEvent] = q.Name
			return true
		}); err != nil {
			return "", ticketDeferred, err
		}
	}
	// ADMISSION NO LONGER APPLIES AN EDGE (#521). It used to: `approved` was a
	// POD-LESS gate the Task waited in, and admitting its ticket was what moved
	// it into `implementing`. That state is gone - `refined` replaced it and is a
	// LIVE state that runs the gate agent itself - so admission spawns a pod
	// where the Task already is, and the ONLY thing that moves a Task into
	// under-implementation is restapi's approval gate granting. What is left here
	// is the agentKind self-heal.
	if task.Status.AgentKind == agentKind {
		return task.Name, ticketAdmitted, nil // already consistent; no status write
	}
	// Self-heal: status.agentKind is what the pod spec and /outcome key on, and
	// an empty one 409s the agent's submit.
	task.Status.AgentKind = agentKind
	if err := patchTaskStatus(ctx, r.Client, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Status.AgentKind == agentKind {
			return false
		}
		fresh.Status.AgentKind = agentKind
		return true
	}); err != nil {
		return "", ticketDeferred, err
	}
	return task.Name, ticketAdmitted, nil
}

// dropTicket deletes a stale admission ticket. A ticket whose Task is gone,
// terminal, or past its pods is dropped CLEANLY: no slot burned, no requeue
// loop, no error propagated to the pool.
func (r *DispatcherReconciler) dropTicket(ctx context.Context, q *tatarav1alpha1.QueuedEvent, reason string) error {
	log.FromContext(ctx).Info("queue: dropped stale admission ticket",
		"action", "queue_ticket_drop", "resource_id", q.Name, "task", q.Spec.Payload.TaskRef,
		"class", q.Spec.Class, "kind", q.Spec.Kind, "reason", reason)
	if err := r.Delete(ctx, q); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// reconcileDone GC-deletes Admitted events whose Task is done or gone, or which
// the Task has already moved past (contract B.7: the ticket is released "once
// the Task has left the stage that requested it"). Completed QueuedEvents are
// removed, not tombstoned to Done.
func (r *DispatcherReconciler) reconcileDone(ctx context.Context, qes []tatarav1alpha1.QueuedEvent, tasks []tatarav1alpha1.Task) (bool, error) {
	changed := false
	for i := range qes {
		q := &qes[i]
		if q.Status.State != tatarav1alpha1.QueueStateAdmitted {
			continue
		}
		t := taskByName(tasks, q.Status.TaskRef)
		if t == nil || queueTaskDone(t) || ticketSpent(q, t) {
			log.FromContext(ctx).Info("queue: event done", "action", "queue_done", "resource_id", q.Name, "class", q.Spec.Class)
			if err := r.Delete(ctx, q); err != nil && !apierrors.IsNotFound(err) {
				return changed, err
			}
			changed = true
		}
	}
	return changed, nil
}

// +kubebuilder:rbac:groups=tatara.dev,resources=queuedevents,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tatara.dev,resources=queuedevents/status,verbs=get;update;patch

// The body is doReconcile; this wrapper is the #538 shutdown-cancellation
// boundary (see classifyReconcileShutdown).
func (r *DispatcherReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.doReconcile(ctx, req)
	return classifyReconcileShutdown(ctx, "dispatcher", req.Name, res, err)
}

func (r *DispatcherReconciler) doReconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var qe tatarav1alpha1.QueuedEvent
	if err := r.Get(ctx, req.NamespacedName, &qe); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Spec.ProjectRef}, &proj); err != nil {
		// Project gone (deleted mid-flight): nothing to admit against. Drop
		// the not-found so we do not loop on a zero-value Project whose
		// capacities read 0 and silently block all admission.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if proj.Name == "" {
		return ctrl.Result{}, nil
	}

	listProject := func() ([]tatarav1alpha1.QueuedEvent, []tatarav1alpha1.Task, error) {
		return r.listProjectVia(ctx, r.Client, &proj)
	}

	qes, tasks, err := listProject()
	if err != nil {
		return ctrl.Result{}, err
	}
	if _, err := r.reconcileDone(ctx, qes, tasks); err != nil {
		return ctrl.Result{}, err
	}
	// Re-list after Done mutations so admission sees fresh QE and Task state.
	qes, tasks, err = listProject()
	if err != nil {
		return ctrl.Result{}, err
	}
	// Token-budget decision for this admit pass (issue #189): computed once from
	// the project's resolved config + persisted window/snapshot state, used both
	// to gate admission and to drive the boundary-aware requeue below.
	budgetCfg := proj.BudgetConfig(r.BudgetDefaults)
	sub := proj.BudgetSubscription()
	// claudeSubscription mode reads the fleet store's snapshot, which is fed by
	// the agent pods' statusline reports (see AccountUsageFeedReconciler) and,
	// when USAGE_ENABLED is set, by the /api/oauth/usage poller. It reads the
	// last-known snapshot even when the poll has gone stale (Store.Healthy=false).
	//
	// The spec's "fall back to customWindow when stale" is still deliberately
	// NOT implemented (F7): a claudeSubscription Project has no customWindow
	// inputs (TokenLimit/ResetSchedule) to fall back to. Staleness is governed
	// by TWO gates now, not one: budget.active() expires each window at its own
	// reset, and budget.Config.MaxSnapshotAge expires the whole snapshot once it
	// stops being refreshed. The second closes the F7 de-scope recorded in
	// MEMORY.md 2026-07-04, where active() alone let an un-refreshed snapshot
	// keep governing until its last-reported reset.
	//
	// Both gates FAIL OPEN: past max age the snapshot stops governing and
	// admission goes back to admitting everything. That is deliberate (a broken
	// feed must not wedge the platform) and is only defensible because
	// TataraAccountUsageFeedDead is the compensating control - it alerts on
	// tatara_account_usage_gate_ready == 0. The wrapper's OTel 429 floor is the
	// hard backstop underneath both.
	if budgetCfg.Mode == budget.ModeClaudeSubscription && r.Usage != nil {
		sub = r.Usage.Get().Subscription()
	}
	decision := budget.Evaluate(budgetCfg, proj.BudgetWindowState(), sub, time.Now())
	requeue, heldOnUsage, heldOnLiveCeiling, err := r.admit(ctx, &proj, qes, tasks, decision, budgetCfg, sub, time.Now())
	if err != nil {
		return ctrl.Result{}, err
	}
	// Re-list after admission to get fresh state for gauge snapshot.
	qes, tasks, err = listProject()
	if err != nil {
		return ctrl.Result{}, err
	}
	if r.Metrics != nil {
		for _, class := range []string{tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueClassAlert} {
			depth := 0
			for i := range qes {
				if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
					depth++
				}
			}
			r.Metrics.SetQueueDepth(proj.Name, class, depth)
			r.Metrics.SetQueueInflight(proj.Name, class, r.poolInflight(qes, tasks, class))
		}
		// Token-budget gauges (issue #189): track usage against both thresholds so
		// a dashboard plots used vs proactive/emergency per project. Only emitted
		// when the budget is enabled, so disabled projects create no series.
		if budgetCfg.Enabled {
			r.Metrics.SetTokenBudgetUsedRatio(proj.Name, "used", decision.UsedPercent/100)
			// The thresholds are the GOVERNING window's own pair (per-window
			// thresholds mean there is no single mode-wide pair to plot
			// against), so the used/proactive/emergency triple always describes
			// one coherent window. Decision fills them for customWindow mode too.
			r.Metrics.SetTokenBudgetUsedRatio(proj.Name, "proactive", float64(decision.GoverningProactivePercent)/100)
			r.Metrics.SetTokenBudgetUsedRatio(proj.Name, "emergency", float64(decision.GoverningEmergencyPercent)/100)
		}
	}
	// Pool-full hold: does any pool have waiting (Queued/empty-state) work while
	// at capacity? Only ever turns into a self-poll when NO admission backstop is
	// wired to catch a missed Task-terminal watch event (see blockedPoll).
	waiting := false
	for _, class := range []string{tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueClassAlert} {
		cap := proj.QueueCapacity()
		if class == tatarav1alpha1.QueueClassAlert {
			cap = proj.AlertCapacity()
		}
		if r.poolInflight(qes, tasks, class) >= cap {
			for i := range qes {
				if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
					waiting = true
					break
				}
			}
		}
	}
	// Budget-hold backstop: when a pool is paused on the token budget and still has
	// Queued work, re-check near the next window reset (the cron boundary, capped)
	// so the held work resumes promptly once the window rolls, even if no new
	// QueuedEvent arrives to re-trigger the dispatcher (issue #189).
	budgetHeld := (decision.ProactiveBlocked && poolHasQueued(qes, tatarav1alpha1.QueueClassNormal)) ||
		(decision.EmergencyBlocked && poolHasQueued(qes, tatarav1alpha1.QueueClassAlert))
	if requeue {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if budgetHeld {
		return ctrl.Result{RequeueAfter: budgetRequeueAfter(budgetCfg, time.Now())}, nil
	}
	// Account-usage-held backstop (claudeSubscription mode): the fleet Store is not
	// a watched resource, so a per-kind or pool-class hold would never resume until
	// an unrelated QueuedEvent re-triggered the dispatcher. Requeue so the gate
	// re-evaluates once the next poll refreshes the store (F2).
	if heldOnUsage {
		return ctrl.Result{RequeueAfter: r.usageRequeueAfter()}, nil
	}
	// A live-ceiling mint hold (#570) is the same SHAPE as a pool_full hold -
	// work left Queued behind a capacity bound - so it rides the same backstop
	// and the same fallback poll rather than inventing a third requeue policy.
	// Everything that can change the answer already wakes admission: a live
	// conversation ending and a park un-parking are both Task writes, and the
	// Task carries queue.LabelQueuedEvent, so Watches(&Task{}, mapTaskToQE)
	// fires; the leader-only backstop re-enqueues a per-pool representative
	// unconditionally on top of that. blockedPoll is the belt for a wiring with
	// no backstop at all.
	if waiting || heldOnLiveCeiling {
		if d := r.blockedPoll(); d > 0 {
			return ctrl.Result{RequeueAfter: d}, nil
		}
	}
	return ctrl.Result{}, nil
}

// blockedPollInterval is the self-poll cadence a pool_full-blocked reconcile
// falls back to when NO admission backstop is wired (BackstopEvents nil: unit
// tests, and any wiring that does not run the leader-only sweep). With no
// backstop there is nothing to heal a lost Task-watch event, so the poll is
// the only thing standing between a missed wakeup and a permanently stalled
// queue - it must stay.
const blockedPollInterval = 30 * time.Second

// blockedPoll is how long this reconcile waits before re-attempting admission
// on its own after a pool_full hold. ZERO when the backstop is wired.
//
// Issue #496: a reconcile of ANY QueuedEvent walks and admits the WHOLE
// project pool, so N events blocked behind a full pool each scheduling their
// own poll produced N copies of one answer - measured at 40/min for a queue of
// depth 20, each pass burning a reconcile and one INFO line, and scaling up
// precisely when the operator is most loaded. Nothing about a blocked event is
// time-dependent: the only things that can change the answer are a slot
// freeing, a new event arriving, or a capacity change, and admission is woken
// for all three:
//
//   - a slot freeing wakes it through Watches(&Task{}, mapTaskToQE) - the
//     finishing Task carries queue.LabelQueuedEvent naming the ticket it was
//     admitted through, and that reconcile GCs the spent ticket and admits the
//     next waiting event in the same pass;
//   - a new event wakes it through For(&QueuedEvent{});
//   - the leader-only backstop (RunBackstop/EnqueuePending) re-enqueues one
//     representative per pool every dispatcherBackstopInterval UNCONDITIONALLY,
//     whether or not the pool is full.
//
// That last one is why dropping the poll cannot lose a wakeup: the backstop is
// a level-triggered sweep over authoritative cluster state, not an edge, so the
// worst case for a dropped watch event is one sweep interval of latency rather
// than an indefinite stall. Making the backstop SKIP already-blocked events
// would break exactly that property, which is why it does not (see
// TestBackstop_DoesNotSkipASaturatedPool).
func (r *DispatcherReconciler) blockedPoll() time.Duration {
	if r.BackstopEvents != nil {
		return 0
	}
	return blockedPollInterval
}

// poolHasQueued reports whether the pool of the given class has any Queued
// (not-yet-admitted) QueuedEvent.
func poolHasQueued(qes []tatarav1alpha1.QueuedEvent, class string) bool {
	for i := range qes {
		if qes[i].Spec.Class == class && isQueued(qes[i].Status.State) {
			return true
		}
	}
	return false
}

// budgetRequeueAfter returns how long to wait before re-checking a budget-held
// pool: the time to the next custom-window reset boundary (from the cron, plus a
// small slack and capped at 5m to bound staleness), or a 60s fallback when there
// is no parseable schedule (e.g. claudeSubscription mode, where the snapshot's
// own reset time drives unblocking).
func budgetRequeueAfter(cfg budget.Config, now time.Time) time.Duration {
	const fallback = 60 * time.Second
	const maxWait = 5 * time.Minute
	if cfg.ResetSchedule == "" {
		return fallback
	}
	sched, err := budget.ParseSchedule(cfg.ResetSchedule)
	if err != nil {
		return fallback
	}
	next := sched.Next(now)
	if next.IsZero() {
		return fallback
	}
	wait := next.Sub(now) + time.Second
	if wait <= 0 {
		return fallback
	}
	if wait > maxWait {
		return maxWait
	}
	return wait
}

// usageRequeueAfter is the delay before re-checking work held on the account-
// usage gate (claudeSubscription mode): the configured poll interval when known,
// else a 60s fallback. Held work resumes on the reconcile this schedules once the
// next poll has refreshed the fleet store (F2).
func (r *DispatcherReconciler) usageRequeueAfter() time.Duration {
	if r.UsagePollInterval > 0 {
		return r.UsagePollInterval
	}
	return 60 * time.Second
}

func filterQEsByProject(in []tatarav1alpha1.QueuedEvent, project string) []tatarav1alpha1.QueuedEvent {
	out := make([]tatarav1alpha1.QueuedEvent, 0)
	for i := range in {
		if in[i].Spec.ProjectRef == project {
			out = append(out, in[i])
		}
	}
	return out
}

func filterTasksByProject(in []tatarav1alpha1.Task, project string) []tatarav1alpha1.Task {
	out := make([]tatarav1alpha1.Task, 0)
	for i := range in {
		if in[i].Spec.ProjectRef == project {
			out = append(out, in[i])
		}
	}
	return out
}

func (r *DispatcherReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if err := r.registerFieldIndexes(mgr.GetFieldIndexer()); err != nil {
		return err
	}
	mapTaskToQE := func(ctx context.Context, obj client.Object) []reconcile.Request {
		reqs := []reconcile.Request{}
		if qeName := obj.GetLabels()[queue.LabelQueuedEvent]; qeName != "" {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: qeName}})
		}
		// A ticket for a Task that has not yet reached its pod stage is deferred, and
		// the Task carries no queued-event label until it is admitted - so the label
		// mapping alone would never re-trigger it. Map the Task back to any ticket
		// that NAMES it, so admission follows the stage transition immediately.
		var qel tatarav1alpha1.QueuedEventList
		if err := r.List(ctx, &qel, client.InNamespace(obj.GetNamespace())); err != nil {
			return reqs
		}
		for i := range qel.Items {
			q := &qel.Items[i]
			if q.Spec.Payload.TaskRef != obj.GetName() || !isQueued(q.Status.State) {
				continue
			}
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: q.Namespace, Name: q.Name}})
		}
		return reqs
	}
	// MaxConcurrentReconciles: 1 is explicit here to match every sibling
	// controller (task/project/repository) - it was already controller-runtime's
	// implicit default (unset == 1), so this changes no behavior, only makes the
	// serialization the #348 conflict fix relies on visible rather than assumed.
	b := ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.QueuedEvent{}).
		Watches(&tatarav1alpha1.Task{}, handler.EnqueueRequestsFromMapFunc(mapTaskToQE)).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 1})
	// BackstopEvents (issue #395) is the leader-only admission backstop's
	// output channel: wire it as an extra raw event source so a GenericEvent
	// the backstop pushes for a pending QueuedEvent reconciles exactly like a
	// watch trigger would. Nil (every pre-existing caller/unit test) leaves the
	// controller purely watch-driven, unchanged.
	if r.BackstopEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.BackstopEvents, &handler.EnqueueRequestForObject{}))
	}
	return b.Complete(r)
}

// pendingPoolHeads picks ONE representative pending QueuedEvent per pool class
// from a single project's events: the head, by the same (priority, seq)
// ordering admitPool admits in. Classes are returned in a stable order.
//
// Issue #496: the backstop used to push every pending QueuedEvent on every
// sweep, which at the observed depth of 20 was 20 re-enqueues/minute of pure
// duplication - a reconcile of ANY QueuedEvent walks and admits the WHOLE
// project pool, so one representative drives an IDENTICAL, complete admit pass
// for the project at O(1) cost instead of O(queue depth).
//
// One per POOL rather than one per project purely for redundancy: the pass
// covers both pools whichever event triggers it, but a representative deleted
// between this list and its reconcile would waste the sweep, and the two pools
// drain independently. Two is still O(1).
func pendingPoolHeads(qes []tatarav1alpha1.QueuedEvent) []*tatarav1alpha1.QueuedEvent {
	heads := map[string]*tatarav1alpha1.QueuedEvent{}
	for i := range qes {
		q := &qes[i]
		if !isQueued(q.Status.State) {
			continue
		}
		if cur, ok := heads[q.Spec.Class]; !ok || queueOrderBefore(q, cur) {
			heads[q.Spec.Class] = q
		}
	}
	classes := make([]string, 0, len(heads))
	for class := range heads {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	out := make([]*tatarav1alpha1.QueuedEvent, 0, len(classes))
	for _, class := range classes {
		out = append(out, heads[class])
	}
	return out
}

// EnqueuePending is the leader-only admission backstop's list-and-push step
// (issue #395): it lists every Project and, for each, pushes a GenericEvent for
// ONE representative pending QueuedEvent per pool class (pendingPoolHeads) onto
// BackstopEvents, so DispatcherReconciler.Reconcile runs a full project admit
// pass even if no watch event ever fired (a rollout/leader-handoff race can
// otherwise strand a QueuedEvent in Queued indefinitely). It returns the number
// of events pushed. A nil BackstopEvents is a no-op returning (0, nil).
//
// The sweep is UNCONDITIONAL: it does not skip a project whose pool is already
// full. That is the whole point - a slot can free at any moment, and if the
// Task watch event for that release is lost, this sweep is the only thing that
// notices. Skipping saturated pools would turn a bounded one-sweep delay into
// an indefinite stall (issue #496).
//
// The push is guarded by ctx: source.Channel's consumer (wired in
// SetupWithManager) stops reading once the manager's runnable ctx is
// cancelled (leader loss / shutdown), since producer and consumer share that
// ctx. Without the select below, a full, now-unconsumed BackstopEvents
// buffer would block this goroutine on the unconditional channel send until
// the manager's GracefulShutdownTimeout forcibly tears it down. The select
// lets ctx cancellation win instead, returning the count already pushed.
func (r *DispatcherReconciler) EnqueuePending(ctx context.Context) (int, error) {
	if r.BackstopEvents == nil {
		return 0, nil
	}
	var pl tatarav1alpha1.ProjectList
	if err := r.List(ctx, &pl); err != nil {
		return 0, err
	}
	n := 0
	for i := range pl.Items {
		proj := &pl.Items[i]
		var qel tatarav1alpha1.QueuedEventList
		if err := r.List(ctx, &qel, client.InNamespace(proj.Namespace)); err != nil {
			return n, err
		}
		qesInProject := filterQEsByProject(qel.Items, proj.Name)
		for _, q := range pendingPoolHeads(qesInProject) {
			select {
			case r.BackstopEvents <- event.GenericEvent{Object: q}:
			case <-ctx.Done():
				return n, ctx.Err()
			}
			n++
			if r.Metrics != nil {
				r.Metrics.DispatcherBackstopEnqueued(proj.Name)
			}
		}
	}
	if n > 0 {
		log.FromContext(ctx).Info("queue: backstop enqueue pass",
			"action", "queue_backstop_enqueue", "enqueued", n)
	}
	return n, nil
}

// RunBackstop drives the leader-only admission backstop on a fixed ticker
// until ctx is done: it runs EnqueuePending once immediately on Start (so a
// gap that predates this replica becoming leader heals right away) and again
// every interval, independent of any watch event. Implements manager.Runnable
// via the thin dispatcherBackstopRunnable wrapper in cmd/manager/wire.go.
func (r *DispatcherReconciler) RunBackstop(ctx context.Context, interval time.Duration) error {
	if _, err := r.EnqueuePending(ctx); err != nil {
		log.FromContext(ctx).Error(err, "queue: backstop enqueue failed", "action", "queue_backstop_enqueue_error")
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if _, err := r.EnqueuePending(ctx); err != nil {
				log.FromContext(ctx).Error(err, "queue: backstop enqueue failed", "action", "queue_backstop_enqueue_error")
			}
		}
	}
}
