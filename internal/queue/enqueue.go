package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	LabelQueuedEvent = "tatara.dev/queued-event"
	LabelDedupKey    = "tatara.dev/dedup-key"

	// LabelMintedBy carries the UID of the QueuedEvent that MINTED this Task. It
	// is the mint-idempotency link (issue #443): Task names are GenerateName-
	// minted, so a Create can no longer collide and AlreadyExists is no longer
	// an idempotency primitive. A crash between the Create and the Admitted
	// status patch is absorbed by FINDING the Task through this label instead
	// of minting a second one.
	//
	// Two properties are load-bearing, and neither is true of LabelQueuedEvent:
	//   - it is never rewritten. The dispatcher moves LabelQueuedEvent onto the
	//     current per-stage admission ticket (admitTicket) the moment the Task
	//     takes its first stage step.
	//   - it is the UID, not the name. QueuedEventName is a pure function of
	//     (project, dedupKey), so every brainstorm/refine cycle re-creates the
	//     SAME QueuedEvent name; matching by name would let a new cycle adopt
	//     (and, if terminal, DELETE) the previous cycle's Task.
	LabelMintedBy = "tatara.dev/minted-by"

	// LabelAlertRuleKey stamps the incident rule-key (16-hex incidentDedupKey) on
	// an Issue CR so admission (dedupExists) can suppress a same-rule refire while
	// the open tracker Issue lives, even after its Task terminates. Same VALUE as
	// LabelDedupKey; distinct KEY because it lives on Issues, not QE/Task.
	LabelAlertRuleKey = "tatara.dev/alert-rule-key"

	// LabelAlertGroupKey stamps the coarser incident GROUP-key (16-hex
	// incidentGroupKey) on an Issue CR so file_issue can find the oldest open
	// SIBLING tracker sharing a root cause (different rule, same group) and
	// auto-link the new tracker under it. Correlation is link-only; it never
	// suppresses.
	LabelAlertGroupKey = "tatara.dev/alert-group-key"

	// DedupKeyIndex is the field index key for QueuedEvent.Spec.DedupKey
	// (contract B.7 addendum 7: "queuedEventDedupKey" on QueuedEvent ->
	// spec.dedupKey). It is the AUTHORITATIVE in-flight QueuedEvent dedup
	// lookup ("is an identical event already Queued but not yet Admitted?"),
	// registered by DispatcherReconciler.SetupWithManager
	// (internal/controller/queue_controller.go) - NEVER a label selector.
	DedupKeyIndex = ".spec.dedupKey"

	// TaskRefIndex is the field index key for QueuedEvent.Spec.Payload.TaskRef:
	// "which admission tickets name this Task?". Registered by
	// TaskReconciler.registerFieldIndexes (internal/controller/task_controller.go),
	// which owns ensureTicket, this index's sole consumer.
	TaskRefIndex = ".spec.payload.taskRef"
)

// DedupKeyIndexer is the client.IndexerFunc for DedupKeyIndex.
func DedupKeyIndexer(obj client.Object) []string {
	q, ok := obj.(*tatarav1alpha1.QueuedEvent)
	if !ok || q.Spec.DedupKey == "" {
		return nil
	}
	return []string{q.Spec.DedupKey}
}

// TaskRefIndexer is the client.IndexerFunc for TaskRefIndex. A mint names no
// Task and is indexed under nothing.
func TaskRefIndexer(obj client.Object) []string {
	q, ok := obj.(*tatarav1alpha1.QueuedEvent)
	if !ok || q.Spec.Payload.TaskRef == "" {
		return nil
	}
	return []string{q.Spec.Payload.TaskRef}
}

// fieldSelectorUnsupported reports whether a list error is "field label not
// supported", which happens when a direct (non-cached) client is used without
// a registered field index (e.g. this package's unit tests, or an envtest
// suite with no manager). Mirrors internal/controller.isFieldSelectorUnsupported;
// duplicated rather than imported to avoid a controller->queue->controller cycle.
func fieldSelectorUnsupported(err error) bool {
	return err != nil && strings.Contains(err.Error(), "field label not supported")
}

// QueuedEventStillQueued reports whether a QueuedEvent with the given
// dedupKey is still Queued (not yet Admitted) for projectRef, via the
// DedupKeyIndex field index - the NEW in-flight dedup mechanism (contract B.7
// addendum 7, item 3). Falls back to a full-namespace scan filtered in Go
// when the index is unregistered. This is additive: EnqueueEvent/dedupExists
// below remain the OLD label-based mechanism until Task 20/21 rewires their
// callers (webhook/server.go and friends).
func QueuedEventStillQueued(ctx context.Context, c client.Client, ns, projectRef, dedupKey string) (bool, error) {
	if dedupKey == "" {
		return false, nil
	}
	var qel tatarav1alpha1.QueuedEventList
	err := c.List(ctx, &qel, client.InNamespace(ns), client.MatchingFields{DedupKeyIndex: dedupKey})
	if err != nil {
		if !fieldSelectorUnsupported(err) {
			return false, err
		}
		qel = tatarav1alpha1.QueuedEventList{}
		if err := c.List(ctx, &qel, client.InNamespace(ns)); err != nil {
			return false, err
		}
	}
	for i := range qel.Items {
		q := &qel.Items[i]
		if q.Spec.DedupKey != dedupKey || q.Spec.ProjectRef != projectRef {
			continue
		}
		if q.Status.State == "" || q.Status.State == tatarav1alpha1.QueueStateQueued {
			return true, nil
		}
	}
	return false, nil
}

// dedupLabel converts a dedup key into a K8s-label-safe value.
// K8s label values must match [a-zA-Z0-9][-_.a-zA-Z0-9]* and be <= 63 chars.
// Keys that contain '/', '#', '\x00', or other unsafe chars are sha256-hashed.
// Simple alphanumeric keys (e.g. "grp1", "brainstorm-myproj") pass through unchanged.
//
// Deprecated: this is the OLD dedup mechanism (contract B.7 addendum 7 deletes
// it), kept because EnqueueEvent/dedupExists/BuildTaskFromQueuedEvent below are
// still the live path for every webhook producer (internal/webhook/server.go)
// and cannot be repointed without rewiring those callers - out of scope for
// task 15 (the greenness rule, tasks 1-19 are purely additive). Task 20/21
// rewires the callers and deletes this.
func dedupLabel(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 63 && isLabelSafe(key) {
		return key
	}
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", h[:16])
}

// isLabelSafe returns true when key matches the K8s label-value format:
// [a-zA-Z0-9][-_.a-zA-Z0-9]* with an optional leading char requirement.
// Empty string is handled by the caller.
func isLabelSafe(key string) bool {
	for i, ch := range key {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '-' || ch == '_' || ch == '.':
			if i == 0 {
				return false // must start with alphanumeric
			}
		default:
			return false
		}
	}
	return true
}

// dedupExists reports whether a non-Done QueuedEvent or a non-terminal Task
// with dedupKey already exists for the project. In practice no QE ever reaches
// state Done (reconcileDone GC-deletes them on Task completion), so the != Done
// guard is defensive; do not remove it as callers may set Done in tests.
//
// checkOpenIssue folds an open tracker Issue carrying the rule-key into the
// suppression authority (A2). The escalation path passes false: a liveness
// re-admission MUST proceed PAST the open tracker (that tracker is the whole
// reason it is escalating), while still deduping on a live QueuedEvent/Task so a
// re-fire storm cannot mint a second escalation Task in flight.
func dedupExists(ctx context.Context, c client.Client, ns, projectRef, dedupKey string, checkOpenIssue bool) (bool, error) {
	if dedupKey == "" {
		return false, nil
	}
	label := dedupLabel(dedupKey)
	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(ctx, &qel, client.InNamespace(ns), client.MatchingLabels{LabelDedupKey: label}); err != nil {
		return false, err
	}
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == projectRef && qel.Items[i].Status.State != tatarav1alpha1.QueueStateDone {
			return true, nil
		}
	}
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl, client.InNamespace(ns), client.MatchingLabels{LabelDedupKey: label}); err != nil {
		return false, err
	}
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == projectRef && !tatarav1alpha1.TaskDone(&tl.Items[i]) {
			return true, nil
		}
	}
	if !checkOpenIssue {
		return false, nil
	}
	if _, ok, err := FindOpenIncidentIssue(ctx, c, ns, projectRef, dedupKey); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	return false, nil
}

// FindOpenIncidentIssue returns the open incident Issue CR carrying the given
// rule-key for projectRef, if any. "Open" means Status.State != "closed" (an
// empty State is treated as open: the CR was just minted). Used both by
// dedupExists (suppression authority, A2) and the webhook refire path (A4).
func FindOpenIncidentIssue(ctx context.Context, c client.Client, ns, projectRef, dedupKey string) (*tatarav1alpha1.Issue, bool, error) {
	if dedupKey == "" {
		return nil, false, nil
	}
	var il tatarav1alpha1.IssueList
	if err := c.List(ctx, &il, client.InNamespace(ns), client.MatchingLabels{LabelAlertRuleKey: dedupLabel(dedupKey)}); err != nil {
		return nil, false, err
	}
	for i := range il.Items {
		iss := &il.Items[i]
		if iss.Spec.ProjectRef == projectRef && iss.Status.State != "closed" {
			return iss, true, nil
		}
	}
	return nil, false, nil
}

// FindOldestOpenGroupSibling returns the OLDEST open incident Issue CR that
// shares groupKey with a firing alert but carries a DIFFERENT rule-key than
// excludeRuleKey - i.e. the canonical tracker of a co-firing sibling alert for
// the same root cause. file_issue links a new tracker under it (auto-parent)
// when the agent did not name a parent itself. "Oldest" (by CreationTimestamp)
// is deterministic so a storm of co-firing rules converges on one root tracker.
// Returns (nil,false,nil) when groupKey is empty or no sibling exists.
func FindOldestOpenGroupSibling(ctx context.Context, c client.Client, ns, projectRef, groupKey, excludeRuleKey string) (*tatarav1alpha1.Issue, bool, error) {
	if groupKey == "" {
		return nil, false, nil
	}
	var il tatarav1alpha1.IssueList
	if err := c.List(ctx, &il, client.InNamespace(ns), client.MatchingLabels{LabelAlertGroupKey: dedupLabel(groupKey)}); err != nil {
		return nil, false, err
	}
	var oldest *tatarav1alpha1.Issue
	excl := dedupLabel(excludeRuleKey)
	for i := range il.Items {
		iss := &il.Items[i]
		if iss.Spec.ProjectRef != projectRef || iss.Status.State == "closed" {
			continue
		}
		if excl != "" && iss.Labels[LabelAlertRuleKey] == excl {
			continue
		}
		if oldest == nil || iss.CreationTimestamp.Before(&oldest.CreationTimestamp) {
			oldest = iss
		}
	}
	if oldest == nil {
		return nil, false, nil
	}
	return oldest, true, nil
}

// QueuedEventName is the DETERMINISTIC name a QueuedEvent for (projectRef,
// dedupKey) carries, so a concurrent second EnqueueEvent for the same natural
// key collides on AlreadyExists at the apiserver rather than creating a second
// event behind a lagging dedupExists cache read. A keyless event (dedupKey=="")
// keeps a GenerateName-shaped random name (see EnqueueEvent).
func QueuedEventName(projectRef, dedupKey string) string {
	sum := sha256.Sum256([]byte(projectRef + "|" + dedupKey))
	return "qe-" + hex.EncodeToString(sum[:])[:16]
}

// AdoptUpgradeDedupKey is the natural key of ONE queued dependency-upgrade
// adoption. Both producers derive it from the same two facts they each already
// hold - the Repository CR's NAME and the merge request number - so a webhook
// enqueue and a sweep enqueue of the same merge request collide on
// AlreadyExists at the API server rather than creating two events.
//
// THE LOSER OF A GENUINELY CONCURRENT COLLISION HAS ALREADY BURNED A SEQUENCE
// NUMBER, which this comment used to deny. EnqueueEvent allocates the seq
// AFTER dedupExists but BEFORE Create, so only the dedup-hit path is free; the
// AlreadyExists path has spent one by the time it collides. That is harmless -
// seq is an ordering key and nothing counts it or requires it to be dense - but
// the claim mattered enough to be written down twice, so it is corrected in
// both places (see sweep.go's PRAdoptUpgrade arm).
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

// EnqueueOption tunes EnqueueEvent. The zero set is the normal producer path.
type EnqueueOption func(*enqueueOpts)

type enqueueOpts struct {
	ignoreOpenIssueDedup bool
	priority             *int
}

// WithIgnoreOpenIssueDedup makes EnqueueEvent proceed PAST an open tracker Issue
// carrying the same rule-key (it still dedups on a live QueuedEvent/Task). Only
// the incident liveness-escalation path uses it: the open tracker is exactly
// what it is escalating, so folding it into suppression would make escalation
// impossible.
func WithIgnoreOpenIssueDedup() EnqueueOption {
	return func(o *enqueueOpts) { o.ignoreOpenIssueDedup = true }
}

// WithPriority overrides EnqueueEvent's class-derived Spec.Priority default
// (issue #402). Callers use this when a ticket's stage-agent-kind, not its
// class, decides urgency - e.g. an incident Task's downstream normal-class
// tickets must still outrank sweep work.
func WithPriority(p int) EnqueueOption {
	return func(o *enqueueOpts) { o.priority = &p }
}

// EnqueueEvent writes a QueuedEvent (seq-assigned, owned by Project, state=Queued).
// Returns created=false when dedupKey already has live work. The per-project seq
// is allocated durably (per-project ConfigMap CAS) AFTER the dedup check so a
// deduped event never burns a sequence number.
func EnqueueEvent(ctx context.Context, c client.Client, seq *SeqSource, proj *tatarav1alpha1.Project,
	class string, autonomous bool, dedupKey string, payload tatarav1alpha1.QueuedEventPayload, opts ...EnqueueOption) (*tatarav1alpha1.QueuedEvent, bool, error) {

	var o enqueueOpts
	for _, opt := range opts {
		opt(&o)
	}
	// Clamp at the ONE funnel every producer goes through (issue #495's class).
	// The incident goals are rendered from Grafana alert labels and annotations,
	// which nothing in this repo bounds; an over-long one used to be accepted
	// here and rejected two hops later when the dispatcher built the Task.
	payload.Goal = tatarav1alpha1.TruncateUTF8(payload.Goal, tatarav1alpha1.GoalMaxBytes)
	if payload.NewTask != nil {
		payload.NewTask.Goal = tatarav1alpha1.TruncateUTF8(payload.NewTask.Goal, tatarav1alpha1.GoalMaxBytes)
	}
	dup, err := dedupExists(ctx, c, proj.Namespace, proj.Name, dedupKey, !o.ignoreOpenIssueDedup)
	if err != nil {
		return nil, false, err
	}
	if dup {
		return nil, false, nil
	}
	seqNum, err := seq.Next(ctx, proj.Name)
	if err != nil {
		return nil, false, fmt.Errorf("enqueue: allocate seq: %w", err)
	}
	labels := map[string]string{}
	if dedupKey != "" {
		labels[LabelDedupKey] = dedupLabel(dedupKey)
	}
	om := metav1.ObjectMeta{
		Namespace: proj.Namespace,
		Labels:    labels,
	}
	if dedupKey != "" {
		om.Name = QueuedEventName(proj.Name, dedupKey)
	} else {
		om.GenerateName = "qe-"
	}
	priority := o.priority
	if priority == nil {
		p := 2
		if class == tatarav1alpha1.QueueClassAlert {
			p = 0
		}
		priority = &p
	}
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: om,
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq:           seqNum,
			Class:         class,
			Kind:          payload.Kind,
			Autonomous:    autonomous,
			ProjectRef:    proj.Name,
			RepositoryRef: payload.RepositoryRef,
			DedupKey:      dedupKey,
			Payload:       payload,
			Priority:      priority,
		},
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
	qe.Status.State = tatarav1alpha1.QueueStateQueued
	if err := c.Status().Update(ctx, qe); err != nil {
		return nil, false, fmt.Errorf("enqueue: set state: %w", err)
	}
	return qe, true, nil
}

// IsAdmissionTicket reports whether a QueuedEvent ADMITS AN EXISTING Task
// (contract B.7: payload.taskRef) rather than minting one. In the task-centric
// model a Task already exists by the time it reaches a pod stage, so its
// QueuedEvent is an admission ticket for that Task's POD - not a Task-creation
// request. The dispatcher keys off this: a ticket resolves the Task, a mint
// (payload.newTask, or a legacy flat payload) calls BuildTaskFromQueuedEvent.
func IsAdmissionTicket(qe *tatarav1alpha1.QueuedEvent) bool {
	return qe.Spec.Payload.TaskRef != ""
}

// BuildTaskFromQueuedEvent reconstructs the Task the producer described, labelled
// with the QueuedEvent name (dispatcher completion mapping) and dedup key.
//
// Two MINT shapes are accepted: the B.7 blueprint (payload.newTask), and the
// legacy flat payload the sweep/webhook still produce. A payload carrying
// payload.taskRef is NOT a mint (see IsAdmissionTicket) and is refused: minting
// from it would create a SECOND Task alongside the one the ticket names.
//
// When payload.Name is empty the Task is GENERATENAME-minted, from
// payload.GenerateName+qe.Name+"-" (issue #443). It used to be NAMED
// payload.GenerateName+qe.Name, which is a constant for a constant dedupKey
// ("brainstorm-<project>", "refine-<project>"), so every cycle of a
// project-scoped kind reused ONE Task name and therefore ONE agent branch
// (agent.TaskBranch falls back to "tatara/task-<task-name>"): each cycle
// collided with the previous cycle's abandoned branch.
//
// The Task carries LabelMintedBy (the minting QueuedEvent's UID) so the
// dispatcher can still find the Task an event already minted - see
// LabelMintedBy: without that lookup a GenerateName mint has no idempotency
// primitive at all.
func BuildTaskFromQueuedEvent(qe *tatarav1alpha1.QueuedEvent, proj *tatarav1alpha1.Project, scheme *runtime.Scheme) (*tatarav1alpha1.Task, error) {
	p := qe.Spec.Payload
	if p.TaskRef != "" {
		return nil, fmt.Errorf("build task: queuedevent %s admits existing task %q; it mints nothing", qe.Name, p.TaskRef)
	}
	labels := map[string]string{}
	for k, v := range p.Labels {
		labels[k] = v
	}
	om := metav1.ObjectMeta{Namespace: qe.Namespace, Annotations: p.Annotations}
	spec := tatarav1alpha1.TaskSpec{
		ProjectRef:    proj.Name,
		RepositoryRef: p.RepositoryRef,
		Goal:          tatarav1alpha1.TruncateUTF8(p.Goal, tatarav1alpha1.GoalMaxBytes),
		Kind:          p.Kind,
		DedupKey:      p.DedupKey,
		GroupKey:      p.GroupKey,
		Source:        p.Source,
		// Empty is `new` (the TaskReconciler create edge applies that default),
		// so every existing producer keeps triaging unchanged.
		InitialState: p.InitialState,
	}
	if p.AlertRule != "" {
		spec.AlertRules = []string{p.AlertRule}
	}
	if p.Name != "" {
		om.Name = p.Name
	} else {
		om.GenerateName = p.GenerateName + qe.Name + "-"
	}
	if bp := p.NewTask; bp != nil {
		// The B.7 mint blueprint WINS over the flat legacy fields: a stage-driven
		// producer fills only the blueprint. bp.IssueKeys has NO home on TaskSpec
		// (the contract defines the field but no Task field to land it in); Issue
		// CRs are minted from the forge mirror in triaging, not from the payload.
		labels = map[string]string{}
		for k, v := range bp.Labels {
			labels[k] = v
		}
		if bp.Annotations != nil {
			om.Annotations = bp.Annotations
		}
		if bp.Name != "" {
			// A blueprint that names the Task keeps that exact name; one that does
			// not keeps the GenerateName mint set above (a bare `om.Name = bp.Name`
			// would blank BOTH and leave the Create nameless).
			om.Name = bp.Name
			om.GenerateName = ""
		}
		spec.RepositoryRef = bp.RepositoryRef
		spec.Goal = bp.Goal
		spec.Kind = bp.Kind
		spec.AlertRules = bp.AlertRules
	}
	for k, v := range MintStamp(qe) {
		labels[k] = v
	}
	om.Labels = labels
	task := &tatarav1alpha1.Task{ObjectMeta: om, Spec: spec}
	agent.StampPodName(task, proj.Name, p.PodRepo)
	if err := controllerutil.SetControllerReference(proj, task, scheme); err != nil {
		return nil, fmt.Errorf("build task: set ownerref: %w", err)
	}
	return task, nil
}
