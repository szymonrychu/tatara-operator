package obs

import "github.com/prometheus/client_golang/prometheus"

// queueMetrics holds the QueuedEvent-admission Prometheus collectors,
// embedded into OperatorMetrics.
type queueMetrics struct {
	queueAdmittedTotal         *prometheus.CounterVec
	queueDepth                 *prometheus.GaugeVec
	queueInflight              *prometheus.GaugeVec
	queueAge                   *prometheus.GaugeVec
	dispatcherBackstopEnqueued *prometheus.CounterVec
	admissionWakeTotal         *prometheus.CounterVec
}

// newQueueMetrics registers the queue collectors on reg and returns the bundle.
func newQueueMetrics(reg prometheus.Registerer) *queueMetrics {
	m := &queueMetrics{
		queueAdmittedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_queue_admitted_total",
			Help: "Total QueuedEvents admitted to a Task, by pool class and event kind.",
		}, []string{"class", "kind"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_queue_depth",
			Help: "Number of Queued (not yet admitted) QueuedEvents per project and pool class.",
		}, []string{"project", "class"}),
		queueInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_queue_inflight",
			Help: "Number of admitted in-flight QueuedEvents per project and pool class.",
		}, []string{"project", "class"}),
		// The project label (issue #418) matches queueDepth/queueInflight: without
		// it an alert-class backlog in project infrastructure or mtg pages as if
		// it were tatara's, because every project shares one bucket.
		queueAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_queue_age_seconds",
			Help: "Age of the OLDEST QueuedEvent per (project,class,priority,state) bucket (contract K.1).",
		}, []string{"project", "class", "priority", "state"}),
		// The leader-only admission backstop (issue #395): DispatcherReconciler
		// is otherwise purely watch-driven, so a QueuedEvent left Queued across a
		// rollout/leader-handoff window with no fresh watch trigger can stall
		// admission indefinitely. This counts backstop-driven (not watch-driven)
		// re-enqueues per project.
		//
		// Since issue #496 the sweep pushes ONE representative per pool with
		// pending work, not one per pending event, so this is bounded at 2 per
		// project per sweep and no longer scales with queue depth - it reads as
		// "the backstop swept this project", not "the queue is deep". Depth
		// belongs to operator_queue_depth.
		dispatcherBackstopEnqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_dispatcher_backstop_enqueued_total",
			Help: "Dispatcher re-enqueues by the leader-only admission backstop sweep, one per pool with pending work, by project.",
		}, []string{"project"}),
		// The EVENT-DRIVEN WAKE on a ticket reaching Admitted (task_ticket_watch.go).
		// Before this edge existed, the dispatcher's Task write woke TaskReconciler
		// while the ticket still read Queued, and the later Admitted flip woke
		// nobody: every admission cost a full admissionRequeue (5m) before the pod
		// spawned. A non-zero rate here is the proof the watch is live; a rate that
		// drops to zero while operator_queue_admitted_total keeps climbing means the
		// wake is lost again and admission is back on the 5m backstop.
		admissionWakeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_admission_wake_total",
			Help: "Task reconcile enqueues from an admission ticket reaching Admitted, by pool class and agent kind.",
		}, []string{"class", "agent_kind"}),
	}
	reg.MustRegister(
		m.queueAdmittedTotal,
		m.queueDepth,
		m.queueInflight,
		m.queueAge,
		m.dispatcherBackstopEnqueued,
		m.admissionWakeTotal,
	)
	return m
}

// QueueAdmitted increments operator_queue_admitted_total for the pool class and event kind.
func (m *queueMetrics) QueueAdmitted(class, kind string) {
	m.queueAdmittedTotal.WithLabelValues(class, kind).Inc()
}

// DispatcherBackstopEnqueued increments operator_dispatcher_backstop_enqueued_total
// for project: the leader-only backstop sweep (issue #395) pushed a
// GenericEvent for a pending QueuedEvent that no fresh watch trigger reached.
func (m *queueMetrics) DispatcherBackstopEnqueued(project string) {
	m.dispatcherBackstopEnqueued.WithLabelValues(project).Inc()
}

// DispatcherBackstopEnqueuedCounter returns the counter for project, for test
// assertions.
func (m *queueMetrics) DispatcherBackstopEnqueuedCounter(project string) prometheus.Counter {
	return m.dispatcherBackstopEnqueued.WithLabelValues(project)
}

// AdmissionWake increments operator_admission_wake_total: an admission ticket
// reached Admitted and woke its Task's reconcile, by pool class
// ("normal"|"alert") and the ticket's payload agent kind.
func (m *queueMetrics) AdmissionWake(class, agentKind string) {
	m.admissionWakeTotal.WithLabelValues(class, agentKind).Inc()
}

// AdmissionWakeCounter returns the counter for (class, agentKind), for test
// assertions.
func (m *queueMetrics) AdmissionWakeCounter(class, agentKind string) prometheus.Counter {
	return m.admissionWakeTotal.WithLabelValues(class, agentKind)
}

// SetQueueDepth sets operator_queue_depth for a project and pool class to n (Queued-state count).
func (m *queueMetrics) SetQueueDepth(project, class string, n int) {
	m.queueDepth.WithLabelValues(project, class).Set(float64(n))
}

// SetQueueInflight sets operator_queue_inflight for a project and pool class to n (in-flight admitted count).
func (m *queueMetrics) SetQueueInflight(project, class string, n int) {
	m.queueInflight.WithLabelValues(project, class).Set(float64(n))
}

// SeedQueueGauges creates the operator_queue_depth / operator_queue_inflight
// series for a project's two pools if they do not exist yet, leaving any value
// already set untouched (WithLabelValues creates at 0 and is idempotent).
//
// Both gauges are only ever written from DispatcherReconciler.Reconcile, so a
// project with no QueuedEvent activity has NO series at all rather than a series
// reading 0 - which is exactly the state a saturation alert most wants to read.
// Absence and zero are indistinguishable on a dashboard, no baseline is
// graphable across a restart, and a depth-based alert cannot tell "idle" from
// "the exporter went away" (issue #496: the tatara/normal series vanished for
// 5.4h after a rollout). Called per project from the ProjectReconciler gauge
// recompute, which every enrolled project reaches.
func (m *OperatorMetrics) SeedQueueGauges(project, class string) {
	if m == nil || m.queueDepth == nil || m.queueInflight == nil {
		return
	}
	m.queueDepth.WithLabelValues(project, class)
	m.queueInflight.WithLabelValues(project, class)
}

// ResetQueueAge clears operator_queue_age_seconds so a recompute pass leaves
// no stale bucket for a project/class/priority/state combination with no
// QueuedEvents left (contract M22). Nil-safe.
func (m *OperatorMetrics) ResetQueueAge() {
	if m == nil || m.queueAge == nil {
		return
	}
	m.queueAge.Reset()
}

// SetQueueAge sets operator_queue_age_seconds{project,class,priority,state} to
// ageSeconds, the age of the OLDEST QueuedEvent in that bucket. Nil-safe.
func (m *OperatorMetrics) SetQueueAge(project, class, priority, state string, ageSeconds float64) {
	if m == nil || m.queueAge == nil {
		return
	}
	m.queueAge.WithLabelValues(project, class, priority, state).Set(ageSeconds)
}

// QueueAgeGauge returns the operator_queue_age_seconds gauge for
// (project,class,priority,state) for test assertions.
func (m *OperatorMetrics) QueueAgeGauge(project, class, priority, state string) prometheus.Gauge {
	return m.queueAge.WithLabelValues(project, class, priority, state)
}
