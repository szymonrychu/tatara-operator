package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func TestAccountUsageGaugeRunnableNeedLeaderElection(t *testing.T) {
	r := accountUsageGaugeRunnable{}
	if !r.NeedLeaderElection() {
		t.Fatal("accountUsageGaugeRunnable.NeedLeaderElection() = false, want true: " +
			"three replicas racing to Set the same gauge produce an arbitrary flapping series, " +
			"and the store it seeds is only read by leader-only code")
	}
}

func accountUsageScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := tataradevv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

// The seed is fleet-wide by construction: the newest observedAt across EVERY
// Task in the namespace wins, no matter which Project it belongs to. A Task
// carrying no snapshot, or one whose observedAt never got set, contributes
// nothing - it must never seed a zero-valued snapshot, which the gate would
// read as "0% used".
func TestSeedStoreFromTasks(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Hour))
	older := &tataradevv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}
	older.Status.AccountUsage = &tataradevv1alpha1.TaskAccountUsage{ObservedAt: t0, FiveHourPercent: 11}
	newer := &tataradevv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}
	newer.Status.AccountUsage = &tataradevv1alpha1.TaskAccountUsage{ObservedAt: t1, FiveHourPercent: 77}
	none := &tataradevv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}}
	zero := &tataradevv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "d", Namespace: "ns"}}
	zero.Status.AccountUsage = &tataradevv1alpha1.TaskAccountUsage{FiveHourPercent: 99}
	other := &tataradevv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "elsewhere"}}
	other.Status.AccountUsage = &tataradevv1alpha1.TaskAccountUsage{ObservedAt: metav1.NewTime(t1.Add(time.Hour)), FiveHourPercent: 5}
	c := fake.NewClientBuilder().WithScheme(accountUsageScheme(t)).
		WithObjects(older, newer, none, zero, other).Build()

	store := &accountusage.Store{}
	if err := seedStoreFromTasks(context.Background(), c, store, "ns"); err != nil {
		t.Fatalf("seedStoreFromTasks: %v", err)
	}
	got := store.Get()
	if got.FiveHour.Percent != 77 {
		t.Fatalf("FiveHour.Percent = %v, want 77", got.FiveHour.Percent)
	}
	if got.Source != accountusage.SourceWrapper {
		t.Fatalf("Source = %q, want %q", got.Source, accountusage.SourceWrapper)
	}
	if !got.UpdatedAt.Equal(t1.UTC()) {
		t.Fatalf("UpdatedAt = %v, want %v", got.UpdatedAt, t1.UTC())
	}
}

// A namespace with no snapshots anywhere leaves the store empty rather than
// zero-valued, so the gate keeps reading "no data" and not "0% used".
func TestSeedStoreFromTasksLeavesAnEmptyFleetEmpty(t *testing.T) {
	none := &tataradevv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}
	c := fake.NewClientBuilder().WithScheme(accountUsageScheme(t)).WithObjects(none).Build()
	store := &accountusage.Store{}
	if err := seedStoreFromTasks(context.Background(), c, store, "ns"); err != nil {
		t.Fatalf("seedStoreFromTasks: %v", err)
	}
	if got := store.Get(); !got.UpdatedAt.IsZero() {
		t.Fatalf("store = %+v, want empty", got)
	}
}

// A failed List must not stop the manager: Start returning an error tears the
// whole control plane down, and a seed is not worth that trade.
func TestAccountUsageGaugeRunnableStartSwallowsSeedFailure(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(accountUsageScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("injected list failure")
		},
	}).Build()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := accountUsageGaugeRunnable{reader: c, store: &accountusage.Store{}, namespace: "ns", interval: time.Minute}
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start must swallow a seed failure, got %v", err)
	}
}

// Start seeds BEFORE its first emit, so a leader restart publishes the restored
// fleet state immediately instead of a 0% reading that fully opens the gate.
func TestAccountUsageGaugeRunnableSeedsThenEmits(t *testing.T) {
	obsAt := metav1.NewTime(time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC))
	reset := metav1.NewTime(obsAt.Add(3 * time.Hour))
	task := &tataradevv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}
	task.Status.AccountUsage = &tataradevv1alpha1.TaskAccountUsage{
		ObservedAt: obsAt, FiveHourPercent: 63, WeeklyPercent: 41, FiveHourReset: &reset,
	}
	c := fake.NewClientBuilder().WithScheme(accountUsageScheme(t)).WithObjects(task).Build()

	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	store := &accountusage.Store{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := accountUsageGaugeRunnable{reader: c, store: store, metrics: metrics, namespace: "ns", interval: time.Minute}
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := store.Get().FiveHour.Percent; got != 63 {
		t.Fatalf("store not seeded: FiveHour.Percent = %v, want 63", got)
	}
	if got := testutil.ToFloat64(utilGauge(t, reg, "five_hour")); got != 63 {
		t.Fatalf("five_hour utilization = %v, want 63", got)
	}
	if got := testutil.ToFloat64(utilGauge(t, reg, "seven_day")); got != 41 {
		t.Fatalf("seven_day utilization = %v, want 41", got)
	}
}

// An empty store must publish NO utilization series at all. Publishing 0 would
// be indistinguishable from a genuinely idle account and would make the gate's
// inertness invisible all over again.
func TestAccountUsageGaugeRunnableEmitsNothingWhenEmpty(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(accountUsageScheme(t)).Build()
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := accountUsageGaugeRunnable{reader: c, store: &accountusage.Store{}, metrics: metrics, namespace: "ns", interval: time.Minute}
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() == "tatara_account_usage_utilization" && len(f.GetMetric()) > 0 {
			t.Fatalf("utilization series published for an empty store: %v", f.GetMetric())
		}
	}
}

// utilGauge finds the utilization child for one window in reg.
func utilGauge(t *testing.T, reg *prometheus.Registry, window string) prometheus.Gauge {
	t.Helper()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "probe"})
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != "tatara_account_usage_utilization" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "window" && l.GetValue() == window {
					g.Set(m.GetGauge().GetValue())
					return g
				}
			}
		}
	}
	t.Fatalf("no tatara_account_usage_utilization series for window %q", window)
	return nil
}

// gaugeValue reads one gauge child out of reg by metric name and exact label
// set. ok=false means the series is absent, which several of these cases assert
// on directly.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			match := true
			for k, v := range labels {
				if got[k] != v {
					match = false
					break
				}
			}
			if match {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func claudeSubscriptionDefaults() budget.Config {
	return budget.Config{
		Enabled:        true,
		Mode:           budget.ModeClaudeSubscription,
		MaxSnapshotAge: budget.DefaultMaxSnapshotAge,
	}
}

// A fresh snapshot under an enabled claudeSubscription gate publishes
// gate_ready=1 and the snapshot's age under its own feed source. Age is what
// tells an operator WHICH feed went quiet and how long ago.
func TestAccountUsageGaugeRunnableEmitsGateReadyAndAge(t *testing.T) {
	reg := prometheus.NewRegistry()
	store := &accountusage.Store{}
	store.Set(accountusage.Snapshot{
		FiveHour:  accountusage.Window{Percent: 63},
		UpdatedAt: time.Now().Add(-42 * time.Second),
		Source:    accountusage.SourceWrapper,
	})
	r := accountUsageGaugeRunnable{store: store, metrics: obs.NewOperatorMetrics(reg), defaults: claudeSubscriptionDefaults()}
	r.emit()

	ready, ok := gaugeValue(t, reg, "tatara_account_usage_gate_ready", map[string]string{})
	if !ok || ready != 1 {
		t.Fatalf("gate_ready = %v (present=%v), want 1", ready, ok)
	}
	age, ok := gaugeValue(t, reg, "tatara_account_usage_snapshot_age_seconds", map[string]string{"source": accountusage.SourceWrapper})
	if !ok {
		t.Fatal("no tatara_account_usage_snapshot_age_seconds{source=\"wrapper\"} series")
	}
	if age < 42 || age > 60 {
		t.Fatalf("snapshot age = %v, want ~42s", age)
	}
}

// THE OUTAGE CASE. An enabled gate with no snapshot at all must publish
// gate_ready=0: that is the only signal that the gate is admitting everything
// while believing it is gating. It must still publish no age series, because
// there is no snapshot whose age could be reported.
func TestAccountUsageGaugeRunnableEmitsGateNotReadyWhenStoreEmpty(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := accountUsageGaugeRunnable{store: &accountusage.Store{}, metrics: obs.NewOperatorMetrics(reg), defaults: claudeSubscriptionDefaults()}
	r.emit()

	ready, ok := gaugeValue(t, reg, "tatara_account_usage_gate_ready", map[string]string{})
	if !ok || ready != 0 {
		t.Fatalf("gate_ready = %v (present=%v), want 0", ready, ok)
	}
	if _, ok := gaugeValue(t, reg, "tatara_account_usage_snapshot_age_seconds", map[string]string{"source": accountusage.SourceWrapper}); ok {
		t.Fatal("snapshot age published for an empty store")
	}
}

// Past MaxSnapshotAge the gate FAILS OPEN, so gate_ready must go 0 even though
// a snapshot is still held. The age series stays, because "how stale" is the
// diagnostic.
func TestAccountUsageGaugeRunnableGateNotReadyWhenSnapshotStale(t *testing.T) {
	reg := prometheus.NewRegistry()
	store := &accountusage.Store{}
	store.Set(accountusage.Snapshot{
		FiveHour:  accountusage.Window{Percent: 63},
		UpdatedAt: time.Now().Add(-2 * budget.DefaultMaxSnapshotAge),
		Source:    accountusage.SourceWrapper,
	})
	r := accountUsageGaugeRunnable{store: store, metrics: obs.NewOperatorMetrics(reg), defaults: claudeSubscriptionDefaults()}
	r.emit()

	ready, ok := gaugeValue(t, reg, "tatara_account_usage_gate_ready", map[string]string{})
	if !ok || ready != 0 {
		t.Fatalf("gate_ready = %v (present=%v), want 0 past MaxSnapshotAge", ready, ok)
	}
	if _, ok := gaugeValue(t, reg, "tatara_account_usage_snapshot_age_seconds", map[string]string{"source": accountusage.SourceWrapper}); !ok {
		t.Fatal("a stale snapshot must still report its age")
	}
}

// An operator whose gate is off, or is in customWindow mode, must publish NO
// gate_ready series at all: TataraAccountUsageFeedDead alerts on == 0, so a
// rendered 0 would page every cluster that never enabled the gate.
func TestAccountUsageGaugeRunnableNoGateReadySeriesWhenGateOff(t *testing.T) {
	cases := []struct {
		name     string
		defaults budget.Config
	}{
		{"gate disabled", budget.Config{Mode: budget.ModeClaudeSubscription}},
		{"custom window mode", budget.Config{Enabled: true, Mode: budget.ModeCustomWindow}},
		{"zero defaults", budget.Config{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			store := &accountusage.Store{}
			store.Set(accountusage.Snapshot{UpdatedAt: time.Now(), Source: accountusage.SourceWrapper})
			r := accountUsageGaugeRunnable{store: store, metrics: obs.NewOperatorMetrics(reg), defaults: tc.defaults}
			r.emit()
			if _, ok := gaugeValue(t, reg, "tatara_account_usage_gate_ready", map[string]string{}); ok {
				t.Fatal("gate_ready series published while the claudeSubscription gate is off")
			}
		})
	}
}
