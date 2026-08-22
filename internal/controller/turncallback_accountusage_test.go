package controller

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	"github.com/szymonrychu/tatara-operator/internal/budget"
)

// mkAccountUsageTask creates a minimal Task fixture for the account-usage
// writes. It does not reuse mkTask because these tests need the object back to
// seed status on it and to key the reload by.
func mkAccountUsageTask(t *testing.T, ctx context.Context, name string) *tatarav1alpha1.Task {
	t.Helper()
	tk := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}}
	tk.Spec.ProjectRef = "acctusage-proj"
	tk.Spec.RepositoryRef = "acctusage-repo"
	tk.Spec.Goal = "ship the feature"
	mustCreate(t, ctx, tk)
	return tk
}

func TestRecordAccountUsage(t *testing.T) {
	ctx := context.Background()
	t0 := metav1.NewTime(time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	tests := []struct {
		name     string
		existing *tatarav1alpha1.TaskAccountUsage
		in       *turnAccountUsage
		wantFive int32
		wantSet  bool
	}{
		{name: "nil payload writes nothing", in: nil, wantSet: false},
		{
			name:    "zero observedAt writes nothing",
			in:      &turnAccountUsage{FiveHourPercent: 41.5},
			wantSet: false,
		},
		{
			name:     "first snapshot is written",
			in:       &turnAccountUsage{ObservedAt: t1.Time, FiveHourPercent: 41.5, WeeklyPercent: 73},
			wantFive: 42, // math.Round(41.5)
			wantSet:  true,
		},
		{
			name:     "newer snapshot replaces older",
			existing: &tatarav1alpha1.TaskAccountUsage{ObservedAt: t0, FiveHourPercent: 10},
			in:       &turnAccountUsage{ObservedAt: t1.Time, FiveHourPercent: 62},
			wantFive: 62,
			wantSet:  true,
		},
		{
			name:     "older snapshot is ignored",
			existing: &tatarav1alpha1.TaskAccountUsage{ObservedAt: t1, FiveHourPercent: 62},
			in:       &turnAccountUsage{ObservedAt: t0.Time, FiveHourPercent: 10},
			wantFive: 62,
			wantSet:  true,
		},
		{
			name:     "out of range percent is clamped",
			in:       &turnAccountUsage{ObservedAt: t1.Time, FiveHourPercent: 140},
			wantFive: 100,
			wantSet:  true,
		},
		{
			name:     "negative percent is clamped",
			in:       &turnAccountUsage{ObservedAt: t1.Time, FiveHourPercent: -5},
			wantFive: 0,
			wantSet:  true,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mkAccountUsageTask(t, ctx, "acctusage-"+strconv.Itoa(i))
			if tc.existing != nil {
				task.Status.AccountUsage = tc.existing
				mustStatusUpdate(t, ctx, task)
			}
			s := &CallbackServer{Client: k8sClient, Namespace: testNS}
			if err := s.recordAccountUsage(ctx, task, tc.in); err != nil {
				t.Fatalf("recordAccountUsage: %v", err)
			}

			fresh := &tatarav1alpha1.Task{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				t.Fatalf("get task: %v", err)
			}
			if !tc.wantSet {
				if fresh.Status.AccountUsage != nil {
					t.Fatalf("status.accountUsage = %+v, want nil", fresh.Status.AccountUsage)
				}
				return
			}
			if fresh.Status.AccountUsage == nil {
				t.Fatal("status.accountUsage = nil, want a snapshot")
			}
			if got := fresh.Status.AccountUsage.FiveHourPercent; got != tc.wantFive {
				t.Fatalf("fiveHourPercent = %d, want %d", got, tc.wantFive)
			}
		})
	}
}

// A snapshot with a known reset must land as a *metav1.Time; an unknown (zero)
// reset must stay nil rather than becoming the unix epoch, which the gate reads
// as an expired window.
func TestRecordAccountUsageResets(t *testing.T) {
	ctx := context.Background()
	obs := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	task := mkAccountUsageTask(t, ctx, "acctusage-resets")
	s := &CallbackServer{Client: k8sClient, Namespace: testNS}
	if err := s.recordAccountUsage(ctx, task, &turnAccountUsage{
		ObservedAt:      obs,
		FiveHourPercent: 20,
		// Only the five-hour window reset is known; the weekly one is absent,
		// which the spike confirms is a normal, per-window-optional payload.
		FiveHourResetUnix: 1755864000,
		WeeklyPercent:     30,
	}); err != nil {
		t.Fatalf("recordAccountUsage: %v", err)
	}
	fresh := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	au := fresh.Status.AccountUsage
	if au == nil || au.FiveHourReset == nil {
		t.Fatalf("status.accountUsage.fiveHourReset = nil, want %d", 1755864000)
	}
	if got := au.FiveHourReset.Unix(); got != 1755864000 {
		t.Fatalf("fiveHourReset = %d, want 1755864000", got)
	}
	if au.WeeklyReset != nil {
		t.Fatalf("weeklyReset = %v, want nil for an absent reset", au.WeeklyReset)
	}
	if !au.ObservedAt.Time.Equal(obs) {
		t.Fatalf("observedAt = %v, want %v", au.ObservedAt.Time, obs)
	}
}

// A terminal Task never takes a further status write: its snapshot is history
// and the fleet store is fed by whichever Task is still running.
func TestRecordAccountUsageSkipsTerminalTask(t *testing.T) {
	ctx := context.Background()
	task := mkAccountUsageTask(t, ctx, "acctusage-terminal")
	task.Status.State = tatarav1alpha1.StateDone
	mustStatusUpdate(t, ctx, task)

	s := &CallbackServer{Client: k8sClient, Namespace: testNS}
	if err := s.recordAccountUsage(ctx, task, &turnAccountUsage{
		ObservedAt: time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC), FiveHourPercent: 55,
	}); err != nil {
		t.Fatalf("recordAccountUsage: %v", err)
	}
	fresh := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.AccountUsage != nil {
		t.Fatalf("status.accountUsage = %+v, want nil on a terminal Task", fresh.Status.AccountUsage)
	}
}

// A payload from an OLD wrapper (no accountUsage key) must decode cleanly and
// leave the status untouched: the forward-compat half of the skew invariant.
func TestTurnCompletePayloadWithoutAccountUsage(t *testing.T) {
	var p turnCompletePayload
	if err := json.Unmarshal([]byte(`{"turnId":"t1","state":"complete"}`), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.AccountUsage != nil {
		t.Fatalf("accountUsage = %+v, want nil", p.AccountUsage)
	}
}

// A payload carrying the RETIRED rateLimit key must still be ignored.
func TestTurnCompletePayloadIgnoresRetiredRateLimit(t *testing.T) {
	var p turnCompletePayload
	if err := json.Unmarshal([]byte(
		`{"turnId":"t1","state":"complete","rateLimit":{"fiveHourPercent":90}}`), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.AccountUsage != nil {
		t.Fatalf("accountUsage = %+v, want nil", p.AccountUsage)
	}
}

// The live accountUsage key decodes with the spike's wire encoding: 0-100
// float percents and int64 unix-second resets.
func TestTurnCompletePayloadDecodesAccountUsage(t *testing.T) {
	var p turnCompletePayload
	body := `{"turnId":"t1","state":"complete","accountUsage":{"observedAt":"2026-08-22T10:00:00Z",` +
		`"fiveHourPercent":41.5,"fiveHourResetUnix":1755864000,"weeklyPercent":73,"weeklyResetUnix":1756296000}}`
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.AccountUsage == nil {
		t.Fatal("accountUsage = nil, want a snapshot")
	}
	if p.AccountUsage.FiveHourPercent != 41.5 {
		t.Fatalf("fiveHourPercent = %v, want 41.5", p.AccountUsage.FiveHourPercent)
	}
	if p.AccountUsage.WeeklyResetUnix != 1756296000 {
		t.Fatalf("weeklyResetUnix = %d, want 1756296000", p.AccountUsage.WeeklyResetUnix)
	}
	if !p.AccountUsage.ObservedAt.Equal(time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("observedAt = %v", p.AccountUsage.ObservedAt)
	}
}

// TestGateInertWithoutAnySnapshot is the NEW-OPERATOR/OLD-WRAPPER half of the
// deploy-skew invariant: an operator carrying the whole feed, running against a
// fleet that reports nothing, must decide exactly as the pre-feed operator did.
// Nothing held, no window governing, no usage claimed.
//
// The empty store is the real pre-feed shape - not a hand-built zero
// Subscription - because the projection in accountusage.Snapshot.Subscription
// is the dispatcher's only view of the fleet snapshot, so the inertness has to
// hold through it.
func TestGateInertWithoutAnySnapshot(t *testing.T) {
	cfg := budget.Config{
		Enabled:        true,
		Mode:           budget.ModeClaudeSubscription,
		MaxSnapshotAge: budget.DefaultMaxSnapshotAge,
	}
	store := &accountusage.Store{}

	d := budget.Evaluate(cfg, budget.WindowState{}, store.Get().Subscription(), time.Now())

	require.False(t, d.ProactiveBlocked, "an empty fleet snapshot must not hold proactive work")
	require.False(t, d.EmergencyBlocked, "an empty fleet snapshot must not hold incident work")
	require.Zero(t, d.UsedPercent)
	// A window with no reset time is not active, so none may govern. If this
	// ever names a window, a zero-valued snapshot has started reading as "0%
	// used" - the exact silent failure the whole feed exists to fix.
	require.Empty(t, string(d.GoverningWindow))
}
