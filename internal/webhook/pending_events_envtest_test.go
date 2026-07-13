package webhook_test

// Task 18 / contract Section I: the two pendingEvents properties that need a
// REAL API server, not a fake client:
//
//   - "the Go-side drop-oldest cap never lets the API-server MaxItems 422
//     fire" - the fake client does not enforce CRD validation (MaxItems) at
//     all, so this property is vacuously true against it.
//   - "a webhook arriving between render and clear is NOT dropped" - this
//     must be a REAL CONCURRENT test (goroutines + envtest client), not a
//     sequential simulation, per the plan. A blind nil-assign is the
//     2026-07-11 "blind Status.WorkItems clobber" in new clothes.
//
// This file owns the package's one TestMain (shared by every *_test.go file
// under internal/webhook, whichever of the two co-located test packages -
// webhook and webhook_test - it lives in).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

var (
	peTestEnv   *envtest.Environment
	peCfg       *rest.Config
	peK8sClient client.Client
)

const (
	peEnvtestStartAttempts = 3
	peEnvtestStartBackoff  = 3 * time.Second
)

func TestMain(m *testing.M) {
	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	code := func() int {
		var err error
		for attempt := 1; ; attempt++ {
			peTestEnv = &envtest.Environment{
				CRDDirectoryPaths:     []string{filepath.Join("..", "..", "charts", "tatara-operator", "crd-bases")},
				ErrorIfCRDPathMissing: true,
			}
			if peCfg, err = peTestEnv.Start(); err == nil {
				break
			}
			_ = peTestEnv.Stop()
			if attempt >= peEnvtestStartAttempts {
				panic("start envtest: " + err.Error())
			}
			time.Sleep(peEnvtestStartBackoff)
		}
		defer func() { _ = peTestEnv.Stop() }()

		if err := tatarav1.AddToScheme(scheme.Scheme); err != nil {
			panic("add scheme: " + err.Error())
		}
		peK8sClient, err = client.New(peCfg, client.Options{Scheme: scheme.Scheme})
		if err != nil {
			panic("new client: " + err.Error())
		}

		nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
		if err := peK8sClient.Create(context.Background(), nsObj); err != nil && !apierrors.IsAlreadyExists(err) {
			panic("create namespace: " + err.Error())
		}

		return m.Run()
	}()
	os.Exit(code)
}

func mkPETask(t *testing.T, name string) *tatarav1.Task {
	t.Helper()
	tk := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       tatarav1.TaskSpec{ProjectRef: "p-test", Goal: "pending events envtest task"},
	}
	if err := peK8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create Task %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = peK8sClient.Delete(context.Background(), tk)
	})
	return tk
}

func getPETaskEnvtest(t *testing.T, name string) *tatarav1.Task {
	t.Helper()
	var tk tatarav1.Task
	if err := peK8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &tk); err != nil {
		t.Fatalf("get Task %s: %v", name, err)
	}
	return &tk
}

// peEv builds a distinct TaskEvent for index i. The eventKey the clear step
// diffs on is (Kind, Repo, Number, At) with At truncated to SECOND precision
// (the API server's own round-trip precision) - so Number, not a timestamp
// offset, is what must vary across a batch built within the same second.
func peEv(i int) tatarav1.TaskEvent {
	return tatarav1.TaskEvent{
		At:   metav1.NewTime(time.Now().UTC().Truncate(time.Second)),
		Kind: "issue_comment", Repo: "r1", Number: i + 1,
		Author: fmt.Sprintf("user%d", i), Body: fmt.Sprintf("comment %d", i),
	}
}

// TestAppendTaskEvent_CapNeverTripsBackstop422 proves two things against a
// REAL API server: (1) the CRD's MaxItems=25 backstop is real - a raw write
// of 26 items 422s; (2) AppendTaskEvent's Go-side drop-oldest cap at 20 never
// lets a caller reach that backstop, across many more than 20 appends.
func TestAppendTaskEvent_CapNeverTripsBackstop422(t *testing.T) {
	ctx := context.Background()

	// Half 1: prove the backstop is real, not vacuous.
	raw := mkPETask(t, "pe-cap-raw")
	fresh := getPETaskEnvtest(t, raw.Name)
	for i := 0; i < 26; i++ {
		fresh.Status.PendingEvents = append(fresh.Status.PendingEvents, peEv(i))
	}
	err := peK8sClient.Status().Update(ctx, fresh)
	if err == nil {
		t.Fatal("API server accepted 26 pendingEvents; MaxItems=25 backstop is not enforced - the whole premise of the Go-side cap is moot")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("26-item write error = %v, want 422 Invalid", err)
	}

	// Half 2: AppendTaskEvent, called far past 20 times, never trips it.
	task := mkPETask(t, "pe-cap-capped")
	for i := 0; i < 30; i++ {
		if err := webhook.AppendTaskEvent(ctx, peK8sClient, task, peEv(i)); err != nil {
			t.Fatalf("AppendTaskEvent call %d: %v (the Go-side cap must keep every write under the API-server MaxItems=25 backstop)", i, err)
		}
	}
	got := getPETaskEnvtest(t, task.Name)
	if len(got.Status.PendingEvents) != 20 {
		t.Fatalf("pendingEvents = %d, want 20 (Go-side cap, drop-oldest)", len(got.Status.PendingEvents))
	}
	// Drop-oldest: the survivors are the LAST 20 appended (events 10..29).
	if got.Status.PendingEvents[0].Author != "user10" {
		t.Fatalf("oldest survivor author = %q, want user10 (events 0-9 must have been dropped, oldest-first)", got.Status.PendingEvents[0].Author)
	}
	if got.Status.PendingEvents[19].Author != "user29" {
		t.Fatalf("newest survivor author = %q, want user29", got.Status.PendingEvents[19].Author)
	}
}

// blockingStatusClient wraps a real client.Client and lets a test synchronize
// on the FIRST Status().Update call it makes for a Task - the exact moment
// ClearDeliveredEvents has already Get'd the object and is about to write.
type blockingStatusClient struct {
	client.Client
	once          sync.Once
	onFirstUpdate func()
}

func (b *blockingStatusClient) Status() client.SubResourceWriter {
	return &blockingStatusWriter{SubResourceWriter: b.Client.Status(), parent: b}
}

type blockingStatusWriter struct {
	client.SubResourceWriter
	parent *blockingStatusClient
}

func (w *blockingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if _, ok := obj.(*tatarav1.Task); ok {
		w.parent.once.Do(func() {
			if w.parent.onFirstUpdate != nil {
				w.parent.onFirstUpdate()
			}
		})
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

// TestClearDeliveredEvents_ConcurrentAppendSurvives is THE race test: a
// webhook event (AppendTaskEvent) arrives on the REAL API server WHILE
// ClearDeliveredEvents' RetryOnConflict is between its Get and its first
// Update attempt. A blind `PendingEvents = nil` would drop the concurrently-
// appended event; the set-difference-inside-RetryOnConflict implementation
// must not.
func TestClearDeliveredEvents_ConcurrentAppendSurvives(t *testing.T) {
	ctx := context.Background()
	task := mkPETask(t, "pe-race")

	// Seed two "already delivered" events - what a render just bundled up.
	delivered := []tatarav1.TaskEvent{peEv(1), peEv(2)}
	for _, ev := range delivered {
		if err := webhook.AppendTaskEvent(ctx, peK8sClient, task, ev); err != nil {
			t.Fatalf("seed delivered event: %v", err)
		}
	}

	ready := make(chan struct{})
	proceed := make(chan struct{})
	blocking := &blockingStatusClient{
		Client: peK8sClient,
		onFirstUpdate: func() {
			close(ready)
			<-proceed
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var clearErr error
	go func() {
		defer wg.Done()
		clearTask := &tatarav1.Task{ObjectMeta: metav1.ObjectMeta{Name: task.Name, Namespace: ns}}
		clearErr = webhook.ClearDeliveredEvents(ctx, blocking, clearTask, delivered)
	}()

	// Wait until Clear has Get'd the task and is blocked right before its
	// first Update - the exact "between render and clear" window.
	<-ready

	// A NEW webhook event arrives concurrently, on the REAL unblocked client.
	arriving := peEv(3)
	if err := webhook.AppendTaskEvent(ctx, peK8sClient, task, arriving); err != nil {
		t.Fatalf("concurrent AppendTaskEvent: %v", err)
	}

	// Let Clear's first Update proceed - it now races against a resourceVersion
	// bump from the concurrent append and MUST conflict-retry.
	close(proceed)
	wg.Wait()

	if clearErr != nil {
		t.Fatalf("ClearDeliveredEvents: %v", clearErr)
	}

	final := getPETaskEnvtest(t, task.Name)
	foundArriving := false
	for _, ev := range final.Status.PendingEvents {
		if ev.Author == arriving.Author && ev.Body == arriving.Body {
			foundArriving = true
		}
		if ev.Author == delivered[0].Author && ev.Body == delivered[0].Body {
			t.Fatalf("a DELIVERED event survived the clear: %+v", ev)
		}
		if ev.Author == delivered[1].Author && ev.Body == delivered[1].Body {
			t.Fatalf("a DELIVERED event survived the clear: %+v", ev)
		}
	}
	if !foundArriving {
		t.Fatalf("the event that arrived BETWEEN render and clear was DROPPED - pendingEvents = %+v", final.Status.PendingEvents)
	}
}
