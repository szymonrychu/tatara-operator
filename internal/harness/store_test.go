package harness

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newStoreClient(t *testing.T) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).Build()
}

func TestStore_GetMissingReturnsEmpty(t *testing.T) {
	s := &Store{Client: newStoreClient(t), Namespace: "tatara"}
	e, err := s.Get(context.Background(), "p", "LENS_CYCLE")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e.Value != "" || e.Version != "" {
		t.Fatalf("missing state must be empty, got %+v", e)
	}
}

func TestStore_CASCreatesThenGet(t *testing.T) {
	s := &Store{Client: newStoreClient(t), Namespace: "tatara"}
	ctx := context.Background()
	e, err := s.CAS(ctx, "p", "LENS_CYCLE", "failure-modes", "")
	if err != nil {
		t.Fatalf("CAS create: %v", err)
	}
	if e.Value != "failure-modes" || e.Version == "" {
		t.Fatalf("CAS create returned %+v", e)
	}
	got, err := s.Get(ctx, "p", "LENS_CYCLE")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Value != "failure-modes" || got.Version != e.Version {
		t.Fatalf("Get after create = %+v, want value=failure-modes version=%s", got, e.Version)
	}
}

func TestStore_CASStaleVersionConflicts(t *testing.T) {
	s := &Store{Client: newStoreClient(t), Namespace: "tatara"}
	ctx := context.Background()
	if _, err := s.CAS(ctx, "p", "LENS_CYCLE", "failure-modes", ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A stale (empty) version now that the CM exists must conflict.
	if _, err := s.CAS(ctx, "p", "LENS_CYCLE", "coupling", ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale empty version: want ErrConflict, got %v", err)
	}
	// A wrong non-empty version must conflict.
	if _, err := s.CAS(ctx, "p", "LENS_CYCLE", "coupling", "does-not-match"); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong version: want ErrConflict, got %v", err)
	}
}

func TestStore_CASCorrectVersionUpdatesAndBumps(t *testing.T) {
	s := &Store{Client: newStoreClient(t), Namespace: "tatara"}
	ctx := context.Background()
	first, err := s.CAS(ctx, "p", "LENS_CYCLE", "failure-modes", "")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	second, err := s.CAS(ctx, "p", "LENS_CYCLE", "coupling", first.Version)
	if err != nil {
		t.Fatalf("CAS update: %v", err)
	}
	if second.Value != "coupling" || second.Version == first.Version {
		t.Fatalf("CAS update = %+v, want value=coupling and a bumped version", second)
	}
}

func TestStore_MultiKeyOneConfigMap(t *testing.T) {
	s := &Store{Client: newStoreClient(t), Namespace: "tatara"}
	ctx := context.Background()
	a, err := s.CAS(ctx, "p", "LENS_CYCLE", "operability", "")
	if err != nil {
		t.Fatalf("key a: %v", err)
	}
	// Second key writes into the same CM; must use the bumped version.
	if _, err := s.CAS(ctx, "p", "ALERT_DEDUP", "abc123", a.Version); err != nil {
		t.Fatalf("key b: %v", err)
	}
	got, err := s.Get(ctx, "p", "LENS_CYCLE")
	if err != nil || got.Value != "operability" {
		t.Fatalf("Get key a after key b write = (%+v,%v)", got, err)
	}
}

func TestCMName_Sanitizes(t *testing.T) {
	if got := cmName("My/Project_Name"); got != "harness-state-my-project-name" {
		t.Fatalf("cmName = %q", got)
	}
}
