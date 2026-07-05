package accountusage

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMirrorRoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	m := &Mirror{Client: cl, Namespace: "tatara", Name: "tatara-account-usage"}
	reset := time.Now().Add(time.Hour).Truncate(time.Second)
	want := Snapshot{FiveHour: Window{Percent: 61, Reset: reset}, Weekly: Window{Percent: 40, Reset: reset}, Healthy: true}
	if err := m.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := m.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.FiveHour.Percent != 61 || !got.FiveHour.Reset.Equal(reset) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}
