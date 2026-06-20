package queue

import (
	"context"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newSeqClient(t *testing.T) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).Build()
}

func TestSeqSource_PerProjectMonotonic(t *testing.T) {
	c := newSeqClient(t)
	s := &SeqSource{Client: c, Namespace: "tatara"}
	ctx := context.Background()

	// Project A: 1,2,3 independent of project B: 1,2,3.
	for _, proj := range []string{"projA", "projB"} {
		for want := int64(1); want <= 3; want++ {
			got, err := s.Next(ctx, proj)
			if err != nil {
				t.Fatalf("Next(%s): %v", proj, err)
			}
			if got != want {
				t.Fatalf("Next(%s) = %d, want %d", proj, got, want)
			}
		}
	}
}

func TestSeqSource_PersistenceAcrossInstances(t *testing.T) {
	c := newSeqClient(t)
	ctx := context.Background()

	s1 := &SeqSource{Client: c, Namespace: "tatara"}
	if _, err := s1.Next(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if n, err := s1.Next(ctx, "p"); err != nil || n != 2 {
		t.Fatalf("s1.Next = (%d,%v), want (2,nil)", n, err)
	}

	// A fresh SeqSource over the same backing store continues the counter.
	s2 := &SeqSource{Client: c, Namespace: "tatara"}
	if n, err := s2.Next(ctx, "p"); err != nil || n != 3 {
		t.Fatalf("s2.Next = (%d,%v), want (3,nil)", n, err)
	}
}

func TestSeqSource_ConcurrentUniquePerProject(t *testing.T) {
	c := newSeqClient(t)
	s := &SeqSource{Client: c, Namespace: "tatara"}
	ctx := context.Background()

	const n = 50
	seen := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := s.Next(ctx, "concurrent")
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			seen[i] = v
		}(i)
	}
	wg.Wait()

	set := map[int64]bool{}
	for _, v := range seen {
		if set[v] {
			t.Fatalf("duplicate seq %d", v)
		}
		set[v] = true
	}
	if len(set) != n {
		t.Fatalf("got %d unique seqs, want %d", len(set), n)
	}
}

func TestSeqSource_CorruptCounter_ReturnsError(t *testing.T) {
	c := newSeqClient(t)
	ctx := context.Background()
	// Pre-seed the per-project CM with an unparseable value.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: seqConfigMapName("p"), Namespace: "tatara"},
		Data:       map[string]string{seqDataKey: "not-a-number"},
	}
	if err := c.Create(ctx, cm); err != nil {
		t.Fatalf("seed CM: %v", err)
	}
	s := &SeqSource{Client: c, Namespace: "tatara"}
	_, err := s.Next(ctx, "p")
	if err == nil {
		t.Fatal("expected error on corrupt counter, got nil")
	}
	if !strings.Contains(err.Error(), "corrupt seq counter") {
		t.Fatalf("expected corrupt-counter error, got %v", err)
	}
}

func TestSeqSource_EmptyValueAllocatesOne(t *testing.T) {
	c := newSeqClient(t)
	ctx := context.Background()
	// CM with an empty value is legitimately 0 -> allocate 1.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: seqConfigMapName("p"), Namespace: "tatara"},
		Data:       map[string]string{seqDataKey: ""},
	}
	if err := c.Create(ctx, cm); err != nil {
		t.Fatalf("seed CM: %v", err)
	}
	s := &SeqSource{Client: c, Namespace: "tatara"}
	n, err := s.Next(ctx, "p")
	if err != nil || n != 1 {
		t.Fatalf("Next = (%d,%v), want (1,nil)", n, err)
	}
}

func TestSeqConfigMapName_Sanitizes(t *testing.T) {
	got := seqConfigMapName("My/Project_Name")
	if got != "queue-seq-my-project-name" {
		t.Fatalf("seqConfigMapName = %q", got)
	}
}
