package own

import (
	"testing"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// task returns an in-memory Task with a stable UID derived from its name, so
// the unit tests can compare owner refs without a cluster.
func task(name string) *tataradevv1alpha1.Task {
	return &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "tatara",
			UID:       types.UID("uid-" + name),
		},
	}
}

func issue(name string) *tataradevv1alpha1.Issue {
	return &tataradevv1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
	}
}

// controllerRefs returns the names of every owner ref carrying controller=true.
func controllerRefs(refs []metav1.OwnerReference) []string {
	var out []string
	for _, r := range refs {
		if r.Controller != nil && *r.Controller {
			out = append(out, r.Name)
		}
	}
	return out
}

// TestAddPlainOwnerSetsBlockOwnerDeletion pins contract B.2 rule 2: a custom
// controller does NOT get blockOwnerDeletion for free, so the pointer must be
// non-nil and true, and the ref must NOT be a controller ref.
func TestAddPlainOwnerSetsBlockOwnerDeletion(t *testing.T) {
	iss := issue("iss-repo-1")
	tk := task("t-a")

	if added := AddPlainOwner(iss, tk); !added {
		t.Fatalf("AddPlainOwner returned false on a fresh object")
	}

	refs := iss.GetOwnerReferences()
	if len(refs) != 1 {
		t.Fatalf("owner refs = %d, want 1", len(refs))
	}
	r := refs[0]
	if r.APIVersion != tataradevv1alpha1.GroupVersion.String() {
		t.Errorf("apiVersion = %q, want %q", r.APIVersion, tataradevv1alpha1.GroupVersion.String())
	}
	if r.Kind != "Task" {
		t.Errorf("kind = %q, want Task", r.Kind)
	}
	if r.Name != "t-a" || r.UID != types.UID("uid-t-a") {
		t.Errorf("ref identity = %s/%s, want t-a/uid-t-a", r.Name, r.UID)
	}
	if r.BlockOwnerDeletion == nil {
		t.Fatalf("blockOwnerDeletion is nil; B.2 rule 2 requires it set EXPLICITLY")
	}
	if !*r.BlockOwnerDeletion {
		t.Errorf("blockOwnerDeletion = false, want true")
	}
	if r.Controller != nil && *r.Controller {
		t.Errorf("plain owner ref carries controller=true")
	}
}

// TestAddPlainOwnerIdempotent: called twice, appends once.
func TestAddPlainOwnerIdempotent(t *testing.T) {
	iss := issue("iss-repo-1")
	tk := task("t-a")

	if added := AddPlainOwner(iss, tk); !added {
		t.Fatalf("first AddPlainOwner returned false")
	}
	if added := AddPlainOwner(iss, tk); added {
		t.Errorf("second AddPlainOwner returned true; want false (no-op)")
	}
	if got := len(iss.GetOwnerReferences()); got != 1 {
		t.Fatalf("owner refs = %d after two AddPlainOwner calls, want 1", got)
	}
}

// TestAddPlainOwnerDoesNotDemoteController: adding a plain owner for a Task
// that is ALREADY the controller owner must not silently strip the flag.
func TestAddPlainOwnerDoesNotDemoteController(t *testing.T) {
	iss := issue("iss-repo-1")
	a, b := task("t-a"), task("t-b")
	AddPlainOwner(iss, a)
	AddPlainOwner(iss, b)
	if err := HandOverController(iss, nil, b); err != nil {
		t.Fatalf("HandOverController: %v", err)
	}

	if added := AddPlainOwner(iss, b); added {
		t.Errorf("AddPlainOwner re-added an existing controller owner")
	}
	if got := controllerRefs(iss.GetOwnerReferences()); len(got) != 1 || got[0] != "t-b" {
		t.Fatalf("controller refs = %v, want [t-b]", got)
	}
}

// TestHandOverControllerSwapsExactlyOne is the ATOMIC single-PUT swap: after
// the mutation there is EXACTLY one controller=true ref and it is `to`. The
// API server rejects two controller refs, so this cannot be two PUTs.
func TestHandOverControllerSwapsExactlyOne(t *testing.T) {
	iss := issue("iss-repo-1")
	from, to := task("t-member"), task("t-umbrella")
	AddPlainOwner(iss, from)
	if err := HandOverController(iss, nil, from); err != nil {
		t.Fatalf("seed HandOverController: %v", err)
	}
	AddPlainOwner(iss, to)

	if err := HandOverController(iss, from, to); err != nil {
		t.Fatalf("HandOverController: %v", err)
	}

	refs := iss.GetOwnerReferences()
	if len(refs) != 2 {
		t.Fatalf("owner refs = %d, want 2 (the swap must not drop an owner)", len(refs))
	}
	got := controllerRefs(refs)
	if len(got) != 1 || got[0] != "t-umbrella" {
		t.Fatalf("controller refs = %v, want exactly [t-umbrella]", got)
	}
	for _, r := range refs {
		if r.BlockOwnerDeletion == nil || !*r.BlockOwnerDeletion {
			t.Errorf("ref %q lost blockOwnerDeletion in the swap", r.Name)
		}
	}
	if name, ok := ControllerOwner(iss); !ok || name != "t-umbrella" {
		t.Errorf("ControllerOwner = (%q, %v), want (t-umbrella, true)", name, ok)
	}
}

// TestHandOverControllerIsIdempotent: a retried handover (the first Update
// landed, the caller crashed before observing it) is a no-op, not an error.
func TestHandOverControllerIsIdempotent(t *testing.T) {
	iss := issue("iss-repo-1")
	from, to := task("t-member"), task("t-umbrella")
	AddPlainOwner(iss, from)
	AddPlainOwner(iss, to)

	if err := HandOverController(iss, from, to); err != nil {
		t.Fatalf("first HandOverController: %v", err)
	}
	if err := HandOverController(iss, from, to); err != nil {
		t.Fatalf("second HandOverController: %v", err)
	}
	if got := controllerRefs(iss.GetOwnerReferences()); len(got) != 1 || got[0] != "t-umbrella" {
		t.Fatalf("controller refs = %v, want [t-umbrella]", got)
	}
}

// TestHandOverControllerRefusesUnknownTarget: `to` must ALREADY be an owner.
// Promoting a non-owner would create a controller ref with no plain ref behind
// it and skip the append half of the B.3 fold.
func TestHandOverControllerRefusesUnknownTarget(t *testing.T) {
	iss := issue("iss-repo-1")
	from, to := task("t-member"), task("t-umbrella")
	AddPlainOwner(iss, from)

	err := HandOverController(iss, from, to)
	if err == nil {
		t.Fatalf("HandOverController accepted a `to` that is not an owner")
	}
	if got := controllerRefs(iss.GetOwnerReferences()); len(got) != 0 {
		t.Errorf("refused handover still mutated the object: controller refs = %v", got)
	}
}

func TestControllerOwner(t *testing.T) {
	iss := issue("iss-repo-1")
	if _, ok := ControllerOwner(iss); ok {
		t.Fatalf("ControllerOwner reported ok on an unowned object")
	}
	a := task("t-a")
	AddPlainOwner(iss, a)
	if _, ok := ControllerOwner(iss); ok {
		t.Fatalf("ControllerOwner reported ok with only plain owners")
	}
	if err := HandOverController(iss, nil, a); err != nil {
		t.Fatalf("HandOverController: %v", err)
	}
	name, ok := ControllerOwner(iss)
	if !ok || name != "t-a" {
		t.Fatalf("ControllerOwner = (%q, %v), want (t-a, true)", name, ok)
	}
}

// TestOldestSurvivingOwner: owner refs are appended in creation order, so
// ownerRef ORDER is the age order. Owners absent from `live` are skipped.
func TestOldestSurvivingOwner(t *testing.T) {
	tests := []struct {
		name  string
		owner []string
		live  map[string]bool
		want  string
		wantK bool
	}{
		{
			name:  "first owner alive wins",
			owner: []string{"t-a", "t-b", "t-c"},
			live:  map[string]bool{"t-a": true, "t-b": true, "t-c": true},
			want:  "t-a",
			wantK: true,
		},
		{
			name:  "skips dead owners, picks oldest survivor",
			owner: []string{"t-a", "t-b", "t-c"},
			live:  map[string]bool{"t-b": true, "t-c": true},
			want:  "t-b",
			wantK: true,
		},
		{
			name:  "live map with an explicit false is not alive",
			owner: []string{"t-a", "t-b"},
			live:  map[string]bool{"t-a": false, "t-b": true},
			want:  "t-b",
			wantK: true,
		},
		{
			name:  "no survivors",
			owner: []string{"t-a", "t-b"},
			live:  map[string]bool{},
			want:  "",
			wantK: false,
		},
		{
			name:  "no owners",
			owner: nil,
			live:  map[string]bool{"t-a": true},
			want:  "",
			wantK: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			iss := issue("iss-repo-1")
			for _, n := range tc.owner {
				AddPlainOwner(iss, task(n))
			}
			got, ok := OldestSurvivingOwner(iss, tc.live)
			if got != tc.want || ok != tc.wantK {
				t.Fatalf("OldestSurvivingOwner = (%q, %v), want (%q, %v)", got, ok, tc.want, tc.wantK)
			}
		})
	}
}

// TestTaskOwnersIgnoresForeignRefs: a Project ownerRef (or any non-Task ref)
// is not a Task owner and must never be promoted to the controller flag.
func TestTaskOwnersIgnoresForeignRefs(t *testing.T) {
	iss := issue("iss-repo-1")
	iss.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: tataradevv1alpha1.GroupVersion.String(),
		Kind:       "Project",
		Name:       "p-1",
		UID:        types.UID("uid-p-1"),
	}})
	AddPlainOwner(iss, task("t-a"))

	if got, ok := OldestSurvivingOwner(iss, map[string]bool{"p-1": true, "t-a": true}); !ok || got != "t-a" {
		t.Fatalf("OldestSurvivingOwner = (%q, %v), want (t-a, true)", got, ok)
	}
}
