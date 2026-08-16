package queue

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// THE KEY IS DERIVED FROM WHAT BOTH PRODUCERS HAVE IN HAND. The webhook has a
// matched Repository CR and the delivery's number; the sweep has the same
// Repository CR and the listing row's number. Keying on the Repository CR NAME
// rather than the forge slug is what makes the two collide on AlreadyExists
// instead of double-enqueueing.
func TestAdoptUpgradeDedupKey(t *testing.T) {
	if got := AdoptUpgradeDedupKey("charts", 1026); got != "adopt-upgrade|charts|1026" {
		t.Fatalf("AdoptUpgradeDedupKey = %q", got)
	}
	if AdoptUpgradeDedupKey("charts", 1026) == AdoptUpgradeDedupKey("charts", 1027) {
		t.Fatal("two merge requests in one repo must not share a dedup key")
	}
	if AdoptUpgradeDedupKey("charts", 1) == AdoptUpgradeDedupKey("containers", 1) {
		t.Fatal("two repos must not share a dedup key for the same number")
	}
}

func TestIsAdoptedUpgradeMint(t *testing.T) {
	plain := &tatarav1alpha1.QueuedEvent{}
	if IsAdoptedUpgradeMint(plain) {
		t.Fatal("a plain mint is not an adoption")
	}
	adopted := &tatarav1alpha1.QueuedEvent{Spec: tatarav1alpha1.QueuedEventSpec{
		Payload: tatarav1alpha1.QueuedEventPayload{
			AdoptedUpgrade: &tatarav1alpha1.AdoptedUpgradeRef{Number: 1},
		}}}
	if !IsAdoptedUpgradeMint(adopted) {
		t.Fatal("an adoptedUpgrade payload IS an adoption")
	}
}

// MintStamp IS THE MINT-ACCOUNTABILITY LABEL SET, and it is extracted so the
// adoption mint - which builds its Task by hand rather than through
// BuildTaskFromQueuedEvent - cannot drift from it. All three labels are
// load-bearing: LabelQueuedEvent is what mapTaskToQE and reconcileDone follow,
// LabelMintedBy is the #443 idempotency link, and LabelDedupKey is what stops a
// redelivered webhook enqueueing a second event once the first is GC'd.
func TestMintStamp_CarriesAllThreeLabels(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-abc", UID: types.UID("u-1")},
		Spec:       tatarav1alpha1.QueuedEventSpec{DedupKey: "adopt-upgrade|charts|41"},
	}
	got := MintStamp(qe)
	if got[LabelQueuedEvent] != "qe-abc" {
		t.Errorf("LabelQueuedEvent = %q", got[LabelQueuedEvent])
	}
	if got[LabelMintedBy] != "u-1" {
		t.Errorf("LabelMintedBy = %q", got[LabelMintedBy])
	}
	if got[LabelDedupKey] != dedupLabel("adopt-upgrade|charts|41") {
		t.Errorf("LabelDedupKey = %q", got[LabelDedupKey])
	}
}

// A UID-LESS EVENT (a Go literal that never met an API server) adopts nothing,
// so it stamps no minted-by link - mintedTask treats an unset UID that way too.
func TestMintStamp_OmitsMintedByWithoutAUID(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{ObjectMeta: metav1.ObjectMeta{Name: "qe-abc"}}
	if _, ok := MintStamp(qe)[LabelMintedBy]; ok {
		t.Fatal("MintStamp must omit LabelMintedBy for a UID-less event")
	}
}

// THE REFACTOR MUST NOT MOVE THE GENERIC MINT. BuildTaskFromQueuedEvent's own
// output has to keep carrying exactly what MintStamp returns.
func TestBuildTaskFromQueuedEvent_StampsExactlyMintStamp(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-xyz", Namespace: "tatara", UID: types.UID("u-2")},
		Spec: tatarav1alpha1.QueuedEventSpec{
			DedupKey: "k1",
			Payload:  tatarav1alpha1.QueuedEventPayload{Kind: "implement", Goal: "g", GenerateName: "t-"},
		},
	}
	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: "tatara"}}
	task, err := BuildTaskFromQueuedEvent(qe, proj, newEnqueueTestScheme(t))
	if err != nil {
		t.Fatalf("BuildTaskFromQueuedEvent: %v", err)
	}
	for k, v := range MintStamp(qe) {
		if task.Labels[k] != v {
			t.Errorf("task label %s = %q, want %q", k, task.Labels[k], v)
		}
	}
}
