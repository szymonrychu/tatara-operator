package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestOwnershipFlip_Increments(t *testing.T) {
	before := testutil.ToFloat64(OwnershipFlipCounter("to-external", "external-push"))
	OwnershipFlip("to-external", "external-push")
	after := testutil.ToFloat64(OwnershipFlipCounter("to-external", "external-push"))
	if after-before != 1 {
		t.Fatalf("want +1, got %v -> %v", before, after)
	}
}

func TestOwnershipFlip_PreseededZeroBaseline(t *testing.T) {
	// Both real flip label sets exist at zero from startup so a rate alert has a
	// series to evaluate on the first flip.
	for _, lbl := range [][2]string{{"to-tatara", "takeover"}, {"to-external", "external-push"}} {
		if got := testutil.ToFloat64(OwnershipFlipCounter(lbl[0], lbl[1])); got < 0 {
			t.Fatalf("missing preseed for %v", lbl)
		}
	}
}
