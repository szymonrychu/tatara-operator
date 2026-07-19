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
	// series to evaluate on the first flip. CollectAndCount does not lazily
	// create series (unlike OwnershipFlipCounter), so this genuinely fails if
	// the init() pre-seed is removed.
	if got := testutil.CollectAndCount(OwnershipFlipTotal); got != 2 {
		t.Fatalf("operator_mr_ownership_flip_total has %d series, want 2 (pre-seeded)", got)
	}
}
