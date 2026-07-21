package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRestTakeoverErrorTotal_PreseededZeroBaseline(t *testing.T) {
	// The three real takeover-error stages (mint, ownerref, stamp) exist at
	// zero from startup so a rate alert has a series to evaluate on the first
	// real failure. CollectAndCount does not lazily create series, unlike
	// WithLabelValues, so this genuinely fails if the init() pre-seed is
	// removed.
	if got := testutil.CollectAndCount(RestTakeoverErrorTotal); got != 3 {
		t.Fatalf("operator_rest_takeover_error_total has %d series, want 3 (pre-seeded)", got)
	}
}

func TestRestTakeoverErrorTotal_Increments(t *testing.T) {
	for _, stage := range []string{"mint", "ownerref", "stamp"} {
		before := testutil.ToFloat64(RestTakeoverErrorTotal.WithLabelValues(stage))
		RestTakeoverErrorTotal.WithLabelValues(stage).Inc()
		after := testutil.ToFloat64(RestTakeoverErrorTotal.WithLabelValues(stage))
		if after-before != 1 {
			t.Fatalf("stage %q: want +1, got %v -> %v", stage, before, after)
		}
	}
}
