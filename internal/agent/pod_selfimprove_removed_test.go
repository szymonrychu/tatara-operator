package agent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestKindProfilesHasNoSelfImprove asserts the selfImprove profile has been
// fully removed from kindProfiles (WS2 deletion, Task 13).
func TestKindProfilesHasNoSelfImprove(t *testing.T) {
	_, ok := kindProfiles["selfImprove"]
	assert.False(t, ok, "selfImprove profile must be removed")
}
