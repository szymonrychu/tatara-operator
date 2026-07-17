package restapi

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// SPEC TEST 5, at the level the invariant actually lives: release is a CAS that
// only ever drops a condition still carrying OUR fingerprint AND Reason
// "Outcome". Never a committed condition; never another request's claim - a
// slow handler can be released long after another replica re-claimed and
// committed the slot underneath it.
func TestRelease_IsOwnershipChecked(t *testing.T) {
	tests := []struct {
		name       string
		condReason string
		condFP     string
		ourFP      string
		wantGone   bool
	}{
		{"our own bare claim is released", tatarav1alpha1.OutcomeReasonClaimed, "ourfp", "ourfp", true},
		{"another request's claim is NEVER released", tatarav1alpha1.OutcomeReasonClaimed, "theirfp", "ourfp", false},
		{"a COMMITTED condition is NEVER released", "Review", "ourfp", "ourfp", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: "tatara"},
				Status: tatarav1alpha1.TaskStatus{
					Stage: tatarav1alpha1.StageReviewing,
					Conditions: []metav1.Condition{{
						Type:               tatarav1alpha1.ConditionOutcomeAccepted,
						Status:             metav1.ConditionTrue,
						Reason:             tc.condReason,
						Message:            tc.condFP,
						LastTransitionTime: metav1.NewTime(time.Unix(0, 0)),
					}},
				},
			}
			fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(task).
				WithStatusSubresource(&tatarav1alpha1.Task{}).Build()
			s := NewServer(Config{Client: fc, Namespace: "tatara", Now: func() time.Time { return time.Unix(0, 0) }})
			o := &outcomeCtx{
				s: s, task: task, fp: tc.ourFP, kind: "review",
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("POST", "/tasks/t1/outcome", nil),
			}
			o.release()

			var got tatarav1alpha1.Task
			require.NoError(t, fc.Get(context.Background(),
				client.ObjectKey{Namespace: "tatara", Name: "t1"}, &got))
			if tc.wantGone {
				require.Nil(t, tatarav1alpha1.OutcomeCondition(&got))
			} else {
				require.NotNil(t, tatarav1alpha1.OutcomeCondition(&got))
			}
		})
	}
}
