package restapi

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// writeClientErr must map k8s apiserver error kinds onto the right HTTP status:
// NotFound -> 404, Invalid (e.g. a CRD validation rejection like #398's
// line=0-fails-Minimum=1) -> 422 with the validation detail surfaced to the
// caller, anything else -> 500 with the detail withheld.
func TestWriteClientErr(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{
			name:       "not found",
			err:        apierrors.NewNotFound(schema.GroupResource{Group: "tatara.dev", Resource: "mergerequests"}, "mr1"),
			wantStatus: 404,
			wantBody:   "not found",
		},
		{
			name: "invalid",
			err: apierrors.NewInvalid(schema.GroupKind{Group: "tatara.dev", Kind: "MergeRequest"}, "mr1",
				field.ErrorList{field.Invalid(field.NewPath("status", "pendingReview", "findings").Index(0).Child("line"), 0, "must be greater than or equal to 1")}),
			wantStatus: 422,
			wantBody:   "must be greater than or equal to 1",
		},
		{
			name:       "generic error",
			err:        errors.New("boom"),
			wantStatus: 500,
			wantBody:   "internal error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeClientErr(w, tt.err)
			require.Equal(t, tt.wantStatus, w.Code)
			require.Contains(t, w.Body.String(), tt.wantBody)
		})
	}
}

// TestSortTasksNewestFirst pins the Name tiebreak (#641). It is a direct unit
// test because the fake client behind the handler tests returns Items already
// sorted by name, so a request-level test cannot distinguish a comparator with
// the tiebreak from one without: the input arriving pre-sorted makes
// SliceStable produce the right answer either way. Production input comes from
// the informer cache, whose Indexer iterates a Go map, so the order among
// same-second Tasks is arbitrary - hence the deliberately reversed input here.
func TestSortTasksNewestFirst(t *testing.T) {
	at := func(sec int) metav1.Time {
		return metav1.NewTime(time.Date(2026, 1, 1, 0, 0, sec, 0, time.UTC))
	}
	mk := func(name string, ts metav1.Time) *tatarav1alpha1.Task {
		return &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: ts}}
	}
	// One sweep's worth of same-second Tasks, fed in reverse name order, with an
	// older and a newer Task around them.
	tasks := []*tatarav1alpha1.Task{
		mk("old", at(0)),
		mk("sweep-c", at(10)),
		mk("sweep-b", at(10)),
		mk("sweep-a", at(10)),
		mk("new", at(20)),
	}
	sortTasksNewestFirst(tasks)

	got := make([]string, 0, len(tasks))
	for _, task := range tasks {
		got = append(got, task.Name)
	}
	require.Equal(t, []string{"new", "sweep-a", "sweep-b", "sweep-c", "old"}, got)
}
