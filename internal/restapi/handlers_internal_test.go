package restapi

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
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
