package controller

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func TestRecordSCM(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantResult string
		wantStatus string // "" means no status counter expected
	}{
		{name: "ok", err: nil, wantResult: "ok"},
		{name: "error", err: errors.New("boom"), wantResult: "error", wantStatus: "network"},
		{
			name:       "gone-404",
			err:        &scm.HTTPError{Status: 404, Path: "/issues/5"},
			wantResult: "gone",
			wantStatus: "404",
		},
		{
			name:       "gone-410",
			err:        &scm.HTTPError{Status: 410, Path: "/issues/5"},
			wantResult: "gone",
			wantStatus: "410",
		},
		{
			name:       "transient-403-is-error-not-gone",
			err:        &scm.HTTPError{Status: 403, Path: "/issues/5"},
			wantResult: "error",
			wantStatus: "403",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := obs.NewOperatorMetrics(reg)

			RecordSCM(m, "github", "comment", tc.err)

			got := testutil.ToFloat64(m.SCMWriteCounter("github", "comment", tc.wantResult))
			if got != 1 {
				t.Fatalf("SCMWriteCounter(github, comment, %s) = %v, want 1", tc.wantResult, got)
			}
			if tc.wantStatus != "" {
				gotStatus := testutil.ToFloat64(m.SCMRequestErrorByStatusCounter("github", "comment", tc.wantStatus))
				if gotStatus != 1 {
					t.Fatalf("SCMRequestErrorByStatusCounter(github, comment, %s) = %v, want 1", tc.wantStatus, gotStatus)
				}
			}
		})
	}
}

func TestRecordSCMNilMetricsDoesNotPanic(t *testing.T) {
	RecordSCM(nil, "github", "comment", nil)
	RecordSCM(nil, "github", "comment", errors.New("boom"))
	RecordSCM(nil, "github", "comment", &scm.HTTPError{Status: 404, Path: "/x"})
}
