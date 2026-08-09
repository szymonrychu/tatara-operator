package agent

import (
	"context"
	"testing"
)

type skewSession struct {
	Session
	info SessionInfo
}

func (s skewSession) GetSession(context.Context, string) (SessionInfo, error) { return s.info, nil }

func intp(v int) *int { return &v }

// TestAssertContractVersion_SkewIsTransientNotTerminal is the #544 regression.
//
// The v2.0.x release train is NOT atomic: the wrapper image pin and the
// operator version live in different helm releases, and helmfile rolls them
// independently. On 2026-08-08 the wrapper landed 56 minutes before the
// operator, and the strict-equality gate at session.go destroyed 5 Tasks at
// turn-0 inside that window. An adjacent version is a release train mid-flight,
// not a broken wrapper: it must be reported as a SKEW the caller can wait out,
// never as the terminal mismatch.
func TestAssertContractVersion_SkewIsTransientNotTerminal(t *testing.T) {
	cases := []struct {
		name         string
		info         SessionInfo
		wantSkew     bool
		wantTerminal bool
	}{
		{
			name: "exact match",
			info: SessionInfo{ContractVersion: intp(ContractVersion)},
		},
		{
			// The EXACT 2026-08-08 shape: the wrapper is AHEAD because its pin
			// landed first. The operator rolls forward minutes later and the very
			// same pod then passes.
			name:     "wrapper one version AHEAD (the train landed the image first)",
			info:     SessionInfo{ContractVersion: intp(ContractVersion + 1)},
			wantSkew: true,
		},
		{
			name:     "wrapper one version BEHIND (the train landed the operator first)",
			info:     SessionInfo{ContractVersion: intp(ContractVersion - 1)},
			wantSkew: true,
		},
		{
			name:         "wrapper far ahead is a real mismatch",
			info:         SessionInfo{ContractVersion: intp(ContractVersion + 4)},
			wantTerminal: true,
		},
		{
			name:         "wrapper far behind is a real mismatch",
			info:         SessionInfo{ContractVersion: intp(1)},
			wantTerminal: true,
		},
		{
			name:         "no contractVersion field at all is a real mismatch",
			info:         SessionInfo{},
			wantTerminal: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := AssertContractVersion(context.Background(), skewSession{info: tc.info}, "http://wrapper")
			switch {
			case tc.wantSkew:
				if !IsContractSkew(err) {
					t.Fatalf("got %v, want a transient contract skew", err)
				}
				if IsContractMismatch(err) {
					t.Fatal("a rollout-window skew must NOT satisfy IsContractMismatch: that is the terminal park")
				}
			case tc.wantTerminal:
				if !IsContractMismatch(err) {
					t.Fatalf("got %v, want the terminal contract mismatch", err)
				}
				if IsContractSkew(err) {
					t.Fatal("a real mismatch must not be waited out")
				}
			default:
				if err != nil {
					t.Fatalf("got %v, want nil", err)
				}
			}
		})
	}
}
