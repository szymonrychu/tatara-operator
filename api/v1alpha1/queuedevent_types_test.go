// api/v1alpha1/queuedevent_types_test.go
package v1alpha1

import (
	"strings"
	"testing"
)

func TestValidateQueuedEventSpec(t *testing.T) {
	tests := []struct {
		name    string
		spec    QueuedEventSpec
		wantErr bool
	}{
		{"valid normal repo-scoped", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "documentation", ProjectRef: "p", RepositoryRef: "r", Payload: QueuedEventPayload{Kind: "documentation"}}, false},
		{"valid alert project-scoped", QueuedEventSpec{Seq: 2, Class: QueueClassAlert, Kind: "incident", ProjectRef: "p", Payload: QueuedEventPayload{Kind: "incident"}}, false},
		{"bad class", QueuedEventSpec{Seq: 1, Class: "urgent", Kind: "incident", ProjectRef: "p"}, true},
		{"bad kind", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "nope", ProjectRef: "p"}, true},
		{"missing projectRef", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "review"}, true},
		{"project-scoped kind with repoRef", QueuedEventSpec{Seq: 1, Class: QueueClassAlert, Kind: "incident", ProjectRef: "p", RepositoryRef: "r"}, true},
		{"repo-scoped kind without repoRef", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "documentation", ProjectRef: "p"}, true},
		{"zero seq", QueuedEventSpec{Seq: 0, Class: QueueClassNormal, Kind: "review", ProjectRef: "p", RepositoryRef: "r"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateQueuedEventSpec(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateQueuedEventSpec_NewPayloadShape(t *testing.T) {
	base := func(p QueuedEventPayload) QueuedEventSpec {
		return QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "incident", ProjectRef: "p", Payload: p}
	}
	tests := []struct {
		name    string
		payload QueuedEventPayload
		wantErr bool
	}{
		{"agentKind + taskRef only: valid", QueuedEventPayload{Kind: "incident", AgentKind: "implement", TaskRef: "t-1"}, false},
		{"agentKind + newTask only: valid", QueuedEventPayload{Kind: "incident", AgentKind: "implement", NewTask: &QueuedTaskBlueprint{Name: "n", Kind: "implement", Goal: "g", ProjectRef: "p"}}, false},
		{"agentKind with neither taskRef nor newTask: invalid", QueuedEventPayload{Kind: "incident", AgentKind: "implement"}, true},
		{"agentKind with both taskRef and newTask: invalid", QueuedEventPayload{Kind: "incident", AgentKind: "implement", TaskRef: "t-1", NewTask: &QueuedTaskBlueprint{Name: "n", Kind: "implement", Goal: "g", ProjectRef: "p"}}, true},
		{"unknown agentKind: invalid", QueuedEventPayload{Kind: "incident", AgentKind: "triageIssue", TaskRef: "t-1"}, true},
		{"newTask goal over 16384 bytes: invalid", QueuedEventPayload{Kind: "incident", AgentKind: "implement", NewTask: &QueuedTaskBlueprint{Name: "n", Kind: "implement", Goal: strings.Repeat("x", 16385), ProjectRef: "p"}}, true},
		{"old-style payload with no agentKind: still valid (backward compat)", QueuedEventPayload{Kind: "incident", GenerateName: "x-"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateQueuedEventSpec(base(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestEffectiveQueuePriority(t *testing.T) {
	two := 2
	zero := 0
	tests := []struct {
		name string
		spec QueuedEventSpec
		want int
	}{
		{"nil priority defaults to 2", QueuedEventSpec{}, 2},
		{"explicit 0 (incident) is honoured, not treated as absent", QueuedEventSpec{Priority: &zero}, 0},
		{"explicit 2 round-trips", QueuedEventSpec{Priority: &two}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EffectiveQueuePriority(tt.spec); got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}
