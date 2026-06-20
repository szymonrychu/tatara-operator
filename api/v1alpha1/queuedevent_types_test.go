// api/v1alpha1/queuedevent_types_test.go
package v1alpha1

import "testing"

func TestValidateQueuedEventSpec(t *testing.T) {
	tests := []struct {
		name    string
		spec    QueuedEventSpec
		wantErr bool
	}{
		{"valid normal repo-scoped", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "issueLifecycle", ProjectRef: "p", RepositoryRef: "r", Payload: QueuedEventPayload{Kind: "issueLifecycle"}}, false},
		{"valid alert project-scoped", QueuedEventSpec{Seq: 2, Class: QueueClassAlert, Kind: "incident", ProjectRef: "p", Payload: QueuedEventPayload{Kind: "incident"}}, false},
		{"bad class", QueuedEventSpec{Seq: 1, Class: "urgent", Kind: "incident", ProjectRef: "p"}, true},
		{"bad kind", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "nope", ProjectRef: "p"}, true},
		{"missing projectRef", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "review"}, true},
		{"project-scoped kind with repoRef", QueuedEventSpec{Seq: 1, Class: QueueClassAlert, Kind: "incident", ProjectRef: "p", RepositoryRef: "r"}, true},
		{"repo-scoped kind without repoRef", QueuedEventSpec{Seq: 1, Class: QueueClassNormal, Kind: "issueLifecycle", ProjectRef: "p"}, true},
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
