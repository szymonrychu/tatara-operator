package v1alpha1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestCRDMaxLengthMatchesConstants is the guard the "keep these in sync"
// comments could not be. Every constant in limits.go claims to mirror a
// +kubebuilder:validation:MaxLength marker; a constant that quietly disagrees
// with its marker is worse than no constant at all, because every writer that
// trusts it writes past the real limit and 422s the whole status update - which
// is how issue #495 dropped a human's review reply on every sweep for three
// days. This reads the GENERATED CRDs, so it fails if either side moves.
func TestCRDMaxLengthMatchesConstants(t *testing.T) {
	cases := []struct {
		crd  string
		path []string
		want int
	}{
		{"tatara.dev_tasks.yaml", []string{"spec", "goal"}, GoalMaxBytes},
		{"tatara.dev_tasks.yaml", []string{"status", "notes", "body"}, NoteBodyMaxBytes},
		{"tatara.dev_tasks.yaml", []string{"status", "pendingEvents", "body"}, TaskEventBodyMaxBytes},
		{"tatara.dev_queuedevents.yaml", []string{"spec", "payload", "goal"}, GoalMaxBytes},
		// The adoption snapshot's body. Its twin is MergeRequestBodyMaxBytes and
		// not a constant of its own, because the string's destination IS
		// MergeRequest.status.body - a second name for the same number would be
		// two things to keep in sync instead of one.
		{"tatara.dev_queuedevents.yaml", []string{"spec", "payload", "adoptedUpgrade", "body"}, MergeRequestBodyMaxBytes},
		{"tatara.dev_issues.yaml", []string{"status", "body"}, IssueBodyMaxBytes},
		{"tatara.dev_issues.yaml", []string{"status", "comments", "body"}, CommentBodyMaxBytes},
		{"tatara.dev_issues.yaml", []string{"status", "pendingComments", "body"}, PendingCommentBodyMaxBytes},
		{"tatara.dev_mergerequests.yaml", []string{"status", "body"}, MergeRequestBodyMaxBytes},
		{"tatara.dev_mergerequests.yaml", []string{"status", "comments", "body"}, CommentBodyMaxBytes},
		{"tatara.dev_mergerequests.yaml", []string{"status", "pendingComments", "body"}, PendingCommentBodyMaxBytes},
		{"tatara.dev_mergerequests.yaml", []string{"status", "pendingReview", "body"}, PendingReviewBodyMaxBytes},
		{"tatara.dev_mergerequests.yaml", []string{"status", "pendingReview", "findings", "body"}, ReviewFindingBodyMaxBytes},
	}
	for _, tc := range cases {
		t.Run(tc.crd+":"+strings.Join(tc.path, "."), func(t *testing.T) {
			got := crdMaxLength(t, tc.crd, tc.path...)
			if got != tc.want {
				t.Fatalf("CRD maxLength = %d, Go constant = %d: the marker and its Go twin have drifted", got, tc.want)
			}
		})
	}
}

// crdMaxLength walks the generated CRD's v1alpha1 openAPIV3Schema down path
// (descending through items for array fields) and returns the leaf's maxLength.
func crdMaxLength(t *testing.T, crdFile string, path ...string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "tatara-operator", "crd-bases", crdFile))
	if err != nil {
		t.Fatalf("read CRD %s: %v (run `make manifests`)", crdFile, err)
	}
	var crd struct {
		Spec struct {
			Versions []struct {
				Name   string `json:"name"`
				Schema struct {
					OpenAPIV3Schema map[string]any `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("parse CRD %s: %v", crdFile, err)
	}
	node := map[string]any(nil)
	for _, v := range crd.Spec.Versions {
		if v.Name == "v1alpha1" {
			node = v.Schema.OpenAPIV3Schema
		}
	}
	if node == nil {
		t.Fatalf("CRD %s has no v1alpha1 version", crdFile)
	}
	for _, key := range path {
		props, ok := node["properties"].(map[string]any)
		if !ok {
			t.Fatalf("CRD %s: no properties at %q", crdFile, key)
		}
		next, ok := props[key].(map[string]any)
		if !ok {
			t.Fatalf("CRD %s: no property %q", crdFile, key)
		}
		if items, ok := next["items"].(map[string]any); ok {
			next = items
		}
		node = next
	}
	maxLen, ok := node["maxLength"].(float64)
	if !ok {
		t.Fatalf("CRD %s: %v carries no maxLength - the marker is missing", crdFile, path)
	}
	return int(maxLen)
}
