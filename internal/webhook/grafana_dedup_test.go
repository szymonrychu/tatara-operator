package webhook

import "testing"

func TestIncidentDedupKey(t *testing.T) {
	den := denylistSet(nil) // operator default

	// #320: single-pod CrashLoopBackOff. #328: same rule, 6-pod fan-out to
	// CreateContainerError on a different container. The ONLY differences are
	// volatile labels (pod, reason, container), so the keys MUST be equal.
	i320 := GrafanaAlert{CommonLabels: map[string]string{
		"alertname": "Memory postgres or neo4j container stuck waiting",
		"namespace": "tatara-memory",
		"pod":       "tatara-memory-postgres-1-0",
		"reason":    "CrashLoopBackOff",
		"container": "postgres",
	}}
	i328 := GrafanaAlert{CommonLabels: map[string]string{
		"alertname": "Memory postgres or neo4j container stuck waiting",
		"namespace": "tatara-memory",
		"pod":       "tatara-memory-neo4j-3",
		"reason":    "CreateContainerError",
		"container": "neo4j",
	}}

	tests := []struct {
		name      string
		a         GrafanaAlert
		b         GrafanaAlert
		project   string
		bproject  string
		wantEqual bool
	}{
		{"regression_320_328_collapse", i320, i328, "tatara", "tatara", true},
		{"different_project_no_collision", i320, i320, "tatara", "other", false},
		{"different_stable_label_differs", i320, GrafanaAlert{CommonLabels: map[string]string{
			"alertname": "Memory postgres or neo4j container stuck waiting",
			"namespace": "tatara-chat", "pod": "x", "reason": "y",
		}}, "tatara", "tatara", false},
		{"different_rule_differs", i320, GrafanaAlert{CommonLabels: map[string]string{
			"alertname": "Some Other Rule", "namespace": "tatara-memory",
		}}, "tatara", "tatara", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ka := incidentDedupKey(tt.a, tt.project, den)
			kb := incidentDedupKey(tt.b, tt.bproject, den)
			if len(ka) != 16 {
				t.Fatalf("key not 16 hex chars: %q", ka)
			}
			if (ka == kb) != tt.wantEqual {
				t.Fatalf("equal=%v, want %v (ka=%s kb=%s)", ka == kb, tt.wantEqual, ka, kb)
			}
		})
	}
}

// #320 and #328's REAL commonLabels sets, verbatim, per the incident writeup:
// the pinned regression fixture the dedup key MUST collapse.
func TestIncidentDedupKey_Real320328LabelSets(t *testing.T) {
	den := denylistSet(nil)
	i320 := GrafanaAlert{CommonLabels: map[string]string{
		"alertname":      "Memory postgres or neo4j container stuck waiting",
		"component":      "memory",
		"pod":            "mem-tatara-pg-2",
		"reason":         "CrashLoopBackOff",
		"severity":       "critical",
		"system":         "tatara",
		"homelab":        "true",
		"grafana_folder": "Tatara",
	}}
	i328 := GrafanaAlert{CommonLabels: map[string]string{
		"alertname":      "Memory postgres or neo4j container stuck waiting",
		"component":      "memory",
		"severity":       "critical",
		"system":         "tatara",
		"homelab":        "true",
		"grafana_folder": "Tatara",
		"reason":         "CreateContainerError",
	}}
	k320 := incidentDedupKey(i320, "tatara", den)
	k328 := incidentDedupKey(i328, "tatara", den)
	if k320 != k328 {
		t.Fatalf("#320 and #328 must hash to the SAME key: k320=%s k328=%s", k320, k328)
	}

	// Negative: a different alertname must NOT collapse.
	other := GrafanaAlert{CommonLabels: map[string]string{
		"alertname":      "Some Other Rule",
		"component":      "memory",
		"severity":       "critical",
		"system":         "tatara",
		"homelab":        "true",
		"grafana_folder": "Tatara",
	}}
	if incidentDedupKey(other, "tatara", den) == k320 {
		t.Fatal("a different alertname must produce a different key")
	}

	// Negative: a different component (a stable label) must NOT collapse.
	otherComponent := GrafanaAlert{CommonLabels: map[string]string{
		"alertname":      "Memory postgres or neo4j container stuck waiting",
		"component":      "chat",
		"severity":       "critical",
		"system":         "tatara",
		"homelab":        "true",
		"grafana_folder": "Tatara",
	}}
	if incidentDedupKey(otherComponent, "tatara", den) == k320 {
		t.Fatal("a different component must produce a different key")
	}
}

func TestIncidentDedupKey_AlertnameFallbackToGroupKey(t *testing.T) {
	den := denylistSet(nil)
	a := GrafanaAlert{GroupKey: "grp-abc", CommonLabels: map[string]string{"namespace": "x"}}
	b := GrafanaAlert{GroupKey: "grp-zzz", CommonLabels: map[string]string{"namespace": "x"}}
	if incidentDedupKey(a, "p", den) == incidentDedupKey(b, "p", den) {
		t.Fatal("with alertname absent, differing groupKey must change the key")
	}
}

func TestDenylistSet_CustomOverridesDefault(t *testing.T) {
	set := denylistSet([]string{"foo", "bar"})
	if !set["foo"] || set["pod"] {
		t.Fatalf("custom denylist should replace default, got %v", set)
	}
}
