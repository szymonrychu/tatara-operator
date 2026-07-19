package webhook

import "testing"

func TestIncidentDedupKey(t *testing.T) {
	// #320: single-pod CrashLoopBackOff. #328: same rule, 6-pod fan-out to
	// CreateContainerError on a different container. The only differences are
	// per-series label VALUES, so the keys MUST be equal.
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
	// #398: co-firing member-set churn. CommonLabels is the intersection of
	// whatever instances co-fire in one evaluation, so it grows or shrinks a
	// key run to run even though the rule itself is unchanged - one
	// evaluation carries a "severity" common label, the next drops it because
	// a co-firing member without that label joined.
	i398MemberSetA := GrafanaAlert{CommonLabels: map[string]string{
		"alertname": "Memory postgres or neo4j container stuck waiting",
		"namespace": "tatara-memory",
		"severity":  "critical",
	}}
	i398MemberSetB := GrafanaAlert{CommonLabels: map[string]string{
		"alertname": "Memory postgres or neo4j container stuck waiting",
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
		{"398_common_label_member_set_churn_collapse", i398MemberSetA, i398MemberSetB, "tatara", "tatara", true},
		{"different_project_no_collision", i320, i320, "tatara", "other", false},
		{"different_rule_differs", i320, GrafanaAlert{CommonLabels: map[string]string{
			"alertname": "Some Other Rule", "namespace": "tatara-memory",
		}}, "tatara", "tatara", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ka := incidentDedupKey(tt.a, tt.project)
			kb := incidentDedupKey(tt.b, tt.bproject)
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
	k320 := incidentDedupKey(i320, "tatara")
	k328 := incidentDedupKey(i328, "tatara")
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
	if incidentDedupKey(other, "tatara") == k320 {
		t.Fatal("a different alertname must produce a different key")
	}
}

func TestIncidentDedupKey_AlertnameFallbackToGroupKey(t *testing.T) {
	a := GrafanaAlert{GroupKey: "grp-abc", CommonLabels: map[string]string{"namespace": "x"}}
	b := GrafanaAlert{GroupKey: "grp-zzz", CommonLabels: map[string]string{"namespace": "x"}}
	if incidentDedupKey(a, "p") == incidentDedupKey(b, "p") {
		t.Fatal("with alertname absent, differing groupKey must change the key")
	}
}
