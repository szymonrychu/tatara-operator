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

// tatara-operator#523: the SAME alertname recurring for a genuinely different
// root cause (a different "category") must NOT collapse onto one tracker -
// #523's own refire history shows category=memory_inconsistent,
// workspace_broken, and directive_contradiction all landing on the same
// issue. incidentDedupKey now includes category (read per-item, see
// alertCategory), so two otherwise-identical firings differing only in
// category must produce DIFFERENT keys.
func TestIncidentDedupKey_523_CategoryDiffers(t *testing.T) {
	base := func(category string) GrafanaAlert {
		return GrafanaAlert{
			CommonLabels: map[string]string{
				"alertname": "Tatara agent reported platform problem",
				"component": "operator",
				"severity":  "warning",
			},
			Alerts: []GrafanaAlertItem{
				{Labels: map[string]string{"category": category, "component": "operator"}},
			},
		}
	}
	memoryInconsistent := incidentDedupKey(base("memory_inconsistent"), "tatara")
	workspaceBroken := incidentDedupKey(base("workspace_broken"), "tatara")
	directiveContradiction := incidentDedupKey(base("directive_contradiction"), "tatara")

	if memoryInconsistent == workspaceBroken {
		t.Fatalf("category memory_inconsistent vs workspace_broken must differ: both %s", memoryInconsistent)
	}
	if memoryInconsistent == directiveContradiction {
		t.Fatalf("category memory_inconsistent vs directive_contradiction must differ: both %s", memoryInconsistent)
	}
	if workspaceBroken == directiveContradiction {
		t.Fatalf("category workspace_broken vs directive_contradiction must differ: both %s", workspaceBroken)
	}
}

// #398's actual failure mode, replicated for category specifically: within one
// Grafana evaluation, category=tool_error co-fired alongside category=other
// under the SAME alertname (see #398's "live re-confirmation" comment), which
// makes category DROP OUT of Grafana's CommonLabels for that evaluation even
// though the rule identity hasn't changed. A dedup key that trusted
// CommonLabels["category"] would flip presence/absence run to run. Because
// incidentDedupKey never reads category from CommonLabels (alertCategory
// reads per-item Labels instead), a batch where CommonLabels happens to carry
// a churning "category" entry - or not - must NOT change the key.
func TestIncidentDedupKey_398_CategoryChurnInCommonLabelsIgnored(t *testing.T) {
	withCategoryInCommonLabels := GrafanaAlert{
		CommonLabels: map[string]string{
			"alertname": "Tatara agent reported platform problem",
			"category":  "tool_error", // present this evaluation: only tool_error fired
		},
	}
	withoutCategoryInCommonLabels := GrafanaAlert{
		CommonLabels: map[string]string{
			"alertname": "Tatara agent reported platform problem",
			// category absent this evaluation: tool_error co-fired with a
			// different category, so it fell out of the CommonLabels
			// intersection (#398's member-set churn).
		},
	}
	ka := incidentDedupKey(withCategoryInCommonLabels, "tatara")
	kb := incidentDedupKey(withoutCategoryInCommonLabels, "tatara")
	if ka != kb {
		t.Fatalf("CommonLabels category churn must not change the key: ka=%s kb=%s", ka, kb)
	}
}

// A genuinely mixed batch (distinct categories co-firing at once, per-item)
// must not silently and arbitrarily pick one - alertCategory falls back to ""
// (no category component in the key), matching pre-#523-fix behaviour rather
// than depending on item order.
func TestAlertCategory_MixedBatchIsUnresolved(t *testing.T) {
	mixed := GrafanaAlert{Alerts: []GrafanaAlertItem{
		{Labels: map[string]string{"category": "tool_error"}},
		{Labels: map[string]string{"category": "other"}},
	}}
	if got := alertCategory(mixed); got != "" {
		t.Fatalf("mixed-category batch must resolve to \"\", got %q", got)
	}
	agreeing := GrafanaAlert{Alerts: []GrafanaAlertItem{
		{Labels: map[string]string{"category": "tool_error"}},
		{Labels: map[string]string{"category": "tool_error"}},
	}}
	if got := alertCategory(agreeing); got != "tool_error" {
		t.Fatalf("unanimous category must resolve, got %q", got)
	}
}
