package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// GrafanaAlert is the subset of the Grafana unified-alerting webhook payload
// (Alertmanager-compatible) the receiver needs.
type GrafanaAlert struct {
	Status            string             `json:"status"`
	GroupKey          string             `json:"groupKey"`
	CommonLabels      map[string]string  `json:"commonLabels"`
	CommonAnnotations map[string]string  `json:"commonAnnotations"`
	ExternalURL       string             `json:"externalURL"`
	Alerts            []GrafanaAlertItem `json:"alerts"`
}

// GrafanaAlertItem is one alert within a Grafana webhook payload.
type GrafanaAlertItem struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

func parseGrafanaAlert(body []byte) (GrafanaAlert, error) {
	var a GrafanaAlert
	if err := json.Unmarshal(body, &a); err != nil {
		return GrafanaAlert{}, fmt.Errorf("parse grafana alert: %w", err)
	}
	if a.Status == "" {
		return GrafanaAlert{}, fmt.Errorf("grafana alert missing status")
	}
	return a, nil
}

func sortedKV(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ", ")
}

// renderAlertContext produces the compact alert block embedded in the incident
// goal (one line per fact; per-alert generatorURL/labels included).
func renderAlertContext(a GrafanaAlert) string {
	var b strings.Builder
	fmt.Fprintf(&b, "status=%s groupKey=%s\n", a.Status, a.GroupKey)
	fmt.Fprintf(&b, "commonLabels: {%s}\n", sortedKV(a.CommonLabels))
	fmt.Fprintf(&b, "commonAnnotations: {%s}\n", sortedKV(a.CommonAnnotations))
	fmt.Fprintf(&b, "externalURL: %s\n", a.ExternalURL)
	for i, it := range a.Alerts {
		fmt.Fprintf(&b, "alert[%d]: status=%s labels={%s} annotations={%s} startsAt=%s generatorURL=%s\n",
			i, it.Status, sortedKV(it.Labels), sortedKV(it.Annotations), it.StartsAt, it.GeneratorURL)
	}
	return strings.TrimRight(b.String(), "\n")
}

// alertRuleName is the human-readable rule identity for an incident Task:
// the alertname common label, falling back to the raw group key.
func alertRuleName(a GrafanaAlert) string {
	if n := a.CommonLabels["alertname"]; n != "" {
		return n
	}
	return a.GroupKey
}

// defaultVolatileDenylist is the set of alert labels whose VALUE varies per
// firing series (pod name, restart reason, ...) and so must NOT enter the
// incident dedup key. Stripping them is what makes the same rule firing for a
// different pod/reason (or escalating from one pod to a fan-out) collapse to a
// single tracker - the #320 vs #328 bug. Overridable via
// INCIDENT_DEDUP_VOLATILE_LABELS.
var defaultVolatileDenylist = []string{
	"pod", "reason", "instance", "container",
	"endpoint", "uid", "id", "replica", "ordinal",
}

// denylistSet builds the volatile-label lookup set. An empty labels slice falls
// back to defaultVolatileDenylist (a configured empty list means "use default").
func denylistSet(labels []string) map[string]bool {
	if len(labels) == 0 {
		labels = defaultVolatileDenylist
	}
	set := make(map[string]bool, len(labels))
	for _, l := range labels {
		set[l] = true
	}
	return set
}

// incidentDedupKey is the rule+workload identity of a firing alert: the project,
// the alert rule name, and the alert's STABLE common labels (volatile per-series
// labels stripped, alertname stripped because it already occupies its own slot).
// Same rule + same stable workload -> same key, so a pod/reason churn or a
// single->fan-out escalation collapses to one tracker. 16 hex chars of sha256.
// alertname falls back to the raw groupKey only when the alertname label is
// absent (via alertRuleName).
func incidentDedupKey(a GrafanaAlert, project string, denylist map[string]bool) string {
	name := alertRuleName(a)
	stable := make(map[string]string, len(a.CommonLabels))
	for k, v := range a.CommonLabels {
		if k == "alertname" || denylist[k] {
			continue
		}
		stable[k] = v
	}
	h := sha256.Sum256([]byte(project + "\x00" + name + "\x00" + sortedKV(stable)))
	return hex.EncodeToString(h[:])[:16]
}
