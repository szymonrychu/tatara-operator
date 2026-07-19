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

// incidentDedupKey is the rule identity of a firing alert: project + alert
// rule name only. It deliberately does NOT hash CommonLabels: CommonLabels is
// the intersection of whatever instances co-fire in one Grafana evaluation, so
// its member set churns run to run (an instance joins or drops out) even
// though the rule itself hasn't changed - hashing it made the key unstable and
// bypassed the open tracker, re-spawning a fresh investigation on every
// churn (#398). Same project + same rule -> same key, always. 16 hex chars of
// sha256. alertname falls back to the raw groupKey only when the alertname
// label is absent (via alertRuleName).
func incidentDedupKey(a GrafanaAlert, project string) string {
	name := alertRuleName(a)
	h := sha256.Sum256([]byte(project + "\x00" + name))
	return hex.EncodeToString(h[:])[:16]
}

// defaultCorrelationLabels is the fallback alert common-label set whose values
// form the coarser incident GROUP key. See config.DefaultIncidentCorrelationLabels.
var defaultCorrelationLabels = []string{"namespace", "cluster"}

// correlationSet builds the correlation-label lookup set. An empty labels slice
// falls back to defaultCorrelationLabels (a configured empty list means "use
// default").
func correlationSet(labels []string) map[string]bool {
	if len(labels) == 0 {
		labels = defaultCorrelationLabels
	}
	set := make(map[string]bool, len(labels))
	for _, l := range labels {
		set[l] = true
	}
	return set
}

// incidentGroupKey is the CORRELATION identity of a firing alert: the project
// plus ONLY the alert's common labels named in the correlation set (e.g.
// namespace, cluster). It deliberately excludes the alertname, so DIFFERENT
// rules that fire for one shared workload/root cause hash to the SAME group key
// while keeping DISTINCT incidentDedupKeys. It returns "" when NONE of the
// correlation labels is present on the alert - an all-empty group would bucket
// every unlabelled alert together, so no correlation is safer than a false one.
// 16 hex chars of sha256.
func incidentGroupKey(a GrafanaAlert, project string, correlation map[string]bool) string {
	group := make(map[string]string, len(correlation))
	for k, v := range a.CommonLabels {
		if correlation[k] {
			group[k] = v
		}
	}
	if len(group) == 0 {
		return ""
	}
	h := sha256.Sum256([]byte(project + "\x00" + "group" + "\x00" + sortedKV(group)))
	return hex.EncodeToString(h[:])[:16]
}
