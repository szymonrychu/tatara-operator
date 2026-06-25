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

// alertGroupHash is the dedup key for an alert group (16 hex chars of sha256(groupKey)).
func alertGroupHash(a GrafanaAlert) string {
	h := sha256.Sum256([]byte(a.GroupKey))
	return hex.EncodeToString(h[:])[:16]
}
