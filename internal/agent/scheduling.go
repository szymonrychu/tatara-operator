package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Scheduling is the cluster-specific Pod-placement configuration for spawned
// agent wrapper Pods. It is delivered as a single JSON document (rule 6:
// list-shaped data lives in a templated ConfigMap, not as plain values), kept
// EMPTY in the chart so the chart stays cluster-agnostic (rule 14). The
// deploying helmfile supplies the actual node selector, tolerations, and
// affinity for its cluster.
type Scheduling struct {
	NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity    `json:"affinity,omitempty"`
}

// ParseScheduling parses the agent scheduling JSON document. An empty or
// whitespace-only document (the chart default) yields a zero Scheduling with no
// constraints. A malformed document returns an error so the operator fails fast
// at startup instead of silently dropping placement constraints.
//
// DisallowUnknownFields is intentional: it catches camelCase typos in
// helmfile-authored affinity/toleration docs loudly at operator boot. The
// blast radius is a startup abort (operator will not serve until the config is
// corrected). The caller (config.Load) wraps the error with the env var name
// AGENT_SCHEDULING so the log message pinpoints the misconfigured input.
func ParseScheduling(doc string) (Scheduling, error) {
	var s Scheduling
	doc = strings.TrimSpace(doc)
	if doc == "" {
		return s, nil
	}
	dec := json.NewDecoder(strings.NewReader(doc))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return Scheduling{}, fmt.Errorf("agent: parse scheduling config: %w", err)
	}
	return s, nil
}
