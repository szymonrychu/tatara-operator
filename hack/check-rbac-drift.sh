#!/usr/bin/env bash
# Fail-closed RBAC drift guard.
#
# The chart Role (charts/tatara-operator/templates/rbac.yaml) is the
# hand-maintained source of truth helm renders. The +kubebuilder:rbac markers
# in internal/controller/*.go are a VERIFIED MIRROR of those rules: this guard
# regenerates a throwaway ClusterRole from the markers and asserts set-equality
# (per {group,resource,verb} tuple) against the chart's namespaced Role and the
# crd-reader ClusterRole.
#
# controller-gen emits a SINGLE ClusterRole and cannot encode the
# namespaced-Role-vs-crd-reader-ClusterRole scope split, so the guard partitions
# the generated rules via an explicit KNOWN_NAMESPACED allowlist plus a
# crd-reader allowlist. A generated group|resource pair in neither list is an
# unknown scope and fails the guard closed.
#
# The comparison is on canonicalized tuple SETS, never a textual YAML diff:
# controller-gen coalesces resources by shared verb-set and sorts verbs, so the
# generated YAML never matches the hand-grouped chart YAML textually.
set -euo pipefail

CONTROLLER_GEN="${CONTROLLER_GEN:-go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.18.0}"
RBAC_GEN_DIR="${RBAC_GEN_DIR:-.rbac-gen}"
HELM_BIN="${HELM_BIN:-helm}"
CHART_DIR="charts/tatara-operator"

# crd-reader (cluster-scoped) allowlist: exactly one pair.
CRDREADER_PAIRS="apiextensions.k8s.io|customresourcedefinitions"

# Namespaced-Role allowlist: the group|resource pairs that belong in the
# namespaced Role. Empty apiGroup is the literal empty string "".
KNOWN_NAMESPACED='
tatara.dev|projects
tatara.dev|repositories
tatara.dev|tasks
tatara.dev|subtasks
tatara.dev|queuedevents
tatara.dev|projects/status
tatara.dev|repositories/status
tatara.dev|tasks/status
tatara.dev|subtasks/status
tatara.dev|queuedevents/status
batch|jobs
""|pods
""|services
""|configmaps
""|secrets
""|events
""|persistentvolumeclaims
apps|deployments
apps|statefulsets
postgresql.cnpg.io|clusters
postgresql.cnpg.io|clusters/status
monitoring.coreos.com|servicemonitors
monitoring.coreos.com|prometheusrules
networking.k8s.io|ingresses
coordination.k8s.io|leases
'

# classify GROUP RESOURCE -> echoes "crdreader" or "role"; exits 1 on unknown.
classify() {
	local pair="$1|$2"
	if [ "$pair" = "$CRDREADER_PAIRS" ]; then
		echo crdreader
		return 0
	fi
	if grep -qxF "$pair" <<<"$KNOWN_NAMESPACED"; then
		echo role
		return 0
	fi
	echo "rbac-drift: unknown RBAC scope for $1/$2: teach hack/check-rbac-drift.sh" >&2
	exit 1
}

# flatten <file> <yq-doc-selector> -> sorted-unique group|resource|verb tuples.
# Empty apiGroup is normalized to the literal "".
flatten() {
	local file="$1" sel="$2"
	# eval-all emits "---" separators between the multi-doc stream; keep only
	# well-formed group|resource|verb triples (exactly two pipes).
	yq eval-all "${sel} | .rules[] | .apiGroups[] as \$g | .resources[] as \$r | .verbs[] as \$v | \$g + \"|\" + \$r + \"|\" + \$v" "$file" |
		sed 's/^|/""|/' | grep -E '^[^|]*\|[^|]*\|[^|]+$' | sort -u
}

# Regenerate markers -> throwaway ClusterRole.
mkdir -p "$RBAC_GEN_DIR"
# shellcheck disable=SC2086
$CONTROLLER_GEN rbac:roleName=tatara-operator-manager paths="./internal/controller/..." output:dir="$RBAC_GEN_DIR"

GEN_FILE="$RBAC_GEN_DIR/role.yaml"
RENDER_FILE="$(mktemp)"
trap 'rm -f "$RENDER_FILE"' EXIT
"$HELM_BIN" template "$CHART_DIR" >"$RENDER_FILE"

# Canonicalized tuple sets.
GEN="$(flatten "$GEN_FILE" 'select(.kind == "ClusterRole")')"
CHART_ROLE="$(flatten "$RENDER_FILE" 'select(.kind == "Role" and (.metadata.name | test("-manager$")))')"
CHART_CRDREADER="$(flatten "$RENDER_FILE" 'select(.kind == "ClusterRole" and (.metadata.name | test("-crd-reader$")))')"

if [ -z "$CHART_ROLE" ]; then
	echo "rbac-drift: could not find namespaced Role *-manager in rendered chart" >&2
	exit 1
fi
if [ -z "$CHART_CRDREADER" ]; then
	echo "rbac-drift: could not find ClusterRole *-crd-reader in rendered chart" >&2
	exit 1
fi

# Partition GEN into role-scope and crdreader-scope by the explicit allowlists.
# Fails closed on any unknown group|resource pair.
GEN_ROLE=""
GEN_CRDREADER=""
while IFS='|' read -r g r v; do
	[ -z "$g$r$v" ] && continue
	scope="$(classify "$g" "$r")"
	line="$g|$r|$v"
	if [ "$scope" = "crdreader" ]; then
		GEN_CRDREADER+="$line"$'\n'
	else
		GEN_ROLE+="$line"$'\n'
	fi
done <<<"$GEN"

GEN_ROLE="$(printf '%s' "$GEN_ROLE" | sed '/^$/d' | sort -u)"
GEN_CRDREADER="$(printf '%s' "$GEN_CRDREADER" | sed '/^$/d' | sort -u)"

fail=0
report_diff() {
	local label="$1" markers="$2" chart="$3"
	local only_markers only_chart
	only_markers="$(comm -23 <(printf '%s\n' "$markers") <(printf '%s\n' "$chart"))"
	only_chart="$(comm -13 <(printf '%s\n' "$markers") <(printf '%s\n' "$chart"))"
	if [ -n "$only_markers" ] || [ -n "$only_chart" ]; then
		fail=1
		echo "rbac-drift: $label mismatch" >&2
		if [ -n "$only_markers" ]; then
			echo "  markers-only (rule in +kubebuilder markers but NOT in chart):" >&2
			sed 's/^/    /' <<<"$only_markers" >&2
		fi
		if [ -n "$only_chart" ]; then
			echo "  chart-only (rule in chart but NOT in +kubebuilder markers):" >&2
			sed 's/^/    /' <<<"$only_chart" >&2
		fi
	fi
}

report_diff "namespaced Role" "$GEN_ROLE" "$CHART_ROLE"
report_diff "crd-reader ClusterRole" "$GEN_CRDREADER" "$CHART_CRDREADER"

if [ "$fail" -ne 0 ]; then
	echo "rbac-drift: FAILED. The chart Role/ClusterRole and +kubebuilder:rbac markers have diverged." >&2
	echo "rbac-drift: chart is the source of truth helm renders; bring the markers and chart back into agreement." >&2
	exit 1
fi

total=$(( $(wc -l <<<"$GEN_ROLE") + $(wc -l <<<"$GEN_CRDREADER") ))
echo "rbac-drift: chart Role/ClusterRole == markers ($total tuples)"
