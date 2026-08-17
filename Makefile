SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

REGISTRY ?= harbor.szymonrichert.pl
IMAGE_NAME ?= containers/tatara-operator
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

IMAGE_REF := $(REGISTRY)/$(IMAGE_NAME):$(VERSION)

CONTROLLER_GEN_VERSION ?= v0.18.0
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

ENVTEST_VERSION ?= release-0.21
ENVTEST ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)
ENVTEST_K8S_VERSION ?= 1.33.0

CHART_CRD_DIR := charts/tatara-operator/crd-bases
RBAC_GEN_DIR := .rbac-gen

# Resolve helm binary via mise to avoid homebrew helm 4.x shadow.
HELM_BIN := $(shell mise exec -- bash -c 'echo $$PATH' 2>/dev/null | tr ':' '\n' | grep -m1 'mise/installs/helm/' || true)
ifdef HELM_BIN
HELM_BIN := $(HELM_BIN)/helm
else
HELM_BIN := helm
endif

.PHONY: all generate manifests test lint build image fmt tidy chart-lint clean ci rbac rbac-check test-cd-jobs

all: generate manifests lint test build

generate:
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

manifests:
	mkdir -p $(CHART_CRD_DIR)
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=$(CHART_CRD_DIR)

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

lint:
	golangci-lint run ./... || [ $$? -eq 5 ]

test:
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
		go test ./... -race -count=1

build:
	CGO_ENABLED=0 go build \
		-trimpath \
		-ldflags "-s -w \
		  -X github.com/szymonrychu/tatara-operator/internal/version.Version=$(VERSION) \
		  -X github.com/szymonrychu/tatara-operator/internal/version.Commit=$(COMMIT) \
		  -X github.com/szymonrychu/tatara-operator/internal/version.Date=$(DATE)" \
		-o bin/tatara-operator \
		./cmd/manager

image:
	docker buildx build \
		--platform=linux/amd64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg DATE=$(DATE) \
		-t $(IMAGE_REF) \
		--load \
		.

chart-lint:
	$(HELM_BIN) lint charts/tatara-operator
	@n=$$($(HELM_BIN) template charts/tatara-operator | grep -c 'helm.sh/resource-policy: keep'); \
		if [ "$$n" -ne 6 ]; then \
			echo "chart-lint: expected 6 tatara.dev CRDs with resource-policy:keep, got $$n (controller-gen format may have shifted, breaking templates/crds.yaml inject)"; \
			exit 1; \
		fi
	@on=$$($(HELM_BIN) template charts/tatara-operator | grep -c 'kind: PrometheusRule'); \
		off=$$($(HELM_BIN) template charts/tatara-operator --set prometheusRule.enabled=false | grep -c 'kind: PrometheusRule' || true); \
		if [ "$$on" -ne 1 ] || [ "$$off" -ne 0 ]; then \
			echo "chart-lint: PrometheusRule gating broken (enabled renders $$on want 1, disabled renders $$off want 0)"; \
			exit 1; \
		fi
	@rendered="$$($(HELM_BIN) template charts/tatara-operator)"; \
	for m in operator_reconcile_total operator_task_terminal_total operator_task_parked_total operator_task_residency_exceeded_total operator_task_parked_with_live_pod_repaired_total operator_turn_timeout_total operator_agent_boot_crash_total operator_agent_unreachable_termination_total operator_ingest_job_total operator_scm_writes_total operator_reap_delete_error_total operator_push_receive_total operator_memory_stacks operator_tasks_inflight operator_tasks_minted_per_sweep operator_sweep_mint_cap_hit_total operator_sweep_skipped_total tatara_lifecycle_giveup_total operator_admission_blocked_total operator_token_budget_used_ratio; do \
		if ! grep -q "$$m" <<<"$$rendered"; then \
			echo "chart-lint: PrometheusRule references unknown/absent metric $$m"; \
			exit 1; \
		fi; \
	done
	@don=$$($(HELM_BIN) template charts/tatara-operator | grep -c 'grafana_dashboard: "1"'); \
		doff=$$($(HELM_BIN) template charts/tatara-operator --set dashboard.enabled=false | grep -c 'grafana_dashboard: "1"' || true); \
		if [ "$$don" -ne 1 ] || [ "$$doff" -ne 0 ]; then \
			echo "chart-lint: dashboard ConfigMap gating broken (enabled renders $$don want 1, disabled renders $$doff want 0)"; \
			exit 1; \
		fi
	@python3 -m json.tool charts/tatara-operator/dashboards/tatara-loop.json >/dev/null || \
		{ echo "chart-lint: dashboards/tatara-loop.json is not valid JSON"; exit 1; }
	$(HELM_BIN) lint charts/tatara-project -f deploy-samples/tatara-project-values.yaml
	@echo "chart-lint: placement knobs must render NOTHING when unset (rule 14)"
	@out="$$($(HELM_BIN) template release charts/tatara-operator -s templates/deployment.yaml)"; \
		for k in nodeSelector tolerations affinity topologySpreadConstraints strategy; do \
			if echo "$$out" | grep -q "^\( *\)$$k:"; then \
				echo "chart-lint: default render emitted $$k - the chart must stay cluster-agnostic (rule 14)"; \
				exit 1; \
			fi; \
		done
	@echo "chart-lint: placement knobs must render when the deployer sets them"
	@vals="$$(mktemp)"; trap 'rm -f "$$vals"' EXIT; \
		printf '%s\n' \
			'nodeSelector: {kubernetes.io/os: linux}' \
			'tolerations: [{key: dedicated, operator: Exists, effect: NoSchedule}]' \
			'affinity: {nodeAffinity: {requiredDuringSchedulingIgnoredDuringExecution: {nodeSelectorTerms: [{matchExpressions: [{key: node-role.kubernetes.io/control-plane, operator: Exists}]}]}}}' \
			'topologySpreadConstraints: [{maxSkew: 1, topologyKey: kubernetes.io/hostname, whenUnsatisfiable: ScheduleAnyway, labelSelector: {matchLabels: {app.kubernetes.io/name: tatara-operator}}}]' \
			'strategy: {type: RollingUpdate, rollingUpdate: {maxSurge: 0, maxUnavailable: 1}}' \
			'mcpScheduling: {nodeSelector: {node-role.kubernetes.io/control-plane: ""}}' \
			> "$$vals"; \
		out="$$($(HELM_BIN) template release charts/tatara-operator -f "$$vals" -s templates/deployment.yaml)"; \
		for k in nodeSelector tolerations affinity topologySpreadConstraints strategy; do \
			echo "$$out" | grep -q "^\( *\)$$k:" || { echo "chart-lint: $$k set but not rendered"; exit 1; }; \
		done; \
		echo "$$out" | grep -q "maxSurge: 0" || { echo "chart-lint: strategy.rollingUpdate not rendered"; exit 1; }; \
		cout="$$($(HELM_BIN) template release charts/tatara-operator -f "$$vals" -s templates/configmap.yaml)"; \
		echo "$$cout" | grep -q 'MCP_SCHEDULING:.*node-role.kubernetes.io/control-plane' \
			|| { echo "chart-lint: mcpScheduling set but not rendered into MCP_SCHEDULING"; exit 1; }
	@echo "chart-lint: MCP_SCHEDULING must default to an empty JSON object"
	@$(HELM_BIN) template release charts/tatara-operator -s templates/configmap.yaml \
		| grep -q 'MCP_SCHEDULING: "{}"' \
		|| { echo "chart-lint: MCP_SCHEDULING missing or non-empty by default"; exit 1; }

rbac:
	mkdir -p $(RBAC_GEN_DIR)
	$(CONTROLLER_GEN) rbac:roleName=tatara-operator-manager paths="./internal/controller/..." output:dir=$(RBAC_GEN_DIR)

rbac-check:
	HELM_BIN="$(HELM_BIN)" CONTROLLER_GEN="$(CONTROLLER_GEN)" RBAC_GEN_DIR="$(RBAC_GEN_DIR)" bash hack/check-rbac-drift.sh

# release.yml's verify-pin/fanout-alarm scripts, run against a stubbed forge.
test-cd-jobs:
	bash hack/test-cd-release-jobs.sh

ci: generate manifests lint test rbac-check chart-lint test-cd-jobs

clean:
	rm -rf bin dist
