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

CHART_CRD_DIR := charts/tatara-operator/crds

# Resolve helm binary via mise to avoid homebrew helm 4.x shadow.
HELM_BIN := $(shell mise exec -- bash -c 'echo $$PATH' 2>/dev/null | tr ':' '\n' | grep -m1 'mise/installs/helm' || true)
ifdef HELM_BIN
HELM_BIN := $(HELM_BIN)/helm
else
HELM_BIN := helm
endif

.PHONY: all generate manifests test lint build image fmt tidy chart-lint clean ci

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

ci: generate manifests lint test

clean:
	rm -rf bin dist
