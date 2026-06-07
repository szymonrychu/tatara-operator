#!/usr/bin/env bash
# Vendors the cnpg Cluster CRD matching the go.mod cnpg version into the
# chart crds dir so envtest can create Cluster objects. Re-run after bumping
# the cnpg dependency. The chart deploys this CRD to real clusters too.
set -euo pipefail
cd "$(dirname "$0")/.."
ver="$(go list -m -f '{{.Version}}' github.com/cloudnative-pg/cloudnative-pg)"
url="https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/${ver}/config/crd/bases/postgresql.cnpg.io_clusters.yaml"
echo "fetching ${url}"
curl -fsSL "${url}" -o charts/tatara-operator/crds/postgresql.cnpg.io_clusters.yaml
echo "wrote charts/tatara-operator/crds/postgresql.cnpg.io_clusters.yaml"
