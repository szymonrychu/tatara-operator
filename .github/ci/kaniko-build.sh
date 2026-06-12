#!/usr/bin/env bash
# Dispatch a one-shot kaniko Job that builds this repo's image and pushes it to
# harbor. Runs on the ARC runner (in-cluster, namespace arc-runners) and uses the
# runner's mounted ServiceAccount (tatara-ci-dispatcher) to manage the Job and its
# short-lived clone/docker-config secrets. The dispatcher SA may create/delete
# secrets but NOT read them (no secrets:get) so a build cannot read the ARC GitHub
# App key that also lives in arc-runners. Harbor push auth and the private-repo
# clone token come from the workflow's GitHub secrets. Streams kaniko logs and
# propagates the Job result.
set -euo pipefail

REPO="${1:?repo name required}"          # e.g. tatara-operator
NS="arc-runners"
SHORT_SHA="${GITHUB_SHA:0:7}"
VERSION="$(git describe --tags --always --dirty)"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB="kaniko-${REPO}-${SHORT_SHA}"
CLONE_SECRET="clone-${REPO}-${SHORT_SHA}"
DOCKERCFG_SECRET="dockercfg-${REPO}-${SHORT_SHA}"

# shellcheck disable=SC2329  # invoked via the trap below
cleanup() {
  kubectl -n "$NS" delete secret "$CLONE_SECRET" "$DOCKERCFG_SECRET" \
    --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Idempotency: clear leftovers from a prior run of the same commit (create, not
# apply, so the SA needs no secrets:get).
kubectl -n "$NS" delete job "$JOB" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl -n "$NS" delete secret "$CLONE_SECRET" "$DOCKERCFG_SECRET" --ignore-not-found >/dev/null 2>&1 || true

# Short-lived clone-token secret (kaniko git-context auth for the private repo).
kubectl -n "$NS" create secret generic "$CLONE_SECRET" \
  --from-literal=username=x-access-token \
  --from-literal=token="${GITHUB_TOKEN:?GITHUB_TOKEN required}" >/dev/null

# Short-lived harbor docker-config secret (kaniko push auth), from workflow secrets.
kubectl -n "$NS" create secret docker-registry "$DOCKERCFG_SECRET" \
  --docker-server=harbor.szymonrichert.pl \
  --docker-username="${HARBOR_USERNAME:?HARBOR_USERNAME required}" \
  --docker-password="${HARBOR_PASSWORD:?HARBOR_PASSWORD required}" >/dev/null

# Create the kaniko Job.
kubectl create -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB}
  namespace: ${NS}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  activeDeadlineSeconds: 1500
  template:
    spec:
      restartPolicy: Never
      imagePullSecrets:
        - name: regcred
      containers:
        - name: kaniko
          image: harbor.szymonrichert.pl/containers/kaniko-executor:v1.24.0-debug
          command: ["/kaniko/executor"]
          args:
            - --context=git://github.com/szymonrychu/${REPO}.git#${GITHUB_SHA}
            - --dockerfile=Dockerfile
            - --destination=harbor.szymonrichert.pl/containers/${REPO}:${SHORT_SHA}
            - --destination=harbor.szymonrichert.pl/containers/${REPO}:${VERSION}
            - --build-arg=VERSION=${VERSION}
            - --build-arg=COMMIT=${SHORT_SHA}
            - --build-arg=DATE=${BUILD_DATE}
            - --compressed-caching=false
            - --cache-copy-layers=true
          env:
            - name: GIT_USERNAME
              valueFrom:
                secretKeyRef: { name: ${CLONE_SECRET}, key: username }
            - name: GIT_PASSWORD
              valueFrom:
                secretKeyRef: { name: ${CLONE_SECRET}, key: token }
          volumeMounts:
            - name: docker-config
              mountPath: /kaniko/.docker
      volumes:
        - name: docker-config
          secret:
            secretName: ${DOCKERCFG_SECRET}
            items:
              - { key: .dockerconfigjson, path: config.json }
EOF

# Wait for the pod, stream kaniko logs to completion, then read the Job result.
for _ in $(seq 1 60); do
  if kubectl -n "$NS" get pod -l job-name="$JOB" -o name 2>/dev/null | grep -q .; then break; fi
  sleep 2
done
kubectl -n "$NS" logs -f "job/${JOB}" || true

for _ in $(seq 1 30); do
  succeeded="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
  failed="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
  if [[ "$succeeded" == "1" ]]; then echo "kaniko: build pushed"; exit 0; fi
  if [[ -n "$failed" && "$failed" != "0" ]]; then echo "kaniko: build failed"; exit 1; fi
  sleep 2
done
echo "kaniko: timed out waiting for Job result"
exit 1
