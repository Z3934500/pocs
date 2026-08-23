#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:?environment required: dev, staging, or production}"
IMAGE_REPOSITORY="${2:?image repository required}"
IMAGE_TAG="${3:?image tag required}"
DRY_RUN="${4:-false}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ENV_PATH="${ROOT_DIR}/deploy/k8s/environments/${ENVIRONMENT}"
NAMESPACE="cce-${ENVIRONMENT}"
IMAGE="${IMAGE_REPOSITORY}:${IMAGE_TAG}"

if [ "${ENVIRONMENT}" = "production" ]; then
  NAMESPACE="cce-production"
fi

RENDERED="/tmp/cce-${ENVIRONMENT}-rendered.yaml"
PATCHED="/tmp/cce-${ENVIRONMENT}-patched.yaml"

echo "Rendering environment: ${ENVIRONMENT}"
kubectl kustomize --load-restrictor=LoadRestrictionsNone "${ENV_PATH}" > "${RENDERED}"

echo "Setting CCE API/importer image to: ${IMAGE}"
sed -E "s#ghcr\.io/OWNER/cce-feature-platform:[A-Za-z0-9._-]+#${IMAGE}#g" "${RENDERED}" > "${PATCHED}"

if [ "${DRY_RUN}" = "true" ]; then
  kubectl apply --dry-run=server -f "${PATCHED}"
  echo "Dry run completed for ${ENVIRONMENT}."
  exit 0
fi

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "${PATCHED}"
kubectl rollout status deployment/cce-feature-platform -n "${NAMESPACE}"
echo "Deployment completed for ${ENVIRONMENT} with image ${IMAGE}."
