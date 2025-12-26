#!/bin/bash
# Delete and recreate Elasticsearch indices to free up disk space
# This is the recommended approach when disk is too full for delete-by-query
# See: https://www.elastic.co/docs/troubleshoot/elasticsearch/fix-watermark-errors

set -e

GE_ENVIRONMENT="${GE_ENVIRONMENT:-local}"
K8S_NAMESPACE="greenearth-${GE_ENVIRONMENT}"

echo "Running ES index deletion and recreation in ${GE_ENVIRONMENT} environment (namespace: ${K8S_NAMESPACE})"
echo ""

# Get the elastic superuser credentials (needed for index deletion)
ELASTICSEARCH_USERNAME="elastic"
ELASTICSEARCH_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n "${K8S_NAMESPACE}" -o jsonpath='{.data.elastic}' | base64 -d)

if [ -z "$ELASTICSEARCH_PASSWORD" ]; then
  echo "Error: Could not retrieve elastic superuser password"
  exit 1
fi

echo "Using elastic superuser (required for index deletion)"
echo ""

# The internal service name for Elasticsearch (HTTPS)
GE_ELASTICSEARCH_URL="https://greenearth-es-http:9200"

echo "Will connect to: ${GE_ELASTICSEARCH_URL}"
echo "WARNING: This will DELETE and RECREATE all data indices!"
echo "This deletes the actual indices: posts_v1, likes_v1, post_tombstones_v1, like_tombstones_v1"
echo "The indices will be recreated from templates automatically."
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

# Stop ingest services for stage/prod to prevent them from writing during recreation
if [ "$GE_ENVIRONMENT" = "stage" ] || [ "$GE_ENVIRONMENT" = "prod" ]; then
  echo ""
  echo "Stopping ingest services to prevent writes during recreation..."

  # Find ingestctl script
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  INGESTCTL="${SCRIPT_DIR}/ingestctl.sh"

  if [ -f "$INGESTCTL" ]; then
    # Stop all services
    "$INGESTCTL" stop || {
      echo "Warning: Failed to stop services. Continuing anyway..."
    }
    echo "Ingest services stopped. Waiting 10 seconds for in-flight requests to complete..."
    sleep 10
  else
    echo "Warning: ingestctl.sh not found at ${INGESTCTL}"
    echo "Services will not be stopped. This may cause alias conflicts."
    read -p "Continue anyway? (type 'yes' to confirm): " continue_confirm
    if [ "$continue_confirm" != "yes" ]; then
      echo "Aborted."
      exit 0
    fi
  fi
  echo ""
fi

# Create a job that deletes and recreates the indices
kubectl run es-index-recreation-$(date +%s) \
  --namespace="${K8S_NAMESPACE}" \
  --image=curlimages/curl:latest \
  --restart=Never \
  --rm -i --tty \
  --env="GE_ELASTICSEARCH_URL=${GE_ELASTICSEARCH_URL}" \
  --env="ELASTICSEARCH_USERNAME=${ELASTICSEARCH_USERNAME}" \
  --env="ELASTICSEARCH_PASSWORD=${ELASTICSEARCH_PASSWORD}" \
  -- sh -c '
echo "Removing read-only blocks from all indices..."
curl -k -X PUT "${GE_ELASTICSEARCH_URL}/_all/_settings" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{\"index.blocks.read_only_allow_delete\": null}"

echo ""
echo ""
echo "Deleting posts_v1 index..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/posts_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deleting likes_v1 index..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/likes_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deleting post_tombstones_v1 index..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/post_tombstones_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deleting like_tombstones_v1 index (if exists)..."
curl -k -X DELETE "${GE_ELASTICSEARCH_URL}/like_tombstones_v1" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}" \
  2>/dev/null || echo "like_tombstones_v1 does not exist, skipping"

echo ""
echo ""
echo "Resetting disk watermark settings to defaults..."
curl -k -X PUT "${GE_ELASTICSEARCH_URL}/_cluster/settings" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{
    \"persistent\": {
      \"cluster.routing.allocation.disk.watermark.low\": null,
      \"cluster.routing.allocation.disk.watermark.high\": null,
      \"cluster.routing.allocation.disk.watermark.flood_stage\": null
    },
    \"transient\": {
      \"cluster.routing.allocation.disk.watermark.low\": null,
      \"cluster.routing.allocation.disk.watermark.high\": null,
      \"cluster.routing.allocation.disk.watermark.flood_stage\": null
    }
  }"

echo ""
echo ""
echo "Verifying cluster health..."
curl -k -X GET "${GE_ELASTICSEARCH_URL}/_cluster/health?pretty" \
  -u "${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_PASSWORD}"

echo ""
echo ""
echo "Deletion complete! Indices removed."
'

echo ""
echo "Recreating indices with bootstrap job..."

# Find the git repository root
GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
if [ -z "$GIT_ROOT" ]; then
  echo "Error: Not in a git repository. Cannot locate bootstrap job YAML file."
  exit 1
fi

# Determine which environment overlay to use
K8S_ENV_DIR="${GIT_ROOT}/index/deploy/k8s/environments/${GE_ENVIRONMENT}"
if [ ! -d "$K8S_ENV_DIR" ]; then
  echo "Warning: Environment directory not found at: ${K8S_ENV_DIR}"
  echo "Falling back to base configuration"
  K8S_ENV_DIR="${GIT_ROOT}/index/deploy/k8s/base"
fi

echo "Using Kubernetes configuration from: ${K8S_ENV_DIR}"

# Delete existing bootstrap job if it exists (to allow recreation)
echo "Deleting existing bootstrap job if present..."
kubectl delete job elasticsearch-bootstrap -n "${K8S_NAMESPACE}" 2>/dev/null || true

# Apply all templates using Kustomize to properly substitute variables
echo "Applying index templates, aliases ConfigMaps, and bootstrap job using Kustomize..."
kubectl apply -k "${K8S_ENV_DIR}" -n "${K8S_NAMESPACE}"

echo ""
echo "Waiting for bootstrap job to complete..."
kubectl wait --for=condition=complete --timeout=300s job/elasticsearch-bootstrap -n "${K8S_NAMESPACE}" || {
  echo "Warning: Bootstrap job did not complete within timeout. Check job status:"
  echo "  kubectl get jobs -n ${K8S_NAMESPACE}"
  echo "  kubectl logs -n ${K8S_NAMESPACE} job/elasticsearch-bootstrap"
}

echo ""
echo "Done! Indices have been recreated."

# Restart ingest services for stage/prod
if [ "$GE_ENVIRONMENT" = "stage" ] || [ "$GE_ENVIRONMENT" = "prod" ]; then
  echo ""
  echo "To restart ingest services, run ingetsctl.sh start"
fi

echo ""
echo "Check disk space with:"
ES_POD=$(kubectl get pods -n "${K8S_NAMESPACE}" -l common.k8s.elastic.co/type=elasticsearch -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$ES_POD" ]; then
  echo "kubectl exec -n ${K8S_NAMESPACE} ${ES_POD} -- df -h /usr/share/elasticsearch/data"
else
  echo "kubectl exec -n ${K8S_NAMESPACE} <elasticsearch-pod-name> -- df -h /usr/share/elasticsearch/data"
  echo "(Run 'kubectl get pods -n ${K8S_NAMESPACE}' to find the Elasticsearch pod name)"
fi
