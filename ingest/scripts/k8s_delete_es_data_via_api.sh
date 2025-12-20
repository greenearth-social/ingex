#!/bin/bash
# Delete and recreate Elasticsearch indices to free up disk space
# This is the recommended approach when disk is too full for delete-by-query
# See: https://www.elastic.co/docs/troubleshoot/elasticsearch/fix-watermark-errors

set -e

ENVIRONMENT="${ENVIRONMENT:-local}"
NAMESPACE="greenearth-${ENVIRONMENT}"

echo "Running ES index deletion and recreation in ${ENVIRONMENT} environment (namespace: ${NAMESPACE})"
echo ""

# Get the elastic superuser credentials (needed for index deletion)
ES_USERNAME="elastic"
ES_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n "${NAMESPACE}" -o jsonpath='{.data.elastic}' | base64 -d)

if [ -z "$ES_PASSWORD" ]; then
  echo "Error: Could not retrieve elastic superuser password"
  exit 1
fi

echo "Using elastic superuser (required for index deletion)"
echo ""

# The internal service name for Elasticsearch (HTTPS)
ES_HOST="https://greenearth-es-http:9200"

echo "Will connect to: ${ES_HOST}"
echo "WARNING: This will DELETE and RECREATE all data indices!"
echo "This deletes the actual indices: posts_v1, likes_v1, post_tombstones_v1, like_tombstones_v1"
echo "The indices will be recreated from templates automatically."
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

# Create a job that deletes and recreates the indices
kubectl run es-index-recreation-$(date +%s) \
  --namespace="${NAMESPACE}" \
  --image=curlimages/curl:latest \
  --restart=Never \
  --rm -i --tty \
  --env="ES_HOST=${ES_HOST}" \
  --env="ES_USERNAME=${ES_USERNAME}" \
  --env="ES_PASSWORD=${ES_PASSWORD}" \
  -- sh -c '
echo "Step 1: Removing read-only blocks from all indices..."
curl -k -X PUT "${ES_HOST}/_all/_settings" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{\"index.blocks.read_only_allow_delete\": null}"

echo ""
echo ""
echo "Step 2: Deleting posts_v1 index..."
curl -k -X DELETE "${ES_HOST}/posts_v1" \
  -u "${ES_USERNAME}:${ES_PASSWORD}"

echo ""
echo ""
echo "Step 3: Deleting likes_v1 index..."
curl -k -X DELETE "${ES_HOST}/likes_v1" \
  -u "${ES_USERNAME}:${ES_PASSWORD}"

echo ""
echo ""
echo "Step 4: Deleting post_tombstones_v1 index..."
curl -k -X DELETE "${ES_HOST}/post_tombstones_v1" \
  -u "${ES_USERNAME}:${ES_PASSWORD}"

echo ""
echo ""
echo "Step 5: Deleting like_tombstones_v1 index (if exists)..."
curl -k -X DELETE "${ES_HOST}/like_tombstones_v1" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
  2>/dev/null || echo "like_tombstones_v1 does not exist, skipping"

echo ""
echo ""
echo "Step 6: Resetting disk watermark settings to defaults..."
curl -k -X PUT "${ES_HOST}/_cluster/settings" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
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
echo "Step 7: Verifying cluster health..."
curl -k -X GET "${ES_HOST}/_cluster/health?pretty" \
  -u "${ES_USERNAME}:${ES_PASSWORD}"

echo ""
echo ""
echo "Deletion complete! Indices removed."
'

echo ""
echo "Step 8: Recreating indices with bootstrap job..."

# Find the git repository root
GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
if [ -z "$GIT_ROOT" ]; then
  echo "Error: Not in a git repository. Cannot locate bootstrap job YAML file."
  exit 1
fi

BOOTSTRAP_JOB_YAML="${GIT_ROOT}/index/deploy/k8s/base/bootstrap-job.yaml"
TEMPLATES_DIR="${GIT_ROOT}/index/deploy/k8s/base/templates"

if [ ! -f "$BOOTSTRAP_JOB_YAML" ]; then
  echo "Warning: bootstrap job YAML not found at: ${BOOTSTRAP_JOB_YAML}"
  echo "Cannot recreate indices automatically. You may need to apply the bootstrap job manually."
  exit 1
fi

if [ ! -d "$TEMPLATES_DIR" ]; then
  echo "Warning: templates directory not found at: ${TEMPLATES_DIR}"
  echo "Cannot apply index templates and aliases."
  exit 1
fi

# Apply all index templates and aliases ConfigMaps
echo "Applying index templates and aliases ConfigMaps..."
kubectl apply -f "${TEMPLATES_DIR}/" -n "${NAMESPACE}"

# Delete existing bootstrap job if it exists (to allow recreation)
kubectl delete job elasticsearch-bootstrap -n "${NAMESPACE}" 2>/dev/null || true

# Create new bootstrap job from the YAML file
echo "Creating bootstrap job to recreate indices..."
kubectl apply -f "$BOOTSTRAP_JOB_YAML" -n "$NAMESPACE"

echo ""
echo "Waiting for bootstrap job to complete..."
kubectl wait --for=condition=complete --timeout=300s job/elasticsearch-bootstrap -n "${NAMESPACE}" || {
  echo "Warning: Bootstrap job did not complete within timeout. Check job status:"
  echo "  kubectl get jobs -n ${NAMESPACE}"
  echo "  kubectl logs -n ${NAMESPACE} job/elasticsearch-bootstrap"
}

echo ""
echo "Done! Indices have been recreated."
echo ""
echo "Check disk space with:"
ES_POD=$(kubectl get pods -n "${NAMESPACE}" -l common.k8s.elastic.co/type=elasticsearch -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$ES_POD" ]; then
  echo "kubectl exec -n ${NAMESPACE} ${ES_POD} -- df -h /usr/share/elasticsearch/data"
else
  echo "kubectl exec -n ${NAMESPACE} <elasticsearch-pod-name> -- df -h /usr/share/elasticsearch/data"
  echo "(Run 'kubectl get pods -n ${NAMESPACE}' to find the Elasticsearch pod name)"
fi
