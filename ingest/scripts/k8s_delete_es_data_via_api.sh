#!/bin/bash
# Delete and recreate Elasticsearch indices to free up disk space
# This is the recommended approach when disk is too full for delete-by-query
# See: https://www.elastic.co/docs/troubleshoot/elasticsearch/fix-watermark-errors

set -e

ENVIRONMENT="${1:-stage}"
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
echo "This deletes the indices behind aliases: posts, likes, post_tombstones, like_tombstones"
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
echo "Step 2: Deleting posts index (via alias)..."
curl -k -X DELETE "${ES_HOST}/posts" \
  -u "${ES_USERNAME}:${ES_PASSWORD}"

echo ""
echo ""
echo "Step 3: Deleting likes index (via alias)..."
curl -k -X DELETE "${ES_HOST}/likes" \
  -u "${ES_USERNAME}:${ES_PASSWORD}"

echo ""
echo ""
echo "Step 4: Deleting post_tombstones index (via alias)..."
curl -k -X DELETE "${ES_HOST}/post_tombstones" \
  -u "${ES_USERNAME}:${ES_PASSWORD}"

echo ""
echo ""
echo "Step 5: Deleting like_tombstones index (via alias, if exists)..."
curl -k -X DELETE "${ES_HOST}/like_tombstones" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
  2>/dev/null || echo "like_tombstones does not exist, skipping"

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

# Check if bootstrap job exists
if ! kubectl get job elasticsearch-bootstrap -n "${NAMESPACE}" &>/dev/null; then
  echo "Warning: elasticsearch-bootstrap job not found in namespace ${NAMESPACE}"
  echo "Cannot recreate indices automatically. You may need to apply the bootstrap job first."
  exit 1
fi

# Delete existing bootstrap job if it exists (to allow recreation)
kubectl delete job elasticsearch-bootstrap -n "${NAMESPACE}" 2>/dev/null || true

# Create new bootstrap job from the existing job definition
echo "Creating bootstrap job to recreate indices..."
kubectl create job --from=cronjob/elasticsearch-bootstrap elasticsearch-bootstrap-manual-$(date +%s) -n "${NAMESPACE}" 2>/dev/null || \
  kubectl create job --from=job/elasticsearch-bootstrap elasticsearch-bootstrap-manual-$(date +%s) -n "${NAMESPACE}"

echo ""
echo "Waiting for bootstrap job to complete..."
kubectl wait --for=condition=complete --timeout=300s job -l job-name=elasticsearch-bootstrap-manual-$(date +%s) -n "${NAMESPACE}" || {
  echo "Warning: Bootstrap job did not complete within timeout. Check job status:"
  echo "  kubectl get jobs -n ${NAMESPACE} | grep elasticsearch-bootstrap"
  echo "  kubectl logs -n ${NAMESPACE} job/elasticsearch-bootstrap-manual-$(date +%s)"
}

echo ""
echo "Done! Check disk space with:"
echo "kubectl exec -n ${NAMESPACE} greenearth-es-data-only-0 -- df -h /usr/share/elasticsearch/data"
