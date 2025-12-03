#!/bin/bash
# Run the ES data deletion script inside the Kubernetes cluster
# This uses kubectl to create a temporary pod that can access the internal Elasticsearch service

set -e

ENVIRONMENT="${1:-stage}"
NAMESPACE="ingex-${ENVIRONMENT}"

echo "Running ES data deletion in ${ENVIRONMENT} environment (namespace: ${NAMESPACE})"
echo ""

# Get the ES API key from the secret
ES_API_KEY=$(kubectl get secret elasticsearch-api-keys -n "${NAMESPACE}" -o jsonpath='{.data.service-user-key}' | base64 -d)

if [ -z "$ES_API_KEY" ]; then
  echo "Error: Could not retrieve ES API key from secret"
  exit 1
fi

echo "Found ES API key"
echo ""

# The internal service name for Elasticsearch
ES_HOST="http://elasticsearch-internal:9200"

echo "Will connect to: ${ES_HOST}"
echo "WARNING: This will delete ALL data from Elasticsearch indices!"
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

# Create a job that runs the deletion commands
kubectl run es-data-deletion-$(date +%s) \
  --namespace="${NAMESPACE}" \
  --image=curlimages/curl:latest \
  --restart=Never \
  --rm -i --tty \
  --env="ES_HOST=${ES_HOST}" \
  --env="ES_API_KEY=${ES_API_KEY}" \
  -- sh -c '
echo "Removing read-only blocks..."
curl -X PUT "${ES_HOST}/_all/_settings" \
  -H "Authorization: ApiKey ${ES_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"index.blocks.read_only_allow_delete\": null}"

echo ""
echo ""
echo "Deleting all documents from posts index..."
curl -X POST "${ES_HOST}/posts/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -H "Authorization: ApiKey ${ES_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"query\": {\"match_all\": {}}}"

echo ""
echo ""
echo "Deleting all documents from likes index..."
curl -X POST "${ES_HOST}/likes/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -H "Authorization: ApiKey ${ES_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"query\": {\"match_all\": {}}}"

echo ""
echo ""
echo "Deleting all documents from post-tombstones index..."
curl -X POST "${ES_HOST}/post-tombstones/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -H "Authorization: ApiKey ${ES_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"query\": {\"match_all\": {}}}"

echo ""
echo ""
echo "Deletion tasks started!"
'

echo ""
echo "Done! Check progress with:"
echo "kubectl exec -n ${NAMESPACE} -it elasticsearch-0 -- curl -X GET \"http://localhost:9200/_tasks?detailed=true&actions=*delete/byquery\""
