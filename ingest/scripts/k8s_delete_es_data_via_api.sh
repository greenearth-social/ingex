#!/bin/bash
# Run the ES data deletion script inside the Kubernetes cluster
# This uses kubectl to create a temporary pod that can access the internal Elasticsearch service

set -e

ENVIRONMENT="${1:-stage}"
NAMESPACE="greenearth-${ENVIRONMENT}"

echo "Running ES data deletion in ${ENVIRONMENT} environment (namespace: ${NAMESPACE})"
echo ""

# Get the ES credentials from the secret
ES_USERNAME=$(kubectl get secret es-service-user-secret -n "${NAMESPACE}" -o jsonpath='{.data.username}' | base64 -d)
ES_PASSWORD=$(kubectl get secret es-service-user-secret -n "${NAMESPACE}" -o jsonpath='{.data.password}' | base64 -d)

if [ -z "$ES_USERNAME" ] || [ -z "$ES_PASSWORD" ]; then
  echo "Error: Could not retrieve ES credentials from secret"
  exit 1
fi

echo "Found ES credentials for user: ${ES_USERNAME}"
echo ""

# The internal service name for Elasticsearch (HTTPS)
ES_HOST="https://greenearth-es-http:9200"

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
  --env="ES_USERNAME=${ES_USERNAME}" \
  --env="ES_PASSWORD=${ES_PASSWORD}" \
  -- sh -c '
echo "Removing read-only blocks..."
curl -k -X PUT "${ES_HOST}/_all/_settings" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{\"index.blocks.read_only_allow_delete\": null}"

echo ""
echo ""
echo "Deleting all documents from posts index..."
curl -k -X POST "${ES_HOST}/posts/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{\"query\": {\"match_all\": {}}}"

echo ""
echo ""
echo "Deleting all documents from likes index..."
curl -k -X POST "${ES_HOST}/likes/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{\"query\": {\"match_all\": {}}}"

echo ""
echo ""
echo "Deleting all documents from post-tombstones index..."
curl -k -X POST "${ES_HOST}/post-tombstones/_delete_by_query?conflicts=proceed&wait_for_completion=false" \
  -u "${ES_USERNAME}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{\"query\": {\"match_all\": {}}}"

echo ""
echo ""
echo "Deletion tasks started!"
'

echo ""
echo "Done! Check progress with:"
echo "kubectl exec -n ${NAMESPACE} -it greenearth-es-data-only-0 -- curl -k -u ${ES_USERNAME}:${ES_PASSWORD} -X GET \"https://localhost:9200/_tasks?detailed=true&actions=*delete/byquery\""
