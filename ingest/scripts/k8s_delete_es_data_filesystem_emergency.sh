#!/bin/bash
# Manually free up disk space on Elasticsearch data node
# This deletes index data directly from the filesystem when ES is unresponsive

set -e

GE_ENVIRONMENT="${1:-stage}"
GE_K8S_NAMESPACE="greenearth-${GE_ENVIRONMENT}"
K8S_POD="greenearth-es-data-only-0"

echo "WARNING: This will delete index data directories directly from the filesystem!"
echo "Environment: ${GE_ENVIRONMENT}"
echo "Namespace: ${GE_K8S_NAMESPACE}"
echo "Pod: ${K8S_POD}"
echo ""

# Check current disk usage
echo "Current disk usage:"
kubectl exec -n "${GE_K8S_NAMESPACE}" "${K8S_POD}" -- df -h /usr/share/elasticsearch/data
echo ""

read -p "Are you sure you want to delete all index data? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

echo ""
echo "Listing index directories..."
kubectl exec -n "${GE_K8S_NAMESPACE}" "${K8S_POD}" -- sh -c 'ls -la /usr/share/elasticsearch/data/indices/' || true

echo ""
echo "Deleting ALL index data..."
kubectl exec -n "${GE_K8S_NAMESPACE}" "${K8S_POD}" -- sh -c 'rm -rf /usr/share/elasticsearch/data/indices/*' || true

echo ""
echo "Disk usage after cleanup:"
kubectl exec -n "${GE_K8S_NAMESPACE}" "${K8S_POD}" -- df -h /usr/share/elasticsearch/data
echo ""

echo "Restarting Elasticsearch pod to recover..."
kubectl delete pod -n "${GE_K8S_NAMESPACE}" "${K8S_POD}"

echo ""
echo "Done! Wait for the pod to restart and ES to recover."
echo "Check status with: kubectl get pods -n ${GE_K8S_NAMESPACE}"
