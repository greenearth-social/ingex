GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"
GE_K8S_NAMESPACE="greenearth-${GE_ENVIRONMENT}"

# Confirm environment before proceeding
echo "================================================="
echo "Target Environment: ${GE_ENVIRONMENT}"
echo "Target Namespace:   ${GE_K8S_NAMESPACE}"
echo "================================================="
read -p "Continue with this environment? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Aborted by user"
  exit 1
fi

echo "Proceeding with ${GE_ENVIRONMENT} environment..."
echo ""

ES_PODS=$(kubectl get pods -n "${GE_K8S_NAMESPACE}" -l common.k8s.elastic.co/type=elasticsearch --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null)

if [ -n "$ES_PODS" ]; then
  echo "Checking disk space for all Elasticsearch pods..."
  echo "================================================="
  for ES_POD in $ES_PODS; do
    DISK_USAGE=$(kubectl exec -n "${GE_K8S_NAMESPACE}" "$ES_POD" -- df -h /usr/share/elasticsearch/data 2>/dev/null | tail -n 1 | awk '{print $5}')
    echo "$ES_POD: $DISK_USAGE"
  done
else
  echo "No Elasticsearch pods found in namespace ${GE_K8S_NAMESPACE}"
fi