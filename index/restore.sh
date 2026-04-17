#!/bin/bash

# Green Earth Ingex - Elasticsearch Restore Script
# Restores from a GCS snapshot. The ES cluster must be healthy and accepting requests.
# For disk-full scenarios, first increase storage via deploy.sh --ctypes resource (or --ctypes init
# for a full rebuild), then run this script.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_ENVIRONMENT=""
SNAPSHOT_NAME=""
DRY_RUN=false
LOCAL_PORT=19200

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_usage() {
    echo "Usage: $0 --environment <env> [options]"
    echo ""
    echo "Options:"
    echo "  --environment ENV    Target environment: stage or prod (required)"
    echo "  --snapshot NAME      Snapshot name to restore (default: most recent SUCCESS snapshot)"
    echo "  --dry-run            Print steps without executing restore"
    echo "  --help               Show this help message"
    echo ""
    echo "Prerequisites:"
    echo "  - The ES cluster must be healthy and accepting requests."
    echo "  - For disk-full failures, increase storage first:"
    echo "      ./deploy.sh <env> --ctypes resource   (or --ctypes init for full rebuild)"
    echo "    then re-run this script."
}

setup_kubectl_context() {
    log_info "Getting credentials for GKE cluster..."
    GE_K8S_CLUSTER="${GE_K8S_CLUSTER:-greenearth-$GE_ENVIRONMENT-cluster}"
    gcloud container clusters get-credentials "$GE_K8S_CLUSTER" \
        --location="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID"
    log_info "kubectl context set."
}

start_port_forward() {
    local namespace="greenearth-$GE_ENVIRONMENT"
    log_info "Starting port-forward on localhost:$LOCAL_PORT..."
    kubectl port-forward service/greenearth-es-http "$LOCAL_PORT:9200" -n "$namespace" &
    PF_PID=$!
    trap "kill $PF_PID 2>/dev/null || true" EXIT
    sleep 5
    log_info "Port-forward running (PID $PF_PID)."
}

get_elastic_password() {
    local namespace="greenearth-$GE_ENVIRONMENT"
    ELASTIC_PASSWORD=$(kubectl get secret greenearth-es-elastic-user \
        -n "$namespace" \
        -o go-template='{{.data.elastic | base64decode}}')
}

es_curl() {
    curl -k -s -u "elastic:$ELASTIC_PASSWORD" "$@"
}

register_snapshot_repo() {
    local bucket="greenearth-471522-es-snapshots-$GE_ENVIRONMENT"
    log_info "Registering GCS snapshot repository (bucket: $bucket)..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would PUT /_snapshot/gcs_backup with bucket=$bucket"
        return
    fi

    es_curl --fail-with-body -X PUT "https://localhost:$LOCAL_PORT/_snapshot/gcs_backup" \
        -H "Content-Type: application/json" \
        -d "{
          \"type\": \"gcs\",
          \"settings\": {
            \"bucket\": \"$bucket\",
            \"application_name\": \"elasticsearch\"
          }
        }"
    echo ""
    log_info "Snapshot repository registered."
}

select_snapshot() {
    log_info "Fetching available snapshots..."

    local snapshots_json
    snapshots_json=$(es_curl "https://localhost:$LOCAL_PORT/_snapshot/gcs_backup/_all?verbose=false")

    echo ""
    echo "Available snapshots:"
    echo "$snapshots_json" | grep -o '"snapshot":"[^"]*"' | sed 's/"snapshot":"//;s/"//' || true
    echo "$snapshots_json" | grep -o '"state":"[^"]*"' | sed 's/"state":"//;s/"//' || true
    echo ""

    if [ -n "$SNAPSHOT_NAME" ]; then
        log_info "Using specified snapshot: $SNAPSHOT_NAME"
        return
    fi

    SNAPSHOT_NAME=$(echo "$snapshots_json" | \
        python3 -c "
import sys, json
data = json.load(sys.stdin)
snaps = [s for s in data.get('snapshots', []) if s.get('state') == 'SUCCESS']
if not snaps:
    print('')
else:
    print(snaps[-1]['snapshot'])
" 2>/dev/null || echo "")

    if [ -z "$SNAPSHOT_NAME" ]; then
        log_error "No successful snapshots found. Cannot proceed."
        exit 1
    fi

    log_info "Selected most recent successful snapshot: $SNAPSHOT_NAME"
}

SNAPSHOT_INDICES="posts*,post_tombstones*,post-tombstones*,hashtags*,likes*,like_tombstones*,like-tombstones*,inferences*"

delete_existing_indices() {
    log_info "Checking for existing indices on cluster..."

    local existing existing_csv
    existing=$(es_curl "https://localhost:$LOCAL_PORT/_cat/indices/$SNAPSHOT_INDICES?h=index" 2>/dev/null | tr '\n' ' ' | xargs)

    if [ -z "$existing" ]; then
        log_info "No existing indices found. Proceeding with restore."
        return
    fi

    existing_csv=$(echo "$existing" | tr ' ' ',')

    log_warn "The following indices already exist on the cluster:"
    echo "  $existing"
    echo ""

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would delete existing indices before restoring."
        return
    fi

    read -r -p "Delete these indices before restoring? [y/N] " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        log_info "Deleting existing indices..."
        es_curl --fail-with-body -X DELETE "https://localhost:$LOCAL_PORT/$existing_csv"
        echo ""
        log_info "Indices deleted."
    else
        log_error "Restore aborted."
        exit 1
    fi
}

restore_snapshot() {
    log_info "Restoring snapshot: $SNAPSHOT_NAME"

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would POST /_snapshot/gcs_backup/$SNAPSHOT_NAME/_restore"
        log_info "[DRY RUN] Would monitor restore status until complete"
        return
    fi

    es_curl --fail-with-body -X POST \
        "https://localhost:$LOCAL_PORT/_snapshot/gcs_backup/$SNAPSHOT_NAME/_restore?wait_for_completion=false" \
        -H "Content-Type: application/json" \
        -d "{
          \"indices\": \"$SNAPSHOT_INDICES\",
          \"include_global_state\": false,
          \"feature_states\": []
        }"
    echo ""
    log_info "Restore initiated. Monitoring status..."

    while true; do
        local state
        state=$(es_curl "https://localhost:$LOCAL_PORT/_snapshot/gcs_backup/$SNAPSHOT_NAME" | \
            python3 -c "
import sys, json
data = json.load(sys.stdin)
snaps = data.get('snapshots', [{}])
print(snaps[0].get('state', 'UNKNOWN') if snaps else 'UNKNOWN')
" 2>/dev/null || echo "UNKNOWN")

        echo -n "  state: $state"
        if [ "$state" = "SUCCESS" ]; then
            echo ""
            log_info "Restore completed successfully!"
            return 0
        elif [ "$state" = "FAILED" ] || [ "$state" = "PARTIAL" ]; then
            echo ""
            log_error "Restore ended with state: $state"
            return 1
        fi
        echo " — waiting..."
        sleep 15
    done
}

main() {
    if [ "$GE_ENVIRONMENT" = "local" ]; then
        log_error "Restore is not supported for local environment."
        exit 1
    fi

    echo "=================================================="
    echo "Green Earth Ingex - Elasticsearch Restore"
    echo "Environment: $GE_ENVIRONMENT"
    echo "Project:     $GE_GCP_PROJECT_ID"
    echo "=================================================="
    echo

    setup_kubectl_context
    get_elastic_password
    start_port_forward
    register_snapshot_repo
    select_snapshot
    delete_existing_indices
    restore_snapshot

    log_info "Done."
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --environment)
            GE_ENVIRONMENT="$2"
            shift 2
            ;;
        --snapshot)
            SNAPSHOT_NAME="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

if [ -z "$GE_ENVIRONMENT" ]; then
    log_error "--environment is required"
    print_usage
    exit 1
fi

if [ "$GE_ENVIRONMENT" != "stage" ] && [ "$GE_ENVIRONMENT" != "prod" ]; then
    log_error "Invalid environment: $GE_ENVIRONMENT (must be stage or prod)"
    exit 1
fi

main
