#!/bin/bash

# Green Earth Ingex - Elasticsearch Snapshot GCP Setup Script
# Sets up GCS bucket and service accounts for ES snapshots via Workload Identity
# Run once per environment (stage, prod)

set -e

GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"

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

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first."
        exit 1
    fi

    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
        log_error "Please log in to gcloud first: gcloud auth login"
        exit 1
    fi

    log_info "Prerequisites check complete."
}

setup_snapshot_bucket() {
    local bucket_name="$GE_GCP_PROJECT_ID-es-snapshots-$GE_ENVIRONMENT"
    local sa_email="es-snapshot-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com"

    log_info "Setting up snapshot bucket: $bucket_name"

    if ! gcloud storage buckets describe "gs://$bucket_name" > /dev/null 2>&1; then
        gcloud storage buckets create "gs://$bucket_name" \
            --location="$GE_GCP_REGION" \
            --uniform-bucket-level-access \
            --project="$GE_GCP_PROJECT_ID"
        log_info "Bucket created: $bucket_name"
    else
        log_info "Bucket already exists: $bucket_name"
    fi

    log_info "Granting objectAdmin to $sa_email on bucket $bucket_name..."
    gcloud storage buckets add-iam-policy-binding "gs://$bucket_name" \
        --member="serviceAccount:$sa_email" \
        --role="roles/storage.objectAdmin"
    log_info "Bucket permissions configured."
}

create_snapshot_service_account() {
    local sa_name="es-snapshot-$GE_ENVIRONMENT"
    local sa_email="$sa_name@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com"

    log_info "Creating snapshot service account: $sa_email"

    if ! gcloud iam service-accounts describe "$sa_email" --project="$GE_GCP_PROJECT_ID" > /dev/null 2>&1; then
        gcloud iam service-accounts create "$sa_name" \
            --display-name="Elasticsearch Snapshot Service Account ($GE_ENVIRONMENT)" \
            --description="Service account for ES snapshots to GCS in $GE_ENVIRONMENT" \
            --project="$GE_GCP_PROJECT_ID"
        log_info "Service account created: $sa_email"
    else
        log_info "Service account already exists: $sa_email"
    fi
}

setup_workload_identity() {
    local sa_email="es-snapshot-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com"
    local k8s_namespace="greenearth-$GE_ENVIRONMENT"
    local k8s_sa="es-node-sa"
    local member="serviceAccount:$GE_GCP_PROJECT_ID.svc.id.goog[$k8s_namespace/$k8s_sa]"

    log_info "Binding K8s SA $k8s_namespace/$k8s_sa to GCP SA $sa_email via Workload Identity..."

    gcloud iam service-accounts add-iam-policy-binding "$sa_email" \
        --project="$GE_GCP_PROJECT_ID" \
        --role="roles/iam.workloadIdentityUser" \
        --member="$member"

    log_info "Workload Identity binding configured."
}

main() {
    echo "=================================================="
    echo "Green Earth Ingex - ES Snapshot GCP Setup"
    echo "Environment: $GE_ENVIRONMENT"
    echo "Project:     $GE_GCP_PROJECT_ID"
    echo "Region:      $GE_GCP_REGION"
    echo "=================================================="
    echo

    if [ "$GE_ENVIRONMENT" = "local" ]; then
        log_error "Snapshots are not configured for the local environment."
        exit 1
    fi

    check_prerequisites
    create_snapshot_service_account
    setup_snapshot_bucket
    setup_workload_identity

    echo
    log_info "GCP snapshot setup complete for $GE_ENVIRONMENT."
    echo
    echo "Next steps:"
    echo "1. Deploy to $GE_ENVIRONMENT: cd index && ./deploy.sh $GE_ENVIRONMENT --ctypes init --no-timeout"
    echo "2. Confirm snapshot repo: GET /_snapshot/gcs_backup"
    echo "3. Confirm SLM policy:    GET /_slm/policy/daily-snapshots"
    echo
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --environment)
            GE_ENVIRONMENT="$2"
            shift 2
            ;;
        --project-id)
            GE_GCP_PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            GE_GCP_REGION="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --environment ENV    Target environment: stage or prod (default: stage)"
            echo "  --project-id ID      GCP project ID (default: greenearth-471522)"
            echo "  --region REGION      GCP region (default: us-east1)"
            echo "  --help               Show this help message"
            echo
            echo "Environment variables: GE_ENVIRONMENT, GE_GCP_PROJECT_ID, GE_GCP_REGION"
            echo
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

main
