#!/bin/bash

# Green Earth Ingex - Cloud Run Source Deployment Script
# This script deploys all ingex services to Google Cloud Run using source deployment
# Source deployment uses Google Cloud buildpacks to automatically build from Go source
#
# Prerequisites: Run scripts/gcp_setup.sh first to configure the GCP environment

set -e

# Configuration
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"  # you can override with --environment

# Non-secret configuration
GE_AWS_S3_BUCKET="${GE_AWS_S3_BUCKET:-graze-mega-02}"
GE_AWS_S3_PREFIX="${GE_AWS_S3_PREFIX:-mega/}"

# Service configuration
GE_JETSTREAM_INSTANCES="${GE_JETSTREAM_INSTANCES:-1}"
GE_MEGASTREAM_INSTANCES="${GE_MEGASTREAM_INSTANCES:-1}"

# Get current git SHA (short version) for deployment tracking
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_build() {
    echo -e "${BLUE}[BUILD]${NC} $1"
}

cleanup_old_revisions() {
    local resource_type="$1"  # "service" or "job"
    local resource_name="$2"
    local max_revisions=10

    log_info "Cleaning up old revisions..."

    local list_cmd
    if [ "$resource_type" = "service" ]; then
        list_cmd="gcloud run revisions list --service=$resource_name"
    else
        list_cmd="gcloud run jobs revisions list --job=$resource_name"
    fi

    local all_revisions=$($list_cmd \
        --region="$GE_GCP_REGION" \
        --format="value(name)" \
        --sort-by="~metadata.creationTimestamp" 2>/dev/null || true)

    if [ -n "$all_revisions" ]; then
        local revision_count=$(echo "$all_revisions" | wc -l | tr -d ' ')
        if [ "$revision_count" -gt "$max_revisions" ]; then
            log_info "Found $revision_count revisions, keeping the $max_revisions most recent"
            echo "$all_revisions" | tail -n +$((max_revisions + 1)) | while read -r revision; do
                log_info "Deleting old revision: $revision"
                if [ "$resource_type" = "service" ]; then
                    gcloud run revisions delete "$revision" \
                        --region="$GE_GCP_REGION" \
                        --quiet 2>/dev/null || log_warn "Failed to delete $revision"
                else
                    gcloud run jobs revisions delete "$revision" \
                        --region="$GE_GCP_REGION" \
                        --quiet 2>/dev/null || log_warn "Failed to delete $revision"
                fi
            done
        fi
    fi
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$GE_GCP_PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set GE_GCP_PROJECT_ID environment variable or use --project-id"
        exit 1
    fi

    # Set gcloud project
    gcloud config set project "$GE_GCP_PROJECT_ID"

    log_info "Configuration validation complete."
}

get_elasticsearch_internal_lb_ip() {
    log_info "Getting Elasticsearch internal load balancer IP..."

    # Try to get the internal load balancer IP from the Kubernetes service
    # This assumes the load balancer has been deployed and has an assigned IP
    if command -v kubectl &> /dev/null; then
        local lb_ip
        lb_ip=$(kubectl get service greenearth-es-internal-lb -n "greenearth-$GE_ENVIRONMENT" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")

        if [ -n "$lb_ip" ] && [ "$lb_ip" != "null" ]; then
            # Use the internal load balancer IP but note that certificate verification
            # may fail since the cert doesn't include this IP in SANs
            GE_ELASTICSEARCH_URL="https://$lb_ip:9200"
            log_info "Using internal load balancer IP: $GE_ELASTICSEARCH_URL"
            log_warn "Note: Certificate verification may fail for IP-based connections"
            log_warn "Services should be configured to skip certificate verification for internal LB"
        else
            log_warn "Could not get internal load balancer IP"
            log_warn "Make sure the Elasticsearch cluster is deployed with internal load balancer"
            log_error "Please deploy Elasticsearch cluster first or set GE_ELASTICSEARCH_URL manually"
            exit 1
        fi
    else
        log_error "kubectl not available - cannot determine Elasticsearch internal load balancer IP"
        log_error "Please install kubectl or set GE_ELASTICSEARCH_URL manually"
        exit 1
    fi
}

verify_vpc_connector() {
    log_info "Verifying VPC connector exists..."

    CONNECTOR_NAME="ingex-vpc-connector-$GE_ENVIRONMENT"

    if ! gcloud compute networks vpc-access connectors describe "$CONNECTOR_NAME" --region="$GE_GCP_REGION" > /dev/null 2>&1; then
        log_error "VPC connector '$CONNECTOR_NAME' does not exist"
        log_error "Please run gcp_setup.sh first to create the VPC connector"
        log_error "Command: cd ingest && ./scripts/gcp_setup.sh"
        exit 1
    fi

    # Check connector status
    local connector_status=$(gcloud compute networks vpc-access connectors describe "$CONNECTOR_NAME" --region="$GE_GCP_REGION" --format="value(state)" 2>/dev/null || echo "UNKNOWN")

    if [ "$connector_status" != "READY" ]; then
        log_warn "VPC connector '$CONNECTOR_NAME' is not ready (status: $connector_status)"
        log_warn "This may cause deployment to fail. Wait a few minutes and try again."
        log_warn "You can check status with: gcloud compute networks vpc-access connectors describe $CONNECTOR_NAME --region=$GE_GCP_REGION"
    else
        log_info "VPC connector '$CONNECTOR_NAME' is ready"
    fi
}

deploy_jetstream_service() {
    log_info "Deploying jetstream-ingest service from source..."

    # Determine secret names based on environment
    # Stage uses no suffix for backwards compatibility, prod uses -prod suffix
    local es_api_key_secret="elasticsearch-api-key"
    if [ "$GE_ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-prod"
    fi

    # Set max-rewind based on environment
    # Stage: 15 minutes (prevent disk overflow on restart)
    # Prod: 0 (unlimited rewind for data integrity)
    local max_rewind
    if [ "$GE_ENVIRONMENT" = "stage" ]; then
        max_rewind=15
    else
        max_rewind=0
    fi

    gcloud run deploy "jetstream-ingest-$GE_ENVIRONMENT" \
        --source=. \
        --region="$GE_GCP_REGION" \
        --service-account="ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com" \
        --vpc-connector="ingex-vpc-connector-$GE_ENVIRONMENT" \
        --vpc-egress=private-ranges-only \
        --set-build-env-vars="GOOGLE_BUILDABLE=./cmd/jetstream_ingest" \
        --set-env-vars="GE_JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe" \
        --set-env-vars="GE_LOGGING_ENABLED=true" \
        --set-env-vars="GE_GIT_SHA=$GIT_SHA" \
        --set-env-vars="GE_JETSTREAM_STATE_FILE=gs://$GE_GCP_PROJECT_ID-ingex-state-$GE_ENVIRONMENT/jetstream_state.json" \
        --set-env-vars="GE_ELASTICSEARCH_URL=$GE_ELASTICSEARCH_URL" \
        --set-env-vars="GE_ELASTICSEARCH_TLS_SKIP_VERIFY=true" \
        --set-env-vars="GE_METRIC_SAMPLING_RATIO=0.005" \
        --set-secrets="GE_ELASTICSEARCH_API_KEY=$es_api_key_secret:latest" \
        --scaling="$GE_JETSTREAM_INSTANCES" \
        --cpu=1 \
        --memory=512Mi \
        --timeout=3600 \
        --concurrency=1000 \
        --no-cpu-throttling \
        --allow-unauthenticated \
        --args="--max-rewind,$max_rewind"

    cleanup_old_revisions "service" "jetstream-ingest-$GE_ENVIRONMENT"
}

deploy_megastream_service() {
    log_info "Deploying megastream-ingest service from source..."

    # Determine secret names based on environment
    # Stage uses no suffix for backwards compatibility, prod uses -prod suffix
    local es_api_key_secret="elasticsearch-api-key"
    local aws_access_key_secret="aws-s3-access-key"
    local aws_secret_key_secret="aws-s3-secret-key"
    if [ "$GE_ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-prod"
        aws_access_key_secret="aws-s3-access-key-prod"
        aws_secret_key_secret="aws-s3-secret-key-prod"
    fi

    # Set max-rewind based on environment
    # Stage: 15 minutes (prevent disk overflow on restart)
    # Prod: 0 (unlimited rewind for data integrity)
    local max_rewind
    if [ "$GE_ENVIRONMENT" = "stage" ]; then
        max_rewind=15
    else
        max_rewind=0
    fi

    gcloud run deploy "megastream-ingest-$GE_ENVIRONMENT" \
        --source=. \
        --region="$GE_GCP_REGION" \
        --service-account="ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com" \
        --vpc-connector="ingex-vpc-connector-$GE_ENVIRONMENT" \
        --vpc-egress=private-ranges-only \
        --set-build-env-vars="GOOGLE_BUILDABLE=./cmd/megastream_ingest" \
        --set-env-vars="GE_LOGGING_ENABLED=true" \
        --set-env-vars="GE_GIT_SHA=$GIT_SHA" \
        --set-env-vars="GE_SPOOL_INTERVAL_SEC=60" \
        --set-env-vars="GE_AWS_REGION=us-east-1" \
        --set-env-vars="GE_MEGASTREAM_STATE_FILE=gs://$GE_GCP_PROJECT_ID-ingex-state-$GE_ENVIRONMENT/megastream_state.json" \
        --set-env-vars="GE_ELASTICSEARCH_URL=$GE_ELASTICSEARCH_URL" \
        --set-env-vars="GE_ELASTICSEARCH_TLS_SKIP_VERIFY=true" \
        --set-env-vars="GE_METRIC_SAMPLING_RATIO=0.01" \
        --set-env-vars="GE_AWS_S3_BUCKET=$GE_AWS_S3_BUCKET" \
        --set-env-vars="GE_AWS_S3_PREFIX=$GE_AWS_S3_PREFIX" \
        --set-secrets="GE_ELASTICSEARCH_API_KEY=$es_api_key_secret:latest,GE_AWS_S3_ACCESS_KEY=$aws_access_key_secret:latest,GE_AWS_S3_SECRET_KEY=$aws_secret_key_secret:latest" \
        --scaling="$GE_MEGASTREAM_INSTANCES" \
        --cpu=1 \
        --memory=1Gi \
        --timeout=3600 \
        --concurrency=1000 \
        --no-cpu-throttling \
        --allow-unauthenticated \
        --args="--source,s3,--mode,spool,--max-rewind,$max_rewind"

    cleanup_old_revisions "service" "megastream-ingest-$GE_ENVIRONMENT"
}

deploy_expiry_job() {
    log_info "Deploying elasticsearch-expiry job from source..."

    # Determine secret names based on environment
    # Stage uses no suffix for backwards compatibility, prod uses -prod suffix
    local es_api_key_secret="elasticsearch-api-key"
    if [ "$GE_ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-prod"
    fi

    # Set retention hours based on environment
    # Stage: 2 hours (aggressive cleanup for limited 8-hour capacity)
    # Prod: 720 hours = 30 days (standard retention)
    local retention_hours
    local hashtag_retention_hours
    if [ "$GE_ENVIRONMENT" = "stage" ]; then
        retention_hours=2
        hashtag_retention_hours=72  # 3 days
        log_info "Stage environment: Using 2-hour retention period, 3-day hashtag retention"
    else
        retention_hours=720
        hashtag_retention_hours=1440  # 60 days
        log_info "Production environment: Using 720-hour (30-day) retention period, 60-day hashtag retention"
    fi

    # Create a temporary directory structure for buildpacks
    # Buildpacks expect a go.mod at the root with the main package
    log_info "Preparing source directory for buildpack..."

    local temp_dir=$(mktemp -d)
    trap "rm -rf $temp_dir" EXIT

    # Copy the necessary files for building just this binary
    cp go.mod go.sum "$temp_dir/"
    cp -r internal "$temp_dir/"
    mkdir -p "$temp_dir/cmd/elasticsearch_expiry"
    cp cmd/elasticsearch_expiry/main.go "$temp_dir/cmd/elasticsearch_expiry/"

    # Create a simple main.go at the root that imports the cmd package
    cat > "$temp_dir/main.go" << 'EOF'
package main

import "github.com/greenearth/ingest/cmd/elasticsearch_expiry"

func main() {
    // This file exists to make buildpacks happy
    // The actual main is in cmd/elasticsearch_expiry
}
EOF

    # Replace the main.go with a redirect
    cat > "$temp_dir/main.go" << 'EOF'
// Build tag to use the cmd/elasticsearch_expiry as main
package main

import (
    _ "github.com/greenearth/ingest/cmd/elasticsearch_expiry"
)
EOF

    # Actually, simpler: just copy the main.go content to root
    cp cmd/elasticsearch_expiry/main.go "$temp_dir/"

    log_info "Deploying elasticsearch-expiry job with buildpacks..."

    gcloud run jobs deploy "elasticsearch-expiry-$GE_ENVIRONMENT" \
        --source="$temp_dir" \
        --region="$GE_GCP_REGION" \
        --service-account="ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com" \
        --vpc-connector="ingex-vpc-connector-$GE_ENVIRONMENT" \
        --vpc-egress=private-ranges-only \
        --set-env-vars="GE_ELASTICSEARCH_URL=$GE_ELASTICSEARCH_URL" \
        --set-env-vars="GE_ELASTICSEARCH_TLS_SKIP_VERIFY=true" \
        --set-secrets="GE_ELASTICSEARCH_API_KEY=$es_api_key_secret:latest" \
        --set-env-vars="GE_LOGGING_ENABLED=true" \
        --set-env-vars="GE_GIT_SHA=$GIT_SHA" \
        --cpu=1 \
        --memory=512Mi \
        --task-timeout=3600 \
        --args="--retention-hours,$retention_hours,--hashtag-retention-hours,$hashtag_retention_hours"

    cleanup_old_revisions "job" "elasticsearch-expiry-$GE_ENVIRONMENT"
}

deploy_extract_job() {
    log_info "Deploying extract job from source..."

    # Determine secret names based on environment
    # Stage uses no suffix for backwards compatibility, prod uses -prod suffix
    local es_api_key_secret="elasticsearch-api-key"
    if [ "$GE_ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-prod"
    fi

    # Set extraction parameters based on environment
    local max_records
    local window_minutes
    local indices
    local destination_bucket

    max_records=1000000      # 1M records
    window_minutes=33       # ~1/2 hour
    indices="posts,likes,hashtags"
    log_info "$GE_ENVIRONMENT environment: 1M max records, approx. 30 min window, indices: posts,likes,hashtags"
    destination_bucket="$GE_GCP_PROJECT_ID-ingex-extract-$GE_ENVIRONMENT"

    # Prepare source directory (similar to expiry job)
    local temp_dir=$(mktemp -d)
    trap "rm -rf $temp_dir" EXIT

    cp go.mod go.sum "$temp_dir/"
    cp -r internal "$temp_dir/"
    mkdir -p "$temp_dir/cmd/extract"
    cp cmd/extract/main.go "$temp_dir/cmd/extract/"
    cp cmd/extract/main.go "$temp_dir/"

    # Prepare env variables file
    local temp_var_dir=$(mktemp -d)
    trap "rm -rf $temp_var_dir" EXIT
    cat > "$temp_var_dir/extract-env-vars.yaml" <<EOF
GE_ELASTICSEARCH_TLS_SKIP_VERIFY: "true"
GE_LOGGING_ENABLED: "true"
GE_GIT_SHA: "$GIT_SHA"
GE_EXTRACT_INDICES: "posts,likes,hashtags"
GE_ELASTICSEARCH_URL: "$GE_ELASTICSEARCH_URL"
GE_PARQUET_DESTINATION: "gs://$destination_bucket"
GE_PARQUET_MAX_RECORDS: "$max_records"
EOF

    log_info "Deploying extract job with buildpacks..."

    gcloud run jobs deploy "extract-$GE_ENVIRONMENT" \
        --source="$temp_dir" \
        --region="$GE_GCP_REGION" \
        --service-account="ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com" \
        --vpc-connector="ingex-vpc-connector-$GE_ENVIRONMENT" \
        --vpc-egress=private-ranges-only \
        --env-vars-file="$temp_var_dir/extract-env-vars.yaml" \
        --set-secrets="GE_ELASTICSEARCH_API_KEY=$es_api_key_secret:latest" \
        --cpu=2 \
        --memory=2Gi \
        --task-timeout=7200 \
        --args="--window-size-min,$window_minutes"

    cleanup_old_revisions "job" "extract-$GE_ENVIRONMENT"
}

deploy_all_services() {
    log_info "Deploying all services to Cloud Run..."

    deploy_jetstream_service
    deploy_megastream_service
    deploy_expiry_job
    deploy_extract_job

    log_info "All services deployed successfully!"
}

show_service_status() {
    log_info "Checking service status..."

    echo
    echo "=== Cloud Run Services ==="
    gcloud run services list --region="$GE_GCP_REGION" --filter="metadata.name:(jetstream-ingest-$GE_ENVIRONMENT OR megastream-ingest-$GE_ENVIRONMENT)"

    echo
    echo "=== Cloud Run Jobs ==="
    gcloud run jobs list --region="$GE_GCP_REGION" --filter="metadata.name:(elasticsearch-expiry-$GE_ENVIRONMENT OR extract-$GE_ENVIRONMENT)"

    echo
    echo "=== Service URLs ==="
    local jetstream_url=$(gcloud run services describe "jetstream-ingest-$GE_ENVIRONMENT" --region="$GE_GCP_REGION" --format="value(status.url)" 2>/dev/null || echo "Not deployed")
    local megastream_url=$(gcloud run services describe "megastream-ingest-$GE_ENVIRONMENT" --region="$GE_GCP_REGION" --format="value(status.url)" 2>/dev/null || echo "Not deployed")

    echo "Jetstream Ingest ($GE_ENVIRONMENT): $jetstream_url"
    echo "Megastream Ingest ($GE_ENVIRONMENT): $megastream_url"
    echo

    log_info "Use 'gcloud run services logs read SERVICE_NAME --region=$GE_GCP_REGION' to view logs"
    log_info "Use 'gcloud run jobs execute elasticsearch-expiry-$GE_ENVIRONMENT --region=$GE_GCP_REGION' to manually run expiry"
    log_info "Use 'gcloud run jobs execute extract-$GE_ENVIRONMENT --region=$GE_GCP_REGION' to manually run extract"
}

main() {
    local service="${1:-all}"

    echo "=================================================="
    echo "Green Earth Ingex - Cloud Run Source Deployment"
    echo "Environment: $GE_ENVIRONMENT"
    echo "Project: $GE_GCP_PROJECT_ID"
    echo "Region: $GE_GCP_REGION"
    echo "Git SHA: $GIT_SHA"
    echo "=================================================="
    echo

    validate_config
    verify_vpc_connector
    get_elasticsearch_internal_lb_ip

    case "$service" in
        jetstream|jetstream-ingest)
            log_info "Deploying jetstream-ingest service..."
            deploy_jetstream_service
            ;;
        megastream|megastream-ingest)
            log_info "Deploying megastream-ingest service..."
            deploy_megastream_service
            ;;
        expiry|elasticsearch-expiry)
            log_info "Deploying elasticsearch-expiry job..."
            deploy_expiry_job
            ;;
        extract|extract-job)
            log_info "Deploying extract job..."
            deploy_extract_job
            ;;
        all)
            deploy_all_services
            ;;
        *)
            log_error "Unknown service: $service"
            echo "Valid services: jetstream, megastream, expiry, extract, all"
            exit 1
            ;;
    esac

    show_service_status

    log_info "Deployment complete!"
    echo
    echo "Next steps:"
    echo "1. Check service logs to ensure they're running correctly"
    echo "2. Verify data is being ingested into Elasticsearch"
    echo "3. Use './ingestctl status' to check service status"
    echo
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project-id)
            GE_GCP_PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            GE_GCP_REGION="$2"
            shift 2
            ;;
        --environment)
            GE_ENVIRONMENT="$2"
            shift 2
            ;;
        --jetstream-instances)
            GE_JETSTREAM_INSTANCES="$2"
            shift 2
            ;;
        --megastream-instances)
            GE_MEGASTREAM_INSTANCES="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [SERVICE] [OPTIONS]"
            echo
            echo "Services:"
            echo "  jetstream                   Deploy jetstream-ingest service only"
            echo "  megastream                  Deploy megastream-ingest service only"
            echo "  expiry                      Deploy elasticsearch-expiry job only"
            echo "  extract                     Deploy extract job only"
            echo "  all                         Deploy all services (default)"
            echo
            echo "Examples:"
            echo "  $0 jetstream                Deploy only jetstream-ingest"
            echo "  $0 megastream               Deploy only megastream-ingest"
            echo "  $0                          Deploy all services"
            echo
            echo "Prerequisites:"
            echo "  Run scripts/gcp_setup.sh first to configure the GCP environment"
            echo "  Deploy Elasticsearch cluster (../index/deploy.sh) to get internal load balancer IP"
            echo
            echo "Options:"
            echo "  --project-id ID              GCP project ID"
            echo "  --region REGION             GCP region (default: us-east1)"
            echo "  --environment ENV           Environment name (default: stage)"
            echo "  --jetstream-instances N     Number of instances for jetstream (default: 1)"
            echo "  --megastream-instances N    Number of instances for megastream (default: 1)"
            echo "  --help                      Show this help message"
            echo
            echo "Environment variables:"
            echo "  GE_GCP_PROJECT_ID              GCP project ID"
            echo "  GE_GCP_REGION                  GCP region"
            echo "  GE_ENVIRONMENT                 Environment name"
            echo "  GE_JETSTREAM_INSTANCES         Number of jetstream instances (default: 1)"
            echo "  GE_MEGASTREAM_INSTANCES        Number of megastream instances (default: 1)"
            echo "  GE_ELASTICSEARCH_URL           Elasticsearch URL (auto-detect internal LB)"
            echo "  GE_AWS_S3_BUCKET               S3 bucket name (default: graze-mega-02)"
            echo "  GE_AWS_S3_PREFIX               S3 prefix (default: mega/)"
            echo
            exit 0
            ;;
        jetstream|megastream|expiry|extract|all)
            # Handle service as first positional argument
            break
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

main "$@"
