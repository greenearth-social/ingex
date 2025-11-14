#!/bin/bash

# Green Earth Ingex - Cloud Run Deployment Script
# This script builds and deploys all ingex services to Google Cloud Run

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-greenearth-471522}"
REGION="${REGION:-us-east1}"
ENVIRONMENT="${ENVIRONMENT:-prod}"  # TODO: change default when we have more environments

# Build configuration
BUILD_CONCURRENT="${BUILD_CONCURRENT:-true}"
SKIP_BUILD="${SKIP_BUILD:-false}"

# Service configuration
JETSTREAM_MIN_INSTANCES="${JETSTREAM_MIN_INSTANCES:-1}"
JETSTREAM_MAX_INSTANCES="${JETSTREAM_MAX_INSTANCES:-1}"
MEGASTREAM_MIN_INSTANCES="${MEGASTREAM_MIN_INSTANCES:-1}"
MEGASTREAM_MAX_INSTANCES="${MEGASTREAM_MAX_INSTANCES:-1}"

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

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first."
        exit 1
    fi

    if ! command -v docker &> /dev/null; then
        log_warn "Docker is not installed. Using Cloud Build for container builds."
    fi

    # Check if user is logged in
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
        log_error "Please log in to gcloud first: gcloud auth login"
        exit 1
    fi

    # Check if project exists
    if ! gcloud projects describe "$PROJECT_ID" > /dev/null 2>&1; then
        log_error "Project '$PROJECT_ID' not found or access denied"
        exit 1
    fi

    log_info "Prerequisites check complete."
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set PROJECT_ID environment variable or use --project-id"
        exit 1
    fi

    # Set gcloud project
    gcloud config set project "$PROJECT_ID"

    # Check if Artifact Registry exists
    if ! gcloud artifacts repositories describe ingex --location="$REGION" > /dev/null 2>&1; then
        log_error "Artifact Registry 'ingex' not found. Please run './setup.sh' first."
        exit 1
    fi

    log_info "Configuration validation complete."
}

build_service() {
    local service=$1
    local dockerfile=$2

    log_build "Building $service..."

    local image_name="$REGION-docker.pkg.dev/$PROJECT_ID/ingex/$service"
    local tag="latest"

    if [ "$BUILD_CONCURRENT" = "true" ]; then
        # Use Cloud Build for concurrent builds
        gcloud builds submit \
            --config="deploy/cloud-run/cloudbuild-$service.yaml" \
            --substitutions="_IMAGE_NAME=$image_name:$tag" \
            --async \
            --quiet \
            . &
    else
        # Use Cloud Build synchronously
        gcloud builds submit \
            --config="deploy/cloud-run/cloudbuild-$service.yaml" \
            --substitutions="_IMAGE_NAME=$image_name:$tag" \
            --quiet \
            .
    fi

    echo "$image_name:$tag"
}

create_cloudbuild_configs() {
    log_info "Creating Cloud Build configurations..."

    mkdir -p deploy/cloud-run

    # Jetstream ingest Cloud Build config
    cat > deploy/cloud-run/cloudbuild-jetstream-ingest.yaml << 'EOF'
steps:
- name: 'gcr.io/cloud-builders/go'
  env: ['GOOS=linux', 'GOARCH=amd64', 'CGO_ENABLED=0']
  args: ['build', '-o', 'jetstream-ingest', './cmd/jetstream_ingest']
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', '${_IMAGE_NAME}', '-f', 'deploy/cloud-run/Dockerfile.jetstream-ingest', '.']
- name: 'gcr.io/cloud-builders/docker'
  args: ['push', '${_IMAGE_NAME}']
options:
  machineType: 'E2_HIGHCPU_8'
EOF

    # Megastream ingest Cloud Build config
    cat > deploy/cloud-run/cloudbuild-megastream-ingest.yaml << 'EOF'
steps:
- name: 'gcr.io/cloud-builders/go'
  env: ['GOOS=linux', 'GOARCH=amd64', 'CGO_ENABLED=0']
  args: ['build', '-o', 'megastream-ingest', './cmd/megastream_ingest']
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', '${_IMAGE_NAME}', '-f', 'deploy/cloud-run/Dockerfile.megastream-ingest', '.']
- name: 'gcr.io/cloud-builders/docker'
  args: ['push', '${_IMAGE_NAME}']
options:
  machineType: 'E2_HIGHCPU_8'
EOF

    # Elasticsearch expiry Cloud Build config
    cat > deploy/cloud-run/cloudbuild-elasticsearch-expiry.yaml << 'EOF'
steps:
- name: 'gcr.io/cloud-builders/go'
  env: ['GOOS=linux', 'GOARCH=amd64', 'CGO_ENABLED=0']
  args: ['build', '-o', 'elasticsearch-expiry', './cmd/elasticsearch_expiry']
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', '${_IMAGE_NAME}', '-f', 'deploy/cloud-run/Dockerfile.elasticsearch-expiry', '.']
- name: 'gcr.io/cloud-builders/docker'
  args: ['push', '${_IMAGE_NAME}']
options:
  machineType: 'E2_HIGHCPU_8'
EOF
}

create_dockerfiles() {
    log_info "Creating Dockerfiles..."

    mkdir -p deploy/cloud-run

    # Jetstream ingest Dockerfile
    cat > deploy/cloud-run/Dockerfile.jetstream-ingest << 'EOF'
FROM alpine:latest

# Install CA certificates and timezone data
RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -s /bin/sh appuser

WORKDIR /app

# Copy the binary
COPY jetstream-ingest .

# Create data directory for state files
RUN mkdir -p /data && chown appuser:appuser /data

USER appuser

# Health check endpoint (if your service supports it)
EXPOSE 8080

CMD ["./jetstream-ingest"]
EOF

    # Megastream ingest Dockerfile
    cat > deploy/cloud-run/Dockerfile.megastream-ingest << 'EOF'
FROM alpine:latest

# Install CA certificates and timezone data
RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -s /bin/sh appuser

WORKDIR /app

# Copy the binary
COPY megastream-ingest .

# Create data directory for state files
RUN mkdir -p /data && chown appuser:appuser /data

USER appuser

# Health check endpoint (if your service supports it)
EXPOSE 8080

CMD ["./megastream-ingest"]
EOF

    # Elasticsearch expiry Dockerfile
    cat > deploy/cloud-run/Dockerfile.elasticsearch-expiry << 'EOF'
FROM alpine:latest

# Install CA certificates and timezone data
RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -s /bin/sh appuser

WORKDIR /app

# Copy the binary
COPY elasticsearch-expiry .

USER appuser

CMD ["./elasticsearch-expiry"]
EOF
}

build_all_services() {
    if [ "$SKIP_BUILD" = "true" ]; then
        log_info "Skipping build phase (SKIP_BUILD=true)"
        return
    fi

    log_info "Building all services..."

    create_cloudbuild_configs
    create_dockerfiles

    # Build all services
    local jetstream_image=$(build_service "jetstream-ingest" "deploy/cloud-run/Dockerfile.jetstream-ingest")
    local megastream_image=$(build_service "megastream-ingest" "deploy/cloud-run/Dockerfile.megastream-ingest")
    local expiry_image=$(build_service "elasticsearch-expiry" "deploy/cloud-run/Dockerfile.elasticsearch-expiry")

    if [ "$BUILD_CONCURRENT" = "true" ]; then
        log_info "Waiting for concurrent builds to complete..."
        wait
    fi

    # Store image names for deployment
    echo "$jetstream_image" > .jetstream-image
    echo "$megastream_image" > .megastream-image
    echo "$expiry_image" > .expiry-image

    log_info "All builds complete!"
}

deploy_jetstream_service() {
    log_info "Deploying jetstream-ingest service..."

    local image=$(cat .jetstream-image 2>/dev/null || echo "$REGION-docker.pkg.dev/$PROJECT_ID/ingex/jetstream-ingest:latest")

    gcloud run deploy jetstream-ingest \
        --image="$image" \
        --region="$REGION" \
        --service-account="ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com" \
        --set-env-vars="JETSTREAM_URL=wss://jetstream2.us-east.bsky.network/subscribe" \
        --set-env-vars="LOGGING_ENABLED=true" \
        --set-env-vars="JETSTREAM_STATE_FILE=/data/jetstream_state.json" \
        --set-secrets="ELASTICSEARCH_URL=elasticsearch-url:latest" \
        --set-secrets="ELASTICSEARCH_API_KEY=elasticsearch-api-key:latest" \
        --min-instances="$JETSTREAM_MIN_INSTANCES" \
        --max-instances="$JETSTREAM_MAX_INSTANCES" \
        --cpu=1 \
        --memory=512Mi \
        --timeout=3600 \
        --concurrency=1000 \
        --no-cpu-throttling \
        --allow-unauthenticated
}

deploy_megastream_service() {
    log_info "Deploying megastream-ingest service..."

    local image=$(cat .megastream-image 2>/dev/null || echo "$REGION-docker.pkg.dev/$PROJECT_ID/ingex/megastream-ingest:latest")

    gcloud run deploy megastream-ingest \
        --image="$image" \
        --region="$REGION" \
        --service-account="ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com" \
        --set-env-vars="LOGGING_ENABLED=true" \
        --set-env-vars="SPOOL_INTERVAL_SEC=300" \
        --set-env-vars="AWS_REGION=us-east-1" \
        --set-env-vars="MEGASTREAM_STATE_FILE=/data/megastream_state.json" \
        --set-secrets="ELASTICSEARCH_URL=elasticsearch-url:latest" \
        --set-secrets="ELASTICSEARCH_API_KEY=elasticsearch-api-key:latest" \
        --set-secrets="S3_SQLITE_DB_BUCKET=s3-bucket:latest" \
        --set-secrets="S3_SQLITE_DB_PREFIX=s3-prefix:latest" \
        --min-instances="$MEGASTREAM_MIN_INSTANCES" \
        --max-instances="$MEGASTREAM_MAX_INSTANCES" \
        --cpu=1 \
        --memory=1Gi \
        --timeout=3600 \
        --concurrency=1000 \
        --no-cpu-throttling \
        --allow-unauthenticated \
        --args="--source,s3,--mode,spool"
}

deploy_expiry_job() {
    log_info "Deploying elasticsearch-expiry job..."

    local image=$(cat .expiry-image 2>/dev/null || echo "$REGION-docker.pkg.dev/$PROJECT_ID/ingex/elasticsearch-expiry:latest")

    gcloud run jobs replace deploy/cloud-run/elasticsearch-expiry-job.yaml \
        --region="$REGION"

    # Update the job with the correct image
    gcloud run jobs update elasticsearch-expiry \
        --image="$image" \
        --region="$REGION" \
        --service-account="ingex-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com" \
        --set-secrets="ELASTICSEARCH_URL=elasticsearch-url:latest" \
        --set-secrets="ELASTICSEARCH_API_KEY=elasticsearch-api-key:latest" \
        --set-env-vars="LOGGING_ENABLED=true" \
        --cpu=1 \
        --memory=512Mi \
        --task-timeout=3600 \
        --args="--retention-days,60"
}

deploy_all_services() {
    log_info "Deploying all services to Cloud Run..."

    deploy_jetstream_service
    deploy_megastream_service
    deploy_expiry_job

    log_info "All services deployed successfully!"
}

cleanup_temp_files() {
    rm -f .jetstream-image .megastream-image .expiry-image
}

show_service_status() {
    log_info "Checking service status..."

    echo
    echo "=== Cloud Run Services ==="
    gcloud run services list --region="$REGION" --filter="metadata.name:(jetstream-ingest OR megastream-ingest)"

    echo
    echo "=== Cloud Run Jobs ==="
    gcloud run jobs list --region="$REGION" --filter="metadata.name:elasticsearch-expiry"

    echo
    echo "=== Service URLs ==="
    local jetstream_url=$(gcloud run services describe jetstream-ingest --region="$REGION" --format="value(status.url)" 2>/dev/null || echo "Not deployed")
    local megastream_url=$(gcloud run services describe megastream-ingest --region="$REGION" --format="value(status.url)" 2>/dev/null || echo "Not deployed")

    echo "Jetstream Ingest: $jetstream_url"
    echo "Megastream Ingest: $megastream_url"
    echo

    log_info "Use 'gcloud run services logs read SERVICE_NAME --region=$REGION' to view logs"
    log_info "Use 'gcloud run jobs execute elasticsearch-expiry --region=$REGION' to manually run expiry"
}

main() {
    echo "=================================================="
    echo "Green Earth Ingex - Cloud Run Deployment"
    echo "Environment: $ENVIRONMENT"
    echo "Project: $PROJECT_ID"
    echo "Region: $REGION"
    echo "=================================================="
    echo

    check_prerequisites
    validate_config
    build_all_services
    deploy_all_services
    cleanup_temp_files
    show_service_status

    log_info "Deployment complete!"
    echo
    echo "Next steps:"
    echo "1. Check service logs to ensure they're running correctly"
    echo "2. Verify data is being ingested into Elasticsearch"
    echo "3. Test the expiry job manually if needed"
    echo
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project-id)
            PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD="true"
            shift
            ;;
        --no-concurrent-build)
            BUILD_CONCURRENT="false"
            shift
            ;;
        --jetstream-instances)
            JETSTREAM_MIN_INSTANCES="$2"
            JETSTREAM_MAX_INSTANCES="$2"
            shift 2
            ;;
        --megastream-instances)
            MEGASTREAM_MIN_INSTANCES="$2"
            MEGASTREAM_MAX_INSTANCES="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --project-id ID              GCP project ID"
            echo "  --region REGION             GCP region (default: us-east1)"
            echo "  --environment ENV           Environment name (default: prod)"
            echo "  --skip-build                Skip the build phase, use existing images"
            echo "  --no-concurrent-build       Build services sequentially instead of in parallel"
            echo "  --jetstream-instances N     Set min/max instances for jetstream service"
            echo "  --megastream-instances N    Set min/max instances for megastream service"
            echo "  --help                      Show this help message"
            echo
            echo "Environment variables:"
            echo "  PROJECT_ID                  GCP project ID"
            echo "  REGION                      GCP region"
            echo "  ENVIRONMENT                 Environment name"
            echo "  SKIP_BUILD                  Skip build phase (true/false)"
            echo "  BUILD_CONCURRENT           Build services concurrently (true/false)"
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
