#!/bin/bash

# Green Earth Ingex - GCP Environment Setup Script
# This script sets up the GCP environment for the first time
# Run this once per environment (dev, stage, prod])

set -e

# Configuration
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"  # TODO: change default when we have more environments

# Elasticsearch configuration - only API key is secret, URL is public
GE_ELASTICSEARCH_URL="${GE_ELASTICSEARCH_URL:-INTERNAL_LB_PLACEHOLDER}"
GE_ELASTICSEARCH_API_KEY="${GE_ELASTICSEARCH_API_KEY:-your-api-key}"

# S3 configuration for Megastream data
# TODO: actual s3 bucket name
GE_AWS_S3_BUCKET="${GE_AWS_S3_BUCKET:-greenearth-megastream-data}"
GE_AWS_S3_PREFIX="${GE_AWS_S3_PREFIX:-megastream/databases/}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first."
        exit 1
    fi

    if ! command -v kubectl &> /dev/null; then
        log_warn "kubectl is not installed. You may need it for debugging."
    fi

    # Check if user is logged in
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
        log_error "Please log in to gcloud first: gcloud auth login"
        exit 1
    fi

    log_info "Prerequisites check complete."
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$GE_GCP_PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set GE_GCP_PROJECT_ID environment variable or update the script"
        exit 1
    fi

    log_info "Configuration validation complete."
    log_info "Using Elasticsearch URL: $GE_ELASTICSEARCH_URL"
    log_info "Using S3 bucket: $GE_AWS_S3_BUCKET with prefix: $GE_AWS_S3_PREFIX"

    if [ -n "$GE_ELASTICSEARCH_API_KEY" ] && [ "$GE_ELASTICSEARCH_API_KEY" != "your-api-key" ]; then
        log_info "Elasticsearch API key provided - will be stored/updated in Secret Manager"
    else
        log_warn "Elasticsearch API key not provided - skipping secret creation (assuming it already exists)"
    fi

    if [ -n "$GE_AWS_S3_ACCESS_KEY" ] && [ -n "$GE_AWS_S3_SECRET_KEY" ]; then
        log_info "AWS S3 credentials provided - will be stored in Secret Manager"
    else
        log_warn "AWS S3 credentials not provided - skipping secret creation"
    fi
}

setup_gcp_project() {
    log_info "Setting up GCP project: $GE_GCP_PROJECT_ID"

    # Set the project
    gcloud config set project "$GE_GCP_PROJECT_ID"

    # Enable required APIs
    log_info "Enabling required GCP APIs..."
    gcloud services enable \
        cloudbuild.googleapis.com \
        run.googleapis.com \
        cloudscheduler.googleapis.com \
        secretmanager.googleapis.com \
        storage.googleapis.com \
        artifactregistry.googleapis.com \
        vpcaccess.googleapis.com \
        compute.googleapis.com

    log_info "GCP project setup complete."
}

create_artifact_registry() {
    log_info "Creating Artifact Registry repository..."

    # Create repository for container images
    if ! gcloud artifacts repositories describe ingex --location="$GE_GCP_REGION" > /dev/null 2>&1; then
        gcloud artifacts repositories create ingex \
            --repository-format=docker \
            --location="$GE_GCP_REGION" \
            --description="Green Earth Ingex container images"
        log_info "Artifact Registry repository created."
    else
        log_info "Artifact Registry repository already exists."
    fi
}

create_service_account() {
    log_info "Creating service account for Cloud Run services..."

    SA_NAME="ingex-runner-$GE_ENVIRONMENT"
    SA_EMAIL="$SA_NAME@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com"

    # Create service account
    if ! gcloud iam service-accounts describe "$SA_EMAIL" > /dev/null 2>&1; then
        gcloud iam service-accounts create "$SA_NAME" \
            --display-name="Ingex Cloud Run Service Account ($GE_ENVIRONMENT)" \
            --description="Service account for running ingex services in $GE_ENVIRONMENT"
        log_info "Service account created: $SA_EMAIL"
    else
        log_info "Service account already exists: $SA_EMAIL"
    fi

    # Grant necessary permissions
    log_info "Granting permissions to service account..."

    # Permission to read from Cloud Storage (for S3-compatible access)
    gcloud projects add-iam-policy-binding "$GE_GCP_PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/storage.objectViewer"

    # Permission to access secrets
    gcloud projects add-iam-policy-binding "$GE_GCP_PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/secretmanager.secretAccessor"

    # Permission for Cloud Run to run jobs
    gcloud projects add-iam-policy-binding "$GE_GCP_PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/run.invoker"

    # Permission to use VPC connectors
    gcloud projects add-iam-policy-binding "$GE_GCP_PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/vpcaccess.user"

    log_info "Service account permissions configured."
}

create_secrets() {
    log_info "Creating secrets in Secret Manager..."

    SA_EMAIL="ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com"

    # Elasticsearch API key
    if [ -n "$GE_ELASTICSEARCH_API_KEY" ] && [ "$GE_ELASTICSEARCH_API_KEY" != "your-api-key" ]; then
        if ! gcloud secrets describe elasticsearch-api-key > /dev/null 2>&1; then
            echo -n "$GE_ELASTICSEARCH_API_KEY" | gcloud secrets create elasticsearch-api-key --data-file=-
            log_info "Elasticsearch API key secret created."
        else
            log_info "Elasticsearch API key secret already exists. Updating..."
            echo -n "$GE_ELASTICSEARCH_API_KEY" | gcloud secrets versions add elasticsearch-api-key --data-file=-
            log_info "Elasticsearch API key secret updated."
        fi

        # Grant service account access to elasticsearch-api-key
        gcloud secrets add-iam-policy-binding elasticsearch-api-key \
            --member="serviceAccount:$SA_EMAIL" \
            --role="roles/secretmanager.secretAccessor" \
            --condition=None
    else
        log_warn "Elasticsearch API key not provided. Skipping secret creation."
        log_info "Ensuring service account has access to existing secret..."
        if gcloud secrets describe elasticsearch-api-key > /dev/null 2>&1; then
            # Grant service account access even if we're not creating/updating the secret
            gcloud secrets add-iam-policy-binding elasticsearch-api-key \
                --member="serviceAccount:$SA_EMAIL" \
                --role="roles/secretmanager.secretAccessor" \
                --condition=None 2>/dev/null || log_info "Service account already has access to elasticsearch-api-key"
        else
            log_warn "Elasticsearch API key secret does not exist. You'll need to create it manually or re-run with --elasticsearch-api-key"
        fi
    fi

    # AWS S3 Access Key
    if [ -n "$GE_AWS_S3_ACCESS_KEY" ]; then
        if ! gcloud secrets describe aws-s3-access-key > /dev/null 2>&1; then
            echo -n "$GE_AWS_S3_ACCESS_KEY" | gcloud secrets create aws-s3-access-key --data-file=-
            log_info "AWS S3 access key secret created."
        else
            log_info "AWS S3 access key secret already exists. Updating..."
            echo -n "$GE_AWS_S3_ACCESS_KEY" | gcloud secrets versions add aws-s3-access-key --data-file=-
            log_info "AWS S3 access key secret updated."
        fi

        # Grant service account access to aws-s3-access-key
        gcloud secrets add-iam-policy-binding aws-s3-access-key \
            --member="serviceAccount:$SA_EMAIL" \
            --role="roles/secretmanager.secretAccessor" \
            --condition=None
    else
        log_warn "GE_AWS_S3_ACCESS_KEY not set. Skipping AWS S3 access key secret creation."
        log_warn "Set this if you need megastream-ingest to access S3 data."
    fi

    # AWS S3 Secret Key
    if [ -n "$GE_AWS_S3_SECRET_KEY" ]; then
        if ! gcloud secrets describe aws-s3-secret-key > /dev/null 2>&1; then
            echo -n "$GE_AWS_S3_SECRET_KEY" | gcloud secrets create aws-s3-secret-key --data-file=-
            log_info "AWS S3 secret key secret created."
        else
            log_info "AWS S3 secret key secret already exists. Updating..."
            echo -n "$GE_AWS_S3_SECRET_KEY" | gcloud secrets versions add aws-s3-secret-key --data-file=-
            log_info "AWS S3 secret key secret updated."
        fi

        # Grant service account access to aws-s3-secret-key
        gcloud secrets add-iam-policy-binding aws-s3-secret-key \
            --member="serviceAccount:$SA_EMAIL" \
            --role="roles/secretmanager.secretAccessor" \
            --condition=None
    else
        log_warn "GE_AWS_S3_SECRET_KEY not set. Skipping AWS S3 secret key secret creation."
        log_warn "Set this if you need megastream-ingest to access S3 data."
    fi

    log_info "Note: Non-secret configuration (Elasticsearch URL, S3 bucket, S3 prefix) is now stored in the deployment scripts."
}

create_ingest_state_storage() {
    log_info "Setting up storage for ingest state files..."

    # Create a Cloud Storage bucket for ingest state files
    BUCKET_NAME="$GE_GCP_PROJECT_ID-ingex-state-$GE_ENVIRONMENT"

    if ! gsutil ls -b gs://"$BUCKET_NAME" > /dev/null 2>&1; then
        gsutil mb -l "$GE_GCP_REGION" gs://"$BUCKET_NAME"
        log_info "Storage bucket created: $BUCKET_NAME"
    else
        log_info "Storage bucket already exists: $BUCKET_NAME"
    fi

    # Set appropriate permissions
    gsutil iam ch serviceAccount:"ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com":objectAdmin gs://"$BUCKET_NAME"
}

create_extract_storage() {
    log_info "Setting up storage bucket for extracted parquet files..."

    BUCKET_NAME="$GE_GCP_PROJECT_ID-ingex-extract-$GE_ENVIRONMENT"

    if ! gsutil ls -b gs://"$BUCKET_NAME" > /dev/null 2>&1; then
        gsutil mb -l "$GE_GCP_REGION" gs://"$BUCKET_NAME"
        log_info "Extract storage bucket created: $BUCKET_NAME"
    else
        log_info "Extract storage bucket already exists: $BUCKET_NAME"
    fi

    # Grant service account objectAdmin permission
    gsutil iam ch serviceAccount:"ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com":objectAdmin gs://"$BUCKET_NAME"
    log_info "Granted objectAdmin to service account for bucket: $BUCKET_NAME"
}

create_vpc_connector() {
    log_info "Creating VPC connector for Cloud Run services..."

    CONNECTOR_NAME="ingex-vpc-connector-$GE_ENVIRONMENT"

    # Check if VPC connector already exists
    if gcloud compute networks vpc-access connectors describe "$CONNECTOR_NAME" --region="$GE_GCP_REGION" > /dev/null 2>&1; then
        log_info "VPC connector already exists: $CONNECTOR_NAME"
        return
    fi

    # Create VPC connector service account if it doesn't exist
    CONNECTOR_SA_NAME="vpc-connector-sa-$GE_ENVIRONMENT"
    CONNECTOR_SA_EMAIL="$CONNECTOR_SA_NAME@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com"

    if ! gcloud iam service-accounts describe "$CONNECTOR_SA_EMAIL" > /dev/null 2>&1; then
        gcloud iam service-accounts create "$CONNECTOR_SA_NAME" \
            --display-name="VPC Connector Service Account ($GE_ENVIRONMENT)" \
            --description="Service account for VPC connector in $GE_ENVIRONMENT"
        log_info "VPC connector service account created: $CONNECTOR_SA_EMAIL"
    fi

    # Create VPC connector
    # Use a small IP range for the connector (only needs a few IPs for Cloud Run)
    # Using 192.168.1.0/28 to avoid conflicts with existing subnets
    gcloud compute networks vpc-access connectors create "$CONNECTOR_NAME" \
        --network=default \
        --region="$GE_GCP_REGION" \
        --range=192.168.1.0/28 \
        --min-instances=2 \
        --max-instances=10 \
        --machine-type=e2-micro

    log_info "VPC connector created: $CONNECTOR_NAME"

    # Grant the default Cloud Run service account permission to use VPC connectors
    gcloud projects add-iam-policy-binding "$GE_GCP_PROJECT_ID" \
        --member="serviceAccount:$GE_GCP_PROJECT_ID-compute@developer.gserviceaccount.com" \
        --role="roles/vpcaccess.user"

    log_info "VPC connector permissions configured"
}

setup_firewall_rules() {
    log_info "Setting up firewall rules for VPC access..."

    # Allow Cloud Run services to access Elasticsearch through internal load balancer
    FIREWALL_RULE_NAME="allow-cloud-run-to-elasticsearch-$GE_ENVIRONMENT"

    if ! gcloud compute firewall-rules describe "$FIREWALL_RULE_NAME" > /dev/null 2>&1; then
        gcloud compute firewall-rules create "$FIREWALL_RULE_NAME" \
            --network=default \
            --allow=tcp:9200,tcp:9300 \
            --source-ranges=192.168.1.0/28 \
            --target-tags=gke-greenearth-$GE_ENVIRONMENT \
            --description="Allow Cloud Run services to access Elasticsearch internal load balancer"
        log_info "Firewall rule created: $FIREWALL_RULE_NAME"
    else
        log_info "Firewall rule already exists: $FIREWALL_RULE_NAME"
    fi

    # Allow internal load balancer health checks
    HEALTH_CHECK_RULE="allow-internal-lb-health-checks-$GE_ENVIRONMENT"

    if ! gcloud compute firewall-rules describe "$HEALTH_CHECK_RULE" > /dev/null 2>&1; then
        gcloud compute firewall-rules create "$HEALTH_CHECK_RULE" \
            --network=default \
            --allow=tcp:9200 \
            --source-ranges=130.211.0.0/22,35.191.0.0/16 \
            --target-tags=gke-greenearth-$GE_ENVIRONMENT \
            --description="Allow Google Cloud load balancer health checks"
        log_info "Health check firewall rule created: $HEALTH_CHECK_RULE"
    else
        log_info "Health check firewall rule already exists: $HEALTH_CHECK_RULE"
    fi
}

setup_expiry_cloud_scheduler() {
    log_info "Setting up Cloud Scheduler for elasticsearch-expiry..."

    # Get project number for default compute service account
    PROJECT_NUMBER=$(gcloud projects describe "$GE_GCP_PROJECT_ID" --format="value(projectNumber)")
    COMPUTE_SERVICE_ACCOUNT="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

    # Use Cloud Run v2 API endpoint format
    JOB_URI="https://run.googleapis.com/v2/projects/$GE_GCP_PROJECT_ID/locations/$GE_GCP_REGION/jobs/elasticsearch-expiry:run"

    # Only configure for stage environment (hourly cleanup for limited capacity cluster)
    if [ "$GE_ENVIRONMENT" != "stage" ]; then
        log_info "Skipping Cloud Scheduler setup for $GE_ENVIRONMENT (only stage is configured)"
        return 0
    fi

    local schedule="*/30 * * * *"  # Every 30 minutes
    local job_name="elasticsearch-expiry-halfhourly-stage"
    local description="Half-hourly Elasticsearch data expiry for stage (limited capacity)"
    log_info "Stage environment: Configuring 30-minute expiry schedule"

    # Grant the default compute service account permission to invoke the Cloud Run job
    log_info "Granting default compute service account permission to invoke Cloud Run job..."
    gcloud run jobs add-iam-policy-binding elasticsearch-expiry \
        --region="$GE_GCP_REGION" \
        --member="serviceAccount:$COMPUTE_SERVICE_ACCOUNT" \
        --role="roles/run.invoker" \
        2>/dev/null || log_info "Service account already has run.invoker permission"

    # Create or update the scheduler job
    # Note: Uses OAuth (not OIDC) as documented in https://docs.cloud.google.com/run/docs/execute/jobs-on-schedule#command-line
    if ! gcloud scheduler jobs describe "$job_name" --location="$GE_GCP_REGION" > /dev/null 2>&1; then
        gcloud scheduler jobs create http "$job_name" \
            --location="$GE_GCP_REGION" \
            --schedule="$schedule" \
            --uri="$JOB_URI" \
            --http-method=POST \
            --oauth-service-account-email="$COMPUTE_SERVICE_ACCOUNT" \
            --description="$description"
        log_info "Cloud Scheduler job created: $job_name"
    else
        # Update existing job to ensure schedule and other settings are current
        gcloud scheduler jobs update http "$job_name" \
            --location="$GE_GCP_REGION" \
            --schedule="$schedule" \
            --uri="$JOB_URI" \
            --http-method=POST \
            --oauth-service-account-email="$COMPUTE_SERVICE_ACCOUNT" \
            --description="$description"
        log_info "Cloud Scheduler job updated: $job_name"
    fi
}

setup_extract_cloud_scheduler() {
    log_info "Setting up Cloud Scheduler for extract job..."

    # Get project number for default compute service account
    PROJECT_NUMBER=$(gcloud projects describe "$GE_GCP_PROJECT_ID" --format="value(projectNumber)")
    COMPUTE_SERVICE_ACCOUNT="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

    # Use Cloud Run v2 API endpoint format
    JOB_URI="https://run.googleapis.com/v2/projects/$GE_GCP_PROJECT_ID/locations/$GE_GCP_REGION/jobs/extract:run"

    # Only configure for stage environment
    if [ "$GE_ENVIRONMENT" != "stage" ]; then
        log_info "Skipping extract Cloud Scheduler setup for $GE_ENVIRONMENT (only stage is configured)"
        return 0
    fi

    local schedule="0 * * * *"  # Every 1 hour at the top of the hour
    local job_name="extract-hourly-stage"
    local description="hourly extract job for stage"
    log_info "Stage environment: Configuring hourly extract schedule"

    # Grant the default compute service account permission to invoke the Cloud Run job
    log_info "Granting default compute service account permission to invoke extract job..."
    gcloud run jobs add-iam-policy-binding extract \
        --region="$GE_GCP_REGION" \
        --member="serviceAccount:$COMPUTE_SERVICE_ACCOUNT" \
        --role="roles/run.invoker" \
        2>/dev/null || log_info "Service account already has run.invoker permission"

    # Create or update the scheduler job
    if ! gcloud scheduler jobs describe "$job_name" --location="$GE_GCP_REGION" > /dev/null 2>&1; then
        gcloud scheduler jobs create http "$job_name" \
            --location="$GE_GCP_REGION" \
            --schedule="$schedule" \
            --uri="$JOB_URI" \
            --http-method=POST \
            --oauth-service-account-email="$COMPUTE_SERVICE_ACCOUNT" \
            --description="$description"
        log_info "Cloud Scheduler job created: $job_name"
    else
        gcloud scheduler jobs update http "$job_name" \
            --location="$GE_GCP_REGION" \
            --schedule="$schedule" \
            --uri="$JOB_URI" \
            --http-method=POST \
            --oauth-service-account-email="$COMPUTE_SERVICE_ACCOUNT" \
            --description="$description"
        log_info "Cloud Scheduler job updated: $job_name"
    fi
}

main() {
    echo "=================================================="
    echo "Green Earth Ingex - GCP Environment Setup"
    echo "Environment: $GE_ENVIRONMENT"
    echo "Project: $GE_GCP_PROJECT_ID"
    echo "Region: $GE_GCP_REGION"
    echo "=================================================="
    echo

    check_prerequisites
    validate_config
    setup_gcp_project
    create_artifact_registry
    create_service_account
    create_secrets
    create_ingest_state_storage
    create_extract_storage
    create_vpc_connector
    setup_firewall_rules
    setup_expiry_cloud_scheduler
    setup_extract_cloud_scheduler

    log_info "Environment setup complete!"
    echo
    echo "Next steps:"
    echo "1. Run './scripts/deploy.sh' to build and deploy your services"
    echo "2. Check Cloud Run console to verify services are running"
    echo "3. Monitor logs for any issues"
    echo
    echo "Important notes:"
    echo "- Elasticsearch expiry runs daily at 2 AM UTC"
    echo "- State files are stored in: gs://$GE_GCP_PROJECT_ID-ingex-state-$GE_ENVIRONMENT"
    echo "- Service account: ingex-runner-$GE_ENVIRONMENT@$GE_GCP_PROJECT_ID.iam.gserviceaccount.com"
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
        --elasticsearch-url)
            GE_ELASTICSEARCH_URL="$2"
            shift 2
            ;;
        --elasticsearch-api-key)
            GE_ELASTICSEARCH_API_KEY="$2"
            shift 2
            ;;
        --s3-bucket)
            GE_AWS_S3_BUCKET="$2"
            shift 2
            ;;
        --s3-prefix)
            GE_AWS_S3_PREFIX="$2"
            shift 2
            ;;
        --aws-access-key)
            GE_AWS_S3_ACCESS_KEY="$2"
            shift 2
            ;;
        --aws-secret-key)
            GE_AWS_S3_SECRET_KEY="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --project-id ID              GCP project ID"
            echo "  --region REGION              GCP region (default: us-east1)"
            echo "  --environment ENV            Environment name (default: stage)"
            echo "  --elasticsearch-url URL      Elasticsearch cluster URL (default: INTERNAL_LB_PLACEHOLDER)"
            echo "  --elasticsearch-api-key KEY  Elasticsearch API key (optional if secret already exists)"
            echo "  --s3-bucket BUCKET           S3 bucket for Megastream data (default: greenearth-megastream-data)"
            echo "  --s3-prefix PREFIX           S3 prefix for Megastream data (default: megastream/databases/)"
            echo "  --aws-access-key KEY         AWS S3 access key (optional, for megastream S3 access)"
            echo "  --aws-secret-key KEY         AWS S3 secret key (optional, for megastream S3 access)"
            echo "  --help                       Show this help message"
            echo
            echo "All secrets are optional if they already exist in Secret Manager."
            echo "The script is idempotent and safe to re-run to ensure correct configuration."
            echo
            echo "Environment variables:"
            echo "  GE_GCP_PROJECT_ID, GE_GCP_REGION, GE_ENVIRONMENT, GE_ELASTICSEARCH_URL"
            echo "  GE_ELASTICSEARCH_API_KEY, GE_AWS_S3_BUCKET, GE_AWS_S3_PREFIX"
            echo "  GE_AWS_S3_ACCESS_KEY, GE_AWS_S3_SECRET_KEY"
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
