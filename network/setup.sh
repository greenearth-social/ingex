#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_usage() {
    echo "Usage: $0"
    echo ""
    echo "Required Environment Variables:"
    echo "  GKE_PROJECT_ID      GCP project ID"
    echo "  GKE_REGION          GCP region (e.g., us-east1)"
    echo ""
    echo "Examples:"
    echo "  $0                  # Setup network"
}

log_info() {
    echo "[INFO] $1"
}

log_success() {
    echo "[SUCCESS] $1"
}

log_error() {
    echo "[ERROR] $1" >&2
}

log_warning() {
    echo "[WARNING] $1"
}

verify_prerequisites() {
    log_info "Verifying prerequisites..."

    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed"
        exit 1
    fi

    if [ -z "$GKE_PROJECT_ID" ]; then
        log_error "GKE_PROJECT_ID environment variable is not set"
        exit 1
    fi

    if [ -z "$GKE_REGION" ]; then
        log_error "GKE_REGION environment variable is not set"
        exit 1
    fi

    log_success "Prerequisites verified"
}

setup_cloud_router() {
    local router_name="greenearth-router"

    log_info "Checking if Cloud Router exists: $router_name"
    if gcloud compute routers describe "$router_name" \
        --region="$GKE_REGION" \
        --project="$GKE_PROJECT_ID" &> /dev/null; then
        log_info "Cloud Router already exists: $router_name"
    else
        log_info "Creating Cloud Router: $router_name"
        gcloud compute routers create "$router_name" \
            --network=default \
            --region="$GKE_REGION" \
            --project="$GKE_PROJECT_ID"
        log_success "Cloud Router created successfully"
    fi
}

setup_cloud_nat() {
    local router_name="greenearth-router"
    local nat_name="greenearth-nat"

    log_info "Checking if Cloud NAT exists: $nat_name"
    if gcloud compute routers nats describe "$nat_name" \
        --router="$router_name" \
        --region="$GKE_REGION" \
        --project="$GKE_PROJECT_ID" &> /dev/null; then
        log_info "Cloud NAT already exists: $nat_name"
    else
        log_info "Creating Cloud NAT: $nat_name"
        gcloud compute routers nats create "$nat_name" \
            --router="$router_name" \
            --region="$GKE_REGION" \
            --project="$GKE_PROJECT_ID" \
            --nat-all-subnet-ip-ranges \
            --auto-allocate-nat-external-ips
        log_success "Cloud NAT created successfully"
    fi
}

enable_private_google_access() {
    log_info "Enabling Private Google Access on default subnet..."

    gcloud compute networks subnets update default \
        --region="$GKE_REGION" \
        --project="$GKE_PROJECT_ID" \
        --enable-private-ip-google-access

    log_success "Private Google Access enabled"
}

setup_eck_webhook_firewall() {
    log_info "Setting up firewall rules for ECK webhook..."

    local stage_rule="allow-stage-master-to-eck-webhook"
    local prod_rule="allow-prod-master-to-eck-webhook"
    local stage_cidr="172.16.0.0/28"
    local prod_cidr="172.16.0.16/28"

    log_info "Checking if stage firewall rule exists: $stage_rule"
    if gcloud compute firewall-rules describe "$stage_rule" \
        --project="$GKE_PROJECT_ID" &> /dev/null; then
        log_info "Stage firewall rule already exists: $stage_rule"
    else
        log_info "Creating stage firewall rule: $stage_rule"
        gcloud compute firewall-rules create "$stage_rule" \
            --project="$GKE_PROJECT_ID" \
            --network=default \
            --allow=tcp:9443,tcp:8443 \
            --source-ranges="$stage_cidr" \
            --description="Allow stage GKE control plane to reach ECK webhook"
        log_success "Stage firewall rule created successfully"
    fi

    log_info "Checking if prod firewall rule exists: $prod_rule"
    if gcloud compute firewall-rules describe "$prod_rule" \
        --project="$GKE_PROJECT_ID" &> /dev/null; then
        log_info "Prod firewall rule already exists: $prod_rule"
    else
        log_info "Creating prod firewall rule: $prod_rule"
        gcloud compute firewall-rules create "$prod_rule" \
            --project="$GKE_PROJECT_ID" \
            --network=default \
            --allow=tcp:9443,tcp:8443 \
            --source-ranges="$prod_cidr" \
            --description="Allow prod GKE control plane to reach ECK webhook"
        log_success "Prod firewall rule created successfully"
    fi

    log_success "ECK webhook firewall rules configured"
}

main() {
    log_info "Setting up network..."
    log_info "Project: $GKE_PROJECT_ID"
    log_info "Region: $GKE_REGION"
    echo ""

    verify_prerequisites
    setup_cloud_router
    setup_cloud_nat
    enable_private_google_access
    setup_eck_webhook_firewall

    echo ""
    log_success "Network setup complete"
    log_info "Cloud Router: greenearth-router"
    log_info "Cloud NAT: greenearth-nat"
    log_info "Private Google Access: Enabled"
    log_info "ECK Webhook Firewall: Configured"
    echo ""
    log_info "You can now create GKE clusters with private nodes"
}

main "$@"
