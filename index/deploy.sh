#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$SCRIPT_DIR/deploy/k8s"

print_usage() {
    echo "Usage: $0 <environment> [options]"
    echo ""
    echo "Arguments:"
    echo "  environment         Target environment: local, stage, or prod"
    echo ""
    echo "Options:"
    echo "  --install-eck       Install ECK operator before deployment"
    echo "  --skip-templates    Skip template/alias ConfigMaps (for updates)"
    echo "  --dry-run           Show what would be deployed without applying"
    echo "  --teardown          Delete the entire environment (prompts for confirmation)"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Required Environment Variables:"
    echo "  ES_SERVICE_USER_PASSWORD    Password for the Elasticsearch service user"
    echo ""
    echo "Examples:"
    echo "  $0 local                    # Deploy to local environment"
    echo "  $0 stage --install-eck      # Deploy to stage with ECK installation"
    echo "  $0 local --teardown         # Delete local environment"
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

cleanup_on_failure() {
    local namespace=$1
    log_error "Deployment failed. Cleaning up resources..."

    kubectl delete -k "$K8S_DIR/environments/$ENVIRONMENT" -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete secret es-service-user-secret -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete job elasticsearch-bootstrap -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete job es-service-user-setup -n "$namespace" --ignore-not-found=true 2>/dev/null || true
    kubectl delete namespace "$namespace"

    log_info "Cleanup completed"
}

wait_for_resource() {
    local resource_type=$1
    local resource_name=$2
    local namespace=$3
    local timeout=${4:-300}
    local start_time=$(date +%s)

    log_info "Waiting for $resource_type/$resource_name to be ready (timeout: ${timeout}s)..."

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -gt $timeout ]; then
            log_error "Timeout waiting for $resource_type/$resource_name"
            return 1
        fi

        local health=$(kubectl get $resource_type $resource_name -n $namespace -o jsonpath='{.status.health}' 2>/dev/null || echo "")

        # Check readiness based on resource type
        if [ "$resource_type" = "kibana" ]; then
            # Kibana only has health status, no phase field
            if [ "$health" = "green" ]; then
                log_success "$resource_type/$resource_name is ready"
                return 0
            fi
        else
            # Elasticsearch and other resources may have both health and phase
            local phase=$(kubectl get $resource_type $resource_name -n $namespace -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
            if [ "$health" = "green" ] && [ "$phase" = "Ready" ]; then
                log_success "$resource_type/$resource_name is ready"
                return 0
            fi
        fi

        echo -n "."
        sleep 5
    done
}

wait_for_job() {
    local job_name=$1
    local namespace=$2
    local timeout=${3:-300}
    local start_time=$(date +%s)

    log_info "Waiting for job/$job_name to complete (timeout: ${timeout}s)..."

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -gt $timeout ]; then
            log_error "Timeout waiting for job/$job_name"
            kubectl logs -l job-name=$job_name -n $namespace --tail=50 2>/dev/null || true
            return 1
        fi

        local succeeded=$(kubectl get job $job_name -n $namespace -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")
        local failed=$(kubectl get job $job_name -n $namespace -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")

        if [ "$succeeded" = "1" ]; then
            log_success "job/$job_name completed successfully"
            return 0
        fi

        if [ "$failed" != "0" ]; then
            log_error "job/$job_name failed"
            kubectl logs -l job-name=$job_name -n $namespace --tail=50 2>/dev/null || true
            return 1
        fi

        echo -n "."
        sleep 5
    done
}

verify_prerequisites() {
    log_info "Verifying prerequisites..."

    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi

    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi

    if [ -z "$ES_SERVICE_USER_PASSWORD" ]; then
        log_error "ES_SERVICE_USER_PASSWORD environment variable is not set"
        exit 1
    fi

    log_success "Prerequisites verified"
}

install_eck_operator() {
    log_info "Installing ECK operator..."

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would install ECK CRDs and operator"
        return 0
    fi

    kubectl create -f https://download.elastic.co/downloads/eck/3.1.0/crds.yaml 2>/dev/null || log_warning "ECK CRDs may already exist"
    kubectl apply -f https://download.elastic.co/downloads/eck/3.1.0/operator.yaml

    log_info "Waiting for ECK operator to be ready..."
    kubectl wait --for=condition=available --timeout=120s deployment/elastic-operator -n elastic-system

    log_success "ECK operator installed"
}

teardown_environment() {
    local namespace=$1

    echo ""
    log_warning "WARNING: This will DELETE the entire $ENVIRONMENT environment!"
    log_warning "Namespace: $namespace"
    log_warning "All data will be permanently lost."
    echo ""
    read -p "Are you sure you want to proceed? Type 'yes' to confirm: " confirmation

    if [ "$confirmation" != "yes" ]; then
        log_info "Teardown cancelled"
        exit 0
    fi

    log_info "Tearing down $ENVIRONMENT environment..."

    kubectl delete namespace $namespace --ignore-not-found=true

    log_success "Environment $ENVIRONMENT has been deleted"
}

deploy_environment() {
    local environment=$1
    local namespace="greenearth-$environment"

    if [ "$TEARDOWN" = true ]; then
        teardown_environment "$namespace"
        exit 0
    fi

    log_info "Deploying to $environment environment (namespace: $namespace)"

    trap "cleanup_on_failure $namespace" ERR

    if [ "$INSTALL_ECK" = true ]; then
        install_eck_operator
    fi

    log_info "Creating namespace $namespace..."
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would create namespace $namespace"
    else
        kubectl create namespace $namespace 2>/dev/null || log_info "Namespace $namespace already exists"
    fi

    if [ "$environment" = "stage" ]; then
        log_info "Deploying DaemonSet for vm.max_map_count (stage only)..."
        if [ "$DRY_RUN" = true ]; then
            log_info "[DRY RUN] Would deploy max-map-count-daemonset"
        else
            kubectl apply -f "$K8S_DIR/environments/stage/max-map-count-daemonset.yaml"
            log_info "Waiting 30 seconds for DaemonSet to initialize..."
            sleep 30
        fi
    fi

    log_info "Applying Kustomize manifests..."
    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would apply:"
        kubectl kustomize "$K8S_DIR/environments/$environment"
    else
        kubectl apply -k "$K8S_DIR/environments/$environment"
    fi

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Deployment would continue with resource waiting and verification"
        exit 0
    fi

    wait_for_resource "elasticsearch" "greenearth-es" "$namespace" 600 || {
        log_error "Elasticsearch failed to become ready"
        exit 1
    }

    wait_for_resource "kibana" "greenearth-kibana" "$namespace" 300 || {
        log_error "Kibana failed to become ready"
        exit 1
    }

    log_info "Creating service user secret..."
    kubectl create secret generic es-service-user-secret \
        --from-literal=username="es-service-user" \
        --from-literal=password="$ES_SERVICE_USER_PASSWORD" \
        -n "$namespace" \
        --dry-run=client -o yaml | kubectl apply -f -

    log_info "Waiting for service user setup job..."
    wait_for_job "es-service-user-setup" "$namespace" 180 || {
        log_error "Service user setup job failed"
        exit 1
    }

    log_info "Waiting for bootstrap job..."
    wait_for_job "elasticsearch-bootstrap" "$namespace" 180 || {
        log_error "Bootstrap job failed"
        exit 1
    }

    log_success "Deployment to $environment completed successfully!"
    echo ""
    log_info "Next steps:"
    log_info "  - Access Kibana: kubectl port-forward service/greenearth-kibana-kb-http 5601 -n $namespace"
    log_info "  - Access Elasticsearch: kubectl port-forward service/greenearth-es-http 9200 -n $namespace"
    log_info "  - Get elastic password: kubectl get secret greenearth-es-elastic-user -o go-template='{{.data.elastic | base64decode}}' -n $namespace"
}

ENVIRONMENT=""
INSTALL_ECK=false
SKIP_TEMPLATES=false
DRY_RUN=false
TEARDOWN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        local|stage|prod)
            ENVIRONMENT="$1"
            shift
            ;;
        --install-eck)
            INSTALL_ECK=true
            shift
            ;;
        --skip-templates)
            SKIP_TEMPLATES=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --teardown)
            TEARDOWN=true
            shift
            ;;
        -h|--help)
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

if [ -z "$ENVIRONMENT" ]; then
    log_error "Environment argument is required"
    print_usage
    exit 1
fi

if [ "$ENVIRONMENT" != "local" ] && [ "$ENVIRONMENT" != "stage" ] && [ "$ENVIRONMENT" != "prod" ]; then
    log_error "Invalid environment: $ENVIRONMENT (must be local, stage, or prod)"
    exit 1
fi

if [ "$TEARDOWN" != true ]; then
    verify_prerequisites
fi

deploy_environment "$ENVIRONMENT"
