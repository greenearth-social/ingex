#!/bin/bash

# ingestctl - Simple control script for ingestion services

set -e

# Configuration
GE_ENVIRONMENT="${GE_ENVIRONMENT:-stage}"
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"
GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"

SERVICES=(
    "jetstream-ingest-${GE_ENVIRONMENT}"
    "megastream-ingest-${GE_ENVIRONMENT}"
    # "elasticsearch-expiry-${GE_ENVIRONMENT}"
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if a service exists
service_exists() {
    local service=$1
    gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" > /dev/null 2>&1
}

# Get service status (simplified)
get_service_status() {
    local service=$1

    if ! service_exists "$service"; then
        echo "NOT_DEPLOYED"
        return
    fi

    # Check manual instance count (0 = stopped)
    local instance_count=$(gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(spec.template.metadata.annotations['run.googleapis.com/scaling-mode'])" 2>/dev/null || echo "")

    local manual_count=$(gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="value(spec.template.metadata.annotations['run.googleapis.com/manual-scaling'])" 2>/dev/null || echo "")

    if [[ "$manual_count" == "0" ]]; then
        echo "STOPPED"
    elif [[ -n "$manual_count" && "$manual_count" != "0" ]]; then
        echo "RUNNING"
    else
        # Check ready condition
        local ready=$(gcloud run services describe "$service" \
            --region="$GE_GCP_REGION" \
            --project="$GE_GCP_PROJECT_ID" \
            --format="value(status.url)" 2>/dev/null)

        if [[ -n "$ready" ]]; then
            echo "RUNNING"
        else
            echo "STARTING"
        fi
    fi
}

# Start a service
start_service() {
    local service=$1

    if ! service_exists "$service"; then
        echo -e "${RED}Error: Service $service is not deployed${NC}"
        return 1
    fi

    echo -e "${BLUE}Starting $service...${NC}"

    gcloud run services update "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --scaling=1 \
        --quiet

    echo -e "${GREEN}Service $service started (manual scaling: 1 instance)${NC}"
}

# Stop a service
stop_service() {
    local service=$1

    if ! service_exists "$service"; then
        echo -e "${RED}Error: Service $service is not deployed${NC}"
        return 1
    fi

    echo -e "${BLUE}Stopping $service...${NC}"

    gcloud run services update "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --scaling=0 \
        --quiet

    echo -e "${YELLOW}Service $service stopped (manual scaling: 0 instances)${NC}"
}

# Show service status
show_status() {
    echo -e "${BLUE}Ingestion Services Status:${NC}"
    echo "========================="

    for service in "${SERVICES[@]}"; do
        local status=$(get_service_status "$service")
        local color

        case "$status" in
            "RUNNING") color="$GREEN" ;;
            "STOPPED") color="$YELLOW" ;;
            "NOT_DEPLOYED") color="$RED" ;;
            *) color="$BLUE" ;;
        esac

        printf "%-20s %b%s%b\n" "$service" "$color" "$status" "$NC"
    done
}

# Show detailed service information
show_details() {
    local service=$1

    if [[ -z "$service" ]]; then
        echo -e "${RED}Error: Please specify a service name${NC}"
        echo "Available services: ${SERVICES[*]}"
        return 1
    fi

    if ! service_exists "$service"; then
        echo -e "${RED}Error: Service $service is not deployed${NC}"
        return 1
    fi

    echo -e "${BLUE}Details for $service:${NC}"
    echo "===================="

    gcloud run services describe "$service" \
        --region="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" \
        --format="table(
            metadata.name,
            status.url,
            status.conditions[0].status,
            spec.template.metadata.annotations['autoscaling.knative.dev/minScale']:label=MIN_INSTANCES,
            spec.template.metadata.annotations['autoscaling.knative.dev/maxScale']:label=MAX_INSTANCES
        )"
}

# Main command handling
main() {
    local command=$1
    local service=$2

    case "$command" in
        "start")
            if [[ -n "$service" ]]; then
                start_service "$service"
            else
                echo -e "${BLUE}Starting all services...${NC}"
                for svc in "${SERVICES[@]}"; do
                    start_service "$svc"
                done
            fi
            ;;
        "stop")
            if [[ -n "$service" ]]; then
                stop_service "$service"
            else
                echo -e "${BLUE}Stopping all services...${NC}"
                for svc in "${SERVICES[@]}"; do
                    stop_service "$svc"
                done
            fi
            ;;
        "status")
            show_status
            ;;
        "details")
            show_details "$service"
            ;;
        "restart")
            if [[ -n "$service" ]]; then
                stop_service "$service"
                echo "Waiting 5 seconds..."
                sleep 5
                start_service "$service"
            else
                echo -e "${RED}Error: Please specify a service to restart${NC}"
                return 1
            fi
            ;;
        *)
            echo "Usage: $0 {start|stop|status|details|restart} [service_name]"
            echo ""
            echo "Commands:"
            echo "  start [service]    - Start service(s)"
            echo "  stop [service]     - Stop service(s)"
            echo "  status            - Show status of all services"
            echo "  details <service> - Show detailed info for a service"
            echo "  restart <service> - Restart a specific service"
            echo ""
            echo "Available services: ${SERVICES[*]}"
            return 1
            ;;
    esac
}

# Run the main function with all arguments
main "$@"
