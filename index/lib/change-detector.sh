#!/bin/bash

# Change detection library for Elasticsearch deployments
# Detects whether templates (schema) or resources have changed

detect_template_changes() {
    local namespace=$1
    local kustomize_dir=$2

    # Check if namespace exists
    if ! kubectl get namespace "$namespace" &>/dev/null; then
        echo "fresh"
        return 0
    fi

    # Check if deployment state ConfigMap exists
    if ! kubectl get configmap elasticsearch-deployment-state -n "$namespace" &>/dev/null; then
        echo "fresh"
        return 0
    fi

    # Get current template checksum from deployment state
    local current_checksum=$(kubectl get configmap elasticsearch-deployment-state \
        -n "$namespace" \
        -o jsonpath='{.data.template-checksum}' 2>/dev/null || echo "")

    if [ -z "$current_checksum" ]; then
        echo "fresh"
        return 0
    fi

    # Compute new template checksum from kustomized manifests
    local new_checksum=$(kubectl kustomize "$kustomize_dir" | \
        grep -A 1000 "kind: ConfigMap" | \
        grep -B 1 -A 100 "name:.*-index-template\|name:.*-alias" | \
        sha256sum | \
        awk '{print $1}')

    if [ "$current_checksum" != "$new_checksum" ]; then
        echo "changed"
        return 0
    fi

    echo "unchanged"
    return 0
}

detect_resource_changes() {
    local namespace=$1
    local kustomize_dir=$2

    # Check if namespace exists
    if ! kubectl get namespace "$namespace" &>/dev/null; then
        echo "fresh"
        return 0
    fi

    # Check if Elasticsearch resource exists
    if ! kubectl get elasticsearch greenearth -n "$namespace" &>/dev/null 2>&1; then
        echo "fresh"
        return 0
    fi

    # Get current Elasticsearch spec (remove metadata and status fields)
    local current_spec=$(kubectl get elasticsearch greenearth -n "$namespace" -o yaml 2>/dev/null | \
        grep -v "resourceVersion\|generation\|uid\|creationTimestamp\|managedFields\|selfLink" | \
        grep -A 1000 "^spec:" | \
        grep -B 1000 "^status:" | \
        head -n -1)

    if [ -z "$current_spec" ]; then
        echo "fresh"
        return 0
    fi

    # Get new Elasticsearch spec from kustomize
    local new_spec=$(kubectl kustomize "$kustomize_dir" | \
        grep -A 1000 "kind: Elasticsearch" | \
        grep -A 1000 "^spec:" | \
        grep -B 1000 "^---" | \
        head -n -1)

    if [ -z "$new_spec" ]; then
        echo "fresh"
        return 0
    fi

    # Compare specs (normalize whitespace)
    local current_normalized=$(echo "$current_spec" | tr -s ' ' | sort)
    local new_normalized=$(echo "$new_spec" | tr -s ' ' | sort)

    if [ "$current_normalized" != "$new_normalized" ]; then
        echo "changed"
        return 0
    fi

    echo "unchanged"
    return 0
}

get_deployment_mode() {
    local namespace=$1
    local kustomize_dir=$2

    local template_status=$(detect_template_changes "$namespace" "$kustomize_dir")
    local resource_status=$(detect_resource_changes "$namespace" "$kustomize_dir")

    # Determine deployment mode
    if [ "$template_status" = "fresh" ] || [ "$resource_status" = "fresh" ]; then
        echo "fresh"
    elif [ "$template_status" = "changed" ] && [ "$resource_status" = "changed" ]; then
        echo "both"
    elif [ "$template_status" = "changed" ]; then
        echo "schema"
    elif [ "$resource_status" = "changed" ]; then
        echo "resource"
    else
        echo "none"
    fi
}

compute_template_checksum() {
    local kustomize_dir=$1

    kubectl kustomize "$kustomize_dir" | \
        grep -A 1000 "kind: ConfigMap" | \
        grep -B 1 -A 100 "name:.*-index-template\|name:.*-alias" | \
        sha256sum | \
        awk '{print $1}'
}
