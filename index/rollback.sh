#!/bin/bash

# Green Earth Elasticsearch - deployment rollback
#
# Elasticsearch does not roll back the way the Cloud Run services do. There is
# no image to swap: the cluster is described by manifests that the ECK operator
# reconciles, so "rolling back" means re-applying the manifests from an earlier
# revision and letting the operator converge on them (see
# greenearth-social/api#181).
#
# This script does that without asking an operator to hand-manage a detached
# checkout mid-incident:
#
#   1. Finds the previous deployment's git sha, from the deployment-history
#      recorded in the cluster's own state ConfigMap by deploy.sh.
#   2. Materializes that revision's manifests in a temporary git worktree, so
#      your working tree is never touched and nothing needs stashing.
#   3. Applies them with the *current* deploy.sh via --manifests-dir, so the
#      rollback runs through reviewed deploy logic rather than re-executing
#      whatever deploy.sh looked like at that sha.
#
# What it will not do:
#
#   - Downgrade the Elasticsearch version. Elasticsearch does not support
#     downgrades; a cluster that has started on a newer version cannot be moved
#     back in place. The escape hatch is restoring a snapshot into a cluster at
#     the older version — see restore.sh. The script refuses and says so.
#   - Undo data. Rolling back an index template changes what future indices look
#     like; documents already written keep the mapping they were written with.
#     A breaking mapping change needs a reindex (tools/reindex.py) or a snapshot
#     restore, not a manifest rollback.
#
# Rollbacks are manual, and kubectl/ECK health behavior is untouched.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GE_GCP_REGION="${GE_GCP_REGION:-us-east1}"
GE_GCP_PROJECT_ID="${GE_GCP_PROJECT_ID:-greenearth-471522}"

GE_ENVIRONMENT=""
TARGET_SHA=""
CHANGE_TYPES=""
LIST_ONLY=false
DRY_RUN=false
ASSUME_YES=false

# Cleaned up on exit; set once the worktree is created.
WORKTREE_DIR=""

print_usage() {
    echo "Usage: $0 <environment> [options]"
    echo ""
    echo "Rolls the Elasticsearch deployment back to an earlier revision's"
    echo "manifests. Unlike the Cloud Run services there is no image to swap:"
    echo "the manifests are re-applied and the ECK operator reconciles."
    echo ""
    echo "Arguments:"
    echo "  environment         Target environment: local, stage, or prod"
    echo ""
    echo "Options:"
    echo "  --to <git-sha>      Revision to roll back to (default: the previous"
    echo "                      deployment recorded in deployment-history)"
    echo "  --ctypes <types>    Change types to re-apply: schema, resource, or"
    echo "                      schema,resource (default: whatever the deployment"
    echo "                      being undone applied)"
    echo "  --list              Show deployment history and exit"
    echo "  --dry-run           Show what would be applied, change nothing"
    echo "  --yes               Skip the confirmation prompt"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 stage --list                    # what has been deployed, newest first"
    echo "  $0 stage                           # undo the last deployment"
    echo "  $0 prod --to a1b2c3d --ctypes schema"
    echo ""
    echo "Notes:"
    echo "  - Elasticsearch version downgrades are not supported by Elasticsearch."
    echo "    The script refuses them; use restore.sh (snapshot restore) instead."
    echo "  - Rolling back index templates affects future indices only. Existing"
    echo "    documents keep the mapping they were written with; a breaking change"
    echo "    needs tools/reindex.py or a snapshot restore."
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

cleanup_worktree() {
    if [ -n "$WORKTREE_DIR" ] && [ -d "$WORKTREE_DIR" ]; then
        git -C "$SCRIPT_DIR" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || true
    fi
}
trap cleanup_worktree EXIT

setup_kubectl_context() {
    local environment=$1

    if [ "$environment" = "local" ]; then
        log_info "Using current kubectl context for local environment"
        return 0
    fi

    local cluster="${GE_K8S_CLUSTER:-greenearth-$environment-cluster}"

    log_info "Configuring kubectl for cluster $cluster..."
    if ! gcloud container clusters get-credentials "$cluster" \
        --location="$GE_GCP_REGION" \
        --project="$GE_GCP_PROJECT_ID" 2>/dev/null; then
        log_error "Could not configure kubectl for cluster $cluster"
        exit 1
    fi
}

# --- deployment history ----------------------------------------------------

# What is deployed right now. deployment-history's newest entry is authoritative
# when it exists: it is appended on every deploy, whereas deployment-git-sha is
# a single slot that older deploy.sh versions did not always update. Falling
# back to the slot keeps this working against clusters deployed before history
# existed.
current_sha() {
    local namespace=$1

    local head
    head="$(deployment_history "$namespace" | head -n 1 | awk '{ print $2 }')"
    if [ -n "$head" ]; then
        echo "$head"
        return 0
    fi

    kubectl get configmap elasticsearch-deployment-state \
        -n "$namespace" \
        -o jsonpath='{.data.deployment-git-sha}' 2>/dev/null || echo ""
}

deployment_history() {
    local namespace=$1
    kubectl get configmap elasticsearch-deployment-state \
        -n "$namespace" \
        -o jsonpath='{.data.deployment-history}' 2>/dev/null || echo ""
}

list_history() {
    local namespace=$1

    local history current
    history="$(deployment_history "$namespace")"
    current="$(current_sha "$namespace")"

    log_info "Deployment history for $namespace (newest first):"
    echo ""

    if [ -z "$history" ]; then
        log_warning "No deployment history recorded yet."
        log_warning "deploy.sh records history from this change onward; deployments made"
        log_warning "before it only left deployment-git-sha: ${current:-(unset)}"
        echo ""
        log_info "Until history accumulates, pick a target from the log:"
        echo "  git log --oneline -- deploy/k8s"
        return 0
    fi

    printf "    %-22s %-10s %s\n" "DEPLOYED" "GIT SHA" "CHANGE TYPES"
    while read -r timestamp sha ctypes; do
        [ -z "$timestamp" ] && continue
        local marker="  "
        if [ "$sha" = "$current" ]; then
            marker="=>"
        fi
        printf "%s  %-22s %-10s %s\n" "$marker" "$timestamp" "$sha" "$ctypes"
    done <<< "$history"

    echo ""
    echo "  => currently deployed"
    echo ""
    echo "  Undo the last deployment:  $0 $GE_ENVIRONMENT"
    echo "  Roll back to a revision:   $0 $GE_ENVIRONMENT --to <git-sha>"
}

# Echoes "sha ctypes" for the newest history entry whose sha differs from what
# is deployed now.
resolve_previous_deployment() {
    local namespace=$1
    local current
    current="$(current_sha "$namespace")"

    local timestamp sha ctypes
    while read -r timestamp sha ctypes; do
        [ -z "$sha" ] && continue
        if [ "$sha" != "$current" ]; then
            echo "$sha $ctypes"
            return 0
        fi
    done <<< "$(deployment_history "$namespace")"

    return 1
}

# The change types the deployment being undone applied — re-applying the same
# kinds is what actually reverses it.
ctypes_of_current_deployment() {
    local namespace=$1
    local current
    current="$(current_sha "$namespace")"

    local timestamp sha ctypes
    while read -r timestamp sha ctypes; do
        [ -z "$sha" ] && continue
        if [ "$sha" = "$current" ]; then
            echo "$ctypes"
            return 0
        fi
    done <<< "$(deployment_history "$namespace")"

    return 1
}

# --- target preparation ----------------------------------------------------

require_valid_target() {
    if ! git -C "$SCRIPT_DIR" cat-file -e "$TARGET_SHA^{commit}" 2>/dev/null; then
        log_error "Not a commit in this repository: $TARGET_SHA"
        log_error "Fetch first, or pick a target from '$0 $GE_ENVIRONMENT --list'."
        exit 1
    fi
}

# Materializes the target revision's manifests without disturbing the working
# tree. A worktree (rather than `git checkout`) means an interrupted rollback
# leaves nothing to clean up by hand and no detached HEAD to discover later.
create_worktree() {
    WORKTREE_DIR="$(mktemp -d -t es-rollback)"
    rm -rf "$WORKTREE_DIR"

    log_info "Materializing manifests for $TARGET_SHA..."
    if ! git -C "$SCRIPT_DIR" worktree add --detach "$WORKTREE_DIR" "$TARGET_SHA" > /dev/null 2>&1; then
        log_error "Could not create a git worktree for $TARGET_SHA"
        exit 1
    fi
}

manifests_dir() {
    echo "$WORKTREE_DIR/index/deploy/k8s"
}

require_manifests() {
    local dir
    dir="$(manifests_dir)"

    if [ ! -d "$dir/environments/$GE_ENVIRONMENT" ]; then
        log_error "Revision $TARGET_SHA has no manifests at index/deploy/k8s/environments/$GE_ENVIRONMENT"
        log_error "That revision predates the current layout — roll back by hand from a checkout."
        exit 1
    fi
}

# POSIX character class rather than \s: \s is a GNU extension, and if the
# pattern silently matched nothing the downgrade check below would fail open.
manifest_es_version() {
    local dir=$1
    grep -E '^[[:space:]]+version:' "$dir/base/elasticsearch.yaml" 2>/dev/null \
        | head -n 1 \
        | awk '{ print $2 }'
}

# Elasticsearch does not support downgrades. Applying a manifest with a lower
# spec.version does not roll the cluster back — at best ECK refuses, at worst
# nodes fail to start against data written by the newer version. Refuse loudly
# and point at the only mechanism that does work.
require_no_version_downgrade() {
    local target_version current_version
    target_version="$(manifest_es_version "$(manifests_dir)")"
    current_version="$(manifest_es_version "$SCRIPT_DIR/deploy/k8s")"

    if [ -z "$target_version" ] || [ -z "$current_version" ]; then
        log_warning "Could not read spec.version from one of the manifests — skipping the"
        log_warning "version-downgrade check. Confirm by hand that the version is unchanged."
        return 0
    fi

    if [ "$target_version" = "$current_version" ]; then
        return 0
    fi

    # Sort the two versions; if the target sorts first, it is older.
    local older
    older="$(printf '%s\n%s\n' "$target_version" "$current_version" | sort -V | head -n 1)"

    if [ "$older" = "$target_version" ]; then
        log_error "Refusing to roll back: this would downgrade Elasticsearch."
        log_error "  deployed: $current_version"
        log_error "  target:   $target_version"
        log_error ""
        log_error "Elasticsearch does not support in-place downgrades. To get back to"
        log_error "$target_version, restore a snapshot into a cluster at that version:"
        log_error "  ./restore.sh --environment $GE_ENVIRONMENT --snapshot <name>"
        exit 1
    fi

    log_warning "Target manifests specify a NEWER Elasticsearch version ($target_version)"
    log_warning "than what is deployed ($current_version). This is an upgrade, not a rollback."
}

# --- rollback --------------------------------------------------------------

confirm() {
    local namespace=$1

    echo ""
    if [ "$GE_ENVIRONMENT" = "prod" ]; then
        echo "*** PRODUCTION ELASTICSEARCH ROLLBACK ***"
    fi
    echo "  namespace:    $namespace"
    echo "  deployed:     $(current_sha "$namespace")"
    echo "  target:       $TARGET_SHA"
    echo "  change types: $CHANGE_TYPES"
    echo ""
    echo "  $(git -C "$SCRIPT_DIR" log -1 --format='%h %s' "$TARGET_SHA")"
    echo ""
    echo "  Re-applies that revision's manifests; the ECK operator reconciles."
    echo "  Documents already indexed keep their existing mappings."
    echo ""

    if [ "$ASSUME_YES" = true ]; then
        return 0
    fi

    local reply
    read -r -p "Apply this rollback? Type 'yes' to confirm: " reply
    if [ "$reply" != "yes" ]; then
        log_info "Aborted — nothing changed."
        exit 0
    fi
}

apply_rollback() {
    local cmd=("$SCRIPT_DIR/deploy.sh" "$GE_ENVIRONMENT"
               --ctypes "$CHANGE_TYPES"
               --manifests-dir "$(manifests_dir)")

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] would execute:"
        echo "  ${cmd[*]}"
        log_info "[DRY RUN] manifests that would be applied:"
        kubectl kustomize "$(manifests_dir)/environments/$GE_ENVIRONMENT" | head -n 60
        log_info "[DRY RUN] (output truncated; nothing was changed)"
        return 0
    fi

    log_info "Applying rollback via deploy.sh..."
    "${cmd[@]}"
}

main() {
    local namespace="greenearth-$GE_ENVIRONMENT"

    setup_kubectl_context "$GE_ENVIRONMENT"

    if [ "$LIST_ONLY" = true ]; then
        list_history "$namespace"
        exit 0
    fi

    if [ -z "$TARGET_SHA" ]; then
        local previous
        if ! previous="$(resolve_previous_deployment "$namespace")"; then
            log_error "No previous deployment found in deployment-history."
            log_error "Run '$0 $GE_ENVIRONMENT --list', or pass --to <git-sha> explicitly."
            exit 1
        fi
        TARGET_SHA="${previous%% *}"
        if [ -z "$CHANGE_TYPES" ]; then
            # Reverse the deployment being undone by re-applying the same kinds
            # of change it made.
            CHANGE_TYPES="$(ctypes_of_current_deployment "$namespace" || echo "")"
        fi
    fi

    if [ -z "$CHANGE_TYPES" ]; then
        log_warning "Could not infer change types; defaulting to schema,resource."
        CHANGE_TYPES="schema,resource"
    fi

    if [ "$CHANGE_TYPES" = "init" ]; then
        log_error "Cannot roll back with --ctypes init: that path builds a fresh"
        log_error "environment and deletes the namespace on failure. Use schema,"
        log_error "resource, or schema,resource."
        exit 1
    fi

    require_valid_target
    create_worktree
    require_manifests
    require_no_version_downgrade

    confirm "$namespace"
    apply_rollback

    if [ "$DRY_RUN" != true ]; then
        echo ""
        log_success "Rollback applied. The ECK operator reconciles asynchronously."
        log_info "Watch it converge:"
        echo "  kubectl get elasticsearch greenearth -n $namespace -w"
        echo "  kubectl get pods -n $namespace"
        log_info "Deployment history now records $TARGET_SHA as the deployed revision."
    fi
}

while [[ $# -gt 0 ]]; do
    case $1 in
        local|stage|prod)
            GE_ENVIRONMENT="$1"
            shift
            ;;
        --to)
            TARGET_SHA="$2"
            shift 2
            ;;
        --ctypes)
            CHANGE_TYPES="$2"
            if ! [[ "$CHANGE_TYPES" =~ ^(schema|resource|snapshot|schema,resource|resource,schema)$ ]]; then
                log_error "Invalid --ctypes value: $CHANGE_TYPES"
                log_error "Valid options for a rollback: schema, resource, snapshot, schema,resource"
                exit 1
            fi
            shift 2
            ;;
        --list)
            LIST_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --yes)
            ASSUME_YES=true
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

if [ -z "$GE_ENVIRONMENT" ]; then
    log_error "Environment argument is required"
    print_usage
    exit 1
fi

main
