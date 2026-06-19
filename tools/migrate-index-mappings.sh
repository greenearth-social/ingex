#!/usr/bin/env bash
# migrate-index-mappings.sh
#
# Reindexes posts-* and replies-* indices to pick up the updated ES mappings
# introduced in PR #389 (reduced inverted-index footprint, tighter HNSW set).
#
# ILM templates only apply to newly created indices.  Existing indices retain
# their old mappings and must be reindexed to benefit from the changes.
#
# Each source index is reindexed into <name>-<commit> where <commit> is the
# short hash of the HEAD commit at run time (picks up the updated template
# mapping automatically), all aliases are moved atomically, and the source
# index is deleted.
#
# The index currently pointed to by posts_recent is still receiving live
# writes, so it is skipped by default.  Re-run with --include-active after
# the period rolls over naturally and the new period's index is live.
#
# Prerequisites:
#   - Updated ILM templates have been deployed (bootstrap job or manual PUT)
#   - ingex has been deployed with the PostDoc/ReplyDoc code from this PR
#
# Usage:
#   export GE_ELASTICSEARCH_URL=https://...
#   export GE_ELASTICSEARCH_API_KEY=...
#   ./tools/migrate-index-mappings.sh [--dry-run] [--include-active]
#
# Flags:
#   --dry-run         Print what would happen; make no changes.
#   --include-active  Also migrate the index currently aliased by posts_recent.
#                     Only safe once the current period has rolled over.

set -euo pipefail

ES_URL="${GE_ELASTICSEARCH_URL:?GE_ELASTICSEARCH_URL must be set}"
ES_API_KEY="${GE_ELASTICSEARCH_API_KEY:?GE_ELASTICSEARCH_API_KEY must be set}"

DRY_RUN=false
INCLUDE_ACTIVE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run)        DRY_RUN=true ;;
    --include-active) INCLUDE_ACTIVE=true ;;
  esac
done

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# Thin wrapper around curl; exits non-zero on HTTP error.
es() {
  local method="$1" path="$2"; shift 2
  curl -sf -X "$method" \
    -H "Authorization: ApiKey $ES_API_KEY" \
    -H "Content-Type: application/json" \
    "$ES_URL/$path" "$@"
}

# Return the index name that backs a given alias, or empty string if none.
active_index_for() {
  local alias="$1"
  es GET "*/_alias/$alias" 2>/dev/null \
    | jq -r '(keys // []) | first // empty' \
    || true
}

# List all indices matching a wildcard pattern, newest-creation-date first.
list_indices() {
  es GET "_cat/indices/$1?h=index,creation.date&s=creation.date:desc&format=json" \
    | jq -r '.[].index'
}

# Return the alias configuration object for a given index.
get_aliases() {
  es GET "$1/_alias" | jq -c ".\"$1\".aliases // {}"
}

# ── Core migration for a single index ────────────────────────────────────────
migrate_index() {
  local src="$1"
  local dst="${src}-${COMMIT}"

  if $DRY_RUN; then
    info "[dry-run] $src → $dst  (move aliases, delete $src)"
    return 0
  fi

  # Restart-safe: if the destination already exists, skip rather than clobber.
  if es GET "$dst" &>/dev/null; then
    warn "$dst already exists — skipping (delete it manually to re-run this index)"
    return 0
  fi

  info "Reindexing $src → $dst ..."
  local result
  result=$(es POST "_reindex?wait_for_completion=true" -d \
    "{\"source\":{\"index\":\"$src\"},\"dest\":{\"index\":\"$dst\"}}")

  local total failures took
  total=$(   echo "$result" | jq -r '.total')
  failures=$(echo "$result" | jq -r '.failures | length')
  took=$(    echo "$result" | jq -r '.took')
  info "  reindexed ${total} docs in ${took}ms, ${failures} failures"

  if [[ "$failures" -gt 0 ]]; then
    error "Reindex of $src reported failures — skipping alias swap and deletion."
    echo "$result" | jq '.failures'
    return 1
  fi

  # Build an atomic alias-swap: remove each alias from src, add to dst.
  local aliases
  aliases=$(get_aliases "$src")

  local actions='[]'
  while IFS= read -r alias_name; do
    local alias_cfg
    alias_cfg=$(echo "$aliases" | jq -c --arg a "$alias_name" '.[$a]')
    # Preserve is_write_index flag if present.
    local write_flag='{}'
    if echo "$alias_cfg" | jq -e '.is_write_index == true' &>/dev/null; then
      write_flag='{"is_write_index": true}'
    fi
    actions=$(echo "$actions" | jq \
      --arg src "$src" --arg dst "$dst" --arg a "$alias_name" \
      --argjson wf "$write_flag" \
      '. + [
        {"remove": {"index": $src, "alias": $a}},
        {"add":    ({"index": $dst, "alias": $a} + $wf)}
      ]')
  done < <(echo "$aliases" | jq -r 'keys[]')

  if [[ $(echo "$actions" | jq 'length') -gt 0 ]]; then
    local alias_names
    alias_names=$(echo "$aliases" | jq -r 'keys | join(", ")')
    info "  Moving aliases: $alias_names"
    es POST "_aliases" -d "{\"actions\": $actions}" \
      | jq -r '"  acknowledged: " + (.acknowledged | tostring)'
  fi

  # Remove the original index.
  es DELETE "$src" | jq -r '"  deleted: " + (.acknowledged | tostring)'
  info "✓ $src migrated to $dst"
}

# ── Main ─────────────────────────────────────────────────────────────────────

info "Connecting to $ES_URL ..."
es GET "" | jq -r '"  Elasticsearch " + .version.number'
echo ""

# Identify active write indices (backed by the sliding alias).
active_posts=$(active_index_for "posts_recent")
active_replies=$(active_index_for "replies_recent")

if $INCLUDE_ACTIVE; then
  active_posts=""
  active_replies=""
else
  [[ -n "$active_posts" ]]   && warn "Skipping active posts index:   $active_posts"
  [[ -n "$active_replies" ]] && warn "Skipping active replies index: $active_replies"
  echo "  (Re-run with --include-active after the period rolls over.)"
  echo ""
fi

$DRY_RUN && warn "Dry-run mode — no changes will be made." && echo ""

COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
info "Commit hash for destination suffix: $COMMIT"
echo ""

errors=0

# ── posts-* ──────────────────────────────────────────────────────────────────
info "=== posts-* (newest first) ==="
while IFS= read -r idx; do
  if [[ "$idx" == "$active_posts" ]]; then
    warn "Skipping active index: $idx"
    continue
  fi
  # Skip indices that were already renamed by a previous run of this script
  # (suffix is a 7-char hex commit hash appended with a hyphen).
  if [[ "$idx" =~ -[0-9a-f]{7}$ ]]; then
    info "Skipping already-migrated index: $idx"
    continue
  fi
  migrate_index "$idx" || (( errors++ )) || true
done < <(list_indices "posts-*")

echo ""

# ── replies-* ────────────────────────────────────────────────────────────────
info "=== replies-* (newest first) ==="
while IFS= read -r idx; do
  if [[ "$idx" == "$active_replies" ]]; then
    warn "Skipping active index: $idx"
    continue
  fi
  if [[ "$idx" == *-migrated ]]; then
    info "Skipping already-migrated index: $idx"
    continue
  fi
  migrate_index "$idx" || (( errors++ )) || true
done < <(list_indices "replies-*")

echo ""

if [[ "$errors" -gt 0 ]]; then
  error "Migration finished with $errors error(s). Review output above."
  exit 1
fi

info "Migration complete."
