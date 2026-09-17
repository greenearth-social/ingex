#!/bin/bash
#
# Tests for deploy.sh + rollback.sh that need no cluster.
#
# Fake kubectl/gcloud/sleep are placed early on PATH, so deploy.sh and
# rollback.sh run to completion (or fail at a chosen point) without touching any
# cluster, and every command they issue is recorded for assertions.
#
# The reason this exists rather than a stage exercise: the most dangerous
# behavior here is cleanup_on_failure, which ends in `kubectl delete namespace`.
# Verifying when it does and does not fire cannot be done against a real
# cluster, because getting it wrong deletes the cluster. Everything else that is
# cheaper to test here than on stage came along for the ride.
#
# Usage:  ./rollback_test.sh                  # from ingex/index
#         KEEP_WORKDIR=1 ./rollback_test.sh   # leave temp files for inspection
#
# Requires: git, and kubectl on PATH (used only to render kustomize locally --
# no cluster contact). Makes no network calls and never contacts GCP.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INDEX_DIR="${1:-$SCRIPT_DIR}"

# Resolve the real kubectl before the fake shadows it. Rendering kustomize is a
# local operation, and the assertion about what the rendered ConfigMap contains
# would pass vacuously against the shim.
REAL_KUBECTL="$(command -v kubectl || true)"

WORK="$(mktemp -d)"
BIN="$WORK/bin"
mkdir -p "$BIN"
export CMDLOG="$WORK/commands.log"
: > "$CMDLOG"

TEMP_BRANCH="rollback-test-version-$$"

cleanup() {
    git -C "$INDEX_DIR" worktree remove --force "$WORK/verworktree" 2>/dev/null || true
    git -C "$INDEX_DIR" branch -D "$TEMP_BRANCH" 2>/dev/null || true
    git -C "$INDEX_DIR" worktree remove --force "$WORK/worktree" 2>/dev/null || true
    if [ -z "${KEEP_WORKDIR:-}" ]; then
        rm -rf "$WORK"
    else
        echo "  workdir kept: $WORK"
    fi
}
trap cleanup EXIT

PASS=0
FAIL=0
ok()  { echo "  ok   - $1"; PASS=$((PASS + 1)); }
bad() { echo "  FAIL - $1"; FAIL=$((FAIL + 1)); }

# --- fakes -----------------------------------------------------------------

cat > "$BIN/kubectl" <<'SHIM'
#!/bin/bash
echo "kubectl $*" >> "$CMDLOG"

# Fail on demand, to drive deploy.sh down its error path.
if [ -n "${FAIL_ON_KUBECTL:-}" ] && [[ "$*" == *"$FAIL_ON_KUBECTL"* ]]; then
    echo "simulated kubectl failure: $*" >&2
    exit 1
fi

case "$*" in
    # Report a healthy, ready cluster so waits complete on the first poll.
    *"get elasticsearch greenearth"*health*) echo "green" ;;
    *"get elasticsearch greenearth"*phase*)  echo "Ready" ;;
    *"get kibana"*health*)                   echo "green" ;;
    *"get job"*Complete*)                    echo "True" ;;
    *"get job"*Failed*)                      echo "" ;;
    # Deployment state reads, driven by the fixture files.
    *"get configmap elasticsearch-deployment-state"*deployment-history*)
        cat "${FAKE_HISTORY_FILE:-/dev/null}" 2>/dev/null || true ;;
    *"get configmap elasticsearch-deployment-state"*deployment-git-sha*)
        cat "${FAKE_SHA_FILE:-/dev/null}" 2>/dev/null || true ;;
    *) : ;;
esac
exit 0
SHIM

cat > "$BIN/gcloud" <<'SHIM'
#!/bin/bash
echo "gcloud $*" >> "$CMDLOG"
exit 0
SHIM

# deploy.sh waits 30s for a DaemonSet and polls on 5s intervals. Those waits are
# real time we do not need to spend: the fake kubectl reports ready immediately,
# so every polling loop exits on its first pass. A short real sleep is kept
# rather than a no-op so that a future change which does loop cannot spin hot.
cat > "$BIN/sleep" <<'SHIM'
#!/bin/bash
exec /bin/sleep 0.05
SHIM

chmod +x "$BIN/kubectl" "$BIN/gcloud" "$BIN/sleep"
export PATH="$BIN:$PATH"
export GE_ELASTICSEARCH_SERVICE_USER_PWD="harness-dummy"
export GE_K8S_CLUSTER="greenearth-stage-cluster"

# Guard against the fakes not taking effect, which would let these tests reach a
# real cluster with real credentials.
if [ "$(command -v kubectl)" != "$BIN/kubectl" ]; then
    echo "refusing to run: kubectl is not the harness fake" >&2
    exit 1
fi

run_deploy() {
    : > "$CMDLOG"
    ( cd "$INDEX_DIR" && ./deploy.sh "$@" ) > "$WORK/out.log" 2>&1
    echo $?
}

run_rollback() {
    : > "$CMDLOG"
    ( cd "$INDEX_DIR" && ./rollback.sh "$@" ) > "$WORK/out.log" 2>&1
    echo $?
}

echo ""
echo "=== 1. ERR trap scoping (the namespace-deletion guard) ==="

# deploy.sh runs under `set -e` but not `set -E`. Bash does not inherit an ERR
# trap into nested functions without errtrace, so today the trap cannot fire for
# a failure inside deploy_init/deploy_schema_update/etc. That is the baseline:
# the guard is latent, not active.
for ctype in schema resource init; do
    FAIL_ON_KUBECTL="apply -k" run_deploy stage --ctypes "$ctype" > /dev/null
    if ! grep -q "apply -k" "$CMDLOG"; then
        bad "[stock] --ctypes $ctype never reached 'apply -k' — assertion would be vacuous"
    elif grep -q "delete namespace" "$CMDLOG"; then
        bad "[stock] failed --ctypes $ctype deleted the namespace"
    else
        ok "[stock] failed --ctypes $ctype does not delete the namespace"
    fi
done

# The case that matters: adding `set -E` is an ordinary hardening change, and it
# arms the trap. Under that condition the scoping must hold.
HARDENED="$WORK/hardened"
rm -rf "$HARDENED"
cp -R "$INDEX_DIR" "$HARDENED"
sed -i.bak 's/^set -e$/set -eE/' "$HARDENED/deploy.sh"
rm -f "$HARDENED/deploy.sh.bak"
if grep -q "^set -eE$" "$HARDENED/deploy.sh"; then
    ok "[errtrace] built hardened copy with set -eE"
else
    bad "[errtrace] could not build hardened copy — following assertions are vacuous"
fi

run_hardened() {
    : > "$CMDLOG"
    ( cd "$HARDENED" && ./deploy.sh "$@" ) > "$WORK/out.log" 2>&1
    echo $?
}

for ctype in schema resource; do
    FAIL_ON_KUBECTL="apply -k" run_hardened stage --ctypes "$ctype" > /dev/null
    if grep -q "delete namespace" "$CMDLOG"; then
        bad "[errtrace] failed --ctypes $ctype DELETED THE NAMESPACE"
    else
        ok "[errtrace] failed --ctypes $ctype does not delete the namespace"
    fi
done

FAIL_ON_KUBECTL="apply -k" run_hardened stage --ctypes init > /dev/null
if grep -q "delete namespace" "$CMDLOG"; then
    ok "[errtrace] failed --ctypes init still cleans up (guard kept where it belongs)"
else
    bad "[errtrace] failed --ctypes init did not clean up"
fi

echo ""
echo "=== 2. deployment-history survives 'apply -k' ==="

# deploy.sh applies the kustomize base before recording state. A declared
# deployment-history key would therefore be reset to "" moments before the patch
# writes to it, capping history at one entry and making rollback.sh useless.
if grep -q "^  deployment-history:" "$INDEX_DIR/deploy/k8s/base/deployment-state-configmap.yaml"; then
    bad "deployment-history is declared in the base ConfigMap (apply -k would wipe it each deploy)"
else
    ok "deployment-history is not declared in the base ConfigMap"
fi

if [ -n "$REAL_KUBECTL" ]; then
    if "$REAL_KUBECTL" kustomize "$INDEX_DIR/deploy/k8s/environments/stage" 2>/dev/null \
        | grep -q "deployment-history"; then
        bad "rendered kustomize output contains deployment-history"
    else
        ok "rendered kustomize output does not contain deployment-history"
    fi
else
    echo "  skip - kubectl not installed; cannot render kustomize"
fi

echo ""
echo "=== 3. deployment-git-sha recorded for every change type ==="

# A change type that records history but not the sha leaves the two disagreeing,
# which sends rollback.sh at the revision that is already deployed.
for ctype in schema resource; do
    run_deploy stage --ctypes "$ctype" > /dev/null
    for key in deployment-git-sha deployment-history; do
        if grep -q "patch configmap elasticsearch-deployment-state.*$key" "$CMDLOG"; then
            ok "--ctypes $ctype records $key"
        else
            bad "--ctypes $ctype does not record $key"
        fi
    done
done

echo ""
echo "=== 4. --manifests-dir ==="

BOGUS="$WORK/not-a-manifest-root"
mkdir -p "$BOGUS"
rc=$(run_deploy stage --ctypes schema --manifests-dir "$BOGUS")
if [ "$rc" -ne 0 ] && grep -q "must point at a k8s manifest root" "$WORK/out.log"; then
    ok "--manifests-dir rejects a directory with no environments/"
else
    bad "--manifests-dir accepted a bogus directory (rc=$rc)"
fi

OLD_SHA="$(git -C "$INDEX_DIR" log --format=%h -8 -- deploy/k8s | tail -1)"
WT="$WORK/worktree"
if git -C "$INDEX_DIR" worktree add --detach "$WT" "$OLD_SHA" > /dev/null 2>&1; then
    rc=$(run_deploy stage --ctypes schema --manifests-dir "$WT/index/deploy/k8s")
    if [ "$rc" -eq 0 ]; then
        ok "--manifests-dir accepts a worktree of an older revision ($OLD_SHA)"
    else
        bad "--manifests-dir run failed against worktree (rc=$rc)"
    fi
    # get_git_sha must describe the manifests being applied, not the checkout.
    if grep -q "deployment-git-sha[^,]*$OLD_SHA" "$CMDLOG"; then
        ok "recorded sha is the worktree's ($OLD_SHA), not the checkout's HEAD"
    else
        bad "recorded sha is not $OLD_SHA — get_git_sha read the wrong repo"
    fi
    git -C "$INDEX_DIR" worktree remove --force "$WT" > /dev/null 2>&1
else
    bad "could not create test worktree for $OLD_SHA"
fi

echo ""
echo "=== 5. rollback.sh target resolution ==="

HEAD_SHA="$(git -C "$INDEX_DIR" rev-parse --short HEAD)"
export FAKE_HISTORY_FILE="$WORK/history"
export FAKE_SHA_FILE="$WORK/sha"

printf '2026-08-27T10:00:00Z %s schema\n2026-08-20T09:00:00Z %s schema\n' \
    "$HEAD_SHA" "$OLD_SHA" > "$FAKE_HISTORY_FILE"
echo -n "$HEAD_SHA" > "$FAKE_SHA_FILE"

rc=$(run_rollback stage --dry-run --yes)
if [ "$rc" -eq 0 ] && grep "target:" "$WORK/out.log" | grep -q "$OLD_SHA"; then
    ok "default target is the previous history entry ($OLD_SHA)"
else
    bad "wrong default target (rc=$rc): $(grep 'target:' "$WORK/out.log" | head -1)"
fi

# A stale deployment-git-sha -- what resource-only deploys used to leave behind
# -- must not send the rollback at the already-deployed revision.
echo -n "$OLD_SHA" > "$FAKE_SHA_FILE"
run_rollback stage --dry-run --yes > /dev/null
if grep "target:" "$WORK/out.log" | grep -q "$OLD_SHA"; then
    ok "stale deployment-git-sha ignored; history head is authoritative"
else
    bad "stale deployment-git-sha changed the target: $(grep 'target:' "$WORK/out.log" | head -1)"
fi
echo -n "$HEAD_SHA" > "$FAKE_SHA_FILE"

rc=$(run_rollback stage --to deadbee --yes)
if [ "$rc" -ne 0 ] && grep -q "Not a commit in this repository" "$WORK/out.log"; then
    ok "unknown revision is refused"
else
    bad "unknown revision was not refused (rc=$rc)"
fi

rc=$(run_rollback stage --ctypes init --yes)
if [ "$rc" -ne 0 ]; then
    ok "--ctypes init is refused for rollback"
else
    bad "--ctypes init was accepted for rollback"
fi

run_rollback stage --dry-run --yes > /dev/null
if grep -qE "kubectl (apply|patch|delete)" "$CMDLOG"; then
    bad "--dry-run issued a mutating kubectl command"
else
    ok "--dry-run issues no mutating kubectl commands"
fi

echo ""
echo "=== 6. Elasticsearch version-downgrade refusal ==="

CUR_VERSION="$(grep -E '^[[:space:]]+version:' "$INDEX_DIR/deploy/k8s/base/elasticsearch.yaml" \
    | head -1 | awk '{print $2}')"
echo "  (deployed manifests declare Elasticsearch $CUR_VERSION)"

# A worktree on a scratch branch whose manifests claim a newer version, so that
# rolling back to the real revision is a downgrade. The branch and its commit
# are deleted on exit.
VERWT="$WORK/verworktree"
if git -C "$INDEX_DIR" worktree add -q -b "$TEMP_BRANCH" "$VERWT" HEAD > /dev/null 2>&1; then
    (
        cd "$VERWT"
        sed -i.bak "s/^\([[:space:]]*version:\).*/\1 9.9.9/" index/deploy/k8s/base/elasticsearch.yaml
        rm -f index/deploy/k8s/base/elasticsearch.yaml.bak
        git commit -qam "test: bump ES version to 9.9.9"
    )
    NEWER_SHA="$(git -C "$VERWT" rev-parse --short HEAD)"
    BASE_SHA="$(git -C "$VERWT" rev-parse --short HEAD~1)"

    if git -C "$VERWT" diff --quiet HEAD~1 HEAD -- index/deploy/k8s/base/elasticsearch.yaml; then
        bad "scratch version bump did not take — downgrade assertions would be vacuous"
    fi

    # Checkout is at 9.9.9; target is the real revision at $CUR_VERSION.
    printf '2026-08-27T10:00:00Z %s schema\n2026-08-20T09:00:00Z %s schema\n' \
        "$NEWER_SHA" "$BASE_SHA" > "$FAKE_HISTORY_FILE"
    echo -n "$NEWER_SHA" > "$FAKE_SHA_FILE"

    rc=$( ( cd "$VERWT/index" && ./rollback.sh stage --yes ) > "$WORK/out.log" 2>&1; echo $? )
    if [ "$rc" -ne 0 ] && grep -q "would downgrade Elasticsearch" "$WORK/out.log"; then
        ok "downgrade 9.9.9 -> $CUR_VERSION refused, pointing at restore.sh"
    else
        bad "downgrade was NOT refused (rc=$rc)"
    fi

    # The reverse is an upgrade: warn, do not refuse. The checkout must sit at
    # the OLD version or this compares a version against itself.
    git -C "$VERWT" checkout -q "$BASE_SHA"
    CHECKOUT_VERSION="$(grep -E '^[[:space:]]+version:' "$VERWT/index/deploy/k8s/base/elasticsearch.yaml" \
        | head -1 | awk '{print $2}')"
    if [ "$CHECKOUT_VERSION" != "$CUR_VERSION" ]; then
        bad "upgrade setup wrong: checkout declares $CHECKOUT_VERSION, expected $CUR_VERSION"
    fi
    printf '2026-08-27T10:00:00Z %s schema\n2026-08-20T09:00:00Z %s schema\n' \
        "$BASE_SHA" "$NEWER_SHA" > "$FAKE_HISTORY_FILE"
    echo -n "$BASE_SHA" > "$FAKE_SHA_FILE"

    rc=$( ( cd "$VERWT/index" && ./rollback.sh stage --to "$NEWER_SHA" --dry-run --yes ) \
        > "$WORK/out.log" 2>&1; echo $? )
    if [ "$rc" -eq 0 ] && grep -q "NEWER Elasticsearch version" "$WORK/out.log"; then
        ok "version increase warns but proceeds"
    else
        bad "version increase not handled as a warning (rc=$rc)"
    fi
else
    bad "could not create scratch worktree for the version tests"
fi

echo ""
echo "=================================================="
echo "  passed: $PASS   failed: $FAIL"
echo "=================================================="
[ "$FAIL" -eq 0 ]
