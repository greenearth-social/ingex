#!/usr/bin/env python3
"""Reindex Elasticsearch indices to pick up updated index mappings.

ILM templates only apply to newly created indices. Existing indices retain
their old mappings and must be reindexed to pick up changes. Each source
index is copied into ``<name>-<commit>`` where ``<commit>`` is the short HEAD
commit hash, all aliases are moved atomically, and the source index is deleted.

The index currently pointed to by the write alias (``posts``, ``replies``, or
``likes``) is still receiving live writes and is skipped by default. After the
period rolls over, use ``--indices`` to target the formerly-active index by
name. After migration the ingest service continues writing without changes
because the alias ``is_write_index`` flag is preserved during the swap.

State is written to tools/state/reindex-state.json after every transition so
the script can be cancelled and resumed. If a run is interrupted, simply
re-run with the same flags and the same commit in the working tree; the script
will skip completed indices and pick up where it left off.

Run from the tools/ directory:
    pipenv run python reindex.py --migrate --types posts replies --dry-run
    pipenv run python reindex.py --migrate --types posts replies
    pipenv run python reindex.py --migrate --force-merge --types posts replies
    pipenv run python reindex.py --force-merge --types posts replies
    pipenv run python reindex.py --migrate --indices posts-2026-w26 replies-2026-w26

Prerequisites:
  - Updated ILM templates have been deployed (bootstrap job or manual PUT).
  - The new ingest service version has been deployed.

Reads Elasticsearch connection from env vars:
    GE_ELASTICSEARCH_URL              Elasticsearch endpoint URL
    GE_ELASTICSEARCH_USERNAME         Username (default: elastic)
    GE_ELASTICSEARCH_PASSWORD         Password for the above user
    GE_ELASTICSEARCH_TLS_SKIP_VERIFY  Set to "true" to skip TLS verification
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import subprocess
import sys
from typing import Any

from elasticsearch import AsyncElasticsearch, AuthorizationException, NotFoundError
from rich.console import Console

from reindex_state import (  # noqa: F401 — re-exported for tests
    DONE,
    FAILED,
    IndexState,
    PENDING,
    REINDEXING,
    RunState,
    SKIPPED,
    STATE_FILE,
    SWAP_PENDING,
    _now,
)

console = Console()

# ---------------------------------------------------------------------------
# Index type registry — add new types here as new data classes are introduced.
# ---------------------------------------------------------------------------

INDEX_TYPES: dict[str, dict[str, str]] = {
    "posts": {
        "pattern": "posts-*",
        "active_alias": "posts",    # write alias used by megastream/jetstream ingest
    },
    "replies": {
        "pattern": "replies-*",
        "active_alias": "replies",  # write alias used by megastream ingest
    },
    "likes": {
        "pattern": "likes-*",
        "active_alias": "likes",    # write alias used by jetstream ingest
    },
}

POLL_INTERVAL_SECS = 10
_MIGRATED_RE = re.compile(r"-[0-9a-f]{7}$")


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _info(msg: str) -> None:
    console.print(f"[green][INFO][/green]  {msg}")


def _warn(msg: str) -> None:
    console.print(f"[yellow][WARN][/yellow]  {msg}")


def _die(msg: str) -> None:
    console.print(f"[red][ERROR][/red] {msg}")
    sys.exit(1)


def _confirm_include_active(active_index: str, type_name: str) -> bool:
    """Warn about data loss and prompt for confirmation before reindexing the active write index."""
    console.print()
    console.print(
        f"[bold red][WARNING][/bold red]  --include-active will reindex [bold]{active_index}[/bold] "
        f"(active write index for '{type_name}')."
    )
    console.print()
    console.print(
        "  [yellow]Data-loss risk:[/yellow] While reindexing, the write alias continues pointing to the\n"
        "  source index. Documents written after reindexing starts will NOT be present in\n"
        "  the destination and will be [bold]permanently lost[/bold] when the alias is swapped."
    )
    console.print()
    console.print("  [bold]Only proceed when all ingest services have been stopped.[/bold]")
    console.print()
    try:
        answer = input("  Proceed with reindexing the active index? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        console.print()
        return False
    return answer == "y"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _git_short_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "unknown"


async def _active_index_for(es: AsyncElasticsearch, alias: str) -> str | None:
    """Return the index with is_write_index:true for alias, or None if alias doesn't exist."""
    try:
        resp = await es.indices.get_alias(name=alias)
        # Prefer the explicit write index; fall back to the sole member if unambiguous.
        for index_name, info in resp.items():
            if info.get("aliases", {}).get(alias, {}).get("is_write_index"):
                return index_name
        indices = list(resp.keys())
        return indices[0] if len(indices) == 1 else None
    except NotFoundError:
        return None
    except AuthorizationException:
        _die(
            f"User lacks 'view_index_metadata' privilege needed to read alias '{alias}'. "
            f"Use --include-active to skip the active-index check "
            f"(only safe after the write period rolls over)."
        )


async def _list_indices(es: AsyncElasticsearch, pattern: str) -> list[str]:
    try:
        resp = await es.cat.indices(
            index=pattern,
            h="index,creation.date",
            s="creation.date:desc",
            format="json",
        )
        return [entry["index"] for entry in (resp or [])]
    except NotFoundError:
        return []


async def _get_aliases(es: AsyncElasticsearch, index: str) -> dict[str, Any]:
    try:
        resp = await es.indices.get_alias(index=index)
        return resp.get(index, {}).get("aliases", {})
    except NotFoundError:
        return {}


async def _index_exists(es: AsyncElasticsearch, index: str) -> bool:
    try:
        await es.indices.get(index=index)
        return True
    except NotFoundError:
        return False


async def _doc_count(es: AsyncElasticsearch, index: str) -> int | None:
    """Return the document count for index, or None if it does not exist."""
    try:
        resp = await es.count(index=index)
        return int(resp["count"])
    except NotFoundError:
        return None


async def _task_running(es: AsyncElasticsearch, task_id: str) -> bool:
    """Return True if the task exists and has not yet completed."""
    try:
        task = await es.tasks.get(task_id=task_id)
        return task.get("completed") is not True
    except NotFoundError:
        return False


async def _find_running_reindex_to(es: AsyncElasticsearch, dst: str) -> str | None:
    """Return the id of a running reindex task writing into dst, if any.

    Adopting an orphaned reindex (e.g. one whose driving script was Ctrl-C'd)
    lets us re-attach to it instead of starting a competing reindex into the
    same destination — which would collide on op_type=create version conflicts.
    """
    try:
        resp = await es.tasks.list(
            actions="indices:data/write/reindex", detailed=True,
        )
    except Exception:
        return None

    suffix = f"to [{dst}]"
    for node in (resp.get("nodes") or {}).values():
        for task_id, task in (node.get("tasks") or {}).items():
            desc = task.get("description") or ""
            # Only the parent reindex task carries the "from [..] to [..]"
            # description; sliced child tasks are just "reindex" and are skipped.
            if desc.startswith("reindex from") and desc.endswith(suffix):
                return task_id
    return None


# ---------------------------------------------------------------------------
# Reindex state machine (per-index)
# ---------------------------------------------------------------------------

async def _start_reindex(
    es: AsyncElasticsearch,
    state: RunState,
    src: str,
) -> str:
    """Kick off an async sliced reindex. Returns REINDEXING or FAILED."""
    dst = state.indices[src].dst
    _info(f"  Starting async reindex (slices=auto, 2000 req/s) ...")

    try:
        resp = await es.reindex(
            source={"index": src},
            dest={"index": dst, "op_type": "create"},
            wait_for_completion=False,
            slices="auto",
            requests_per_second=2000,
        )
    except Exception as exc:
        _warn(f"  Failed to start reindex: {exc}")
        return FAILED

    task_id: str = resp["task"]
    _info(f"  Task: [cyan]{task_id}[/cyan]")
    state.update(src, status=REINDEXING, task_id=task_id, started_at=_now())
    return REINDEXING


async def _poll_task(
    es: AsyncElasticsearch,
    state: RunState,
    src: str,
) -> str:
    """Poll a running reindex task until it finishes.

    Returns SWAP_PENDING on success, FAILED on failure.
    """
    task_id = state.indices[src].task_id
    dst = state.indices[src].dst
    _info(f"  Polling task [cyan]{task_id}[/cyan] ...")

    # Source is no longer receiving writes (active indices are skipped), so its
    # count is a stable denominator for progress.
    src_count = await _doc_count(es, src)

    while True:
        await asyncio.sleep(POLL_INTERVAL_SECS)

        try:
            task = await es.tasks.get(task_id=task_id)
        except NotFoundError:
            # Task evicted from the task store. Only treat as done if the
            # destination is actually complete — otherwise the reindex was
            # interrupted and must be restarted.
            _info(f"  Task {task_id} evicted from task store — checking destination count ...")
            dst_count = await _doc_count(es, dst)
            if dst_count is not None and src_count is not None and dst_count >= src_count:
                _info(f"  Destination {dst} complete ({dst_count:,}/{src_count:,}) — proceeding to alias swap.")
                return SWAP_PENDING
            _warn(f"  Destination {dst} incomplete after eviction ({dst_count}/{src_count}) — will restart reindex.")
            return FAILED

        completed: bool = task.get("completed") is True

        # Report progress from document counts. The sliced-reindex parent task
        # reports total=0 with null slices until every slice finishes its first
        # scroll, so the task status is unreliable for progress.
        dst_count = await _doc_count(es, dst)
        if src_count and dst_count is not None:
            pct = dst_count * 100 / src_count
            _info(f"  {src}: {dst_count:,} / {src_count:,} docs ({pct:.1f}%)")
        else:
            _info(f"  {src}: reindexing (dst={dst_count}, src={src_count}) ...")

        if not completed:
            continue

        result = task.get("response") or {}
        final_created   = result.get("created", 0)
        final_conflicts = result.get("version_conflicts", 0)
        final_failures  = result.get("failures") or []
        took_ms         = result.get("took", 0)

        _info(
            f"  Done: {final_created:,} created, {final_conflicts:,} version_conflicts, "
            f"{len(final_failures)} failures ({took_ms}ms)"
        )

        if final_failures:
            console.print("[red][ERROR][/red] Reindex failures:")
            for failure in final_failures:
                console.print(f"  {failure}")
            return FAILED

        return SWAP_PENDING


async def _ensure_reindex(
    es: AsyncElasticsearch,
    state: RunState,
    src: str,
) -> str:
    """Decide how to (re)start work for src and return the next status.

    Returns REINDEXING (a task is running — poll it), SWAP_PENDING (destination
    already complete), DONE (destination already live behind an alias), or
    FAILED (could not start a fresh reindex).
    """
    idx = state.indices[src]
    dst = idx.dst

    # 1. A reindex we already know about is still running — re-attach to it.
    if idx.task_id and await _task_running(es, idx.task_id):
        _info(f"  Reattaching to running task [cyan]{idx.task_id}[/cyan]")
        return REINDEXING

    # 2. An untracked reindex is already writing to dst (e.g. orphaned by an
    #    earlier Ctrl-C) — adopt it rather than competing with it.
    orphan = await _find_running_reindex_to(es, dst)
    if orphan:
        _info(f"  Adopting running reindex into {dst}: [cyan]{orphan}[/cyan]")
        state.update(src, task_id=orphan, status=REINDEXING)
        return REINDEXING

    # 3. No reindex running — inspect the destination.
    if await _index_exists(es, dst):
        dst_aliases = await _get_aliases(es, dst)
        if dst_aliases:
            # Destination already serves an alias — never delete a live index.
            _info(f"  Destination {dst} already live (aliases: {', '.join(dst_aliases)}).")
            return DONE

        src_count = await _doc_count(es, src)
        dst_count = await _doc_count(es, dst)
        if dst_count is not None and src_count is not None and dst_count >= src_count:
            _info(f"  Destination {dst} already complete ({dst_count:,}/{src_count:,}) — skipping reindex.")
            return SWAP_PENDING

        # Alias-less, incomplete leftover — safe to delete and restart fresh.
        _warn(f"  Removing partial destination {dst} ({dst_count}/{src_count}) and restarting.")
        try:
            await es.indices.delete(index=dst)
        except NotFoundError:
            pass

    # 4. Start a fresh reindex.
    return await _start_reindex(es, state, src)


async def _do_swap(
    es: AsyncElasticsearch,
    state: RunState,
    src: str,
) -> str:
    """Atomically move aliases from src to dst, then delete src. Returns DONE or FAILED."""
    dst = state.indices[src].dst

    aliases = await _get_aliases(es, src)
    actions: list[dict[str, Any]] = []
    for alias_name, alias_cfg in aliases.items():
        add: dict[str, Any] = {"index": dst, "alias": alias_name}
        if alias_cfg.get("is_write_index"):
            add["is_write_index"] = True
        actions.append({"remove": {"index": src, "alias": alias_name}})
        actions.append({"add": add})

    if actions:
        _info(f"  Moving aliases: {', '.join(aliases)}")
        try:
            await es.indices.update_aliases(actions=actions)
        except Exception as exc:
            _warn(f"  Alias swap failed: {exc}")
            return FAILED

    try:
        await es.indices.delete(index=src)
    except NotFoundError:
        pass  # Already deleted — idempotent.
    except Exception as exc:
        _warn(f"  Delete of {src} failed: {exc}")
        return FAILED

    return DONE


async def _process_index(
    es: AsyncElasticsearch,
    state: RunState | None,
    src: str,
    commit: str,
    dry_run: bool,
) -> None:
    """Drive one index through its full migration state machine (reindex → swap)."""
    if dry_run:
        _info(f"[dim][dry-run][/dim] Would reindex {src} → {src}-{commit}, swap aliases, delete src.")
        return

    assert state is not None
    idx = state.indices[src]

    if idx.status in (DONE, SKIPPED):
        _info(f"Skipping [{idx.status}]: {src}")
        return

    _info(f"Migrating [bold]{src}[/bold] → [bold]{idx.dst}[/bold]"
          + (f" [dim](resuming from {idx.status})[/dim]" if idx.status != PENDING else ""))

    # ── Step 1: ensure a reindex is running, complete, or already live ──────
    if idx.status in (PENDING, FAILED, REINDEXING):
        new_status = await _ensure_reindex(es, state, src)
        if new_status == FAILED:
            state.update(src, status=FAILED, error="Failed to start reindex")
            return
        # Clear any stale error from a previous failed attempt.
        state.update(src, status=new_status, error=None)
        if new_status == DONE:
            state.update(src, completed_at=_now())
            _info(f"  [green]✓[/green] {src} → {idx.dst} (already migrated)")
            return

    # ── Step 2: poll until reindex finishes ─────────────────────────────────
    if state.indices[src].status == REINDEXING:
        new_status = await _poll_task(es, state, src)
        state.update(src, status=new_status)
        if new_status == FAILED:
            state.update(src, error="Reindex task failed or destination missing after eviction")
            return

    # ── Step 3: atomic alias swap + delete ──────────────────────────────────
    if state.indices[src].status == SWAP_PENDING:
        new_status = await _do_swap(es, state, src)
        state.update(src, status=new_status, completed_at=_now() if new_status == DONE else None)
        if new_status == FAILED:
            state.update(src, error="Alias swap or source deletion failed")
            return

    _info(f"  [green]✓[/green] {src} → {idx.dst}")


# ---------------------------------------------------------------------------
# Force-merge operations
# ---------------------------------------------------------------------------

async def _start_force_merge_task(es: AsyncElasticsearch, index: str) -> str | None:
    """Submit an async force-merge to 1 segment. Returns the task_id or None on failure."""
    _info(f"  Submitting force-merge for [bold]{index}[/bold] ...")
    try:
        resp = await es.indices.forcemerge(index=index, max_num_segments=1, wait_for_completion=False)
        task_id: str = resp["task"]
        _info(f"  Task: [cyan]{task_id}[/cyan]")
        return task_id
    except Exception as exc:
        _warn(f"  Failed to submit force-merge for {index}: {exc}")
        return None


async def _poll_all_force_merges(
    es: AsyncElasticsearch,
    tasks: dict[str, str],
) -> None:
    """Poll a set of async force-merge tasks concurrently until all complete. Non-fatal on errors."""
    pending = dict(tasks)  # index → task_id
    while pending:
        await asyncio.sleep(POLL_INTERVAL_SECS)
        done: list[str] = []
        for idx, task_id in pending.items():
            try:
                task = await es.tasks.get(task_id=task_id)
            except NotFoundError:
                _warn(f"  {idx}: task evicted — assuming complete.")
                done.append(idx)
                continue

            completed: bool = task.get("completed") is True
            running_nanos = task.get("task", {}).get("running_time_in_nanos", 0)
            _info(f"  {idx}: running {running_nanos / 1e9:.0f}s ...")

            if completed:
                failures = (task.get("response") or {}).get("_shards", {}).get("failures") or []
                if failures:
                    _warn(f"  {idx}: complete with {len(failures)} shard failure(s) (non-fatal):")
                    for f in failures:
                        console.print(f"    {f}")
                else:
                    _info(f"  [green]✓[/green] {idx}: force-merge complete.")
                done.append(idx)

        for idx in done:
            del pending[idx]


async def _discover_for_force_merge(
    es: AsyncElasticsearch,
    sorted_types: list[str],
) -> list[str]:
    """Discover all non-active indices for the given types (newest first per type).

    Force-merging the active write index is wasteful — new segments are created
    continuously — so it is always skipped here. Use --indices to target a
    specific index by name if needed.
    """
    result: list[str] = []
    for type_name in sorted_types:
        cfg = INDEX_TYPES[type_name]
        active = await _active_index_for(es, cfg["active_alias"])
        indices = await _list_indices(es, cfg["pattern"])
        for idx in indices:
            if idx == active:
                _warn(f"Skipping active write index {idx} (force-merging it is wasteful).")
                continue
            result.append(idx)
    return result


# ---------------------------------------------------------------------------
# Migration orchestration helpers
# ---------------------------------------------------------------------------

def _load_or_create_state(
    args: argparse.Namespace,
    commit: str,
    sorted_types: list[str],
    sorted_explicit: list[str],
) -> RunState | None:
    """Load existing or create new migration state. Returns None in dry-run mode.

    Raises ValueError when a state file exists for a different run (mismatched
    commit or target set). The caller should treat this as a fatal error and
    tell the user to pass --reset.
    """
    if args.dry_run:
        return None

    if args.reset:
        _warn("--reset: discarding previous state.")
        STATE_FILE.unlink(missing_ok=True)
    else:
        existing = RunState.load()
        if existing is not None:
            if sorted_explicit:
                matches = (existing.commit == commit
                           and sorted(existing.explicit_indices) == sorted_explicit)
                desc = f"commit={existing.commit}, indices={existing.explicit_indices}"
            else:
                matches = (existing.commit == commit
                           and sorted(existing.types) == sorted_types)
                desc = f"commit={existing.commit}, types={existing.types}"

            if matches:
                done = sum(1 for s in existing.indices.values() if s.status == DONE)
                _info(
                    f"Resuming previous run from {existing.created_at} "
                    f"({done}/{len(existing.indices)} indices already done)."
                )
                return existing
            else:
                raise ValueError(
                    f"State file is for a different run ({desc}). "
                    f"Use --reset to start fresh."
                )

    state = RunState.create(commit, sorted_types, explicit_indices=sorted_explicit or None)
    state.save()
    _info(f"State file: {STATE_FILE}")
    return state


def _classify_for_migration(
    idx: str,
    active_index: str | None,
    state: RunState | None,
    commit: str,
) -> bool:
    """Classify idx and register it in state on first encounter.

    Returns True if the index should be skipped (active write index or
    already migrated). Also records the skip reason in state so resume
    runs don't re-evaluate the decision.
    """
    # Already registered — let _process_index handle the status (DONE/SKIPPED/etc.)
    if state is not None and idx in state.indices:
        return False

    if idx == active_index:
        if state is not None:
            state.indices[idx] = IndexState(
                status=SKIPPED, src=idx, dst=f"{idx}-{commit}", error="active write index",
            )
            state.save()
        _warn(f"Skipping active write index: {idx}")
        return True

    if _MIGRATED_RE.search(idx):
        if state is not None:
            state.indices[idx] = IndexState(
                status=SKIPPED, src=idx, dst=idx, error="already migrated",
            )
            state.save()
        _info(f"Skipping already-migrated index: {idx}")
        return True

    if state is not None:
        state.indices[idx] = IndexState(status=PENDING, src=idx, dst=f"{idx}-{commit}")
        state.save()

    return False


async def _resolve_active_write_index(
    es: AsyncElasticsearch,
    cfg: dict[str, str],
    args: argparse.Namespace,
    type_name: str,
) -> str | None:
    """Return the active write index that should be excluded from migration, or None.

    With --include-active: prompts for confirmation. If the operator confirms,
    returns None so the index is included. If declined, returns the index name
    so it is excluded.

    Without --include-active: always returns the active index name (exclude it)
    and prints a hint about using --indices after rollover.
    """
    active = await _active_index_for(es, cfg["active_alias"])
    if active is None:
        return None

    if args.include_active:
        if _confirm_include_active(active, type_name):
            return None  # confirmed: include it in migration
        _warn(f"Skipping {type_name} — confirmation declined.")
        console.print()
    else:
        _warn(f"Skipping active {type_name} index: {active}")
        console.print("  (Use --indices to target it by name after the period rolls over.)")
        console.print()

    return active


async def _migrate_and_maybe_fm(
    es: AsyncElasticsearch,
    state: RunState | None,
    idx: str,
    commit: str,
    dry_run: bool,
    fire_fm: bool,
    fm_tasks: dict[str, str],
) -> bool:
    """Migrate one index. Returns True on error.

    If fire_fm is True and migration succeeds, submits a fire-and-forget
    force-merge (combined --migrate --force-merge mode).
    """
    try:
        await _process_index(es, state, idx, commit, dry_run=dry_run)
        if state is not None and state.indices.get(idx, IndexState(PENDING, "", "")).status == FAILED:
            return True
        if fire_fm and not dry_run and state is not None and state.indices[idx].status == DONE:
            task_id = await _start_force_merge_task(es, state.indices[idx].dst)
            if task_id:
                fm_tasks[state.indices[idx].dst] = task_id
        return False
    except Exception as exc:
        console.print(f"[red][ERROR][/red] {idx}: {exc}")
        if state is not None:
            state.update(idx, status=FAILED, error=str(exc))
        return True


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

async def _run_migrate(
    es: AsyncElasticsearch,
    args: argparse.Namespace,
    sorted_types: list[str],
    sorted_explicit: list[str],
) -> int:
    """Reindex all target indices and atomically swap their aliases."""
    commit = args.commit or _git_short_hash()
    _info(f"Destination suffix: [cyan]{commit}[/cyan]")
    console.print()

    try:
        state = _load_or_create_state(args, commit, sorted_types, sorted_explicit)
    except ValueError as exc:
        _warn(str(exc))
        return 1

    if not args.dry_run:
        console.print()

    errors = 0
    fm_tasks: dict[str, str] = {}  # dst index → task_id (combined mode fire-and-forget)
    fire_fm = args.force_merge

    if sorted_explicit:
        # ── Explicit-index mode: active-index guard is bypassed ──────────────
        _info("=== Explicit indices (active-index guard bypassed) ===")
        for idx in sorted_explicit:
            if _classify_for_migration(idx, None, state, commit):
                continue
            if await _migrate_and_maybe_fm(es, state, idx, commit, args.dry_run, fire_fm, fm_tasks):
                errors += 1
        console.print()

    else:
        # ── Type-discovery mode: discover and skip the active write index ────
        for type_name in sorted_types:
            cfg = INDEX_TYPES[type_name]
            active_index = await _resolve_active_write_index(es, cfg, args, type_name)

            _info(f"=== {cfg['pattern']} (newest first) ===")
            indices = await _list_indices(es, cfg["pattern"])
            if not indices:
                _info(f"No indices found matching {cfg['pattern']}")
                console.print()
                continue

            for idx in indices:
                if _classify_for_migration(idx, active_index, state, commit):
                    continue
                if await _migrate_and_maybe_fm(es, state, idx, commit, args.dry_run, fire_fm, fm_tasks):
                    errors += 1

            console.print()

    if errors:
        console.print(
            f"[red][ERROR][/red] Migration finished with {errors} error(s). "
            f"Review output above, then re-run to retry failed indices."
        )
        return 1

    if state is not None:
        STATE_FILE.unlink(missing_ok=True)
        _info("State file removed (migration complete).")

    await _print_alias_summary(es)
    console.print()
    _info("Migration complete.")
    console.print()
    console.print(
        "[yellow][WARN][/yellow]  ILM note: migrated indices have a new creation date, so their "
        "ILM deletion timer resets to zero. If storage capacity is a concern, manually delete "
        "migrated indices for periods that have already expired under your retention policy:\n"
        "  DELETE /<index-name>  (e.g. via Kibana Dev Tools or curl)"
    )

    if fm_tasks:
        console.print()
        _warn(
            f"{len(fm_tasks)} force-merge task(s) are running in the background "
            f"(submitted after each alias swap, not waited on):"
        )
        for dst, tid in fm_tasks.items():
            console.print(f"  [bold]{dst}[/bold]: [cyan]{tid}[/cyan]")
        console.print(
            "  Monitor progress: GET /_tasks/<task_id>\n"
            "  Or run --force-merge --indices <name> to poll with progress."
        )

    return 0


async def _run_force_merge(
    es: AsyncElasticsearch,
    args: argparse.Namespace,
    sorted_types: list[str],
    sorted_explicit: list[str],
) -> int:
    """Submit async force-merges for all targets and poll until complete."""
    _info("=== Force-merge ===")
    console.print()

    targets = sorted_explicit or await _discover_for_force_merge(es, sorted_types)

    if not targets:
        _info("No indices to force-merge.")
        return 0

    if args.dry_run:
        for idx in targets:
            _info(f"[dim][dry-run][/dim] Would force-merge {idx} to 1 segment.")
        return 0

    fm_tasks: dict[str, str] = {}
    for idx in targets:
        task_id = await _start_force_merge_task(es, idx)
        if task_id:
            fm_tasks[idx] = task_id
    console.print()

    if fm_tasks:
        _info(f"Polling {len(fm_tasks)} force-merge task(s) ...")
        console.print()
        await _poll_all_force_merges(es, fm_tasks)
        console.print()

    _info("Force-merge complete.")
    return 0


async def _print_alias_summary(es: AsyncElasticsearch) -> None:
    """Print which index each key alias currently points to."""
    aliases_to_show = ["posts", "replies", "likes", "posts_recent"]
    console.print()
    console.print("[bold]Alias summary:[/bold]")
    for alias in aliases_to_show:
        try:
            resp = await es.indices.get_alias(name=alias)
            for index_name, info in resp.items():
                is_write = info.get("aliases", {}).get(alias, {}).get("is_write_index", False)
                write_tag = " [dim](write)[/dim]" if is_write else ""
                console.print(f"  [cyan]{alias}[/cyan] → {index_name}{write_tag}")
        except NotFoundError:
            console.print(f"  [cyan]{alias}[/cyan] → [dim](no index)[/dim]")
        except AuthorizationException:
            console.print(f"  [cyan]{alias}[/cyan] → [dim](insufficient privileges)[/dim]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def _run(args: argparse.Namespace) -> int:
    if not args.migrate and not args.force_merge:
        _die("Specify at least one operation: --migrate and/or --force-merge.")

    url      = os.environ.get("GE_ELASTICSEARCH_URL", "https://localhost:9200")
    username = os.environ.get("GE_ELASTICSEARCH_USERNAME", "elastic")
    password = os.environ.get("GE_ELASTICSEARCH_PASSWORD")
    skip_tls = os.environ.get("GE_ELASTICSEARCH_TLS_SKIP_VERIFY", "false").lower() in (
        "1", "true", "yes",
    )

    if not password:
        _die("GE_ELASTICSEARCH_PASSWORD is not set")

    es = AsyncElasticsearch(
        hosts=[url],
        basic_auth=(username, password),
        verify_certs=not skip_tls,
    )

    try:
        info = await es.info()
        _info(f"Connected to Elasticsearch {info['version']['number']} at {url}")
        console.print()

        sorted_types    = sorted(args.types or [])
        sorted_explicit = sorted(args.indices or [])

        if args.dry_run:
            _warn("Dry-run mode — no changes will be made and state will not be saved.")
        console.print()

        if args.migrate:
            rc = await _run_migrate(es, args, sorted_types, sorted_explicit)
            if rc != 0:
                return rc

        if args.force_merge and not args.migrate:
            return await _run_force_merge(es, args, sorted_types, sorted_explicit)

        return 0

    finally:
        await es.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Reindex and/or force-merge Elasticsearch indices.\n"
            "At least one of --migrate / --force-merge must be specified."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Migration only — reindex historical indices and swap aliases
  pipenv run python reindex.py --migrate --types posts replies --dry-run
  pipenv run python reindex.py --migrate --types posts replies
  pipenv run python reindex.py --migrate --indices posts-2026-w26 replies-2026-w26

  # Force-merge only — submit async, poll with progress (run off-peak)
  pipenv run python reindex.py --force-merge --types posts replies
  pipenv run python reindex.py --force-merge --indices posts-2026-w25-abc1234

  # Combined — migrate then fire-and-forget force-merge (runs concurrently with next index)
  pipenv run python reindex.py --migrate --force-merge --types posts replies

  # Reindex the active write index (requires confirmation; stop ingest services first)
  pipenv run python reindex.py --migrate --types posts replies --include-active

  # Discard resume state and start migration fresh
  pipenv run python reindex.py --migrate --types posts --reset
""",
    )

    # ── Operations (at least one required) ───────────────────────────────────
    parser.add_argument(
        "--migrate",
        action="store_true",
        help=(
            "Reindex each source index into a new destination with the updated shard count, "
            "then atomically swap all aliases and delete the source."
        ),
    )
    parser.add_argument(
        "--force-merge",
        action="store_true",
        help=(
            "Force-merge indices to 1 segment to reduce per-shard term-seek overhead. "
            "When used alone: submits all force-merges async then polls with progress. "
            "When combined with --migrate: fires force-merge after each alias swap "
            "without waiting (runs concurrently with the next index migration). "
            "Heavy I/O — run off-peak."
        ),
    )

    # ── Index targeting (mutually exclusive, required) ────────────────────────
    index_group = parser.add_mutually_exclusive_group(required=True)
    index_group.add_argument(
        "--types",
        nargs="+",
        choices=list(INDEX_TYPES),
        metavar="TYPE",
        help=(
            f"Discover all indices of the given type(s). "
            f"Choices: {', '.join(INDEX_TYPES)}. "
            f"Mutually exclusive with --indices."
        ),
    )
    index_group.add_argument(
        "--indices",
        nargs="+",
        metavar="INDEX",
        help=(
            "Target specific named indices (e.g. posts-2026-w26) instead of discovering "
            "by type. For --migrate: bypasses the active-index guard so you can reindex "
            "a formerly-active index after rollover without --include-active. "
            "Mutually exclusive with --types."
        ),
    )

    # ── Migration options ─────────────────────────────────────────────────────
    parser.add_argument(
        "--include-active",
        action="store_true",
        help=(
            "(--migrate only) Also migrate the index currently receiving live writes. "
            "Prompts for confirmation — only safe when all ingest services are stopped. "
            "Prefer --indices after the write period rolls over."
        ),
    )
    parser.add_argument(
        "--commit",
        metavar="HASH",
        help="(--migrate only) Override the git commit hash used as the destination index suffix.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="(--migrate only) Discard any saved resume state and start fresh.",
    )

    # ── General ───────────────────────────────────────────────────────────────
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without making any changes. State is not saved.",
    )

    args = parser.parse_args()
    try:
        exit_code = asyncio.run(_run(args))
    except KeyboardInterrupt:
        console.print()
        _warn(
            "Interrupted — any in-flight Elasticsearch tasks keep running server-side. "
            "Re-run with the same flags to resume migration; the script will re-attach "
            "to running reindex tasks. Use GET /_tasks to monitor force-merge tasks."
        )
        sys.exit(130)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
