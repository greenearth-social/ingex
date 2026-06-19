#!/usr/bin/env python3
"""Reindex Elasticsearch indices to pick up updated index mappings.

ILM templates only apply to newly created indices. Existing indices retain
their old mappings and must be reindexed to pick up changes. Each source
index is copied into ``<name>-<commit>`` where ``<commit>`` is the short HEAD
commit hash, all aliases are moved atomically, and the source index is deleted.

The index currently pointed to by the ``<type>_recent`` alias is still
receiving live writes and is skipped by default. Re-run with --include-active
after the period rolls over and the new period's index is live.

Run from the tools/ directory:
    pipenv run python reindex.py --dry-run
    pipenv run python reindex.py --types posts replies
    pipenv run python reindex.py --include-active

Prerequisites:
  - Updated ILM templates have been deployed (bootstrap job or manual PUT).
  - The new ingest service version has been deployed.

Reads Elasticsearch connection from env vars (same as the ingest service):
    GE_ELASTICSEARCH_URL              Elasticsearch endpoint URL
    GE_ELASTICSEARCH_API_KEY          API key for authentication
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

from elasticsearch import AsyncElasticsearch, NotFoundError
from rich.console import Console

console = Console()

# ---------------------------------------------------------------------------
# Index type registry
# ---------------------------------------------------------------------------

# Add new index types here as new data types are introduced.
INDEX_TYPES: dict[str, dict[str, str]] = {
    "posts": {
        "pattern": "posts-*",
        "active_alias": "posts_recent",
    },
    "replies": {
        "pattern": "replies-*",
        "active_alias": "replies_recent",
    },
    "likes": {
        "pattern": "likes-*",
        "active_alias": "likes_recent",
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


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _git_short_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "unknown"


async def _active_index_for(es: AsyncElasticsearch, alias: str) -> str | None:
    """Return the index backing an alias, or None if the alias does not exist."""
    try:
        resp = await es.indices.get_alias(name=alias)
        indices = list(resp.keys())
        return indices[0] if indices else None
    except NotFoundError:
        return None


async def _list_indices(es: AsyncElasticsearch, pattern: str) -> list[str]:
    """List indices matching a wildcard pattern, newest-first by creation date."""
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
    """Return the alias config dict for an index (empty dict if none)."""
    try:
        resp = await es.indices.get_alias(index=index)
        return resp.get(index, {}).get("aliases", {})
    except NotFoundError:
        return {}


# ---------------------------------------------------------------------------
# Core: async reindex with task polling
# ---------------------------------------------------------------------------


async def _reindex_and_wait(es: AsyncElasticsearch, src: str, dst: str) -> None:
    """Kick off an async sliced reindex and poll until ES reports it complete."""
    _info("  Starting async reindex (slices=auto, 2000 req/s) ...")
    resp = await es.reindex(
        source={"index": src},
        dest={"index": dst, "op_type": "create"},
        wait_for_completion=False,
        slices="auto",
        requests_per_second=2000,
    )
    task_id: str = resp["task"]
    _info(f"  Task: [cyan]{task_id}[/cyan]")

    while True:
        await asyncio.sleep(POLL_INTERVAL_SECS)

        try:
            task = await es.tasks.get(task_id=task_id)
        except NotFoundError:
            # Task completed and was evicted from the task store.
            _info(f"  Task {task_id} no longer in task list — treating as complete.")
            return

        completed: bool = task.get("completed") is True

        status = (task.get("task") or {}).get("status") or {}
        total     = int(status.get("total", 0))
        created   = int(status.get("created", 0))
        conflicts = int(status.get("version_conflicts", 0))
        processed = created + conflicts

        if total > 0:
            pct = processed * 100 / total
            _info(f"  {src}: {processed:,} / {total:,} docs ({pct:.1f}%)")
        else:
            _info(f"  {src}: task running, waiting for doc count...")

        if completed:
            break

    result = task.get("response") or {}
    final_created   = result.get("created", created)
    final_conflicts = result.get("version_conflicts", conflicts)
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
        raise RuntimeError(f"Reindex of {src} had {len(final_failures)} failure(s)")


# ---------------------------------------------------------------------------
# Core: single-index migration
# ---------------------------------------------------------------------------


async def _migrate_index(
    es: AsyncElasticsearch,
    src: str,
    commit: str,
    *,
    dry_run: bool,
) -> None:
    dst = f"{src}-{commit}"

    if dry_run:
        _info(f"[dim][dry-run][/dim] {src} → {dst}  (reindex, move aliases, delete src)")
        return

    # Restart-safe: if the destination already exists, skip rather than clobber.
    try:
        await es.indices.get(index=dst)
        _warn(f"{dst} already exists — skipping (delete it manually to re-run this index)")
        return
    except NotFoundError:
        pass

    _info(f"Migrating [bold]{src}[/bold] → [bold]{dst}[/bold]")
    await _reindex_and_wait(es, src, dst)

    # Atomically move all aliases from src → dst.
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
        await es.indices.update_aliases(actions=actions)

    await es.indices.delete(index=src)
    _info(f"  [green]✓[/green] {src} migrated to {dst}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def _run(args: argparse.Namespace) -> int:
    url = os.environ.get("GE_ELASTICSEARCH_URL", "https://localhost:9200")
    api_key = os.environ.get("GE_ELASTICSEARCH_API_KEY")
    skip_tls = os.environ.get("GE_ELASTICSEARCH_TLS_SKIP_VERIFY", "false").lower() in (
        "1", "true", "yes",
    )

    if not api_key:
        _die("GE_ELASTICSEARCH_API_KEY is not set")

    es = AsyncElasticsearch(
        hosts=[url],
        api_key=api_key,
        verify_certs=not skip_tls,
    )

    try:
        info = await es.info()
        _info(f"Connected to Elasticsearch {info['version']['number']} at {url}")
        console.print()

        commit = args.commit or _git_short_hash()
        _info(f"Destination suffix: [cyan]{commit}[/cyan]")

        if args.dry_run:
            _warn("Dry-run mode — no changes will be made.")
        console.print()

        selected_types = args.types or list(INDEX_TYPES)
        errors = 0

        for type_name in selected_types:
            cfg = INDEX_TYPES[type_name]
            pattern = cfg["pattern"]
            active_alias = cfg["active_alias"]

            active_index: str | None = None
            if not args.include_active:
                active_index = await _active_index_for(es, active_alias)
                if active_index:
                    _warn(f"Skipping active {type_name} index: {active_index}")
                    console.print(
                        "  (Re-run with --include-active after the period rolls over.)"
                    )
                    console.print()

            _info(f"=== {pattern} (newest first) ===")
            indices = await _list_indices(es, pattern)

            if not indices:
                _info(f"No indices found matching {pattern}")
                console.print()
                continue

            for idx in indices:
                if idx == active_index:
                    _warn(f"Skipping active index: {idx}")
                    continue
                if _MIGRATED_RE.search(idx):
                    _info(f"Skipping already-migrated index: {idx}")
                    continue
                try:
                    await _migrate_index(es, idx, commit, dry_run=args.dry_run)
                except Exception as exc:
                    console.print(f"[red][ERROR][/red] {idx}: {exc}")
                    errors += 1

            console.print()

        if errors:
            console.print(f"[red][ERROR][/red] Migration finished with {errors} error(s). Review output above.")
            return 1

        _info("Migration complete.")
        return 0

    finally:
        await es.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reindex Elasticsearch indices to pick up updated mappings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  pipenv run python reindex.py --dry-run
  pipenv run python reindex.py --types posts replies
  pipenv run python reindex.py --include-active
""",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=list(INDEX_TYPES),
        metavar="TYPE",
        help=(
            f"Index types to migrate: {', '.join(INDEX_TYPES)}. "
            "Defaults to all types."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without making any changes.",
    )
    parser.add_argument(
        "--include-active",
        action="store_true",
        help=(
            "Also migrate the index currently aliased by <type>_recent. "
            "Only safe once the current write period has rolled over."
        ),
    )
    parser.add_argument(
        "--commit",
        metavar="HASH",
        help="Override the git commit hash used as the destination index suffix.",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
