#!/usr/bin/env python3
"""Reindex Elasticsearch indices to pick up updated index mappings.

ILM templates only apply to newly created indices. Existing indices retain
their old mappings and must be reindexed to pick up changes. Each source
index is copied into ``<name>-<commit>`` where ``<commit>`` is the short HEAD
commit hash, all aliases are moved atomically, and the source index is deleted.

The index currently pointed to by the write alias (``posts``, ``replies``, or
``likes``) is still receiving live writes and is skipped by default. Re-run
with --include-active after the period rolls over and the new period's index
is live. After migration the ingest service continues writing without changes
because the alias ``is_write_index`` flag is preserved during the swap.

State is written to tools/state/reindex-state.json after every transition so
the script can be cancelled and resumed. If a run is interrupted, simply
re-run with the same --types flags and the same commit in the working tree;
the script will skip completed indices and pick up where it left off.

Run from the tools/ directory:
    pipenv run python reindex.py --types posts replies --dry-run
    pipenv run python reindex.py --types posts replies --include-active
    pipenv run python reindex.py --types posts replies likes

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
import dataclasses
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from elasticsearch import AsyncElasticsearch, AuthorizationException, NotFoundError
from rich.console import Console

console = Console()

STATE_DIR = Path(__file__).parent / "state"
STATE_FILE = STATE_DIR / "reindex-state.json"

# Per-index status values.
PENDING      = "pending"
REINDEXING   = "reindexing"
SWAP_PENDING = "swap_pending"
DONE         = "done"
SKIPPED      = "skipped"
FAILED       = "failed"

# ---------------------------------------------------------------------------
# Index type registry — add new types here as new data classes are introduced.
# ---------------------------------------------------------------------------

INDEX_TYPES: dict[str, dict[str, str]] = {
    "posts": {
        "pattern": "posts-*",
        "active_alias": "posts",   # write alias used by megastream/jetstream ingest
    },
    "replies": {
        "pattern": "replies-*",
        "active_alias": "replies", # write alias used by megastream ingest
    },
    "likes": {
        "pattern": "likes-*",
        "active_alias": "likes",   # write alias used by jetstream ingest
    },
}

POLL_INTERVAL_SECS = 10
_MIGRATED_RE = re.compile(r"-[0-9a-f]{7}$")


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class IndexState:
    status: str    # PENDING | REINDEXING | SWAP_PENDING | DONE | SKIPPED | FAILED
    src: str
    dst: str
    task_id: str | None = None
    error: str | None = None
    started_at: str | None = None
    completed_at: str | None = None


@dataclass
class RunState:
    commit: str
    types: list[str]
    created_at: str
    indices: dict[str, IndexState] = field(default_factory=dict)

    # ---- Persistence -------------------------------------------------------

    def save(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "commit": self.commit,
            "types": self.types,
            "created_at": self.created_at,
            "indices": {
                src: dataclasses.asdict(s)
                for src, s in self.indices.items()
            },
        }
        STATE_FILE.write_text(json.dumps(data, indent=2))

    def update(self, src: str, **kwargs: Any) -> None:
        """Update fields on an index state entry and persist to disk immediately."""
        for k, v in kwargs.items():
            setattr(self.indices[src], k, v)
        self.save()

    @classmethod
    def load(cls) -> RunState | None:
        if not STATE_FILE.exists():
            return None
        try:
            data = json.loads(STATE_FILE.read_text())
            return cls(
                commit=data["commit"],
                types=data["types"],
                created_at=data["created_at"],
                indices={
                    src: IndexState(**vals)
                    for src, vals in data.get("indices", {}).items()
                },
            )
        except Exception:
            return None

    @classmethod
    def create(cls, commit: str, types: list[str]) -> RunState:
        return cls(commit=commit, types=sorted(types), created_at=_now())


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


# ---------------------------------------------------------------------------
# Migration steps
# ---------------------------------------------------------------------------

async def _poll_task(
    es: AsyncElasticsearch,
    state: RunState,
    src: str,
) -> str:
    """Poll a running reindex task until it finishes.

    Returns the final task status: SWAP_PENDING on success, FAILED on failure.
    """
    task_id = state.indices[src].task_id
    _info(f"  Polling task [cyan]{task_id}[/cyan] ...")

    while True:
        await asyncio.sleep(POLL_INTERVAL_SECS)

        try:
            task = await es.tasks.get(task_id=task_id)
        except NotFoundError:
            # Task completed and was evicted from the task store; treat as done.
            _info(f"  Task {task_id} evicted from task store — checking destination index ...")
            dst = state.indices[src].dst
            if await _index_exists(es, dst):
                _info(f"  Destination {dst} exists — proceeding to alias swap.")
                return SWAP_PENDING
            else:
                _warn(f"  Destination {dst} not found after task eviction — will restart reindex.")
                return FAILED

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

        if not completed:
            continue

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
            return FAILED

        return SWAP_PENDING


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
    """Drive one index through its full migration state machine."""
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

    # ── Step 1: ensure reindex is running ───────────────────────────────────
    if idx.status in (PENDING, FAILED):
        new_status = await _start_reindex(es, state, src)
        if new_status == FAILED:
            state.update(src, status=FAILED, error="Failed to start reindex")
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
# Main
# ---------------------------------------------------------------------------

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


async def _run(args: argparse.Namespace) -> int:
    url = os.environ.get("GE_ELASTICSEARCH_URL", "https://localhost:9200")
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

        commit = args.commit or _git_short_hash()
        _info(f"Destination suffix: [cyan]{commit}[/cyan]")

        if args.dry_run:
            _warn("Dry-run mode — no changes will be made and state will not be saved.")
        console.print()

        # ── State init ───────────────────────────────────────────────────────
        sorted_types = sorted(args.types)
        state: RunState | None = None

        if not args.dry_run:
            if args.reset:
                _warn("--reset: discarding previous state.")
                STATE_FILE.unlink(missing_ok=True)
            else:
                state = RunState.load()
                if state is not None:
                    if state.commit == commit and sorted(state.types) == sorted_types:
                        done = sum(1 for s in state.indices.values() if s.status == DONE)
                        total_tracked = len(state.indices)
                        _info(
                            f"Resuming previous run from {state.created_at} "
                            f"({done}/{total_tracked} indices already done)."
                        )
                    else:
                        _warn(
                            f"State file is for a different run "
                            f"(commit={state.commit}, types={state.types}). "
                            f"Use --reset to start fresh."
                        )
                        return 1

            if state is None:
                state = RunState.create(commit, sorted_types)
                state.save()
                _info(f"State file: {STATE_FILE}")

            console.print()

        # ── Discover indices ─────────────────────────────────────────────────
        errors = 0

        for type_name in sorted_types:
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
                # Register indices in state on first encounter (non-dry-run).
                if state is not None and idx not in state.indices:
                    if idx == active_index:
                        state.indices[idx] = IndexState(
                            status=SKIPPED, src=idx, dst=f"{idx}-{commit}",
                            error="active write index",
                        )
                        state.save()
                        _warn(f"Skipping active index: {idx}")
                        continue
                    if _MIGRATED_RE.search(idx):
                        state.indices[idx] = IndexState(
                            status=SKIPPED, src=idx, dst=idx,
                            error="already migrated",
                        )
                        state.save()
                        _info(f"Skipping already-migrated index: {idx}")
                        continue
                    state.indices[idx] = IndexState(
                        status=PENDING, src=idx, dst=f"{idx}-{commit}",
                    )
                    state.save()
                else:
                    # Dry-run or already in state: apply simple guards.
                    if idx == active_index:
                        _warn(f"Skipping active index: {idx}")
                        continue
                    if _MIGRATED_RE.search(idx) and (state is None or idx not in state.indices):
                        _info(f"Skipping already-migrated index: {idx}")
                        continue

                try:
                    await _process_index(es, state, idx, commit, dry_run=args.dry_run)
                    if state is not None and state.indices[idx].status == FAILED:
                        errors += 1
                except Exception as exc:
                    console.print(f"[red][ERROR][/red] {idx}: {exc}")
                    if state is not None:
                        state.update(idx, status=FAILED, error=str(exc))
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
            _info(f"State file removed (migration complete).")

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
        return 0

    finally:
        await es.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reindex Elasticsearch indices to pick up updated mappings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  pipenv run python reindex.py --types posts replies --dry-run
  pipenv run python reindex.py --types posts replies --include-active
  pipenv run python reindex.py --types posts replies likes
  pipenv run python reindex.py --types posts --reset
""",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=list(INDEX_TYPES),
        required=True,
        metavar="TYPE",
        help=(
            f"Index types to migrate (required). "
            f"Choices: {', '.join(INDEX_TYPES)}."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without making any changes. State is not saved.",
    )
    parser.add_argument(
        "--include-active",
        action="store_true",
        help=(
            "Also migrate the index currently receiving live writes "
            "(the is_write_index member of the posts/replies/likes alias). "
            "Only safe once the current write period has rolled over."
        ),
    )
    parser.add_argument(
        "--commit",
        metavar="HASH",
        help="Override the git commit hash used as the destination index suffix.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Discard any saved state and start the migration from scratch.",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
