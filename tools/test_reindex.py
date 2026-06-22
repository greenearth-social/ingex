"""Unit tests for the resilience logic in reindex.py.

These cover the interrupt-safe resume path: re-attaching to running reindex
tasks, adopting orphaned reindexes, count-based progress, and the safety guard
that prevents deleting a destination that is already serving an alias.
"""

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from elasticsearch import NotFoundError

sys.path.insert(0, str(Path(__file__).parent))

import reindex  # noqa: E402
from reindex import (  # noqa: E402
    DONE,
    FAILED,
    PENDING,
    REINDEXING,
    SWAP_PENDING,
    IndexState,
    RunState,
)


def _nf() -> NotFoundError:
    return NotFoundError("not found", meta=None, body=None)


def _es() -> AsyncMock:
    return AsyncMock()


def _state(src: str, dst: str, status: str = PENDING, task_id=None) -> RunState:
    st = RunState.create("abc1234", ["posts"])
    st.indices[src] = IndexState(status=status, src=src, dst=dst, task_id=task_id)
    st.save = lambda: None  # never touch disk in tests
    return st


# ---------------------------------------------------------------------------
# _doc_count
# ---------------------------------------------------------------------------

async def test_doc_count_returns_count():
    es = _es()
    es.count.return_value = {"count": 42}
    assert await reindex._doc_count(es, "x") == 42


async def test_doc_count_missing_returns_none():
    es = _es()
    es.count.side_effect = _nf()
    assert await reindex._doc_count(es, "x") is None


# ---------------------------------------------------------------------------
# _task_running
# ---------------------------------------------------------------------------

async def test_task_running_true_when_not_completed():
    es = _es()
    es.tasks.get.return_value = {"completed": False}
    assert await reindex._task_running(es, "t1") is True


async def test_task_running_false_when_completed():
    es = _es()
    es.tasks.get.return_value = {"completed": True}
    assert await reindex._task_running(es, "t1") is False


async def test_task_running_false_when_evicted():
    es = _es()
    es.tasks.get.side_effect = _nf()
    assert await reindex._task_running(es, "t1") is False


# ---------------------------------------------------------------------------
# _find_running_reindex_to
# ---------------------------------------------------------------------------

async def test_find_running_reindex_to_matches_description():
    es = _es()
    es.tasks.list.return_value = {
        "nodes": {
            "node1": {
                "tasks": {
                    "node1:111": {
                        "description": "reindex from [posts-2026-w25] to [posts-2026-w25-abc1234]",
                    },
                    "node1:112": {  # a slice child — must be ignored
                        "description": "reindex",
                    },
                }
            }
        }
    }
    found = await reindex._find_running_reindex_to(es, "posts-2026-w25-abc1234")
    assert found == "node1:111"


async def test_find_running_reindex_to_no_match():
    es = _es()
    es.tasks.list.return_value = {
        "nodes": {
            "node1": {
                "tasks": {
                    "node1:111": {
                        "description": "reindex from [other] to [other-abc1234]",
                    }
                }
            }
        }
    }
    assert await reindex._find_running_reindex_to(es, "posts-2026-w25-abc1234") is None


# ---------------------------------------------------------------------------
# _ensure_reindex
# ---------------------------------------------------------------------------

async def test_ensure_reindex_reattaches_running_stored_task(monkeypatch):
    es = _es()
    st = _state("s", "d", status=FAILED, task_id="t1")
    monkeypatch.setattr(reindex, "_task_running", AsyncMock(return_value=True))
    start = AsyncMock()
    monkeypatch.setattr(reindex, "_start_reindex", start)

    assert await reindex._ensure_reindex(es, st, "s") == REINDEXING
    start.assert_not_awaited()


async def test_ensure_reindex_adopts_orphan(monkeypatch):
    es = _es()
    st = _state("s", "d", status=FAILED, task_id="dead")
    monkeypatch.setattr(reindex, "_task_running", AsyncMock(return_value=False))
    monkeypatch.setattr(reindex, "_find_running_reindex_to", AsyncMock(return_value="orphan99"))
    start = AsyncMock()
    monkeypatch.setattr(reindex, "_start_reindex", start)

    assert await reindex._ensure_reindex(es, st, "s") == REINDEXING
    assert st.indices["s"].task_id == "orphan99"
    start.assert_not_awaited()


async def test_ensure_reindex_dst_aliased_returns_done_without_delete(monkeypatch):
    es = _es()
    st = _state("s", "d", status=FAILED)
    monkeypatch.setattr(reindex, "_task_running", AsyncMock(return_value=False))
    monkeypatch.setattr(reindex, "_find_running_reindex_to", AsyncMock(return_value=None))
    monkeypatch.setattr(reindex, "_index_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(reindex, "_get_aliases", AsyncMock(return_value={"posts": {}}))
    start = AsyncMock()
    monkeypatch.setattr(reindex, "_start_reindex", start)

    assert await reindex._ensure_reindex(es, st, "s") == DONE
    es.indices.delete.assert_not_awaited()
    start.assert_not_awaited()


async def test_ensure_reindex_complete_dst_skips_to_swap(monkeypatch):
    es = _es()
    st = _state("s", "d", status=FAILED)
    monkeypatch.setattr(reindex, "_task_running", AsyncMock(return_value=False))
    monkeypatch.setattr(reindex, "_find_running_reindex_to", AsyncMock(return_value=None))
    monkeypatch.setattr(reindex, "_index_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(reindex, "_get_aliases", AsyncMock(return_value={}))
    monkeypatch.setattr(reindex, "_doc_count", AsyncMock(side_effect=[100, 100]))  # src, dst
    start = AsyncMock()
    monkeypatch.setattr(reindex, "_start_reindex", start)

    assert await reindex._ensure_reindex(es, st, "s") == SWAP_PENDING
    es.indices.delete.assert_not_awaited()
    start.assert_not_awaited()


async def test_ensure_reindex_partial_dst_deletes_and_restarts(monkeypatch):
    es = _es()
    st = _state("s", "d", status=FAILED)
    monkeypatch.setattr(reindex, "_task_running", AsyncMock(return_value=False))
    monkeypatch.setattr(reindex, "_find_running_reindex_to", AsyncMock(return_value=None))
    monkeypatch.setattr(reindex, "_index_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(reindex, "_get_aliases", AsyncMock(return_value={}))
    monkeypatch.setattr(reindex, "_doc_count", AsyncMock(side_effect=[100, 30]))  # src, dst partial
    start = AsyncMock(return_value=REINDEXING)
    monkeypatch.setattr(reindex, "_start_reindex", start)

    assert await reindex._ensure_reindex(es, st, "s") == REINDEXING
    es.indices.delete.assert_awaited_once()
    start.assert_awaited_once()


async def test_ensure_reindex_missing_dst_starts_fresh(monkeypatch):
    es = _es()
    st = _state("s", "d", status=PENDING)
    monkeypatch.setattr(reindex, "_task_running", AsyncMock(return_value=False))
    monkeypatch.setattr(reindex, "_find_running_reindex_to", AsyncMock(return_value=None))
    monkeypatch.setattr(reindex, "_index_exists", AsyncMock(return_value=False))
    start = AsyncMock(return_value=REINDEXING)
    monkeypatch.setattr(reindex, "_start_reindex", start)

    assert await reindex._ensure_reindex(es, st, "s") == REINDEXING
    es.indices.delete.assert_not_awaited()
    start.assert_awaited_once()


# ---------------------------------------------------------------------------
# _poll_task
# ---------------------------------------------------------------------------

async def test_poll_task_reports_counts_and_completes(monkeypatch):
    es = _es()
    st = _state("s", "d", status=REINDEXING, task_id="t1")
    monkeypatch.setattr(reindex.asyncio, "sleep", AsyncMock())
    # src count (pre-loop), then dst counts per iteration
    monkeypatch.setattr(reindex, "_doc_count", AsyncMock(side_effect=[100, 50, 100]))
    es.tasks.get.side_effect = [
        {"completed": False, "task": {"status": {}}},
        {
            "completed": True,
            "task": {"status": {}},
            "response": {"created": 100, "version_conflicts": 0, "failures": [], "took": 5},
        },
    ]

    assert await reindex._poll_task(es, st, "s") == SWAP_PENDING


async def test_poll_task_failures_return_failed(monkeypatch):
    es = _es()
    st = _state("s", "d", status=REINDEXING, task_id="t1")
    monkeypatch.setattr(reindex.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(reindex, "_doc_count", AsyncMock(side_effect=[100, 100]))
    es.tasks.get.side_effect = [
        {
            "completed": True,
            "task": {"status": {}},
            "response": {"created": 0, "version_conflicts": 5, "failures": [{"x": 1}], "took": 2},
        },
    ]

    assert await reindex._poll_task(es, st, "s") == FAILED


async def test_poll_task_eviction_complete_returns_swap(monkeypatch):
    es = _es()
    st = _state("s", "d", status=REINDEXING, task_id="t1")
    monkeypatch.setattr(reindex.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(reindex, "_doc_count", AsyncMock(side_effect=[100, 100]))  # src, dst on eviction
    es.tasks.get.side_effect = _nf()

    assert await reindex._poll_task(es, st, "s") == SWAP_PENDING


async def test_poll_task_eviction_partial_returns_failed(monkeypatch):
    es = _es()
    st = _state("s", "d", status=REINDEXING, task_id="t1")
    monkeypatch.setattr(reindex.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(reindex, "_doc_count", AsyncMock(side_effect=[100, 30]))  # src, dst on eviction
    es.tasks.get.side_effect = _nf()

    assert await reindex._poll_task(es, st, "s") == FAILED
