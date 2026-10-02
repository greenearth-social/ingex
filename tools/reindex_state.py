"""Persistent run-state for reindex.py.

State is written to tools/state/reindex-state.json after every status
transition so migrations can be resumed after interruption.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STATE_DIR  = Path(__file__).parent / "state"
STATE_FILE = STATE_DIR / "reindex-state.json"

# Per-index status values — drive the migration state machine.
PENDING      = "pending"
REINDEXING   = "reindexing"
SWAP_PENDING = "swap_pending"
DONE         = "done"
SKIPPED      = "skipped"
FAILED       = "failed"


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
    explicit_indices: list[str] = field(default_factory=list)

    @classmethod
    def create(
        cls,
        commit: str,
        types: list[str],
        explicit_indices: list[str] | None = None,
    ) -> RunState:
        return cls(
            commit=commit,
            types=sorted(types),
            explicit_indices=sorted(explicit_indices or []),
            created_at=_now(),
        )

    @classmethod
    def load(cls) -> RunState | None:
        if not STATE_FILE.exists():
            return None
        try:
            data = json.loads(STATE_FILE.read_text())
            return cls(
                commit=data["commit"],
                types=data["types"],
                explicit_indices=data.get("explicit_indices", []),
                created_at=data["created_at"],
                indices={
                    src: IndexState(**vals)
                    for src, vals in data.get("indices", {}).items()
                },
            )
        except Exception:
            return None

    def save(self) -> None:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "commit": self.commit,
            "types": self.types,
            "explicit_indices": self.explicit_indices,
            "created_at": self.created_at,
            "indices": {src: dataclasses.asdict(s) for src, s in self.indices.items()},
        }
        STATE_FILE.write_text(json.dumps(data, indent=2))

    def update(self, src: str, **kwargs: Any) -> None:
        """Update fields on one index entry and persist immediately."""
        for k, v in kwargs.items():
            setattr(self.indices[src], k, v)
        self.save()
