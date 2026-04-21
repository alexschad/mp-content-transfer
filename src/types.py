from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from pathlib import Path
from typing import Any


JSON = dict[str, Any]


@dataclass
class Bundle:
    manifest: JSON
    root: Path


@dataclass
class ImportSummary:
    created: int = 0
    skipped_existing: int = 0
    relationship_created: int = 0
    relationship_skipped: int = 0
    import_section_routed: int = 0


@dataclass
class ExportQueueItem:
    resource_type: str
    uuid: str


@dataclass
class GraphState:
    seen: set[tuple[str, str]] = field(default_factory=set)
    queue: deque[ExportQueueItem] = field(default_factory=deque)

    def enqueue(self, resource_type: str, uuid: str | None) -> None:
        if not uuid:
            return
        key = (resource_type, uuid)
        if key in self.seen:
            return
        self.seen.add(key)
        self.queue.append(ExportQueueItem(resource_type=resource_type, uuid=uuid))
