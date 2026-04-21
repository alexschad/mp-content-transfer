from __future__ import annotations

from datetime import datetime
from urllib.parse import urlparse


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def uuid_from_resource_url(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    parts = path.split("/")
    if not parts:
        return None
    for segment in reversed(parts):
        if _looks_like_uuid(segment):
            return segment
    return None


def _looks_like_uuid(value: str) -> bool:
    parts = value.split("-")
    if len(parts) != 5:
        return False
    lengths = [8, 4, 4, 4, 12]
    return all(len(part) == expected for part, expected in zip(parts, lengths))


def slugify(value: str) -> str:
    value = value.strip().lower().replace(" ", "-")
    return "".join(ch for ch in value if ch.isalnum() or ch in {"-", "_"}).strip("-") or "import"
