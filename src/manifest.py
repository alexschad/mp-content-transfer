from __future__ import annotations

import json
from pathlib import Path

from .types import Bundle


def create_manifest(from_date: str, source_instance_id: str) -> dict:
    return {
        "meta": {
            "from_date": from_date,
            "source_instance_id": source_instance_id,
        },
        "content": {},
        "locations": {},
        "tags": {},
        "files": {},
        "sections": {},
        "relationships": {
            "taggings": [],
            "content_slots": {},
            "roundups": {
                "content_to_locations": {},
                "content_to_content": {},
            },
        },
    }


def save_bundle(manifest: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "files").mkdir(exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def load_manifest_if_exists(output_dir: Path) -> dict | None:
    manifest_path = output_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def load_bundle(input_dir: Path) -> Bundle:
    manifest = json.loads((input_dir / "manifest.json").read_text(encoding="utf-8"))
    return Bundle(manifest=manifest, root=input_dir)
