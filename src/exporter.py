from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import ApiError, MPClient
from .manifest import create_manifest, load_manifest_if_exists, save_bundle
from .types import GraphState
from .utils import uuid_from_resource_url


CONTENT_LIST_FIELDS = [
    "uuid",
    "content_type",
    "modified",
    "created",
]

LOCATION_LIST_FIELDS = [
    "uuid",
    # "modified",
    # "created",
]

@dataclass
class Exporter:
    client: MPClient
    output_dir: Path
    from_date: str
    limit: int | None = None
    resume: bool = False

    def __post_init__(self) -> None:
        self._seeded_content_count = 0
        self._seeded_location_count = 0

    def export(self) -> Path:
        manifest = self._load_or_create_manifest()
        state = GraphState()
        self._seed_content(state)
        self._seed_locations(state)
        while state.queue:
            item = state.queue.popleft()
            if self._already_exported(item.resource_type, item.uuid, manifest):
                continue
            print(f"Exporting {item.resource_type} {item.uuid}")
            try:
                if item.resource_type == "content":
                    self._export_content(item.uuid, manifest, state)
                elif item.resource_type == "location":
                    self._export_location(item.uuid, manifest, state)
                elif item.resource_type == "tag":
                    self._export_tag(item.uuid, manifest, state)
                elif item.resource_type == "file":
                    self._export_file(item.uuid, manifest, state)
                save_bundle(manifest, self.output_dir)
            except ApiError:
                save_bundle(manifest, self.output_dir)
                print(f"Checkpoint saved to {self.output_dir / 'export.json'}")
                print("Retry the same export with --resume and the same --output directory.")
                raise
        return save_bundle(manifest, self.output_dir)

    def _load_or_create_manifest(self) -> dict:
        if self.resume:
            existing = load_manifest_if_exists(self.output_dir)
            if existing is not None:
                return existing
        return create_manifest(self.from_date, self.client.endpoint.instance_id)

    def _already_exported(self, resource_type: str, uuid: str, manifest: dict) -> bool:
        mapping = {
            "content": "content",
            "location": "locations",
            "tag": "tags",
            "file": "files",
        }
        manifest_key = mapping.get(resource_type)
        if not manifest_key:
            return False
        return uuid in manifest.get(manifest_key, {})

    def _seed_content(self, state: GraphState) -> None:
        rows = self.client.iter_collection(
            "/content",
            params={
                "fields": "-".join(CONTENT_LIST_FIELDS),
                "created": self._created_period_filter(),
                "order": "title.desc",
            },
        )
        for row in rows:
            if self._limit_reached("content"):
                return
            values = dict(zip(CONTENT_LIST_FIELDS, row))
            state.enqueue("content", values["uuid"])
            self._seeded_content_count += 1

    def _seed_locations(self, state: GraphState) -> None:
        rows = self.client.iter_collection(
            "/locations",
            params={
                "fields": "-".join(LOCATION_LIST_FIELDS),
                "created": self._created_period_filter(),
                "order": "title.desc",
            },
        )
        for row in rows:
            if self._limit_reached("location"):
                return
            values = dict(zip(LOCATION_LIST_FIELDS, row))
            state.enqueue("location", values["uuid"])
            self._seeded_location_count += 1

    def _limit_reached(self, resource_type: str) -> bool:
        if self.limit is None:
            return False
        if resource_type == "content":
            return self._seeded_content_count >= self.limit
        if resource_type == "location":
            return self._seeded_location_count >= self.limit
        return False

    def _created_period_filter(self) -> str:
        return f"{self.from_date}T00:00:00_"

    def _export_content(self, uuid: str, manifest: dict, state: GraphState) -> None:
        if uuid in manifest["content"]:
            return
        data = self.client.get_json(f"/content/{uuid}")
        manifest["content"][uuid] = data
        for key in ("teaser_image_uuid", "header_image_uuid", "feature_image_uuid", "recipe_image_uuid", "album_image_uuid"):
            state.enqueue("file", data.get(key))
        for key in ("teaser_image_url", "header_image_url", "feature_image_url", "recipe_image_url", "album_image_url"):
            state.enqueue("file", uuid_from_resource_url(data.get(key)))
        state.enqueue("location", data.get("location_uuid"))
        for roundup in data.get("roundup_locations", []) or []:
            location_uuid = roundup.get("location_uuid") or roundup.get("target_uuid")
            state.enqueue("location", location_uuid)
        for roundup in data.get("roundup_content_targets", []) or []:
            target_uuid = roundup.get("target_uuid") or roundup.get("content_uuid")
            state.enqueue("content", target_uuid)
        manifest["relationships"]["roundups"]["content_to_locations"][uuid] = data.get("roundup_locations", []) or []
        manifest["relationships"]["roundups"]["content_to_content"][uuid] = data.get("roundup_content_targets", []) or []
        self._export_related_links(uuid, manifest, state)
        self._export_content_tags(uuid, manifest, state)
        self._export_slots(uuid, manifest, state)

    def _export_location(self, uuid: str, manifest: dict, state: GraphState) -> None:
        if uuid in manifest["locations"]:
            return
        data = self.client.get_json(f"/locations/{uuid}")
        manifest["locations"][uuid] = data
        state.enqueue("file", data.get("thumb_uuid"))
        state.enqueue("file", uuid_from_resource_url(data.get("thumb_url")))
        state.enqueue("file", data.get("coupon_img_uuid"))
        self._export_object_tags(resource_path=f"/locations/{uuid}/tags", object_type="location", object_uuid=uuid, manifest=manifest, state=state)

    def _export_tag(self, uuid: str, manifest: dict, state: GraphState) -> None:
        if uuid in manifest["tags"]:
            return
        data = self.client.get_json(f"/tags/{uuid}")
        manifest["tags"][uuid] = data
        state.enqueue("file", data.get("feature_image_uuid") or uuid_from_resource_url(data.get("feature_image_url")))

    def _export_file(self, uuid: str, manifest: dict, state: GraphState) -> None:
        if uuid in manifest["files"]:
            return
        data = self.client.get_json(f"/files/{uuid}")
        relative_path = Path("files") / f"{uuid}_{data['filename']}"
        data["local_path"] = str(relative_path)
        manifest["files"][uuid] = data
        output_path = self.output_dir / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            self._export_object_tags(resource_path=f"/files/{uuid}/tags", object_type="file", object_uuid=uuid, manifest=manifest, state=state)
            return
        try:
            file_bytes = self.client.download(data["download_url"])
            output_path.write_bytes(file_bytes)
        except ApiError as exc:
            manifest["files"][uuid]["download_error"] = str(exc)
            print(f"Skipping file download for {uuid}: {exc}")
        self._export_object_tags(resource_path=f"/files/{uuid}/tags", object_type="file", object_uuid=uuid, manifest=manifest, state=state)

    def _export_content_tags(self, uuid: str, manifest: dict, state: GraphState) -> None:
        self._export_object_tags(resource_path=f"/content/{uuid}/tags", object_type="content", object_uuid=uuid, manifest=manifest, state=state)

    def _export_related_links(self, uuid: str, manifest: dict, state: GraphState) -> None:
        response = self.client.get_json(f"/content/{uuid}/related_links", ok_statuses=(200, 404))
        if not response or "items" not in response:
            manifest["relationships"]["related_links"][uuid] = []
            return
        items = response.get("items", [])
        manifest["relationships"]["related_links"][uuid] = items
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "content":
                continue
            state.enqueue("content", item.get("uuid") or uuid_from_resource_url(item.get("url")))

    def _export_object_tags(
        self,
        resource_path: str,
        object_type: str,
        object_uuid: str,
        manifest: dict,
        state: GraphState,
    ) -> None:
        response = self.client.get_json(resource_path, ok_statuses=(200, 404))
        if not response or "items" not in response:
            return
        for row in response["items"]:
            if isinstance(row, dict):
                tag_uuid = row.get("uuid") or row.get("tag_uuid") or uuid_from_resource_url(row.get("url"))
                predicate = row.get("predicate", "describes")
            else:
                tag_uuid = None
                predicate = "describes"
                for value in row:
                    if isinstance(value, str) and len(value) == 36 and value.count("-") == 4:
                        tag_uuid = value
                        break
            if not tag_uuid:
                continue
            state.enqueue("tag", tag_uuid)
            manifest["relationships"]["taggings"].append(
                {
                    "object_type": object_type,
                    "object_uuid": object_uuid,
                    "tag_uuid": tag_uuid,
                    "predicate": predicate,
                }
            )

    def _export_slots(self, uuid: str, manifest: dict, state: GraphState) -> None:
        slots_data = self.client.get_json(f"/content/{uuid}/slots")
        items = slots_data.get("items") if isinstance(slots_data, dict) else None
        if items is None:
            items = slots_data if isinstance(slots_data, list) else []
        normalized_slots = []
        for raw_slot in items:
            slot_uuid = None
            if isinstance(raw_slot, dict):
                slot_uuid = raw_slot.get("uuid")
                slot_data = raw_slot
            else:
                slot_uuid = next((value for value in raw_slot if isinstance(value, str) and len(value) == 36 and value.count("-") == 4), None)
                slot_data = {"uuid": slot_uuid}
            if not slot_uuid:
                continue
            detailed_slot = self.client.get_json(f"/content/{uuid}/slots/{slot_uuid}")
            media = self.client.get_json(f"/content/{uuid}/slots/{slot_uuid}/media")
            slot_record = {"slot": detailed_slot, "media": media}
            normalized_slots.append(slot_record)
            media_items = []
            if isinstance(media, dict) and "items" in media:
                media_items = media["items"]
            elif isinstance(media, list):
                media_items = media
            for media_item in media_items:
                if isinstance(media_item, dict):
                    state.enqueue("file", media_item.get("image_uuid"))
                    state.enqueue("file", media_item.get("file_uuid"))
                    state.enqueue("file", media_item.get("thumb_uuid"))
        manifest["relationships"]["content_slots"][uuid] = normalized_slots
