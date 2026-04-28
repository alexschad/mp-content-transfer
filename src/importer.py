from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from .client import MPClient
from .manifest import load_import_state_if_exists, save_import_state
from .types import Bundle, ImportSummary
from .utils import slugify, uuid_from_resource_url


RESOURCE_PATHS = {
    "content": "/content/{uuid}",
    "comment": "/comments/{uuid}",
    "location": "/locations/{uuid}",
    "tag": "/tags/{uuid}",
    "category": "/tags/categories/{uuid}",
    "file": "/files/{uuid}",
}

TAGGING_PATHS = {
    "content": "/content/{object_uuid}",
    "location": "/locations/{object_uuid}",
    "file": "/files/{object_uuid}",
}


@dataclass
class Importer:
    client: MPClient
    bundle: Bundle

    def import_bundle(self) -> ImportSummary:
        state = self._load_or_create_state()
        summary = _summary_from_state(state)
        created_sets = _created_sets_from_state(state)
        categories = _collect_categories(self.bundle.manifest.get("tags", {}))

        try:
            self._import_files_metadata(summary, state, created_sets)
            self._import_file_binaries(summary, state, created_sets)
            self._import_tags(summary, state, created_sets)
            self._import_categories(summary, state, categories, created_sets)
            self._import_tag_categories(summary, state, categories)
            self._import_content(summary, state, created_sets, event_only=False)
            self._import_content(summary, state, created_sets, event_only=True)
            self._restore_slots(summary, state, created_sets)
            self._import_comments(summary, state, created_sets)
            self._import_locations(summary, state, created_sets)
            self._restore_location_listing_images(summary, state)
            self._restore_related_links(summary, state, created_sets)
            self._restore_roundups(summary, state, created_sets)
            self._restore_taggings(summary, state)
        finally:
            self._checkpoint(summary, state)
        return summary

    def _load_or_create_state(self) -> dict[str, Any]:
        existing = load_import_state_if_exists(self.bundle.root)
        if existing is not None:
            return existing
        return {
            "summary": asdict(ImportSummary()),
            "import_section_uuid": None,
            "processed": {},
        }

    def _checkpoint(self, summary: ImportSummary, state: dict[str, Any]) -> None:
        state["summary"] = asdict(summary)
        save_import_state(state, self.bundle.root)

    def _stage_map(self, state: dict[str, Any], stage: str) -> dict[str, str]:
        return state.setdefault("processed", {}).setdefault(stage, {})

    def _mark_stage(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        stage: str,
        key: str,
        status: str,
    ) -> None:
        self._stage_map(state, stage)[key] = status
        self._checkpoint(summary, state)

    def _ensure_import_section(self, summary: ImportSummary, state: dict[str, Any]) -> str:
        if state.get("import_section_uuid"):
            return state["import_section_uuid"]
        sections = self.client.iter_collection("/sections", params={"fields": "title-uuid-urlname"})
        for row in sections:
            title, uuid, _urlname = row
            if title == "Import":
                state["import_section_uuid"] = uuid
                self._checkpoint(summary, state)
                return uuid
        import_uuid = str(uuid4())
        self.client.put(
            f"/sections/{import_uuid}",
            json={"title": "Import", "urlname": slugify("Import")},
        )
        state["import_section_uuid"] = import_uuid
        self._checkpoint(summary, state)
        return import_uuid

    def _import_files_metadata(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "files_metadata"
        for uuid, payload in _sorted_items(self.bundle.manifest.get("files", {})):
            if uuid in self._stage_map(state, stage):
                continue
            if self.client.resource_exists(f"/files/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            self.client.put(f"/files/{uuid}", json=_file_payload(payload))
            summary.created += 1
            created_sets["files"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "created")

    def _import_file_binaries(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "files_data"
        file_stage = self._stage_map(state, "files_metadata")
        for uuid, payload in _sorted_items(self.bundle.manifest.get("files", {})):
            if uuid in self._stage_map(state, stage):
                continue
            if file_stage.get(uuid) != "created":
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            file_bytes = (self.bundle.root / payload["local_path"]).read_bytes()
            self.client.post(f"/files/{uuid}", data=file_bytes, headers={"Content-Type": payload["mimetype"]})
            created_sets["files"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "uploaded")

    def _import_tags(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "tags"
        for uuid, payload in _sorted_items(self.bundle.manifest.get("tags", {})):
            if uuid in self._stage_map(state, stage):
                continue
            if self.client.resource_exists(f"/tags/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            payload = dict(payload)
            feature_image_uuid = payload.get("feature_image_uuid") or uuid_from_resource_url(payload.get("feature_image_url"))
            if feature_image_uuid:
                payload["feature_image_uuid"] = feature_image_uuid
            self.client.put(f"/tags/{uuid}", json=_tag_payload(payload))
            summary.created += 1
            created_sets["tags"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "created")

    def _import_categories(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        categories: dict[str, dict[str, Any]],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "categories"
        for uuid, payload in sorted(categories.items(), key=lambda item: ((item[1].get("title") or "").lower(), item[0])):
            if uuid in self._stage_map(state, stage):
                continue
            if self.client.resource_exists(f"/tags/categories/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            self.client.put(f"/tags/categories/{uuid}", json={"title": payload["title"]})
            summary.created += 1
            created_sets["categories"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "created")

    def _import_tag_categories(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        categories: dict[str, dict[str, Any]],
    ) -> None:
        stage = "tag_categories"
        existing_tags_by_category: dict[str, set[str]] = {}
        for category_uuid, payload in sorted(categories.items()):
            for tag_uuid in sorted(payload["tag_uuids"]):
                key = f"{category_uuid}:{tag_uuid}"
                if key in self._stage_map(state, stage):
                    continue
                if not self.client.resource_exists(f"/tags/categories/{category_uuid}") or not self.client.resource_exists(f"/tags/{tag_uuid}"):
                    summary.relationship_skipped += 1
                    self._mark_stage(summary, state, stage, key, "missing_dependency")
                    continue
                existing = existing_tags_by_category.setdefault(category_uuid, self._existing_category_tag_uuids(category_uuid))
                if tag_uuid in existing:
                    summary.relationship_skipped += 1
                    self._mark_stage(summary, state, stage, key, "skipped_existing")
                    continue
                self.client.post(f"/tags/categories/{category_uuid}/tags", json={"tag_uuid": tag_uuid})
                existing.add(tag_uuid)
                summary.relationship_created += 1
                self._mark_stage(summary, state, stage, key, "created")

    def _import_content(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
        *,
        event_only: bool,
    ) -> None:
        stage = "content_events" if event_only else "content_articles"
        for uuid, payload in _sorted_content_items(self.bundle.manifest.get("content", {}), event_only=event_only):
            if uuid in self._stage_map(state, stage):
                continue
            if self.client.resource_exists(f"/content/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            put_payload = dict(payload)
            section_uuid = put_payload.get("section_uuid")
            if section_uuid and not self.client.resource_exists(f"/sections/{section_uuid}"):
                put_payload["section_uuid"] = self._ensure_import_section(summary, state)
                summary.import_section_routed += 1
            self.client.put(f"/content/{uuid}", json=_content_payload(put_payload))
            summary.created += 1
            created_sets["content"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "created")

    def _import_comments(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "comments"
        pending = {uuid for uuid in self.bundle.manifest.get("comments", {}) if uuid not in self._stage_map(state, stage)}
        while pending:
            progressed = False
            for uuid, payload in _sorted_items(self.bundle.manifest.get("comments", {})):
                if uuid not in pending:
                    continue
                parent_type = payload.get("parent_type")
                parent_uuid = payload.get("parent_uuid") or uuid_from_resource_url(payload.get("parent_url"))
                parent_path = RESOURCE_PATHS.get(parent_type, "").format(uuid=parent_uuid) if parent_uuid else ""
                if parent_path and not self.client.resource_exists(parent_path):
                    continue
                if self.client.resource_exists(f"/comments/{uuid}"):
                    summary.skipped_existing += 1
                    self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                else:
                    self.client.put(f"/comments/{uuid}", json=_comment_payload(payload))
                    summary.created += 1
                    created_sets["comments"].add(uuid)
                    self._mark_stage(summary, state, stage, uuid, "created")
                pending.remove(uuid)
                progressed = True
            if not progressed:
                unresolved = ", ".join(sorted(pending)[:5])
                raise RuntimeError(f"Could not import remaining comments because parent objects are missing: {unresolved}")

    def _import_locations(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "locations"
        for uuid, payload in _sorted_items(self.bundle.manifest.get("locations", {})):
            if uuid in self._stage_map(state, stage):
                continue
            if self.client.resource_exists(f"/locations/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            self.client.put(f"/locations/{uuid}", json=_location_payload(payload))
            summary.created += 1
            created_sets["locations"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "created")

    def _restore_slots(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "slots"
        slot_map = self.bundle.manifest.get("relationships", {}).get("content_slots", {})
        for content_uuid, slots in sorted(slot_map.items()):
            for record in slots:
                slot = record.get("slot", {})
                media = record.get("media", {})
                slot_uuid = slot.get("uuid")
                if not slot_uuid:
                    continue
                key = f"{content_uuid}:{slot_uuid}"
                if key in self._stage_map(state, stage):
                    continue
                if content_uuid not in created_sets["content"]:
                    summary.relationship_skipped += 1
                    self._mark_stage(summary, state, stage, key, "skipped_existing_parent")
                    continue
                self.client.put(
                    f"/content/{content_uuid}/slots/{slot_uuid}",
                    json={"display": slot.get("display"), "relevance": slot.get("relevance")},
                )
                items = media.get("items") if isinstance(media, dict) else media
                if items is None:
                    items = []
                if isinstance(items, list):
                    current_items = self._existing_slot_media_items(content_uuid, slot_uuid)
                    if current_items:
                        summary.relationship_skipped += 1
                        self._mark_stage(summary, state, stage, key, "non_empty_target_list")
                        continue
                    self.client.put(f"/content/{content_uuid}/slots/{slot_uuid}/media", json={"items": items})
                summary.relationship_created += 1
                self._mark_stage(summary, state, stage, key, "created")

    def _restore_location_listing_images(self, summary: ImportSummary, state: dict[str, Any]) -> None:
        stage = "location_listing_images"
        relationship_map = self.bundle.manifest.get("relationships", {}).get("location_listing_images", {})
        for location_uuid, items in sorted(relationship_map.items()):
            if location_uuid in self._stage_map(state, stage):
                continue
            desired_items: list[dict[str, str]] = []
            missing_dependency = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                file_uuid = item.get("uuid") or uuid_from_resource_url(item.get("url"))
                if not file_uuid:
                    continue
                if not self.client.resource_exists(f"/locations/{location_uuid}") or not self.client.resource_exists(f"/files/{file_uuid}"):
                    missing_dependency = True
                    break
                desired_items.append({"uuid": file_uuid})
            if missing_dependency:
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, location_uuid, "missing_dependency")
                continue
            existing_items = self._existing_listing_image_items(location_uuid)
            if not desired_items:
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, location_uuid, "empty_source_list")
                continue
            if existing_items:
                if existing_items == desired_items:
                    summary.relationship_skipped += 1
                    self._mark_stage(summary, state, stage, location_uuid, "skipped_existing")
                    continue
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, location_uuid, "non_empty_target_list")
                continue
            self.client.put(f"/locations/{location_uuid}/listing_images", json={"items": desired_items})
            summary.relationship_created += 1
            self._mark_stage(summary, state, stage, location_uuid, "created")

    def _restore_related_links(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "related_links"
        relationship_map = self.bundle.manifest.get("relationships", {}).get("related_links", {})
        for content_uuid, items in sorted(relationship_map.items()):
            if content_uuid in self._stage_map(state, stage):
                continue
            if content_uuid not in created_sets["content"]:
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, content_uuid, "skipped_existing_parent")
                continue
            desired_items: list[dict[str, Any]] = []
            missing_dependency = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                normalized = self._normalize_related_link_for_put(item)
                if normalized is None:
                    continue
                link_type = normalized.get("type")
                target_uuid = normalized.get("target_uuid")
                if link_type == "content" and target_uuid and not self.client.resource_exists(f"/content/{target_uuid}"):
                    missing_dependency = True
                    break
                if link_type == "location" and target_uuid and not self.client.resource_exists(f"/locations/{target_uuid}"):
                    missing_dependency = True
                    break
                desired_items.append(normalized)
            if missing_dependency:
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, content_uuid, "missing_dependency")
                continue
            if not desired_items:
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, content_uuid, "empty_source_list")
                continue
            existing_items = self._existing_related_link_items(content_uuid)
            if existing_items:
                if existing_items == desired_items:
                    summary.relationship_skipped += 1
                    self._mark_stage(summary, state, stage, content_uuid, "skipped_existing")
                    continue
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, content_uuid, "non_empty_target_list")
                continue
            self.client.put(f"/content/{content_uuid}/related_links", json={"items": desired_items})
            summary.relationship_created += 1
            self._mark_stage(summary, state, stage, content_uuid, "created")

    def _restore_roundups(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        stage = "roundups"
        roundups = self.bundle.manifest.get("relationships", {}).get("roundups", {})
        location_map = roundups.get("content_to_locations", {})
        content_map = roundups.get("content_to_content", {})
        for content_uuid in sorted(set(location_map) | set(content_map)):
            if content_uuid in self._stage_map(state, stage):
                continue
            if content_uuid not in created_sets["content"]:
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, content_uuid, "skipped_existing_parent")
                continue
            payload: dict[str, Any] = {}
            location_targets = location_map.get(content_uuid, [])
            content_targets = content_map.get(content_uuid, [])
            if any(not self.client.resource_exists(f"/locations/{item.get('location_uuid') or item.get('target_uuid')}") for item in location_targets if isinstance(item, dict)):
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, content_uuid, "missing_location_dependency")
                continue
            if any(not self.client.resource_exists(f"/content/{item.get('target_uuid') or item.get('content_uuid')}") for item in content_targets if isinstance(item, dict)):
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, content_uuid, "missing_content_dependency")
                continue
            if content_uuid in location_map:
                payload["roundup_locations"] = location_targets
            if content_uuid in content_map:
                payload["roundup_content_targets"] = content_targets
            self.client.put(f"/content/{content_uuid}", json=_content_payload(payload))
            summary.relationship_created += 1
            self._mark_stage(summary, state, stage, content_uuid, "created")

    def _restore_taggings(self, summary: ImportSummary, state: dict[str, Any]) -> None:
        stage = "taggings"
        for tagging in self.bundle.manifest.get("relationships", {}).get("taggings", []):
            object_uuid = tagging["object_uuid"]
            object_type = tagging["object_type"]
            tag_uuid = tagging["tag_uuid"]
            predicate = tagging["predicate"]
            key = f"{object_type}:{object_uuid}:{predicate}:{tag_uuid}"
            if key in self._stage_map(state, stage):
                continue
            object_path = TAGGING_PATHS.get(object_type, "").format(object_uuid=object_uuid)
            if not object_path or not self.client.resource_exists(object_path) or not self.client.resource_exists(f"/tags/{tag_uuid}"):
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, key, "missing_dependency")
                continue
            path = f"/tags/{tag_uuid}/{predicate}/{object_uuid}"
            self.client.put(path)
            summary.relationship_created += 1
            self._mark_stage(summary, state, stage, key, "created")

    def _existing_category_tag_uuids(self, category_uuid: str) -> set[str]:
        rows = self.client.iter_collection(f"/tags/categories/{category_uuid}/tags", params={"fields": "uuid"})
        values: set[str] = set()
        for row in rows:
            if isinstance(row, (list, tuple)) and row:
                values.add(row[0])
            elif isinstance(row, str):
                values.add(row)
        return values

    def _existing_listing_image_items(self, location_uuid: str) -> list[dict[str, str]]:
        response = self.client.get_json(f"/locations/{location_uuid}/listing_images", ok_statuses=(200, 404))
        if not response or "items" not in response:
            return []
        values: list[dict[str, str]] = []
        for item in response.get("items", []):
            if not isinstance(item, dict):
                continue
            file_uuid = item.get("uuid") or uuid_from_resource_url(item.get("url"))
            if not file_uuid:
                continue
            values.append({"uuid": file_uuid})
        return values

    def _existing_related_link_items(self, content_uuid: str) -> list[dict[str, Any]]:
        response = self.client.get_json(f"/content/{content_uuid}/related_links", ok_statuses=(200, 404))
        if not response or "items" not in response:
            return []
        values: list[dict[str, Any]] = []
        for item in response.get("items", []):
            if not isinstance(item, dict):
                continue
            normalized = self._normalize_related_link_for_put(item)
            if normalized is not None:
                values.append(normalized)
        return values

    def _normalize_related_link_for_put(self, item: dict[str, Any]) -> dict[str, Any] | None:
        link_type = item.get("type")
        if link_type == "content" or link_type == "location":
            target_uuid = item.get("target_uuid") or item.get("uuid") or uuid_from_resource_url(item.get("url"))
            if not target_uuid:
                return None
            return {"type": link_type, "target_uuid": target_uuid}
        if link_type == "url":
            link_url = item.get("link_url")
            if not link_url:
                return None
            payload: dict[str, Any] = {"type": "url", "link_url": link_url}
            if item.get("text") is not None:
                payload["text"] = item.get("text")
            return payload
        return None

    def _existing_slot_media_items(self, content_uuid: str, slot_uuid: str) -> list[Any]:
        response = self.client.get_json(f"/content/{content_uuid}/slots/{slot_uuid}/media", ok_statuses=(200, 404))
        if not response:
            return []
        items = response.get("items") if isinstance(response, dict) else None
        if not isinstance(items, list):
            return []
        return items


def _summary_from_state(state: dict[str, Any]) -> ImportSummary:
    return ImportSummary(**state.get("summary", {}))


def _created_sets_from_state(state: dict[str, Any]) -> dict[str, set[str]]:
    processed = state.get("processed", {})
    return {
        "files": {uuid for uuid, status in processed.get("files_metadata", {}).items() if status == "created"},
        "tags": {uuid for uuid, status in processed.get("tags", {}).items() if status == "created"},
        "categories": {uuid for uuid, status in processed.get("categories", {}).items() if status == "created"},
        "content": {
            uuid
            for stage in ("content_articles", "content_events")
            for uuid, status in processed.get(stage, {}).items()
            if status == "created"
        },
        "comments": {uuid for uuid, status in processed.get("comments", {}).items() if status == "created"},
        "locations": {uuid for uuid, status in processed.get("locations", {}).items() if status == "created"},
    }


def _sorted_items(values: dict[str, dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    return sorted(values.items(), key=lambda item: (item[1].get("created") or "", item[0]))


def _sorted_content_items(values: dict[str, dict[str, Any]], *, event_only: bool) -> list[tuple[str, dict[str, Any]]]:
    def include(payload: dict[str, Any]) -> bool:
        return payload.get("content_type") == "event" if event_only else payload.get("content_type") != "event"

    return sorted(
        [(uuid, payload) for uuid, payload in values.items() if include(payload)],
        key=lambda item: (item[1].get("created") or item[1].get("issued") or "", item[0]),
    )


def _collect_categories(tags: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    categories: dict[str, dict[str, Any]] = {}
    for tag_uuid, payload in tags.items():
        for category in payload.get("categories", []) or []:
            if not isinstance(category, dict):
                continue
            category_uuid = category.get("uuid")
            title = category.get("title")
            if not category_uuid or not title:
                continue
            entry = categories.setdefault(
                category_uuid,
                {
                    "uuid": category_uuid,
                    "title": title,
                    "tag_uuids": set(),
                },
            )
            entry["tag_uuids"].add(tag_uuid)
    return categories


def _file_payload(data: dict[str, Any]) -> dict[str, Any]:
    allowed = ["title", "description", "filename", "created", "modified", "credits"]
    return {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}


def _tag_payload(data: dict[str, Any]) -> dict[str, Any]:
    allowed = [
        "urlname",
        "last_name_or_title",
        "first_name",
        "description",
        "state",
        "type",
        "synonyms",
        "content",
        "feature_image_uuid",
        "created",
        "modified",
        "email",
        "website",
        "twitter_username",
        "fb_username",
        "instagram_username",
        "linkedin_url",
    ]
    payload = {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}
    payload.setdefault("type", data.get("type", "default"))
    return payload


def _location_payload(data: dict[str, Any]) -> dict[str, Any]:
    allowed = [
        "urlname",
        "title",
        "description",
        "coords",
        "state",
        "thumb_uuid",
        "street",
        "streetnumber",
        "pcode",
        "geoname_id",
        "phone",
        "fax",
        "email",
        "website",
        "price_index",
        "opening_hours",
        "content",
        "created",
        "modified",
        "closed",
        "print_description",
        "sort_title",
        "fb_headline",
        "fb_url",
        "fb_show_faces",
        "fb_show_stream",
        "twitter_username",
        "coupon_img_uuid",
    ]
    payload = {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}
    thumb_uuid = data.get("thumb_uuid") or uuid_from_resource_url(data.get("thumb_url"))
    if thumb_uuid:
        payload["thumb_uuid"] = thumb_uuid
    return payload


def _content_payload(data: dict[str, Any]) -> dict[str, Any]:
    allowed = [
        "urlname",
        "content_type",
        "perma_url_path",
        "canonical_url",
        "title",
        "sub_title",
        "description",
        "content",
        "created",
        "modified",
        "issued",
        "state",
        "meta_title",
        "meta_description",
        "teaser_image_uuid",
        "header_image_uuid",
        "section_uuid",
        "blog_uuid",
        "location_uuid",
        "location_alt",
        "dtstart",
        "dtend",
        "website",
        "phone",
        "prices",
        "user_email",
        "email",
        "sponsored",
        "event_status_type",
        "rrule",
        "rdates",
        "exdates",
        "recurrence_id",
        "ical_uid",
        "sort_title",
        "ticket_urls",
        "print_description",
        "kicker",
        "evergreen",
        "roundup_locations",
        "roundup_content_targets",
    ]
    payload = {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}
    for url_key, uuid_key in [
        ("teaser_image_url", "teaser_image_uuid"),
        ("header_image_url", "header_image_uuid"),
        ("feature_image_url", "feature_image_uuid"),
    ]:
        if uuid_key not in payload:
            uuid = uuid_from_resource_url(data.get(url_key))
            if uuid:
                payload[uuid_key] = uuid
    return payload


def _comment_payload(data: dict[str, Any]) -> dict[str, Any]:
    allowed = [
        "parent_type",
        "parent_uuid",
        "title",
        "edited_title",
        "comment",
        "edited_comment",
        "created",
        "creator",
        "state",
        "fb_uid",
        "email",
    ]
    payload = {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}
    parent_uuid = payload.get("parent_uuid") or uuid_from_resource_url(data.get("parent_url"))
    if parent_uuid:
        payload["parent_uuid"] = parent_uuid
    return payload
