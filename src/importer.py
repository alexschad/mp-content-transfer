from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from .client import ApiError, MPClient
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
        """Run the staged import pipeline and checkpoint progress after each stage item."""
        state = self._load_or_create_state()
        summary = _summary_from_state(state)
        created_sets = _created_sets_from_state(state)
        categories = _collect_categories(self.bundle.manifest.get("tags", {}))

        try:
            print("Import stage: file metadata")
            self._import_files_metadata(summary, state, created_sets)
            print("Import stage: file binaries")
            self._import_file_binaries(summary, state, created_sets)
            print("Import stage: tags")
            self._import_tags(summary, state, created_sets)
            print("Import stage: categories")
            self._import_categories(summary, state, categories, created_sets)
            print("Import stage: tag-category links")
            self._import_tag_categories(summary, state, categories)
            print("Import stage: locations")
            self._import_locations(summary, state, created_sets)
            print("Import stage: non-event content")
            self._import_content(summary, state, created_sets, event_only=False)
            print("Import stage: event content")
            self._import_content(summary, state, created_sets, event_only=True)
            print("Import stage: slots")
            self._restore_slots(summary, state, created_sets)
            print("Import stage: comments")
            self._import_comments(summary, state, created_sets)
            print("Import stage: location listing images")
            self._restore_location_listing_images(summary, state)
            print("Import stage: related links")
            self._restore_related_links(summary, state, created_sets)
            print("Import stage: roundups")
            self._restore_roundups(summary, state, created_sets)
            print("Import stage: taggings")
            self._restore_taggings(summary, state)
        finally:
            self._checkpoint(summary, state)
        return summary

    def _load_or_create_state(self) -> dict[str, Any]:
        """Load the resumable import checkpoint or create a new empty one."""
        existing = load_import_state_if_exists(self.bundle.root)
        if existing is not None:
            return existing
        return {
            "summary": asdict(ImportSummary()),
            "import_section_uuid": None,
            "processed": {},
        }

    def _checkpoint(self, summary: ImportSummary, state: dict[str, Any]) -> None:
        """Persist the current import summary and stage progress to disk."""
        state["summary"] = asdict(summary)
        save_import_state(state, self.bundle.root)

    def _stage_map(self, state: dict[str, Any], stage: str) -> dict[str, str]:
        """Return the processed-item status map for a single import stage."""
        return state.setdefault("processed", {}).setdefault(stage, {})

    def _mark_stage(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        stage: str,
        key: str,
        status: str,
    ) -> None:
        """Record the outcome for one stage item and flush the checkpoint immediately."""
        self._stage_map(state, stage)[key] = status
        self._checkpoint(summary, state)

    def _ensure_import_section(self, summary: ImportSummary, state: dict[str, Any]) -> str:
        """Find or create the fallback Import section for missing source sections."""
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
            json={"title": "Import", "urlname": slugify("Import"), "hide_in_nav": True},
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
        """Create file metadata records before any binary uploads are attempted."""
        stage = "files_metadata"
        for uuid, payload in _sorted_items(self.bundle.manifest.get("files", {})):
            if uuid in self._stage_map(state, stage):
                continue
            print(f"Importing file metadata {uuid}")
            if self.client.resource_exists(f"/files/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            # remove later used for import testing only
            if not payload.get("title"):
                payload["title"] = payload.get("filename", uuid)
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
        """Upload file binary data only for files created in this import run."""
        stage = "files_data"
        file_stage = self._stage_map(state, "files_metadata")
        for uuid, payload in _sorted_items(self.bundle.manifest.get("files", {})):
            if uuid in self._stage_map(state, stage):
                continue
            if file_stage.get(uuid) != "created":
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            print(f"Uploading file binary {uuid}")
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
        """Create tags after files so feature images can already exist on the target."""
        stage = "tags"
        for uuid, payload in _sorted_items(self.bundle.manifest.get("tags", {})):
            if uuid in self._stage_map(state, stage):
                continue
            print(f"Importing tag {uuid}")
            if self.client.resource_exists(f"/tags/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            payload = dict(payload)
            feature_image_uuid = payload.get("feature_image_uuid") or uuid_from_resource_url(payload.get("feature_image_url"))
            if feature_image_uuid:
                payload["feature_image_uuid"] = feature_image_uuid
            result = self._put_with_urlname_retry(
                path=f"/tags/{uuid}",
                payload=_tag_payload(payload),
            )
            if result == "urlname_exists":
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, uuid, "urlname_exists")
                continue
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
        """Create tag category objects before linking tags into those categories."""
        stage = "categories"
        for uuid, payload in sorted(categories.items(), key=lambda item: ((item[1].get("title") or "").lower(), item[0])):
            if uuid in self._stage_map(state, stage):
                continue
            print(f"Importing tag category {uuid}")
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
        """Attach tags to categories once both sides exist on the target instance."""
        stage = "tag_categories"
        existing_tags_by_category: dict[str, set[str]] = {}
        for category_uuid, payload in sorted(categories.items()):
            for tag_uuid in sorted(payload["tag_uuids"]):
                key = f"{category_uuid}:{tag_uuid}"
                if key in self._stage_map(state, stage):
                    continue
                print(f"Linking tag {tag_uuid} to category {category_uuid}")
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
        """Create article-like content first and event content in its dedicated stage."""
        stage = "content_events" if event_only else "content_articles"
        for uuid, payload in _sorted_content_items(self.bundle.manifest.get("content", {}), event_only=event_only):
            if uuid in self._stage_map(state, stage):
                continue
            print(f"Importing content {uuid}")
            if self.client.resource_exists(f"/content/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            put_payload = dict(payload)
            section_uuid = put_payload.get("section_uuid")
            if section_uuid and not self.client.resource_exists(f"/sections/{section_uuid}"):
                put_payload["section_uuid"] = self._ensure_import_section(summary, state)
                summary.import_section_routed += 1
            # Roundup relationships are restored later in their own stage once all targets exist.
            put_payload["roundup_locations"] = []
            put_payload["roundup_content_targets"] = []
            result = self._put_with_urlname_retry(
                path=f"/content/{uuid}",
                payload=_content_payload(put_payload),
            )
            if result == "urlname_exists":
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, uuid, "urlname_exists")
                continue
            summary.created += 1
            created_sets["content"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "created")

    def _import_comments(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        """Import comments only when their parent content or parent comment already exists."""
        stage = "comments"
        pending = {uuid for uuid in self.bundle.manifest.get("comments", {}) if uuid not in self._stage_map(state, stage)}
        while pending:
            progressed = False
            for uuid, payload in _sorted_items(self.bundle.manifest.get("comments", {})):
                if uuid not in pending:
                    continue
                print(f"Importing comment {uuid}")
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
        """Create base location objects before any location-level lists are restored."""
        stage = "locations"
        for uuid, payload in _sorted_items(self.bundle.manifest.get("locations", {})):
            if uuid in self._stage_map(state, stage):
                continue
            print(f"Importing location {uuid}")
            if self.client.resource_exists(f"/locations/{uuid}"):
                summary.skipped_existing += 1
                self._mark_stage(summary, state, stage, uuid, "skipped_existing")
                continue
            result = self._put_with_urlname_retry(
                path=f"/locations/{uuid}",
                payload=_location_payload(payload),
            )
            if result == "urlname_exists":
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, uuid, "urlname_exists")
                continue
            summary.created += 1
            created_sets["locations"].add(uuid)
            self._mark_stage(summary, state, stage, uuid, "created")

    def _restore_slots(
        self,
        summary: ImportSummary,
        state: dict[str, Any],
        created_sets: dict[str, set[str]],
    ) -> None:
        """Restore slot definitions and only fill slot media when the target list is empty."""
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
                print(f"Restoring slot {slot_uuid} for content {content_uuid}")
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
                    items = _prune_none(items)
                    self.client.put(f"/content/{content_uuid}/slots/{slot_uuid}/media", json={"items": items})
                summary.relationship_created += 1
                self._mark_stage(summary, state, stage, key, "created")

    def _restore_location_listing_images(self, summary: ImportSummary, state: dict[str, Any]) -> None:
        """Restore listing image order only when the current target list is still empty."""
        stage = "location_listing_images"
        relationship_map = self.bundle.manifest.get("relationships", {}).get("location_listing_images", {})
        for location_uuid, items in sorted(relationship_map.items()):
            if location_uuid in self._stage_map(state, stage):
                continue
            print(f"Restoring listing images for location {location_uuid}")
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
        """Restore related links only when all targets exist and the target list is empty."""
        stage = "related_links"
        relationship_map = self.bundle.manifest.get("relationships", {}).get("related_links", {})
        for content_uuid, items in sorted(relationship_map.items()):
            if content_uuid in self._stage_map(state, stage):
                continue
            print(f"Restoring related links for content {content_uuid}")
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
        """Restore roundup references after all referenced content and locations exist."""
        stage = "roundups"
        roundups = self.bundle.manifest.get("relationships", {}).get("roundups", {})
        location_map = roundups.get("content_to_locations", {})
        content_map = roundups.get("content_to_content", {})
        for content_uuid in sorted(set(location_map) | set(content_map)):
            if content_uuid in self._stage_map(state, stage):
                continue
            print(f"Restoring roundups for content {content_uuid}")
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
            self.client.patch(f"/content/{content_uuid}", json=_content_payload(payload))
            summary.relationship_created += 1
            self._mark_stage(summary, state, stage, content_uuid, "created")

    def _restore_taggings(self, summary: ImportSummary, state: dict[str, Any]) -> None:
        """Recreate tag associations for newly created parents once tags exist on the target."""
        stage = "taggings"
        for tagging in self.bundle.manifest.get("relationships", {}).get("taggings", []):
            object_uuid = tagging["object_uuid"]
            object_type = tagging["object_type"]
            tag_uuid = tagging["tag_uuid"]
            predicate = tagging["predicate"]
            key = f"{object_type}:{object_uuid}:{predicate}:{tag_uuid}"
            if key in self._stage_map(state, stage):
                continue
            print(f"Restoring tagging {predicate} tag={tag_uuid} object={object_type}:{object_uuid}")
            object_path = TAGGING_PATHS.get(object_type, "").format(object_uuid=object_uuid)
            if not object_path or not self.client.resource_exists(object_path) or not self.client.resource_exists(f"/tags/{tag_uuid}"):
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, key, "missing_dependency")
                continue
            path = f"/tags/{tag_uuid}/{predicate}/{object_uuid}"
            if self.client.resource_exists(path):
                summary.relationship_skipped += 1
                self._mark_stage(summary, state, stage, key, "skipped_existing")
                continue
            self.client.put(path, json={})
            summary.relationship_created += 1
            self._mark_stage(summary, state, stage, key, "created")

    def _existing_category_tag_uuids(self, category_uuid: str) -> set[str]:
        """Read the current tag UUIDs assigned to one category on the target."""
        rows = self.client.iter_collection(f"/tags/categories/{category_uuid}/tags", params={"fields": "uuid"})
        values: set[str] = set()
        for row in rows:
            if isinstance(row, (list, tuple)) and row:
                values.add(row[0])
            elif isinstance(row, str):
                values.add(row)
        return values

    def _existing_listing_image_items(self, location_uuid: str) -> list[dict[str, str]]:
        """Read and normalize the current ordered listing image records for one location."""
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
        """Read and normalize the current related-link list into PUT-compatible payload items."""
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
        """Convert exported or fetched related-link rows into the canonical PUT payload form."""
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
        """Read the current media list for a slot to avoid replacing non-empty target lists."""
        response = self.client.get_json(f"/content/{content_uuid}/slots/{slot_uuid}/media", ok_statuses=(200, 404))
        if not response:
            return []
        items = response.get("items") if isinstance(response, dict) else None
        if not isinstance(items, list):
            return []
        return items

    def _put_with_urlname_retry(self, path: str, payload: dict[str, Any]) -> str:
        """Retry PUTs with suffixed urlnames when the API rejects a duplicate urlname."""
        current_payload = dict(payload)
        for attempt in range(11):
            try:
                self.client.put(path, json=current_payload)
                return "created"
            except ApiError as exc:
                if not _is_urlname_unique_error(exc) or "urlname" not in current_payload or not current_payload.get("urlname"):
                    raise
                if attempt >= 10:
                    return "urlname_exists"
                current_payload["urlname"] = _suffix_urlname(payload["urlname"], attempt + 1)
                print(f"Retrying {path} with urlname {current_payload['urlname']}")
        return "urlname_exists"


def _summary_from_state(state: dict[str, Any]) -> ImportSummary:
    """Rebuild the current summary counters from the saved import checkpoint."""
    return ImportSummary(**state.get("summary", {}))


def _created_sets_from_state(state: dict[str, Any]) -> dict[str, set[str]]:
    """Reconstruct which objects were created in prior runs from the stage checkpoint."""
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
    """Sort generic resource maps in a stable created-date-first order."""
    return sorted(values.items(), key=lambda item: (item[1].get("created") or "", item[0]))


def _sorted_content_items(values: dict[str, dict[str, Any]], *, event_only: bool) -> list[tuple[str, dict[str, Any]]]:
    """Split exported content into article-like and event subsets with stable ordering."""
    def include(payload: dict[str, Any]) -> bool:
        return payload.get("content_type") == "event" if event_only else payload.get("content_type") != "event"

    return sorted(
        [(uuid, payload) for uuid, payload in values.items() if include(payload)],
        key=lambda item: (item[1].get("created") or item[1].get("issued") or "", item[0]),
    )


def _collect_categories(tags: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Collapse per-tag exported category rows into unique category objects plus memberships."""
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
    """Select only the file fields that are valid for file metadata creation."""
    allowed = ["title", "description", "filename", "created", "modified", "credits", "focal_point", "img_quality"]
    return {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}


def _tag_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Select and normalize the subset of tag fields accepted by the tag PUT route."""
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
    """Select and normalize location fields accepted by the location PUT route."""
    allowed = [
        "urlname",
        "title",
        "description",
        "coords",
        "state",
        "thumb_uuid",
        "contact_email",
        "contact_person",
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
        "coupon_description",
        "coupon_expires",
        "coupon_img_uuid",
        "coupon_start",
        "coupon_title",
        "coupon_url",
        "is_listing",
        "kicker",
        "listing_expires",
        "listing_start",
        "location_types",
        "print_description",
        "sort_title",
        "sponsored",
        "fb_headline",
        "fb_url",
        "fb_show_faces",
        "fb_show_stream",
        "twitter_username",
        "instagram_username",
        "linkedin_url",
        "video_title",
        "video_ref",
        "video_type",
        "og_title",
        "og_description",
        "meta_title",
        "meta_description",
        "reservation_url",
        "reservation_url_text",
    ]
    payload = {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}
    thumb_uuid = data.get("thumb_uuid") or uuid_from_resource_url(data.get("thumb_url"))
    if thumb_uuid:
        payload["thumb_uuid"] = thumb_uuid
    return payload


def _content_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Select and normalize content fields accepted by the content PUT route."""
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
        "og_title",
        "og_description",
        "header_code",
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
        "rating",
        "recipe_prep_time",
        "recipe_cook_time",
        "recipe_yield",
        "recipe_ingredients",
        "recipe_image_uuid",
        "album_title",
        "album_image_uuid",
        "album_issued",
        "album_provider_urls",
        "album_buy_urls",
        "album_buy_url",
        "album_buy_link_text",
        "book_title",
        "book_image_uuid",
        "book_isbn",
        "book_issued",
        "book_provider_urls",
        "book_buy_urls",
        "book_buy_url",
        "book_buy_link_text",
        "movie_title",
        "movie_image_uuid",
        "movie_issued",
        "movie_provider_urls",
        "movie_buy_urls",
        "movie_duration",
        "product_title",
        "product_image_uuid",
        "product_issued",
        "product_provider_urls",
        "product_buy_urls",
        "product_buy_url",
        "product_buy_link_text",
        "roundup_locations",
        "roundup_location_tour_type",
        "roundup_location_hide_map",
        "roundup_content_targets",
        "roundup_numbering",
        "video_type",
        "video_data",
        "event_source",
    ]
    payload = {key: data.get(key) for key in allowed if key in data and data.get(key) is not None}
    if "rating" in payload:
        payload["rating"] = str(payload["rating"])
    if "rrule" in payload:
        payload["rrule"] = _prune_none(payload["rrule"])
    for url_key, uuid_key in [
        ("teaser_image_url", "teaser_image_uuid"),
        ("header_image_url", "header_image_uuid"),
        ("recipe_image_url", "recipe_image_uuid"),
        ("album_image_url", "album_image_uuid"),
        ("book_image_url", "book_image_uuid"),
        ("movie_image_url", "movie_image_uuid"),
        ("product_image_url", "product_image_uuid"),
    ]:
        if uuid_key not in payload:
            uuid = uuid_from_resource_url(data.get(url_key))
            if uuid:
                payload[uuid_key] = uuid
    return payload


def _comment_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Select and normalize comment fields accepted by the comment PUT route."""
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


def _prune_none(value: Any) -> Any:
    """Recursively remove None values from nested dict payloads and nested list items."""
    if isinstance(value, dict):
        return {key: _prune_none(inner) for key, inner in value.items() if inner is not None}
    if isinstance(value, list):
        return [_prune_none(inner) for inner in value if inner is not None]
    return value


def _is_urlname_unique_error(exc: ApiError) -> bool:
    """Detect the specific MetroPublisher validation error for duplicate urlnames."""
    message = str(exc).lower()
    return (
        ("urlname not unique within section" in message)
        or ("urlname must be unique within blog" in message)
        or ("urlname must be unique for events" in message)
    )


def _suffix_urlname(base: str, attempt: int) -> str:
    """Append the numeric retry suffix expected by the duplicate-urlname fallback."""
    return f"{base}-{attempt}"
