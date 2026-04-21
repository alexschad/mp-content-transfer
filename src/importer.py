from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from .client import MPClient
from .types import Bundle, ImportSummary
from .utils import slugify, uuid_from_resource_url


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
        summary = ImportSummary()
        import_section_uuid: str | None = None
        created_files: set[str] = set()
        created_tags: set[str] = set()
        created_locations: set[str] = set()
        created_content: set[str] = set()

        for uuid, payload in self.bundle.manifest.get("files", {}).items():
            if self.client.resource_exists(f"/files/{uuid}"):
                summary.skipped_existing += 1
                continue
            self.client.put(f"/files/{uuid}", json=_file_payload(payload))
            file_bytes = (self.bundle.root / payload["local_path"]).read_bytes()
            self.client.post(f"/files/{uuid}", data=file_bytes, headers={"Content-Type": payload["mimetype"]})
            summary.created += 1
            created_files.add(uuid)

        for uuid, payload in self.bundle.manifest.get("tags", {}).items():
            if self.client.resource_exists(f"/tags/{uuid}"):
                summary.skipped_existing += 1
                continue
            payload = dict(payload)
            feature_image_uuid = payload.get("feature_image_uuid") or uuid_from_resource_url(payload.get("feature_image_url"))
            if feature_image_uuid:
                payload["feature_image_uuid"] = feature_image_uuid
            self.client.put(f"/tags/{uuid}", json=_tag_payload(payload))
            summary.created += 1
            created_tags.add(uuid)

        for uuid, payload in self.bundle.manifest.get("locations", {}).items():
            if self.client.resource_exists(f"/locations/{uuid}"):
                summary.skipped_existing += 1
                continue
            self.client.put(f"/locations/{uuid}", json=_location_payload(payload))
            summary.created += 1
            created_locations.add(uuid)

        for uuid, payload in self.bundle.manifest.get("content", {}).items():
            if self.client.resource_exists(f"/content/{uuid}"):
                summary.skipped_existing += 1
                continue
            section_uuid = payload.get("section_uuid")
            if section_uuid:
                if not self.client.resource_exists(f"/sections/{section_uuid}"):
                    if not import_section_uuid:
                        import_section_uuid = self._ensure_import_section()
                    payload = dict(payload)
                    payload["section_uuid"] = import_section_uuid
                    summary.import_section_routed += 1
            self.client.put(f"/content/{uuid}", json=_content_payload(payload))
            summary.created += 1
            created_content.add(uuid)

        self._restore_slots(summary, created_content)
        self._restore_roundups(summary, created_content)
        self._restore_taggings(summary, created_content, created_locations, created_files, created_tags)
        return summary

    def _ensure_import_section(self) -> str:
        sections = self.client.iter_collection("/sections", params={"fields": "title-uuid-urlname"})
        for row in sections:
            title, uuid, _urlname = row
            if title == "Import":
                return uuid
        import_uuid = str(uuid4())
        self.client.put(
            f"/sections/{import_uuid}",
            json={"title": "Import", "urlname": slugify("Import")},
        )
        return import_uuid

    def _restore_slots(self, summary: ImportSummary, created_content: set[str]) -> None:
        slot_map = self.bundle.manifest.get("relationships", {}).get("content_slots", {})
        for content_uuid, slots in slot_map.items():
            if content_uuid not in created_content:
                summary.relationship_skipped += 1
                continue
            for record in slots:
                slot = record.get("slot", {})
                media = record.get("media", {})
                slot_uuid = slot.get("uuid")
                if not slot_uuid:
                    continue
                self.client.put(
                    f"/content/{content_uuid}/slots/{slot_uuid}",
                    json={"display": slot.get("display"), "relevance": slot.get("relevance")},
                )
                items = media.get("items") if isinstance(media, dict) else media
                if items is None:
                    items = []
                if isinstance(items, list):
                    self.client.put(f"/content/{content_uuid}/slots/{slot_uuid}/media", json={"items": items})
                    summary.relationship_created += 1

    def _restore_roundups(self, summary: ImportSummary, created_content: set[str]) -> None:
        roundups = self.bundle.manifest.get("relationships", {}).get("roundups", {})
        location_map = roundups.get("content_to_locations", {})
        content_map = roundups.get("content_to_content", {})
        for content_uuid, payload in self.bundle.manifest.get("content", {}).items():
            if content_uuid not in created_content:
                summary.relationship_skipped += 1
                continue
            if content_uuid not in location_map and content_uuid not in content_map:
                continue
            current_payload: dict[str, Any] = {}
            if content_uuid in location_map:
                current_payload["roundup_locations"] = location_map[content_uuid]
            if content_uuid in content_map:
                current_payload["roundup_content_targets"] = content_map[content_uuid]
            self.client.put(f"/content/{content_uuid}", json=_content_payload(current_payload))
            summary.relationship_created += 1

    def _restore_taggings(
        self,
        summary: ImportSummary,
        created_content: set[str],
        created_locations: set[str],
        created_files: set[str],
        created_tags: set[str],
    ) -> None:
        for tagging in self.bundle.manifest.get("relationships", {}).get("taggings", []):
            object_uuid = tagging["object_uuid"]
            object_type = tagging["object_type"]
            tag_uuid = tagging["tag_uuid"]
            predicate = tagging["predicate"]
            created_lookup = {
                "content": created_content,
                "location": created_locations,
                "file": created_files,
            }
            if tag_uuid not in created_tags and not self.client.resource_exists(f"/tags/{tag_uuid}"):
                summary.relationship_skipped += 1
                continue
            if object_uuid not in created_lookup.get(object_type, set()):
                summary.relationship_skipped += 1
                continue
            path = f"/tags/{tag_uuid}/{predicate}/{object_uuid}"
            self.client.put(path)
            summary.relationship_created += 1


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
