from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.importer import Importer
from src.types import Bundle


class FakeImportClient:
    def __init__(self) -> None:
        self.existing = {
            "/sections/source-section": False,
            "/content/existing-content": True,
            "/tags/tag-existing": True,
        }
        self.put_calls: list[tuple[str, dict | None]] = []
        self.post_calls: list[tuple[str, bytes | None, dict | None]] = []
        self.section_rows = []

    def resource_exists(self, path: str) -> bool:
        return self.existing.get(path, False)

    def put(self, path: str, json=None, ok_statuses=(200,)):
        self.put_calls.append((path, json))
        if path.startswith("/sections/"):
            self.existing[path] = True
        if path.startswith("/content/"):
            self.existing[path] = True
        if path.startswith("/locations/"):
            self.existing[path] = True
        if path.startswith("/tags/"):
            self.existing[path] = True
        if path.startswith("/files/"):
            self.existing[path] = True
        return None

    def post(self, path: str, data=None, headers=None, ok_statuses=(200, 201)):
        self.post_calls.append((path, data, headers))
        return None

    def iter_collection(self, path: str, params=None):
        if path == "/sections":
            yield from self.section_rows
        else:
            yield from []


class ImporterTest(TestCase):
    def test_content_with_missing_section_goes_to_import(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = Bundle(
                root=root,
                manifest={
                    "files": {},
                    "tags": {},
                    "locations": {},
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "article",
                            "title": "New Content",
                            "section_uuid": "source-section",
                        }
                    },
                    "relationships": {"taggings": [], "content_slots": {}, "roundups": {"content_to_locations": {}, "content_to_content": {}}},
                },
            )
            client = FakeImportClient()
            summary = Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [call for call in client.put_calls if call[0].startswith("/content/new-content")]
            self.assertEqual(summary.import_section_routed, 1)
            self.assertTrue(any(payload and payload.get("section_uuid") != "source-section" for _, payload in content_puts))

    def test_content_without_source_section_stays_unsectioned(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = Bundle(
                root=root,
                manifest={
                    "files": {},
                    "tags": {},
                    "locations": {},
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "article",
                            "title": "New Content",
                        }
                    },
                    "relationships": {"taggings": [], "content_slots": {}, "roundups": {"content_to_locations": {}, "content_to_content": {}}},
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [call for call in client.put_calls if call[0] == "/content/new-content"]
            self.assertEqual(len(content_puts), 1)
            self.assertNotIn("section_uuid", content_puts[0][1])

    def test_restore_roundups_puts_only_roundup_fields(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = Bundle(
                root=root,
                manifest={
                    "files": {},
                    "tags": {},
                    "locations": {},
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "roundup_content",
                            "title": "New Content",
                            "description": "should not be resent",
                        }
                    },
                    "relationships": {
                        "taggings": [],
                        "content_slots": {},
                        "roundups": {"content_to_locations": {}, "content_to_content": {"new-content": [{"target_uuid": "other"}]}},
                    },
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [payload for path, payload in client.put_calls if path == "/content/new-content"]
            self.assertEqual(len(content_puts), 2)
            self.assertEqual(set(content_puts[-1].keys()), {"roundup_content_targets"})
