from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.exporter import Exporter


class FakeClient:
    def __init__(self) -> None:
        self.endpoint = type("Endpoint", (), {"instance_id": "123"})()
        self.collection_calls = []

    def iter_collection(self, path: str, params=None):
        self.collection_calls.append((path, params))
        if path == "/content":
            return [["content-1", "roundup_content", "2026-01-02T00:00:00", "2026-01-01T00:00:00"]]
        if path == "/locations":
            return []
        return []

    def get_json(self, path: str, params=None, ok_statuses=(200,)):
        mapping = {
            "/content/content-1": {
                "uuid": "content-1",
                "content_type": "roundup_content",
                "roundup_content_targets": [{"target_uuid": "content-2"}],
                "roundup_locations": [{"target_uuid": "location-1"}],
            },
            "/content/content-1/tags": {"items": []},
            "/content/content-1/slots": {"items": []},
            "/content/content-2": {"uuid": "content-2", "content_type": "article"},
            "/content/content-2/tags": {"items": []},
            "/content/content-2/slots": {"items": []},
            "/locations/location-1": {"uuid": "location-1"},
            "/locations/location-1/tags": {"items": []},
        }
        return mapping.get(path, {"items": []})

    def download(self, url: str) -> bytes:
        return b""


class ExporterGraphTest(TestCase):
    def test_exporter_follows_roundup_targets(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=FakeClient(), output_dir=tmp_path, from_date="2026-01-01")
            manifest_path = exporter.export()
            manifest = manifest_path.read_text(encoding="utf-8")
            self.assertIn("content-2", manifest)
            self.assertIn("location-1", manifest)

    def test_exporter_uses_created_filter_for_seed_queries(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            client = FakeClient()
            exporter = Exporter(client=client, output_dir=tmp_path, from_date="2026-01-01")
            exporter.export()
        self.assertEqual(
            client.collection_calls[0],
            (
                "/content",
                {
                    "fields": "uuid-content_type-modified-created",
                    "created": "2026-01-01T00:00:00_",
                    "order": "title.desc",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[1],
            (
                "/locations",
                {
                    "fields": "uuid",
                    "created": "2026-01-01T00:00:00_",
                    "order": "title.desc",
                },
            ),
        )

    def test_exporter_limit_applies_only_to_top_level_seed_items(self) -> None:
        class LimitedFakeClient(FakeClient):
            def iter_collection(self, path: str, params=None):
                self.collection_calls.append((path, params))
                if path == "/content":
                    return [
                        ["content-1", "roundup_content", "2026-01-02T00:00:00", "2026-01-01T00:00:00"],
                        ["content-3", "article", "2026-01-03T00:00:00", "2026-01-01T00:00:00"],
                    ]
                if path == "/locations":
                    return [["location-top", "2026-01-02T00:00:00", "2026-01-01T00:00:00"]]
                return []

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=LimitedFakeClient(), output_dir=tmp_path, from_date="2026-01-01", limit=1)
            manifest_path = exporter.export()
            manifest = manifest_path.read_text(encoding="utf-8")
            self.assertIn("content-1", manifest)
            self.assertNotIn("content-3", manifest)
            self.assertIn("location-top", manifest)
            self.assertIn("content-2", manifest)
            self.assertIn("location-1", manifest)

    def test_exporter_limit_is_separate_for_content_and_locations(self) -> None:
        class SeparateLimitFakeClient(FakeClient):
            def iter_collection(self, path: str, params=None):
                self.collection_calls.append((path, params))
                if path == "/content":
                    return [
                        ["content-1", "article", "2026-01-02T00:00:00", "2026-01-01T00:00:00"],
                        ["content-2", "article", "2026-01-03T00:00:00", "2026-01-01T00:00:00"],
                    ]
                if path == "/locations":
                    return [
                        ["location-1"],
                        ["location-2"],
                    ]
                return []

            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/content/content-1": {"uuid": "content-1", "content_type": "article"},
                    "/content/content-1/tags": {"items": []},
                    "/content/content-1/slots": {"items": []},
                    "/content/content-2": {"uuid": "content-2", "content_type": "article"},
                    "/content/content-2/tags": {"items": []},
                    "/content/content-2/slots": {"items": []},
                    "/locations/location-1": {"uuid": "location-1"},
                    "/locations/location-1/tags": {"items": []},
                    "/locations/location-2": {"uuid": "location-2"},
                    "/locations/location-2/tags": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=SeparateLimitFakeClient(), output_dir=tmp_path, from_date="2026-01-01", limit=1)
            manifest_path = exporter.export()
            manifest = manifest_path.read_text(encoding="utf-8")
            self.assertIn("content-1", manifest)
            self.assertNotIn("content-2", manifest)
            self.assertIn("location-1", manifest)
            self.assertNotIn("location-2", manifest)
