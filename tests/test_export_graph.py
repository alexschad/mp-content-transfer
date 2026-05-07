from __future__ import annotations

import json
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
        if path == "/comments":
            return []
        if path == "/locations":
            return []
        if path.endswith("/categories"):
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
            "/content/content-1/related_links": {"items": []},
            "/content/content-1/tags": {"items": []},
            "/content/content-1/slots": {"items": []},
            "/content/content-2": {"uuid": "content-2", "content_type": "article"},
            "/content/content-2/related_links": {"items": []},
            "/content/content-2/tags": {"items": []},
            "/content/content-2/slots": {"items": []},
            "/locations/location-1": {"uuid": "location-1"},
            "/locations/location-1/listing_images": {"items": []},
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

    def test_exporter_preserves_open_graph_fields_in_content_payload(self) -> None:
        class OGClient(FakeClient):
            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                if path == "/content/content-1":
                    return {
                        "uuid": "content-1",
                        "content_type": "article",
                        "og_title": "Share title",
                        "og_description": "Share description",
                    }
                return super().get_json(path, params=params, ok_statuses=ok_statuses)

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=OGClient(), output_dir=tmp_path, from_date="2026-01-01")
            export_path = exporter.export()
            export_data = json.loads(export_path.read_text(encoding="utf-8"))
            content = export_data["content"]["content-1"]
            self.assertEqual(content.get("og_title"), "Share title")
            self.assertEqual(content.get("og_description"), "Share description")

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
                    "order": "created.asc",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[1],
            (
                "/comments",
                {
                    "fields": "uuid-created",
                    "order": "created.asc",
                    "created": "2026-01-01T00:00:00_",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[2],
            (
                "/locations",
                {
                    "fields": "uuid",
                    "created": "2026-01-01T00:00:00_",
                    "order": "created.asc",
                },
            ),
        )

    def test_exporter_uses_bounded_created_filter_when_from_and_to_dates_are_set(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            client = FakeClient()
            exporter = Exporter(
                client=client,
                output_dir=tmp_path,
                from_date="2026-01-01",
                to_date="2026-01-31",
            )
            exporter.export()
        self.assertEqual(
            client.collection_calls[0],
            (
                "/content",
                {
                    "fields": "uuid-content_type-modified-created",
                    "created": "2026-01-01T00:00:00_2026-01-31T00:00:00",
                    "order": "created.asc",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[1],
            (
                "/comments",
                {
                    "fields": "uuid-created",
                    "order": "created.asc",
                    "created": "2026-01-01T00:00:00_2026-01-31T00:00:00",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[2],
            (
                "/locations",
                {
                    "fields": "uuid",
                    "created": "2026-01-01T00:00:00_2026-01-31T00:00:00",
                    "order": "created.asc",
                },
            ),
        )

    def test_exporter_uses_open_ended_created_filter_when_only_to_date_is_set(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            client = FakeClient()
            exporter = Exporter(client=client, output_dir=tmp_path, to_date="2026-01-31")
            exporter.export()
        self.assertEqual(
            client.collection_calls[0],
            (
                "/content",
                {
                    "fields": "uuid-content_type-modified-created",
                    "created": "_2026-01-31T00:00:00",
                    "order": "created.asc",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[1],
            (
                "/comments",
                {
                    "fields": "uuid-created",
                    "order": "created.asc",
                    "created": "_2026-01-31T00:00:00",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[2],
            (
                "/locations",
                {
                    "fields": "uuid",
                    "created": "_2026-01-31T00:00:00",
                    "order": "created.asc",
                },
            ),
        )

    def test_exporter_omits_created_filter_when_no_dates_are_set(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            client = FakeClient()
            exporter = Exporter(client=client, output_dir=tmp_path)
            exporter.export()
        self.assertEqual(
            client.collection_calls[0],
            (
                "/content",
                {
                    "fields": "uuid-content_type-modified-created",
                    "order": "created.asc",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[1],
            (
                "/comments",
                {
                    "fields": "uuid-created",
                    "order": "created.asc",
                },
            ),
        )
        self.assertEqual(
            client.collection_calls[2],
            (
                "/locations",
                {
                    "fields": "uuid",
                    "order": "created.asc",
                },
            ),
        )

    def test_exporter_exports_comments_and_enqueues_parent_content(self) -> None:
        class CommentClient(FakeClient):
            def iter_collection(self, path: str, params=None):
                self.collection_calls.append((path, params))
                if path == "/content":
                    return []
                if path == "/comments":
                    return [["comment-1", "2026-01-02T00:00:00"]]
                if path == "/locations":
                    return []
                return []

            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/comments/comment-1": {
                        "uuid": "comment-1",
                        "parent_type": "content",
                        "parent_uuid": "content-99",
                        "comment": "Nice article",
                    },
                    "/content/content-99": {"uuid": "content-99", "content_type": "article"},
                    "/content/content-99/related_links": {"items": []},
                    "/content/content-99/tags": {"items": []},
                    "/content/content-99/slots": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=CommentClient(), output_dir=tmp_path, from_date="2026-01-01")
            export_path = exporter.export()
            export_data = export_path.read_text(encoding="utf-8")
            self.assertIn('"comments"', export_data)
            self.assertIn("comment-1", export_data)
            self.assertIn("content-99", export_data)

    def test_exporter_exports_location_listing_images_and_enqueues_files(self) -> None:
        class ListingImageClient(FakeClient):
            def iter_collection(self, path: str, params=None):
                self.collection_calls.append((path, params))
                if path == "/content":
                    return []
                if path == "/comments":
                    return []
                if path == "/locations":
                    return [["location-1"]]
                return []

            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/locations/location-1": {"uuid": "location-1"},
                    "/locations/location-1/listing_images": {
                        "items": [
                            {"uuid": "file-1", "url": "https://api.metropublisher.com/123/files/file-1"},
                            {"uuid": "file-2", "url": "https://api.metropublisher.com/123/files/file-2"},
                        ]
                    },
                    "/locations/location-1/tags": {"items": []},
                    "/files/file-1": {
                        "uuid": "file-1",
                        "filename": "one.jpg",
                        "download_url": "https://cdn.example.com/one.jpg",
                    },
                    "/files/file-2": {
                        "uuid": "file-2",
                        "filename": "two.jpg",
                        "download_url": "https://cdn.example.com/two.jpg",
                    },
                    "/files/file-1/tags": {"items": []},
                    "/files/file-2/tags": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=ListingImageClient(), output_dir=tmp_path)
            export_path = exporter.export()
            export_data = export_path.read_text(encoding="utf-8")
            self.assertIn('"location_listing_images"', export_data)
            self.assertIn("file-1", export_data)
            self.assertIn("file-2", export_data)

    def test_exporter_follows_location_coupon_image_url(self) -> None:
        class CouponImageClient(FakeClient):
            def iter_collection(self, path: str, params=None):
                self.collection_calls.append((path, params))
                if path == "/content":
                    return []
                if path == "/comments":
                    return []
                if path == "/locations":
                    return [["location-1"]]
                return []

            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/locations/location-1": {
                        "uuid": "location-1",
                        "coupon_img_url": "https://api.metropublisher.com/123/files/file-coupon/download/1",
                    },
                    "/locations/location-1/listing_images": {"items": []},
                    "/locations/location-1/tags": {"items": []},
                    "/files/file-coupon": {
                        "uuid": "file-coupon",
                        "filename": "coupon.jpg",
                        "download_url": "https://cdn.example.com/coupon.jpg",
                    },
                    "/files/file-coupon/tags": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=CouponImageClient(), output_dir=tmp_path)
            export_path = exporter.export()
            export_data = export_path.read_text(encoding="utf-8")
            self.assertIn("file-coupon", export_data)

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
                    "/content/content-1/related_links": {"items": []},
                    "/content/content-1/tags": {"items": []},
                    "/content/content-1/slots": {"items": []},
                    "/content/content-2": {"uuid": "content-2", "content_type": "article"},
                    "/content/content-2/related_links": {"items": []},
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

    def test_exporter_limit_is_separate_for_comments_too(self) -> None:
        class SeparateCommentLimitClient(FakeClient):
            def iter_collection(self, path: str, params=None):
                self.collection_calls.append((path, params))
                if path == "/content":
                    return []
                if path == "/comments":
                    return [
                        ["comment-1", "2026-01-02T00:00:00"],
                        ["comment-2", "2026-01-03T00:00:00"],
                    ]
                if path == "/locations":
                    return []
                return []

            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/comments/comment-1": {
                        "uuid": "comment-1",
                        "parent_type": "content",
                        "parent_uuid": "content-1",
                        "comment": "First",
                    },
                    "/comments/comment-2": {
                        "uuid": "comment-2",
                        "parent_type": "content",
                        "parent_uuid": "content-2",
                        "comment": "Second",
                    },
                    "/content/content-1": {"uuid": "content-1", "content_type": "article"},
                    "/content/content-1/related_links": {"items": []},
                    "/content/content-1/tags": {"items": []},
                    "/content/content-1/slots": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=SeparateCommentLimitClient(), output_dir=tmp_path, from_date="2026-01-01", limit=1)
            export_path = exporter.export()
            export_data = export_path.read_text(encoding="utf-8")
            self.assertIn("comment-1", export_data)
            self.assertNotIn("comment-2", export_data)

    def test_exporter_exports_related_links_and_enqueues_content_targets(self) -> None:
        class RelatedLinkClient(FakeClient):
            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/content/content-1": {"uuid": "content-1", "content_type": "article"},
                    "/content/content-1/related_links": {
                        "items": [
                            {"type": "url", "ord": 0, "text": "External", "link_url": "https://example.com"},
                            {"type": "content", "ord": 1, "uuid": "content-99", "url": "https://api.metropublisher.com/123/content/content-99"},
                            {"type": "location", "ord": 2, "uuid": "location-77", "url": "https://api.metropublisher.com/123/locations/location-77"},
                        ]
                    },
                    "/content/content-1/tags": {"items": []},
                    "/content/content-1/slots": {"items": []},
                    "/content/content-99": {"uuid": "content-99", "content_type": "article"},
                    "/content/content-99/related_links": {"items": []},
                    "/content/content-99/tags": {"items": []},
                    "/content/content-99/slots": {"items": []},
                    "/locations/location-77": {"uuid": "location-77"},
                    "/locations/location-77/tags": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=RelatedLinkClient(), output_dir=tmp_path, from_date="2026-01-01")
            export_path = exporter.export()
            export_data = export_path.read_text(encoding="utf-8")
            self.assertIn('"related_links"', export_data)
            self.assertIn("content-99", export_data)
            self.assertIn("location-77", export_data)

    def test_exporter_adds_tag_categories_with_uuid_title_url_and_tag_uuid(self) -> None:
        class TagCategoryClient(FakeClient):
            def iter_collection(self, path: str, params=None):
                self.collection_calls.append((path, params))
                if path == "/content":
                    return [["content-1", "article", "2026-01-02T00:00:00", "2026-01-01T00:00:00"]]
                if path == "/locations":
                    return []
                if path == "/tags/tag-1/categories":
                    return [
                        ["cat-1", "People", "https://api.metropublisher.com/123/tags/categories/cat-1"],
                        ["cat-2", "Writers", "https://api.metropublisher.com/123/tags/categories/cat-2"],
                    ]
                return []

            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/content/content-1": {"uuid": "content-1", "content_type": "article"},
                    "/content/content-1/related_links": {"items": []},
                    "/content/content-1/tags": {"items": [{"uuid": "tag-1", "predicate": "describes", "title": "Tag 1"}]},
                    "/content/content-1/slots": {"items": []},
                    "/tags/tag-1": {"uuid": "tag-1", "title": "Tag 1"},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=TagCategoryClient(), output_dir=tmp_path, from_date="2026-01-01")
            export_path = exporter.export()
            export_data = export_path.read_text(encoding="utf-8")
            self.assertIn('"categories"', export_data)
            self.assertIn("cat-1", export_data)
            self.assertIn("People", export_data)
            self.assertIn("https://api.metropublisher.com/123/tags/categories/cat-1", export_data)
            self.assertIn('"tag_uuid": "tag-1"', export_data)

    def test_exporter_follows_all_content_image_reference_fields(self) -> None:
        class ImageFieldClient(FakeClient):
            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/content/content-1": {
                        "uuid": "content-1",
                        "content_type": "review_book",
                        "book_image_url": "https://api.metropublisher.com/123/files/file-book/download/1",
                        "product_image_uuid": "file-product",
                        "feature_thumb_url": "https://api.metropublisher.com/123/files/file-feature-thumb/download/1",
                        "thumb_url": "https://api.metropublisher.com/123/files/file-thumb/download/1",
                    },
                    "/content/content-1/related_links": {"items": []},
                    "/content/content-1/tags": {"items": []},
                    "/content/content-1/slots": {"items": []},
                    "/files/file-book": {
                        "uuid": "file-book",
                        "filename": "book.jpg",
                        "download_url": "https://cdn.example.com/book.jpg",
                    },
                    "/files/file-book/tags": {"items": []},
                    "/files/file-product": {
                        "uuid": "file-product",
                        "filename": "product.jpg",
                        "download_url": "https://cdn.example.com/product.jpg",
                    },
                    "/files/file-product/tags": {"items": []},
                    "/files/file-feature-thumb": {
                        "uuid": "file-feature-thumb",
                        "filename": "feature-thumb.jpg",
                        "download_url": "https://cdn.example.com/feature-thumb.jpg",
                    },
                    "/files/file-feature-thumb/tags": {"items": []},
                    "/files/file-thumb": {
                        "uuid": "file-thumb",
                        "filename": "thumb.jpg",
                        "download_url": "https://cdn.example.com/thumb.jpg",
                    },
                    "/files/file-thumb/tags": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=ImageFieldClient(), output_dir=tmp_path, from_date="2026-01-01")
            export_path = exporter.export()
            export_data = export_path.read_text(encoding="utf-8")
            self.assertIn("file-book", export_data)
            self.assertIn("file-product", export_data)
            self.assertIn("file-feature-thumb", export_data)
            self.assertIn("file-thumb", export_data)

    def test_exporter_omits_deprecated_scalar_buy_url_fields(self) -> None:
        class DeprecatedBuyFieldClient(FakeClient):
            def get_json(self, path: str, params=None, ok_statuses=(200,)):
                mapping = {
                    "/content/content-1": {
                        "uuid": "content-1",
                        "content_type": "review_book",
                        "book_buy_url": "https://example.com/book",
                        "book_buy_link_text": "Buy book",
                        "book_buy_urls": [{"url": "https://example.com/book", "link_text": "Buy book"}],
                        "product_buy_url": "https://example.com/product",
                        "product_buy_link_text": "Buy product",
                        "product_buy_urls": [{"url": "https://example.com/product", "link_text": "Buy product"}],
                        "album_buy_url": "https://example.com/album",
                        "album_buy_link_text": "Buy album",
                        "album_buy_urls": [{"url": "https://example.com/album", "link_text": "Buy album"}],
                    },
                    "/content/content-1/related_links": {"items": []},
                    "/content/content-1/tags": {"items": []},
                    "/content/content-1/slots": {"items": []},
                }
                return mapping.get(path, {"items": []})

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            exporter = Exporter(client=DeprecatedBuyFieldClient(), output_dir=tmp_path, from_date="2026-01-01")
            export_path = exporter.export()
            export_data = json.loads(export_path.read_text(encoding="utf-8"))
            content = export_data["content"]["content-1"]
            self.assertIn("book_buy_urls", content)
            self.assertIn("product_buy_urls", content)
            self.assertIn("album_buy_urls", content)
            self.assertNotIn("book_buy_url", content)
            self.assertNotIn("book_buy_link_text", content)
            self.assertNotIn("product_buy_url", content)
            self.assertNotIn("product_buy_link_text", content)
            self.assertNotIn("album_buy_url", content)
            self.assertNotIn("album_buy_link_text", content)
