from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.importer import Importer
from src.client import ApiError
from src.types import Bundle


class FakeImportClient:
    def __init__(self) -> None:
        self.existing = {
            "/sections/source-section": False,
            "/content/existing-content": True,
            "/tags/tag-existing": True,
        }
        self.put_calls: list[tuple[str, dict | None]] = []
        self.patch_calls: list[tuple[str, dict | None]] = []
        self.post_calls: list[tuple[str, dict | bytes | None, dict | None]] = []
        self.section_rows = []
        self.category_tag_rows: dict[str, list[list[str]]] = {}
        self.listing_image_items: dict[str, list[dict[str, str]]] = {}
        self.related_link_items: dict[str, list[dict[str, str]]] = {}
        self.slot_media_items: dict[tuple[str, str], list[dict[str, str]]] = {}
        self.fail_on_put_paths: set[str] = set()
        self.urlname_conflicts: dict[str, int] = {}

    def resource_exists(self, path: str) -> bool:
        return self.existing.get(path, False)

    def put(self, path: str, json=None, ok_statuses=(200,)):
        if path in self.fail_on_put_paths:
            raise RuntimeError(f"boom on {path}")
        if path in self.urlname_conflicts and self.urlname_conflicts[path] > 0:
            self.urlname_conflicts[path] -= 1
            raise ApiError('PUT failed with 400: {"error": "bad_parameters", "error_info": {"urlname": "\'urlname\' must be unique for tags."}, "error_description": "One or more of your incoming parameters failed validation, see info for details"}')
        self.put_calls.append((path, json))
        if path.startswith("/sections/"):
            self.existing[path] = True
        if path.startswith("/content/"):
            self.existing[path] = True
        if path.startswith("/locations/"):
            self.existing[path] = True
        if path.startswith("/tags/categories/"):
            self.existing[path] = True
        if path.startswith("/tags/"):
            self.existing[path] = True
        if path.startswith("/files/"):
            self.existing[path] = True
        if path.startswith("/comments/"):
            self.existing[path] = True
        return None

    def patch(self, path: str, json=None, ok_statuses=(200,)):
        self.patch_calls.append((path, json))
        return None

    def post(self, path: str, json=None, data=None, headers=None, ok_statuses=(200, 201)):
        payload = json if json is not None else data
        self.post_calls.append((path, payload, headers))
        if path.endswith("/listing_images") and isinstance(json, dict) and json.get("uuid"):
            location_uuid = path.split("/")[2]
            self.listing_image_items.setdefault(location_uuid, []).append({"uuid": json["uuid"]})
        if path.endswith("/tags") and "/tags/categories/" in path and isinstance(json, dict) and json.get("tag_uuid"):
            category_uuid = path.split("/")[3]
            self.category_tag_rows.setdefault(category_uuid, []).append([json["tag_uuid"]])
        return None

    def iter_collection(self, path: str, params=None):
        if path == "/sections":
            yield from self.section_rows
            return
        if path.startswith("/tags/categories/") and path.endswith("/tags"):
            category_uuid = path.split("/")[3]
            yield from self.category_tag_rows.get(category_uuid, [])
            return
        yield from []

    def get_json(self, path: str, params=None, ok_statuses=(200,)):
        if path.startswith("/locations/") and path.endswith("/listing_images"):
            location_uuid = path.split("/")[2]
            return {"items": self.listing_image_items.get(location_uuid, [])}
        if path.startswith("/content/") and path.endswith("/related_links"):
            content_uuid = path.split("/")[2]
            return {"items": self.related_link_items.get(content_uuid, [])}
        if "/slots/" in path and path.endswith("/media"):
            parts = path.split("/")
            content_uuid = parts[2]
            slot_uuid = parts[4]
            return {"items": self.slot_media_items.get((content_uuid, slot_uuid), [])}
        return {"items": []}


def make_bundle(root: Path, manifest: dict) -> Bundle:
    base_manifest = {
        "files": {},
        "tags": {},
        "comments": {},
        "locations": {},
        "content": {},
        "relationships": {
            "taggings": [],
            "related_links": {},
            "location_listing_images": {},
            "content_slots": {},
            "roundups": {"content_to_locations": {}, "content_to_content": {}},
        },
    }
    base_manifest.update(manifest)
    return Bundle(root=root, manifest=base_manifest)


class ImporterTest(TestCase):
    def test_content_with_missing_section_goes_to_import(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "article",
                            "title": "New Content",
                            "section_uuid": "source-section",
                        }
                    }
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
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "article",
                            "title": "New Content",
                        }
                    }
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [call for call in client.put_calls if call[0] == "/content/new-content"]
            self.assertEqual(len(content_puts), 1)
            self.assertNotIn("section_uuid", content_puts[0][1])

    def test_content_import_maps_book_image_url_to_book_image_uuid(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "article",
                            "title": "New Content",
                            "book_image_url": "https://api.metropublisher.com/123/files/12345678-1234-1234-1234-123456789abc",
                        }
                    }
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [call for call in client.put_calls if call[0] == "/content/new-content"]
            self.assertEqual(len(content_puts), 1)
            self.assertEqual(content_puts[0][1].get("book_image_uuid"), "12345678-1234-1234-1234-123456789abc")
            self.assertNotIn("book_image_url", content_puts[0][1])

    def test_content_import_preserves_header_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "article",
                            "title": "New Content",
                            "header_code": "<script>console.log('hello');</script>",
                        }
                    }
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [call for call in client.put_calls if call[0] == "/content/new-content"]
            self.assertEqual(len(content_puts), 1)
            self.assertEqual(content_puts[0][1].get("header_code"), "<script>console.log('hello');</script>")

    def test_restore_roundups_puts_only_roundup_fields(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "new-content": {
                            "uuid": "new-content",
                            "urlname": "new-content",
                            "content_type": "roundup_content",
                            "title": "New Content",
                            "description": "should not be resent",
                        },
                        "other": {
                            "uuid": "other",
                            "urlname": "other",
                            "content_type": "article",
                            "title": "Other",
                        },
                    },
                    "relationships": {
                        "taggings": [],
                        "related_links": {},
                        "location_listing_images": {},
                        "content_slots": {},
                        "roundups": {"content_to_locations": {}, "content_to_content": {"new-content": [{"target_uuid": "other"}]}},
                    },
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [payload for path, payload in client.put_calls if path == "/content/new-content"]
            roundup_patches = [payload for path, payload in client.patch_calls if path == "/content/new-content"]
            self.assertEqual(len(content_puts), 1)
            self.assertEqual(len(roundup_patches), 1)
            self.assertEqual(content_puts[0].get("roundup_locations"), [])
            self.assertEqual(content_puts[0].get("roundup_content_targets"), [])
            self.assertEqual(set(roundup_patches[0].keys()), {"roundup_content_targets"})

    def test_imports_categories_and_tag_category_links(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "tags": {
                        "tag-1": {
                            "uuid": "tag-1",
                            "urlname": "tag-1",
                            "last_name_or_title": "Tag 1",
                            "categories": [
                                {
                                    "uuid": "category-1",
                                    "title": "People",
                                    "tag_uuid": "tag-1",
                                }
                            ],
                        }
                    }
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            self.assertIn(("/tags/categories/category-1", {"title": "People"}), client.put_calls)
            self.assertTrue(any(path == "/tags/categories/category-1/tags" and payload == {"tag_uuid": "tag-1"} for path, payload, _ in client.post_calls))

    def test_imports_location_listing_images(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            file_path = root / "files" / "file-1_one.jpg"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(b"img")
            bundle = make_bundle(
                root,
                {
                    "files": {
                        "file-1": {
                            "uuid": "file-1",
                            "filename": "one.jpg",
                            "mimetype": "image/jpeg",
                            "local_path": "files/file-1_one.jpg",
                        }
                    },
                    "locations": {
                        "location-1": {
                            "uuid": "location-1",
                            "title": "Location",
                            "urlname": "location",
                        }
                    },
                    "relationships": {
                        "taggings": [],
                        "related_links": {},
                        "location_listing_images": {
                            "location-1": [{"uuid": "file-1", "url": "https://api.metropublisher.com/123/files/file-1"}]
                        },
                        "content_slots": {},
                        "roundups": {"content_to_locations": {}, "content_to_content": {}},
                    },
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            self.assertIn(
                ("/locations/location-1/listing_images", {"items": [{"uuid": "file-1"}]}),
                client.put_calls,
            )

    def test_skips_listing_image_put_when_existing_order_matches(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            file_path = root / "files" / "file-1_one.jpg"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(b"img")
            bundle = make_bundle(
                root,
                {
                    "files": {
                        "file-1": {
                            "uuid": "file-1",
                            "filename": "one.jpg",
                            "mimetype": "image/jpeg",
                            "local_path": "files/file-1_one.jpg",
                        }
                    },
                    "locations": {
                        "location-1": {
                            "uuid": "location-1",
                            "title": "Location",
                            "urlname": "location",
                        }
                    },
                    "relationships": {
                        "taggings": [],
                        "related_links": {},
                        "location_listing_images": {
                            "location-1": [{"uuid": "file-1", "url": "https://api.metropublisher.com/123/files/file-1"}]
                        },
                        "content_slots": {},
                        "roundups": {"content_to_locations": {}, "content_to_content": {}},
                    },
                },
            )
            client = FakeImportClient()
            client.listing_image_items["location-1"] = [{"uuid": "file-1"}]
            Importer(client=client, bundle=bundle).import_bundle()
            listing_puts = [call for call in client.put_calls if call[0] == "/locations/location-1/listing_images"]
            self.assertEqual(listing_puts, [])

    def test_skips_listing_image_put_when_target_list_is_non_empty_and_different(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            file_path = root / "files" / "file-1_one.jpg"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(b"img")
            bundle = make_bundle(
                root,
                {
                    "files": {
                        "file-1": {
                            "uuid": "file-1",
                            "filename": "one.jpg",
                            "mimetype": "image/jpeg",
                            "local_path": "files/file-1_one.jpg",
                        }
                    },
                    "locations": {
                        "location-1": {
                            "uuid": "location-1",
                            "title": "Location",
                            "urlname": "location",
                        }
                    },
                    "relationships": {
                        "taggings": [],
                        "related_links": {},
                        "location_listing_images": {
                            "location-1": [{"uuid": "file-1", "url": "https://api.metropublisher.com/123/files/file-1"}]
                        },
                        "content_slots": {},
                        "roundups": {"content_to_locations": {}, "content_to_content": {}},
                    },
                },
            )
            client = FakeImportClient()
            client.listing_image_items["location-1"] = [{"uuid": "different-file"}]
            Importer(client=client, bundle=bundle).import_bundle()
            listing_puts = [call for call in client.put_calls if call[0] == "/locations/location-1/listing_images"]
            self.assertEqual(listing_puts, [])

    def test_skips_slot_media_put_when_target_list_is_non_empty(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "content-1": {
                            "uuid": "content-1",
                            "urlname": "content-1",
                            "content_type": "article",
                            "title": "Content 1",
                        }
                    },
                    "relationships": {
                        "taggings": [],
                        "related_links": {},
                        "location_listing_images": {},
                        "content_slots": {
                            "content-1": [
                                {
                                    "slot": {"uuid": "slot-1", "display": "gallery", "relevance": 1},
                                    "media": {"items": [{"uuid": "file-1"}]},
                                }
                            ]
                        },
                        "roundups": {"content_to_locations": {}, "content_to_content": {}},
                    },
                },
            )
            client = FakeImportClient()
            client.slot_media_items[("content-1", "slot-1")] = [{"uuid": "different-file"}]
            Importer(client=client, bundle=bundle).import_bundle()
            media_puts = [call for call in client.put_calls if call[0] == "/content/content-1/slots/slot-1/media"]
            self.assertEqual(media_puts, [])

    def test_imports_related_links_when_target_list_is_empty(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "content-1": {
                            "uuid": "content-1",
                            "urlname": "content-1",
                            "content_type": "article",
                            "title": "Content 1",
                        },
                        "content-2": {
                            "uuid": "content-2",
                            "urlname": "content-2",
                            "content_type": "article",
                            "title": "Content 2",
                        },
                    },
                    "locations": {
                        "location-1": {
                            "uuid": "location-1",
                            "title": "Location 1",
                            "urlname": "location-1",
                        }
                    },
                    "relationships": {
                        "taggings": [],
                        "related_links": {
                            "content-1": [
                                {"type": "content", "uuid": "content-2"},
                                {"type": "location", "uuid": "location-1"},
                                {"type": "url", "link_url": "https://example.com", "text": "Example"},
                            ]
                        },
                        "location_listing_images": {},
                        "content_slots": {},
                        "roundups": {"content_to_locations": {}, "content_to_content": {}},
                    },
                },
            )
            client = FakeImportClient()
            Importer(client=client, bundle=bundle).import_bundle()
            self.assertIn(
                (
                    "/content/content-1/related_links",
                    {
                        "items": [
                            {"type": "content", "target_uuid": "content-2"},
                            {"type": "location", "target_uuid": "location-1"},
                            {"type": "url", "link_url": "https://example.com", "text": "Example"},
                        ]
                    },
                ),
                client.put_calls,
            )

    def test_skips_related_links_put_when_target_list_is_non_empty(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "content-1": {
                            "uuid": "content-1",
                            "urlname": "content-1",
                            "content_type": "article",
                            "title": "Content 1",
                        },
                        "content-2": {
                            "uuid": "content-2",
                            "urlname": "content-2",
                            "content_type": "article",
                            "title": "Content 2",
                        },
                    },
                    "relationships": {
                        "taggings": [],
                        "related_links": {
                            "content-1": [
                                {"type": "content", "uuid": "content-2"},
                            ]
                        },
                        "location_listing_images": {},
                        "content_slots": {},
                        "roundups": {"content_to_locations": {}, "content_to_content": {}},
                    },
                },
            )
            client = FakeImportClient()
            client.related_link_items["content-1"] = [{"type": "url", "link_url": "https://existing.example"}]
            Importer(client=client, bundle=bundle).import_bundle()
            related_link_puts = [call for call in client.put_calls if call[0] == "/content/content-1/related_links"]
            self.assertEqual(related_link_puts, [])

    def test_resume_skips_already_processed_items(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "tags": {
                        "tag-1": {
                            "uuid": "tag-1",
                            "urlname": "tag-1",
                            "last_name_or_title": "Tag 1",
                        },
                        "tag-2": {
                            "uuid": "tag-2",
                            "urlname": "tag-2",
                            "last_name_or_title": "Tag 2",
                        },
                    }
                },
            )
            failing_client = FakeImportClient()
            failing_client.fail_on_put_paths.add("/tags/tag-2")
            with self.assertRaises(RuntimeError):
                Importer(client=failing_client, bundle=bundle).import_bundle()
            self.assertTrue((root / "import_state.json").exists())

            resumed_client = FakeImportClient()
            Importer(client=resumed_client, bundle=bundle).import_bundle()
            resumed_tag_puts = [path for path, _ in resumed_client.put_calls if path.startswith("/tags/")]
            self.assertNotIn("/tags/tag-1", resumed_tag_puts)
            self.assertIn("/tags/tag-2", resumed_tag_puts)

    def test_retries_tag_urlname_conflict_with_suffix(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "tags": {
                        "tag-1": {
                            "uuid": "tag-1",
                            "urlname": "tag-1",
                            "last_name_or_title": "Tag 1",
                        }
                    }
                },
            )
            client = FakeImportClient()
            client.urlname_conflicts["/tags/tag-1"] = 1
            Importer(client=client, bundle=bundle).import_bundle()
            tag_puts = [payload for path, payload in client.put_calls if path == "/tags/tag-1"]
            self.assertEqual(tag_puts[-1]["urlname"], "tag-1-1")

    def test_marks_tag_urlname_exists_after_too_many_conflicts(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "tags": {
                        "tag-1": {
                            "uuid": "tag-1",
                            "urlname": "tag-1",
                            "last_name_or_title": "Tag 1",
                        }
                    }
                },
            )
            client = FakeImportClient()
            client.urlname_conflicts["/tags/tag-1"] = 11
            Importer(client=client, bundle=bundle).import_bundle()
            state = (root / "import_state.json").read_text(encoding="utf-8")
            self.assertIn('"urlname_exists"', state)

    def test_retries_content_urlname_conflict_with_suffix(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "content": {
                        "content-1": {
                            "uuid": "content-1",
                            "urlname": "content-1",
                            "content_type": "article",
                            "title": "Content 1",
                        }
                    }
                },
            )
            client = FakeImportClient()
            client.urlname_conflicts["/content/content-1"] = 1
            Importer(client=client, bundle=bundle).import_bundle()
            content_puts = [payload for path, payload in client.put_calls if path == "/content/content-1"]
            self.assertEqual(content_puts[-1]["urlname"], "content-1-1")

    def test_retries_location_urlname_conflict_with_suffix(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bundle = make_bundle(
                root,
                {
                    "locations": {
                        "location-1": {
                            "uuid": "location-1",
                            "urlname": "location-1",
                            "title": "Location 1",
                        }
                    }
                },
            )
            client = FakeImportClient()
            client.urlname_conflicts["/locations/location-1"] = 1
            Importer(client=client, bundle=bundle).import_bundle()
            location_puts = [payload for path, payload in client.put_calls if path == "/locations/location-1"]
            self.assertEqual(location_puts[-1]["urlname"], "location-1-1")
