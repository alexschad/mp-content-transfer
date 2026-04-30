# MetroPublisher Content Transfer Details

This document explains how the exporter and importer work, what their responsibilities are, and which functions implement each step.

## 1. Exporter

The exporter lives in `src/exporter.py`.

Its job is to:
- select top-level seed objects from the source instance
- follow supported links between objects
- download file binaries
- write a self-contained bundle made of:
  - `export.json`
  - `files/`

### Exporter entry point

The main public method is:

- `Exporter.export()`

This function is the top-level orchestration method. It does the following:

1. Loads an existing bundle if `--resume` is active, otherwise creates a new manifest.
2. Creates a `GraphState` queue.
3. Seeds the queue with top-level:
   - content
   - comments
   - locations
4. Processes the queue until empty.
5. Saves `export.json` after each successfully exported item.
6. On API failure, saves a checkpoint and re-raises the error.

The queue is breadth-first and deduplicated through `GraphState.enqueue()` in `src/types.py`.

### Export manifest creation and resume

These helper functions live in `src/manifest.py`:

- `create_manifest(...)`
- `load_manifest_if_exists(...)`
- `save_bundle(...)`

`Exporter._load_or_create_manifest()` decides whether to:
- reuse an existing `export.json`
- or create a new manifest structure

This is what makes export resume possible.

### Seed selection

The exporter begins with three seed functions:

- `Exporter._seed_content()`
- `Exporter._seed_comments()`
- `Exporter._seed_locations()`

These functions call `MPClient.iter_collection()` from `src/client.py`.

They build query params such as:
- `fields`
- `order`
- `created`

The `created` filter is built by:

- `Exporter._created_period_filter()`

This function converts optional `from_date` and `to_date` values into the MetroPublisher period syntax:
- `from_`
- `_to`
- `from_to`

The top-level limit is enforced by:

- `Exporter._limit_reached()`

The limit applies separately to:
- content seeds
- comment seeds
- location seeds

### Queue-driven graph traversal

The exporter uses `GraphState` from `src/types.py`.

`GraphState` has:
- `seen`
- `queue`
- `enqueue(...)`

This gives the exporter two important properties:
- no duplicate work for the same `(resource_type, uuid)`
- no infinite traversal loops when objects link back to each other

`Exporter.export()` repeatedly pops one queue item and dispatches based on its type:
- `content`
- `comment`
- `location`
- `tag`
- `file`

Before dispatching, it checks:

- `Exporter._already_exported()`

That prevents re-exporting objects that are already in the manifest, especially important during resume.

### Exporting content

Implemented by:

- `Exporter._export_content()`

This function:
1. fetches `/content/{uuid}`
2. stores the full content payload in `manifest["content"]`
3. enqueues direct image/file references
4. enqueues linked location references
5. enqueues roundup-linked content and locations
6. stores roundup relationship data
7. exports related links
8. exports tags
9. exports slots and slot media

Direct file references are taken from fields such as:
- `teaser_image_uuid`
- `header_image_uuid`
- `feature_image_uuid`
- and their URL equivalents

Roundup relationships are stored under:
- `relationships.roundups.content_to_locations`
- `relationships.roundups.content_to_content`

### Exporting related links

Implemented by:

- `Exporter._export_related_links()`

This function:
1. fetches `/content/{uuid}/related_links`
2. stores the raw related-link items in:
   - `relationships.related_links[content_uuid]`
3. follows linked objects when the link type is:
   - `content`
   - `location`

URL-type related links are stored but not traversed.

### Exporting locations

Implemented by:

- `Exporter._export_location()`

This function:
1. fetches `/locations/{uuid}`
2. stores the payload in `manifest["locations"]`
3. enqueues direct file references such as thumb and coupon image
4. exports listing images
5. exports location tags

### Exporting location listing images

Implemented by:

- `Exporter._export_location_listing_images()`

This function:
1. fetches `/locations/{uuid}/listing_images`
2. stores the ordered items under:
   - `relationships.location_listing_images[location_uuid]`
3. enqueues each referenced file/image UUID

This is separate from the base location payload because the listing image list is its own resource.

### Exporting comments

Implemented by:

- `Exporter._export_comment()`

This function:
1. fetches `/comments/{uuid}`
2. stores the full payload in `manifest["comments"]`
3. looks at:
   - `parent_type`
   - `parent_uuid`
4. enqueues the parent object if it is:
   - a content item
   - a parent comment

This allows comment reply chains and content parents to be pulled into the bundle.

### Exporting tags

Implemented by:

- `Exporter._export_tag()`

This function:
1. fetches `/tags/{uuid}`
2. fetches the tag’s categories
3. stores the tag payload in `manifest["tags"]`
4. enqueues the tag’s feature image

Tag categories are exported by:

- `Exporter._export_tag_categories()`

This calls `/tags/{uuid}/categories` and stores lightweight records containing:
- `uuid`
- `title`
- `url`
- `tag_uuid`

The exporter does not recurse into categories.

### Exporting taggings

Implemented by:

- `Exporter._export_content_tags()`
- `Exporter._export_object_tags()`

`_export_object_tags()` is the shared implementation for:
- content tags
- location tags
- file tags

It:
1. fetches the tag list for an object
2. normalizes the tag UUID
3. enqueues the tag
4. appends a tagging record to:
   - `relationships.taggings`

Each tagging record stores:
- `object_type`
- `object_uuid`
- `tag_uuid`
- `predicate`

### Exporting slots and slot media

Implemented by:

- `Exporter._export_slots()`

This function:
1. fetches `/content/{uuid}/slots`
2. resolves each slot UUID
3. fetches:
   - `/content/{uuid}/slots/{slot_uuid}`
   - `/content/{uuid}/slots/{slot_uuid}/media`
4. stores both pieces as normalized slot records in:
   - `relationships.content_slots[content_uuid]`
5. enqueues file references found in slot media items

### Exporting files

Implemented by:

- `Exporter._export_file()`

This function:
1. fetches `/files/{uuid}`
2. stores metadata in `manifest["files"]`
3. determines a local bundle path under `files/`
4. skips download if the local binary already exists
5. otherwise downloads the binary through `MPClient.download()`
6. stores any download error in the file record instead of aborting the whole export
7. exports file tags

This makes export resilient to partially completed or resumed runs.

### API access during export

The exporter depends on `MPClient` in `src/client.py`.

Important methods are:
- `get_json(...)`
- `iter_collection(...)`
- `download(...)`

The client is responsible for:
- OAuth token retrieval
- request throttling
- retry/backoff
- HTTP request serialization

## 2. Importer

The importer lives in `src/importer.py`.

Its job is to:
- read `export.json`
- recreate supported resources in dependency-aware order
- avoid re-creating objects that already exist
- avoid overwriting non-empty list resources
- checkpoint progress so a failed import can continue later

### Importer entry point

The main public method is:

- `Importer.import_bundle()`

This method:
1. loads or creates import state
2. reconstructs summary counters from checkpoint state
3. reconstructs which resources were already created in prior runs
4. derives unique categories from exported tags
5. runs the import stages in order
6. checkpoints after every processed stage item

### Import checkpointing and resume

Import state is stored in:

- `import_state.json`

The persistence helpers live in `src/manifest.py`:
- `load_import_state_if_exists(...)`
- `save_import_state(...)`

The importer uses:
- `Importer._load_or_create_state()`
- `Importer._checkpoint()`
- `Importer._stage_map()`
- `Importer._mark_stage()`

The checkpoint state tracks:
- summary counters
- processed items per stage
- the fallback Import section UUID if one was created

This means a later run can continue without repeating completed work.

### Stage order

The importer uses a staged flow so dependencies exist before relationships are written.

The current order is:

1. file metadata
2. file binaries
3. tags
4. tag categories
5. tag-category links
6. non-event content
7. event content
8. slots
9. comments
10. locations
11. location listing images
12. related links
13. roundups
14. taggings

This is not just a convenience choice. It reflects the fact that:
- files are needed by tags/content/locations
- tags are needed before taggings
- content and locations are needed before list relationships
- list relationships are restored after base objects exist

### Fallback section handling

Implemented by:

- `Importer._ensure_import_section()`

If source content has a `section_uuid` that does not exist on the target:
- the importer looks for a target section titled `Import`
- if it does not exist, it creates one
- the created or discovered UUID is checkpointed in import state

If source content has no section at all:
- the importer leaves it unsectioned

### Importing file metadata

Implemented by:

- `Importer._import_files_metadata()`

For each exported file:
1. check `/files/{uuid}`
2. if it exists:
   - skip and record `skipped_existing`
3. otherwise:
   - `PUT /files/{uuid}` with normalized file metadata

The payload is built by:

- `_file_payload(...)`

### Importing file binaries

Implemented by:

- `Importer._import_file_binaries()`

This stage only uploads binaries for files that were created by the current import process.

It:
1. reads the file metadata stage result
2. skips files that were already present on target
3. reads the binary from `bundle.root / local_path`
4. `POST`s the raw bytes to `/files/{uuid}`

### Importing tags

Implemented by:

- `Importer._import_tags()`

For each tag:
1. check whether `/tags/{uuid}` already exists
2. normalize the feature image UUID
3. `PUT /tags/{uuid}` if missing

The tag payload is built by:

- `_tag_payload(...)`

### Importing categories

Exported categories are stored per tag, so the importer first collapses them into unique category objects.

This is done by:

- `_collect_categories(...)`

That function produces:
- one category object per category UUID
- a set of tag UUID memberships for that category

The actual creation stage is:

- `Importer._import_categories()`

Each missing category is created with:
- `PUT /tags/categories/{uuid}`

### Importing tag-category links

Implemented by:

- `Importer._import_tag_categories()`

This stage:
1. checks category existence
2. checks tag existence
3. reads existing category-tag assignments using:
   - `Importer._existing_category_tag_uuids()`
4. if the link is missing:
   - `POST /tags/categories/{category_uuid}/tags`

This avoids rewriting the full category tag list.

### Importing content

Implemented by:

- `Importer._import_content(..., event_only=False)`
- `Importer._import_content(..., event_only=True)`

The importer uses the same function twice:
- once for article-like content
- once for events

That keeps the content creation logic shared, but still gives a controlled order.

The filtering is implemented by:

- `_sorted_content_items(...)`

The actual payload is created by:

- `_content_payload(...)`

This payload function also normalizes file UUID fields from URL fields where needed.

### Importing comments

Implemented by:

- `Importer._import_comments()`

Comments can depend on:
- parent content
- parent comments

So this stage works in repeated passes:
1. build a pending set of unprocessed comments
2. try to import any comment whose parent now exists
3. repeat until no pending comments remain
4. if a pass makes no progress, raise an error listing unresolved comments

The comment payload is normalized by:

- `_comment_payload(...)`

### Importing locations

Implemented by:

- `Importer._import_locations()`

This stage:
1. checks `/locations/{uuid}`
2. creates the location if missing
3. defers location-level list relationships to a later stage

The payload is built by:

- `_location_payload(...)`

### Restoring slots

Implemented by:

- `Importer._restore_slots()`

This stage only runs for content created during this import.

For each slot record it:
1. `PUT`s the slot metadata to `/content/{uuid}/slots/{slot_uuid}`
2. reads current slot media through:
   - `Importer._existing_slot_media_items()`
3. only if the current media list is empty:
   - `PUT /content/{uuid}/slots/{slot_uuid}/media`

This follows the “only write list PUTs into empty targets” rule.

### Restoring location listing images

Implemented by:

- `Importer._restore_location_listing_images()`

This stage:
1. normalizes the desired ordered list from exported relationship data
2. checks that:
   - the location exists
   - every file exists
3. reads the current target list through:
   - `Importer._existing_listing_image_items()`
4. only if the target list is empty:
   - `PUT /locations/{uuid}/listing_images`

If the target list is already non-empty:
- the importer skips the write

### Restoring related links

Implemented by:

- `Importer._restore_related_links()`

This stage:
1. reads exported related links for a content object
2. normalizes them into PUT payload form using:
   - `Importer._normalize_related_link_for_put()`
3. verifies content/location targets exist
4. reads current target related links through:
   - `Importer._existing_related_link_items()`
5. only if the current target list is empty:
   - `PUT /content/{uuid}/related_links`

Normalized write payload items look like:
- `{"type": "content", "target_uuid": "..."}`
- `{"type": "location", "target_uuid": "..."}`
- `{"type": "url", "link_url": "...", "text": "..."}`

### Restoring roundups

Implemented by:

- `Importer._restore_roundups()`

This stage only runs for content created during the import.

It:
1. collects roundup content/location relationships from the manifest
2. checks that all linked targets exist
3. `PUT`s only the roundup fields back to `/content/{uuid}`

This is intentionally narrower than re-sending the full content payload.

### Restoring taggings

Implemented by:

- `Importer._restore_taggings()`

For each exported tagging:
1. check that the tagged object exists
2. check that the tag exists
3. recreate the relation with:
   - `PUT /tags/{tag_uuid}/{predicate}/{object_uuid}`

This is a deferred relationship stage because both sides have to exist first.

### Summary reconstruction

When resuming an import, the importer rebuilds its state from disk using:

- `_summary_from_state(...)`
- `_created_sets_from_state(...)`

These functions reconstruct:
- the visible summary counters
- the set of resources created in previous attempts

That is what allows later stages like:
- slots
- roundups
- related links

to know whether they are allowed to modify a parent resource.

## Export and Import Safety Rules

The tool uses a few important safety rules throughout both directions.

### 1. Existence checks before creation

Before creating a resource, the importer checks whether that UUID already exists.

If it does:
- the resource is not recreated
- the state is checkpointed as skipped

### 2. Resume state is persisted

Both directions support resume:
- export checkpoints into `export.json`
- import checkpoints into `import_state.json`

### 3. List PUTs are conservative

For list-style replacement routes, the importer only writes when the current target list is empty.

This currently applies to:
- slot media
- location listing images
- related links

### 4. Relationship stages run after base objects

Many relationships are intentionally deferred until:
- files exist
- tags exist
- content exists
- locations exist

This reduces broken references and avoids partial writes.

## Supporting Modules

### `src/client.py`

This module handles:
- OAuth access token retrieval
- rate limiting
- retry logic
- HTTP requests
- collection pagination

Exporter and importer both rely on:
- `get_json(...)`
- `iter_collection(...)`
- `put(...)`
- `post(...)`
- `resource_exists(...)`

### `src/manifest.py`

This module handles:
- export manifest creation
- export bundle persistence
- import state persistence

### `src/types.py`

This module contains:
- `Bundle`
- `ImportSummary`
- `GraphState`
- `ExportQueueItem`

`GraphState` is especially central to export traversal.
