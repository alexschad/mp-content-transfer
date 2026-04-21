# MetroPublisher Content Transfer

Plain Python script for exporting MetroPublisher content bundles from one instance and importing them into another.

## Features

- exports articles, events, and locations
- follows direct tags and image/file dependencies
- follows `roundup_locations` and `roundup_content_targets`
- imports conservatively and skips resources that already exist
- uses `.env` for source/target credentials
- throttles requests and retries transient failures

## Usage

1. Copy `.env.example` to `.env` and fill in the source and target credentials.
   `MP_EXPORT_LIMIT` can be set there to cap top-level exported seed items by default.
   Passing `--limit` on the command line overrides the `.env` value for that run.
2. Run the script directly from the repo root.

```bash
python3 mp_content_transfer.py --help
```

3. Export:

```bash
python3 mp_content_transfer.py export --from-date 2026-01-01 --output ./bundle --limit 100
```

4. Import:

```bash
python3 mp_content_transfer.py import --input ./bundle
```
