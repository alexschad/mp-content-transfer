from __future__ import annotations

import argparse
from pathlib import Path

from .client import MPClient
from .config import load_settings
from .exporter import Exporter
from .importer import Importer
from .manifest import load_bundle


def main() -> None:
    parser = argparse.ArgumentParser(prog="mp-content-transfer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export")
    export_parser.add_argument("--from-date", required=True)
    export_parser.add_argument("--output", required=True)
    export_parser.add_argument("--limit", type=int)
    export_parser.add_argument("--resume", action="store_true")

    import_parser = subparsers.add_parser("import")
    import_parser.add_argument("--input", required=True)

    args = parser.parse_args()
    settings = load_settings()

    if args.command == "export":
        client = MPClient.create(settings.source, settings.retry, settings.auth_provider)
        exporter = Exporter(
            client=client,
            output_dir=Path(args.output),
            from_date=args.from_date,
            limit=args.limit if args.limit is not None else settings.export_limit,
            resume=args.resume,
        )
        manifest_path = exporter.export()
        print(f"Exported bundle to {manifest_path}")
        return

    client = MPClient.create(settings.target, settings.retry, settings.auth_provider)
    bundle = load_bundle(Path(args.input))
    summary = Importer(client=client, bundle=bundle).import_bundle()
    print(
        "Import summary: "
        f"created={summary.created} "
        f"skipped_existing={summary.skipped_existing} "
        f"relationship_created={summary.relationship_created} "
        f"relationship_skipped={summary.relationship_skipped} "
        f"import_section_routed={summary.import_section_routed}"
    )


if __name__ == "__main__":
    main()
