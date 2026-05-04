from __future__ import annotations

import argparse
from pathlib import Path

from .client import MPClient
from .config import load_settings
from .exporter import Exporter
from .importer import Importer
from .manifest import load_bundle


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mp-content-transfer",
        description=(
            "Export MetroPublisher data from a source instance into an export bundle, "
            "or import a previously exported bundle into a target instance."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Date format:\n"
            "  YYYY-MM-DD\n\n"
            "Bundle layout:\n"
            "  export writes an output directory containing export.json and files/\n"
            "  import reads that same bundle directory and resumes from import_state.json when present\n\n"
            "Examples:\n"
            "  python3 mp_content_transfer.py export --from-date 2026-01-01 --output ./bundle\n"
            "  python3 mp_content_transfer.py export --from-date 2026-01-01 --to-date 2026-01-31 --output ./bundle --resume\n"
            "  python3 mp_content_transfer.py import --input ./bundle"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser(
        "export",
        help="Export content, comments, locations, tags, and files into a bundle directory.",
        description=(
            "Export a MetroPublisher graph from the source instance configured in .env.\n"
            "Top-level seed collections are filtered by optional created-date bounds."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 mp_content_transfer.py export --output ./bundle\n"
            "  python3 mp_content_transfer.py export --from-date 2026-01-01 --output ./bundle\n"
            "  python3 mp_content_transfer.py export --from-date 2026-01-01 --to-date 2026-01-31 --output ./bundle --limit 25 --resume"
        ),
    )
    export_parser.add_argument(
        "--from-date",
        help="Lower created-date bound for top-level seed items.\nFormat: YYYY-MM-DD",
    )
    export_parser.add_argument(
        "--to-date",
        help="Upper created-date bound for top-level seed items.\nFormat: YYYY-MM-DD",
    )
    export_parser.add_argument(
        "--output",
        required=True,
        help="Bundle directory to create or reuse.\nExample: ./bundle",
    )
    export_parser.add_argument(
        "--limit",
        type=int,
        help=(
            "Maximum number of top-level seed items per type.\n"
            "Applies separately to content, comments, and locations.\n"
            "Overrides MP_EXPORT_LIMIT from .env for this run."
        ),
    )
    export_parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Resume an earlier export in the same output directory.\n"
            "Already exported items found in export.json are skipped."
        ),
    )

    import_parser = subparsers.add_parser(
        "import",
        help="Import a previously exported bundle into the target instance.",
        description=(
            "Import an export bundle into the target instance configured in .env.\n"
            "The importer resumes automatically when import_state.json is present."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 mp_content_transfer.py import --input ./bundle\n\n"
            "Resume:\n"
            "  Run the same command again to continue from import_state.json."
        ),
    )
    import_parser.add_argument(
        "--input",
        required=True,
        help="Bundle directory to import.\nExample: ./bundle",
    )

    args = parser.parse_args()
    settings = load_settings()

    if args.command == "export":
        client = MPClient.create(settings.source, settings.retry, settings.auth_provider)
        exporter = Exporter(
            client=client,
            output_dir=Path(args.output),
            from_date=args.from_date,
            to_date=args.to_date,
            limit=args.limit if args.limit is not None else settings.export_limit,
            resume=args.resume,
        )
        export_path = exporter.export()
        print(f"Exported bundle to {export_path}")
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
