"""
Compile NCBI's ``PMC-ids.csv`` crosswalk into an indexed SQLite database.

The resulting ``.sqlite`` file is consumed by ``2-parse-xml-to-jsonl.py``
via ``--pmc-ids-db`` to backfill missing PMCID/PMID/DOI values on papers
and their bibliography entries.

Get the CSV from:
    https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz

Usage (from project root):

```bash
uv run python scripts/pubmed/build-pmc-id-db.py \
  --csv data/pubmed/PMC-ids.csv \
  --db  data/pubmed/PMC-ids.sqlite
```

The build is a one-time step. Re-run it whenever you refresh the CSV from
NCBI. Expect ~2-5 minutes for the full ~10M-row file, most of which is
index creation.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from paper_parser.pubmed.pmc_id_map import PmcIdMap
from paper_parser.shared.utils import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compile PMC-ids.csv into an indexed SQLite crosswalk.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="Path to NCBI's PMC-ids.csv (the uncompressed file).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help=(
            "Output SQLite path. Defaults to --csv with the suffix replaced "
            "by .sqlite (e.g. PMC-ids.csv -> PMC-ids.sqlite)."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite --db if it already exists.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help=(
            "Log file path. By default, logs are written under "
            "logs/pubmed/<script>.log."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    script_name = Path(__file__).stem
    log_path = args.log_file or Path("logs/pubmed") / f"{script_name}.log"
    setup_logging(log_path)
    logger = logging.getLogger(__name__)
    logger.info("Starting build-pmc-id-db")
    logger.info(f"Arguments: {args}")

    if not args.csv.exists():
        raise FileNotFoundError(f"CSV not found: {args.csv}")

    db_path = args.db or args.csv.with_suffix(".sqlite")

    if db_path.exists() and not args.overwrite:
        logger.error(
            f"{db_path} already exists. Pass --overwrite to rebuild, or "
            f"delete it first."
        )
        raise SystemExit(1)

    PmcIdMap.build_from_csv(args.csv, db_path, overwrite=args.overwrite)
    logger.info(f"Done. DB written to {db_path}")


if __name__ == "__main__":
    main()
