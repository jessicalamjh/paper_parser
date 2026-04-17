"""
Extract PMC OA bulk XML archives from raw/ into extracted/.

Discovers baseline .tar.gz archives under base_dir/raw/{oa_comm,oa_noncomm,oa_other}/
and extracts each into base_dir/extracted/{oa_comm,oa_noncomm,oa_other}/.
Does not use the network.

Usage (from project root):

```bash
uv run python scripts/pmcoa/extract-bulk-release-xml.py
uv run python scripts/pmcoa/extract-bulk-release-xml.py --base-dir data/pmcoa
```
"""

from __future__ import annotations

import argparse
import logging
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from paper_parser.shared.utils import setup_logging


def extract_tar_gz(filepath: Path, out_path: Path):
    """Extract a .tar.gz file to the given directory."""
    out_path.mkdir(parents=True, exist_ok=True)
    with tarfile.open(filepath, "r:gz") as tar:
        tar.extractall(out_path)



def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract PMC OA bulk XML archives from raw/ into extracted/."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/pmcoa/raw"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pmcoa/extracted"),
    )
    parser.add_argument(
        "--n-workers",
        type=int,
        default=2,
        help="Number of parallel extraction workers (default: 2)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    script_name = Path(__file__).stem
    log_path = Path("logs/pmcoa") / f"{script_name}.log"
    setup_logging(log_path)
    logger = logging.getLogger(__name__)
    logger.info("Starting PMC OA bulk XML extraction")
    logger.info(f"Arguments: {args}")

    if not args.input_dir.is_dir():
        logger.error(f"Input directory {args.input_dir} does not exist")
        return

    extract_futures = []
    with ThreadPoolExecutor(max_workers=args.n_workers) as executor:
        for input_subdir in args.input_dir.iterdir():
            if not input_subdir.is_dir():
                logger.info(f"Skipping non-directory: {input_subdir}")
                continue
            output_subdir = args.output_dir / input_subdir.name
            output_subdir.mkdir(parents=True, exist_ok=True)

            for input_filepath in input_subdir.iterdir():
                if not input_filepath.is_file():
                    logger.info(f"Skipping non-file: {input_filepath}")
                    continue
                if not input_filepath.name.endswith(".tar.gz") or "baseline" not in input_filepath.name:
                    logger.info(f"Skipping non-baseline tar.gz file: {input_filepath}")
                    continue
                output_filepath = output_subdir / input_filepath.name
                extract_futures.append(
                    executor.submit(extract_tar_gz, input_filepath, output_filepath)
                )

    if extract_futures:
        logger.info("Waiting for extractions to complete...")
        for future in as_completed(extract_futures):
            future.result()

    logger.info("Done.")


if __name__ == "__main__":
    main()
