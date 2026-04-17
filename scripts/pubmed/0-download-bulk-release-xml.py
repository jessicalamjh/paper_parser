"""
Download PMC OA bulk XML releases from NCBI FTP server.

Example run commands (from project root):

```bash
uv run python -m scripts.pmcoa.download-bulk-release-xml
uv run python -m scripts.pmcoa.download-bulk-release-xml --skip-existing
```
"""

from __future__ import annotations

import argparse
import ftplib
import tarfile
import logging
from pathlib import Path

from tqdm import tqdm

from paper_parser.shared.utils import setup_logging


FTP_HOST = "ftp.ncbi.nlm.nih.gov"
FTP_BASE = "pub/pmc/deprecated/oa_bulk"
OA_SUBSET_NAMES = [
    "oa_comm",
    "oa_noncomm",
    "oa_other",
]


def get_remote_filepaths(ftp: ftplib.FTP, oa_subdir: str) -> list[str]:
    """List .tar.gz files in oa_*/xml/ subdirectory."""
    ftp.cwd(oa_subdir)
    remote_filepaths = []
    ftp.retrlines("NLST", remote_filepaths.append)
    return [x for x in remote_filepaths if x.endswith(".tar.gz") and "baseline" in x]


def download_file(ftp: ftplib.FTP, remote_filepath: str, output_filepath: Path):
    """Download a single file from FTP with progress bar."""
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    try:
        file_size = ftp.size(remote_filepath)
    except (ftplib.error_perm, OSError):
        file_size = None
    with open(output_filepath, "wb") as f:
        with tqdm(total=file_size, unit="B", unit_scale=True, desc=remote_filepath) as pbar:
            ftp.retrbinary(
                f"RETR {remote_filepath}",
                lambda b: (f.write(b), pbar.update(len(b))),
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download PMC OA bulk XML releases from NCBI FTP."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pmcoa/raw"),
        help="Where to save downloaded files (default: data/pmcoa/raw)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip downloading files that have already been downloaded",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug mode: download only a few files",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    script_name = Path(__file__).stem
    log_path = Path("logs/pmcoa") / f"{script_name}.log"
    setup_logging(log_path)
    logger = logging.getLogger(__name__)
    logger.info("Starting PMC OA bulk XML download")
    logger.info(f"Arguments: {args}")

    if not Path(args.output_dir).is_dir():
        logger.info(f"Creating output directory: {args.output_dir}")
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"Connecting to {FTP_HOST}...")
    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login()
        for oa_subset_name in OA_SUBSET_NAMES:
            logger.info(f"\n--- {oa_subset_name} ---")
            oa_subdir = FTP_BASE / oa_subset_name
            remote_filepaths = get_remote_filepaths(ftp, oa_subdir)
            logger.info(f"Found {len(remote_filepaths)} files to download")

            if args.debug:
                remote_filepaths = remote_filepaths[:5]
                logger.info("Debug mode: only downloading the first files")

            output_subdir = args.output_dir / oa_subset_name
            for remote_filepath in tqdm(remote_filepaths, desc=oa_subset_name, unit="file"):
                output_filepath = output_subdir / remote_filepath.name
                if args.skip_existing and output_filepath.exists():
                    logger.info(f"  Skip (exists): {output_filepath}")
                    continue
                download_file(ftp, remote_filepath, output_filepath)

    logger.info("\nDone.")


if __name__ == "__main__":
    main()
