"""Shared utilities (e.g. logging setup) for scripts and packages."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from datetime import datetime

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    log_file: Path,
    *,
    level: int = logging.INFO,
    also_stream: bool = True,
    capture_warnings: bool = True,
    ) -> None:
    """Configure root logger: clear handlers, add timestamped file (and optional stderr).

    The actual logfile is prefixed with the current runtime timestamp, e.g.:
    logs/pmcoa/parse-xml-to-jsonl.log -> logs/pmcoa/20260310-153045-parse-xml-to-jsonl.log
    """
    base_path = Path(log_file)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file = base_path.with_name(f"{timestamp}-{base_path.name}")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    root = logging.getLogger()
    root.setLevel(level)

    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()

    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setFormatter(formatter)
    root.addHandler(fh)

    if also_stream:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(formatter)
        root.addHandler(sh)

    if capture_warnings:
        logging.captureWarnings(True)
