"""
Extract per-paper metadata (licenses + publication date) from each PMC
OA XML file.

For each paper, writes one line to a JSONL output file:

    {
      "paper_id": "PMC7000113",
      "licenses": ["open-access", "http://..."],
      "pub_date": {"year": 2020, "month": 1, "day": 5}
    }

Usage (from project root):

```bash
uv run python -m scripts.pubmed.3-extract-split-metadata \
  --xml-dir data/pubmed/raw \
  --output data/pubmed/split_metadata.jsonl \
  --workers 20
```
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import re
from pathlib import Path

from tqdm import tqdm

from paper_parser.pubmed.parser import extract_pmc_id_from_path, extract_pub_date
from paper_parser.pubmed.utils import _local_tag, build_xml_tree
from paper_parser.shared.utils import setup_logging

logger = logging.getLogger(__name__)

XLINK_HREF = "{http://www.w3.org/1999/xlink}href"

_URL_RE = re.compile(
    r"https?://[^\s<>\"'()\[\]{}]*[A-Za-z0-9]",
    re.IGNORECASE,
)

_NOISE_NAMESPACES = (
    "http://www.w3.org",
    "http://purl.org",
    "http://web.resource.org",
)
_NOISE_URL_PREFIXES = (
    "http://www.w3.org",
    "https://www.w3.org",
    "http://purl.org",
    "https://purl.org",
    "http://web.resource.org",
    "https://web.resource.org",
)


def _is_noise_tag(tag) -> bool:
    if not isinstance(tag, str) or not tag.startswith("{"):
        return False
    ns = tag.split("}", 1)[0][1:]
    return any(ns.startswith(n) for n in _NOISE_NAMESPACES)


def _has_noise_ancestor(el, stop_at) -> bool:
    """True if ``el`` or any ancestor up to (but not including) ``stop_at``
    lives in an RDF / Dublin Core / CC-RDF namespace (see above)."""
    cur = el
    while cur is not None and cur is not stop_at:
        if _is_noise_tag(cur.tag):
            return True
        cur = cur.getparent()
    return False


def _extract_urls(s: str | None) -> list[str]:
    """Find every URL in ``s`` using ``_URL_RE``.

    Works uniformly on XML attribute values and on free-text snippets:
    because the regex anchors on ``h`` and on a trailing alphanumeric,
    wrappers around the URL in the source get peeled off automatically.
    URLs whose host is on the RDF/DC/CC-RDF deny-list are dropped.
    """
    if not s:
        return []
    urls: list[str] = []
    for m in _URL_RE.finditer(s):
        url = m.group(0)
        if any(url.startswith(p) for p in _NOISE_URL_PREFIXES):
            continue
        urls.append(url)
    return urls


def extract_metadata(xml_path: str) -> dict:
    """Return ``{'paper_id': ..., 'licenses': [...], 'pub_date': {...}}``.

    See the module docstring for the shape of each field. The XML tree
    is built once and passed to ``extract_pub_date`` to avoid reparsing.
    """
    tree = build_xml_tree(xml_path)
    root = tree.getroot()
    pmc_id = extract_pmc_id_from_path(xml_path).value

    perm_nodes = root.xpath("front/article-meta/permissions") or root.xpath(
        ".//*[local-name()='permissions']"
    )

    licenses: list[str | None] = []
    for perm in perm_nodes:
        for el in perm.iter():
            if _has_noise_ancestor(el, perm):
                continue

            tag = _local_tag(el)

            if tag == "license":
                licenses.append(el.attrib.get("license-type"))
                licenses.extend(_extract_urls(el.attrib.get(XLINK_HREF)))

            elif tag == "license_ref":
                licenses.extend(_extract_urls(el.text))

            elif tag in ("ext-link", "uri"):
                licenses.extend(_extract_urls(el.attrib.get(XLINK_HREF)))
                licenses.extend(_extract_urls(el.attrib.get("href")))

            for piece in (el.text, el.tail):
                licenses.extend(_extract_urls(piece))

    licenses = list(set(s.strip() for s in licenses if s and s.strip()))

    # Mirror the parsing pipeline: a malformed ``<pub-date>`` should not
    # fail the whole record, it just produces ``None``.
    try:
        pub_date = extract_pub_date(tree)
    except Exception:
        pub_date = None
    pub_date_dict = pub_date.model_dump() if pub_date is not None else None

    return {"paper_id": pmc_id, "licenses": licenses, "pub_date": pub_date_dict}


def _process_single_xml(xml_path: str) -> tuple[str, str | None, str | None]:
    try:
        record = extract_metadata(xml_path)
        return xml_path, json.dumps(record, ensure_ascii=False), None
    except Exception as e:
        return xml_path, None, str(e)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract per-paper metadata (licenses + pub_date) from PMC OA XML files."
    )
    parser.add_argument(
        "--xml-dir",
        type=Path,
        default=Path("data/pubmed/extracted"),
        help="Base directory containing XML files (default: data/pubmed/extracted)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/pubmed/licenses.jsonl"),
        help="Output JSONL file (default: data/pubmed/licenses.jsonl)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)",
    )
    parser.add_argument(
        "--max-tasks-per-child",
        type=int,
        default=2000,
        help="Recycle workers after this many files to bound memory (default: 2000)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug mode: only process the first 1000 XML files",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file path (default: logs/pubmed/<script>.log)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    script_name = Path(__file__).stem
    log_path = args.log_file or Path("logs/pubmed") / f"{script_name}.log"
    setup_logging(log_path)
    logger.info("Starting extract-split-metadata")
    logger.info(f"Arguments: {args}")

    base = args.xml_dir.resolve()
    xml_filepaths = sorted(str(p) for p in base.rglob("*.xml"))
    if not xml_filepaths:
        logger.warning(f"No XML files under {base}")
        return

    if args.debug:
        xml_filepaths = xml_filepaths[:1000]
        logger.info("Debug mode: only processing first 1000 XML files")

    logger.info(f"Found {len(xml_filepaths)} XML files; writing to {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    skipped = 0
    with open(args.output, "w", encoding="utf-8") as f:
        if args.workers <= 1:
            for xml_filepath in tqdm(xml_filepaths):
                _, line, error = _process_single_xml(xml_filepath)
                if line is not None:
                    f.write(line + "\n")
                    written += 1
                else:
                    skipped += 1
                    logger.warning(f"Skip #{skipped}: {xml_filepath}: {error}")
        else:
            logger.info(
                f"Using {args.workers} worker processes "
                f"(maxtasksperchild={args.max_tasks_per_child or 'unbounded'})"
            )
            ctx = mp.get_context("fork")
            with ctx.Pool(
                processes=args.workers,
                maxtasksperchild=args.max_tasks_per_child or None,
            ) as pool:
                iterator = pool.imap_unordered(
                    _process_single_xml,
                    iter(xml_filepaths),
                    chunksize=50,
                )
                for xml_filepath, line, error in tqdm(iterator, total=len(xml_filepaths)):
                    if line is not None:
                        f.write(line + "\n")
                        written += 1
                    else:
                        skipped += 1
                        logger.warning(f"Skip #{skipped}: {xml_filepath}: {error}")

    logger.info(f"Wrote {written} records to {args.output} ({skipped} skipped)")


if __name__ == "__main__":
    main()
