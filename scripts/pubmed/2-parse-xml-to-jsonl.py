"""
Run the PMC OA parser on all XML files under the extracted dirs and write papers to a JSONL file.

Each line of the output file is one parsed Paper as JSON (model_dump).

Usage (from project root):

```bash
uv run python scripts/pubmed/2-parse-xml-to-jsonl.py \
  --xml-dir data/pubmed/raw \
  --output data/pubmed/papers.jsonl \
  --spacy-model en_core_web_sm \
  --spacy-batch-size 32 \
  --spacy-n-process 1 \
  --workers 20 \
  --pmc-ids-db data/pubmed/PMC-ids.sqlite
```
"""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
from pathlib import Path

from tqdm import tqdm

from paper_parser.pubmed.parser import PaperParser
from paper_parser.pubmed.pmc_id_map import PmcIdMap
from paper_parser.shared.sentence_tokenizer import SentenceTokenizer, get_sentence_tokenizer
from paper_parser.shared.utils import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse all PMC OA XML files and write Paper JSONL."
    )
    parser.add_argument(
        "--xml-dir",
        type=Path,
        default=Path("data/pubmed/extracted"),
        help="Base directory containing extracted/ (default: data/pubmed/extracted)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/pubmed/papers.jsonl"),
        help="Output JSONL file (default: data/pubmed/papers.jsonl)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug mode: only process the first XML file",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help=(
            "Log file path. By default, logs are written under "
            "logs/pubmed/<timestamp>-parse-xml-to-jsonl.log"
        ),
    )
    parser.add_argument(
        "--spacy-model",
        type=str,
        default="en_core_sci_sm",
        help="spaCy/SciSpaCy model name to use for sentence tokenization "
        "(default: en_core_sci_sm)",
    )
    parser.add_argument(
        "--spacy-batch-size",
        type=int,
        default=64,
        help="Batch size for spaCy sentence tokenizer (default: 64)",
    )
    parser.add_argument(
        "--spacy-n-process",
        type=int,
        default=1,
        help="Number of processes for spaCy sentence tokenizer (default: 1)",
    )
    parser.add_argument(
        "--spacy-max-length",
        type=int,
        default=None,
        help=(
            "Maximum number of characters spaCy will accept per document "
            "(overrides nlp.max_length; default: library default)."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes to parse XML files in parallel (default: 1)",
    )
    parser.add_argument(
        "--pmc-ids-db",
        type=Path,
        default=None,
        help=(
            "Optional path to a compiled PMC-ids SQLite DB (see "
            "scripts/pubmed/build-pmc-id-db.py). When provided, both the "
            "paper's own ids and its bibliography entries are backfilled "
            "from this crosswalk (lookup works by PMCID, PMID, or DOI). "
            "Ids extracted from the XML always take precedence."
        ),
    )
    parser.add_argument(
        "--max-tasks-per-child",
        type=int,
        default=500,
        help=(
            "Recycle each worker after this many XML files to bound memory "
            "growth from spaCy/lxml/thinc caches and heap fragmentation. "
            "Forking a replacement is cheap because spaCy is already loaded "
            "in the parent (copy-on-write). Set to 0 to disable recycling. "
            "Default: 500."
        ),
    )
    return parser.parse_args()


_WORKER_TOKENIZER: SentenceTokenizer | None = None
_WORKER_PMC_ID_MAP: PmcIdMap | None = None


def _init_worker(tokenizer: SentenceTokenizer) -> None:
    """Store the tokenizer on a module global so workers don't have to receive
    it (and pickle it) on every task submitted through the pool's queue."""
    global _WORKER_TOKENIZER
    _WORKER_TOKENIZER = tokenizer


def _process_single_xml(xml_path: str) -> tuple[str, str | None, str | None]:
    assert _WORKER_TOKENIZER is not None, "worker tokenizer not initialized"
    try:
        paper = PaperParser(
            _WORKER_TOKENIZER, pmc_id_map=_WORKER_PMC_ID_MAP
        ).parse(xml_path)
        return xml_path, paper.model_dump_json(), None
    except Exception as e:
        return xml_path, None, str(e)


def main() -> None:
    args = parse_args()

    script_name = Path(__file__).stem
    log_path = Path("logs/pubmed") / f"{script_name}.log"
    setup_logging(log_path)
    logger = logging.getLogger(__name__)
    logger.info("Starting parse-xml-to-jsonl")
    logger.info(f"Arguments: {args}")

    tokenizer = get_sentence_tokenizer(
        engine="spacy",
        model_name=args.spacy_model,
        n_process=args.spacy_n_process,
        batch_size=args.spacy_batch_size,
        max_length=args.spacy_max_length,
    )

    if args.pmc_ids_db is not None:
        global _WORKER_PMC_ID_MAP
        _WORKER_PMC_ID_MAP = PmcIdMap(args.pmc_ids_db)

    base = args.xml_dir.resolve()
    xml_filepaths = sorted(str(p) for p in base.rglob("*.xml"))
    if not xml_filepaths:
        logger.warning(f"No XML files under {base}")
        return

    if args.debug:
        xml_filepaths = xml_filepaths[1000:2000]
        logger.info("Debug mode: only processing the first few XML files")

    logger.info(f"Found {len(xml_filepaths)} XML files; writing to {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with open(args.output, "w", encoding="utf-8") as f:
        written = 0
        skipped = 0
        if args.workers <= 1:
            _init_worker(tokenizer)
            for xml_filepath in tqdm(xml_filepaths):
                _, paper, error = _process_single_xml(xml_filepath)
                if paper is not None:
                    f.write(paper + "\n")
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
                initializer=_init_worker,
                initargs=(tokenizer,),
                maxtasksperchild=args.max_tasks_per_child or None,
            ) as pool:
                iterator = pool.imap_unordered(
                    _process_single_xml,
                    iter(xml_filepaths),
                    chunksize=5,
                )
                for xml_filepath, paper, error in tqdm(iterator, total=len(xml_filepaths)):
                    if paper is not None:
                        f.write(paper + "\n")
                        written += 1
                    else:
                        skipped += 1
                        logger.warning(f"Skip #{skipped}: {xml_filepath}: {error}")

    logger.info(f"Wrote {written} papers to {args.output} ({skipped} skipped)")


if __name__ == "__main__":
    main()
