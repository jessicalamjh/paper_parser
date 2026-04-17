"""
Run the PMC OA parser on all XML files under the extracted dirs and write papers to a JSONL file.

Each line of the output file is one parsed Paper as JSON (model_dump).

Usage (from project root):

```bash
uv run python scripts/pubmed/2-parse-xml-to-jsonl.py \
  --xml-dir sample_data/pubmed/raw \
  --output sample_data/pubmed/processed/sample.jsonl \
  --spacy-model en_core_sci_sm \
  --spacy-batch-size 32 \
  --spacy-n-process 1 \
  --workers 1
```
"""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
from functools import partial
from pathlib import Path

from tqdm import tqdm

from paper_parser.pubmed.parser import PaperParser
from paper_parser.shared.sentence_tokenizer import SentenceTokenizer, get_sentence_tokenizer
from paper_parser.shared.utils import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse all PMC OA XML files and write Paper JSONL."
    )
    parser.add_argument(
        "--xml-dir",
        type=Path,
        default=Path("data/pmcoa/extracted"),
        help="Base directory containing extracted/ (default: data/pmcoa/extracted)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/pmcoa/papers.jsonl"),
        help="Output JSONL file (default: data/pmcoa/papers.jsonl)",
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
            "logs/pmcoa/<timestamp>-parse-xml-to-jsonl.log"
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
        "--split",
        type=int,
        default=None,
    )
    return parser.parse_args()


def _process_single_xml(
    xml_path: str,
    tokenizer: SentenceTokenizer,
) -> tuple[str, str | None, str | None]:
    try:
        paper = PaperParser(tokenizer).parse(xml_path)
        return xml_path, paper.model_dump_json(), None
    except Exception as e:
        return xml_path, None, str(e)


def main() -> None:
    args = parse_args()

    script_name = Path(__file__).stem
    log_path = Path("logs/pmcoa") / f"{script_name}.log"
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

    base = args.xml_dir.resolve()
    xml_filepaths = sorted(str(p) for p in base.rglob("*.xml"))
    if not xml_filepaths:
        logger.warning(f"No XML files under {base}")
        return

    if args.debug:
        xml_filepaths = xml_filepaths[1000:2000]
        logger.info("Debug mode: only processing the first few XML files")

    if isinstance(args.split, int):
        start = args.split * 1000000
        end = start + 1000000
        logger.info(f"Working on filepaths #{start}:{end}")
        xml_filepaths = xml_filepaths[start:end]

    logger.info(f"Found {len(xml_filepaths)} XML files; writing to {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with open(args.output, "w", encoding="utf-8") as f:
        written = 0
        skipped = 0
        if args.workers <= 1:
            for xml_filepath in tqdm(xml_filepaths):
                _, paper, error = _process_single_xml(xml_filepath, tokenizer)
                if paper is not None:
                    f.write(paper + "\n")
                    written += 1
                else:
                    skipped += 1
                    logger.warning(f"Skip #{skipped}: {xml_filepath}: {error}")
        else:
            logger.info(f"Using {args.workers} worker processes for parsing")
            ctx = mp.get_context("fork")
            with ctx.Pool(processes=args.workers) as pool:
                worker_fn = partial(_process_single_xml, tokenizer=tokenizer)
                iterator = pool.imap_unordered(
                    worker_fn,
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
