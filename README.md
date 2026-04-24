# paper_parser

Parse scientific papers from multiple sources into one shared [Pydantic](https://docs.pydantic.dev/) schema so downstream code can treat PMC, arXiv, and future sources the same way.

**Supported today**

- **PubMed Central (PMC)** — JATS XML under `paper_parser.pubmed`.
- **arXiv** — placeholder package only (`paper_parser.arxiv`); no parser yet.

The core document type is **`Paper`** (`paper_parser.shared.schemas`): identifiers, publication date, title, hierarchical sections/paragraphs/sentences, figures with captions, inline reference spans, and a bibliography map. Content is addressable via stable **`content_id`** paths such as `["abstract", 0, 1]` (see `Paper.get_content`).

## How it works (PMC)

Parsing is deliberately split into two phases:

1. **XML tree walk** — Build `Section`, `Paragraph`, and `Figure` nodes with placeholder sentences and collect every block of text that still needs splitting.
2. **Single batch tokenization** — Run `SentenceTokenizer.tokenize_batch` once over all collected texts, then fill each paragraph/figure caption with `Sentence` objects. Inline `<xref>` references in JATS are aligned to sentence character offsets.

Front-matter helpers (IDs, title, date, subjects, bibliography) do not use the tokenizer and can be called on their own.

Optional **`PmcIdMap`** loads an SQLite crosswalk built from NCBI’s [PMC-ids](https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz) file to backfill PMID/DOI on the article and on bibliography entries when the XML only has one id type. The repo’s `scripts/pubmed/1-build-pmc-id-db.py` builds that database; parsing works without it.

**Filename convention:** when you pass a file path into `PaperParser` / `extract_paper`, the stem must yield a numeric PMC id (e.g. `PMC1234567.xml` or `PMC1234567.nxml`) so the parser can set `paper_id`. If you only have an in-memory tree, ensure ids are still present in the XML or supply a path that encodes the PMC id.

## Installation

From [PyPI](https://pypi.org/project/paper-parser/):

```bash
pip install paper-parser
```

Requires **Python 3.12+**. The default install pulls in **`lxml`**, **`pydantic`**, and **`tqdm`** only.

Optional extras:

| Extra | Purpose |
|--------|--------|
| `spacy` | spaCy-based sentence splitting (`SpacySentenceTokenizer`, `get_sentence_tokenizer("spacy", ...)`) |
| `notebook` | Matplotlib, pandas, seaborn, and Jupyter tooling (for `notebooks/` in this repo) |
| `dev` | pytest, pytest-cov, ruff |
| `publish` | build, twine (for PyPI uploads) |

Examples:

```bash
pip install "paper-parser[spacy]"
pip install "paper-parser[notebook,dev]"   # local exploration + tests
```

For local development:

```bash
git clone https://github.com/jessicalamjh/paper_parser.git
cd paper_parser
pip install -e ".[dev]"
```

With [uv](https://docs.astral.sh/uv/):

```bash
uv sync --extra dev
```

Add `--extra spacy` or `--extra notebook` as needed.

## Quickstart (PMC)

Minimal example using the regex sentence splitter (no spaCy):

```python
from pathlib import Path

from paper_parser.pubmed import extract_paper
from paper_parser.shared.sentence_tokenizer import RegexSentenceTokenizer

tokenizer = RegexSentenceTokenizer()
paper = extract_paper(Path("PMC1234567.nxml"), tokenizer)

print(paper.paper_id)       # PaperId(id_type='pmc', value='PMC1234567')
print(paper.title.text)
print(paper.pub_date.year if paper.pub_date else None)
```

`get_sentence_tokenizer()` with no arguments also returns a **regex** tokenizer (no spaCy required).

Using spaCy (install the extra and a model):

```bash
pip install "paper-parser[spacy]"
python -m spacy download en_core_web_sm
```

```python
from paper_parser.pubmed import extract_paper
from paper_parser.shared.sentence_tokenizer import get_sentence_tokenizer

tokenizer = get_sentence_tokenizer("spacy", model_name="en_core_web_sm")
paper = extract_paper("path/to/PMC1234567.nxml", tokenizer)
```

`extract_paper` accepts a path, a string of XML, or an `lxml.etree._ElementTree`. Under the hood it is `PaperParser(tokenizer).parse(...)`.

### Optional PMC id crosswalk

```python
from paper_parser.pubmed import PaperParser, PmcIdMap
from paper_parser.shared.sentence_tokenizer import RegexSentenceTokenizer

pmc_map = PmcIdMap("path/to/PMC-ids.sqlite")
parser = PaperParser(RegexSentenceTokenizer(), pmc_id_map=pmc_map)
paper = parser.parse("PMC1234567.nxml")
```

### Front-matter only

These only need XML, not a tokenizer:

```python
from paper_parser.pubmed import (
    extract_bibliography,
    extract_paper_ids,
    extract_pub_date,
    extract_title_sentence,
)
```

### Serialization

`Paper` and nested models are Pydantic v2 models:

```python
paper.model_dump()
paper.model_dump_json()
Paper.model_json_schema()
```

### Package layout

```text
src/paper_parser/
    __init__.py       # re-exports main schema types + __version__
    shared/           # schemas, sentence tokenizers, utilities
    pubmed/           # PMC JATS parser, PmcIdMap
    arxiv/            # placeholder for future arXiv support
```

Batch utilities (JSONL export, building the PMC id DB, etc.) live under **`scripts/pubmed/`** in this repository; they are not installed with the PyPI wheel—clone the repo if you need those pipelines. The batch script `2-parse-xml-to-jsonl.py` uses spaCy; run it in an environment with `pip install -e ".[spacy]"` (or equivalent).

## Publishing to PyPI (maintainers)

1. Bump **`version`** in `pyproject.toml` and summarize changes in **`CHANGELOG.md`**.
2. Build artifacts in a clean tree:

   ```bash
   pip install -e ".[publish]"
   python -m build
   ```

   This produces `dist/paper_parser-<version>-py3-none-any.whl` and `dist/paper_parser-<version>.tar.gz`.

3. Upload to the real index (use a [PyPI API token](https://pypi.org/help/#apitoken)):

   ```bash
   python -m twine upload dist/*
   ```

4. Tag the release in git to match the published version.

Test uploads can go to [TestPyPI](https://test.pypi.org/) first:

```bash
python -m twine upload --repository testpypi dist/*
```

## License

MIT — see [LICENSE](LICENSE).
