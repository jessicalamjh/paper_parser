# paper_parser

Parse scientific papers from multiple sources into a common schema.

Currently supported sources:

- **PubMed Central (PMC)** — JATS XML, via `paper_parser.pubmed`.
- **arXiv** — placeholder (work in progress), via `paper_parser.arxiv`.

All parsers return [Pydantic](https://docs.pydantic.dev/) models defined in `paper_parser.shared.schemas`, so articles from different sources share a common structure (IDs, publication date, title, abstract, references, ...).

## Installation

```bash
pip install paper-parser
```

Or, for development:

```bash
git clone https://github.com/jessicalamjh/paper_parser.git
cd paper_parser
pip install -e ".[dev]"
```

Requires Python 3.12+.

## Quickstart

### PubMed Central (PMC)

```python
from paper_parser.pubmed import extract_article

article = extract_article("path/to/PMC1234567.xml")
print(article.article_ids.pmc)   # "PMC1234567"
print(article.title)
print(article.pub_date.year)
```

You can also call individual extractors:

```python
from paper_parser.pubmed import (
    extract_article_ids,
    extract_title,
    extract_abstract,
    extract_references,
)
```

`extract_article` accepts either a path to an XML file or an
`lxml.etree.ElementTree` instance. Pass `strict=False` to downgrade extraction
errors of individual fields to warnings.

### Schemas

All extractors return validated `paper_parser.shared.schemas` models:

- `Article`
- `ArticleIDs`
- `Date`
- `Reference`

```python
from paper_parser import Article

Article.model_json_schema()
```

## Project layout

```
src/paper_parser/
    __init__.py
    shared/        # cross-source Pydantic schemas
    pubmed/        # PMC JATS XML parser
    arxiv/         # (WIP) arXiv parser
```

## License

MIT — see [LICENSE](LICENSE).
