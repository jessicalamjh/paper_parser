"""paper_parser: parse scientific papers from multiple sources into a common schema.

Top-level package. Source-specific parsers live in submodules:

- ``paper_parser.pubmed``  PubMed Central (PMC) JATS XML parser
- ``paper_parser.arxiv``   arXiv parser (work in progress)

Shared data structures live in ``paper_parser.shared``.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("paper-parser")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

from paper_parser.shared.schemas import (
    BibEntry,
    Content,
    Date,
    Figure,
    Paper,
    PaperId,
    Paragraph,
    Ref,
    Section,
    Sentence,
)

__all__ = [
    "__version__",
    "BibEntry",
    "Content",
    "Date",
    "Figure",
    "Paper",
    "PaperId",
    "Paragraph",
    "Ref",
    "Section",
    "Sentence",
]
