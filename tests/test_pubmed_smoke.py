"""Smoke tests for PMC parsing (no spaCy)."""

from __future__ import annotations

from pathlib import Path

import pytest

from paper_parser.pubmed import extract_paper, extract_paper_ids, extract_title_sentence
from paper_parser.pubmed.utils import build_xml_tree
from paper_parser.shared.sentence_tokenizer import RegexSentenceTokenizer, get_sentence_tokenizer

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_XML = REPO_ROOT / "sample_data" / "pubmed" / "raw" / "PMC7000113.xml"


@pytest.fixture
def sample_xml_path() -> Path:
    if not SAMPLE_XML.is_file():
        pytest.skip(f"sample XML not present: {SAMPLE_XML}")
    return SAMPLE_XML


def test_get_sentence_tokenizer_defaults_to_regex() -> None:
    tok = get_sentence_tokenizer()
    assert isinstance(tok, RegexSentenceTokenizer)


def test_extract_paper_regex(sample_xml_path: Path) -> None:
    paper = extract_paper(sample_xml_path, RegexSentenceTokenizer())
    assert paper.paper_id.id_type == "pmc"
    assert paper.paper_id.value == "PMC7000113"
    assert paper.title.text
    assert "Detection" in paper.title.text or "Precursor" in paper.title.text


def test_front_matter_helpers(sample_xml_path: Path) -> None:
    tree = build_xml_tree(str(sample_xml_path))
    ids = extract_paper_ids(tree)
    assert any(i.id_type == "pmc" for i in ids)
    title = extract_title_sentence(tree)
    assert title.text


def test_paper_model_dump_roundtrip(sample_xml_path: Path) -> None:
    paper = extract_paper(sample_xml_path, RegexSentenceTokenizer())
    data = paper.model_dump()
    assert data["paper_id"]["id_type"] == "pmc"
    assert data["title"]["text"] == paper.title.text