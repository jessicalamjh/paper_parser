"""Sentence tokenization utilities with pluggable backends.

Current backends:
- 'spacy': uses a spaCy/SciSpaCy pipeline (optional dependency)
- 'regex': simple, fast regex-based splitter
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Literal
import re


class SentenceTokenizer(ABC):
    """Abstract base class for sentence tokenizers."""

    @abstractmethod
    def tokenize_batch(self, texts: Iterable[str]) -> List[List[str]]:
        """Tokenize a batch of texts into sentences."""

    def tokenize(self, text: str) -> List[str]:
        """Tokenize a single text into sentences."""
        if not text:
            return []
        return self.tokenize_batch([text])[0]


class RegexSentenceTokenizer(SentenceTokenizer):
    """Very simple regex-based sentence splitter.

    Splits on punctuation followed by whitespace: (?<=[.!?])\\s+
    """

    def __init__(self, pattern: str = r"(?<=[.!?])\s+"):
        self._regex = re.compile(pattern)

    def tokenize_batch(self, texts: Iterable[str]) -> List[List[str]]:
        out: List[List[str]] = []
        for text in texts:
            text = (text or "").strip()
            if not text:
                out.append([])
                continue
            sents = [sent for sent in self._regex.split(text) if sent.strip()]
            out.append(sents)
        return out


_SPACY_PIPE_CACHE: dict[str, Any] = {}

class SpacySentenceTokenizer(SentenceTokenizer):
    """Sentence tokenizer based on spaCy/SciSpaCy.

    ``spacy`` is imported lazily so that consumers who only need the regex
    backend don't pay the import cost (nor hit spaCy/thinc/numpy ABI issues).
    """

    def __init__(
        self,
        model_name: str = "en_core_web_sm",
        n_process: int = 4,
        batch_size: int = 128,
        max_length: int | None = None,
    ):
        import spacy
        from spacy.symbols import ORTH

        self.model_name = model_name
        self.n_process = n_process
        self.batch_size = batch_size
        self.max_length = max_length
        self.abbreviations = {
            # Latin citation helpers
            "et.",
            "al.",
            "i.e.",
            # Section / figure / equation / number references
            "sec.", "secs.", "Sec.", "Secs.",
            "fig.", "figs.", "Fig.", "Figs.",
            "eq.", "eqs.", "Eq.", "Eqs.",
            "no.", "nos.", "No.", "Nos.",
            # Taxonomic shorthand
            "gen.", "sp.", "nov.",
        }

        nlp = _SPACY_PIPE_CACHE.get(model_name)
        if nlp is None:
            nlp = spacy.load(model_name)

            # Strip heavy components, keeping only a sentence-boundary setter.
            sent_pipes = {"sentencizer", "senter"}
            for name in list(nlp.pipe_names):
                if name not in sent_pipes:
                    nlp.remove_pipe(name)
            if not any(p in sent_pipes for p in nlp.pipe_names):
                nlp.add_pipe("sentencizer")

            _SPACY_PIPE_CACHE[model_name] = nlp

        for abbr in self.abbreviations:
            nlp.tokenizer.add_special_case(abbr, [{ORTH: abbr}])

        if self.max_length is not None:
            nlp.max_length = self.max_length

        self._nlp = nlp

    def tokenize_batch(self, texts: Iterable[str]) -> List[List[str]]:
        docs = self._nlp.pipe(texts, batch_size=self.batch_size, n_process=self.n_process)
        out: List[List[str]] = []
        for doc in docs:
            sents = [s.text for s in doc.sents if s.text and s.text.strip()]
            out.append(sents)
        return out


def get_sentence_tokenizer(
    engine: Literal["spacy", "regex"] = "regex",
    **kwargs,
) -> SentenceTokenizer:
    """Factory for sentence tokenizers.

    Default is ``regex`` so the core package does not require spaCy. Use
    ``engine="spacy"`` after installing ``hierarchical-paper-parser[spacy]`` (and a model).

    - engine="spacy": uses SpacySentenceTokenizer (requires spaCy installed)
        Supported kwargs include:
            - model_name: spaCy/SciSpaCy model name (default: "en_core_web_sm")
            - n_process, batch_size, max_length
            - extra_abbreviations: iterable of additional tokenizer special
              cases on top of DEFAULT_SPACY_ABBREVIATIONS
    - engine="regex": uses RegexSentenceTokenizer
        Supported kwargs include:
            - pattern: regex pattern for splitting
    """
    if engine == "spacy":
        return SpacySentenceTokenizer(**kwargs)
    if engine == "regex":
        return RegexSentenceTokenizer(**kwargs)
    raise ValueError(f"Unknown sentence tokenizer engine {engine!r}")

