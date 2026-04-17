"""Disk-backed crosswalk between PMCID, PMID, and DOI.

Source
------
NCBI publishes ``PMC-ids.csv`` (https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz)
as a canonical crosswalk between PMCID, PMID, and DOI for every article in
PMC. We use it both to backfill missing ids on the paper itself and to
enrich bibliography entries whose JATS ``<pub-id>`` tags only expose one id
type.

Storage
-------
The CSV ships with ~10M rows, and we need efficient lookups by *any* of the
three id types. Keeping three in-memory hash indexes is unacceptable -- that
alone would be several gigabytes. Instead we build a one-time SQLite DB
(typically sitting next to the CSV) with partial indexes on each column,
and ``lookup`` issues a single indexed query per call.

The ``Manuscript Id`` column in the CSV (NIHMS ids, assigned to author
manuscripts routed through NIH's Manuscript Submission system) is ignored
because our ``PaperId`` schema only supports ``pmc | pmid | doi``.

Fork safety
-----------
A ``PmcIdMap`` is typically constructed in the parent of a multiprocessing
pool and then referenced by forked workers. SQLite connections cannot be
shared across processes, so we open connections lazily and re-open whenever
the PID changes.
"""

from __future__ import annotations

import csv
import logging
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from tqdm import tqdm

from paper_parser.shared.schemas import PaperId

logger = logging.getLogger(__name__)


_CREATE_TABLE = """
CREATE TABLE ids (
    pmc  TEXT,
    pmid TEXT,
    doi  TEXT
)
"""

# Partial indexes skip the ~NULL majority for each column, keeping index size
# proportional to the number of actually-populated ids.
_CREATE_INDEXES = """
CREATE INDEX idx_pmc  ON ids(pmc)  WHERE pmc  IS NOT NULL;
CREATE INDEX idx_pmid ON ids(pmid) WHERE pmid IS NOT NULL;
CREATE INDEX idx_doi  ON ids(doi)  WHERE doi  IS NOT NULL;
"""

# sqlite column name for each PaperIdType.
_COL_FOR_TYPE: dict[str, str] = {"pmc": "pmc", "pmid": "pmid", "doi": "doi"}


class PmcIdMap:
    """SQLite-backed lookup from any ``PaperId`` to the other known ids."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path).resolve()
        if not self._db_path.exists():
            raise FileNotFoundError(f"PMC id DB not found: {self._db_path}")
        # Sanity-check the schema up front so mis-pointed paths fail fast
        # instead of on the first lookup inside a worker.
        with closing(self._open_readonly()) as conn:
            conn.execute("SELECT pmc, pmid, doi FROM ids LIMIT 0")

        self._conn: sqlite3.Connection | None = None
        self._conn_pid: int | None = None

    # ----- building -----

    @classmethod
    def build_from_csv(
        cls,
        csv_path: str | Path,
        db_path: str | Path,
        *,
        overwrite: bool = False,
    ) -> "PmcIdMap":
        """Build an indexed SQLite DB from NCBI's ``PMC-ids.csv``.

        The CSV is streamed, so peak memory stays small even for the full
        10M-row file. Bulk inserts run with ``journal_mode=OFF`` /
        ``synchronous=OFF`` to keep build time reasonable; the DB is only
        used read-only afterwards, so durability during build doesn't
        matter.
        """
        csv_path = Path(csv_path)
        db_path = Path(db_path)

        if db_path.exists():
            if not overwrite:
                raise FileExistsError(
                    f"{db_path} already exists; pass overwrite=True to rebuild."
                )
            db_path.unlink()

        logger.info(f"Building PMC-ids DB at {db_path} from {csv_path}")
        db_path.parent.mkdir(parents=True, exist_ok=True)

        with closing(sqlite3.connect(db_path)) as conn:
            conn.executescript(
                "PRAGMA journal_mode = OFF;"
                "PRAGMA synchronous  = OFF;"
                "PRAGMA temp_store   = MEMORY;"
                + _CREATE_TABLE
            )

            def iter_rows():
                with open(csv_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in tqdm(
                        reader, desc=f"Reading {csv_path.name}", unit=" rows"
                    ):
                        pmcid_raw = (row.get("PMCID") or "").strip()
                        digits = "".join(c for c in pmcid_raw if c.isdigit())
                        pmc = f"PMC{digits}" if digits else None

                        doi = (row.get("DOI") or "").strip() or None

                        pmid_raw = (row.get("PMID") or "").strip()
                        pmid_digits = "".join(c for c in pmid_raw if c.isdigit())
                        pmid = pmid_digits or None

                        if pmc is None and pmid is None and doi is None:
                            continue
                        yield (pmc, pmid, doi)

            conn.executemany("INSERT INTO ids VALUES (?, ?, ?)", iter_rows())
            conn.commit()

            logger.info("Creating indexes (this can take a few minutes)...")
            conn.executescript(_CREATE_INDEXES)
            conn.execute("ANALYZE")
            conn.commit()

        logger.info(f"PMC-ids DB ready at {db_path}")
        return cls(db_path)

    # ----- connections -----

    def _open_readonly(self) -> sqlite3.Connection:
        return sqlite3.connect(
            f"file:{self._db_path}?mode=ro",
            uri=True,
            check_same_thread=False,
        )

    def _conn_for_process(self) -> sqlite3.Connection:
        """Return a connection owned by the current process.

        Re-opens if we've been forked since the last call -- inheriting a
        SQLite connection across ``fork`` is undefined behavior.
        """
        pid = os.getpid()
        if self._conn is None or self._conn_pid != pid:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._conn = self._open_readonly()
            self._conn_pid = pid
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
                self._conn_pid = None

    def __len__(self) -> int:
        with closing(self._open_readonly()) as conn:
            (n,) = conn.execute("SELECT COUNT(*) FROM ids").fetchone()
            return int(n)

    # ----- lookup / augment -----

    def _row_for(self, paper_id: PaperId) -> tuple[str | None, str | None, str | None] | None:
        col = _COL_FOR_TYPE.get(paper_id.id_type)
        if col is None or not paper_id.value:
            return None
        conn = self._conn_for_process()
        return conn.execute(
            f"SELECT pmc, pmid, doi FROM ids WHERE {col} = ? LIMIT 1",
            (paper_id.value,),
        ).fetchone()

    def lookup(self, paper_id: PaperId) -> list[PaperId]:
        """Return the *other* known ids for the paper identified by ``paper_id``.

        The queried id itself is never echoed back. Malformed entries are
        logged and skipped so a bad row can't poison a parse run.
        """
        row = self._row_for(paper_id)
        if row is None:
            return []

        pmc, pmid, doi = row
        out: list[PaperId] = []
        for id_type, value in (("pmc", pmc), ("pmid", pmid), ("doi", doi)):
            if not value or id_type == paper_id.id_type:
                continue
            try:
                out.append(PaperId(id_type=id_type, value=value))
            except Exception as e:
                logger.warning(
                    f"Skipping invalid {id_type} {value!r} (looked up via "
                    f"{paper_id.id_type}={paper_id.value!r}): {e}"
                )
        return out

    def augment(self, paper_ids: list[PaperId]) -> list[PaperId]:
        """Return a list with missing id types backfilled from the DB.

        Existing ids always win -- we never overwrite a value that the
        caller already knows. If the input already covers all three id
        types (or is empty), the original list is returned unchanged.

        If the first id doesn't resolve to a row, we fall back to trying
        each remaining input id in turn.
        """
        if not paper_ids:
            return paper_ids

        existing_types = {pid.id_type for pid in paper_ids}
        if len(existing_types) >= len(_COL_FOR_TYPE):
            return paper_ids

        for pid in paper_ids:
            extras = self.lookup(pid)
            if not extras:
                continue
            result = list(paper_ids)
            for extra in extras:
                if extra.id_type not in existing_types:
                    result.append(extra)
                    existing_types.add(extra.id_type)
            return result
        return paper_ids
