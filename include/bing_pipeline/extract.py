"""Step 1 - Extract: land an immutable copy of the source file and check its shape.

Everything is read as *strings* (dtype=str). Typing is deliberately deferred to
the transform step so a malformed value can never crash the read or be silently
coerced before validation has looked at it.
"""
import logging
import shutil
from pathlib import Path

import pandas as pd

from .config import REQUIRED_COLUMNS

log = logging.getLogger(__name__)


class SchemaError(Exception):
    """Source file is missing or lacks required columns (not recoverable by row-level rejection)."""


def read_raw(path: Path) -> pd.DataFrame:
    # utf-8-sig strips the BOM that Bing exports start with; without it the first
    # header would be '\ufeffDate'. keep_default_na=False keeps literal text like "NA" intact.
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig", keep_default_na=False)


def extract(source: Path, run_dir: Path) -> dict:
    """Copy `source` into the run's landing area and verify the header.

    Returns small metadata (paths + row count) suitable for passing via Airflow XCom.
    """
    source = Path(source)
    if not source.is_file() or source.stat().st_size == 0:
        raise SchemaError(f"Source file missing or empty: {source}")

    df = read_raw(source)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(f"Source file is missing required columns: {missing}")
    if df.empty:
        raise SchemaError("Source file has a header but no data rows")

    run_dir.mkdir(parents=True, exist_ok=True)
    landed = run_dir / "01_raw.csv"
    shutil.copyfile(source, landed)  # landing copy = reproducible input for re-runs / audits
    log.info("Extracted %s rows x %s cols from %s", len(df), df.shape[1], source)
    return {"landed_file": str(landed), "rows": int(len(df))}
