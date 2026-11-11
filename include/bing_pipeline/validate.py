"""Step 2 - Validate: row-level data-quality gate.

Philosophy
----------
* REJECT   a row that cannot be loaded correctly (bad date, missing key, non-numeric or
           negative metric, duplicate grain). Rejected rows go to a quarantine file with
           the reason, so nothing disappears silently.
* WARN     on oddities that are real-world facts about the data (e.g. clicks > impressions
           from search-partner attribution). They are loaded and reported.
* FAIL     the whole run if too many rows are rejected (MAX_REJECT_RATE) - that points to an
           upstream problem, and loading a partial week would silently under-report spend.
"""
import logging
from pathlib import Path

import pandas as pd

from .config import MAX_REJECT_RATE
from .extract import read_raw
from .transform import clean_id, is_blank, parse_date, parse_number, valid_id

log = logging.getLogger(__name__)

# Columns that identify an entity or a grain member: blank = unusable row.
REQUIRED_VALUES = [
    "Date", "Customer", "Account number", "Campaign name", "Ad group ID", "Ad ID",
    "Device type", "Device OS", "Network", "Top vs. other",
    "Delivered match type", "BidMatchType", "Currency code",
]
ID_COLUMNS = ["Customer", "Ad group ID", "Ad ID"]
REQUIRED_METRICS = ["Impressions", "Clicks", "Spend", "Conversions", "Assists"]
GRAIN = ["Date", "Ad ID", "Device type", "Device OS", "Network", "Top vs. other",
         "Delivered match type", "BidMatchType"]


class ValidationError(Exception):
    """Too many rejected rows - the run must not proceed."""


def find_rejects(raw: pd.DataFrame) -> pd.Series:
    """Return a Series (same index as raw) with '' for good rows, else '; '-joined reasons."""
    reasons = pd.Series([[] for _ in range(len(raw))], index=raw.index, dtype=object)

    def flag(mask: pd.Series, reason: str):
        for i in reasons.index[mask.values]:
            reasons[i].append(reason)

    for col in REQUIRED_VALUES:
        flag(is_blank(raw[col]), f"missing {col}")

    flag(parse_date(raw["Date"]).isna() & ~is_blank(raw["Date"]), "unparseable Date (expected M/D/YYYY)")

    for col in ID_COLUMNS:
        flag(~valid_id(raw[col]) & ~is_blank(raw[col]), f"non-numeric {col}")

    for col in REQUIRED_METRICS:
        num = parse_number(raw[col])
        flag(num.isna(), f"non-numeric {col}")
        flag(num < 0, f"negative {col}")

    pos = parse_number(raw["Avg. position"])  # optional metric: blank is fine, garbage is not
    flag(pos.isna() & ~is_blank(raw["Avg. position"]), "non-numeric Avg. position")

    flag(~raw["Top vs. other"].str.contains(" - ", regex=False), "Top vs. other not in '<network> - <Top|Other>' form")

    # Duplicate grain: keep the first occurrence, quarantine the rest. Compared on *cleaned*
    # values so '[123]' and '123' count as the same ad.
    key = raw[GRAIN].apply(lambda c: c.str.strip())
    key["Ad ID"] = clean_id(key["Ad ID"])
    flag(key.duplicated(keep="first"), "duplicate fact grain")

    return reasons.map("; ".join)


def collect_warnings(good: pd.DataFrame) -> dict:
    """Non-blocking observations about rows that were accepted."""
    warnings = {}
    clicks, imps = parse_number(good["Clicks"]), parse_number(good["Impressions"])
    n = int((clicks > imps).sum())
    if n:
        warnings["clicks_exceed_impressions_rows"] = n
    currencies = sorted(good["Currency code"].str.strip().str.upper().unique())
    if len(currencies) > 1:
        # Spend would not be summable across currencies - analysts must group by currency_code.
        warnings["multiple_currencies"] = currencies
    placements = good["Top vs. other"].str.rsplit(" - ", n=1).str[-1].str.strip().unique()
    odd = sorted(set(placements) - {"Top", "Other"})
    if odd:
        warnings["unexpected_placement_values"] = odd
    return warnings


def validate(landed_file: Path, run_dir: Path) -> dict:
    raw = read_raw(Path(landed_file))
    reasons = find_rejects(raw)
    bad = reasons != ""

    good = raw[~bad]
    rejected = raw[bad].assign(reject_reason=reasons[bad])

    good_path, rejected_path = run_dir / "02_validated.csv", run_dir / "02_rejected.csv"
    good.to_csv(good_path, index=False)
    rejected.to_csv(rejected_path, index=False)  # always written (possibly header-only) for a stable path

    report = {
        "validated_file": str(good_path),
        "rejected_file": str(rejected_path),
        "rows_in": int(len(raw)),
        "rows_valid": int(len(good)),
        "rows_rejected": int(bad.sum()),
        "warnings": collect_warnings(good) if len(good) else {},
    }
    log.info("Validation report: %s", report)

    reject_rate = bad.mean()
    if reject_rate > MAX_REJECT_RATE:
        raise ValidationError(
            f"{report['rows_rejected']} of {report['rows_in']} rows rejected "
            f"({reject_rate:.2%} > allowed {MAX_REJECT_RATE:.2%}). See {rejected_path}."
        )
    return report
