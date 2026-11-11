"""Step 3 - Transform: turn validated raw strings into the typed, normalised
staging layout (one row per fact-grain record).

The small parse_* helpers are shared with validate.py so "what counts as a valid
value" is defined in exactly one place.
"""
import re

import pandas as pd

DATE_FORMAT = "%m/%d/%Y"  # Bing exports dates as M/D/YYYY (no zero padding)
_ID_RE = re.compile(r"^\d+$")

# source column -> staging column
COLUMN_MAP = {
    "Date": "report_date",
    "Customer": "customer_id",
    "Account number": "account_number",
    "Account name": "account_name",
    "Account status": "account_status",
    "Campaign name": "campaign_name",
    "Campaign status": "campaign_status",
    "Ad group ID": "ad_group_id",
    "Ad group": "ad_group_name",
    "Ad group status": "ad_group_status",
    "Ad ID": "ad_id",
    "Ad title": "ad_title",
    "Ad description": "ad_description",
    "Ad type": "ad_type",
    "Ad status": "ad_status",
    "Ad distribution": "ad_distribution",
    "Display URL": "display_url",
    "Final URL": "final_url",
    "Destination URL": "destination_url",
    "Tracking Template": "tracking_template",
    "Device type": "device_type",
    "Device OS": "device_os",
    "Network": "network",
    "Top vs. other": "top_vs_other",   # split into `placement` below
    "Delivered match type": "delivered_match_type",
    "BidMatchType": "bid_match_type",
    "Currency code": "currency_code",
    "Impressions": "impressions",
    "Clicks": "clicks",
    "Spend": "spend",
    "Avg. position": "avg_position",
    "Conversions": "conversions",
    "Assists": "assists",
}
# Intentionally NOT carried over (documented in README):
#   Custom Parameters, Final Mobile URL, Final App URL  -> 100% empty in the export
#   Language                                            -> constant ('English'), no analytic value
#   Customer/Account "status" etc. are kept; Ad distribution is kept (single value today, may vary).

ID_COLUMNS = ["customer_id", "ad_group_id", "ad_id"]
INT_METRICS = ["impressions", "clicks"]
DEC_METRICS = ["spend", "conversions", "assists"]


# ----------------------------------------------------------------- shared parsing helpers
def is_blank(s: pd.Series) -> pd.Series:
    return s.str.strip() == ""


def parse_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s.str.strip(), format=DATE_FORMAT, errors="coerce")


def clean_id(s: pd.Series) -> pd.Series:
    """'[6772783690]' -> '6772783690'. Bing wraps IDs in brackets to stop Excel mangling them."""
    return s.str.strip().str.strip("[]").str.strip()


def valid_id(s: pd.Series) -> pd.Series:
    return clean_id(s).map(lambda v: bool(_ID_RE.match(v)))


def parse_number(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.strip(), errors="coerce")


# ----------------------------------------------------------------- main transform
def transform(raw: pd.DataFrame) -> pd.DataFrame:
    """`raw` must already have passed validation (all-string frame, original headers)."""
    df = raw.rename(columns=COLUMN_MAP)[list(COLUMN_MAP.values())].copy()

    # Trim whitespace everywhere and turn empty strings into real NULLs.
    for col in df.columns:
        df[col] = df[col].str.strip().replace("", pd.NA)

    df["report_date"] = parse_date(df["report_date"]).dt.date
    # Integer surrogate-friendly date key (YYYYMMDD) joins to dim_date.
    df["date_key"] = pd.to_datetime(df["report_date"]).dt.strftime("%Y%m%d").astype(int)

    for col in ID_COLUMNS:
        df[col] = pd.to_numeric(clean_id(df[col]))

    for col in INT_METRICS:
        df[col] = pd.to_numeric(df[col]).astype(int)
    for col in DEC_METRICS + ["avg_position"]:
        df[col] = pd.to_numeric(df[col])  # avg_position stays NaN when there were no impressions

    # 'Bing and Yahoo! search - Top' -> placement 'Top'. The network part already exists in `Network`.
    df["placement"] = df["top_vs_other"].str.rsplit(" - ", n=1).str[-1].str.strip()
    df = df.drop(columns=["top_vs_other"])

    df["currency_code"] = df["currency_code"].str.upper()
    return df


def transform_file(validated_file, run_dir) -> dict:
    """File-in / file-out wrapper used by the Airflow task."""
    from pathlib import Path

    from .extract import read_raw

    clean = transform(read_raw(Path(validated_file)))
    out = Path(run_dir) / "03_clean.csv"
    clean.to_csv(out, index=False)
    return {"clean_file": str(out), "rows": int(len(clean))}
