"""Steps 4-8 - Load: schema, staging, dimensions, fact, post-load checks, views.

Design notes
------------
* Idempotent: every function can be re-run for the same file with the same result.
    - staging   : truncate + reload
    - dimensions: upsert on the natural key (insert new, overwrite changed = SCD1)
    - fact      : delete the dates present in staging, then insert (partition-style reload)
* Dimension loads are small (hundreds of rows) so they are done with plain Python dicts keyed
  by natural key. That keeps them portable, dtype-safe and easy to read.
* The fact load is set-based SQL (INSERT ... SELECT with joins) because it moves the volume
  and must resolve every surrogate key in one consistent transaction.
* Each public function opens its own transaction (engine.begin()) so an Airflow retry never
  leaves half-applied state.
"""
import logging
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, create_engine, delete, func, insert, select, text, update
from sqlalchemy.engine import Engine

from . import models as m
from .config import SQL_DIR, WAREHOUSE_URL

log = logging.getLogger(__name__)
CHUNK = 500


class DataQualityError(Exception):
    """Post-load reconciliation failed."""


def get_engine(url: str = None) -> Engine:
    # pool_pre_ping survives the MySQL idle-connection timeouts that hit long-lived workers.
    return create_engine(url or WAREHOUSE_URL, pool_pre_ping=True, future=True)


def _py(v):
    """pandas/numpy scalar -> plain Python value (NaN/NA -> None) for DB-API binding."""
    if v is None or (not isinstance(v, (str, date)) and pd.isna(v)):
        return None
    if hasattr(v, "item"):  # numpy scalar
        v = v.item()
    return v


def _records(df: pd.DataFrame) -> list:
    return [{k: _py(v) for k, v in row.items()} for row in df.to_dict("records")]


def read_clean(clean_file) -> pd.DataFrame:
    # keep_default_na=False + na_values=[''] -> only genuinely empty cells become NULL.
    return pd.read_csv(clean_file, keep_default_na=False, na_values=[""], parse_dates=["report_date"])


# ------------------------------------------------------------------ schema
def create_schema(engine: Engine) -> None:
    """Create any missing tables (checkfirst). Does not alter existing ones."""
    m.metadata.create_all(engine, checkfirst=True)


# ------------------------------------------------------------------ staging
def load_staging(engine: Engine, clean_file) -> int:
    df = read_clean(clean_file)
    df["report_date"] = df["report_date"].dt.date
    recs = _records(df)
    with engine.begin() as conn:
        conn.execute(delete(m.stg_ad_performance))
        for i in range(0, len(recs), CHUNK):
            conn.execute(insert(m.stg_ad_performance), recs[i:i + CHUNK])
    log.info("Staged %s rows", len(recs))
    return len(recs)


# ------------------------------------------------------------------ generic dimension upsert (SCD1)
def _upsert(conn, table, nk: list, attrs: list, rows: list) -> dict:
    """Insert unseen natural keys; overwrite attributes that changed. Returns counts."""
    pk = list(table.primary_key.columns)[0]
    existing = {
        tuple(r[c] for c in nk): r
        for r in conn.execute(select(table)).mappings()
    }
    to_insert, to_update = [], []
    for row in rows:
        key = tuple(row[c] for c in nk)
        old = existing.get(key)
        if old is None:
            to_insert.append({c: row[c] for c in nk + attrs})
        elif any(row[a] != old[a] for a in attrs):
            to_update.append({"pk_": old[pk.name], **{f"new_{a}": row[a] for a in attrs}})
    for i in range(0, len(to_insert), CHUNK):
        conn.execute(insert(table), to_insert[i:i + CHUNK])
    if to_update:
        # bindparam names are prefixed because SQLAlchemy forbids names equal to column names.
        stmt = update(table).where(pk == bindparam("pk_")).values({a: bindparam(f"new_{a}") for a in attrs})
        conn.execute(stmt, to_update)
    return {"inserted": len(to_insert), "updated": len(to_update), "unchanged": len(rows) - len(to_insert) - len(to_update)}


def _latest_per_key(df: pd.DataFrame, nk: list) -> pd.DataFrame:
    """Current attribute values = those from the most recent report date for each natural key."""
    return df.sort_values("report_date").drop_duplicates(nk, keep="last")


def _key_map(conn, table, nk: list) -> dict:
    pk = list(table.primary_key.columns)[0].name
    return {tuple(r[c] for c in nk): r[pk] for r in conn.execute(select(table)).mappings()}


# ------------------------------------------------------------------ dimension loaders
# Order matters: a parent dimension must exist before its children resolve their FK.
def load_dim_customer(engine, clean_file):
    df = _latest_per_key(read_clean(clean_file), ["customer_id"])
    with engine.begin() as conn:
        return _upsert(conn, m.dim_customer, ["customer_id"], [], _records(df[["customer_id"]]))


def load_dim_account(engine, clean_file):
    df = _latest_per_key(read_clean(clean_file), ["account_number"])
    with engine.begin() as conn:
        cust = _key_map(conn, m.dim_customer, ["customer_id"])
        rows = _records(df[["account_number", "account_name", "account_status", "customer_id"]])
        for r in rows:
            r["customer_key"] = cust[(r.pop("customer_id"),)]
        return _upsert(conn, m.dim_account, ["account_number"],
                       ["account_name", "account_status", "customer_key"], rows)


def load_dim_campaign(engine, clean_file):
    df = _latest_per_key(read_clean(clean_file), ["account_number", "campaign_name"])
    with engine.begin() as conn:
        acct = _key_map(conn, m.dim_account, ["account_number"])
        rows = _records(df[["account_number", "campaign_name", "campaign_status"]])
        for r in rows:
            r["account_key"] = acct[(r.pop("account_number"),)]
        return _upsert(conn, m.dim_campaign, ["account_key", "campaign_name"], ["campaign_status"], rows)


def load_dim_ad_group(engine, clean_file):
    df = _latest_per_key(read_clean(clean_file), ["ad_group_id"])
    with engine.begin() as conn:
        acct = _key_map(conn, m.dim_account, ["account_number"])
        camp = _key_map(conn, m.dim_campaign, ["account_key", "campaign_name"])
        rows = _records(df[["ad_group_id", "ad_group_name", "ad_group_status", "account_number", "campaign_name"]])
        for r in rows:
            r["campaign_key"] = camp[(acct[(r.pop("account_number"),)], r.pop("campaign_name"))]
        return _upsert(conn, m.dim_ad_group, ["ad_group_id"],
                       ["ad_group_name", "ad_group_status", "campaign_key"], rows)


AD_ATTRS = ["ad_title", "ad_description", "ad_type", "ad_status", "ad_distribution",
            "display_url", "final_url", "destination_url", "tracking_template"]


def load_dim_ad(engine, clean_file):
    df = _latest_per_key(read_clean(clean_file), ["ad_id"])
    with engine.begin() as conn:
        groups = _key_map(conn, m.dim_ad_group, ["ad_group_id"])
        rows = _records(df[["ad_id", "ad_group_id"] + AD_ATTRS])
        for r in rows:
            r["ad_group_key"] = groups[(r.pop("ad_group_id"),)]
        return _upsert(conn, m.dim_ad, ["ad_id"], AD_ATTRS + ["ad_group_key"], rows)


def _load_simple_dim(engine, clean_file, table, cols):
    df = read_clean(clean_file)[cols].drop_duplicates()
    with engine.begin() as conn:
        return _upsert(conn, table, cols, [], _records(df))


def load_dim_device(engine, clean_file):
    return _load_simple_dim(engine, clean_file, m.dim_device, ["device_type", "device_os"])


def load_dim_network(engine, clean_file):
    return _load_simple_dim(engine, clean_file, m.dim_network, ["network", "placement"])


def load_dim_match_type(engine, clean_file):
    return _load_simple_dim(engine, clean_file, m.dim_match_type, ["delivered_match_type", "bid_match_type"])


def load_dim_date(engine, clean_file):
    """Calendar rows for every day in [min, max] of the data (gaps included, so
    'days with no spend' can be reported)."""
    dates = read_clean(clean_file)["report_date"].dt.date
    lo, hi = dates.min(), dates.max()
    days = [lo + timedelta(n) for n in range((hi - lo).days + 1)]
    rows = [{
        "date_key": int(d.strftime("%Y%m%d")), "full_date": d, "year": d.year,
        "quarter": (d.month - 1) // 3 + 1, "month": d.month, "month_name": d.strftime("%B"),
        "day_of_month": d.day, "iso_week": d.isocalendar()[1], "day_of_week": d.isoweekday(),
        "day_name": d.strftime("%A"), "is_weekend": int(d.isoweekday() >= 6),
    } for d in days]
    with engine.begin() as conn:
        # Calendar attributes are deterministic, so "updates" never fire; they are listed as
        # attrs only so that _upsert includes them in the INSERT.
        return _upsert(conn, m.dim_date, ["date_key"], [k for k in rows[0] if k != "date_key"], rows)


# ------------------------------------------------------------------ fact
def load_fact(engine: Engine) -> dict:
    """Reload the fact rows for every date present in staging, resolving surrogate keys by join."""
    s, f = m.stg_ad_performance, m.fact_ad_performance
    with engine.begin() as conn:
        deleted = conn.execute(delete(f).where(f.c.date_key.in_(select(s.c.date_key).distinct()))).rowcount
        # INNER joins on purpose: a staging row that can't resolve a key is dropped here and then
        # caught loudly by the row-count reconciliation in run_quality_checks().
        result = conn.execute(text("""
            INSERT INTO fact_ad_performance
                (date_key, ad_key, device_key, network_key, match_type_key,
                 ad_group_key, campaign_key, account_key, currency_code,
                 impressions, clicks, spend, conversions, assists, avg_position)
            SELECT s.date_key, a.ad_key, dv.device_key, nw.network_key, mt.match_type_key,
                   g.ad_group_key, c.campaign_key, c.account_key, s.currency_code,
                   s.impressions, s.clicks, s.spend, s.conversions, s.assists, s.avg_position
            FROM stg_ad_performance s
            JOIN dim_ad         a  ON a.ad_id = s.ad_id
            JOIN dim_ad_group   g  ON g.ad_group_key = a.ad_group_key
            JOIN dim_campaign   c  ON c.campaign_key = g.campaign_key
            JOIN dim_device     dv ON dv.device_type = s.device_type AND dv.device_os = s.device_os
            JOIN dim_network    nw ON nw.network = s.network AND nw.placement = s.placement
            JOIN dim_match_type mt ON mt.delivered_match_type = s.delivered_match_type
                                  AND mt.bid_match_type = s.bid_match_type
        """))
    log.info("Fact reload: deleted %s, inserted %s", deleted, result.rowcount)
    return {"deleted": deleted, "inserted": result.rowcount}


# ------------------------------------------------------------------ post-load checks
def run_quality_checks(engine: Engine) -> dict:
    """Reconcile warehouse vs. staging. Raises DataQualityError on any mismatch."""
    s, f = m.stg_ad_performance, m.fact_ad_performance
    with engine.connect() as conn:
        stg = conn.execute(select(func.count(), func.sum(s.c.impressions), func.sum(s.c.clicks),
                                  func.sum(s.c.spend), func.sum(s.c.conversions))).one()
        fct = conn.execute(select(func.count(), func.sum(f.c.impressions), func.sum(f.c.clicks),
                                  func.sum(f.c.spend), func.sum(f.c.conversions))
                           .where(f.c.date_key.in_(select(s.c.date_key).distinct()))).one()
    names = ["row_count", "impressions", "clicks", "spend", "conversions"]
    result, failures = {}, []
    for name, a, b in zip(names, stg, fct):
        a, b = Decimal(str(a or 0)), Decimal(str(b or 0))
        result[name] = {"staging": float(a), "fact": float(b)}
        if abs(a - b) > Decimal("0.0001"):
            failures.append(f"{name}: staging={a} fact={b}")
    if stg[0] == 0:
        failures.append("staging is empty")
    if failures:
        raise DataQualityError("Reconciliation failed: " + "; ".join(failures))
    log.info("Quality checks passed: %s", result)
    return result


# ------------------------------------------------------------------ views
def refresh_views(engine: Engine, sql_file: Path = None) -> int:
    script = (sql_file or SQL_DIR / "views.sql").read_text()
    statements = [p.strip() for p in script.split("\n;;\n") if p.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    return len(statements)
